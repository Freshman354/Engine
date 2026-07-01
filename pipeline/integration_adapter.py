"""
pipeline/integration_adapter.py
--------------------------------
Executes one client_integration_actions row against the client's actual
external system (their Calendly, Shopify, custom REST API, etc.).

This is the agentic execution layer that complements tools.py — tools.py
calls Lumvi's own DB tables (orders, appointments); this adapter calls
OUT to whatever system the agency wired up for a specific client during
onboarding (models/integrations.py).

Usage (called from the agent loop once Gemini returns a function_call
whose name matches a row from models.get_actions_for_client):

    from pipeline.integration_adapter import execute_client_action
    result = execute_client_action(action_id=42, params={"date": "2026-07-01"})
"""
import logging
import requests

import models
from utils import log_crash

logger = logging.getLogger('lumvi.integration_adapter')

_REQUEST_TIMEOUT_SECONDS = 10


class GenericRESTAdapter:
    """Builds auth + request from an integration row, fires it, maps the response."""

    def __init__(self, integration: dict):
        self.base_url = integration['base_url']
        self.auth_type = integration['auth_type']
        self.credentials = integration.get('credentials') or {}

    def _build_auth(self):
        """Returns (requests_auth_tuple_or_None, extra_headers_dict)."""
        if self.auth_type == 'api_key':
            header_name = self.credentials.get('header_name', 'X-API-Key')
            return None, {header_name: self.credentials.get('api_key', '')}
        if self.auth_type == 'bearer':
            return None, {'Authorization': f"Bearer {self.credentials.get('token', '')}"}
        if self.auth_type == 'basic':
            return (self.credentials.get('username', ''), self.credentials.get('password', '')), {}
        return None, {}

    def execute(self, action: dict, params: dict) -> dict:
        """
        action: a row from models.get_actions_for_client() / get_action_by_id()
        params: the raw args Gemini extracted (Lumvi-side param names)

        Returns {success: True, ...mapped fields} or {success: False, error: str}.
        Never raises — callers (the agent loop, confirmation flow) depend on that.
        """
        auth, headers = self._build_auth()
        url = self.base_url.rstrip('/') + '/' + action['endpoint_path'].lstrip('/')

        param_mapping = action.get('param_mapping') or {}
        mapped_params = {
            param_mapping[k]: v
            for k, v in (params or {}).items()
            if k in param_mapping
        }

        method = action['http_method']

        try:
            resp = requests.request(
                method=method,
                url=url,
                json=mapped_params if method in ('POST', 'PUT', 'PATCH') else None,
                params=mapped_params if method in ('GET', 'DELETE') else None,
                headers=headers,
                auth=auth,
                timeout=_REQUEST_TIMEOUT_SECONDS,
            )
            resp.raise_for_status()
            try:
                data = resp.json()
            except ValueError:
                data = {}
            return self._extract_response(data, action.get('response_mapping') or {})
        except requests.exceptions.RequestException as e:
            log_crash(logger, 'IntegrationAdapter/execute', e,
                      action=action.get('action_name'), url=url)
            return {'success': False, 'error': 'The external system could not complete this request right now.'}

    @staticmethod
    def _extract_response(data: dict, mapping: dict) -> dict:
        out = {'success': True}
        for key, path in mapping.items():
            val = data
            for part in path.split('.'):
                val = val.get(part) if isinstance(val, dict) else None
                if val is None:
                    break
            out[key] = val
        # If no response_mapping configured, hand back the raw payload (capped)
        if not mapping:
            out['raw'] = data
        return out


def execute_client_action(action_id: int, params: dict, client_id: str = None, session_id: str = None) -> dict:
    """
    High-level entry point: looks up the action + its parent integration,
    fires the request, and writes the audit log row. This is what the agent
    loop / confirmation flow should call — not GenericRESTAdapter directly.
    """
    action = models.get_action_by_id(action_id)
    if not action:
        return {'success': False, 'error': 'This action is no longer configured.'}

    integration = models.get_integration_with_credentials(action['integration_id'])
    if not integration or not integration.get('active', True):
        return {'success': False, 'error': 'This integration is currently unavailable.'}

    adapter = GenericRESTAdapter(integration)
    result = adapter.execute(action, params)

    models.log_action_event(
        client_id=client_id or integration.get('client_id'),
        session_id=session_id,
        integration_id=action['integration_id'],
        action_name=action['action_name'],
        params=params,
        result=result,
    )

    return result
