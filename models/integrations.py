"""
models/integrations.py
-----------------------
Per-client external system integrations — agency-configured during onboarding.
Lets the chatbot call a client's OWN backend (Calendly, Shopify, a custom REST
API, etc.) via a generic adapter instead of Lumvi's internal tables.

Mirrors the pattern in models/webhooks.py: plain functions, get_db() per call,
explicit cursor/conn close. Credentials are Fernet-encrypted via crypto_utils
before they ever touch Postgres.
"""
import json
import uuid
import logging
from datetime import datetime

from .db import get_db
from crypto_utils import encrypt_credentials, decrypt_credentials

logger = logging.getLogger(__name__)

_VALID_AUTH_TYPES = {'api_key', 'bearer', 'basic'}
_VALID_HTTP_METHODS = {'GET', 'POST', 'PUT', 'PATCH', 'DELETE'}


# =====================================================================
# client_ext_integrations — one row per external system an agency connects
# =====================================================================

def create_integration(client_id: str, name: str, base_url: str,
                        auth_type: str, credentials: dict,
                        created_by_agency_user_id: int = None) -> dict:
    """
    Create a new integration for a client. credentials is encrypted before storage.
    Returns {success, integration_id} or {success: False, error}.
    """
    auth_type = (auth_type or '').strip().lower()
    if auth_type not in _VALID_AUTH_TYPES:
        return {'success': False, 'error': f'auth_type must be one of {sorted(_VALID_AUTH_TYPES)}'}

    base_url = (base_url or '').strip()
    if not base_url.startswith(('http://', 'https://')):
        return {'success': False, 'error': 'base_url must start with http:// or https://'}

    conn, cursor = get_db()
    try:
        integration_id = str(uuid.uuid4())
        encrypted = encrypt_credentials(credentials or {})
        cursor.execute(
            """INSERT INTO client_ext_integrations
                   (integration_id, client_id, name, base_url, auth_type,
                    encrypted_credentials, active, created_by_agency_user_id)
               VALUES (%s, %s, %s, %s, %s, %s, TRUE, %s)""",
            (integration_id, client_id, (name or 'Integration')[:100],
             base_url, auth_type, encrypted, created_by_agency_user_id)
        )
        conn.commit()
        logger.info(f'[Integrations] created integration={integration_id} client={client_id} name={name}')
        return {'success': True, 'integration_id': integration_id}
    except Exception as e:
        conn.rollback()
        logger.error(f'[Integrations] create_integration error client={client_id}: {e}')
        return {'success': False, 'error': 'Could not save the integration.'}
    finally:
        cursor.close()
        conn.close()


def get_integrations(client_id: str) -> list:
    """Return all integrations for a client (credentials NOT decrypted here — list view only)."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            """SELECT integration_id, name, base_url, auth_type, active, created_at
               FROM client_ext_integrations WHERE client_id = %s ORDER BY created_at""",
            (client_id,)
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return [{
            'integration_id': r['integration_id'],
            'name':            r['name'],
            'base_url':        r['base_url'],
            'auth_type':       r['auth_type'],
            'active':          bool(r['active']),
            'created_at':      r['created_at'].isoformat() if r['created_at'] else None,
        } for r in rows]
    except Exception as e:
        logger.error(f'[Integrations] get_integrations error client={client_id}: {e}')
        return []


def get_integration_with_credentials(integration_id: str) -> dict:
    """
    Fetch one integration row WITH decrypted credentials.
    Only called server-side at execution time (pipeline/integration_adapter.py)
    — never returned to the dashboard/API.
    """
    try:
        conn, cursor = get_db()
        cursor.execute(
            """SELECT integration_id, client_id, name, base_url, auth_type,
                      encrypted_credentials, active
               FROM client_ext_integrations WHERE integration_id = %s""",
            (integration_id,)
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if not row:
            return {}
        return {
            'integration_id': row['integration_id'],
            'client_id':       row['client_id'],
            'name':            row['name'],
            'base_url':        row['base_url'],
            'auth_type':       row['auth_type'],
            'credentials':     decrypt_credentials(row['encrypted_credentials']),
            'active':          bool(row['active']),
        }
    except Exception as e:
        logger.error(f'[Integrations] get_integration_with_credentials error id={integration_id}: {e}')
        return {}


def update_integration_credentials(integration_id: str, credentials: dict) -> bool:
    """Re-encrypt and replace stored credentials (e.g. agency rotates a client's API key)."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            """UPDATE client_ext_integrations SET encrypted_credentials = %s, updated_at = CURRENT_TIMESTAMP
               WHERE integration_id = %s""",
            (encrypt_credentials(credentials or {}), integration_id)
        )
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f'[Integrations] update_integration_credentials error id={integration_id}: {e}')
        return False


def set_integration_active(integration_id: str, active: bool) -> bool:
    """Enable/disable an integration without deleting it (e.g. client churns, agency pauses it)."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            "UPDATE client_ext_integrations SET active = %s, updated_at = CURRENT_TIMESTAMP WHERE integration_id = %s",
            (bool(active), integration_id)
        )
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f'[Integrations] set_integration_active error id={integration_id}: {e}')
        return False


def delete_integration(integration_id: str) -> bool:
    """Delete an integration and its actions (ON DELETE CASCADE handles actions)."""
    try:
        conn, cursor = get_db()
        cursor.execute("DELETE FROM client_ext_integrations WHERE integration_id = %s", (integration_id,))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f'[Integrations] delete_integration error id={integration_id}: {e}')
        return False


# =====================================================================
# client_ext_integration_actions — the abstract "tools" Gemini can call,
# each mapped to a real endpoint on the client's external system
# =====================================================================

def add_action(integration_id: str, action_name: str, http_method: str,
                endpoint_path: str, param_mapping: dict, response_mapping: dict = None,
                requires_confirmation: bool = True, description: str = '',
                amount_param: str = None, max_auto_amount: float = None) -> dict:
    """
    Define one callable action on an integration. action_name is what Gemini sees
    as the function name (e.g. 'book_appointment'), param_mapping translates
    Lumvi's param names to the client API's field names.

    amount_param / max_auto_amount: optional spend-cap pair for financial actions
    (refunds, discounts, deposits). amount_param must be a key already present in
    param_mapping — it names which extracted parameter holds a dollar amount. If
    set, any call where that amount exceeds max_auto_amount is escalated to a
    human instead of executing or asking the end user to confirm, regardless of
    requires_confirmation. See pipeline/stages/agent_actions.py::_check_spend_cap.
    """
    http_method = (http_method or '').strip().upper()
    if http_method not in _VALID_HTTP_METHODS:
        return {'success': False, 'error': f'http_method must be one of {sorted(_VALID_HTTP_METHODS)}'}
    if not action_name or not action_name.replace('_', '').isalnum():
        return {'success': False, 'error': 'action_name must be alphanumeric/underscore (used as a Gemini function name).'}
    if not endpoint_path:
        return {'success': False, 'error': 'endpoint_path is required'}
    if amount_param and amount_param not in (param_mapping or {}):
        return {'success': False, 'error': 'amount_param must be one of the keys in param_mapping'}
    if max_auto_amount is not None and amount_param is None:
        return {'success': False, 'error': 'max_auto_amount requires amount_param to be set'}

    conn, cursor = get_db()
    try:
        cursor.execute(
            """INSERT INTO client_ext_integration_actions
                   (integration_id, action_name, description, http_method, endpoint_path,
                    param_mapping, response_mapping, requires_confirmation,
                    amount_param, max_auto_amount, active)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE)
               RETURNING id""",
            (integration_id, action_name[:50], (description or '')[:500], http_method,
             endpoint_path[:300], json.dumps(param_mapping or {}),
             json.dumps(response_mapping or {}), bool(requires_confirmation),
             amount_param, max_auto_amount)
        )
        action_id = cursor.fetchone()['id']
        conn.commit()
        logger.info(f'[Integrations] added action={action_name} integration={integration_id}')
        return {'success': True, 'action_id': action_id}
    except Exception as e:
        conn.rollback()
        logger.error(f'[Integrations] add_action error integration={integration_id}: {e}')
        return {'success': False, 'error': 'Could not save the action.'}
    finally:
        cursor.close()
        conn.close()


def get_actions_for_client(client_id: str) -> list:
    """
    Every active action across every active integration for this client —
    this is the single source of truth pipeline/integration_tools.py reads
    to build the per-client Gemini function-calling schema.
    """
    try:
        conn, cursor = get_db()
        cursor.execute(
            """SELECT cia.id AS action_id, cia.integration_id, cia.action_name,
                      cia.description, cia.http_method, cia.endpoint_path,
                      cia.param_mapping, cia.response_mapping, cia.requires_confirmation,
                      cia.amount_param, cia.max_auto_amount,
                      ci.name AS integration_name
               FROM client_ext_integration_actions cia
               JOIN client_ext_integrations ci ON ci.integration_id = cia.integration_id
               WHERE ci.client_id = %s AND ci.active = TRUE AND cia.active = TRUE
               ORDER BY cia.id""",
            (client_id,)
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        result = []
        for r in rows:
            result.append({
                'action_id':             r['action_id'],
                'integration_id':        r['integration_id'],
                'integration_name':      r['integration_name'],
                'action_name':           r['action_name'],
                'description':           r['description'] or '',
                'http_method':           r['http_method'],
                'endpoint_path':         r['endpoint_path'],
                'param_mapping':         json.loads(r['param_mapping'] or '{}'),
                'response_mapping':      json.loads(r['response_mapping'] or '{}'),
                'requires_confirmation': bool(r['requires_confirmation']),
                'amount_param':          r['amount_param'],
                'max_auto_amount':       float(r['max_auto_amount']) if r['max_auto_amount'] is not None else None,
            })
        return result
    except Exception as e:
        logger.error(f'[Integrations] get_actions_for_client error client={client_id}: {e}')
        return []


def get_action_by_id(action_id: int) -> dict:
    """Single action lookup, used at execution time."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            """SELECT id AS action_id, integration_id, action_name, http_method,
                      endpoint_path, param_mapping, response_mapping, requires_confirmation,
                      amount_param, max_auto_amount
               FROM client_ext_integration_actions WHERE id = %s""",
            (action_id,)
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if not row:
            return {}
        return {
            'action_id':             row['action_id'],
            'integration_id':        row['integration_id'],
            'action_name':           row['action_name'],
            'http_method':           row['http_method'],
            'endpoint_path':         row['endpoint_path'],
            'param_mapping':         json.loads(row['param_mapping'] or '{}'),
            'response_mapping':      json.loads(row['response_mapping'] or '{}'),
            'requires_confirmation': bool(row['requires_confirmation']),
            'amount_param':          row['amount_param'],
            'max_auto_amount':       float(row['max_auto_amount']) if row['max_auto_amount'] is not None else None,
        }
    except Exception as e:
        logger.error(f'[Integrations] get_action_by_id error id={action_id}: {e}')
        return {}


def delete_action(action_id: int) -> bool:
    try:
        conn, cursor = get_db()
        cursor.execute("DELETE FROM client_ext_integration_actions WHERE id = %s", (action_id,))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f'[Integrations] delete_action error id={action_id}: {e}')
        return False


# =====================================================================
# action_log — plain-English audit trail, surfaced to the small business
# (see chat thread: "favor both agency and end users" requirement)
# =====================================================================

def log_action_event(client_id: str, session_id: str, integration_id: str,
                      action_name: str, params: dict, result: dict,
                      summary_override: str = None) -> None:
    """
    Record one executed action for the audit trail. Never raises —
    a logging failure must not break the chat response.

    summary_override: use for events that weren't a normal execute
    attempt (e.g. spend-cap escalation) so the log doesn't misleadingly
    say "failed" for something that was never attempted.
    """
    try:
        conn, cursor = get_db()
        success = bool(result.get('success'))
        summary = summary_override or _build_plain_english_summary(action_name, params, result)
        cursor.execute(
            """INSERT INTO integration_action_log
                   (client_id, session_id, integration_id, action_name,
                    params, result, success, summary)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (client_id, session_id, integration_id, action_name,
             json.dumps(params or {})[:2000], json.dumps(result or {})[:2000],
             success, summary)
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f'[Integrations] log_action_event error client={client_id}: {e}')


def get_action_log(client_id: str, limit: int = 50) -> list:
    """Plain-English action history for a client — what the dashboard shows the small business."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            """SELECT action_name, summary, success, fired_at
               FROM integration_action_log
               WHERE client_id = %s ORDER BY fired_at DESC LIMIT %s""",
            (client_id, limit)
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return [{
            'action_name': r['action_name'],
            'summary':     r['summary'],
            'success':     bool(r['success']),
            'fired_at':    r['fired_at'].isoformat() if r['fired_at'] else None,
        } for r in rows]
    except Exception as e:
        logger.error(f'[Integrations] get_action_log error client={client_id}: {e}')
        return []


def _build_plain_english_summary(action_name: str, params: dict, result: dict) -> str:
    """Best-effort human-readable line for the audit log. Stays generic — action-specific
    phrasing can be layered in later per action type if needed."""
    label = action_name.replace('_', ' ')
    if result.get('success'):
        detail = ', '.join(f'{k}: {v}' for k, v in list(params.items())[:3])
        return f'{label.capitalize()} completed' + (f' ({detail})' if detail else '')
    return f'{label.capitalize()} failed — {result.get("error", "unknown error")}'
