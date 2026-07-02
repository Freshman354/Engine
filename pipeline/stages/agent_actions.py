"""
pipeline/stages/agent_actions.py
---------------------------------
Gemini function-calling dispatch for agency-configured external integrations
(models/integrations.py + pipeline/integration_adapter.py).

This is intentionally separate from pipeline/stages/intent.py's keyword-based
dispatch_tool() (Tier 2B), which routes to tools.py's fixed, Lumvi-internal
tools (orders/appointments/human_inbox). This module routes to a CLIENT'S
OWN external system (Calendly, Shopify, a custom REST API, etc.) and the
available actions are arbitrary and per-client, so they can't be matched
with a static keyword dict — they require an actual Gemini function-calling
call.

Cost discipline: client_has_active_actions() is a cheap, short-TTL cached
check. Most clients have configured zero external integrations (this is an
opt-in, agency-onboarding feature), so the Gemini tools-call below only
ever runs for clients who configured at least one action. It is NOT free
for clients who do — folds into the per-turn Gemini call budget described
in pipeline/context.py and should be accounted for there as the pipeline
matures.
"""
import logging
import re
import threading
import time
from typing import Any, Dict, List, Optional

import models
from pipeline.integration_adapter import execute_client_action
from utils import log_crash

logger = logging.getLogger('lumvi.agent_actions')

# Word-boundary matched, not substring matched — plain `in` matching would
# false-positive on e.g. "it's broken" (contains "ok") or "is it not
# possible" (contains "no" inside "not"), which could silently fire or
# cancel a real action (booking, refund) the user never actually confirmed.
_CONFIRM_YES = {'yes', 'yeah', 'yep', 'sure', 'please', 'ok', 'okay', 'confirm'}
_CONFIRM_YES_PHRASES = {'go ahead', 'do it'}
_CONFIRM_NO = {'no', 'nope', 'cancel', 'stop', 'dont'}
_CONFIRM_NO_PHRASES = {'not now', "don't", 'never mind'}


def _matches_confirm_set(msg_lower: str, words: set, phrases: set) -> bool:
    tokens = set(re.findall(r"[a-z']+", msg_lower))
    if tokens & words:
        return True
    return any(p in msg_lower for p in phrases)

# ── Cheap "does this client have any active external actions?" cache ──────────
# In-process, short TTL. Avoids a DB round trip on every single message for
# the (large) majority of clients who have zero integrations configured.
_actions_cache: Dict[str, tuple] = {}   # client_id -> (actions_list, expires_at)
_actions_cache_lock = threading.Lock()
_ACTIONS_CACHE_TTL_SECONDS = 60


def _get_cached_actions(client_id: str) -> List[Dict]:
    if not client_id:
        return []
    now = time.time()
    with _actions_cache_lock:
        cached = _actions_cache.get(client_id)
        if cached and cached[1] > now:
            return cached[0]
    try:
        actions = models.get_actions_for_client(client_id)
    except Exception as e:
        log_crash(logger, 'AgentActions/get_actions', e, client_id=client_id)
        actions = []
    with _actions_cache_lock:
        _actions_cache[client_id] = (actions, now + _ACTIONS_CACHE_TTL_SECONDS)
    return actions


def client_has_active_actions(client_id: Optional[str]) -> bool:
    """Cheap gate — call this BEFORE doing anything Gemini-related."""
    if not client_id:
        return False
    return len(_get_cached_actions(client_id)) > 0


# =====================================================================
# Gemini function-calling schema
# =====================================================================

def _build_function_declarations(actions: List[Dict]):
    """Convert client_ext_integration_actions rows into Gemini FunctionDeclarations."""
    from google.genai import types as _types

    declarations = []
    for action in actions:
        param_names = list((action.get('param_mapping') or {}).keys())
        declarations.append(
            _types.FunctionDeclaration(
                name=action['action_name'],
                description=action.get('description') or
                    f"Performs {action['action_name'].replace('_', ' ')} on the client's system.",
                parameters={
                    'type': 'object',
                    'properties': {p: {'type': 'string'} for p in param_names},
                    'required': param_names,
                },
            )
        )
    return declarations


def _call_gemini_with_tools(genai_client: Any, model_name: str, clean_message: str,
                             conversation_history: List[Dict], actions: List[Dict]) -> Optional[Dict]:
    """
    Single Gemini call with function-calling enabled. Returns
    {'name': action_name, 'args': {...}} if Gemini chose to call a tool,
    or None if it didn't (i.e. this message isn't an action request).
    """
    from google.genai import types as _types

    tools = [_types.Tool(function_declarations=_build_function_declarations(actions))]

    # Minimal conversational context — last few turns, not the full history,
    # to keep this call cheap and fast.
    recent = conversation_history[-6:] if conversation_history else []
    history_text = '\n'.join(
        f"{t.get('role', 'user')}: {t.get('content', '')}" for t in recent
    )
    prompt = (
        f"Recent conversation:\n{history_text}\n\n"
        f"Latest customer message: \"{clean_message}\"\n\n"
        "If this message is requesting an action you have a tool for, call that tool "
        "with the best arguments you can extract from the conversation. "
        "If it's just a question or doesn't match any available action, don't call a tool."
    )

    try:
        response = genai_client.generate_content(
            model=model_name,
            contents=prompt,
            config=_types.GenerateContentConfig(
                temperature=0.1,
                max_output_tokens=256,
                tools=tools,
            ),
        )
        candidates = getattr(response, 'candidates', None) or []
        if not candidates:
            return None
        parts = getattr(candidates[0].content, 'parts', None) or []
        for part in parts:
            fn_call = getattr(part, 'function_call', None)
            if fn_call and getattr(fn_call, 'name', None):
                return {'name': fn_call.name, 'args': dict(fn_call.args or {})}
        return None
    except Exception as e:
        log_crash(logger, 'AgentActions/call_gemini_with_tools', e, model=model_name)
        return None


# =====================================================================
# Dispatch — called from ai_helper.generate_response()
# =====================================================================

def _check_spend_cap(action: Dict, args: Dict) -> Optional[str]:
    """
    Returns None if no cap is configured or the amount is within it.
    Returns a short, user-safe reason string if the cap is breached (or the
    amount couldn't be determined) — caller should escalate to a human
    rather than execute or ask the end user to confirm. A confirmation
    prompt is not real oversight for an above-cap amount; only a human
    is.
    """
    amount_param = action.get('amount_param')
    cap = action.get('max_auto_amount')
    if not amount_param or cap is None:
        return None

    raw = args.get(amount_param)
    try:
        amount = float(str(raw).replace(',', '').replace('$', ''))
    except (TypeError, ValueError):
        return f"the {amount_param.replace('_', ' ')} value couldn't be determined"

    if amount > cap:
        return f"${amount:,.2f} exceeds the ${cap:,.2f} auto-approval limit set for this action"
    return None


def _escalation_result(action: Dict, reason: str, client_id: str, session_id: str,
                        params: Dict) -> Dict:
    label = action['action_name'].replace('_', ' ')
    logger.info(f"[AgentActions] spend cap breach client={client_id} action={action['action_name']} reason={reason}")
    models.log_action_event(
        client_id=client_id, session_id=session_id, integration_id=action['integration_id'],
        action_name=action['action_name'], params=params, result={'success': False},
        summary_override=f"{label.capitalize()} held for approval — {reason}",
    )
    return {
        'response': (
            f"I want to get this right, so I'm not going to {label} on my own here — "
            "let me get someone from the team to take care of this for you."
        ),
        'method': 'external_action_cap_exceeded',
        'action': {'action_name': action['action_name'], 'status': 'escalated', 'reason': reason},
    }


def try_dispatch_external_action(genai_client: Any, model_name: str, client_id: str,
                                  session_id: str, clean_message: str,
                                  conversation_history: List[Dict]) -> Optional[Dict]:
    """
    Attempt to match the message against this client's configured external
    actions via Gemini function-calling. Returns a dict shaped for
    PipelineResult construction in ai_helper.py, or None if no action
    should fire (caller should fall through to normal RAG/response flow).

    Return shape on a match:
        {
            'response':   str,                       # what to say to the user
            'method':     'external_action' | 'external_action_confirm'
                           | 'external_action_cap_exceeded',
            'pending':    Optional[Dict],              # set on session_mem if confirmation needed
            'action':     {...},                       # audit/debug payload
        }
    """
    actions = _get_cached_actions(client_id)
    if not actions:
        return None

    matched = _call_gemini_with_tools(genai_client, model_name, clean_message, conversation_history, actions)
    if not matched:
        return None

    action = next((a for a in actions if a['action_name'] == matched['name']), None)
    if not action:
        logger.warning(f"[AgentActions] Gemini called unknown action '{matched['name']}' client={client_id}")
        return None

    label = action['action_name'].replace('_', ' ')

    # Spend cap overrides requires_confirmation — an above-cap amount needs a
    # human, not just a "yes" from the end user.
    cap_breach_reason = _check_spend_cap(action, matched['args'])
    if cap_breach_reason:
        return _escalation_result(action, cap_breach_reason, client_id, session_id, matched['args'])

    if action['requires_confirmation']:
        return {
            'response': f"Just to confirm — should I go ahead and {label}?",
            'method': 'external_action_confirm',
            'pending': {
                'action_id': action['action_id'],
                'action_name': action['action_name'],
                'params': matched['args'],
            },
            'action': {'action_name': action['action_name'], 'status': 'pending_confirmation'},
        }

    result = execute_client_action(
        action_id=action['action_id'], params=matched['args'],
        client_id=client_id, session_id=session_id,
    )
    return _result_to_pipeline_dict(label, result)


def handle_pending_confirmation(client_id: str, session_id: str, session_mem: Dict,
                                 clean_message: str) -> Optional[Dict]:
    """
    Called early in generate_response(), mirroring the existing
    email_capture_pending / handoff_offered state-machine checks.

    Returns a PipelineResult-shaped dict if a pending confirmation was
    resolved (yes/no/unclear) this turn, or None if there's nothing
    pending (caller proceeds with the normal pipeline).
    """
    pending = session_mem.get('pending_integration_action')
    if not pending:
        return None

    msg = clean_message.strip().lower()
    label = pending.get('action_name', 'that').replace('_', ' ')

    if _matches_confirm_set(msg, _CONFIRM_NO, _CONFIRM_NO_PHRASES):
        session_mem['pending_integration_action'] = None
        return {
            'response': "No problem, I won't go ahead with that.",
            'method': 'external_action_declined',
            'clear_pending': True,
        }

    if _matches_confirm_set(msg, _CONFIRM_YES, _CONFIRM_YES_PHRASES):
        session_mem['pending_integration_action'] = None
        result = execute_client_action(
            action_id=pending['action_id'], params=pending.get('params') or {},
            client_id=client_id, session_id=session_id,
        )
        out = _result_to_pipeline_dict(label, result)
        out['clear_pending'] = True
        return out

    # Unclear reply — re-ask once rather than silently dropping the pending action
    return {
        'response': f"Sorry, should I go ahead and {label}? (yes/no)",
        'method': 'external_action_confirm_retry',
        'clear_pending': False,
    }


def _result_to_pipeline_dict(label: str, result: Dict) -> Dict:
    if result.get('success'):
        return {
            'response': f"Done — {label} completed.",
            'method': 'external_action_executed',
            'action': {'status': 'success', **result},
        }
    return {
        'response': "I wasn't able to complete that. Let me connect you with someone who can help.",
        'method': 'external_action_failed',
        'action': {'status': 'failed', **result},
    }
