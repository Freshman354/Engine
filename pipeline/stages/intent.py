"""
pipeline/stages/intent.py
==========================
Intent detection (3 tiers), action detection, and tool dispatch.
Previously: detect_intent, detect_action_intent, handle_detected_action,
_dispatch_tool, _extract_tool_args, _format_tool_response on AIHelper.

Tier 1 (keyword) and Tier 2 (action/tool) are pure functions.
Tier 3 (Gemini classification) takes `model` as an explicit parameter
so this module has no reference to AIHelper.
"""

import json
import re
from typing import Any, Dict, List, Optional, Tuple

from constants import (
    ACTION_KEYWORDS,
    ACTION_LABELS,
    GLOBAL_PRICING_KW,
    PERSONALITIES,
    SIMPLE_INTENTS,
    TOOL_KEYWORDS,
)
from utils import get_logger, log_crash, generate as _gemini_generate

logger = get_logger('lumvi.intent')


# ── Tier 1 — Keyword intent ───────────────────────────────────────────────────

def detect_simple_intent(clean_message: str) -> Optional[str]:
    """
    Zero-cost keyword check for greeting / gratitude / goodbye.
    Returns the intent name string, or None.
    Called before any embedding or model work.
    """
    msg = clean_message.lower().strip()
    for intent, keywords in SIMPLE_INTENTS.items():
        if any(kw in msg for kw in keywords):
            return intent
    return None


# ── Tier 2A — Action detection ────────────────────────────────────────────────

def detect_action_intent(clean_message: str) -> Optional[str]:
    """
    Keyword scan for explicit user actions (demo, meeting, pricing, contact).
    Returns an action key from ACTION_KEYWORDS, or None.
    Pure — no model calls.
    """
    msg = clean_message.lower()
    for action_key, keywords in ACTION_KEYWORDS.items():
        if any(kw in msg for kw in keywords):
            return action_key
    return None


def handle_detected_action(action_key: str, vertical: str = 'general') -> Dict:
    """
    Build the action payload returned to the frontend when an explicit
    action is detected. The widget uses this to show a booking/pricing CTA.
    """
    label = ACTION_LABELS.get(action_key, action_key.replace('_', ' '))
    personality = PERSONALITIES.get(vertical, PERSONALITIES['general'])

    return {
        'action':    action_key,
        'label':     label,
        'response':  (
            f"I'd be happy to help you with that! "
            f"Let me put you in touch with the right person to arrange your {label}."
        ),
        'tone':      personality.get('tone', ''),
        'is_action': True,
    }


# ── Tier 2B — Tool detection ──────────────────────────────────────────────────

def detect_tool_intent(clean_message: str) -> Optional[str]:
    """
    Keyword scan for transactional tool calls (order lookup, booking, etc.).
    Returns a tool key from TOOL_KEYWORDS, or None.
    Pure — no model calls.
    """
    msg = clean_message.lower()
    for tool_key, keywords in TOOL_KEYWORDS.items():
        if any(kw in msg for kw in keywords):
            return tool_key
    return None


def dispatch_tool(
    tool_name: str,
    user_message: str,
    client_id: Optional[str],
    session_mem: Dict,
) -> Dict:
    """
    Route to the correct tool function from tools.py.
    Returns a standardised tool result dict.
    """
    try:
        import tools as _t
        tool_fn = getattr(_t, tool_name, None)
        if tool_fn is None:
            logger.warning(f"[Tool] No handler for tool={tool_name}")
            return {
                'success': False,
                'error': f"Tool '{tool_name}' is not available.",
            }
        args = _extract_tool_args(tool_name, user_message, session_mem)
        result = tool_fn(client_id=client_id, **args)
        return _format_tool_response(tool_name, result)
    except Exception as e:
        log_crash(logger, 'Tool/dispatch', e, tool=tool_name, client_id=client_id)
        return {
            'success': False,
            'error': "Something went wrong with that request. Please try again.",
        }


def _extract_tool_args(
    tool_name: str,
    user_message: str,
    session_mem: Dict,
) -> Dict:
    """
    Extract named arguments from user_message + session_mem for a given tool.
    Conservative — only extracts what the tool needs.
    """
    args: Dict = {}

    if tool_name in ('lookup_order', 'cancel_order'):
        # Try to find an order/reference number
        order_match = re.search(r'\b([A-Z]{2,4}[-–]?\d{4,10})\b', user_message)
        if order_match:
            args['order_id'] = order_match.group(1)
        elif session_mem.get('order_id'):
            args['order_id'] = session_mem['order_id']

    elif tool_name in ('book_appointment', 'check_availability'):
        # Pass session contact info if we have it
        if session_mem.get('email'):
            args['email'] = session_mem['email']
        if session_mem.get('name'):
            args['name'] = session_mem['name']
        if session_mem.get('phone'):
            args['phone'] = session_mem['phone']

    elif tool_name == 'escalate_to_human':
        args['reason'] = user_message[:200]
        args['session_data'] = {
            k: v for k, v in session_mem.items()
            if k in ('name', 'email', 'phone', 'purchase_stage')
        }

    return args


def _format_tool_response(tool_name: str, raw_result: Any) -> Dict:
    """
    Normalise a raw tool result into { success, response, data }.
    """
    if isinstance(raw_result, dict):
        if 'error' in raw_result:
            return {'success': False, 'error': raw_result['error']}
        return {
            'success':  True,
            'response': raw_result.get('message', 'Done!'),
            'data':     raw_result,
        }
    return {
        'success':  True,
        'response': str(raw_result),
        'data':     raw_result,
    }


# ── Tier 3 — Gemini intent classification ────────────────────────────────────

def classify_intent_gemini(
    clean_message: str,
    vertical: str,
    lead_triggers: List[str],
    model: Any,
) -> Dict:
    """
    Use Gemini to classify intent when Tier 1 and Tier 2 return nothing.
    Returns a dict with keys: intent, is_sales, is_lead, confidence.

    model: a google.genai Client().models object (from AIHelper).
    Only called when BOTH Tier 1 and Tier 2 fail — keep expensive.
    """
    personality = PERSONALITIES.get(vertical, PERSONALITIES['general'])
    lead_kws    = personality.get('lead_keywords', []) + (lead_triggers or [])

    prompt = (
        f"Classify this customer message for a {vertical} business.\n\n"
        f"Message: \"{clean_message}\"\n\n"
        f"Lead trigger keywords for this business: {', '.join(lead_kws[:20])}\n\n"
        "Respond ONLY with valid JSON (no markdown, no explanation):\n"
        '{"intent": "question|complaint|greeting|goodbye|gratitude|other", '
        '"is_sales": true|false, '
        '"is_lead": true|false, '
        '"confidence": 0.0-1.0}'
    )
    try:
        resp = _gemini_generate(model, prompt)
        text = (resp.text or '').strip().strip('`')
        if text.startswith('json'):
            text = text[4:].strip()
        parsed = json.loads(text)
        parsed.setdefault('intent',     'question')
        parsed.setdefault('is_sales',   False)
        parsed.setdefault('is_lead',    False)
        parsed.setdefault('confidence', 0.5)
        logger.debug(
            f"[Intent/Gemini] intent={parsed['intent']} "
            f"is_lead={parsed['is_lead']} conf={parsed['confidence']}"
        )
        return parsed
    except Exception as e:
        log_crash(logger, 'Intent/Gemini', e, msg_preview=clean_message[:60])
        return {'intent': 'question', 'is_sales': False, 'is_lead': False, 'confidence': 0.3}


# ── Combined intent runner ────────────────────────────────────────────────────

def detect_intent(
    clean_message: str,
    vertical: str,
    lead_triggers: List[str],
    model: Any,
    skip_gemini: bool = False,
) -> Dict:
    """
    Run the three-tier intent pipeline and return a unified intent dict.

    Tier 1: keyword match (free)
    Tier 2: action/tool match (free)
    Tier 3: Gemini classification (1 Gemini call — only when skip_gemini=False)

    Returns dict with: intent, is_sales, is_lead, action, tool, confidence
    """
    result: Dict = {
        'intent':     'question',
        'is_sales':   False,
        'is_lead':    False,
        'action':     None,
        'tool':       None,
        'confidence': 0.5,
    }

    # Tier 1 — simple intent
    simple = detect_simple_intent(clean_message)
    if simple:
        result['intent']     = simple
        result['confidence'] = 1.0
        return result

    # Tier 2A — action
    action = detect_action_intent(clean_message)
    if action:
        result['intent']     = 'action'
        result['action']     = action
        result['is_lead']    = True
        result['confidence'] = 0.9
        return result

    # Tier 2B — tool
    tool = detect_tool_intent(clean_message)
    if tool:
        result['intent']     = 'tool'
        result['tool']       = tool
        result['confidence'] = 0.9
        return result

    # Pricing keyword shortcut (cheaper than Gemini Tier 3)
    msg_lower = clean_message.lower()
    if any(kw in msg_lower for kw in GLOBAL_PRICING_KW):
        result['is_sales'] = True

    # Tier 3 — Gemini
    if not skip_gemini and model is not None:
        gemini_result = classify_intent_gemini(clean_message, vertical, lead_triggers, model)
        result.update(gemini_result)

    return result
