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
from typing import Any, Dict, List, Optional

from constants import (
    ACTION_KEYWORDS,
    ACTION_LABELS,
    CONTACT_REQUEST_PATTERNS,
    GLOBAL_PRICING_KW,
    PERSONALITIES,
    PROSPECT_INFO_KEYWORDS,
    SIMPLE_INTENTS,
    TOOL_KEYWORDS,
)

from utils import get_logger, log_crash, generate as _gemini_generate

logger = get_logger('lumvi.intent')

# Compiled once at import time — CONTACT_REQUEST_PATTERNS lives in constants.py
# so new phrasing families can be added there without touching this file.
_CONTACT_REQUEST_RE: List[re.Pattern] = [
    re.compile(p, re.IGNORECASE) for p in CONTACT_REQUEST_PATTERNS
]


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

def detect_action_intent(
    clean_message: str,
    lead_triggers: Optional[List[str]] = None,
) -> Optional[str]:
    """
    Keyword scan for explicit user actions (demo, meeting, pricing, contact).
    Also checks agency-defined lead_triggers — matched as contact_request.
    Returns an action key from ACTION_KEYWORDS, or None.
    Pure — no model calls.
    """
    msg = clean_message.lower()
    for action_key, keywords in ACTION_KEYWORDS.items():
        if any(kw in msg for kw in keywords):
            return action_key
    # Regex fallback for human-handoff requests — catches phrasing families
    # ("connect me...", "put me through...", "talk to a human") that a
    # literal keyword list can't keep up with. See CONTACT_REQUEST_PATTERNS
    # in constants.py for the full rationale.
    if any(pattern.search(msg) for pattern in _CONTACT_REQUEST_RE):
        return 'contact_request'
    # Agency-defined lead triggers — treat as contact_request
    if lead_triggers:
        if any(t.lower() in msg for t in lead_triggers):
            return 'contact_request'
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
    Run the five-tier intent pipeline and return a unified intent dict.

    Tier 1:   keyword match — greeting / gratitude / goodbye (free)
    Tier 2A:  action keyword match — explicit booking/demo/contact (free)
    Tier 2B:  tool keyword match — order lookup, appointments, etc. (free)
              NOTE: book_appointment and check_availability also set is_lead=True
              because appointment intent is a high-value signal regardless of
              whether the visitor is a new prospect or existing customer.
    Tier 2.5: prospect informational signal check — setup, onboarding,
              service-scope, new-client, how-it-works questions (free).
              See PROSPECT_INFO_KEYWORDS in constants.py for the full list.
              is_sales stays False intentionally: these are evaluation signals.
    Tier 3:   Gemini classification — anything not caught above (1 Gemini call,
              only when skip_gemini=False and model is not None)

    is_lead priority chain (each layer only fires if the previous didn't set it):
      Tier 2A action → GLOBAL_PRICING_KW → vertical lead_keywords
        → PROSPECT_INFO_KEYWORDS → Tier 3 Gemini

    Keyword-set is_lead=True is preserved through Tier 3: result.update()
    cannot overwrite a True determined by any earlier free-path check.

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

    # ── Tier 1 — simple intent ────────────────────────────────────────────────
    simple = detect_simple_intent(clean_message)
    if simple:
        result['intent']     = simple
        result['confidence'] = 1.0
        return result

    # ── Tier 2A — action ─────────────────────────────────────────────────────
    # lead_triggers checked here so agency keywords work in the fast path
    # without reaching Tier 3.
    action = detect_action_intent(clean_message, lead_triggers)
    if action:
        result['intent']     = 'action'
        result['action']     = action
        result['is_lead']    = True
        result['confidence'] = 0.9
        return result

    # ── Tier 2B — tool ───────────────────────────────────────────────────────
    # book_appointment and check_availability set is_lead=True: a visitor
    # asking to book or check slots is a high-intent signal in any vertical
    # (new patient at a dental practice, new gym member, new client at an
    # agency) and should trigger the email-capture nudge even though it
    # routes to the tool pipeline rather than the action pipeline.
    tool = detect_tool_intent(clean_message)
    if tool:
        result['intent']     = 'tool'
        result['tool']       = tool
        result['confidence'] = 0.9
        if tool in ('book_appointment', 'check_availability'):
            result['is_lead'] = True
        return result

    msg_lower = clean_message.lower()

    # ── Pricing keyword shortcut (cheaper than Gemini Tier 3) ────────────────
    if any(kw in msg_lower for kw in GLOBAL_PRICING_KW):
        result['is_sales'] = True
        result['is_lead']  = True   # pricing enquiry = confirmed lead signal

    # ── Vertical lead keyword check (free — no model call) ───────────────────
    # Catches vertical-specific signals (e.g. 'viewing', 'consultation',
    # 'trial') that aren't in the generic ACTION_KEYWORDS list.
    if not result['is_lead']:
        personality   = PERSONALITIES.get(vertical, PERSONALITIES['general'])
        vert_lead_kws = personality.get('lead_keywords', [])
        if any(kw.lower() in msg_lower for kw in vert_lead_kws):
            result['is_lead']  = True
            result['is_sales'] = True

    # ── Tier 2.5 — prospect informational signal check (free) ────────────────
    # Catches evaluation-stage questions about setup, onboarding, service scope,
    # new-client eligibility, and how-it-works discovery. These don't match
    # ACTION_KEYWORDS (no explicit booking verb) or GLOBAL_PRICING_KW (no price
    # mention), so without this check they fall through to Gemini, which often
    # returns is_lead=False because they look like ordinary informational
    # questions without vertical context about what constitutes a lead signal.
    #
    # Covers both SaaS/agency language ("how does onboarding work") and
    # small-business visitor language ("do you take new patients", "what
    # services do you offer", "i'm looking for a dentist").
    #
    # is_sales stays False: evaluation questions warrant an email nudge, not
    # a pricing CTA. GLOBAL_PRICING_KW handles is_sales independently.
    if not result['is_lead']:
        if any(kw in msg_lower for kw in PROSPECT_INFO_KEYWORDS):
            result['is_lead'] = True
            logger.debug(
                f"[Intent/T2.5] prospect_info signal fired: "
                f"msg_preview={clean_message[:60]!r}"
            )

    # ── Tier 3 — Gemini ───────────────────────────────────────────────────────
    # Preserve any is_lead=True set by the free-path keyword checks above.
    # result.update(gemini_result) would silently overwrite it if Gemini
    # returns is_lead=False for the same message — common for prospect info
    # questions that look like generic questions without vertical context.
    # Keyword signals are definitive; Gemini cannot override them.
    if not skip_gemini and model is not None:
        _is_lead_pre_t3 = result['is_lead']
        gemini_result   = classify_intent_gemini(clean_message, vertical, lead_triggers, model)
        result.update(gemini_result)
        if _is_lead_pre_t3:
            result['is_lead'] = True

    return result