"""
pipeline/stages/intent.py
==========================
Intent detection (3 tiers), action detection, and tool dispatch.
Previously: detect_intent, detect_action_intent, handle_detected_action,
_dispatch_tool, extract_tool_args, _format_tool_response on AIHelper.

Tier 1 (keyword) and Tier 2 (action/tool) are pure functions.
Tier 3 (Gemini classification) takes `model` as an explicit parameter
so this module has no reference to AIHelper.
"""

import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from constants import (
    ACTION_KEYWORDS,
    ACTION_LABELS,
    CONFIRMATION_NO_WORDS,
    CONFIRMATION_YES_WORDS,
    GLOBAL_PRICING_KW,
    PERSONALITIES,
    PROSPECT_INFO_KEYWORDS,
    SIMPLE_INTENTS,
    TOOL_KEYWORDS,
    WRITE_TOOLS,
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

    FIX: this used to return the first tool (in TOOL_KEYWORDS dict
    insertion order) with ANY matching keyword. lookup_order's generic
    'my order' is a substring of cancel_order's own 'cancel my order' —
    and since lookup_order is checked first, "cancel my order ORD-12345"
    matched lookup_order before cancel_order was ever considered,
    misrouting every natural cancellation phrasing into a lookup. Now
    every tool is scanned and the LONGEST matching keyword wins, so a
    more specific phrase (always the longer match) beats a generic one
    regardless of dict order.
    """
    msg = clean_message.lower()
    best_tool: Optional[str] = None
    best_len = 0
    for tool_key, keywords in TOOL_KEYWORDS.items():
        for kw in keywords:
            if kw in msg and len(kw) > best_len:
                best_tool = tool_key
                best_len = len(kw)
    return best_tool


def dispatch_tool(
    tool_name: str,
    user_message: str,
    client_id: Optional[str],
    session_mem: Dict,
    override_args: Optional[Dict] = None,
    session_id: Optional[str] = None,
) -> Dict:
    """
    Route to the correct tool function from tools.py.
    Returns a standardised tool result dict.

    override_args: when set, used instead of re-extracting from
    user_message. Needed for write-tool confirmations — the confirming
    message is just "yes", which contains none of the original order_id /
    slot_id / etc., so the args captured when the action was first
    proposed must be replayed here instead of re-extracted.

    session_id: needed for escalate_to_human, which requires it.
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
        args = (
            override_args if override_args is not None
            else extract_tool_args(tool_name, user_message, session_mem, session_id=session_id)
        )
        result = tool_fn(client_id=client_id, **args)
        return _format_tool_response(tool_name, result)
    except Exception as e:
        log_crash(logger, 'Tool/dispatch', e, tool=tool_name, client_id=client_id)
        return {
            'success': False,
            'error': "Something went wrong with that request. Please try again.",
        }


def extract_tool_args(
    tool_name: str,
    user_message: str,
    session_mem: Dict,
    session_id: Optional[str] = None,
) -> Dict:
    """
    Extract named arguments from user_message + session_mem for a given tool.
    Conservative — only extracts what the tool needs.
    """
    args: Dict = {}

    if tool_name in ('lookup_order', 'cancel_order'):
        # Try to find an order/reference number
        order_match = re.search(r'\b([A-Z]{2,4}[-–]?\d{4,10})\b', user_message, re.IGNORECASE)
        if order_match:
            args['order_id'] = order_match.group(1)
        elif session_mem.get('order_id'):
            args['order_id'] = session_mem['order_id']

        # FIX: cancel_order() in tools.py requires customer_email — it's a
        # required arg with no default, used as an ownership check. This
        # was never extracted, so every cancel_order dispatch raised a
        # TypeError and silently failed with a generic error message
        # instead of ever actually cancelling anything.
        if tool_name == 'cancel_order' and session_mem.get('email'):
            args['customer_email'] = session_mem['email']

    elif tool_name == 'check_availability':
        # FIX: was passing email/name/phone — check_availability(client_id,
        # date="", service_type="") doesn't accept any of those, so this
        # guaranteed a TypeError the moment session_mem had contact info on
        # file (i.e. broke for exactly the customers we already knew).
        # Neither date nor service_type is reliably extractable from free
        # text yet, so this intentionally leaves args empty for now — the
        # tool's own defaults (all slots, any service) apply.
        pass

    elif tool_name == 'book_appointment':
        # FIX: was writing 'email'/'name'/'phone' — book_appointment()
        # actually takes customer_email/customer_name/customer_phone, so
        # every call TypeError'd regardless of session state. Also: slot_id
        # is required with no default and was never populated at all (see
        # the missing_required_args gate below, which now catches this and
        # asks the user to pick a slot instead of silently failing).
        if session_mem.get('email'):
            args['customer_email'] = session_mem['email']
        if session_mem.get('name'):
            args['customer_name'] = session_mem['name']
        if session_mem.get('phone'):
            args['customer_phone'] = session_mem['phone']
        if session_mem.get('pending_slot_id'):
            args['slot_id'] = session_mem['pending_slot_id']

    elif tool_name == 'escalate_to_human':
        # FIX: tools.escalate_to_human()'s real signature is
        # (client_id, session_id, reason, customer_email="", customer_name="",
        # summary="", urgency="normal") — this was building a 'session_data'
        # dict that doesn't match any parameter of that function at all,
        # and never provided the required `session_id` either. Both would
        # raise a TypeError on every dispatch, regardless of session state.
        args['session_id']     = session_id or ''
        args['reason']         = user_message[:200]
        args['customer_email'] = session_mem.get('email', '')
        args['customer_name']  = session_mem.get('name', '')
        args['urgency']        = session_mem.get('urgency', 'normal')

    elif tool_name == 'search_knowledge_base':
        # FIX: had no case at all — query is a required arg with no
        # default, so every search_knowledge_base dispatch TypeError'd
        # before this was added.
        args['query'] = user_message[:500]

    elif tool_name == 'search_products':
        # Same pattern as search_knowledge_base above — query is a
        # required arg with no default. tools.search_products truncates
        # to 300 chars itself; capped a bit tighter here since product
        # search queries are short phrases, not full questions.
        args['query'] = user_message[:300]

    return args


def _format_tool_response(tool_name: str, raw_result: Any) -> Dict:
    """
    Normalise a raw tool result into { success, response, data }.

    FIX: this used to be `raw_result.get('message', 'Done!')` — but only
    cancel_order and escalate_to_human actually return a 'message' key.
    lookup_order returns a structured 'order' dict, check_availability
    returns a 'slots' list, book_appointment uses 'confirmation_message'
    (a different key), and search_knowledge_base returns a 'results'
    list. None of those have a 'message' key, so all four surfaced a bare
    "Done!" to the user on success while the actual data sat unused in
    'data'. Each tool's real shape is now formatted into readable text.
    """
    if not isinstance(raw_result, dict):
        return {'success': True, 'response': str(raw_result), 'data': raw_result}

    if 'error' in raw_result:
        return {'success': False, 'error': raw_result['error']}

    return {
        'success':  True,
        'response': _build_tool_response_text(tool_name, raw_result),
        'data':     raw_result,
    }


def _build_tool_response_text(tool_name: str, result: Dict) -> str:
    """Turn a tool's structured success payload into a natural-language reply."""
    if tool_name == 'lookup_order':
        order = result.get('order', {})
        parts = [
            f"Order {order.get('id', '')} is currently '{order.get('status', 'unknown')}'."
        ]
        if order.get('total_amount') is not None:
            parts.append(f"Total: {order['total_amount']} {order.get('currency', 'USD')}.")
        if order.get('updated_at'):
            parts.append(f"Last updated {order['updated_at']}.")
        return ' '.join(parts)

    if tool_name == 'check_availability':
        # A client on Acuity/Calendly/Square gets redirected to their real
        # booking page instead of a slots list — see
        # get_external_booking_info() in tools.py. That response has no
        # 'slots' key at all, so it falls straight through to 'message'.
        slots = result.get('slots', [])
        if not slots:
            return result.get(
                'message',
                f"No available slots found for {result.get('date', 'that date')}. "
                f"Try a different date."
            )
        lines = [f"Here's what's open on {result.get('date', '')}:"]
        for i, s in enumerate(slots[:10], start=1):
            lines.append(
                f"{i}. {s.get('datetime', '')} — {s.get('service_type', 'general')}"
            )
        lines.append("Let me know which one works (e.g. \"the 2nd one\" or the time) and I can book it.")
        return '\n'.join(lines)

    if tool_name == 'book_appointment':
        return result.get(
            'confirmation_message',
            f"Your appointment has been booked (ref: {result.get('booking_id', '')})."
        )

    if tool_name == 'search_knowledge_base':
        results = result.get('results', [])
        if not results:
            return "I couldn't find anything matching that in our knowledge base."
        return results[0].get('answer', '') or "I found something but couldn't format it — let me try again."

    if tool_name == 'search_products':
        products = result.get('products', [])
        if not products:
            return result.get('message', "I couldn't find anything matching that.")
        lines = ["Here's what I found:"]
        for p in products[:5]:
            status = 'in stock' if p.get('available') else 'out of stock'
            variant_part = f" ({p['variant']})" if p.get('variant') else ''
            price_part = f" — {p['price']}" if p.get('price') else ''
            lines.append(f"• {p.get('title', '')}{variant_part}{price_part} — {status}")
        return '\n'.join(lines)

    # cancel_order, escalate_to_human, and anything else already using 'message'
    return result.get('message', 'Done!')


# ── Write-tool confirmation gate ──────────────────────────────────────────────
# cancel_order and book_appointment mutate real state (an actual order, an
# actual calendar slot) and must never fire on a single keyword match. The
# flow is: propose (this turn) → confirm (a later turn) → dispatch. See
# WRITE_TOOLS / CONFIRMATION_YES_WORDS / CONFIRMATION_NO_WORDS in
# constants.py and the orchestration in ai_helper.py's tool-dispatch block.

_REQUIRED_TOOL_ARGS: Dict[str, List[str]] = {
    'cancel_order':     ['order_id', 'customer_email'],
    'book_appointment': ['slot_id', 'customer_name', 'customer_email'],
}

_MISSING_ARG_PHRASES: Dict[str, str] = {
    'order_id':       'your order number',
    'customer_email': 'the email address on the order',
    'customer_name':  'your name',
    'slot_id':        "which time slot you'd like — I can check availability first if that helps",
}


def resolve_slot_selection(clean_message: str, available_slots: List[Dict]) -> Optional[str]:
    """
    Resolve a natural reply ("the 2nd one", "option 2", "2pm") against the
    slot list shown by check_availability (stored in
    session_mem['available_slots'], numbered 1..N in the order shown).
    Returns the matched slot_id, or None if nothing matches.

    This is what actually lets book_appointment complete — without it,
    there's no way to turn "the 2pm one" into the slot_id book_appointment
    requires, and every booking attempt dead-ends asking which slot,
    forever.
    """
    if not available_slots:
        return None
    msg = clean_message.lower().strip()

    # Position-based: "first"/"1st"/"the second one"/bare "2"/"option 2"
    ordinal_words = {
        'first': 1, '1st': 1, 'second': 2, '2nd': 2, 'third': 3, '3rd': 3,
        'fourth': 4, '4th': 4, 'fifth': 5, '5th': 5,
    }
    position: Optional[int] = None
    for word, n in ordinal_words.items():
        if word in msg:
            position = n
            break
    if position is None and len(msg.split()) <= 6:
        num_match = re.search(r'\b(\d{1,2})\b', msg)
        if num_match:
            position = int(num_match.group(1))
    if position and 1 <= position <= len(available_slots):
        slot_id = available_slots[position - 1].get('slot_id')
        if slot_id:
            return str(slot_id)

    # Time-of-day based: "2pm", "2:30pm", "14:00" matched against each
    # slot's stored datetime.
    time_match = re.search(r'\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\b', msg)
    if time_match:
        hour = int(time_match.group(1))
        ampm = time_match.group(3)
        if ampm == 'pm' and hour != 12:
            hour += 12
        elif ampm == 'am' and hour == 12:
            hour = 0
        if 0 <= hour <= 23:
            for slot in available_slots:
                dt_str = str(slot.get('datetime', ''))
                try:
                    slot_dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
                    if slot_dt.hour == hour:
                        slot_id = slot.get('slot_id')
                        if slot_id:
                            return str(slot_id)
                except Exception:
                    continue

    return None


def booking_redirect_message(client_id: Optional[str]) -> Optional[str]:
    """
    If this client has a real booking page configured for Acuity/Calendly/
    Square, return the message pointing the customer there. Returns None
    otherwise (no such integration, or one exists with no booking_url set).

    Checked before proposing a book_appointment confirmation — without
    this, a client on one of these platforms would never have
    session_mem['available_slots'] populated (check_availability redirects
    instead of listing slots for them), so slot_id could never be
    resolved and the bot would ask "which slot?" forever with no way for
    the user to ever answer it.
    """
    if not client_id:
        return None
    try:
        import tools as _t
        info = _t.get_external_booking_info(client_id)
    except Exception:
        return None
    if not info:
        return None
    return f"You can book directly here: {info['booking_url']}"


def order_cancellation_redirect_message(client_id: Optional[str]) -> Optional[str]:
    """
    If this client has a real self-service order management page
    configured, return the message pointing the customer there. Returns
    None otherwise.

    Checked before proposing a cancel_order confirmation — cancellation
    and refunds are redirected to the client's real store rather than
    executed by the chatbot, the same design as book_appointment's
    redirect for Acuity/Calendly/Square. Without this check, cancel_order
    would go through the full propose/confirm/dispatch dance for an
    outcome that's just a link — no actual mutation happens in that case,
    so there's nothing to confirm.
    """
    if not client_id:
        return None
    try:
        import tools as _t
        url = _t.get_order_management_url(client_id)
    except Exception:
        return None
    if not url:
        return None
    return f"You can cancel your order and see refund options directly here: {url}"


def is_write_tool(tool_name: str) -> bool:
    """True for tools that must go through the confirm-then-dispatch gate."""
    return tool_name in WRITE_TOOLS


def missing_required_args(tool_name: str, args: Dict) -> List[str]:
    """Which required args for this tool are still unfilled."""
    return [a for a in _REQUIRED_TOOL_ARGS.get(tool_name, []) if not args.get(a)]


def build_missing_args_prompt(tool_name: str, missing: List[str]) -> str:
    """A natural-language ask for whatever's still missing before we can propose the action."""
    phrases = [_MISSING_ARG_PHRASES.get(m, m) for m in missing]
    if len(phrases) == 1:
        needed = phrases[0]
    else:
        needed = ', '.join(phrases[:-1]) + f' and {phrases[-1]}'
    return f"I can help with that — could you let me know {needed}?"


def build_confirmation_prompt(tool_name: str, args: Dict) -> str:
    """The explicit confirmation ask shown before a write tool actually dispatches."""
    # FIX: wording updated to match tools.py — cancel_order/book_appointment
    # now submit a REQUEST (staff confirm it against the real Shopify/Acuity/
    # Calendly/etc. account) rather than completing the action outright, since
    # neither tool makes an outbound call to the client's actual platform.
    # "This can't be undone" was accurate when confirming meant an immediate,
    # final cancellation; it no longer is.
    if tool_name == 'cancel_order':
        return (
            f"Just to confirm — you'd like to request cancellation of order "
            f"{args.get('order_id', '')}? I'll submit this for our team to "
            f"confirm. Reply \"yes\" to confirm, or \"no\" to leave it as is."
        )
    if tool_name == 'book_appointment':
        return (
            f"Just to confirm — request this slot for {args.get('customer_name', 'you')} "
            f"({args.get('customer_email', '')})? Our team will confirm it's "
            f"available before it's final. Reply \"yes\" to confirm."
        )
    return 'Shall I go ahead with that? Reply "yes" to confirm.'


def detect_confirmation(clean_message: str) -> Optional[bool]:
    """
    Detect whether a message is confirming or declining a PENDING write-tool
    action. Only meaningful when the caller already knows a pending action
    exists — a bare "yes" means nothing on its own. Returns True/False/None
    (None = not a yes/no reply at all, e.g. an unrelated new question).
    """
    msg = clean_message.lower().strip().strip('.!?')
    if not msg:
        return None
    if any(msg == w or msg.startswith(w + ' ') or msg.endswith(' ' + w) for w in CONFIRMATION_NO_WORDS):
        return False
    if any(msg == w or msg.startswith(w + ' ') or msg.endswith(' ' + w) for w in CONFIRMATION_YES_WORDS):
        return True
    return None


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
        'intent':      'question',
        'is_sales':    False,
        'is_lead':     False,
        'action':      None,
        'tool':        None,
        'confidence':  0.5,
        # FIX: new, additive key — lets the caller know whether Tier 3 spent
        # a real Gemini call this turn, so it can charge that against the
        # same shared budget as query-rewrite/cross-encoder rerank (see
        # ai_helper.py generate_response and PipelineRequest.call1_used).
        'gemini_used': False,
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
    #
    # FIX: previously Tier 3 ran unconditionally whenever a model was
    # available, even when the free keyword tiers had already resolved
    # BOTH is_lead and is_sales (e.g. a pricing-keyword hit). In that case
    # the only thing Tier 3 could still contribute is a generic intent
    # label, which isn't worth a Gemini call on every message — this adds
    # up fast across many client bots. We only call Tier 3 when at least
    # one of is_lead/is_sales is still undetermined.
    if not skip_gemini and model is not None:
        _is_lead_pre_t3  = result['is_lead']
        _is_sales_pre_t3 = result['is_sales']
        if _is_lead_pre_t3 and _is_sales_pre_t3:
            logger.debug(
                "[Intent/T3] skipped — is_lead and is_sales already "
                "resolved by free-path keyword tiers"
            )
        else:
            gemini_result = classify_intent_gemini(clean_message, vertical, lead_triggers, model)
            result.update(gemini_result)
            result['gemini_used'] = True
            if _is_lead_pre_t3:
                result['is_lead'] = True

    return result
