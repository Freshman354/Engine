"""
pipeline/stages/escalation.py
==============================
Frustration and urgency escalation logic.
Previously: _check_escalation method on AIHelper.

check_escalation() is a pure function — no model calls, no DB.
Returns an escalation response string when the user should be handed off,
or None when the pipeline should continue normally.
"""

import random
from typing import Dict, List, Optional

from constants import BILLING_URGENCY_SIGNALS, FRUSTRATION_SIGNALS
from utils import get_logger

logger = get_logger('lumvi.escalation')

# ── Escalation response pool ──────────────────────────────────────────────────
# Varied so repeat escalation triggers don't sound identical.

_ESCALATION_RESPONSES: List[str] = [
    (
        "I can hear this has been frustrating, and I genuinely want to help get this resolved "
        "properly for you. Rather than keep you going in circles, let me connect you with a "
        "member of the team who can sort this out directly. Shall I arrange that?"
    ),
    (
        "I'm sorry this hasn't been sorted yet — that's not the experience we want for you. "
        "I think it's best I get a real person involved who can look into this properly. "
        "Would that work for you?"
    ),
    (
        "It sounds like you've had a tough time with this, and I don't want to keep giving "
        "you answers that aren't landing. Let me put you through to someone on the team who "
        "can get to the bottom of it. OK?"
    ),
    (
        "I can see this has been going on for too long and I don't want to waste any more "
        "of your time. The right move here is to get you speaking with someone directly. "
        "Can I set that up for you?"
    ),
]

_BILLING_ESCALATION_RESPONSES: List[str] = [
    (
        "I can see there's a billing concern here — this is something I want to make sure "
        "is handled correctly and quickly. Let me connect you with someone on the billing "
        "team who can look into this directly. Does that work?"
    ),
    (
        "Billing issues are something we take seriously, and I want to make sure you get "
        "the right help quickly. I'll arrange for someone from the billing team to pick "
        "this up. Is that OK?"
    ),
]


# ── Public function ───────────────────────────────────────────────────────────

def check_escalation(
    clean_message: str,
    session_mem: Dict,
    vertical: str = 'general',
) -> Optional[str]:
    """
    Check whether this turn should trigger a human escalation.

    Triggers on:
      1. Billing urgency signals (always — regardless of frustration score)
      2. Frustration score ≥ 2 from session memory
      3. Explicit frustration language in current message

    Returns an escalation response string, or None to continue the pipeline.
    """
    msg_lower = clean_message.lower()

    # ── Billing urgency (highest priority) ───────────────────────────
    if any(sig in msg_lower for sig in BILLING_URGENCY_SIGNALS):
        logger.debug(f"[Escalation] billing trigger vertical={vertical}")
        return random.choice(_BILLING_ESCALATION_RESPONSES)

    # ── Cumulative frustration from session ───────────────────────────
    frustration_score = session_mem.get('frustration_score', 0)
    if session_mem.get('is_frustrated') or frustration_score >= 2:
        logger.debug(
            f"[Escalation] frustration trigger "
            f"score={frustration_score} is_frustrated={session_mem.get('is_frustrated')}"
        )
        return random.choice(_ESCALATION_RESPONSES)

    # ── In-message frustration signals ────────────────────────────────
    if sum(1 for sig in FRUSTRATION_SIGNALS if sig in msg_lower) >= 2:
        logger.debug("[Escalation] in-message frustration trigger")
        return random.choice(_ESCALATION_RESPONSES)

    return None
