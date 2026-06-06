"""
services/session_store.py
=========================
Session memory extraction, DB persistence, load, and clear.
Previously: extract_session_memory (standalone), _persist_session,
load_chat_session, clear_chat_session on AIHelper.

All functions are standalone. ai_helper.py re-exports them:
  from services.session_store import (
      extract_session_memory, load_chat_session, clear_chat_session,
  )
"""

import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from constants import (
    STAGE_SIGNALS,
    FRUSTRATION_SIGNALS,
    IDK_METHODS_ALL,
)
from utils import get_logger, log_crash

logger = get_logger('lumvi.session_store')

# Signals that suggest the user declined a human handoff last turn.
_DECLINE_OVERLAP = frozenset([
    'no thanks', "no thank you", 'not now', 'no need',
    "don't need", "no human", 'just the bot', 'keep going',
    "i'm fine", 'im fine',
])


# ── Session memory extraction ─────────────────────────────────────────────────

def extract_session_memory(
    conversation_history: List[Dict],
    current_message: str = '',
) -> Dict:
    """
    Scan conversation history (+ current message) for:
      - Contact info  (name, email, phone)
      - Purchase stage
      - Frustration score and flag
      - Repeated-question detection
      - Turn count
      - Handoff offer flag

    Returns a flat dict of session facts. Merges gracefully with an
    existing session dict loaded from the DB — callers merge with:
        mem = {**db_session, **extract_session_memory(history, msg)}
    """
    mem: Dict = {
        'name':            None,
        'email':           None,
        'phone':           None,
        'purchase_stage':  None,
        'frustration_score': 0,
        'is_frustrated':   False,
        'repeated_question': False,
        'turns':           [],
        'turn_count':      0,
        'handoff_offered': False,
        'last_method':     None,
        'last_question':   None,
    }

    all_msgs: List[str] = []
    user_msgs: List[str] = []

    for turn in conversation_history:
        role = turn.get('role', '')
        content = str(turn.get('content', '')).strip()
        if role == 'user':
            user_msgs.append(content)
            all_msgs.append(content)
            mem['turn_count'] += 1
        elif role == 'assistant':
            all_msgs.append(content)
            # detect last IDK method from bot text markers
            if '<!-- method:' in content:
                m = re.search(r'<!-- method:(\w+) -->', content)
                if m:
                    mem['last_method'] = m.group(1)

    if current_message.strip():
        user_msgs.append(current_message.strip())

    full_text = ' '.join(all_msgs).lower()
    cur_lower = current_message.lower()

    # ── Contact extraction ────────────────────────────────────────────
    email_pat = re.compile(r'\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b')
    phone_pat = re.compile(r'\b(?:\+?\d[\d\s\-().]{6,14}\d)\b')
    name_pat  = re.compile(
        r"(?:my name is|i'?m|i am|call me)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)",
        re.IGNORECASE,
    )

    for msg in user_msgs:
        if not mem['email']:
            em = email_pat.search(msg)
            if em:
                mem['email'] = em.group(0)
        if not mem['phone']:
            ph = phone_pat.search(msg)
            if ph:
                mem['phone'] = ph.group(0)
        if not mem['name']:
            nm = name_pat.search(msg)
            if nm:
                mem['name'] = nm.group(1).strip()

    # ── Purchase stage ────────────────────────────────────────────────
    for stage, signals in STAGE_SIGNALS.items():
        if any(s in full_text for s in signals):
            mem['purchase_stage'] = stage

    # ── Frustration scoring ───────────────────────────────────────────
    score = sum(1 for sig in FRUSTRATION_SIGNALS if sig in cur_lower)

    # Escalate score if last bot method was IDK
    last_method = mem.get('last_method') or ''
    if last_method in IDK_METHODS_ALL:
        score += 1

    mem['frustration_score'] = score
    mem['is_frustrated'] = score >= 2

    # ── Repeated question ─────────────────────────────────────────────
    if len(user_msgs) >= 3:
        prev_msgs = [m.lower().strip() for m in user_msgs[:-1]]
        mem['repeated_question'] = cur_lower.strip() in prev_msgs

    # ── Handoff offered detection ─────────────────────────────────────
    # Check bot turns for known handoff phrases
    for turn in conversation_history:
        if turn.get('role') == 'assistant':
            txt = str(turn.get('content', '')).lower()
            if any(p in txt for p in [
                'connect you with', 'speak with a', 'pass you to',
                'transfer to', 'would you like me to', 'put you in touch',
            ]):
                mem['handoff_offered'] = True

    # Check if user declined a handoff
    if any(d in cur_lower for d in _DECLINE_OVERLAP):
        mem['handoff_offered'] = False  # reset — they declined, try fresh

    # ── Last question ─────────────────────────────────────────────────
    mem['last_question'] = current_message.strip() or (user_msgs[-1] if user_msgs else '')
    mem['turns'] = user_msgs[-10:]  # keep last 10 for context

    return mem


# ── DB persistence ────────────────────────────────────────────────────────────

def persist_session(
    client_id: str,
    session_id: str,
    session_mem: Dict,
) -> None:
    """
    Upsert session memory to the ChatSession table.
    Called as a background task — must not block the pipeline.
    """
    if not client_id or not session_id:
        return
    try:
        import json, models as _m
        existing = _m.ChatSession.query.filter_by(
            client_id=client_id, session_id=session_id
        ).first()

        payload = {
            k: v for k, v in session_mem.items()
            if k not in ('turns',)  # don't persist full turn list
        }

        if existing:
            existing.data     = json.dumps(payload)
            existing.updated  = datetime.utcnow()
        else:
            rec = _m.ChatSession(
                client_id=client_id,
                session_id=session_id,
                data=json.dumps(payload),
                created=datetime.utcnow(),
                updated=datetime.utcnow(),
            )
            _m.db.session.add(rec)

        _m.db.session.commit()
    except Exception as e:
        log_crash(logger, 'SessionStore/persist', e,
                  client_id=client_id, session_id=session_id)


def load_chat_session(
    client_id: str,
    session_id: str,
) -> Dict:
    """Load persisted session memory from DB. Returns {} on miss or error."""
    if not client_id or not session_id:
        return {}
    try:
        import json, models as _m
        rec = _m.ChatSession.query.filter_by(
            client_id=client_id, session_id=session_id
        ).first()
        if rec and rec.data:
            return json.loads(rec.data)
        return {}
    except Exception as e:
        log_crash(logger, 'SessionStore/load', e,
                  client_id=client_id, session_id=session_id)
        return {}


def clear_chat_session(
    client_id: str,
    session_id: str,
) -> bool:
    """Delete a session record. Returns True on success."""
    if not client_id or not session_id:
        return False
    try:
        import models as _m
        rec = _m.ChatSession.query.filter_by(
            client_id=client_id, session_id=session_id
        ).first()
        if rec:
            _m.db.session.delete(rec)
            _m.db.session.commit()
        return True
    except Exception as e:
        log_crash(logger, 'SessionStore/clear', e,
                  client_id=client_id, session_id=session_id)
        return False
