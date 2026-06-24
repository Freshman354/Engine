"""
services/kb_gap.py
==================
KB gap recording, poor-answer tracking, and the weekly digest email.
Previously: module-level and AIHelper methods scattered across ai_helper.py.

All functions are standalone — no AIHelper dependency.
ai_helper.py re-exports these at module level for backward compatibility:
  from services.kb_gap import (
      record_kb_gap, get_top_kb_gaps,
      record_poor_answer, get_poor_answers,
      send_kb_gap_digest,
      add_gap_to_kb,
  )
"""

import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from utils import get_logger, log_crash

logger = get_logger('lumvi.kb_gap')


# ── Noise filter ──────────────────────────────────────────────────────────────

# Single-word greetings, filler words, and common non-questions that provide
# zero value as KB gaps.  All comparisons are lower-cased.
_NOISE_WORDS: frozenset = frozenset({
    # greetings / closings
    'hi', 'hey', 'hello', 'hiya', 'yo', 'sup', 'bye', 'goodbye',
    'ciao', 'later', 'ttyl', 'cheers',
    # affirmatives / negatives
    'yes', 'no', 'nope', 'yep', 'yeah', 'yup', 'nah', 'ok', 'okay',
    'sure', 'fine', 'alright', 'alrite',
    # filler
    'thanks', 'thank', 'thx', 'ty', 'please', 'pls', 'plz', 'k',
    'lol', 'lmao', 'haha', 'hmm', 'uh', 'um', 'err',
    # punctuation-only will be caught by the length check below
})

_MIN_WORDS      = 3    # fewer than 3 words → noise
_MIN_CHARS      = 10   # shorter than 10 chars → noise


def _is_noise(question: str) -> bool:
    """
    Return True if the question is too short or trivially non-informative
    to be worth recording as a KB gap.

    Rules (any one match → noise):
      1. Blank or under _MIN_CHARS characters after stripping punctuation/spaces.
      2. Fewer than _MIN_WORDS words.
      3. Entire question (lowercased, stripped) is a single known noise word.
    """
    if not question:
        return True

    stripped = question.strip()

    # Rule 1 — character length
    if len(stripped) < _MIN_CHARS:
        return True

    # Rule 2 — word count
    words = stripped.split()
    if len(words) < _MIN_WORDS:
        # Allow short phrases only if they're clearly a real question
        # e.g. "pricing?" — two words but not in noise set; still reject
        # because it's below the minimum word threshold.
        return True

    # Rule 3 — entire text is a known noise word (catches "No." / "Hey!" etc.)
    clean = re.sub(r'[^\w\s]', '', stripped).strip().lower()
    if clean in _NOISE_WORDS:
        return True

    return False


# ── Gap recording ─────────────────────────────────────────────────────────────

def record_kb_gap(
    client_id: str,
    question: str,
    method: str,
    confidence: float,
    session_id: Optional[str] = None,
) -> None:
    """
    Persist a question the bot couldn't answer via models.record_kb_gap().
    Called as a background task — never blocks the response pipeline.
    Silently skips when client_id is missing (development/testing) or when
    the question is noise (single words, greetings, filler).

    Note: session_id is accepted for call-site compatibility, but the
    kb_gaps table has no session_id column, so it isn't persisted.
    """
    if not client_id or not question:
        return
    if _is_noise(question):
        logger.debug(f"[KbGap] noise filtered client={client_id} q='{question[:60]}'")
        return
    try:
        import models as _m
        _m.record_kb_gap(
            client_id, question.strip()[:500], method, round(confidence, 4)
        )
        logger.debug(f"[KbGap] recorded client={client_id} q='{question[:60]}'")
    except Exception as e:
        log_crash(logger, 'KbGap/record', e, client_id=client_id)


def get_top_kb_gaps(client_id: str, limit: int = 20) -> List[Dict]:
    """
    Return the most-asked unanswered ('open') questions for a client, via
    models.get_kb_gaps() — used to surface the AI Suggestions panel.

    Applies the noise filter as a retrieval-time safety net so that any
    junk already stored in the DB is scrubbed before it reaches the UI.
    We fetch extra rows to compensate for anything filtered out.
    """
    if not client_id:
        return []
    try:
        import models as _m
        # Fetch more than needed so filtering doesn't leave us short
        raw = _m.get_kb_gaps(client_id, limit=limit * 2)
        filtered = [g for g in raw if not _is_noise(g.get('question', ''))]
        return filtered[:limit]
    except Exception as e:
        log_crash(logger, 'KbGap/get_top', e, client_id=client_id)
        return []


# ── Add gap to KB (question + answer) ────────────────────────────────────────

def add_gap_to_kb(
    client_id: str,
    gap_id: int,
    question: str,
    answer: str,
) -> Dict:
    """
    Promote a KB gap into an actual KB entry with both a question AND answer.
    Called by the 'Add to KB' route in the client dashboard.

    Steps:
      1. Validate that question and answer are both present and non-trivial.
      2. Write the Q+A pair to the knowledge base via models.save_faqs().
      3. Mark the gap as 'resolved' via models.mark_kb_gap_resolved() so it
         disappears from the AI Suggestions panel.

    Returns a dict with 'success' bool and optional 'error' string.
    """
    question = (question or '').strip()
    answer   = (answer   or '').strip()

    if not question:
        return {'success': False, 'error': 'Question is required.'}
    if not answer:
        return {'success': False, 'error': 'Answer is required — the KB entry would be useless without one.'}
    if len(answer) < 10:
        return {'success': False, 'error': 'Answer is too short to be useful.'}

    try:
        import models as _m
        import uuid

        # Build a validated FAQ dict and save it via the standard FAQ pipeline
        faq = {
            'faq_id':        str(uuid.uuid4()),
            'question':      question[:500],
            'answer':        answer[:5000],
            'category':      'General',
            'triggers':      [question.lower()[:100]],
            'tags':          [],
            'quality_score': 0.7,
            'embedding':     None,
        }
        _m.save_faqs(client_id, [faq])

        # Mark the gap resolved so it disappears from AI Suggestions.
        # mark_kb_gap_resolved only needs gap_id (no client_id param).
        if gap_id:
            _m.mark_kb_gap_resolved(gap_id)

        logger.info(
            f"[KbGap] gap promoted to KB client={client_id} gap_id={gap_id} "
            f"q='{question[:60]}'"
        )
        return {'success': True}

    except AttributeError as e:
        missing_fn = str(e)
        logger.error(f"[KbGap] add_gap_to_kb missing model function: {missing_fn}")
        return {'success': False, 'error': f'Model function missing: {missing_fn}'}
    except Exception as e:
        log_crash(logger, 'KbGap/add_to_kb', e, client_id=client_id)
        return {'success': False, 'error': 'Failed to add entry to KB.'}


# ── Poor-answer tracking ──────────────────────────────────────────────────────

def record_poor_answer(
    client_id: str,
    question: str,
    bot_answer: str,
    confidence: float,
    method: str,
    session_id: Optional[str] = None,
) -> None:
    """Record a thumbs-down rating against a bot answer via models.record_poor_answer()."""
    if not client_id:
        return
    try:
        import models as _m
        _m.record_poor_answer(
            client_id, question.strip()[:500], bot_answer.strip()[:2000],
            round(confidence, 4), method, session_id,
        )
        logger.debug(f"[PoorAnswer] recorded client={client_id} q='{question[:60]}'")
    except Exception as e:
        log_crash(logger, 'PoorAnswer/record', e, client_id=client_id)


def get_poor_answers(client_id: str, limit: int = 20) -> List[Dict]:
    """
    Return recent poor-answer records for a client via models.get_poor_answers().

    NOTE — shape changed from the old (broken) SQLAlchemy version: rows no
    longer have 'id' or 'created_at'. The actual poor_answers table dedupes
    by question with a hit counter rather than storing one row per
    occurrence, so rows now have 'hit_count', 'first_seen', and 'last_seen'
    instead. If a frontend reads 'id'/'created_at' from this response,
    it'll need a small update — that file wasn't part of this fix.
    """
    if not client_id:
        return []
    try:
        import models as _m
        return _m.get_poor_answers(client_id, limit=limit)
    except Exception as e:
        log_crash(logger, 'PoorAnswer/get', e, client_id=client_id)
        return []


# ── Weekly digest email ───────────────────────────────────────────────────────

def send_kb_gap_digest(
    client_id: str,
    operator_email: str,
    mail_instance: Any,
    app_instance: Any,
    top_n: int = 10,
) -> bool:
    """
    Send a weekly digest of unanswered questions to the operator.
    Returns True on success. Called from a scheduled job — never from the
    request pipeline.
    """
    if not client_id or not operator_email:
        logger.warning("[Digest] Missing client_id or operator_email — skipped")
        return False

    gaps = get_top_kb_gaps(client_id, limit=top_n)
    if not gaps:
        logger.info(f"[Digest] No gaps for client={client_id} — skipped")
        return True

    try:
        from flask_mail import Message

        rows_html = ''.join(
            f"<tr><td style='padding:6px 12px;border-bottom:1px solid #eee'>{i}.</td>"
            f"<td style='padding:6px 12px;border-bottom:1px solid #eee'>"
            f"{g['question']}</td>"
            f"<td style='padding:6px 12px;border-bottom:1px solid #eee;color:#888'>"
            f"asked {g['count']}×</td></tr>"
            for i, g in enumerate(gaps, 1)
        )

        html_body = f"""
        <div style="font-family:sans-serif;max-width:600px;margin:auto">
          <h2 style="color:#1a1a2e">📋 Weekly KB Gap Report</h2>
          <p>Here are the top {len(gaps)} questions your bot couldn't answer
          this week for <strong>{client_id}</strong>:</p>
          <table style="width:100%;border-collapse:collapse">
            <thead>
              <tr style="background:#f5f5f5">
                <th style="padding:8px 12px;text-align:left">#</th>
                <th style="padding:8px 12px;text-align:left">Question</th>
                <th style="padding:8px 12px;text-align:left">Frequency</th>
              </tr>
            </thead>
            <tbody>{rows_html}</tbody>
          </table>
          <p style="margin-top:24px;color:#888;font-size:13px">
            Lumvi · auto-generated digest · {datetime.utcnow().strftime('%Y-%m-%d')}
          </p>
        </div>
        """

        msg = Message(
            subject=f"Lumvi Weekly KB Gaps — {datetime.utcnow().strftime('%b %d')}",
            recipients=[operator_email],
            html=html_body,
        )

        with app_instance.app_context():
            mail_instance.send(msg)

        logger.info(
            f"[Digest] Sent {len(gaps)} gaps to {operator_email} for client={client_id}"
        )
        return True

    except Exception as e:
        log_crash(logger, 'Digest/send', e, client_id=client_id, email=operator_email)
        return False
