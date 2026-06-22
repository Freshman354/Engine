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
  )
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from utils import get_logger, log_crash

logger = get_logger('lumvi.kb_gap')


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
    Silently skips when client_id is missing (development/testing).

    Note: session_id is accepted for call-site compatibility, but the
    kb_gaps table has no session_id column, so it isn't persisted.
    """
    if not client_id or not question:
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
    models.get_kb_gaps() — used to surface the FAQ Manager's 'Suggested
    FAQs' panel.
    """
    if not client_id:
        return []
    try:
        import models as _m
        return _m.get_kb_gaps(client_id, limit=limit)
    except Exception as e:
        log_crash(logger, 'KbGap/get_top', e, client_id=client_id)
        return []


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
