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
    Persist a question the bot couldn't answer to the KbGap table.
    Called as a background task — never blocks the response pipeline.
    Silently skips when client_id is missing (development/testing).
    """
    if not client_id or not question:
        return
    try:
        import models as _m
        existing = _m.KbGap.query.filter_by(
            client_id=client_id, question=question.strip()[:500]
        ).first()
        if existing:
            existing.count = (existing.count or 1) + 1
            existing.last_seen = datetime.utcnow()
        else:
            gap = _m.KbGap(
                client_id=client_id,
                question=question.strip()[:500],
                method=method,
                confidence=round(confidence, 4),
                session_id=session_id,
                count=1,
                last_seen=datetime.utcnow(),
            )
            _m.db.session.add(gap)
        _m.db.session.commit()
        logger.debug(f"[KbGap] recorded client={client_id} q='{question[:60]}'")
    except Exception as e:
        log_crash(logger, 'KbGap/record', e, client_id=client_id)


def get_top_kb_gaps(client_id: str, limit: int = 20) -> List[Dict]:
    """Return the most-asked unanswered questions for a client."""
    if not client_id:
        return []
    try:
        import models as _m
        rows = (
            _m.KbGap.query
            .filter_by(client_id=client_id)
            .order_by(_m.KbGap.count.desc(), _m.KbGap.last_seen.desc())
            .limit(limit)
            .all()
        )
        return [
            {
                'id':         r.id,
                'question':   r.question,
                'count':      r.count or 1,
                'last_seen':  r.last_seen.isoformat() if r.last_seen else None,
                'confidence': r.confidence,
                'method':     r.method,
                'status':     getattr(r, 'status', 'open'),
            }
            for r in rows
        ]
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
    """Record a thumbs-down rating against a bot answer."""
    if not client_id:
        return
    try:
        import models as _m
        rec = _m.PoorAnswer(
            client_id=client_id,
            question=question.strip()[:500],
            bot_answer=bot_answer.strip()[:2000],
            confidence=round(confidence, 4),
            method=method,
            session_id=session_id,
            created_at=datetime.utcnow(),
        )
        _m.db.session.add(rec)
        _m.db.session.commit()
        logger.debug(f"[PoorAnswer] recorded client={client_id} q='{question[:60]}'")
    except Exception as e:
        log_crash(logger, 'PoorAnswer/record', e, client_id=client_id)


def get_poor_answers(client_id: str, limit: int = 20) -> List[Dict]:
    """Return recent poor-answer records for a client."""
    if not client_id:
        return []
    try:
        import models as _m
        rows = (
            _m.PoorAnswer.query
            .filter_by(client_id=client_id)
            .order_by(_m.PoorAnswer.created_at.desc())
            .limit(limit)
            .all()
        )
        return [
            {
                'id':         r.id,
                'question':   r.question,
                'bot_answer': r.bot_answer,
                'confidence': r.confidence,
                'method':     r.method,
                'created_at': r.created_at.isoformat() if r.created_at else None,
            }
            for r in rows
        ]
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
