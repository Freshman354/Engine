"""
models/kb_gaps.py
-----------------
KB gap tracking (unanswered questions), poor answer feedback loop,
and gap digest scheduling.
"""
import json
import uuid
from datetime import datetime
from .db import get_db

def record_kb_gap(client_id: str, question: str, method: str, confidence: float) -> None:
    """
    Record an unanswered question in kb_gaps for later review.
    Called from ai_helper in a background thread — never blocks chat.
    NOTE: kb_gaps table is created once in init_db(), not here.

    Uses ON CONFLICT upsert so repeated identical questions increment count
    rather than being silently dropped. Requires a UNIQUE constraint on
    (client_id, question) — added by migrate_kb_gaps().
    """
    try:
        conn, cursor = get_db()
        cursor.execute('''
            INSERT INTO kb_gaps (client_id, question, method, confidence, count, last_seen)
            VALUES (%s, %s, %s, %s, 1, NOW())
            ON CONFLICT (client_id, question)
            DO UPDATE SET
                count      = kb_gaps.count + 1,
                last_seen  = NOW(),
                confidence = EXCLUDED.confidence,
                method     = EXCLUDED.method
        ''', (client_id, question[:500], method, confidence))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        import logging
        logging.getLogger(__name__).debug(f"[record_kb_gap] non-critical: {e}")


def get_kb_gaps(client_id: str, limit: int = 20, status: str = 'open') -> list:
    """
    Return the top unanswered questions for a client ordered by hit count.
    Used by ai_helper.get_top_kb_gaps() to surface the FAQ Manager's
    'Suggested FAQs' panel.

    Args:
        client_id: Lumvi client identifier.
        limit:     Maximum number of rows to return.
        status:    Filter by gap status ('open', 'resolved', or None for all).
                   Defaults to 'open' so callers only see actionable gaps.

    Returns [] on any failure — never raises.
    Each dict has: id, question, count, confidence, last_seen, method, status.
    """
    try:
        conn, cursor = get_db()
        if status:
            cursor.execute(
                """SELECT id, question, count, confidence, last_seen, method, status
                   FROM kb_gaps
                   WHERE client_id = %s AND status = %s
                   ORDER BY count DESC, last_seen DESC
                   LIMIT %s""",
                (client_id, status, limit)
            )
        else:
            cursor.execute(
                """SELECT id, question, count, confidence, last_seen, method, status
                   FROM kb_gaps
                   WHERE client_id = %s
                   ORDER BY count DESC, last_seen DESC
                   LIMIT %s""",
                (client_id, limit)
            )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        result = []
        for r in rows:
            row = dict(r)
            if row.get('last_seen'):
                row['last_seen'] = row['last_seen'].isoformat()
            result.append(row)
        return result
    except Exception as e:
        import logging
        logging.getLogger(__name__).debug(f"[get_kb_gaps] {e}")
        return []


def get_kb_gap_digest_last_sent(client_id: str):
    """
    Fix 6 — Return the UTC datetime of the last gap digest sent for this
    client, or None if no digest has ever been sent.
    """
    try:
        conn, cursor = get_db()
        cursor.execute(
            "SELECT gap_digest_last_sent FROM clients WHERE client_id = %s",
            (client_id,)
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        return row['gap_digest_last_sent'] if row else None
    except Exception:
        return None


def set_kb_gap_digest_last_sent(client_id: str) -> None:
    """
    Fix 6 — Stamp the current UTC time as the last digest send time for
    this client. Called immediately after a successful digest email send.
    """
    try:
        conn, cursor = get_db()
        cursor.execute(
            "UPDATE clients SET gap_digest_last_sent = NOW() WHERE client_id = %s",
            (client_id,)
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as _e:
        import logging
        logging.getLogger(__name__).debug(
            f"[set_kb_gap_digest_last_sent] non-critical: {_e}"
        )


# =====================================================================
# FIX IMPROVE-9 — POOR ANSWER FEEDBACK LOOP
# =====================================================================

def record_poor_answer(client_id: str, question: str, bot_answer: str,
                       confidence: float, method: str,
                       session_id: str = None) -> None:
    """
    Upsert a thumbs-down record into poor_answers.
    On conflict (same client + question), increments hit_count and
    updates last_seen — never creates duplicates.
    Never raises.
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute(
            """
            INSERT INTO poor_answers
                (client_id, question, bot_answer, confidence, method, session_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT poor_answers_client_question_uq
            DO UPDATE SET
                hit_count  = poor_answers.hit_count + 1,
                last_seen  = NOW(),
                -- Update answer/confidence/method to the most recent occurrence
                bot_answer = EXCLUDED.bot_answer,
                confidence = EXCLUDED.confidence,
                method     = EXCLUDED.method,
                session_id = COALESCE(EXCLUDED.session_id, poor_answers.session_id)
            """,
            (client_id, question[:500], bot_answer[:2000],
             float(confidence), method, session_id)
        )
        conn.commit()
    except Exception as e:
        import logging
        logging.getLogger(__name__).debug(f"[record_poor_answer] {e}")
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def get_poor_answers(client_id: str, limit: int = 20) -> list:
    """
    Return poor answers for a client ordered by hit_count descending.
    Used by the FAQ Manager "Needs Review" panel.
    Returns [] on any failure.
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute(
            """
            SELECT question, bot_answer, confidence, method,
                   hit_count, first_seen, last_seen
            FROM poor_answers
            WHERE client_id = %s
            ORDER BY hit_count DESC, last_seen DESC
            LIMIT %s
            """,
            (client_id, limit)
        )
        rows = cursor.fetchall()
        return [
            {
                'question':   row['question'],
                'bot_answer': row['bot_answer'],
                'confidence': row['confidence'],
                'method':     row['method'],
                'hit_count':  row['hit_count'],
                'first_seen': str(row.get('first_seen', '')),
                'last_seen':  str(row.get('last_seen',  '')),
            }
            for row in rows
        ]
    except Exception as e:
        import logging
        logging.getLogger(__name__).debug(f"[get_poor_answers] {e}")
        return []
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def mark_kb_gap_resolved(gap_id: int) -> None:
    """
    Mark a kb_gaps row as resolved and record the timestamp.

    Sets status='resolved' and resolved_at=NOW() for the given gap_id.
    Called by ai_helper.approve_and_publish_gap() after a new FAQ is inserted.
    Never raises — errors are logged and swallowed.
    """
    try:
        conn, cursor = get_db()
        cursor.execute(
            """UPDATE kb_gaps
               SET status = 'resolved', resolved_at = NOW()
               WHERE id = %s""",
            (gap_id,)
        )
        conn.commit()
        cursor.close()
        conn.close()
        import logging
        logging.getLogger(__name__).info(
            f"[mark_kb_gap_resolved] gap_id={gap_id} marked resolved"
        )
    except Exception as e:
        import logging
        logging.getLogger(__name__).debug(f"[mark_kb_gap_resolved] non-critical: {e}")


