"""
models/sessions.py
------------------
Persistent chat session storage — load, upsert, and delete session state.
Used by the AI pipeline for multi-turn memory across requests.
"""
import json
from datetime import datetime
from .db import get_db

def load_session(client_id: str, session_id: str) -> dict:
    """
    Load a persistent session from PostgreSQL.
    Returns a dict with guaranteed keys (safe defaults when row missing).
    Never raises — returns all-default dict on any DB failure.
    """
    import json as _json
    _defaults = {
        'name': None, 'email': None, 'phone': None,
        'purchase_stage': None, 'frustration_score': 0,
        'turn_count': 0, 'session_data': {},
        # Unpacked from session_data JSONB — no dedicated column needed.
        # Written by upsert_session() via the extra dict path.
        'handoff_offered':        False,
        # Unpacked from session_data JSONB — no dedicated column needed.
        'graceful_close_offered': False,
    }
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute(
            """SELECT name, email, phone, purchase_stage,
                      frustration_score, turn_count, session_data
               FROM chat_sessions
               WHERE client_id = %s AND session_id = %s""",
            (client_id, session_id)
        )
        row = cursor.fetchone()
        if not row:
            return dict(_defaults)
        result = dict(_defaults)
        result['name']              = row['name']
        result['email']             = row['email']
        result['phone']             = row['phone']
        result['purchase_stage']    = row['purchase_stage']
        result['frustration_score'] = int(row['frustration_score'] or 0)
        result['turn_count']        = int(row['turn_count'] or 0)
        raw_sd = row['session_data']
        if isinstance(raw_sd, str):
            try: raw_sd = _json.loads(raw_sd)
            except Exception: raw_sd = {}
        result['session_data'] = raw_sd or {}
        # Unpack handoff_offered from JSONB so ai_helper can read it via
        # _db_session.get('handoff_offered'). Stored there by upsert_session()
        # through the extra-fields path — no dedicated column required.
        result['handoff_offered']        = bool(result['session_data'].get('handoff_offered', False))
        result['graceful_close_offered'] = bool(result['session_data'].get('graceful_close_offered', False))
        return result
    except Exception as e:
        import logging
        logging.getLogger(__name__).debug(f'[load_session] {e}')
        return dict(_defaults)
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def upsert_session(client_id: str, session_id: str, updates: dict) -> bool:
    """
    Create or update a chat session row.
    frustration_score accumulates (not overwritten).
    turn_count increments by 1 on every conflict.
    Returns True on success, False on failure.
    """
    import json as _json
    conn = cursor = None
    try:
        conn, cursor = get_db()
        named    = ('name', 'email', 'phone', 'purchase_stage',
                    'frustration_score', 'turn_count')
        col_vals = {k: updates[k] for k in named if k in updates}
        extra    = {k: v for k, v in updates.items() if k not in named}
        cursor.execute(
            """
            INSERT INTO chat_sessions
                (client_id, session_id, name, email, phone,
                 purchase_stage, frustration_score, turn_count,
                 session_data, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 1, %s, NOW())
            ON CONFLICT ON CONSTRAINT chat_sessions_client_session_uq
            DO UPDATE SET
                name              = COALESCE(EXCLUDED.name,           chat_sessions.name),
                email             = COALESCE(EXCLUDED.email,          chat_sessions.email),
                phone             = COALESCE(EXCLUDED.phone,          chat_sessions.phone),
                purchase_stage    = COALESCE(EXCLUDED.purchase_stage, chat_sessions.purchase_stage),
                frustration_score = chat_sessions.frustration_score
                                    + GREATEST(EXCLUDED.frustration_score, 0),
                turn_count        = chat_sessions.turn_count + 1,
                session_data      = chat_sessions.session_data || EXCLUDED.session_data,
                updated_at        = NOW()
            """,
            (
                client_id, session_id,
                col_vals.get('name'), col_vals.get('email'),
                col_vals.get('phone'), col_vals.get('purchase_stage'),
                max(int(col_vals.get('frustration_score', 0)), 0),
                _json.dumps(extra) if extra else '{}',
            )
        )
        conn.commit()
        return True
    except Exception as e:
        import logging
        logging.getLogger(__name__).debug(f'[upsert_session] {e}')
        return False
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def delete_session(client_id: str, session_id: str) -> bool:
    """Hard-delete a session row on widget reset. Returns True on success."""
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute(
            "DELETE FROM chat_sessions WHERE client_id = %s AND session_id = %s",
            (client_id, session_id)
        )
        conn.commit()
        return True
    except Exception as e:
        import logging
        logging.getLogger(__name__).debug(f'[delete_session] {e}')
        return False
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()




# ═══════════════════════════════════════════════════════════════════════════════
# TIER 1 — CSAT, STATUS FLOW, TYPING INDICATORS
# ═══════════════════════════════════════════════════════════════════════════════

def submit_csat(client_id: str, session_id: str, rating: int) -> bool:
    """
    Record a CSAT rating (1 = positive, -1 = negative) for a session.
    Overwrites any previous rating — idempotent, user can change their mind.
    Returns True on success, False on invalid rating or DB error.
    """
    if rating not in (1, -1):
        return False
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute(
            """
            UPDATE chat_sessions
               SET csat_rating = %s, csat_submitted_at = NOW()
             WHERE client_id = %s AND session_id = %s
            """,
            (rating, client_id, session_id)
        )
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        import logging
        logging.getLogger(__name__).debug(f'[submit_csat] {e}')
        return False
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


_VALID_STATUSES = {'open', 'in_progress', 'pending_customer', 'resolved'}
_STATUS_TS_COL  = {
    'in_progress':      'in_progress_at',
    'pending_customer': 'pending_customer_at',
    'resolved':         'resolved_at',
}


def set_session_status(client_id: str, session_id: str, status: str) -> bool:
    """
    Transition a session to a new status and stamp the matching *_at column.
    Returns True on success, False if status invalid or row not found.
    """
    if status not in _VALID_STATUSES:
        return False
    conn = cursor = None
    try:
        conn, cursor = get_db()
        ts_col = _STATUS_TS_COL.get(status)
        if ts_col:
            cursor.execute(
                f"""
                UPDATE chat_sessions
                   SET status = %s, {ts_col} = NOW(), updated_at = NOW()
                 WHERE client_id = %s AND session_id = %s
                """,
                (status, client_id, session_id)
            )
        else:
            cursor.execute(
                """
                UPDATE chat_sessions
                   SET status = %s, updated_at = NOW()
                 WHERE client_id = %s AND session_id = %s
                """,
                (status, client_id, session_id)
            )
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        import logging
        logging.getLogger(__name__).debug(f'[set_session_status] {e}')
        return False
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def get_session_status(client_id: str, session_id: str) -> str:
    """Return the current status string for a session, defaulting to 'open'."""
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute(
            "SELECT status FROM chat_sessions WHERE client_id = %s AND session_id = %s",
            (client_id, session_id)
        )
        row = cursor.fetchone()
        return (row or {}).get('status') or 'open'
    except Exception:
        return 'open'
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def set_agent_typing(client_id: str, session_id: str) -> bool:
    """
    Record that an agent is currently typing.
    Stores an ISO timestamp in session_data JSONB — no migration needed.
    The widget polls get_agent_typing() and shows the dots if within 4s.
    Returns True on success.
    """
    from datetime import datetime
    return upsert_session(client_id, session_id, {
        'agent_typing_at': datetime.utcnow().isoformat()
    })


def get_agent_typing(client_id: str, session_id: str) -> bool:
    """
    Return True if an agent typing event was stored within the last 4 seconds.
    Used by the widget polling endpoint — fails closed (returns False) on error.
    """
    from datetime import datetime, timedelta
    sess = load_session(client_id, session_id)
    raw  = sess.get('session_data', {}).get('agent_typing_at')
    if not raw:
        return False
    try:
        ts = datetime.fromisoformat(raw)
        return (datetime.utcnow() - ts) < timedelta(seconds=4)
    except Exception:
        return False
