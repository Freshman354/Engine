"""
models/conversations.py
------------------------
Conversation logging, daily message counting, client owner lookup,
conversation history retrieval, and summary storage.
"""
from datetime import datetime
from .db import get_db

def get_daily_message_count(client_id):
    """
    Return the number of chat messages logged for this client today (UTC).
    Excludes lead_captured rows — those are lead form submissions, not chat
    turns, and should not count against the messages_per_day plan limit.
    Fails open (returns 0) if the DB is unavailable so chat is never
    blocked by an infrastructure hiccup.
    """
    try:
        conn, cursor = get_db()
        today = datetime.utcnow().strftime('%Y-%m-%d')
        cursor.execute(
            '''
            SELECT COUNT(*) AS cnt
            FROM conversations
            WHERE client_id = %s
              AND DATE(timestamp) = %s
              AND (method IS NULL OR method != 'lead_captured')
            ''',
            (client_id, today)
        )
        row = cursor.fetchone() or {}
        cursor.close()
        conn.close()
        return int(row.get('cnt', 0))
    except Exception:
        return 0  # fail open — never block chat due to a DB error


def get_monthly_conversation_count(client_id):
    """
    Return the number of distinct CONVERSATIONS (not raw messages) logged
    for this client so far in the current calendar month (UTC).

    Used by the new ai_starter/ai_growth/ai_scale 'conversations_per_month'
    plan limit — a different unit from get_daily_message_count()'s
    'messages_per_day' (which counts every logged row, is per-day, and
    stays exactly as-is for grandfathered solo/starter/pro/growth/agency
    plans).

    A "conversation" = one distinct session_id. Rows with no session_id
    (older logs, or calls made without one) each count as their own
    one-message conversation via COALESCE(session_id, id::text), so they
    aren't dropped or accidentally merged together.

    Excludes lead_captured rows, same reasoning as get_daily_message_count.
    Fails open (returns 0) if the DB is unavailable so chat is never
    blocked by an infrastructure hiccup.
    """
    try:
        conn, cursor = get_db()
        cursor.execute(
            '''
            SELECT COUNT(DISTINCT COALESCE(session_id, id::text)) AS cnt
            FROM conversations
            WHERE client_id = %s
              AND timestamp >= date_trunc('month', CURRENT_DATE)
              AND (method IS NULL OR method != 'lead_captured')
            ''',
            (client_id,)
        )
        row = cursor.fetchone() or {}
        cursor.close()
        conn.close()
        return int(row.get('cnt', 0))
    except Exception:
        return 0  # fail open — never block chat due to a DB error


def get_client_owner(client_id):
    """
    Return the full user row for whoever owns this client_id.
    Used by plan enforcement helpers in app.py.
    Returns None if client or user not found.
    """
    try:
        conn, cursor = get_db()
        cursor.execute(
            'SELECT user_id FROM clients WHERE client_id = %s',
            (client_id,)
        )
        row = cursor.fetchone()
        if not row:
            cursor.close()
            conn.close()
            return None
        user_id = row['user_id']
        cursor.execute('SELECT * FROM users WHERE id = %s', (user_id,))
        user = cursor.fetchone()
        cursor.close()
        conn.close()
        return dict(user) if user else None
    except Exception:
        return None


# =====================================================================
# USER FUNCTIONS
# =====================================================================

def get_conversation_message_count(client_id: str) -> int:
    """Count total conversation turns for a client (used to trigger summarisation)."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            "SELECT COUNT(*) AS cnt FROM conversations WHERE client_id = %s",
            (client_id,)
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        return int(row['cnt']) if row else 0
    except Exception:
        return 0


def save_conversation_summary(client_id: str, summary: str, message_count: int) -> None:
    """Persist a Gemini-generated conversation summary."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            '''INSERT INTO conversation_summaries (client_id, summary, message_count)
               VALUES (%s, %s, %s)''',
            (client_id, summary, message_count)
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        pass  # Non-critical — never break chat over a summary failure


def get_recent_conversations(client_id: str, limit: int = 15) -> list:
    """
    Return the last `limit` real conversation turns for a client,
    oldest → newest, as a list of {role, content} dicts
    ready to pass directly to generate_human_like_response.
    Excludes lead_captured rows (form submissions) — those are not
    real chat turns and would pollute the AI conversation context.
    """
    try:
        conn, cursor = get_db()
        cursor.execute(
            '''SELECT user_message, bot_response
               FROM conversations
               WHERE client_id = %s
                 AND (method IS NULL OR method != 'lead_captured')
               ORDER BY timestamp DESC
               LIMIT %s''',
            (client_id, limit)
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        result = []
        for row in reversed(rows):   # oldest first
            result.append({'role': 'user',      'content': row['user_message']})
            result.append({'role': 'assistant', 'content': row['bot_response']})
        return result
    except Exception:
        return []


def get_conversations(client_id: str, limit: int = 200) -> list:
    """
    Return the last `limit` real conversation turns for a client,
    newest first, as a list of dicts ready for the dashboard UI.
    Excludes lead_captured rows (form submissions).
    Each dict contains: session_id, user_message, bot_response, matched,
    method, timestamp (ISO str). Returns [] on failure.
    """
    try:
        conn, cursor = get_db()
        cursor.execute(
            '''SELECT session_id, user_message, bot_response, matched, method, timestamp
               FROM conversations
               WHERE client_id = %s
                 AND (method IS NULL OR method != 'lead_captured')
               ORDER BY timestamp DESC
               LIMIT %s''',
            (client_id, limit)
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        result = []
        for row in rows:
            result.append({
                'session_id':   row.get('session_id') or '—',
                'user_message': row.get('user_message') or '',
                'bot_response': row.get('bot_response') or '',
                'matched':      bool(row.get('matched')),
                'method':       row.get('method') or '',
                'timestamp':    row['timestamp'].isoformat() if row.get('timestamp') else '',
            })
        return result
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[get_conversations] {e}')
        return []


def get_conversation_clients_summary(days: int = 7) -> list:
    """
    Every client with conversation activity in the last `days` days,
    with turn count and match rate — powers the admin dashboard's
    Conversations tab overview (which chatbots to look at first).
    Match rate excludes lead_captured rows (not a real answer attempt).
    """
    try:
        conn, cursor = get_db()
        cursor.execute(
            '''SELECT c.client_id, cl.company_name, u.email AS owner_email,
                      COUNT(*) AS turn_count,
                      COUNT(*) FILTER (WHERE c.matched = TRUE) AS matched_count,
                      MAX(c.timestamp) AS last_activity
               FROM conversations c
               LEFT JOIN clients cl ON c.client_id = cl.client_id
               LEFT JOIN users   u  ON cl.user_id   = u.id
               WHERE c.timestamp >= NOW() - (INTERVAL '1 day' * %s)
                 AND (c.method IS NULL OR c.method != 'lead_captured')
               GROUP BY c.client_id, cl.company_name, u.email
               ORDER BY turn_count DESC''',
            (days,)
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        result = []
        for r in rows:
            turns = int(r['turn_count'])
            matched = int(r['matched_count'])
            result.append({
                'client_id':     r['client_id'],
                'company_name':  r['company_name'] or r['client_id'],
                'owner_email':   r['owner_email'] or '—',
                'turn_count':    turns,
                'match_rate':    round(100 * matched / turns, 1) if turns else 0.0,
                'last_activity': r['last_activity'].isoformat() if r['last_activity'] else None,
            })
        return result
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[get_conversation_clients_summary] {e}')
        return []


def get_latest_conversation_summary(client_id: str) -> str:
    """Return the most recent summary string, or empty string if none."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            '''SELECT summary FROM conversation_summaries
               WHERE client_id = %s
               ORDER BY created_at DESC LIMIT 1''',
            (client_id,)
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        return row['summary'] if row else ''
    except Exception:
        return ''
# =====================================================================
# KNOWLEDGE BASE — Phase 2 RAG
# =====================================================================

