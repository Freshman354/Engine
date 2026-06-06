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
    Each dict contains: session_id, user_message, bot_response, timestamp (ISO str).
    Returns [] on failure.
    """
    try:
        conn, cursor = get_db()
        cursor.execute(
            '''SELECT session_id, user_message, bot_response, timestamp
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
                'timestamp':    row['timestamp'].isoformat() if row.get('timestamp') else '',
            })
        return result
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[get_conversations] {e}')
        return []


# =====================================================================
# KNOWLEDGE BASE — Phase 2 RAG
# =====================================================================

