"""
models/tags.py
--------------
CRUD for conversation tags (label library per client) and the
session_tags junction table (which sessions carry which tags).
All functions follow the standard _safe() pattern: conn/cursor = None,
try/finally close, return safe defaults on error.
"""
from .db import get_db


def get_client_tags(client_id: str) -> list:
    """Return all tags for a client, sorted alphabetically."""
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute(
            "SELECT id, name, color FROM tags WHERE client_id = %s ORDER BY name",
            (client_id,)
        )
        return [dict(r) for r in cursor.fetchall()]
    except Exception:
        return []
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def create_tag(client_id: str, name: str, color: str = '#6366f1') -> dict | None:
    """
    Create a tag for a client.
    Returns the created {id, name, color} dict, or None on duplicate / error.
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute(
            """
            INSERT INTO tags (client_id, name, color)
            VALUES (%s, %s, %s)
            ON CONFLICT (client_id, name) DO NOTHING
            RETURNING id, name, color
            """,
            (client_id, name[:50].strip(), color or '#6366f1')
        )
        row = cursor.fetchone()
        conn.commit()
        return dict(row) if row else None
    except Exception:
        return None
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def delete_tag(client_id: str, tag_id: int) -> bool:
    """
    Delete a tag and all its session_tags rows (CASCADE handles the junction).
    Returns True if a row was deleted.
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute(
            "DELETE FROM tags WHERE id = %s AND client_id = %s",
            (tag_id, client_id)
        )
        conn.commit()
        return cursor.rowcount > 0
    except Exception:
        return False
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def apply_tag(session_id: str, tag_id: int, client_id: str) -> bool:
    """
    Apply a tag to a session. Idempotent — ON CONFLICT DO NOTHING.
    Returns True on success.
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute(
            """
            INSERT INTO session_tags (session_id, tag_id, client_id)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (session_id, tag_id, client_id)
        )
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def remove_tag(session_id: str, tag_id: int) -> bool:
    """Remove a tag from a session. Returns True if a row was deleted."""
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute(
            "DELETE FROM session_tags WHERE session_id = %s AND tag_id = %s",
            (session_id, tag_id)
        )
        conn.commit()
        return cursor.rowcount > 0
    except Exception:
        return False
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def get_session_tags(session_id: str) -> list:
    """Return all tags applied to a specific session."""
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute(
            """
            SELECT t.id, t.name, t.color
              FROM session_tags st
              JOIN tags t ON t.id = st.tag_id
             WHERE st.session_id = %s
             ORDER BY t.name
            """,
            (session_id,)
        )
        return [dict(r) for r in cursor.fetchall()]
    except Exception:
        return []
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()
