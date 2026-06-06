"""
models/articles.py
------------------
Help Center article CRUD — used by the article manager and chat widget.
"""
from datetime import datetime
from .db import get_db

def get_articles(client_id):
    """Get all articles for a client ordered by position."""
    conn, cursor = get_db()
    cursor.execute(
        'SELECT * FROM articles WHERE client_id = %s ORDER BY position ASC, created_at ASC',
        (client_id,)
    )
    rows = [dict(r) for r in cursor.fetchall()]
    cursor.close()
    conn.close()
    for r in rows:
        for col in ('created_at', 'updated_at'):
            if r.get(col):
                r[col] = r[col].isoformat()
    return rows


def get_article_by_id(article_id, client_id):
    """Get a single article."""
    conn, cursor = get_db()
    cursor.execute(
        'SELECT * FROM articles WHERE id = %s AND client_id = %s',
        (article_id, client_id)
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return dict(row) if row else None


def create_article(client_id, title, content, category='General'):
    """Create a new help article."""
    conn, cursor = get_db()
    cursor.execute(
        '''INSERT INTO articles (client_id, title, content, category)
           VALUES (%s, %s, %s, %s) RETURNING id''',
        (client_id, title, content, category)
    )
    row = cursor.fetchone()
    conn.commit()
    cursor.close()
    conn.close()
    return row['id'] if row else None


def update_article(article_id, client_id, title, content, category='General'):
    """Update an existing article."""
    conn, cursor = get_db()
    cursor.execute(
        '''UPDATE articles
           SET title=%s, content=%s, category=%s, updated_at=NOW()
           WHERE id=%s AND client_id=%s''',
        (title, content, category, article_id, client_id)
    )
    conn.commit()
    cursor.close()
    conn.close()


def delete_article(article_id, client_id):
    """Delete an article."""
    conn, cursor = get_db()
    cursor.execute(
        'DELETE FROM articles WHERE id=%s AND client_id=%s',
        (article_id, client_id)
    )
    conn.commit()
    cursor.close()
    conn.close()


# =====================================================================
# CLIENT PORTAL USERS
# =====================================================================

