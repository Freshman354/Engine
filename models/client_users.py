"""
models/client_users.py
----------------------
Client portal user management — creation, authentication,
retrieval, deletion, and password updates.
"""
import bcrypt
import secrets
from datetime import datetime
from .db import get_db

def create_client_user(client_id, email, password, name, invited_by):
    """Create a client-facing login. Returns id or None if email exists."""
    import hashlib, os as _os
    salt = _os.urandom(32)
    pw_hash = hashlib.pbkdf2_hmac('sha256', password.encode(), salt, 100000)
    stored = salt.hex() + ':' + pw_hash.hex()
    try:
        conn, cursor = get_db()
        cursor.execute(
            '''INSERT INTO client_users (client_id, email, password_hash, name, invited_by)
               VALUES (%s, %s, %s, %s, %s) RETURNING id''',
            (client_id, email.lower().strip(), stored, name, invited_by)
        )
        row = cursor.fetchone()
        conn.commit()
        cursor.close()
        conn.close()
        return row['id'] if row else None
    except Exception:
        return None


def verify_client_user(email, password):
    """Verify client user credentials. Returns user dict or None."""
    import hashlib, hmac
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute('SELECT * FROM client_users WHERE email = %s', (email.lower().strip(),))
        row = cursor.fetchone()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[verify_client_user] DB lookup failed: {e}')
        return None
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass

    if not row:
        return None
    stored = row['password_hash']
    try:
        salt_hex, hash_hex = stored.split(':')
        salt = bytes.fromhex(salt_hex)
        pw_hash = hashlib.pbkdf2_hmac('sha256', password.encode(), salt, 100000)
        # BUG-07 fix: use constant-time comparison to prevent timing attacks
        if hmac.compare_digest(pw_hash.hex(), hash_hex):
            conn2 = cursor2 = None
            try:
                conn2, cursor2 = get_db()
                cursor2.execute('UPDATE client_users SET last_login=NOW() WHERE id=%s', (row['id'],))
                conn2.commit()
            except Exception:
                pass
            finally:
                if cursor2:
                    try: cursor2.close()
                    except Exception: pass
                if conn2:
                    try: conn2.close()
                    except Exception: pass
            return dict(row)
    except Exception:
        pass
    return None


def get_client_users(client_id):
    """Get all users for a client."""
    conn, cursor = get_db()
    cursor.execute(
        '''SELECT id, client_id, email, name, role, created_at, last_login
           FROM client_users WHERE client_id = %s ORDER BY created_at DESC''',
        (client_id,)
    )
    rows = [dict(r) for r in cursor.fetchall()]
    cursor.close()
    conn.close()
    for r in rows:
        for col in ('created_at', 'last_login'):
            if r.get(col):
                r[col] = r[col].isoformat()
    return rows


def get_client_user_by_id(user_id):
    conn, cursor = get_db()
    cursor.execute('SELECT * FROM client_users WHERE id = %s', (user_id,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return dict(row) if row else None


def delete_client_user(client_user_id, client_id):
    conn, cursor = get_db()
    cursor.execute(
        'DELETE FROM client_users WHERE id = %s AND client_id = %s',
        (client_user_id, client_id)
    )
    conn.commit()
    cursor.close()
    conn.close()


def update_client_user_password(client_user_id, new_password):
    import hashlib, os as _os
    salt = _os.urandom(32)
    pw_hash = hashlib.pbkdf2_hmac('sha256', new_password.encode(), salt, 100000)
    stored = salt.hex() + ':' + pw_hash.hex()
    conn, cursor = get_db()
    cursor.execute('UPDATE client_users SET password_hash=%s WHERE id=%s', (stored, client_user_id))
    conn.commit()
    cursor.close()
    conn.close()


