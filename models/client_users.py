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
        '''SELECT id, client_id, email, name, role, is_primary_contact,
                  created_at, last_login
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


def get_primary_contact(client_id):
    """
    Returns the client_user marked as this client's primary contact, or
    None if none has been designated. Used to source clients.notification_
    email from a business-owned account instead of agency-entered text —
    see set_primary_contact() and the write guard in
    blueprints/client_settings.py.
    """
    conn, cursor = get_db()
    cursor.execute(
        '''SELECT id, client_id, email, name, role, created_at, last_login
           FROM client_users WHERE client_id = %s AND is_primary_contact = TRUE
           LIMIT 1''',
        (client_id,)
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return dict(row) if row else None


def set_primary_contact(client_id, client_user_id):
    """
    Marks one client_user as this client's primary contact, unsetting any
    previous one first — at most one primary per client_id. Also copies
    their email into clients.notification_email in the same transaction,
    which is what notify_handoff() (inbox.py) and the lead-notification
    path (leads.py) actually read — see client_settings.py for why the
    agency can no longer overwrite that column directly once this is set.

    Sync-not-live-lookup is a deliberate choice: there's currently no
    self-service "change my own email" for client_users, so drift isn't a
    live risk. If that's added later, it needs to also re-sync
    notification_email here, or this should switch to a live lookup in
    inbox.py/leads.py instead.

    All three updates run in one transaction so a failure partway through
    can't leave the primary-contact flag and notification_email
    disagreeing with each other.

    Returns True on success, False if client_user_id doesn't belong to
    client_id (no matching row to set — the unset still commits, which is
    harmless: clearing a primary that may not have existed is a no-op).
    """
    conn, cursor = get_db()
    try:
        cursor.execute(
            'UPDATE client_users SET is_primary_contact = FALSE WHERE client_id = %s',
            (client_id,)
        )
        cursor.execute(
            '''UPDATE client_users SET is_primary_contact = TRUE
               WHERE id = %s AND client_id = %s''',
            (client_user_id, client_id)
        )
        updated = cursor.rowcount > 0
        if updated:
            cursor.execute(
                '''UPDATE clients SET notification_email = (
                       SELECT email FROM client_users WHERE id = %s
                   ) WHERE client_id = %s''',
                (client_user_id, client_id)
            )
        conn.commit()
        return updated
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


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


