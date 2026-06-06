"""
models/users.py
---------------
User creation, authentication, profile management, password reset,
Google OAuth linking, and onboarding completion.
"""
import bcrypt
import secrets
import uuid
from datetime import datetime
from .db import get_db

def mark_onboarding_complete(user_id: int) -> None:
    """Mark user's onboarding as done — prevents wizard from re-appearing."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            "UPDATE users SET onboarding_completed = TRUE WHERE id = %s",
            (user_id,)
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[mark_onboarding_complete] {e}")


def create_user(email, password, plan_type='starter'):
    """Create a new user. Returns user_id on success, None if email already exists."""
    conn = cursor = None
    try:
        conn, cursor = get_db()
        password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        cursor.execute(
            'INSERT INTO users (email, password_hash, plan_type) VALUES (%s, %s, %s) RETURNING id',
            (email, password_hash, plan_type)
        )
        user_id = cursor.fetchone()['id']
        conn.commit()
        return user_id
    except psycopg2.IntegrityError:
        if conn:
            try: conn.rollback()
            except Exception: pass
        return None  # Email already exists
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[create_user] {e}')
        if conn:
            try: conn.rollback()
            except Exception: pass
        return None
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass

def verify_user(email, password):
    """Verify user credentials. Returns user dict on success, None otherwise."""
    try:
        conn, cursor = get_db()
        cursor.execute('SELECT * FROM users WHERE email = %s', (email,))
        user = cursor.fetchone()
        cursor.close()
        conn.close()
        if user and bcrypt.checkpw(password.encode('utf-8'), user['password_hash'].encode('utf-8')):
            return dict(user)
        return None
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[verify_user] {e}')
        return None


def get_user_by_id(user_id):
    """Get user by ID. Returns None on missing row or DB error."""
    try:
        conn, cursor = get_db()
        cursor.execute('SELECT * FROM users WHERE id = %s', (user_id,))
        user = cursor.fetchone()
        cursor.close()
        conn.close()
        return dict(user) if user else None
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[get_user_by_id] {e}')
        return None


def get_user_by_email(email):
    """Get user by email. Returns None on missing row or DB error."""
    try:
        conn, cursor = get_db()
        cursor.execute('SELECT * FROM users WHERE email = %s', (email,))
        user = cursor.fetchone()
        cursor.close()
        conn.close()
        return dict(user) if user else None
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[get_user_by_email] {e}')
        return None


# =====================================================================
# PASSWORD RESET FUNCTIONS
# =====================================================================
# GOOGLE OAUTH
# =====================================================================

def migrate_google_oauth():
    """Add google_id column to users table for Google OAuth sign-in."""
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS google_id TEXT UNIQUE"
        )
        conn.commit()
        print("✅ migrate_google_oauth: google_id column ready")
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[migrate_google_oauth] {e}')
        if conn:
            try: conn.rollback()
            except Exception: pass
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


def get_user_by_google_id(google_id):
    """Get user by Google ID. Returns None if not found."""
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute('SELECT * FROM users WHERE google_id = %s', (google_id,))
        user = cursor.fetchone()
        return dict(user) if user else None
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[get_user_by_google_id] {e}')
        return None
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


def create_or_link_google_user(google_id, email):
    """
    Find an existing user by email and link their Google ID,
    or create a brand-new free account if no match is found.
    Returns the user dict on success, None on failure.
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()

        # 1. Existing account with this google_id — just return it
        cursor.execute('SELECT * FROM users WHERE google_id = %s', (google_id,))
        user = cursor.fetchone()
        if user:
            return dict(user)

        # 2. Existing account matched by email — link the Google ID
        cursor.execute('SELECT * FROM users WHERE email = %s', (email,))
        user = cursor.fetchone()
        if user:
            if not user.get('google_id'):
                cursor.execute(
                    'UPDATE users SET google_id = %s WHERE email = %s',
                    (google_id, email)
                )
                conn.commit()
            return dict(user)

        # 3. Brand-new user — store a random bcrypt hash so password_hash
        #    NOT NULL is satisfied, but this account can never be accessed
        #    via password login (the hash is unguessable).
        import secrets as _secrets
        random_hash = bcrypt.hashpw(
            _secrets.token_bytes(32), bcrypt.gensalt()
        ).decode('utf-8')

        cursor.execute(
            """
            INSERT INTO users (email, google_id, password_hash, plan_type)
            VALUES (%s, %s, %s, 'free')
            RETURNING *
            """,
            (email, google_id, random_hash)
        )
        user = cursor.fetchone()
        conn.commit()
        return dict(user) if user else None

    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[create_or_link_google_user] {e}')
        if conn:
            try: conn.rollback()
            except Exception: pass
        return None
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


# =====================================================================

def save_password_reset_token(user_id, token, expires_at):
    """Save a password reset token (one per user — delete old ones first)."""
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute('DELETE FROM password_reset_tokens WHERE user_id = %s', (user_id,))
        cursor.execute(
            'INSERT INTO password_reset_tokens (user_id, token, expires_at) VALUES (%s, %s, %s)',
            (user_id, token, expires_at)
        )
        conn.commit()
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


def get_password_reset_token(token):
    """Return token row if it exists, else None."""
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute('SELECT * FROM password_reset_tokens WHERE token = %s', (token,))
        row = cursor.fetchone()
        return dict(row) if row else None
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[get_password_reset_token] {e}')
        return None
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


def delete_password_reset_token(token):
    """Delete a used or expired token."""
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute('DELETE FROM password_reset_tokens WHERE token = %s', (token,))
        conn.commit()
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


def update_user_password(user_id, new_password):
    """Hash and save a new password for a user."""
    import bcrypt
    hashed = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute('UPDATE users SET password_hash = %s WHERE id = %s', (hashed, user_id))
        conn.commit()
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass

