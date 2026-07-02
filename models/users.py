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


# =====================================================================
# PROFILE MANAGEMENT
# =====================================================================

def update_user_profile(user_id, company_name=None, logo_url=None, contact_phone=None):
    """Update the agency's own profile fields. Only provided (non-None) fields are changed."""
    fields, values = [], []
    if company_name is not None:
        fields.append('company_name = %s'); values.append(company_name[:200])
    if logo_url is not None:
        fields.append('logo_url = %s'); values.append(logo_url[:500])
    if contact_phone is not None:
        fields.append('contact_phone = %s'); values.append(contact_phone[:50])
    if not fields:
        return True  # nothing to do
    conn = cursor = None
    try:
        conn, cursor = get_db()
        values.append(user_id)
        cursor.execute(f"UPDATE users SET {', '.join(fields)} WHERE id = %s", values)
        conn.commit()
        return True
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[update_user_profile] {e}')
        if conn:
            try: conn.rollback()
            except Exception: pass
        return False
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


def update_user_email(user_id, new_email):
    """
    Change login email. Returns {'success': True} or {'success': False, 'error': str}.
    Rejects if another account already uses this email (users.email is UNIQUE).
    """
    new_email = (new_email or '').strip().lower()
    if not new_email or '@' not in new_email:
        return {'success': False, 'error': 'Enter a valid email address.'}
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute('SELECT id FROM users WHERE email = %s AND id != %s', (new_email, user_id))
        if cursor.fetchone():
            return {'success': False, 'error': 'That email is already in use.'}
        cursor.execute('UPDATE users SET email = %s WHERE id = %s', (new_email, user_id))
        conn.commit()
        return {'success': True}
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[update_user_email] {e}')
        if conn:
            try: conn.rollback()
            except Exception: pass
        return {'success': False, 'error': 'Could not update email.'}
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


def get_notification_prefs(user_id) -> dict:
    """Returns the agency's notification toggle dict, defaulting all to True if unset."""
    import json
    defaults = {'weekly_digest': True, 'lead_alerts': True, 'billing_alerts': True}
    user = get_user_by_id(user_id)
    if not user or not user.get('notification_prefs'):
        return defaults
    try:
        stored = json.loads(user['notification_prefs'])
        return {**defaults, **stored}
    except (ValueError, TypeError):
        return defaults


def update_notification_prefs(user_id, prefs: dict) -> bool:
    """Merge-update notification toggles (weekly_digest, lead_alerts, billing_alerts)."""
    import json
    current = get_notification_prefs(user_id)
    merged = {**current, **{k: bool(v) for k, v in (prefs or {}).items()}}
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute(
            'UPDATE users SET notification_prefs = %s WHERE id = %s',
            (json.dumps(merged), user_id)
        )
        conn.commit()
        return True
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[update_notification_prefs] {e}')
        if conn:
            try: conn.rollback()
            except Exception: pass
        return False
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


# =====================================================================
# ACCOUNT DELETION (soft delete with grace period, then hard delete)
# =====================================================================
# Soft delete: sets deletion_requested_at + scheduled_hard_delete_at.
# The account is NOT blocked from logging in during the grace period
# on purpose — this lets the agency notice and cancel if it was a
# mistake. A daily cron job (blueprints/cron.py::cron_hard_delete_accounts)
# permanently processes accounts once scheduled_hard_delete_at has passed.
#
# NOTE on hard_delete_user(): this scrubs PII on the users row and removes
# the agency's clients (their most directly-owned data). It does NOT audit
# or cascade every one of the ~70 tables in this schema — session records,
# billing/webhook logs, and other tables referencing client_id or user_id
# indirectly were not individually verified for this pass. Treat this as a
# strong first layer (satisfies "the account and its clients are gone and
# PII is scrubbed"), not a fully audited right-to-be-forgotten guarantee,
# until each related table has been reviewed.

def request_account_deletion(user_id, reason: str = None) -> dict:
    """Start the soft-delete grace period. Returns the scheduled hard-delete date."""
    from datetime import datetime, timedelta
    from constants import ACCOUNT_DELETION_GRACE_DAYS
    now = datetime.utcnow()
    scheduled = now + timedelta(days=ACCOUNT_DELETION_GRACE_DAYS)
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute(
            """UPDATE users
               SET deletion_requested_at = %s, scheduled_hard_delete_at = %s,
                   deletion_reason = %s
               WHERE id = %s""",
            (now, scheduled, (reason or '')[:500], user_id)
        )
        conn.commit()
        return {'success': True, 'scheduled_hard_delete_at': scheduled.isoformat()}
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[request_account_deletion] {e}')
        if conn:
            try: conn.rollback()
            except Exception: pass
        return {'success': False, 'error': 'Could not schedule deletion.'}
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


def cancel_account_deletion(user_id) -> bool:
    """Undo a pending soft delete — available any time before the hard delete runs."""
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute(
            """UPDATE users
               SET deletion_requested_at = NULL, scheduled_hard_delete_at = NULL,
                   deletion_reason = NULL
               WHERE id = %s""",
            (user_id,)
        )
        conn.commit()
        return True
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[cancel_account_deletion] {e}')
        if conn:
            try: conn.rollback()
            except Exception: pass
        return False
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


def get_pending_deletion(user_id) -> dict:
    """Returns {'pending': bool, 'scheduled_hard_delete_at': iso_str|None, 'days_left': int|None}."""
    from datetime import datetime
    user = get_user_by_id(user_id)
    if not user or not user.get('scheduled_hard_delete_at'):
        return {'pending': False, 'scheduled_hard_delete_at': None, 'days_left': None}
    scheduled = user['scheduled_hard_delete_at']
    days_left = max((scheduled - datetime.utcnow()).days, 0)
    return {'pending': True, 'scheduled_hard_delete_at': scheduled.isoformat(), 'days_left': days_left}


def get_users_due_for_hard_delete() -> list:
    """Accounts whose grace period has expired — used by the daily cron job."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            """SELECT id, email FROM users
               WHERE scheduled_hard_delete_at IS NOT NULL
                 AND scheduled_hard_delete_at <= CURRENT_TIMESTAMP"""
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[get_users_due_for_hard_delete] {e}')
        return []


def hard_delete_user(user_id) -> dict:
    """
    Permanently processes one account past its grace period: deletes the
    agency's clients (cascades to that client's own data via each table's
    existing FK behavior), then scrubs PII on the users row and marks it
    permanently deleted rather than removing the row outright (keeps
    billing/audit history referencing this user_id intact and non-orphaned).
    See the module-level note above on cascade coverage.
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute('SELECT client_id FROM clients WHERE user_id = %s', (user_id,))
        client_ids = [r['client_id'] for r in cursor.fetchall()]
        for cid in client_ids:
            cursor.execute('DELETE FROM clients WHERE client_id = %s', (cid,))

        cursor.execute(
            """UPDATE users
               SET email = %s, password_hash = 'deleted', company_name = NULL,
                   logo_url = NULL, contact_phone = NULL, notification_prefs = '{}',
                   google_id = NULL, is_admin = FALSE, subscription_status = 'deleted'
               WHERE id = %s""",
            (f'deleted-user-{user_id}@lumvi.invalid', user_id)
        )
        conn.commit()
        return {'success': True, 'client_ids_deleted': client_ids}
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[hard_delete_user] user={user_id}: {e}')
        if conn:
            try: conn.rollback()
            except Exception: pass
        return {'success': False, 'error': str(e)}
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass

