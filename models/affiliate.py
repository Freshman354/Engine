"""
models/affiliate.py
-------------------
Affiliate programme — account creation, referral tracking,
commission recording, and stats.
"""
import secrets
import uuid
from datetime import datetime
from .db import get_db

def create_affiliate(user_id, payment_email, commission_rate=0.30):
    """Create affiliate account for a user. Returns dict on success, None if already exists."""
    conn = cursor = None
    referral_code = secrets.token_hex(4).upper()
    try:
        conn, cursor = get_db()
        cursor.execute(
            '''INSERT INTO affiliates (user_id, referral_code, commission_rate, payment_email)
               VALUES (%s, %s, %s, %s) RETURNING id''',
            (user_id, referral_code, commission_rate, payment_email)
        )
        affiliate_id = cursor.fetchone()['id']
        conn.commit()
        return {'id': affiliate_id, 'referral_code': referral_code, 'commission_rate': commission_rate}
    except psycopg2.IntegrityError:
        if conn:
            try: conn.rollback()
            except Exception: pass
        return None
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[create_affiliate] {e}')
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

def get_affiliate_by_user_id(user_id):
    """Get affiliate account by user ID. Returns None on missing row or DB error."""
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute('SELECT * FROM affiliates WHERE user_id = %s', (user_id,))
        affiliate = cursor.fetchone()
        return dict(affiliate) if affiliate else None
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[get_affiliate_by_user_id] {e}')
        return None
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass

def get_affiliate_by_code(referral_code):
    """Get affiliate by referral code. Returns None on missing row or DB error."""
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute('SELECT * FROM affiliates WHERE referral_code = %s', (referral_code,))
        affiliate = cursor.fetchone()
        return dict(affiliate) if affiliate else None
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[get_affiliate_by_code] {e}')
        return None
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass

def create_referral(affiliate_id, referred_user_id, referral_code):
    """Track a new referral. Never raises."""
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute(
            '''INSERT INTO referrals (affiliate_id, referred_user_id, referral_code, status)
               VALUES (%s, %s, %s, %s)''',
            (affiliate_id, referred_user_id, referral_code, 'pending')
        )
        cursor.execute(
            'UPDATE affiliates SET total_referrals = total_referrals + 1 WHERE id = %s',
            (affiliate_id,)
        )
        conn.commit()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[create_referral] {e}')
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

def create_commission(affiliate_id, referred_user_id, subscription_amount, plan_type):
    """Create commission record when referred user pays. Never raises."""
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute('SELECT commission_rate FROM affiliates WHERE id = %s', (affiliate_id,))
        result = cursor.fetchone()
        commission_rate = result['commission_rate'] if result else 0.30
        commission_amount = subscription_amount * commission_rate
        cursor.execute(
            '''INSERT INTO commissions (affiliate_id, referred_user_id, amount, subscription_amount, plan_type, status)
               VALUES (%s, %s, %s, %s, %s, %s)''',
            (affiliate_id, referred_user_id, commission_amount, subscription_amount, plan_type, 'pending')
        )
        cursor.execute(
            'UPDATE affiliates SET total_earnings = total_earnings + %s WHERE id = %s',
            (commission_amount, affiliate_id)
        )
        cursor.execute(
            '''UPDATE referrals SET status = 'converted', converted_at = CURRENT_TIMESTAMP
               WHERE affiliate_id = %s AND referred_user_id = %s''',
            (affiliate_id, referred_user_id)
        )
        conn.commit()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[create_commission] {e}')
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

def get_affiliate_stats(affiliate_id):
    """Get affiliate's statistics. Returns None if affiliate not found, raises on other errors."""
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute('SELECT * FROM affiliates WHERE id = %s', (affiliate_id,))
        row = cursor.fetchone()
        if row is None:
            return None   # BUG-04 fix: fetchone() can return None
        affiliate = dict(row)
        cursor.execute(
            'SELECT status, COUNT(*) as count FROM referrals WHERE affiliate_id = %s GROUP BY status',
            (affiliate_id,)
        )
        referral_stats = {row['status']: row['count'] for row in cursor.fetchall()}
        cursor.execute(
            "SELECT SUM(amount) as pending FROM commissions WHERE affiliate_id = %s AND status = 'pending'",
            (affiliate_id,)
        )
        pending_result = cursor.fetchone()
        pending_earnings = (pending_result.get('pending') or 0) if pending_result else 0
        cursor.execute(
            "SELECT SUM(amount) as paid FROM commissions WHERE affiliate_id = %s AND status = 'paid'",
            (affiliate_id,)
        )
        paid_result = cursor.fetchone()
        paid_earnings = (paid_result.get('paid') or 0) if paid_result else 0
        return {
            'affiliate': affiliate,
            'referral_stats': referral_stats,
            'pending_earnings': pending_earnings,
            'paid_earnings': paid_earnings,
            'total_earnings': affiliate['total_earnings'],
        }
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[get_affiliate_stats] {e}')
        return None
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass

def get_affiliate_commissions(affiliate_id):
    """Get all commissions for an affiliate. Returns [] on DB error."""
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute(
            '''SELECT c.*, u.email as referred_email
               FROM commissions c
               JOIN users u ON c.referred_user_id = u.id
               WHERE c.affiliate_id = %s
               ORDER BY c.created_at DESC''',
            (affiliate_id,)
        )
        return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[get_affiliate_commissions] {e}')
        return []
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass



# =====================================================================
# ADMIN MIGRATIONS
# =====================================================================

