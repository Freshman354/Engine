"""
models/billing.py
-----------------
Subscription lifecycle, payment recording, admin billing operations,
and agency overage seat recording.
"""
import json
import uuid
from datetime import datetime
from .db import get_db

def update_user_subscription(user_id, plan_type, billing_provider='flutterwave',
                              subscription_id=None, is_annual=False):
    """
    Upgrade a user to a paid plan and set recurring subscription fields.
    Called after a successful payment callback.
    """
    cycle = 'annual' if is_annual else 'monthly'
    days  = 365 if is_annual else 30
    conn, cursor = get_db()
    try:
        cursor.execute(
            '''UPDATE users
               SET plan_type            = %s,
                   billing_provider     = %s,
                   subscription_id      = %s,
                   billing_cycle        = %s,
                   is_annual            = %s,
                   cancel_at_period_end = FALSE,
                   upgraded_at          = CURRENT_TIMESTAMP,
                   subscription_expires_at = NOW() + INTERVAL %s,
                   grace_period_ends_at    = NOW() + INTERVAL %s
               WHERE id = %s''',
            (plan_type, billing_provider, subscription_id, cycle,
             is_annual, f'{days} days', f'{days + 3} days', user_id)
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def cancel_user_subscription(user_id):
    """
    Mark a subscription to cancel at period end.
    The user keeps access until subscription_expires_at, then the
    scheduler downgrades them automatically.
    Returns True on success.
    """
    conn, cursor = get_db()
    try:
        cursor.execute(
            '''UPDATE users
               SET cancel_at_period_end = TRUE
               WHERE id = %s AND is_admin IS NOT TRUE''',
            (user_id,)
        )
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        conn.rollback()
        cursor.close()
        conn.close()
        return False



def set_subscription_expiry(user_id):
    """Set subscription_expires_at to 30 days from now and grace to 33 days."""
    conn, cursor = get_db()
    cursor.execute(
        '''UPDATE users
           SET subscription_expires_at = NOW() + INTERVAL '30 days',
               grace_period_ends_at    = NOW() + INTERVAL '33 days'
           WHERE id = %s''',
        (user_id,)
    )
    conn.commit()
    cursor.close()
    conn.close()


def downgrade_expired_users():
    """
    Downgrade all non-admin paid users whose subscription has expired.

    Two conditions trigger a downgrade:
      A) grace_period_ends_at IS NOT NULL AND grace_period_ends_at < NOW()
         → normal path: grace period has elapsed
      B) subscription_expires_at IS NOT NULL AND subscription_expires_at < NOW()
         AND grace_period_ends_at IS NULL
         → legacy path: users who signed up before the grace column existed,
           or whose grace was never set. They get downgraded immediately when
           subscription_expires_at passes.

    Admin users (is_admin = TRUE) are always skipped. 'enterprise' no longer
    has a standing exclusion here — it's an obsolete plan_type now
    (see migrate_ai_employee_plan_rename); any account still found on it
    downgrades to 'free' like any other expired paid plan.
    Returns list of user dicts that were downgraded.
    """
    conn, cursor = get_db()
    try:
        cursor.execute(
            '''SELECT id, email, plan_type FROM users
               WHERE plan_type != 'free'
                 AND (is_admin IS NOT TRUE)
                 AND (
                   -- Normal: grace period has passed
                   (grace_period_ends_at IS NOT NULL AND grace_period_ends_at < NOW())
                   OR
                   -- Legacy: no grace set but subscription has expired
                   (grace_period_ends_at IS NULL
                    AND subscription_expires_at IS NOT NULL
                    AND subscription_expires_at < NOW())
                 )'''
        )
        to_downgrade = cursor.fetchall()

        if to_downgrade:
            cursor.execute(
                '''UPDATE users
                   SET plan_type               = 'free',
                       subscription_expires_at = NULL,
                       grace_period_ends_at    = NULL
                   WHERE plan_type != 'free'
                     AND (is_admin IS NOT TRUE)
                     AND (
                       (grace_period_ends_at IS NOT NULL AND grace_period_ends_at < NOW())
                       OR
                       (grace_period_ends_at IS NULL
                        AND subscription_expires_at IS NOT NULL
                        AND subscription_expires_at < NOW())
                     )'''
            )
            conn.commit()

        return [dict(u) for u in to_downgrade]
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def downgrade_single_user(user_id):
    """Immediately downgrade one user to free plan."""
    conn, cursor = get_db()
    cursor.execute(
        '''UPDATE users
           SET plan_type               = 'free',
               subscription_expires_at = NULL,
               grace_period_ends_at    = NULL
           WHERE id = %s AND is_admin IS NOT TRUE''',
        (user_id,)
    )
    conn.commit()
    cursor.close()
    conn.close()


# =====================================================================
# PLAN ENFORCEMENT
# =====================================================================

def track_event(event_name, user_id=None, metadata=None):
    """
    Log a named event to analytics_events.
    Fails silently so it never disrupts the main request.
    Usage: track_event('login', user_id=5, metadata={'plan': 'pro'})
    """
    try:
        conn, cursor = get_db()
        cursor.execute(
            'INSERT INTO analytics_events (user_id, event_name, metadata) VALUES (%s, %s, %s)',
            (user_id, event_name, json.dumps(metadata) if metadata else None)
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception:
        pass


# =====================================================================
# PAYMENT FUNCTIONS
# =====================================================================

def record_payment(user_id, amount, plan_type, provider='manual', currency='USD',
                   status='completed', reference=None, notes=None, payment_date=None):
    """Insert a payment record and return its id."""
    conn, cursor = get_db()
    if payment_date:
        cursor.execute(
            '''INSERT INTO payments
               (user_id, amount, currency, status, provider, plan_type, reference, notes, payment_date)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id''',
            (user_id, amount, currency, status, provider, plan_type, reference, notes, payment_date)
        )
    else:
        cursor.execute(
            '''INSERT INTO payments
               (user_id, amount, currency, status, provider, plan_type, reference, notes)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id''',
            (user_id, amount, currency, status, provider, plan_type, reference, notes)
        )
    payment_id = cursor.fetchone()['id']
    conn.commit()
    cursor.close()
    conn.close()
    return payment_id


def get_all_payments(limit=200):
    """Get recent payments joined with user email."""
    conn, cursor = get_db()
    cursor.execute(
        '''SELECT p.*, u.email
           FROM payments p
           JOIN users u ON p.user_id = u.id
           ORDER BY p.payment_date DESC
           LIMIT %s''',
        (limit,)
    )
    rows = [dict(r) for r in cursor.fetchall()]
    cursor.close()
    conn.close()
    for r in rows:
        if r.get('payment_date'):
            r['payment_date'] = r['payment_date'].isoformat()
    return rows


def get_mrr():
    """Sum completed payments in the current calendar month."""
    conn, cursor = get_db()
    cursor.execute(
        """SELECT COALESCE(SUM(amount), 0) AS mrr
           FROM payments
           WHERE status = 'completed'
             AND DATE_TRUNC('month', payment_date) = DATE_TRUNC('month', CURRENT_DATE)"""
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return float(row['mrr']) if row else 0.0


def get_total_revenue():
    """Sum of all completed payments ever."""
    conn, cursor = get_db()
    cursor.execute("SELECT COALESCE(SUM(amount), 0) AS total FROM payments WHERE status = 'completed'")
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return float(row['total']) if row else 0.0


def get_revenue_by_month(months=6):
    """Monthly revenue totals for the last N months."""
    conn, cursor = get_db()
    cursor.execute(
        """SELECT TO_CHAR(DATE_TRUNC('month', payment_date), 'Mon YYYY') AS month,
                  DATE_TRUNC('month', payment_date) AS month_date,
                  COALESCE(SUM(amount), 0) AS revenue
           FROM payments
           WHERE status = 'completed'
             AND payment_date >= CURRENT_DATE - (INTERVAL '1 month' * %s)
           GROUP BY DATE_TRUNC('month', payment_date)
           ORDER BY month_date ASC""",
        (months,)
    )
    rows = [{'month': r['month'], 'revenue': float(r['revenue'])} for r in cursor.fetchall()]
    cursor.close()
    conn.close()
    return rows


# =====================================================================
# ADMIN USER FUNCTIONS
# =====================================================================

def get_all_users(limit=500):
    """All users for admin panel, newest first."""
    conn, cursor = get_db()
    cursor.execute(
        '''SELECT id, email, plan_type, subscription_status, is_admin,
                  billing_provider, billing_cycle, is_annual,
                  subscription_id, cancel_at_period_end,
                  subscription_expires_at, grace_period_ends_at,
                  created_at, upgraded_at, cancelled_at
           FROM users
           ORDER BY created_at DESC
           LIMIT %s''',
        (limit,)
    )
    rows = [dict(r) for r in cursor.fetchall()]
    cursor.close()
    conn.close()
    for r in rows:
        for col in ('created_at', 'upgraded_at', 'cancelled_at',
                    'subscription_expires_at', 'grace_period_ends_at'):
            if r.get(col):
                r[col] = r[col].isoformat()
    return rows
def record_agency_overage_seat(user_id: int, client_id: str, seat_num: int):
    """
    Record that a newly created client is an overage seat for an agency user.
    Upserts so re-runs are safe.
    """
    conn, cursor = get_db()
    try:
        cursor.execute("""
            INSERT INTO agency_overage_seats (user_id, client_id, seat_num)
            VALUES (%s, %s, %s)
            ON CONFLICT (client_id) DO UPDATE
              SET seat_num = EXCLUDED.seat_num
        """, (user_id, client_id, seat_num))
        conn.commit()
    except Exception as e:
        conn.rollback()
        import logging
        logging.getLogger(__name__).error(f"[record_agency_overage_seat] {e}")
    finally:
        cursor.close()
        conn.close()


def get_agency_users_with_overage(included_clients: int = 20):
    """
    Return all agency users whose active client count exceeds included_clients.
    Used by the monthly billing cron.
    Returns list of dicts: { id, email, client_count }.
    """
    conn, cursor = get_db()
    try:
        cursor.execute("""
            SELECT u.id, u.email, COUNT(c.client_id) AS client_count
            FROM users u
            JOIN clients c ON c.user_id = u.id AND c.is_active = TRUE
            WHERE u.plan_type IN ('agency', 'growth')
              AND u.subscription_status = 'active'
              AND u.subscription_id IS NOT NULL
            GROUP BY u.id, u.email
            HAVING COUNT(c.client_id) > %s
            ORDER BY client_count DESC
        """, (included_clients,))
        return [dict(r) for r in cursor.fetchall()]
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[get_agency_users_with_overage] {e}")
        return []
    finally:
        cursor.close()
        conn.close()


def get_agency_overage_summary(user_id: int, included_clients: int = 20):
    """
    Return a summary of overage seats for a specific agency user.
    Used to show live cost in the dashboard.
    Returns { client_count, extra_seats, overage_cost_per_month } or None.
    """
    conn, cursor = get_db()
    try:
        cursor.execute("""
            SELECT COUNT(*) AS client_count
            FROM clients
            WHERE user_id = %s AND is_active = TRUE
        """, (user_id,))
        row = cursor.fetchone()
        client_count = int(row['client_count']) if row else 0
        extra_seats  = max(0, client_count - included_clients)
        return {
            'client_count':          client_count,
            'included_clients':      included_clients,
            'extra_seats':           extra_seats,
            'overage_cost_per_month': extra_seats * 15.0,
        }
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[get_agency_overage_summary] {e}")
        return None
    finally:
        cursor.close()
        conn.close()


# =====================================================================
# AGENCY OVERAGE PAYMENT TRACKING
# =====================================================================

def set_overage_pending(user_id: int, amount: float, due_date,
                        tx_ref: str, payment_link: str) -> None:
    """
    Stamp an outstanding overage invoice on the user row so the
    client-creation route and the enforcement cron can read it cheaply
    without a JOIN to the payments table.

    Called by bill_agency_overages() in cron.py after the Flutterwave
    payment link is generated.
    """
    conn, cursor = get_db()
    try:
        cursor.execute(
            """UPDATE users
               SET overage_amount_due     = %s,
                   overage_due_date       = %s,
                   overage_payment_status = 'pending',
                   overage_tx_ref         = %s,
                   overage_payment_link   = %s
               WHERE id = %s""",
            (amount, due_date, tx_ref, payment_link, user_id)
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        import logging
        logging.getLogger(__name__).error(f"[set_overage_pending] user={user_id}: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def mark_overage_paid(user_id: int, reference: str = None) -> None:
    """
    Clear the overage hold after a successful payment.
    Called by both the Flutterwave callback and the server webhook when
    tx_ref starts with 'lumvi_overage_'.
    """
    conn, cursor = get_db()
    try:
        cursor.execute(
            """UPDATE users
               SET overage_amount_due     = 0,
                   overage_due_date       = NULL,
                   overage_payment_status = 'paid',
                   overage_tx_ref         = NULL,
                   overage_payment_link   = NULL
               WHERE id = %s""",
            (user_id,)
        )
        conn.commit()
        import logging
        logging.getLogger(__name__).info(
            f"[mark_overage_paid] user={user_id} ref={reference}"
        )
    except Exception as e:
        conn.rollback()
        import logging
        logging.getLogger(__name__).error(f"[mark_overage_paid] user={user_id}: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def mark_overdue_overage_users() -> list:
    """
    Flip overage_payment_status → 'overdue' for every agency user whose
    invoice due date has passed without payment.

    Called daily by /cron/enforce-agency-overages.
    Returns a list of dicts (id, email) for logging.
    """
    conn, cursor = get_db()
    try:
        cursor.execute(
            """SELECT id, email FROM users
               WHERE overage_payment_status = 'pending'
                 AND overage_due_date IS NOT NULL
                 AND overage_due_date < NOW()
                 AND (is_admin IS NOT TRUE)"""
        )
        to_mark = cursor.fetchall()

        if to_mark:
            cursor.execute(
                """UPDATE users
                   SET overage_payment_status = 'overdue'
                   WHERE overage_payment_status = 'pending'
                     AND overage_due_date IS NOT NULL
                     AND overage_due_date < NOW()
                     AND (is_admin IS NOT TRUE)"""
            )
            conn.commit()

        return [dict(u) for u in to_mark]
    except Exception as e:
        conn.rollback()
        import logging
        logging.getLogger(__name__).error(f"[mark_overdue_overage_users] {e}")
        return []
    finally:
        cursor.close()
        conn.close()



# =====================================================================
# SEAT SUBSCRIPTION FUNCTIONS
# =====================================================================

import calendar as _calendar
from datetime import date as _date

SEAT_PACKAGES = {
    'single': {'seats': 1, 'monthly': 15.00},
    'bundle': {'seats': 5, 'monthly': 60.00},
}


def calculate_seat_proration(package_type='single', purchase_date=None):
    """
    Calculate prorated first payment for a seat subscription.

    Returns (prorated_amount, next_billing_date, seat_count, monthly_amount).

    The prorated amount covers the remaining days in the current month
    so renewals always land cleanly on the 1st.
    """
    today        = purchase_date or _date.today()
    package      = SEAT_PACKAGES[package_type]
    days_in_month   = _calendar.monthrange(today.year, today.month)[1]
    days_remaining  = days_in_month - today.day + 1
    prorated        = round((days_remaining / days_in_month) * package['monthly'], 2)
    if today.month == 12:
        next_billing = _date(today.year + 1, 1, 1)
    else:
        next_billing = _date(today.year, today.month + 1, 1)
    return prorated, next_billing, package['seats'], package['monthly']


def get_active_seat_count(agency_id: int) -> int:
    """
    Return total extra seats available from active subscriptions.
    Used by clone_client_route() and auth.py dashboard to compute the ceiling.
    """
    conn, cursor = get_db()
    try:
        cursor.execute(
            """SELECT COALESCE(SUM(seat_count), 0) AS total
               FROM seat_subscriptions
               WHERE agency_id = %s AND status = 'active'""",
            (agency_id,)
        )
        row = cursor.fetchone()
        return int(row['total']) if row else 0
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[get_active_seat_count] {e}")
        return 0
    finally:
        cursor.close()
        conn.close()


def create_seat_subscription(agency_id: int, package_type: str,
                              first_payment: float, monthly_amount: float,
                              seat_count: int, next_billing_date, tx_ref: str) -> int:
    """
    Insert a new active seat subscription and return its id.
    Called from the Flutterwave callback after a successful initial seat payment.
    """
    conn, cursor = get_db()
    try:
        cursor.execute(
            """INSERT INTO seat_subscriptions
               (agency_id, next_billing_date, first_payment, monthly_amount,
                seat_count, package_type, status, tx_ref)
               VALUES (%s, %s, %s, %s, %s, %s, 'active', %s)
               RETURNING id""",
            (agency_id, next_billing_date, first_payment,
             monthly_amount, seat_count, package_type, tx_ref)
        )
        sub_id = cursor.fetchone()['id']
        conn.commit()
        return sub_id
    except Exception as e:
        conn.rollback()
        import logging
        logging.getLogger(__name__).error(f"[create_seat_subscription] {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def get_seats_due_today() -> list:
    """
    Return all active seat subscriptions whose next_billing_date is today or past.
    Called daily by /cron/seat-renewals.
    """
    conn, cursor = get_db()
    try:
        cursor.execute(
            """SELECT ss.id, ss.agency_id, ss.package_type, ss.monthly_amount,
                      ss.seat_count, ss.next_billing_date, u.email
               FROM seat_subscriptions ss
               JOIN users u ON u.id = ss.agency_id
               WHERE ss.status = 'active'
                 AND ss.next_billing_date <= CURRENT_DATE
               ORDER BY ss.id"""
        )
        return [dict(r) for r in cursor.fetchall()]
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[get_seats_due_today] {e}")
        return []
    finally:
        cursor.close()
        conn.close()


def renew_seat(sub_id: int, tx_ref: str) -> None:
    """
    Advance next_billing_date by one month after a successful renewal payment.
    """
    conn, cursor = get_db()
    try:
        cursor.execute(
            """UPDATE seat_subscriptions
               SET next_billing_date = (
                   CASE WHEN EXTRACT(MONTH FROM next_billing_date) = 12
                        THEN MAKE_DATE(EXTRACT(YEAR FROM next_billing_date)::INT + 1, 1, 1)
                        ELSE (DATE_TRUNC('month', next_billing_date)
                              + INTERVAL '1 month')::DATE
                   END
               ),
               tx_ref = %s
               WHERE id = %s""",
            (tx_ref, sub_id)
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        import logging
        logging.getLogger(__name__).error(f"[renew_seat] sub_id={sub_id}: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def fail_seat(sub_id: int) -> None:
    """
    Mark a seat subscription as failed after a renewal payment failure.
    The seat count is removed from get_active_seat_count() immediately.
    """
    conn, cursor = get_db()
    try:
        cursor.execute(
            "UPDATE seat_subscriptions SET status = 'failed' WHERE id = %s",
            (sub_id,)
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        import logging
        logging.getLogger(__name__).error(f"[fail_seat] sub_id={sub_id}: {e}")
    finally:
        cursor.close()
        conn.close()
