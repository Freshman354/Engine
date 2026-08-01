"""
models/analytics.py
-------------------
Platform analytics — revenue, user growth, conversion funnel,
API cost tracking, DB stats, and admin-level reporting queries.
"""
import json
from datetime import datetime
from .db import get_db
from .billing import get_all_users

# ── Per-token pricing, by provider ─────────────────────────────────────────
# Source: provider list prices. OpenRouter aggregates multiple hosting
# providers for the same model at potentially different rates — this is
# their listed default, not a guarantee of the exact rate every request
# routes to. Verify at openrouter.ai/models if this needs to be precise
# for billing (not just directional cost tracking).
# ── Per-token pricing, by model ─────────────────────────────────────────────
# Priced by the model that ACTUALLY answered each call (stored per-row),
# not by whatever the live provider switch happens to be set to right now.
# Using the live switch here would retroactively reprice historical rows
# any time an agency toggles it — e.g. every Gemini-era row would suddenly
# show OpenRouter's rate after a switch. Source: provider list prices.
# OpenRouter aggregates multiple hosting providers for the same model at
# potentially different rates — this is their listed default, not a
# guarantee of the exact rate every request routes to.
_PRICING_PER_TOKEN = {
    'gemini': {
        'input':  0.075 / 1_000_000,
        'output': 0.300 / 1_000_000,
    },
    'openrouter': {  # meta-llama/llama-4-maverick, checked 2026-07-02
        'input':  0.15 / 1_000_000,
        'output': 0.60 / 1_000_000,
    },
}


def _rates_for_model(model: str) -> dict:
    m = (model or '').lower()
    if 'llama' in m or 'openrouter' in m:
        return _PRICING_PER_TOKEN['openrouter']
    return _PRICING_PER_TOKEN['gemini']


def _calc_cost(input_tokens, output_tokens, model: str = 'gemini'):
    """
    Back-compat signature for any caller still passing raw token sums
    without a per-row model (aggregate queries migrated to SUM(cost)
    instead — see log_api_usage). Prefer storing cost at insert time.
    """
    rates = _rates_for_model(model)
    return (
        (input_tokens  or 0) * rates['input'] +
        (output_tokens or 0) * rates['output']
    )


def get_user_count_by_plan():
    """Users grouped by plan_type."""
    conn, cursor = get_db()
    cursor.execute(
        'SELECT plan_type, COUNT(*) AS cnt FROM users GROUP BY plan_type ORDER BY cnt DESC'
    )
    rows = {r['plan_type']: int(r['cnt']) for r in cursor.fetchall()}
    cursor.close()
    conn.close()
    return rows


def get_new_users_this_month():
    """Count signups in the current calendar month."""
    conn, cursor = get_db()
    cursor.execute(
        """SELECT COUNT(*) AS cnt FROM users
           WHERE DATE_TRUNC('month', created_at) = DATE_TRUNC('month', CURRENT_DATE)"""
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return int(row['cnt']) if row else 0


def get_user_growth_by_month(months=6):
    """New signups per month for the last N months."""
    conn, cursor = get_db()
    cursor.execute(
        """SELECT TO_CHAR(DATE_TRUNC('month', created_at), 'Mon YYYY') AS month,
                  DATE_TRUNC('month', created_at) AS month_date,
                  COUNT(*) AS count
           FROM users
           WHERE created_at >= CURRENT_DATE - (INTERVAL '1 month' * %s)
           GROUP BY DATE_TRUNC('month', created_at)
           ORDER BY month_date ASC""",
        (months,)
    )
    rows = [{'month': r['month'], 'count': int(r['count'])} for r in cursor.fetchall()]
    cursor.close()
    conn.close()
    return rows


def admin_update_user(user_id, plan_type=None, subscription_status=None, is_admin=None, grace_days=None):
    """
    Update user plan, subscription_status, or admin flag.

    grace_days: when plan_type is being set to a PAID plan, how many days
    until it should auto-downgrade if nothing else renews it. This exists
    because downgrade_expired_users() (the daily cron) only acts on users
    with a set grace_period_ends_at/subscription_expires_at in the past —
    without this, a manually-granted plan_type has no expiry at all and
    silently becomes permanent, regardless of the admin's intent, since
    neither of the cron's two trigger conditions can ever become true.

    - grace_days=None (default) and plan_type is paid: defaults to 30 days,
      NOT permanent — permanent-by-default was the bug being fixed here.
    - grace_days=0: explicitly permanent (no auto-downgrade) — sets both
      expiry fields to NULL. Use this deliberately, not by omission.
    - plan_type == 'free' or 'enterprise': both expiry fields are cleared
      regardless of grace_days (free has nothing to expire from;
      enterprise is excluded from the downgrade query entirely).
    """
    conn, cursor = get_db()
    updates = []
    params = []
    if plan_type is not None:
        updates.append('plan_type = %s')
        params.append(plan_type)
        updates.append('upgraded_at = CURRENT_TIMESTAMP')

        if plan_type in ('free', 'enterprise'):
            updates.append('grace_period_ends_at = NULL')
            updates.append('subscription_expires_at = NULL')
        elif grace_days == 0:
            # Deliberate, explicit permanent grant.
            updates.append('grace_period_ends_at = NULL')
            updates.append('subscription_expires_at = NULL')
        else:
            days = grace_days if grace_days is not None else 30
            updates.append('grace_period_ends_at = CURRENT_TIMESTAMP + make_interval(days => %s)')
            params.append(int(days))
    if subscription_status is not None:
        updates.append('subscription_status = %s')
        params.append(subscription_status)
        if subscription_status == 'cancelled':
            updates.append('cancelled_at = CURRENT_TIMESTAMP')
    if is_admin is not None:
        updates.append('is_admin = %s')
        params.append(bool(is_admin))
    if not updates:
        cursor.close()
        conn.close()
        return False
    params.append(user_id)
    cursor.execute('UPDATE users SET ' + ', '.join(updates) + ' WHERE id = %s', params)
    conn.commit()
    cursor.close()
    conn.close()
    return True


def admin_delete_user(user_id):
    """Hard-delete a user and cascade all their data."""
    conn, cursor = get_db()
    try:
        cursor.execute('SELECT client_id FROM clients WHERE user_id = %s', (user_id,))
        client_ids = [r['client_id'] for r in cursor.fetchall()]
        for cid in client_ids:
            cursor.execute('DELETE FROM conversations WHERE client_id = %s', (cid,))
            cursor.execute('DELETE FROM leads WHERE client_id = %s', (cid,))
            cursor.execute('DELETE FROM faqs WHERE client_id = %s', (cid,))
        cursor.execute('DELETE FROM clients WHERE user_id = %s', (user_id,))
        cursor.execute('DELETE FROM commissions WHERE referred_user_id = %s', (user_id,))
        cursor.execute('DELETE FROM referrals WHERE referred_user_id = %s', (user_id,))
        cursor.execute('DELETE FROM affiliates WHERE user_id = %s', (user_id,))
        cursor.execute('DELETE FROM payments WHERE user_id = %s', (user_id,))
        cursor.execute('DELETE FROM analytics_events WHERE user_id = %s', (user_id,))
        cursor.execute('DELETE FROM users WHERE id = %s', (user_id,))
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def get_all_leads_admin(limit=500, client_id_filter=None, search=None):
    """Leads across all clients for admin view."""
    conn, cursor = get_db()
    query = '''SELECT l.*, c.company_name, u.email as owner_email
               FROM leads l
               LEFT JOIN clients c ON l.client_id = c.client_id
               LEFT JOIN users u ON c.user_id = u.id
               WHERE 1=1'''
    params = []
    if client_id_filter:
        query += ' AND l.client_id = %s'
        params.append(client_id_filter)
    if search:
        query += ' AND (l.name ILIKE %s OR l.email ILIKE %s)'
        params.extend(['%' + search + '%', '%' + search + '%'])
    query += ' ORDER BY l.created_at DESC LIMIT %s'
    params.append(limit)
    cursor.execute(query, params)
    rows = [dict(r) for r in cursor.fetchall()]
    cursor.close()
    conn.close()
    for r in rows:
        if r.get('created_at'):
            r['created_at'] = r['created_at'].isoformat()
    return rows


def log_api_usage(user_id, client_id, input_tokens, output_tokens,
                  model='gemini-2.0-flash', endpoint=None):
    """Log one AI generation call's token usage for cost tracking. Never raises."""
    try:
        conn, cursor = get_db()
        cost = _calc_cost(input_tokens, output_tokens, model=model)
        cursor.execute(
            """INSERT INTO api_usage_log
                   (user_id, client_id, model, input_tokens, output_tokens, cost, endpoint)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (user_id, client_id, model,
             int(input_tokens or 0), int(output_tokens or 0), cost, endpoint)
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        import logging
        logging.getLogger(__name__).debug(f"[log_api_usage] {e}")


def get_api_cost_summary():
    _zero = {'cost_today': 0.0, 'cost_this_month': 0.0, 'cost_all_time': 0.0,
             'tokens_today': 0, 'tokens_this_month': 0}
    try:
        conn, cursor = get_db()
        cursor.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN DATE_TRUNC('day',   created_at)=DATE_TRUNC('day',   NOW()) THEN cost END),0) AS cost_today,
                COALESCE(SUM(CASE WHEN DATE_TRUNC('month', created_at)=DATE_TRUNC('month', NOW()) THEN cost END),0) AS cost_month,
                COALESCE(SUM(cost),0) AS cost_all,
                COALESCE(SUM(CASE WHEN DATE_TRUNC('day',   created_at)=DATE_TRUNC('day',   NOW()) THEN input_tokens + output_tokens END),0) AS tok_today,
                COALESCE(SUM(CASE WHEN DATE_TRUNC('month', created_at)=DATE_TRUNC('month', NOW()) THEN input_tokens + output_tokens END),0) AS tok_month
            FROM api_usage_log
        """)
        r = cursor.fetchone()
        cursor.close()
        conn.close()
        if not r:
            return _zero
        return {
            'cost_today':        float(r['cost_today']),
            'cost_this_month':   float(r['cost_month']),
            'cost_all_time':     float(r['cost_all']),
            'tokens_today':      int(r['tok_today']),
            'tokens_this_month': int(r['tok_month']),
        }
    except Exception:
        return _zero


def get_top_chatbots_by_cost(months=1, limit=10):
    try:
        conn, cursor = get_db()
        cursor.execute("""
            SELECT a.client_id, c.company_name, u.email AS owner_email,
                   COALESCE(SUM(a.input_tokens),0)  AS input_tokens,
                   COALESCE(SUM(a.output_tokens),0) AS output_tokens,
                   COALESCE(SUM(a.cost),0)           AS est_cost
            FROM api_usage_log a
            LEFT JOIN clients c ON a.client_id = c.client_id
            LEFT JOIN users  u ON c.user_id    = u.id
            WHERE DATE_TRUNC('month', a.created_at) = DATE_TRUNC('month', NOW())
            GROUP BY a.client_id, c.company_name, u.email
            ORDER BY SUM(a.cost) DESC
            LIMIT %s
        """, (limit,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        result = []
        for r in rows:
            result.append({'client_id': r['client_id'], 'company_name': r['company_name'] or r['client_id'],
                           'owner_email': r['owner_email'] or '—', 'input_tokens': int(r['input_tokens']),
                           'output_tokens': int(r['output_tokens']), 'est_cost': float(r['est_cost'])})
        return result
    except Exception:
        return []


def get_user_cost_breakdown():
    try:
        conn, cursor = get_db()
        cursor.execute("""
            SELECT u.id AS user_id, u.email, u.plan_type,
                   COALESCE(SUM(a.input_tokens),0)  AS input_tokens,
                   COALESCE(SUM(a.output_tokens),0) AS output_tokens,
                   COALESCE(SUM(a.cost),0)           AS ai_cost
            FROM api_usage_log a
            JOIN users u ON a.user_id = u.id
            WHERE DATE_TRUNC('month', a.created_at) = DATE_TRUNC('month', NOW())
            GROUP BY u.id, u.email, u.plan_type
            ORDER BY SUM(a.cost) DESC
        """)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return [{'user_id': r['user_id'], 'email': r['email'], 'plan_type': r['plan_type'],
                 'ai_cost': float(r['ai_cost'])} for r in rows]
    except Exception:
        return []


def get_user_ai_costs_dict():
    try:
        conn, cursor = get_db()
        cursor.execute("""
            SELECT user_id, COALESCE(SUM(cost),0) AS cost
            FROM api_usage_log
            WHERE DATE_TRUNC('month', created_at) = DATE_TRUNC('month', NOW()) AND user_id IS NOT NULL
            GROUP BY user_id
        """)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return {int(r['user_id']): float(r['cost']) for r in rows}
    except Exception:
        return {}


def get_cost_revenue_by_month(months=6):
    try:
        conn, cursor = get_db()
        cursor.execute("""
            SELECT TO_CHAR(DATE_TRUNC('month', payment_date),'Mon YYYY') AS month,
                   DATE_TRUNC('month', payment_date) AS month_dt,
                   COALESCE(SUM(amount),0) AS revenue
            FROM payments WHERE status='completed' AND payment_date >= NOW()-(INTERVAL '1 month'*%s)
            GROUP BY DATE_TRUNC('month', payment_date) ORDER BY month_dt
        """, (months,))
        rev = {r['month_dt']: {'month': r['month'], 'revenue': float(r['revenue']), 'cost': 0.0}
               for r in cursor.fetchall()}
        cursor.execute("""
            SELECT TO_CHAR(DATE_TRUNC('month', created_at),'Mon YYYY') AS month,
                   DATE_TRUNC('month', created_at) AS month_dt,
                   COALESCE(SUM(cost),0) AS cost
            FROM api_usage_log WHERE created_at >= NOW()-(INTERVAL '1 month'*%s)
            GROUP BY DATE_TRUNC('month', created_at) ORDER BY month_dt
        """, (months,))
        for r in cursor.fetchall():
            cost = float(r['cost'])
            if r['month_dt'] in rev:
                rev[r['month_dt']]['cost'] = cost
            else:
                rev[r['month_dt']] = {'month': r['month'], 'revenue': 0.0, 'cost': cost}
        cursor.close()
        conn.close()
        return sorted(rev.values(), key=lambda x: x['month'])
    except Exception:
        return []


def get_daily_burn_last_30():
    try:
        conn, cursor = get_db()
        cursor.execute("""
            SELECT TO_CHAR(DATE_TRUNC('day', created_at),'DD Mon') AS date,
                   DATE_TRUNC('day', created_at) AS day_dt,
                   COALESCE(SUM(cost),0) AS cost
            FROM api_usage_log WHERE created_at >= NOW()-INTERVAL '30 days'
            GROUP BY DATE_TRUNC('day', created_at) ORDER BY day_dt
        """)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return [{'date': r['date'], 'cost': float(r['cost'])} for r in rows]
    except Exception:
        return []


def purge_old_api_logs(days=90):
    try:
        conn, cursor = get_db()
        cursor.execute("DELETE FROM api_usage_log WHERE created_at < NOW()-(INTERVAL '1 day'*%s)", (days,))
        deleted = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        return deleted
    except Exception:
        return 0


def get_db_stats():
    tables = ['users', 'clients', 'leads', 'payments', 'analytics_events',
              'conversations', 'api_usage_log', 'faqs', 'knowledge_base']
    results = []
    try:
        conn, cursor = get_db()
        for t in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) AS cnt FROM {t}")
                row = cursor.fetchone()
                results.append({'table': t, 'count': int(row['cnt']) if row else 0})
            except Exception:
                pass
        cursor.close()
        conn.close()
    except Exception:
        pass
    return results


def get_churn_this_week():
    try:
        conn, cursor = get_db()
        cursor.execute("SELECT COUNT(*) AS cnt FROM users WHERE subscription_status='cancelled' AND cancelled_at >= NOW()-INTERVAL '7 days'")
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        return int(row['cnt']) if row else 0
    except Exception:
        return 0


def get_past_due_count():
    try:
        conn, cursor = get_db()
        cursor.execute("SELECT COUNT(*) AS cnt FROM users WHERE subscription_status='past_due'")
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        return int(row['cnt']) if row else 0
    except Exception:
        return 0


def get_active_subscription_count():
    try:
        conn, cursor = get_db()
        cursor.execute("SELECT COUNT(*) AS cnt FROM users WHERE subscription_status = 'active' AND plan_type!='free'")
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        return int(row['cnt']) if row else 0
    except Exception:
        return 0


def get_paid_user_count():
    try:
        conn, cursor = get_db()
        cursor.execute("SELECT COUNT(*) AS cnt FROM users WHERE plan_type!='free'")
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        return int(row['cnt']) if row else 0
    except Exception:
        return 0


def get_free_user_count():
    try:
        conn, cursor = get_db()
        cursor.execute("SELECT COUNT(*) AS cnt FROM users WHERE plan_type='free'")
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        return int(row['cnt']) if row else 0
    except Exception:
        return 0


def get_total_client_count():
    try:
        conn, cursor = get_db()
        cursor.execute("SELECT COUNT(*) AS cnt FROM clients")
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        return int(row['cnt']) if row else 0
    except Exception:
        return 0


def get_analytics_events(limit=300):
    try:
        conn, cursor = get_db()
        cursor.execute("""
            SELECT e.event_name, e.user_id, e.metadata, e.created_at, u.email
            FROM analytics_events e LEFT JOIN users u ON e.user_id=u.id
            ORDER BY e.created_at DESC LIMIT %s
        """, (limit,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return [{'event_name': r['event_name'], 'user_id': r['user_id'], 'email': r['email'],
                 'metadata': r['metadata'],
                 'created_at': r['created_at'].isoformat() if r['created_at'] else None}
                for r in rows]
    except Exception:
        return []


def get_event_counts():
    try:
        conn, cursor = get_db()
        cursor.execute("SELECT event_name, COUNT(*) AS cnt FROM analytics_events GROUP BY event_name ORDER BY cnt DESC")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return {r['event_name']: int(r['cnt']) for r in rows}
    except Exception:
        return {}


def get_conversion_funnel(days=30):
    """
    Daily landing views → signup page views → paid signups → conversion rate.
    Used by /admin/conversion-funnel dashboard page.
    Returns list of dicts (newest first) + summary totals.
    """
    try:
        conn, cursor = get_db()
        cursor.execute(
            """
            SELECT
                DATE(created_at) AS day,
                COUNT(*) FILTER (
                    WHERE event_name = 'page_view'
                    AND   metadata::json->>'page' = 'landing'
                )                                                        AS landing_views,
                COUNT(*) FILTER (
                    WHERE event_name = 'signup_page_view'
                )                                                        AS signup_page_views,
                COUNT(*) FILTER (
                    WHERE event_name = 'signup'
                    AND   metadata IS NOT NULL
                    AND   metadata::json->>'plan' != 'free'
                )                                                        AS paid_signups
            FROM analytics_events
            WHERE created_at >= CURRENT_DATE - (%s * INTERVAL '1 day')
            GROUP BY DATE(created_at)
            ORDER BY day DESC
            """,
            (days,)
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        daily = []
        for r in rows:
            views    = int(r['landing_views']    or 0)
            sp_views = int(r['signup_page_views'] or 0)
            signups  = int(r['paid_signups']     or 0)
            rate     = round(signups / views * 100, 2) if views > 0 else 0.0
            daily.append({
                'day':               str(r['day']),
                'landing_views':     views,
                'signup_page_views': sp_views,
                'paid_signups':      signups,
                'conversion_rate':   rate,
            })

        total_views   = sum(d['landing_views'] for d in daily)
        total_signups = sum(d['paid_signups']  for d in daily)
        overall_rate  = round(total_signups / total_views * 100, 2) if total_views > 0 else 0.0

        return {
            'daily':         daily,
            'total_views':   total_views,
            'total_signups': total_signups,
            'overall_rate':  overall_rate,
            'days':          days,
        }
    except Exception:
        return {
            'daily': [], 'total_views': 0,
            'total_signups': 0, 'overall_rate': 0.0, 'days': days,
        }


# =====================================================================
# SHARED — Shopify connection lookup
# =====================================================================
# One query, reused everywhere something needs "which users currently
# have Shopify connected": suspicion scoring, dashboard overview
# metrics, and the Users table (component 6). Takes an already-open
# cursor from the caller's own connection, so this never opens a
# second DB connection or re-runs the join more than once per caller.

def _shopify_connected_user_ids(cursor):
    """
    client_integrations.client_id references clients.client_id (the
    TEXT public identifier), not clients.id.
    """
    cursor.execute("""
        SELECT DISTINCT c.user_id
        FROM clients c
        JOIN client_integrations ci ON ci.client_id = c.client_id
        WHERE ci.platform = 'shopify' AND ci.is_active = TRUE
    """)
    return {int(r['user_id']) for r in cursor.fetchall()}


# =====================================================================
# BOT / FRAUD SUSPICION SCORING  (admin dashboard — component 3/6)
# =====================================================================
# Each signal below adds fixed points toward a 0-100+ score:
#   extremely fast repeated registrations (same IP, <10 min apart)   +40
#   multiple signups from the same IP within 24h                    +20  (mutually exclusive with the above — fast implies loose)
#   disposable email domain                                         +25
#   random-looking email username                                   +15
#   no activity after signup (3+ days old, no event but the signup) +15
#   never connected Shopify (account 3+ days old)                   +10
# Bands: score >= 50 -> high, >= 20 -> medium, else low.
# Display only, per spec — nothing here blocks or restricts an account.

_DISPOSABLE_EMAIL_DOMAINS = frozenset({
    '10minutemail.com', '10minutemail.net', '20minutemail.com',
    'guerrillamail.com', 'guerrillamail.net', 'guerrillamail.org',
    'guerrillamailblock.com', 'sharklasers.com', 'grr.la',
    'mailinator.com', 'mailinator.net', 'mailinator.org',
    'yopmail.com', 'yopmail.net', 'yopmail.fr',
    'tempmail.com', 'temp-mail.org', 'temp-mail.io', 'tempmail.net', 'tempinbox.com',
    'throwawaymail.com', 'trashmail.com', 'trashmail.net', 'trash-mail.com',
    'getnada.com', 'nada.email', 'dispostable.com', 'mohmal.com',
    'emailondeck.com', 'fakeinbox.com', 'maildrop.cc', 'moakt.com',
    'mintemail.com', 'mytemp.email', 'inboxkitten.com', 'tempr.email',
    'discard.email', 'discardmail.com', 'spamgourmet.com', 'mailnesia.com',
    'mailcatch.com', 'jetable.org', 'burnermail.io',
    'fakemailgenerator.com', 'crazymailing.com', 'emailfake.com',
    'mailsac.com', 'tempmailo.com', 'luxusmail.org', '33mail.com',
})


def _looks_random(local_part: str) -> bool:
    """
    Cheap heuristic for a bot-generated-looking email username, e.g.
    'xk29fj0331' or 'qzxvbnpz'. One signal among several — never used
    alone, and false positives here are fine since nothing auto-blocks.
    """
    s = (local_part or '').lower()
    if len(s) < 6:
        return False
    letters = [c for c in s if c.isalpha()]
    digits  = [c for c in s if c.isdigit()]
    vowels  = sum(1 for c in letters if c in 'aeiou')
    digit_ratio = len(digits) / len(s)
    if digit_ratio >= 0.35:
        return True
    if len(letters) >= 6 and vowels == 0:
        return True
    return False


def get_user_suspicion_scores(user_ids=None, shopify_connected_user_ids=None):
    """
    Compute {user_id: {'score', 'level', 'signals': [str, ...]}} for
    every user, or a subset via user_ids. A handful of cheap aggregate
    queries plus pure-Python scoring — fine to run on every admin
    dashboard/Users-table load, no scheduled job needed.
    Never used to block or restrict a user — display only.

    shopify_connected_user_ids: pass in an already-fetched set (from
    _shopify_connected_user_ids()) when the caller fetched it anyway
    for another reason — e.g. the Users table also showing a Shopify
    column — so this function doesn't run that query a second time.
    If not given, fetches it itself, same as before.
    """
    try:
        conn, cursor = get_db()

        where, params = "", ()
        if user_ids:
            where, params = "WHERE id = ANY(%s)", (list(user_ids),)
        cursor.execute(f"SELECT id, email, created_at FROM users {where}", params)
        users = cursor.fetchall()
        if not users:
            cursor.close(); conn.close()
            return {}

        # Every signup event's IP + timestamp — used for the two
        # IP-clustering signals below.
        cursor.execute("""
            SELECT user_id, ip_address, created_at
            FROM analytics_events
            WHERE event_name = 'signup' AND ip_address IS NOT NULL
            ORDER BY ip_address, created_at
        """)
        signup_events = cursor.fetchall()

        cursor.execute("""
            SELECT user_id, COUNT(*) AS event_count
            FROM analytics_events
            WHERE user_id IS NOT NULL
            GROUP BY user_id
        """)
        event_counts = {int(r['user_id']): int(r['event_count']) for r in cursor.fetchall()}

        if shopify_connected_user_ids is None:
            shopify_connected_user_ids = _shopify_connected_user_ids(cursor)

        cursor.close()
        conn.close()

        by_ip = {}
        for r in signup_events:
            by_ip.setdefault(r['ip_address'], []).append(r)

        fast_cluster_users, loose_cluster_users = set(), set()
        for ip, events in by_ip.items():
            if len(events) < 2:
                continue
            events.sort(key=lambda r: r['created_at'])
            for i in range(1, len(events)):
                delta = (events[i]['created_at'] - events[i - 1]['created_at']).total_seconds()
                if delta <= 600:
                    fast_cluster_users.add(events[i]['user_id'])
                    fast_cluster_users.add(events[i - 1]['user_id'])
            if len(events) >= 3:
                span = (events[-1]['created_at'] - events[0]['created_at']).total_seconds()
                if span <= 86400:
                    for e in events:
                        loose_cluster_users.add(e['user_id'])

        # DB columns are TIMESTAMP (no tz) — compare against a naive
        # "now" to match, same convention as the rest of this module.
        now = datetime.utcnow()
        results = {}
        for u in users:
            uid = u['id']
            signals, score = [], 0

            if uid in fast_cluster_users:
                signals.append('Extremely fast repeated registrations from this IP')
                score += 40
            elif uid in loose_cluster_users:
                signals.append('Multiple signups from the same IP in a short time')
                score += 20

            email = u['email'] or ''
            domain = email.split('@')[-1].lower() if '@' in email else ''
            local_part = email.split('@')[0] if '@' in email else email
            if domain in _DISPOSABLE_EMAIL_DOMAINS:
                signals.append('Disposable email domain')
                score += 25
            if _looks_random(local_part):
                signals.append('Random-looking email username')
                score += 15

            created_at = u['created_at']
            account_age_days = (now - created_at).total_seconds() / 86400 if created_at else 0

            if account_age_days >= 3 and event_counts.get(uid, 0) <= 1:
                signals.append('No activity after signup')
                score += 15

            if account_age_days >= 3 and uid not in shopify_connected_user_ids:
                signals.append('Never connected Shopify')
                score += 10

            level = 'high' if score >= 50 else 'medium' if score >= 20 else 'low'
            results[uid] = {'score': score, 'level': level, 'signals': signals}

        return results
    except Exception:
        return {}


# =====================================================================
# USER DETAIL VIEW  (admin dashboard — component 4/6)
# =====================================================================

def get_user_detail(user_id):
    """
    Account-panel data for one user: base account fields plus the
    activity-summary columns added by migrate_admin_activity_tracking().
    Returns None if the user doesn't exist.
    """
    try:
        conn, cursor = get_db()
        cursor.execute("""
            SELECT id, email, plan_type, subscription_status, is_admin,
                   created_at, last_login_at, login_count, last_activity_at,
                   signup_ip
            FROM users WHERE id = %s
        """, (user_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if not row:
            return None
        d = dict(row)
        for k in ('created_at', 'last_login_at', 'last_activity_at'):
            if d.get(k):
                d[k] = d[k].isoformat()
        return d
    except Exception:
        return None


def get_user_security_activity(user_id, email, limit=50):
    """
    Security & Activity panel data for one user: recent IPs, User-Agent(s),
    the activity timeline, and failed login attempts.

    Failed logins aren't tied to user_id — a failed attempt might not
    even match a real account — so they're matched by the attempted
    email in analytics_events.metadata instead of a join.
    """
    try:
        conn, cursor = get_db()

        cursor.execute("""
            SELECT event_name, created_at, metadata, ip_address, user_agent
            FROM analytics_events
            WHERE user_id = %s
            ORDER BY created_at DESC
            LIMIT %s
        """, (user_id, limit))
        own_events = cursor.fetchall()

        cursor.execute("""
            SELECT created_at, ip_address, user_agent
            FROM analytics_events
            WHERE event_name = 'failed_login'
              AND metadata IS NOT NULL
              AND metadata::json->>'email' = %s
            ORDER BY created_at DESC
            LIMIT %s
        """, (email, limit))
        failed_logins = cursor.fetchall()

        cursor.close()
        conn.close()

        recent_ips, seen_ips = [], set()
        user_agents, seen_uas = [], set()
        for e in own_events:
            if e['ip_address'] and e['ip_address'] not in seen_ips:
                seen_ips.add(e['ip_address'])
                recent_ips.append(e['ip_address'])
            if e['user_agent'] and e['user_agent'] not in seen_uas:
                seen_uas.add(e['user_agent'])
                user_agents.append(e['user_agent'])

        timeline = []
        for e in own_events:
            meta = None
            if e['metadata']:
                try: meta = json.loads(e['metadata'])
                except Exception: meta = None
            timeline.append({
                'event_name': e['event_name'],
                'created_at': e['created_at'].isoformat() if e['created_at'] else None,
                'metadata':   meta,
            })

        return {
            'recent_ips':         recent_ips[:10],
            'user_agents':        user_agents[:5],
            'timeline':           timeline,
            'failed_login_count': len(failed_logins),
            'failed_logins': [
                {
                    'created_at': f['created_at'].isoformat() if f['created_at'] else None,
                    'ip_address': f['ip_address'],
                }
                for f in failed_logins
            ],
        }
    except Exception:
        return {
            'recent_ips': [], 'user_agents': [], 'timeline': [],
            'failed_login_count': 0, 'failed_logins': [],
        }


# =====================================================================
# DASHBOARD ANALYTICS WIDGETS  (admin dashboard — component 5/6)
# =====================================================================

def get_admin_overview_metrics():
    """
    User Management Enhancements — dashboard widgets. One function, a
    handful of queries, everything the new stats-grid needs: signup
    pace, activity (last_activity_at, from migrate_admin_activity_
    tracking + track_event's auto-bump), a snapshot Shopify connection
    rate, and two real cohort conversions:
      - signup_to_shopify_rate: of users who signed up in the last 30
        days, % who have Shopify connected now (a funnel, not just the
        all-time snapshot rate)
      - shopify_to_conversation_rate: of users with Shopify connected
        (ever), % who've had a first AI conversation
    Email verification rate is intentionally omitted — no verification
    system exists in the codebase yet.
    """
    try:
        conn, cursor = get_db()

        cursor.execute("SELECT COUNT(*) AS c FROM users")
        total_users = int(cursor.fetchone()['c'] or 0)

        cursor.execute("SELECT COUNT(*) AS c FROM users WHERE created_at >= CURRENT_DATE")
        new_today = int(cursor.fetchone()['c'] or 0)

        cursor.execute("SELECT COUNT(*) AS c FROM users WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'")
        new_this_week = int(cursor.fetchone()['c'] or 0)

        cursor.execute("SELECT COUNT(*) AS c FROM users WHERE last_activity_at >= NOW() - INTERVAL '7 days'")
        active_7d = int(cursor.fetchone()['c'] or 0)

        cursor.execute("SELECT COUNT(*) AS c FROM users WHERE last_activity_at >= NOW() - INTERVAL '30 days'")
        active_30d = int(cursor.fetchone()['c'] or 0)

        # Same shared query get_user_suspicion_scores() and the Users
        # table (get_users_for_admin_table()) use — written once in
        # _shopify_connected_user_ids().
        shopify_connected_ids = _shopify_connected_user_ids(cursor)
        shopify_connected = len(shopify_connected_ids)

        cursor.execute("""
            SELECT COUNT(DISTINCT user_id) AS c
            FROM analytics_events WHERE event_name = 'first_ai_conversation'
        """)
        first_conversation_users = int(cursor.fetchone()['c'] or 0)

        cursor.execute("""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (
                    WHERE EXISTS (
                        SELECT 1 FROM clients c
                        JOIN client_integrations ci ON ci.client_id = c.client_id
                        WHERE c.user_id = users.id AND ci.platform = 'shopify' AND ci.is_active = TRUE
                    )
                ) AS converted
            FROM users
            WHERE created_at >= NOW() - INTERVAL '30 days'
        """)
        cohort = cursor.fetchone()
        recent_signups = int(cohort['total'] or 0)
        recent_converted = int(cohort['converted'] or 0)

        cursor.close()
        conn.close()

        def pct(n, d):
            return round(n / d * 100, 1) if d > 0 else 0.0

        return {
            'total_users':                   total_users,
            'new_today':                     new_today,
            'new_this_week':                 new_this_week,
            'active_7d':                     active_7d,
            'active_30d':                    active_30d,
            'shopify_connected':             shopify_connected,
            'shopify_connection_rate':       pct(shopify_connected, total_users),
            'first_conversation_users':      first_conversation_users,
            'first_conversation_rate':       pct(first_conversation_users, total_users),
            'signup_to_shopify_rate':        pct(recent_converted, recent_signups),
            'shopify_to_conversation_rate':  pct(first_conversation_users, shopify_connected),
        }
    except Exception:
        return {
            'total_users': 0, 'new_today': 0, 'new_this_week': 0,
            'active_7d': 0, 'active_30d': 0, 'shopify_connected': 0,
            'shopify_connection_rate': 0.0, 'first_conversation_users': 0,
            'first_conversation_rate': 0.0, 'signup_to_shopify_rate': 0.0,
            'shopify_to_conversation_rate': 0.0,
        }


# =====================================================================
# USERS TABLE  (admin dashboard — component 6/6)
# =====================================================================

def get_users_for_admin_table(limit=500):
    """
    Everything the Users table needs — Last Login, Last Activity,
    Shopify Connected, Suspicion score/level/signals, plus a 30-day
    Active flag for the Active/Inactive filter — in a fixed, small
    number of queries, not one per user:

      1. get_all_users()                — 1 query (already existed;
         now also selects last_login_at/login_count/last_activity_at,
         so this adds zero extra queries, just 3 extra columns)
      2. _shopify_connected_user_ids()   — 1 query, shared with
         get_user_suspicion_scores() and get_admin_overview_metrics()
         rather than duplicated a third time
      3. get_user_suspicion_scores()     — reuses the existing
         function as-is (same one the per-user detail modal calls),
         passing in the set from #2 so it doesn't re-run that query

    Sorting and filtering on the result happen entirely client-side
    (per spec) — this function's job is just to hand the browser a
    complete, pre-computed row per user in one page load.
    """
    users = get_all_users(limit)
    if not users:
        return []

    try:
        conn, cursor = get_db()
        shopify_ids = _shopify_connected_user_ids(cursor)
        cursor.close()
        conn.close()
    except Exception:
        shopify_ids = set()

    user_ids = [u['id'] for u in users]
    suspicion = get_user_suspicion_scores(
        user_ids=user_ids, shopify_connected_user_ids=shopify_ids
    )

    now = datetime.utcnow()
    for u in users:
        u['shopify_connected'] = u['id'] in shopify_ids

        s = suspicion.get(u['id'], {'score': 0, 'level': 'low', 'signals': []})
        u['suspicion_score']   = s['score']
        u['suspicion_level']   = s['level']
        u['suspicion_signals'] = s['signals']

        last_activity = u.get('last_activity_at')
        if last_activity:
            # last_activity_at was already .isoformat()'d by get_all_users()
            try:
                delta_days = (now - datetime.fromisoformat(last_activity)).total_seconds() / 86400
                u['active_30d'] = delta_days <= 30
            except Exception:
                u['active_30d'] = False
        else:
            u['active_30d'] = False

    return users
