"""
models/analytics.py
-------------------
Platform analytics — revenue, user growth, conversion funnel,
API cost tracking, DB stats, and admin-level reporting queries.
"""
import json
from datetime import datetime
from .db import get_db

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
