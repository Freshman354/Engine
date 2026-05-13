"""
admin_routes.py — Lumvi Admin Panel Blueprint
=============================================
All routes under /admin/* live here.
Registered in app.py via:
    from admin_routes import admin_bp
    app.register_blueprint(admin_bp)

Every route that renders admin_dashboard.html passes a complete, safe
context dict so the template never crashes on a missing variable.
All DB calls are wrapped in try/except so a single query failure never
takes down the entire page — it degrades gracefully to an empty/zero value.
"""

from flask import Blueprint, render_template, request, jsonify, redirect, url_for, session
from flask_login import login_required, current_user
from functools import wraps
import json
import models

admin_bp = Blueprint('admin', __name__, url_prefix='/admin')


# ── Admin-only decorator ─────────────────────────────────────────────────────

def admin_required(f):
    """Wraps a route so only is_admin=True users can access it.
    Returns 403 JSON for API routes, redirect to /dashboard for page routes.
    """
    @wraps(f)
    @login_required
    def decorated(*args, **kwargs):
        user = models.get_user_by_id(current_user.id)
        if not user or not user.get('is_admin'):
            if request.path.startswith('/admin/api/'):
                return jsonify({'success': False, 'error': 'Admin access required'}), 403
            return redirect(url_for('dashboard'))
        return f(*args, **kwargs)
    return decorated


# ── Safe context builder ─────────────────────────────────────────────────────

def _safe(fn, default=None, *args, **kwargs):
    """Call fn(*args, **kwargs), return default on any exception.
    Prevents a single failing DB query from crashing the whole page.
    """
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"[admin _safe] {fn.__name__}: {e}")
        return default if default is not None else ([] if fn.__name__.startswith('get_all') else 0)


# ── Shared base context (injected into every section) ────────────────────────

def _base_context():
    """Variables every section of admin_dashboard.html needs."""
    return {
        # Used by profit card and cost tracker margin calculation
        'mrr': _safe(models.get_mrr, 0.0),
    }


# =====================================================================
# SECTION: DASHBOARD (Overview)
# =====================================================================

@admin_bp.route('/')
@admin_bp.route('/dashboard')
@admin_required
def dashboard():
    by_plan      = _safe(models.get_user_count_by_plan, {})
    cost_summary = _safe(models.get_api_cost_summary, {
        'cost_today': 0, 'cost_this_month': 0, 'cost_all_time': 0,
        'tokens_today': 0, 'tokens_this_month': 0,
    })

    ctx = _base_context()
    ctx.update({
        'section':          'dashboard',
        'total_users':      _safe(lambda: sum(models.get_user_count_by_plan().values()), 0),
        'new_this_month':   _safe(models.get_new_users_this_month, 0),
        'total_revenue':    _safe(models.get_total_revenue, 0.0),
        'active_subs':      _safe(models.get_active_subscription_count, 0),
        'paid_users':       _safe(models.get_paid_user_count, 0),
        'free_users':       _safe(models.get_free_user_count, 0),
        'total_clients':    _safe(models.get_total_client_count, 0),
        'by_plan':          by_plan,
        'revenue_by_month':  _safe(models.get_revenue_by_month, [], 6),
        'user_growth':       _safe(models.get_user_growth_by_month, [], 6),
        # Health alert counters
        'churned_this_week': _safe(models.get_churn_this_week, 0),
        'past_due_count':    _safe(models.get_past_due_count, 0),
        # Profit card
        'estimated_monthly_ai_cost': cost_summary.get('cost_this_month', 0),
    })
    return render_template('admin_dashboard.html', **ctx)


# =====================================================================
# SECTION: USERS
# =====================================================================

@admin_bp.route('/users')
@admin_required
def users():
    search = request.args.get('search', '').strip()

    all_users = _safe(models.get_all_users, [], 500)

    # Client-side search filter (avoids a second DB call)
    if search:
        sl = search.lower()
        all_users = [
            u for u in all_users
            if sl in (u.get('email') or '').lower()
            or sl in (u.get('plan_type') or '').lower()
        ]

    # Per-user AI cost dict for the new column
    user_ai_costs = _safe(models.get_user_ai_costs_dict, {})

    ctx = _base_context()
    ctx.update({
        'section':       'users',
        'users':         all_users,
        'user_ai_costs': user_ai_costs,
        'search':        search,
    })
    return render_template('admin_dashboard.html', **ctx)


# =====================================================================
# SECTION: REVENUE
# =====================================================================

@admin_bp.route('/revenue')
@admin_required
def revenue():
    payments = _safe(models.get_all_payments, [], 200)

    ctx = _base_context()
    ctx.update({
        'section':        'revenue',
        'payments':       payments,
        'total_revenue':  _safe(models.get_total_revenue, 0.0),
        'revenue_by_month':   _safe(models.get_revenue_by_month, [], 6),
    })
    return render_template('admin_dashboard.html', **ctx)


# =====================================================================
# SECTION: LEADS
# =====================================================================

@admin_bp.route('/leads')
@admin_required
def leads():
    search    = request.args.get('search', '').strip() or None
    client_id = request.args.get('client_id', '').strip() or None

    all_leads = _safe(
        models.get_all_leads_admin, [],
        500, client_id, search
    )

    ctx = _base_context()
    ctx.update({
        'section': 'leads',
        'leads':   all_leads,
        'search':  search or '',
    })
    return render_template('admin_dashboard.html', **ctx)


# =====================================================================
# SECTION: ANALYTICS
# =====================================================================

@admin_bp.route('/analytics')
@admin_required
def analytics():
    events       = _safe(models.get_analytics_events, [], 300)
    event_counts = _safe(models.get_event_counts, {})

    ctx = _base_context()
    ctx.update({
        'section':       'analytics',
        'events':        events,
        'event_counts':  event_counts,
    })
    return render_template('admin_dashboard.html', **ctx)


# =====================================================================
# SECTION: AI COSTS
# =====================================================================

@admin_bp.route('/costs')
@admin_required
def costs():
    cost_summary        = _safe(models.get_api_cost_summary, {
        'cost_today': 0, 'cost_this_month': 0, 'cost_all_time': 0,
        'tokens_today': 0, 'tokens_this_month': 0,
    })
    top_chatbots        = _safe(models.get_top_chatbots_by_cost, [], 1, 10)
    user_cost_breakdown = _safe(models.get_user_cost_breakdown, [], 1)
    cost_revenue_chart  = _safe(models.get_cost_revenue_by_month, [], 6)
    daily_burn          = _safe(models.get_daily_burn_last_30, [])

    ctx = _base_context()
    ctx.update({
        'section':               'costs',
        # Top-line numbers
        'cost_today':            cost_summary.get('cost_today',      0),
        'cost_this_month':       cost_summary.get('cost_this_month', 0),
        'cost_all_time':         cost_summary.get('cost_all_time',   0),
        'tokens_today':          cost_summary.get('tokens_today',    0),
        'tokens_this_month':     cost_summary.get('tokens_this_month', 0),
        # Tables
        'top_chatbots_by_cost':  top_chatbots,
        'user_cost_breakdown':   user_cost_breakdown,
        # Charts
        'cost_revenue_by_month': cost_revenue_chart,
        'daily_burn_last_30':    daily_burn,
    })
    return render_template('admin_dashboard.html', **ctx)


# =====================================================================
# SECTION: SYSTEM
# =====================================================================

@admin_bp.route('/system')
@admin_required
def system():
    ctx = _base_context()
    ctx.update({
        'section':  'system',
        'db_stats': _safe(models.get_db_stats, []),
    })
    return render_template('admin_dashboard.html', **ctx)


# =====================================================================
# API: USER MANAGEMENT
# =====================================================================

@admin_bp.route('/api/users/update', methods=['POST'])
@admin_required
def api_update_user():
    data = request.get_json() or {}
    user_id = data.get('user_id')
    if not user_id:
        return jsonify({'success': False, 'error': 'Missing user_id'}), 400
    try:
        models.admin_update_user(
            user_id,
            plan_type=data.get('plan_type'),
            subscription_status=data.get('subscription_status'),
            is_admin=data.get('is_admin'),
        )
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@admin_bp.route('/api/users/delete', methods=['POST'])
@admin_required
def api_delete_user():
    data = request.get_json() or {}
    user_id = data.get('user_id')
    if not user_id:
        return jsonify({'success': False, 'error': 'Missing user_id'}), 400
    try:
        # Prevent self-deletion
        if int(user_id) == int(current_user.id):
            return jsonify({'success': False, 'error': "You can't delete your own account"}), 400
        models.admin_delete_user(user_id)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =====================================================================
# API: LEADS
# =====================================================================

@admin_bp.route('/api/leads/delete', methods=['POST'])
@admin_required
def api_delete_lead():
    data = request.get_json() or {}
    lead_id = data.get('lead_id')
    if not lead_id:
        return jsonify({'success': False, 'error': 'Missing lead_id'}), 400
    try:
        models.admin_delete_lead(lead_id)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =====================================================================
# API: SYSTEM UTILITIES
# =====================================================================

@admin_bp.route('/api/system/migrate', methods=['POST'])
@admin_required
def api_run_migrations():
    """Run all safe migrations. Returns list of results."""
    results = []

    migration_fns = [
        ('clients table',           models.migrate_clients_table),
        ('faqs table',               models.migrate_faqs_table),
        ('faq → knowledge_base',     models.migrate_faq_to_knowledge_base),
        ('subscription expiry',      models.migrate_subscription_expiry)      if hasattr(models, 'migrate_subscription_expiry')      else None,
        ('recurring subscriptions',  models.migrate_to_recurring_subscriptions) if hasattr(models, 'migrate_to_recurring_subscriptions') else None,
        ('conversation features',    models.migrate_conversation_features)     if hasattr(models, 'migrate_conversation_features')     else None,
        ('knowledge base',           models.migrate_knowledge_base)            if hasattr(models, 'migrate_knowledge_base')            else None,
        ('webhooks',                 models.migrate_webhooks),
        ('white label',              models.migrate_white_label),
        ('client status',            models.migrate_client_status),
        ('onboarding',               models.migrate_onboarding),
        ('api_usage_log',            models.migrate_api_usage_log),
    ]

    for entry in migration_fns:
        if entry is None:
            continue
        label, fn = entry
        try:
            fn()
            results.append(f"✅ {label}")
        except Exception as e:
            results.append(f"⚠️  {label}: {e}")

    return jsonify({'success': True, 'results': results})


@admin_bp.route('/api/system/make-admin', methods=['POST'])
@admin_required
def api_make_admin():
    data  = request.get_json() or {}
    email = (data.get('email') or '').strip().lower()
    if not email:
        return jsonify({'success': False, 'error': 'Email is required'}), 400
    user = models.get_user_by_email(email)
    if not user:
        return jsonify({'success': False, 'error': f'No user found with email: {email}'}), 404
    try:
        models.admin_update_user(user['id'], is_admin=True)
        return jsonify({'success': True, 'message': f'{email} is now an admin'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@admin_bp.route('/api/system/purge-logs', methods=['POST'])
@admin_required
def api_purge_logs():
    """Delete api_usage_log rows older than 90 days."""
    try:
        deleted = models.purge_old_api_logs(days=90)
        return jsonify({'success': True, 'deleted': deleted})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =====================================================================
# API: COST TRACKER (live refresh)
# =====================================================================

@admin_bp.route('/api/costs/summary', methods=['GET'])
@admin_required
def api_cost_summary():
    """Live cost summary for AJAX refresh."""
    summary = _safe(models.get_api_cost_summary, {
        'cost_today': 0, 'cost_this_month': 0, 'cost_all_time': 0,
        'tokens_today': 0, 'tokens_this_month': 0,
    })
    return jsonify({'success': True, **summary})
