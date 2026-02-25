"""
admin_routes.py — Lumvi Admin Control Panel
Blueprint: all routes are prefixed /admin/
Protection: login_required + is_admin check on every route.

Register in app.py:
    from admin_routes import admin_bp
    app.register_blueprint(admin_bp)
"""

import csv
import io
import json
from datetime import datetime
from functools import wraps

from flask import (Blueprint, jsonify, redirect, render_template,
                   request, url_for, Response)
from flask_login import current_user, login_required

import models

admin_bp = Blueprint('admin', __name__, url_prefix='/admin')

PLAN_PRICES = {'free': 0, 'starter': 49, 'pro': 99, 'agency': 299, 'enterprise': 499}
VALID_PLANS = list(PLAN_PRICES.keys())
VALID_STATUSES = ['active', 'cancelled', 'past_due', 'trialing', 'paused']


# ── Admin guard ────────────────────────────────────────────────────────────────

def admin_required(f):
    """Decorator: must be logged in AND have is_admin = True."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated:
            return redirect(url_for('login'))
        user = models.get_user_by_id(current_user.id)
        if not user or not user.get('is_admin'):
            return render_template_string(ACCESS_DENIED_HTML), 403
        return f(*args, **kwargs)
    return decorated


ACCESS_DENIED_HTML = '''<!DOCTYPE html>
<html>
<head><title>Access Denied</title>
<style>body{font-family:-apple-system,sans-serif;background:#0f172a;min-height:100vh;
display:flex;align-items:center;justify-content:center;}
.card{background:#1e293b;border:1px solid rgba(255,255,255,.1);border-radius:16px;
padding:48px;text-align:center;color:#f8fafc;max-width:400px;}
h1{font-size:24px;margin-bottom:12px;}p{color:#64748b;margin-bottom:24px;}
a{color:#06b6d4;text-decoration:none;font-weight:600;}</style>
</head><body>
<div class="card">
  <h1>🚫 Access Denied</h1>
  <p>You need admin privileges to access this page.</p>
  <a href="/dashboard">← Back to Dashboard</a>
</div></body></html>'''

from flask import render_template_string


# ── Dashboard overview ─────────────────────────────────────────────────────────

@admin_bp.route('/')
@admin_bp.route('/dashboard')
@login_required
@admin_required
def dashboard():
    total_users = len(models.get_all_users(limit=9999))
    by_plan = models.get_user_count_by_plan()
    new_this_month = models.get_new_users_this_month()
    mrr = models.get_mrr()
    total_revenue = models.get_total_revenue()
    revenue_by_month = models.get_revenue_by_month(6)
    user_growth = models.get_user_growth_by_month(6)
    total_clients = 0
    try:
        conn, cursor = models.get_db()
        cursor.execute('SELECT COUNT(*) AS c FROM clients')
        total_clients = cursor.fetchone()['c']
        cursor.close()
        conn.close()
    except Exception:
        pass

    paid_users = sum(v for k, v in by_plan.items() if k not in ('free',))
    free_users = by_plan.get('free', 0)
    active_subs = 0
    try:
        conn, cursor = models.get_db()
        cursor.execute("SELECT COUNT(*) AS c FROM users WHERE subscription_status = 'active' AND plan_type != 'free'")
        active_subs = cursor.fetchone()['c']
        cursor.close()
        conn.close()
    except Exception:
        pass

    return render_template(
        'admin_dashboard.html',
        total_users=total_users,
        paid_users=paid_users,
        free_users=free_users,
        new_this_month=new_this_month,
        mrr=mrr,
        total_revenue=total_revenue,
        active_subs=active_subs,
        total_clients=total_clients,
        by_plan=by_plan,
        revenue_by_month=revenue_by_month,
        user_growth=user_growth,
        section='dashboard'
    )


# ── Users ─────────────────────────────────────────────────────────────────────

@admin_bp.route('/users')
@login_required
@admin_required
def users():
    all_users = models.get_all_users(limit=500)
    return render_template(
        'admin_dashboard.html',
        users=all_users,
        valid_plans=VALID_PLANS,
        valid_statuses=VALID_STATUSES,
        section='users'
    )


@admin_bp.route('/api/users/<int:user_id>/update', methods=['POST'])
@login_required
@admin_required
def api_update_user(user_id):
    data = request.json or {}
    plan_type = data.get('plan_type')
    subscription_status = data.get('subscription_status')
    is_admin = data.get('is_admin')

    if plan_type and plan_type not in VALID_PLANS:
        return jsonify({'success': False, 'error': f'Invalid plan: {plan_type}'}), 400
    if subscription_status and subscription_status not in VALID_STATUSES:
        return jsonify({'success': False, 'error': f'Invalid status: {subscription_status}'}), 400

    ok = models.admin_update_user(
        user_id,
        plan_type=plan_type,
        subscription_status=subscription_status,
        is_admin=is_admin
    )
    if ok:
        models.track_event('admin_user_update', user_id=current_user.id,
                           metadata={'target_user': user_id, 'changes': data})
        return jsonify({'success': True})
    return jsonify({'success': False, 'error': 'Nothing to update'}), 400


@admin_bp.route('/api/users/<int:user_id>/delete', methods=['DELETE', 'POST'])
@login_required
@admin_required
def api_delete_user(user_id):
    if user_id == current_user.id:
        return jsonify({'success': False, 'error': 'Cannot delete your own account'}), 400
    try:
        models.admin_delete_user(user_id)
        models.track_event('admin_user_delete', user_id=current_user.id,
                           metadata={'deleted_user_id': user_id})
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ── Revenue ───────────────────────────────────────────────────────────────────

@admin_bp.route('/revenue')
@login_required
@admin_required
def revenue():
    payments = models.get_all_payments(limit=200)
    mrr = models.get_mrr()
    total_revenue = models.get_total_revenue()
    revenue_by_month = models.get_revenue_by_month(12)
    failed_count = sum(1 for p in payments if p.get('status') == 'failed')
    return render_template(
        'admin_dashboard.html',
        payments=payments,
        mrr=mrr,
        total_revenue=total_revenue,
        revenue_by_month=revenue_by_month,
        failed_count=failed_count,
        valid_plans=VALID_PLANS,
        section='revenue'
    )


@admin_bp.route('/api/payments/add', methods=['POST'])
@login_required
@admin_required
def api_add_payment():
    data = request.json or {}
    user_id = data.get('user_id')
    amount = data.get('amount')
    plan_type = data.get('plan_type', 'manual')
    provider = data.get('provider', 'manual')
    notes = data.get('notes', '')

    if not user_id or not amount:
        return jsonify({'success': False, 'error': 'user_id and amount required'}), 400

    user = models.get_user_by_id(int(user_id))
    if not user:
        return jsonify({'success': False, 'error': 'User not found'}), 404

    pid = models.record_payment(
        user_id=int(user_id), amount=float(amount),
        plan_type=plan_type, provider=provider, notes=notes
    )
    models.track_event('admin_payment_recorded', user_id=current_user.id,
                       metadata={'payment_id': pid, 'amount': amount, 'for_user': user_id})
    return jsonify({'success': True, 'payment_id': pid})


# ── Leads ─────────────────────────────────────────────────────────────────────

@admin_bp.route('/leads')
@login_required
@admin_required
def leads():
    search = request.args.get('q', '').strip()
    client_filter = request.args.get('client_id', '').strip()
    all_leads = models.get_all_leads_admin(
        limit=500,
        client_id_filter=client_filter or None,
        search=search or None
    )
    # Get unique clients for filter dropdown
    clients = []
    try:
        conn, cursor = models.get_db()
        cursor.execute('SELECT client_id, company_name FROM clients ORDER BY company_name')
        clients = [dict(r) for r in cursor.fetchall()]
        cursor.close()
        conn.close()
    except Exception:
        pass
    return render_template(
        'admin_dashboard.html',
        leads=all_leads,
        clients=clients,
        search=search,
        client_filter=client_filter,
        section='leads'
    )


@admin_bp.route('/api/leads/<int:lead_id>/delete', methods=['DELETE', 'POST'])
@login_required
@admin_required
def api_delete_lead(lead_id):
    try:
        models.admin_delete_lead(lead_id)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@admin_bp.route('/leads/export')
@login_required
@admin_required
def export_leads():
    search = request.args.get('q', '').strip()
    client_filter = request.args.get('client_id', '').strip()
    all_leads = models.get_all_leads_admin(
        limit=5000,
        client_id_filter=client_filter or None,
        search=search or None
    )
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['ID', 'Name', 'Email', 'Phone', 'Company', 'Message',
                     'Source URL', 'Client', 'Owner', 'Created At'])
    for lead in all_leads:
        writer.writerow([
            lead.get('id'), lead.get('name'), lead.get('email'),
            lead.get('phone'), lead.get('company'), lead.get('message'),
            lead.get('source_url'), lead.get('company_name'),
            lead.get('owner_email'), lead.get('created_at')
        ])
    output.seek(0)
    filename = f'lumvi_leads_{datetime.utcnow().strftime("%Y%m%d_%H%M%S")}.csv'
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename={filename}'}
    )


# ── Analytics Events ──────────────────────────────────────────────────────────

@admin_bp.route('/analytics')
@login_required
@admin_required
def analytics():
    events = []
    event_counts = {}
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            '''SELECT ae.*, u.email
               FROM analytics_events ae
               LEFT JOIN users u ON ae.user_id = u.id
               ORDER BY ae.created_at DESC LIMIT 300'''
        )
        events = [dict(r) for r in cursor.fetchall()]
        for e in events:
            if e.get('created_at'):
                e['created_at'] = e['created_at'].isoformat()
        cursor.execute(
            '''SELECT event_name, COUNT(*) AS cnt
               FROM analytics_events
               GROUP BY event_name ORDER BY cnt DESC LIMIT 20'''
        )
        event_counts = {r['event_name']: int(r['cnt']) for r in cursor.fetchall()}
        cursor.close()
        conn.close()
    except Exception:
        pass
    return render_template(
        'admin_dashboard.html',
        events=events,
        event_counts=event_counts,
        section='analytics'
    )


# ── System / Migrations ───────────────────────────────────────────────────────

@admin_bp.route('/system')
@login_required
@admin_required
def system():
    return render_template('admin_dashboard.html', section='system')


@admin_bp.route('/api/system/migrate', methods=['POST'])
@login_required
@admin_required
def api_migrate():
    results = []
    try:
        models.migrate_admin_columns()
        results.append('Admin columns migration: OK')
    except Exception as e:
        results.append(f'Admin columns migration: {e}')
    try:
        models.migrate_payments_and_events()
        results.append('Payments / events tables: OK')
    except Exception as e:
        results.append(f'Payments / events tables: {e}')
    try:
        models.migrate_clients_table()
        results.append('Clients table migration: OK')
    except Exception as e:
        results.append(f'Clients table migration: {e}')
    try:
        models.migrate_faqs_table()
        results.append('FAQs table migration: OK')
    except Exception as e:
        results.append(f'FAQs table migration: {e}')
    try:
        models.migrate_lead_custom_fields()
        results.append('Lead custom_fields column: OK')
    except Exception as e:
        results.append(f'Lead custom_fields column: {e}')
    try:
        models.migrate_password_reset_tokens()
        results.append('Password reset tokens table: OK')
    except Exception as e:
        results.append(f'Password reset tokens table: {e}')
    models.track_event('admin_migration_run', user_id=current_user.id,
                       metadata={'results': results})
    return jsonify({'success': True, 'results': results})


@admin_bp.route('/api/system/make-admin', methods=['POST'])
@login_required
@admin_required
def api_make_admin():
    data = request.json or {}
    email = data.get('email', '').strip().lower()
    if not email:
        return jsonify({'success': False, 'error': 'Email required'}), 400
    user = models.get_user_by_email(email)
    if not user:
        return jsonify({'success': False, 'error': f'User not found: {email}'}), 404
    models.admin_update_user(user['id'], is_admin=True)
    return jsonify({'success': True, 'message': f'{email} is now an admin'})