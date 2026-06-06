"""
blueprints/agency.py
--------------------
White-label client management, agency analytics, client portal,
portal authentication, client-user (seat) management, and client
deletion routes.

Extracted from app.py. All behaviour is identical to the original;
nothing has been changed except:
  - Route registration: Blueprint vs app
  - app.logger → current_app.logger
  - Inline imports promoted to module level
  - client_login_required decorator defined once at module level
  - _do_delete_client is now a private module helper
  - DNS thread-pool (_dns_executor) injected via init_agency()
  - Dependencies injected at registration time via init_agency()

Routes
------
  GET         /api/admin/white-label                  get_white_label
  POST        /api/admin/white-label                  save_white_label
  GET         /api/admin/agency-branding              get_agency_branding
  POST        /api/admin/agency-branding              save_agency_branding_route
  POST        /api/admin/white-label/verify-domain    verify_custom_domain
  GET         /api/analytics/agency                   get_agency_analytics
  GET         /api/admin/analytics                    get_analytics
  GET         /client-report                          client_report
  GET/POST    /api/clients/delete                     delete_client_legacy
  POST/DELETE /api/clients/<client_id>/delete         delete_client_by_id
  GET         /client-portal                          client_portal
  GET         /agency/clients                         agency_clients
  POST        /api/admin/client/suspend               toggle_suspend_client
  POST        /api/admin/client/clone                 clone_client_route
  POST        /api/admin/client/bulk-action           bulk_client_action
  GET/POST    /client-login                           client_login
  GET         /client-logout                          client_logout
  GET         /client-dashboard                       client_dashboard_router
  GET         /client-dashboard-portal                client_dashboard_client
  GET         /api/client-users                       list_client_users
  POST        /api/client-users/invite                invite_client_user
  POST        /api/client-users/delete                delete_client_user
  POST        /api/client-users/reset-password        reset_client_user_password
  GET         /manage-client-users                    manage_client_users_page

Registration in app.py:
  from blueprints.agency import agency_bp, init_agency
  init_agency(
      mail=mail,
      plan_limits=PLAN_LIMITS,
      dns_executor=_dns_executor,
      futures_timeout=_FuturesTimeout,
  )
  app.register_blueprint(agency_bp)
"""

import json
from concurrent.futures import TimeoutError as _FuturesTimeout
from datetime import datetime, timedelta
from functools import wraps

from flask import (Blueprint, jsonify, redirect, render_template,
                   request, current_app, session, url_for)
from flask_login import current_user, login_required
from flask_mail import Message
from werkzeug.security import generate_password_hash

import models

# ── Blueprint ────────────────────────────────────────────────────────────────

agency_bp = Blueprint('agency', __name__)

# Injected dependencies — populated by init_agency() before first request.
_mail            = None
_plan_limits     = None
_dns_executor    = None
_FuturesTimeout_ = None


def init_agency(mail, plan_limits, dns_executor, futures_timeout):
    """
    Called once in app.py after all shared objects are ready.
    Must be called before the first request reaches this blueprint.
    """
    global _mail, _plan_limits, _dns_executor, _FuturesTimeout_
    _mail            = mail
    _plan_limits     = plan_limits
    _dns_executor    = dns_executor
    _FuturesTimeout_ = futures_timeout


# ── Portal authentication decorator ─────────────────────────────────────────

def client_login_required(f):
    """Decorator for client-portal routes — checks session, not Flask-Login."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('client_user_id'):
            return redirect(url_for('agency.client_login'))
        return f(*args, **kwargs)
    return decorated


# ── White-label settings ─────────────────────────────────────────────────────

@agency_bp.route('/api/admin/white-label', methods=['GET'])
@login_required
def get_white_label():
    """Return white-label settings for a client (custom domain, CSS, email from)."""
    client_id = request.args.get('client_id', '')
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    fresh_user  = models.get_user_by_id(current_user.id)
    plan_type   = (fresh_user or {}).get('plan_type', current_user.plan_type)
    plan_limits = _plan_limits.get(plan_type, _plan_limits['free'])

    if not plan_limits.get('customization'):
        return jsonify({'success': False, 'error': 'Plan upgrade required'}), 403

    client = models.get_client_by_id(client_id)
    if not client:
        return jsonify({'success': False, 'error': 'Client not found'}), 404

    return jsonify({
        'success':              True,
        'custom_widget_domain': client.get('custom_widget_domain') or '',
        'custom_css':           client.get('custom_css') or '',
        'branded_email_from':   client.get('branded_email_from') or '',
        'has_custom_domain':    bool(client.get('custom_widget_domain')),
    })


@agency_bp.route('/api/admin/white-label', methods=['POST'])
@login_required
def save_white_label():
    """
    Save white-label settings for a client.
    Plan gating:
      - branded_email_from          → Pro+
      - custom_widget_domain, CSS   → Agency only
    """
    data      = request.json or {}
    client_id = data.get('client_id', '')

    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    fresh_user  = models.get_user_by_id(current_user.id)
    plan_type   = (fresh_user or {}).get('plan_type', current_user.plan_type)
    plan_limits = _plan_limits.get(plan_type, _plan_limits['free'])
    is_agency   = plan_type in ('agency', 'enterprise')
    is_pro_plus = plan_type in ('pro', 'agency', 'enterprise')

    _raw_domain = data.get('custom_widget_domain')
    _raw_css    = data.get('custom_css')
    _raw_email  = data.get('branded_email_from')
    domain             = _raw_domain.strip().lower() if _raw_domain is not None else None
    custom_css         = _raw_css.strip()            if _raw_css    is not None else None
    branded_email_from = _raw_email.strip()          if _raw_email  is not None else None

    if domain and not models.is_valid_domain(domain):
        return jsonify({
            'success': False,
            'error': f'"{domain}" is not a valid domain name. Use format: chat.yoursite.com',
        }), 400

    if domain and not is_agency:
        return jsonify({'success': False, 'error': 'Custom widget domain requires the Agency plan'}), 403
    if custom_css and not is_agency:
        return jsonify({'success': False, 'error': 'Custom CSS requires the Agency plan'}), 403
    if branded_email_from and not is_pro_plus:
        return jsonify({'success': False, 'error': 'Branded email sender requires Pro or Agency plan'}), 403

    if domain:
        existing = models.get_client_by_custom_domain(domain)
        if existing and existing['client_id'] != client_id:
            return jsonify({
                'success': False,
                'error': f'Domain "{domain}" is already in use by another client',
            }), 409

    try:
        models.save_white_label_settings(client_id, domain, custom_css, branded_email_from)
    except Exception as _wl_err:
        _err_str = str(_wl_err).lower()
        if 'unique' in _err_str or 'duplicate' in _err_str:
            current_app.logger.warning(
                f"[WhiteLabel] domain conflict (race) client={client_id} "
                f"domain={domain}: {_wl_err}"
            )
            return jsonify({
                'success': False,
                'error': (
                    f'Domain "{domain}" was just claimed by another client. '
                    'Please choose a different domain.'
                ),
            }), 409
        raise

    current_app.logger.info(
        f"[WhiteLabel] saved client={client_id} domain={domain} user={current_user.id}"
    )
    return jsonify({
        'success': True,
        'message': 'White-label settings saved',
        'cname_target': 'lumvi.net',
        'cname_instructions': (
            f'Point a CNAME record from {domain} → lumvi.net in your DNS provider, '
            'then wait up to 24h for propagation.'
        ) if domain else None,
    })


# ── Agency branding defaults ─────────────────────────────────────────────────

@agency_bp.route('/api/admin/agency-branding', methods=['GET'])
@login_required
def get_agency_branding():
    """Return the agency-wide default branding for the current user."""
    fresh_user = models.get_user_by_id(current_user.id)
    plan_type  = (fresh_user or {}).get('plan_type', current_user.plan_type)
    if plan_type not in ('agency', 'enterprise'):
        return jsonify({'success': False, 'error': 'Agency plan required'}), 403
    return jsonify({
        'success': True,
        'agency_branding': models.get_agency_branding(current_user.id),
    })


@agency_bp.route('/api/admin/agency-branding', methods=['POST'])
@login_required
def save_agency_branding_route():
    """
    Save agency-wide default branding.
    These defaults are auto-applied when a new client is created.
    """
    fresh_user = models.get_user_by_id(current_user.id)
    plan_type  = (fresh_user or {}).get('plan_type', current_user.plan_type)
    if plan_type not in ('agency', 'enterprise'):
        return jsonify({'success': False, 'error': 'Agency plan required'}), 403

    data = request.json or {}
    agency_branding = {
        'branding':           data.get('branding', {}),
        'bot_settings':       data.get('bot_settings', {}),
        'contact':            data.get('contact', {}),
        'branded_email_from': data.get('branded_email_from', ''),
    }
    models.save_agency_branding(current_user.id, agency_branding)
    current_app.logger.info(f"[AgencyBranding] saved user={current_user.id}")
    return jsonify({'success': True, 'message': 'Agency branding defaults saved'})


# ── Domain verification ───────────────────────────────────────────────────────

@agency_bp.route('/api/admin/white-label/verify-domain', methods=['POST'])
@login_required
def verify_custom_domain():
    """
    DNS check — walks the CNAME chain to verify the domain points to lumvi.net.
    Runs in the injected thread pool so it never blocks a Flask worker.
    """
    data      = request.json or {}
    domain    = data.get('domain', '').strip().lower()
    client_id = data.get('client_id', '')

    if not domain or not models.is_valid_domain(domain):
        return jsonify({'success': False, 'error': 'Invalid domain'}), 400
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    future = _dns_executor.submit(models.check_domain_dns, domain)
    try:
        result = future.result(timeout=8)
    except _FuturesTimeout_:
        current_app.logger.warning(f"[DNS] verify timed out for domain={domain}")
        return jsonify({
            'success': True, 'domain': domain, 'pointed': False,
            'message': '⏳ DNS check timed out — try again in a moment',
            'chain':   [],
        })
    except Exception as exc:
        current_app.logger.error(f"[DNS] verify error domain={domain}: {exc}")
        return jsonify({
            'success': True, 'domain': domain, 'pointed': False,
            'message': '✗ DNS check failed — check your DNS records',
            'chain':   [],
        })

    current_app.logger.info(
        f"[DNS] verify domain={domain} pointed={result['pointed']} "
        f"chain={result.get('chain')} user={current_user.id}"
    )
    return jsonify({
        'success': True,
        'domain':  domain,
        'pointed': result['pointed'],
        'message': result['message'],
        'chain':   result.get('chain', []),
    })


# ── Analytics ────────────────────────────────────────────────────────────────

@agency_bp.route('/api/analytics/agency')
@login_required
def get_agency_analytics():
    """Multi-client overview analytics for the agency dashboard."""
    try:
        fresh_user  = models.get_user_by_id(current_user.id)
        plan_type   = (fresh_user or {}).get('plan_type', current_user.plan_type)
        plan_limits = _plan_limits.get(plan_type, _plan_limits['free'])
        is_admin    = bool((fresh_user or {}).get('is_admin', False))

        if not plan_limits['analytics'] and not is_admin:
            return jsonify({'success': False, 'error': 'Upgrade required'}), 403

        date_range = request.args.get('range', 'week')
        now        = datetime.now()
        if date_range == 'today':
            start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        elif date_range == 'week':
            start_date = now - timedelta(days=7)
        elif date_range == 'month':
            start_date = now - timedelta(days=30)
        else:
            start_date = datetime(2020, 1, 1)

        clients    = models.get_user_clients(current_user.id)
        client_ids = [c['client_id'] for c in clients]

        if not client_ids:
            return jsonify({'success': True, 'clients': [], 'totals': {}, 'timeline': []})

        conn, cursor = models.get_db()
        try:
            cursor.execute(
                """SELECT client_id,
                          COUNT(*) AS total,
                          SUM(CASE WHEN matched = TRUE THEN 1 ELSE 0 END) AS matched
                   FROM conversations
                   WHERE client_id = ANY(%s) AND timestamp >= %s
                   GROUP BY client_id""",
                (client_ids, start_date)
            )
            conv_stats = {r['client_id']: dict(r) for r in cursor.fetchall()}

            cursor.execute(
                "SELECT client_id, COUNT(*) AS cnt FROM leads "
                "WHERE client_id = ANY(%s) AND created_at >= %s GROUP BY client_id",
                (client_ids, start_date)
            )
            lead_stats = {r['client_id']: int(r['cnt']) for r in cursor.fetchall()}

            cursor.execute(
                "SELECT client_id, COUNT(*) AS cnt FROM conversations "
                "WHERE client_id = ANY(%s) AND matched = FALSE AND timestamp >= %s "
                "GROUP BY client_id",
                (client_ids, start_date)
            )
            unanswered_stats = {r['client_id']: int(r['cnt']) for r in cursor.fetchall()}

            cursor.execute(
                "SELECT client_id, COUNT(*) AS cnt FROM conversations "
                "WHERE client_id = ANY(%s) AND DATE(timestamp) = %s GROUP BY client_id",
                (client_ids, now.strftime('%Y-%m-%d'))
            )
            daily_stats = {r['client_id']: int(r['cnt']) for r in cursor.fetchall()}

            cursor.execute(
                "SELECT client_id, MAX(timestamp) AS last_ts FROM conversations "
                "WHERE client_id = ANY(%s) GROUP BY client_id",
                (client_ids,)
            )
            last_active = {r['client_id']: r['last_ts'] for r in cursor.fetchall()}

            timeline = []
            for i in range(7):
                d = (now - timedelta(days=6 - i)).strftime('%Y-%m-%d')
                cursor.execute(
                    "SELECT COUNT(*) AS cnt FROM conversations "
                    "WHERE client_id = ANY(%s) AND DATE(timestamp) = %s",
                    (client_ids, d)
                )
                c_row = cursor.fetchone() or {}
                cursor.execute(
                    "SELECT COUNT(*) AS cnt FROM leads "
                    "WHERE client_id = ANY(%s) AND DATE(created_at) = %s",
                    (client_ids, d)
                )
                l_row = cursor.fetchone() or {}
                timeline.append({
                    'date':          d,
                    'conversations': int(c_row.get('cnt', 0)),
                    'leads':         int(l_row.get('cnt', 0)),
                })
        finally:
            try: cursor.close()
            except Exception: pass
            try: conn.close()
            except Exception: pass

        client_map  = {c['client_id']: c for c in clients}
        daily_limit = _plan_limits.get(plan_type, _plan_limits['free'])['messages_per_day']

        result_clients = []
        for cid in client_ids:
            cs         = conv_stats.get(cid, {'total': 0, 'matched': 0})
            total      = int(cs.get('total', 0))
            matched    = int(cs.get('matched', 0))
            leads      = lead_stats.get(cid, 0)
            daily      = daily_stats.get(cid, 0)
            unanswered = unanswered_stats.get(cid, 0)
            res_rate   = round(matched / total * 100) if total > 0 else 0
            last_ts    = last_active.get(cid)
            usage_pct  = (0 if daily_limit >= 999999
                          else min(round(daily / daily_limit * 100), 100))

            result_clients.append({
                'client_id':       cid,
                'name':            client_map.get(cid, {}).get('company_name', cid),
                'conversations':   total,
                'leads':           leads,
                'resolution_rate': res_rate,
                'daily_msgs':      daily,
                'daily_limit':     'Unlimited' if daily_limit >= 999999 else daily_limit,
                'usage_pct':       usage_pct,
                'unanswered':      unanswered,
                'last_active':     last_ts.isoformat() if last_ts else None,
            })

        result_clients.sort(key=lambda x: x['conversations'], reverse=True)
        tot_conv       = sum(c['conversations']   for c in result_clients)
        tot_leads      = sum(c['leads']            for c in result_clients)
        tot_unanswered = sum(c['unanswered']       for c in result_clients)
        avg_res        = (round(sum(c['resolution_rate'] for c in result_clients)
                                / len(result_clients))
                          if result_clients else 0)

        return jsonify({
            'success':  True,
            'clients':  result_clients,
            'timeline': timeline,
            'totals': {
                'clients':         len(clients),
                'conversations':   tot_conv,
                'leads':           tot_leads,
                'resolution_rate': avg_res,
                'unanswered':      tot_unanswered,
            },
        })

    except Exception as e:
        current_app.logger.error(f'[agency analytics] {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@agency_bp.route('/api/admin/analytics', methods=['GET'])
@login_required
def get_analytics():
    try:
        client_id = request.args.get('client_id', '').strip()
        if not client_id:
            return jsonify({'success': False, 'error': 'No client_id provided'}), 400
        if not models.verify_client_ownership(current_user.id, client_id):
            return jsonify({'success': False, 'error': 'unauthorized'}), 403

        date_range = request.args.get('range', 'month')
        now        = datetime.now()
        if date_range == 'today':
            start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        elif date_range == 'week':
            start_date = now - timedelta(days=7)
        elif date_range == 'month':
            start_date = now - timedelta(days=30)
        else:
            start_date = datetime(2020, 1, 1)

        conn, cursor = models.get_db()

        cursor.execute(
            'SELECT COUNT(*) AS total FROM conversations '
            'WHERE client_id = %s AND timestamp >= %s',
            (client_id, start_date)
        )
        total_conversations = (cursor.fetchone() or {}).get('total', 0)

        cursor.execute(
            'SELECT COUNT(*) AS matched_count FROM conversations '
            'WHERE client_id = %s AND timestamp >= %s AND matched = TRUE',
            (client_id, start_date)
        )
        answered         = (cursor.fetchone() or {}).get('matched_count', 0)
        unanswered_count = total_conversations - answered
        answer_rate      = (int(answered / total_conversations * 100)
                            if total_conversations > 0 else 0)

        cursor.execute(
            'SELECT COUNT(*) AS total_leads FROM leads '
            'WHERE client_id = %s AND created_at >= %s',
            (client_id, start_date)
        )
        total_leads = (cursor.fetchone() or {}).get('total_leads', 0)

        timeline      = []
        days_to_show  = 7 if date_range == 'week' else 30
        for i in range(days_to_show):
            date     = (now - timedelta(days=(days_to_show - 1) - i))
            date_str = date.strftime('%Y-%m-%d')
            cursor.execute(
                'SELECT COUNT(*) AS daily_count FROM conversations '
                'WHERE client_id = %s AND DATE(timestamp) = %s',
                (client_id, date_str)
            )
            conv_count = (cursor.fetchone() or {}).get('daily_count', 0)
            cursor.execute(
                'SELECT COUNT(*) AS daily_leads FROM leads '
                'WHERE client_id = %s AND DATE(created_at) = %s',
                (client_id, date_str)
            )
            lead_count = (cursor.fetchone() or {}).get('daily_leads', 0)
            timeline.append({'date': date_str, 'count': conv_count, 'leads': lead_count})

        cursor.execute(
            'SELECT user_message, COUNT(*) as count FROM conversations '
            'WHERE client_id = %s AND timestamp >= %s AND matched = TRUE '
            'GROUP BY user_message ORDER BY count DESC LIMIT 6',
            (client_id, start_date)
        )
        top_questions = [{'question': r['user_message'], 'count': r['count']}
                         for r in cursor.fetchall()]

        cursor.execute(
            'SELECT user_message, COUNT(*) as count FROM conversations '
            'WHERE client_id = %s AND timestamp >= %s AND matched = FALSE '
            'GROUP BY user_message ORDER BY count DESC LIMIT 6',
            (client_id, start_date)
        )
        unanswered_list = [{'question': r['user_message'], 'count': r['count']}
                           for r in cursor.fetchall()]

        cursor.execute(
            'SELECT name, email, phone, created_at FROM leads '
            'WHERE client_id = %s ORDER BY created_at DESC LIMIT 15',
            (client_id,)
        )
        leads_captured = [
            {
                'name':       r['name'],
                'email':      r['email'],
                'phone':      r['phone'],
                'created_at': r['created_at'].isoformat() if r['created_at'] else '',
            }
            for r in cursor.fetchall()
        ]

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'analytics': {
                'total_conversations': total_conversations,
                'total_leads':         total_leads,
                'answer_rate':         answer_rate,
                'unanswered_count':    unanswered_count,
                'timeline':            timeline,
                'top_questions':       top_questions,
                'unanswered':          unanswered_list,
                'leads_captured':      leads_captured,
            },
        })

    except Exception as e:
        current_app.logger.error(f'Error getting analytics: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500


# ── Client report ─────────────────────────────────────────────────────────────

@agency_bp.route('/client-report')
@login_required
def client_report():
    """Branded one-page client performance report."""
    client_id = request.args.get('client_id', '').strip()
    period    = request.args.get('period', 'month')

    if client_id and not models.verify_client_ownership(current_user.id, client_id):
        return "Unauthorized", 403
    if not client_id:
        clients = models.get_user_clients(current_user.id)
        if clients:
            client_id = clients[0]['client_id']
        else:
            return redirect(url_for('dashboard'))

    client = models.get_client_by_id(client_id)
    if not client:
        return "Client not found", 404

    branding = {}
    try:
        bs_raw   = client.get('branding_settings') or '{}'
        branding = json.loads(bs_raw) if isinstance(bs_raw, str) else bs_raw
    except Exception:
        pass

    branding_inner = branding.get('branding', {})
    primary_color  = branding_inner.get('primary_color') or client.get('widget_color') or '#B8924A'
    logo_url       = branding_inner.get('logo') or branding_inner.get('logo_url') or ''
    company_name   = branding_inner.get('company_name') or client.get('company_name', 'Client')

    period_labels  = {'week': 'Last 7 Days', 'month': 'Last 30 Days', 'all': 'All Time'}
    period_label   = period_labels.get(period, 'Last 30 Days')

    agency_branding = (models.get_agency_branding(current_user.id)
                       if hasattr(models, 'get_agency_branding') else {})
    agency_name     = (agency_branding.get('branding', {}).get('company_name') or
                       current_user.email.split('@')[0].title())

    return render_template(
        'client_report.html',
        client          = client,
        client_id       = client_id,
        company_name    = company_name,
        primary_color   = primary_color,
        logo_url        = logo_url,
        agency_name     = agency_name,
        agency_branding = agency_branding,
        period          = period,
        period_label    = period_label,
        user            = current_user,
    )


# ── Client deletion ───────────────────────────────────────────────────────────

def _do_delete_client(client_id):
    """Shared deletion logic — verifies ownership then cascades delete."""
    try:
        if not models.verify_client_ownership(current_user.id, client_id):
            return jsonify({'success': False, 'error': 'Unauthorized'}), 403
        models.delete_client(client_id)
        current_app.logger.info(
            f'Client {client_id} deleted by user {current_user.id}'
        )
        return jsonify({'success': True, 'message': 'Chatbot deleted successfully'})
    except Exception as e:
        current_app.logger.error(f'Delete client error: {e}')
        return jsonify({'success': False, 'error': 'Failed to delete chatbot'}), 500


@agency_bp.route('/api/clients/delete', methods=['POST', 'DELETE'])
@login_required
def delete_client_legacy():
    """Legacy route — client_id in JSON body."""
    data      = request.json or {}
    client_id = data.get('client_id')
    if not client_id:
        return jsonify({'success': False, 'error': 'client_id required'}), 400
    return _do_delete_client(client_id)


@agency_bp.route('/api/clients/<client_id>/delete', methods=['POST', 'DELETE'])
@login_required
def delete_client_by_id(client_id):
    """RESTful delete route — client_id in URL."""
    return _do_delete_client(client_id)


# ── Client portal dashboard ───────────────────────────────────────────────────

def _enrich_clients(clients, plan_limits, plan_type):
    """
    Shared enrichment logic for client_portal and agency_clients.
    Augments each client dict with usage %, last-active string, branding
    fields, and suspension status.
    """
    stats       = (models.get_clients_enriched_stats([c['client_id'] for c in clients])
                   if hasattr(models, 'get_clients_enriched_stats') else {})
    leads_month = (models.get_leads_this_month_bulk([c['client_id'] for c in clients])
                   if hasattr(models, 'get_leads_this_month_bulk') else {})
    daily_limit = plan_limits['messages_per_day']

    enriched = []
    for c in clients:
        cid  = c['client_id']
        s    = stats.get(cid, {})

        branding = {}
        try:
            bs_raw   = c.get('branding_settings') or '{}'
            branding = json.loads(bs_raw) if isinstance(bs_raw, str) else bs_raw
        except Exception:
            pass

        branding_inner  = branding.get('branding', {})
        bot_settings    = branding.get('bot_settings', {})
        primary_color   = branding_inner.get('primary_color') or c.get('widget_color') or '#B8924A'
        logo_url        = branding_inner.get('logo') or branding_inner.get('logo_url') or ''
        bot_avatar_url  = bot_settings.get('bot_avatar_url') or ''
        branding_removed = bool(branding_inner.get('remove_branding') or c.get('remove_branding'))

        daily_msgs = s.get('daily_msgs', 0)
        if daily_limit >= 999999:
            usage_pct, usage_class = 0, 'success'
        else:
            usage_pct   = min(round(daily_msgs / daily_limit * 100), 100)
            usage_class = ('danger'  if usage_pct >= 90 else
                           'warning' if usage_pct >= 70 else 'success')

        last_active     = s.get('last_active')
        last_active_str = 'No activity'
        if last_active:
            delta = datetime.utcnow() - last_active.replace(tzinfo=None)
            if delta.days == 0:    last_active_str = 'Today'
            elif delta.days == 1:  last_active_str = 'Yesterday'
            elif delta.days < 7:   last_active_str = f'{delta.days}d ago'
            elif delta.days < 30:  last_active_str = f'{delta.days // 7}w ago'
            else:                  last_active_str = last_active.strftime('%b %d')

        is_suspended = bool(c.get('is_suspended', False))

        enriched.append({
            **c,
            'cid':              cid,
            'name':             c.get('company_name', 'Unnamed'),
            'vertical':         branding.get('vertical', 'general').replace('_', ' ').title(),
            'faqs_count':       s.get('faqs_count', 0),
            'leads_count':      s.get('leads_count', 0),
            'leads_month':      leads_month.get(cid, 0),
            'conversations':    s.get('conversations', 0),
            'daily_msgs':       daily_msgs,
            'daily_limit':      ('Unlimited' if plan_limits['messages_per_day'] >= 999999
                                 else str(plan_limits['messages_per_day'])),
            'usage_pct':        usage_pct,
            'usage_class':      usage_class,
            'branding_removed': branding_removed,
            'primary_color':    primary_color,
            'logo_url':         logo_url,
            'bot_avatar_url':   bot_avatar_url,
            'last_active_str':  last_active_str,
            'near_limit':       (not daily_limit >= 999999) and usage_pct >= 80,
            'is_suspended':     is_suspended,
            'status':           'Suspended' if is_suspended else 'Active',
        })
    return enriched, leads_month


@agency_bp.route('/client-portal')
@login_required
def client_portal():
    fresh_user  = models.get_user_by_id(current_user.id)
    plan_type   = (fresh_user or {}).get('plan_type', current_user.plan_type)
    plan_limits = _plan_limits.get(plan_type, _plan_limits['free'])

    if not plan_limits['white_label']:
        # Inline upgrade wall — matches original exactly
        return '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Upgrade Required — Lumvi</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,700;9..144,800&family=DM+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0;}
:root{
  --cream:#F7F4EF;--gold:#B8924A;--gold-lt:rgba(184,146,74,0.12);--gold-dk:#9A7A3A;
  --gold-glow:rgba(184,146,74,0.22);--dark:#1C1917;--mid:#57534E;--sub:#A8A29E;
  --border:#E7E2DA;--white:#fff;
}
body{font-family:'DM Sans',sans-serif;background:var(--cream);min-height:100vh;
  display:flex;flex-direction:column;align-items:center;justify-content:center;padding:24px;}
.card{background:var(--white);border:1px solid var(--border);border-radius:20px;
  padding:48px 40px;max-width:480px;width:100%;text-align:center;
  box-shadow:0 4px 24px rgba(0,0,0,0.06);}
.icon{width:64px;height:64px;border-radius:18px;background:var(--gold-lt);
  display:flex;align-items:center;justify-content:center;margin:0 auto 20px;
  border:1px solid rgba(184,146,74,0.2);}
.icon svg{width:28px;height:28px;color:var(--gold);}
h1{font-family:'Fraunces',serif;font-size:24px;font-weight:800;color:var(--dark);
  margin-bottom:12px;letter-spacing:-0.3px;}
p{font-size:14px;color:var(--mid);line-height:1.7;margin-bottom:8px;}
.features{text-align:left;background:var(--cream);border:1px solid var(--border);
  border-radius:12px;padding:16px 20px;margin:20px 0;display:flex;flex-direction:column;gap:9px;}
.feat{display:flex;align-items:center;gap:9px;font-size:13.5px;color:var(--mid);}
.feat svg{width:15px;height:15px;color:var(--gold);flex-shrink:0;}
.btn-upgrade{display:inline-flex;align-items:center;justify-content:center;gap:7px;
  width:100%;padding:13px 24px;background:var(--gold);color:#fff;border-radius:12px;
  font-weight:700;font-size:14.5px;text-decoration:none;margin-bottom:10px;
  box-shadow:0 2px 8px var(--gold-glow);transition:all 0.2s;border:none;cursor:pointer;}
.btn-upgrade:hover{background:var(--gold-dk);transform:translateY(-1px);}
.btn-back{display:inline-flex;align-items:center;justify-content:center;
  width:100%;padding:11px 24px;background:transparent;color:var(--sub);
  border:1.5px solid var(--border);border-radius:12px;font-weight:600;
  font-size:14px;text-decoration:none;transition:all 0.15s;}
.btn-back:hover{border-color:var(--mid);color:var(--dark);}
.price-chip{display:inline-block;padding:3px 12px;background:var(--gold-lt);
  color:var(--gold-dk);border-radius:20px;font-size:12px;font-weight:700;
  border:1px solid rgba(184,146,74,0.25);margin-bottom:16px;}
</style>
</head>
<body>
<div class="card">
  <div class="icon">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
      <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/>
      <circle cx="9" cy="7" r="4"/>
      <path d="M23 21v-2a4 4 0 0 0-3-3.87"/>
      <path d="M16 3.13a4 4 0 0 1 0 7.75"/>
    </svg>
  </div>
  <div class="price-chip">Agency Plan — $299/mo</div>
  <h1>Client Portal</h1>
  <p>Manage unlimited client chatbots, branding, leads, and analytics from a single command centre.</p>
  <div class="features">
    <div class="feat">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="20 6 9 17 4 12"/></svg>
      Unlimited client chatbots
    </div>
    <div class="feat">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="20 6 9 17 4 12"/></svg>
      Full white-label — your brand, not Lumvi's
    </div>
    <div class="feat">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="20 6 9 17 4 12"/></svg>
      Custom domain per client widget
    </div>
    <div class="feat">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="20 6 9 17 4 12"/></svg>
      Clone clients, bulk actions, agency defaults
    </div>
    <div class="feat">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="20 6 9 17 4 12"/></svg>
      Webhooks, branded email, priority support
    </div>
  </div>
  <a href="/upgrade" class="btn-upgrade">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" style="width:14px;height:14px;"><polyline points="18 15 12 9 6 15"/></svg>
    Upgrade to Agency
  </a>
  <a href="/dashboard" class="btn-back">← Back to Dashboard</a>
</div>
</body>
</html>''', 403

    clients = models.get_user_clients(current_user.id)
    enriched, leads_month = _enrich_clients(clients, plan_limits, plan_type)

    client_limit     = plan_limits['clients']
    daily_limit      = plan_limits['messages_per_day']
    total_leads      = sum(c['leads_count']  for c in enriched)
    total_convos     = sum(c['conversations'] for c in enriched)
    active_clients   = sum(1 for c in enriched if not c['is_suspended'])
    leads_this_month = sum(c['leads_month']  for c in enriched)
    slots_display    = 'Unlimited' if client_limit >= 999999 else str(client_limit)
    daily_display    = 'Unlimited' if daily_limit  >= 999999 else str(daily_limit)
    agency_branding  = (models.get_agency_branding(current_user.id)
                        if hasattr(models, 'get_agency_branding') else {})

    return render_template(
        'client_portal.html',
        user             = current_user,
        plan_type        = plan_type,
        plan_limits      = plan_limits,
        clients          = enriched,
        total_leads      = total_leads,
        total_convos     = total_convos,
        active_clients   = active_clients,
        leads_this_month = leads_this_month,
        client_count     = len(enriched),
        slots_display    = slots_display,
        daily_display    = daily_display,
        agency_branding  = agency_branding,
    )


@agency_bp.route('/agency/clients')
@login_required
def agency_clients():
    fresh_user = models.get_user_by_id(current_user.id)
    plan_type  = (fresh_user or {}).get('plan_type', current_user.plan_type)
    is_admin   = bool((fresh_user or {}).get('is_admin', False))

    if plan_type not in {'pro', 'agency', 'enterprise'} and not is_admin:
        return render_template(
            'agency_clients_upgrade.html',
            user=current_user, plan_type=plan_type
        ), 403

    clients     = models.get_user_clients(current_user.id)
    plan_limits = _plan_limits.get(plan_type, _plan_limits['free'])
    enriched, _ = _enrich_clients(clients, plan_limits, plan_type)

    client_limit     = plan_limits['clients']
    daily_limit      = plan_limits['messages_per_day']
    total_leads      = sum(c['leads_count']   for c in enriched)
    total_convos     = sum(c['conversations']  for c in enriched)
    total_faqs       = sum(c['faqs_count']     for c in enriched)
    active_clients   = sum(1 for c in enriched if not c['is_suspended'])
    leads_this_month = sum(c['leads_month']    for c in enriched)
    slots_display    = 'Unlimited' if client_limit >= 999999 else str(client_limit)
    daily_display    = 'Unlimited' if daily_limit  >= 999999 else str(daily_limit)
    agency_branding  = (models.get_agency_branding(current_user.id)
                        if hasattr(models, 'get_agency_branding') else {})

    return render_template(
        'agency_clients.html',
        user             = current_user,
        plan_type        = plan_type,
        plan_limits      = plan_limits,
        clients          = enriched,
        total_leads      = total_leads,
        total_convos     = total_convos,
        total_faqs       = total_faqs,
        active_clients   = active_clients,
        leads_this_month = leads_this_month,
        slots_display    = slots_display,
        daily_display    = daily_display,
        client_count     = len(enriched),
        agency_branding  = agency_branding,
    )


# ── Client management actions ─────────────────────────────────────────────────

@agency_bp.route('/api/admin/client/suspend', methods=['POST'])
@login_required
def toggle_suspend_client():
    data      = request.json or {}
    client_id = data.get('client_id', '')
    suspend   = bool(data.get('suspend', True))
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    ok     = models.toggle_client_suspended(client_id, suspend)
    action = 'suspended' if suspend else 'reactivated'
    current_app.logger.info(
        f"[Agency] client {client_id} {action} by user {current_user.id}"
    )
    return jsonify({'success': ok, 'suspended': suspend})


@agency_bp.route('/api/admin/client/clone', methods=['POST'])
@login_required
def clone_client_route():
    data             = request.json or {}
    source_client_id = data.get('client_id', '')
    new_name         = data.get('new_name', '').strip()
    if not source_client_id or not models.verify_client_ownership(
            current_user.id, source_client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    if not new_name:
        return jsonify({'success': False, 'error': 'New client name is required'}), 400

    fresh_user  = models.get_user_by_id(current_user.id)
    plan_type   = (fresh_user or {}).get('plan_type', current_user.plan_type)
    plan_limits = _plan_limits.get(plan_type, _plan_limits['free'])

    # Advisory lock — prevents clone + create race past the client limit
    _lc, _lcur = models.get_db()
    try:
        _lcur.execute("SELECT pg_advisory_lock(%s)", (current_user.id,))
        current_count = len(models.get_user_clients(current_user.id))
        if current_count >= plan_limits['clients']:
            return jsonify({
                'success': False,
                'error':   f'Client limit reached for {plan_type} plan',
            }), 403
        new_cid = models.clone_client(source_client_id, current_user.id, new_name)
    finally:
        try:
            _lcur.execute("SELECT pg_advisory_unlock(%s)", (current_user.id,))
            _lc.commit()
        except Exception:
            pass
        try:
            _lcur.close(); _lc.close()
        except Exception:
            pass

    if not new_cid:
        return jsonify({'success': False, 'error': 'Clone failed — please try again'}), 500

    current_app.logger.info(
        f"[Agency] cloned {source_client_id} → {new_cid} by user {current_user.id}"
    )
    return jsonify({
        'success':       True,
        'new_client_id': new_cid,
        'message':       f'"{new_name}" created successfully',
    })


@agency_bp.route('/api/admin/client/bulk-action', methods=['POST'])
@login_required
def bulk_client_action():
    data       = request.json or {}
    action     = data.get('action', '')
    client_ids = data.get('client_ids', [])
    if not client_ids or not isinstance(client_ids, list):
        return jsonify({'success': False, 'error': 'No clients selected'}), 400

    results = {'ok': [], 'fail': []}
    for cid in client_ids:
        if not models.verify_client_ownership(current_user.id, cid):
            results['fail'].append(cid)
            continue
        try:
            if action == 'suspend':
                models.toggle_client_suspended(cid, True);   results['ok'].append(cid)
            elif action == 'reactivate':
                models.toggle_client_suspended(cid, False);  results['ok'].append(cid)
            elif action == 'delete':
                models.delete_client(cid);                   results['ok'].append(cid)
            else:
                results['fail'].append(cid)
        except Exception as e:
            current_app.logger.error(f"[BulkAction] {action} {cid}: {e}")
            results['fail'].append(cid)

    current_app.logger.info(
        f"[Agency] bulk {action}: ok={results['ok']} fail={results['fail']}"
    )
    return jsonify({'success': True, **results})


# ── Client portal authentication ──────────────────────────────────────────────

@agency_bp.route('/client-login', methods=['GET', 'POST'])
def client_login():
    """Login page for client-facing users (separate from agency owner login)."""
    if request.method == 'POST':
        email    = request.form.get('email', '').strip()
        password = request.form.get('password', '')
        user     = models.verify_client_user(email, password)
        if user:
            session['client_user_id']        = user['id']
            session['client_user_email']     = user['email']
            session['client_user_name']      = user.get('name', email)
            session['client_user_client_id'] = user['client_id']
            return redirect(url_for('agency.client_dashboard_router'))
        return render_template('client_login.html', error='Invalid email or password')
    return render_template('client_login.html')


@agency_bp.route('/client-logout')
def client_logout():
    session.pop('client_user_id', None)
    session.pop('client_user_email', None)
    session.pop('client_user_name', None)
    session.pop('client_user_client_id', None)
    return redirect(url_for('agency.client_login'))


@agency_bp.route('/client-dashboard')
def client_dashboard_router():
    """
    Routes to the correct dashboard view:
    - Agency/admin owner: accessed via ?client_id= from the main dashboard
    - Client portal user: authenticated via client session
    """
    client_id_param = request.args.get('client_id', '').strip()
    if client_id_param and current_user.is_authenticated:
        if not models.verify_client_ownership(current_user.id, client_id_param):
            return "Unauthorized", 403
        client   = models.get_client_by_id(client_id_param)
        if not client:
            return "Client not found", 404

        leads    = models.get_leads(client_id_param)
        faqs     = models.get_faqs(client_id_param)
        articles = models.get_articles(client_id_param)

        for lead in leads:
            if lead.get('created_at') and not isinstance(lead['created_at'], str):
                lead['created_at'] = lead['created_at'].isoformat()

        branding = {}
        if client.get('branding_settings'):
            try:
                bs       = (json.loads(client['branding_settings'])
                            if isinstance(client['branding_settings'], str)
                            else client['branding_settings'])
                branding = bs.get('branding', {})
            except Exception:
                pass

        _owner      = models.get_user_by_id(client.get('user_id', ''))
        _plan_type  = (_owner or {}).get('plan_type', 'free')
        _limits     = _plan_limits.get(_plan_type, _plan_limits['free'])
        _usage_warn = (models.get_usage_warning(client_id_param)
                       if hasattr(models, 'get_usage_warning') else None)

        return render_template(
            'client_dashboard.html',
            client            = client,
            branding          = branding,
            leads             = leads,
            faqs              = faqs,
            articles          = articles,
            client_user_name  = current_user.email,
            client_user_email = current_user.email,
            faq_count         = len(faqs),
            lead_count        = len(leads),
            owner_view        = True,
            analytics_level   = _limits.get('analytics_level', 'none'),
            usage_warning     = _usage_warn,
        )

    return client_dashboard_client()


@agency_bp.route('/client-dashboard-portal')
@client_login_required
def client_dashboard_client():
    client_id = session['client_user_client_id']
    client    = models.get_client_by_id(client_id)
    leads     = models.get_leads(client_id)
    faqs      = models.get_faqs(client_id)
    articles  = models.get_articles(client_id)

    for lead in leads:
        if lead.get('created_at') and not isinstance(lead['created_at'], str):
            lead['created_at'] = lead['created_at'].isoformat()

    branding = {}
    if client and client.get('branding_settings'):
        try:
            bs       = (json.loads(client['branding_settings'])
                        if isinstance(client['branding_settings'], str)
                        else client['branding_settings'])
            branding = bs.get('branding', {})
        except Exception:
            pass

    _owner      = (models.get_client_owner(client_id)
                   if hasattr(models, 'get_client_owner') else {})
    _plan_type  = (_owner or {}).get('plan_type', 'starter')
    _limits     = _plan_limits.get(_plan_type, _plan_limits['free'])
    _usage_warn = (models.get_usage_warning(client_id)
                   if hasattr(models, 'get_usage_warning') else None)

    return render_template(
        'client_dashboard.html',
        client            = client,
        branding          = branding,
        leads             = leads,
        faqs              = faqs,
        articles          = articles,
        client_user_name  = session.get('client_user_name'),
        client_user_email = session.get('client_user_email'),
        faq_count         = len(faqs),
        lead_count        = len(leads),
        owner_view        = False,
        analytics_level   = _limits.get('analytics_level', 'basic'),
        usage_warning     = _usage_warn,
    )


# ── Client user (seat) management ─────────────────────────────────────────────

@agency_bp.route('/api/client-users', methods=['GET'])
@login_required
def list_client_users():
    """List all portal logins for a given chatbot."""
    client_id = request.args.get('client_id')
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    users = models.get_client_users(client_id)
    return jsonify({'success': True, 'users': users})


@agency_bp.route('/api/client-users/invite', methods=['POST'])
@login_required
def invite_client_user():
    """Create a client portal login (Pro / Agency / Enterprise only)."""
    user = models.get_user_by_id(current_user.id)
    plan = user.get('plan_type', 'free')
    if plan not in ('pro', 'agency', 'enterprise', 'solo'):
        return jsonify({
            'success': False,
            'error':   'Client logins require Pro plan or above',
        }), 403

    data      = request.get_json()
    client_id = data.get('client_id')
    email     = data.get('email', '').strip()
    name      = data.get('name', '').strip()
    password  = data.get('password', '').strip()

    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    if not email or not password:
        return jsonify({'success': False, 'error': 'Email and password are required'}), 400
    if len(password) < 6:
        return jsonify({'success': False, 'error': 'Password must be at least 6 characters'}), 400

    uid = models.create_client_user(client_id, email, password, name, current_user.id)
    if not uid:
        return jsonify({
            'success': False,
            'error':   'Email already exists or could not be created',
        }), 400

    try:
        client  = models.get_client_by_id(client_id)
        company = (client.get('company_name', 'your chatbot portal')
                   if client else 'your chatbot portal')
        if _mail:
            msg = Message(
                subject=f"Your client portal access — {company}",
                sender="Lumvi <support@lumvi.net>",
                recipients=[email],
                html=f"""
<div style="font-family:Inter,Arial,sans-serif;max-width:480px;margin:0 auto;background:#0f172a;color:#f8fafc;padding:40px;border-radius:16px;">
  <div style="text-align:center;margin-bottom:28px;">
    <div style="display:inline-block;background:linear-gradient(135deg,#6366f1,#a78bfa);border-radius:12px;padding:10px 20px;font-size:22px;font-weight:800;">⚡ {company}</div>
  </div>
  <h2 style="margin:0 0 12px;font-size:20px;">Your portal access is ready</h2>
  <p style="color:#94a3b8;margin:0 0 24px;line-height:1.6;">Hi {name or email}, you've been given access to your client portal where you can view your leads, FAQs and analytics.</p>
  <div style="background:rgba(99,102,241,0.1);border:1px solid rgba(99,102,241,0.2);border-radius:10px;padding:16px;margin-bottom:24px;">
    <p style="margin:0 0 6px;font-size:13px;color:#94a3b8;">Login URL</p>
    <p style="margin:0;font-size:14px;color:#a5b4fc;">lumvi.net/client-login</p>
    <p style="margin:12px 0 6px;font-size:13px;color:#94a3b8;">Email</p>
    <p style="margin:0;font-size:14px;color:#f8fafc;">{email}</p>
    <p style="margin:12px 0 6px;font-size:13px;color:#94a3b8;">Password</p>
    <p style="margin:0;font-size:14px;color:#f8fafc;">{password}</p>
  </div>
  <a href="https://lumvi.net/client-login" style="display:block;text-align:center;background:linear-gradient(135deg,#6366f1,#7c3aed);color:#fff;text-decoration:none;padding:14px;border-radius:10px;font-weight:700;">Access My Portal →</a>
</div>"""
            )
            _mail.send(msg)
    except Exception as e:
        current_app.logger.error(f"Client invite email failed: {e}")

    current_app.logger.info(
        f"Client user created: {email} for client {client_id} by user {current_user.id}"
    )
    return jsonify({'success': True, 'id': uid, 'message': f'Login created for {email}'})


@agency_bp.route('/api/client-users/delete', methods=['POST'])
@login_required
def delete_client_user():
    data      = request.get_json()
    client_id = data.get('client_id')
    user_id   = data.get('user_id')
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    models.delete_client_user(user_id, client_id)
    return jsonify({'success': True})


@agency_bp.route('/api/client-users/reset-password', methods=['POST'])
@login_required
def reset_client_user_password():
    """Reset password for a client portal login."""
    data      = request.get_json() or {}
    client_id = data.get('client_id', '')
    user_id   = data.get('user_id')
    password  = data.get('password', '')

    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    if not password or len(password) < 6:
        return jsonify({'success': False, 'error': 'Password must be at least 6 characters'}), 400

    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            'UPDATE client_users SET password_hash = %s WHERE id = %s AND client_id = %s',
            (generate_password_hash(password), user_id, client_id)
        )
        conn.commit()
        current_app.logger.info(
            f"[ClientUsers] password reset for user {user_id} on client {client_id}"
        )
        return jsonify({'success': True})
    except Exception as e:
        current_app.logger.error(f"[ClientUsers] reset_password error: {e}")
        return jsonify({'success': False, 'error': 'Failed to reset password'}), 500
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


@agency_bp.route('/manage-client-users')
@login_required
def manage_client_users_page():
    """Page for agency owners to manage client portal logins."""
    client_id = request.args.get('client_id')
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return "Unauthorized", 403
    user = models.get_user_by_id(current_user.id)
    plan = user.get('plan_type', 'free')
    if plan not in ('pro', 'agency', 'enterprise', 'solo'):
        return render_template(
            'upgrade_required.html',
            feature     = 'Client Logins',
            description = 'Give your clients their own portal to view leads and analytics.',
            min_plan    = 'Pro',
        ), 403
    client = models.get_client_by_id(client_id)
    return render_template(
        'manage_client_users.html', client=client, client_id=client_id
    )
