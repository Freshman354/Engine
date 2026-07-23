"""
blueprints/agency.py
--------------------
Single-store analytics and store management for a single Shopify/
WooCommerce merchant. One Lumvi account = one connected store.

STRIPPED DOWN (obsolete agency-business-model removal): this blueprint
used to also handle white-label branding, custom domains, agency-wide
branding defaults, seat/client-user billing, client invitations,
multi-client (agency) dashboards, client portal login, and bulk/clone/
suspend client-management actions. All of that assumed an agency
account managing many client businesses — that model has been
permanently removed. See the removal report delivered alongside this
file for what was cut, what was kept, and what's still owed a follow-up
pass elsewhere (models.py, auth.py's dashboard, cron.py's seat-billing
crons).

Extracted from app.py originally. Retained behaviour for the routes
below is otherwise unchanged from before the strip-down, except
client_dashboard_router() no longer needs a ?client_id= param or a
client-portal-session fallback, since there's exactly one store per
account now.

Routes
------
  GET   /api/admin/analytics       get_analytics
  POST  /api/clients/<id>/delete   delete_client_by_id
  GET   /client-dashboard          client_dashboard_router

Registration in app.py:
  from blueprints.agency import agency_bp, init_agency
  init_agency(plan_limits=PLAN_LIMITS)
  app.register_blueprint(agency_bp)
"""

import json
from datetime import datetime, timedelta

from flask import Blueprint, jsonify, request, current_app, redirect, url_for, render_template
from flask_login import current_user, login_required

import models

# ── Blueprint ────────────────────────────────────────────────────────────────

agency_bp = Blueprint('agency', __name__)

# Injected dependency — populated by init_agency() before first request.
_plan_limits = None


def init_agency(plan_limits):
    """
    Called once in app.py after PLAN_LIMITS is ready.
    Must be called before the first request reaches this blueprint.
    """
    global _plan_limits
    _plan_limits = plan_limits


# ── Analytics ────────────────────────────────────────────────────────────────

@agency_bp.route('/api/admin/analytics', methods=['GET'])
@login_required
def get_analytics():
    """
    Single-store analytics: conversation/lead counts, timeline, top
    questions, transcripts, satisfaction, peak times.

    Free/Starter (basic) vs Growth/Scale (advanced) gating, see
    PLAN_LIMITS['analytics_level'] — 'basic' vs 'advanced'):
      - Top questions:      5 (basic)   vs unlimited (advanced)
      - History window:     7 days max (basic) vs 365 days max (advanced)
      - Transcripts:        last 10 sessions (basic) vs unlimited (advanced)
      - Satisfaction:       advanced only (omitted for basic)
      - Peak times:         advanced only (omitted for basic)
    """
    try:
        client_id = request.args.get('client_id', '').strip()
        if not client_id:
            return jsonify({'success': False, 'error': 'No client_id provided'}), 400
        if not models.verify_client_ownership(current_user.id, client_id):
            return jsonify({'success': False, 'error': 'unauthorized'}), 403

        fresh_user = models.get_user_by_id(current_user.id)
        plan_type  = (fresh_user or {}).get('plan_type', current_user.plan_type)
        limits     = _plan_limits.get(plan_type, _plan_limits['free'])
        is_advanced = limits.get('analytics_level') == 'advanced'

        # ── Date range, clamped by tier ──────────────────────────────────
        date_range = request.args.get('range', 'month')
        now        = datetime.now()
        preset_days = {'today': 0, 'week': 7, 'month': 30, 'quarter': 90, 'year': 365}
        requested_days = preset_days.get(date_range, 30 if date_range == 'all' else 30)
        max_days   = 365 if is_advanced else 7
        window_days = min(requested_days, max_days) if date_range != 'today' else 0

        if date_range == 'today':
            start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            start_date = now - timedelta(days=min(requested_days or 30, max_days))

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
        days_to_show  = max(1, min(window_days or 30, max_days))
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

        # ── Top / unanswered questions — tiered limit ────────────────────
        top_limit = 500 if is_advanced else 5   # "unlimited" capped at a sane payload size

        cursor.execute(
            'SELECT user_message, COUNT(*) as count FROM conversations '
            'WHERE client_id = %s AND timestamp >= %s AND matched = TRUE '
            'GROUP BY user_message ORDER BY count DESC LIMIT %s',
            (client_id, start_date, top_limit)
        )
        top_questions = [{'question': r['user_message'], 'count': r['count']}
                         for r in cursor.fetchall()]

        cursor.execute(
            'SELECT user_message, COUNT(*) as count FROM conversations '
            'WHERE client_id = %s AND timestamp >= %s AND matched = FALSE '
            'GROUP BY user_message ORDER BY count DESC LIMIT %s',
            (client_id, start_date, top_limit)
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

        # ── Conversation transcripts — tiered by session count ────────────
        transcript_session_limit = 10 if not is_advanced else 500
        cursor.execute(
            '''SELECT session_id, user_message, bot_response, matched, timestamp
               FROM conversations
               WHERE client_id = %s
                 AND (method IS NULL OR method != 'lead_captured')
               ORDER BY timestamp DESC
               LIMIT 2000''',
            (client_id,)
        )
        raw_turns = cursor.fetchall()
        sessions_order = []
        sessions_map   = {}
        for r in raw_turns:
            sid = r.get('session_id') or f"turn-{r['timestamp'].isoformat() if r.get('timestamp') else len(sessions_order)}"
            if sid not in sessions_map:
                if len(sessions_order) >= transcript_session_limit:
                    continue
                sessions_order.append(sid)
                sessions_map[sid] = {
                    'session_id': sid,
                    'started_at': r['timestamp'].isoformat() if r.get('timestamp') else '',
                    'messages':   [],
                }
            if sid in sessions_map:
                sessions_map[sid]['messages'].append({
                    'user_message': r.get('user_message') or '',
                    'bot_response': r.get('bot_response') or '',
                    'matched':      bool(r.get('matched')),
                    'timestamp':    r['timestamp'].isoformat() if r.get('timestamp') else '',
                })
        transcripts = [sessions_map[sid] for sid in sessions_order]
        for t in transcripts:
            t['messages'].reverse()   # oldest → newest within a session

        result = {
            'total_conversations': total_conversations,
            'total_leads':         total_leads,
            'answer_rate':         answer_rate,
            'unanswered_count':    unanswered_count,
            'timeline':            timeline,
            'top_questions':       top_questions,
            'unanswered':          unanswered_list,
            'leads_captured':      leads_captured,
            'transcripts':         transcripts,
            'analytics_level':     'advanced' if is_advanced else 'basic',
        }

        # ── Advanced-only: customer satisfaction (real CSAT data) ─────────
        # ── Advanced-only: peak conversation times ────────────────────────
        if is_advanced:
            cursor.execute(
                '''SELECT
                     COUNT(*) FILTER (WHERE csat_rating = 1)  AS positive,
                     COUNT(*) FILTER (WHERE csat_rating = -1) AS negative,
                     COUNT(*) FILTER (WHERE csat_rating IS NOT NULL) AS total_rated
                   FROM chat_sessions
                   WHERE client_id = %s AND created_at >= %s''',
                (client_id, start_date)
            )
            csat_row  = cursor.fetchone() or {}
            positive  = int(csat_row.get('positive') or 0)
            negative  = int(csat_row.get('negative') or 0)
            total_rated = int(csat_row.get('total_rated') or 0)
            result['satisfaction'] = {
                'positive':          positive,
                'negative':          negative,
                'total_rated':       total_rated,
                'satisfaction_rate': (round(100 * positive / total_rated) if total_rated else None),
            }

            cursor.execute(
                '''SELECT EXTRACT(HOUR FROM timestamp)::int AS hour, COUNT(*) AS cnt
                   FROM conversations
                   WHERE client_id = %s AND timestamp >= %s
                   GROUP BY hour ORDER BY hour''',
                (client_id, start_date)
            )
            hour_counts = {int(r['hour']): int(r['cnt']) for r in cursor.fetchall()}
            result['peak_times'] = [
                {'hour': h, 'count': hour_counts.get(h, 0)} for h in range(24)
            ]

        cursor.close()
        conn.close()

        return jsonify({'success': True, 'analytics': result})

    except Exception as e:
        current_app.logger.error(f'Error getting analytics: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500


# ── Store (client) deletion ────────────────────────────────────────────────────
# NOTE: kept pending product confirmation — see removal report. In a strict
# one-account-one-store model this may belong on account.py's deletion flow
# instead of standing alone. Left as its pre-existing behaviour (delete this
# one owned client/store row) so nothing regresses silently.

@agency_bp.route('/api/clients/<client_id>/delete', methods=['POST', 'DELETE'])
@login_required
def delete_client_by_id(client_id):
    """Delete/disconnect the merchant's connected store."""
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


# ── Store dashboard (owner view) ────────────────────────────────────────────────
# Simplified: one store per account, so no ?client_id= selection and no
# client-portal-session fallback branch (client portal login is removed).

@agency_bp.route('/client-dashboard')
@login_required
def client_dashboard_router():
    """The merchant's own store dashboard: leads, FAQs, articles."""
    clients = models.get_user_clients(current_user.id)
    if not clients:
        return redirect(url_for('auth.dashboard'))
    client_id = clients[0]['client_id']

    client = models.get_client_by_id(client_id)
    if not client:
        return "Client not found", 404

    leads    = models.get_leads(client_id)
    faqs     = models.get_faqs(client_id)
    articles = models.get_articles(client_id)

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

    fresh_user  = models.get_user_by_id(current_user.id)
    plan_type   = (fresh_user or {}).get('plan_type', current_user.plan_type)
    plan_limits = _plan_limits.get(plan_type, _plan_limits['free'])
    usage_warn  = (models.get_usage_warning(client_id)
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
        analytics_level   = plan_limits.get('analytics_level', 'none'),
        usage_warning     = usage_warn,
    )
