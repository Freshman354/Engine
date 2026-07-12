"""
blueprints/leads.py
-------------------
Lead capture, management, and inbound webhook routes.

Extracted from app.py. All behaviour is identical to the original;
nothing has been changed except:
  - Route registration: Blueprint vs app
  - app.logger → current_app.logger
  - Inline `import hmac` moved to module-level
  - Dependencies injected at registration time via init_leads()

Routes
------
  POST   /api/lead                                  submit_lead
  GET    /api/leads/<client_id>                     list_leads
  PATCH  /api/leads/<client_id>/<lead_id>           update_lead
  DELETE /api/leads/<client_id>/<lead_id>           delete_lead
  POST   /api/leads/<client_id>/bulk                bulk_update_leads
  POST   /api/leads/<client_id>/webhook-inbound     inbound_lead_webhook

Registration in app.py:
  from blueprints.leads import leads_bp, init_leads
  init_leads(mail=mail, limiter=limiter,
             fire_webhook=fire_webhook_event,
             notify_webhook=notify_webhook,
             log_conversation=log_conversation,
             ai_helper=ai_helper)
  app.register_blueprint(leads_bp)
"""

import csv
import hmac
import io
import json
import math

from flask import Blueprint, jsonify, make_response, request, current_app
from flask_login import current_user, login_required
from flask_mail import Message

import models
from app_utils import sanitize_input

# ── Blueprint ────────────────────────────────────────────────────────────────

leads_bp = Blueprint('leads', __name__)

# Injected dependencies — populated by init_leads() before first request.
_mail             = None
_limiter          = None
_fire_webhook     = None
_notify_webhook   = None
_log_conversation = None
_ai_helper        = None


def init_leads(mail, limiter, fire_webhook, notify_webhook, log_conversation, ai_helper=None):
    """
    Called once in app.py after all shared objects are ready.
    Must be called before the first request reaches this blueprint.

    ai_helper is optional — if not passed (or disabled), lead capture
    falls back to the existing behaviour with no intent summary and the
    DB default priority ('high').
    """
    global _mail, _limiter, _fire_webhook, _notify_webhook, _log_conversation, _ai_helper
    _mail             = mail
    _limiter          = limiter
    _fire_webhook     = fire_webhook
    _notify_webhook   = notify_webhook
    _log_conversation = log_conversation
    _ai_helper        = ai_helper


# ── Helpers ──────────────────────────────────────────────────────────────────

def _is_email(text):
    """Return True if text contains a valid email address."""
    import re
    pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    return re.search(pattern, text) is not None


def _fire_lead_stage_webhook(client_id, lead, old_stage):
    """Fire outbound webhook when a lead stage changes."""
    if _fire_webhook:
        _fire_webhook(client_id, 'lead_stage_changed', {
            'lead_id':     str(lead.get('id', '')),
            'name':        lead.get('name'),
            'email':       lead.get('email'),
            'old_stage':   old_stage,
            'new_stage':   lead.get('stage'),
            'notes':       lead.get('notes'),
            'assigned_to': lead.get('assigned_to'),
        })


# ── Routes ───────────────────────────────────────────────────────────────────

@leads_bp.route('/api/lead', methods=['POST'])
def submit_lead():
    # Rate limit applied via init_leads injected limiter
    if _limiter:
        _limiter.limit("10 per hour")(lambda: None)()

    try:
        data      = request.json
        client_id = sanitize_input(data.get('client_id', 'default'), max_length=50)
        name      = sanitize_input(data.get('name', ''), max_length=100)
        email     = sanitize_input(data.get('email', ''), max_length=200)
        phone     = sanitize_input(data.get('phone', ''), max_length=50)
        company   = sanitize_input(data.get('company', ''), max_length=100)
        message   = sanitize_input(data.get('message', ''), max_length=1000)

        custom_fields = data.get('custom_fields', {})
        if not isinstance(custom_fields, dict):
            custom_fields = {}

        if not name or not email:
            return jsonify({'success': False, 'error': 'Name and email are required'}), 400

        if not _is_email(email):
            return jsonify({'success': False, 'error': 'Invalid email format'}), 400

        client = models.get_client_by_id(client_id)
        if not client:
            return jsonify({'success': False, 'error': 'Client not found'}), 404

        config       = json.loads(client['branding_settings']) if client['branding_settings'] else {}
        contact_info = config.get('contact', {})
        vertical     = config.get('vertical', 'general')

        conversation_snippet = sanitize_input(
            data.get('conversation_snippet', ''), max_length=2000
        )

        # Gemini intent extraction — 2-3 sentence summary + auto priority.
        # Degrades silently to {summary: '', priority: 'high'} if ai_helper
        # is unavailable, disabled, or parsing fails (see extract_lead_intent).
        intent_summary = ''
        lead_priority  = 'high'
        if _ai_helper:
            try:
                intent = _ai_helper.extract_lead_intent(
                    message=message,
                    conversation_snippet=conversation_snippet,
                    vertical=vertical,
                )
                intent_summary = intent.get('summary', '')
                lead_priority  = intent.get('priority', 'high')
            except Exception as _intent_err:
                current_app.logger.warning(
                    f"[LeadIntent] extraction failed for {client_id}: {_intent_err}"
                )

        lead_data = {
            'name': name, 'email': email, 'phone': phone, 'company': company,
            'message': message,
            'custom_fields': custom_fields if custom_fields else None,
            'conversation_snippet': conversation_snippet,
            'source_url': data.get('source_url', ''),
            'intent_summary': intent_summary,
            'priority': lead_priority,
        }

        models.save_lead(client_id, lead_data)

        # lead_captured fires via notify_webhook shim → fire_webhook_event
        if _notify_webhook:
            _notify_webhook(client_id, {
                'name': name, 'email': email, 'phone': phone, 'company': company
            })

        # conversation_ended — a submitted lead marks end of a qualified session
        if _fire_webhook:
            _fire_webhook(client_id, 'conversation_ended', {
                'session_id':  data.get('session_id', ''),
                'outcome':     'lead_captured',
                'lead_name':   name,
                'lead_email':  email,
            })

        # Log to conversations table so lead submissions appear in analytics
        user_summary = f"[Lead Captured] Name: {name} | Email: {email}"
        if phone:
            user_summary += f" | Phone: {phone}"
        if company:
            user_summary += f" | Company: {company}"

        if _log_conversation:
            _log_conversation(
                client_id,
                user_summary,
                "Thank you! We've received your information and will be in touch soon.",
                matched=True,
                method='lead_captured',
                session_id=sanitize_input(
                    (request.json or {}).get('session_id', ''), max_length=100
                ) or None
            )

        current_app.logger.info(f'Lead captured for client: {client_id}')

        # Send branded lead notification — supports comma-separated recipients
        # FIX: same bug as inbox.py's notify_handoff — notification_email
        # (the dedicated column, actually saved by manage_client_users.html)
        # was never read; only the agency-inherited contact.email blob was.
        # Falls back to contact.email for clients who've never set
        # notification_email, so nothing breaks for existing clients.
        notify_email_raw = client.get('notification_email') or contact_info.get('email') or ''
        notify_recipients = [
            e.strip() for e in notify_email_raw.split(',')
            if e.strip()
        ]
        if notify_recipients and _mail:
            try:
                sender_info = models.get_email_from_for_client(client_id)
                msg = Message(
                    subject=f"New Lead: {name}",
                    sender=f"{sender_info['name']} <{sender_info['address']}>",
                    recipients=notify_recipients,
                    html=f"""
                    <div style="font-family:'DM Sans',sans-serif;max-width:520px;margin:0 auto;
                                background:#F7F4EF;padding:36px;border-radius:16px;">
                      <h2 style="font-size:20px;font-weight:700;color:#1C1917;margin-bottom:4px;">
                        New Lead Captured</h2>
                      <p style="color:#A8A29E;font-size:13px;margin-bottom:24px;">
                        via {client.get('company_name','your chatbot')}</p>
                      <table style="width:100%;border-collapse:collapse;">
                        <tr><td style="padding:10px 0;border-bottom:1px solid #E7E2DA;
                                       font-size:13px;color:#57534E;width:100px;">Name</td>
                            <td style="padding:10px 0;border-bottom:1px solid #E7E2DA;
                                       font-size:13px;font-weight:600;color:#1C1917;">{name}</td></tr>
                        <tr><td style="padding:10px 0;border-bottom:1px solid #E7E2DA;
                                       font-size:13px;color:#57534E;">Email</td>
                            <td style="padding:10px 0;border-bottom:1px solid #E7E2DA;
                                       font-size:13px;font-weight:600;color:#1C1917;">{email}</td></tr>
                        {'<tr><td style="padding:10px 0;border-bottom:1px solid #E7E2DA;font-size:13px;color:#57534E;">Phone</td><td style="padding:10px 0;border-bottom:1px solid #E7E2DA;font-size:13px;font-weight:600;color:#1C1917;"><a href="tel:'+phone+'" style="color:#B8924A;text-decoration:none;font-weight:600;">'+phone+'</a></td></tr>' if phone else ''}
                        {'<tr><td style="padding:10px 0;border-bottom:1px solid #E7E2DA;font-size:13px;color:#57534E;">Company</td><td style="padding:10px 0;border-bottom:1px solid #E7E2DA;font-size:13px;font-weight:600;color:#1C1917;">'+company+'</td></tr>' if company else ''}
                        {'<tr><td style="padding:10px 0;font-size:13px;color:#57534E;vertical-align:top;">Message</td><td style="padding:10px 0;font-size:13px;color:#1C1917;line-height:1.6;">'+message+'</td></tr>' if message else ''}
                      </table>
                      {'<div style="margin-top:20px;background:#F7F4EF;border:1px solid #E7E2DA;border-radius:10px;padding:16px 18px;"><p style="font-size:11px;text-transform:uppercase;letter-spacing:0.08em;color:#A8A29E;margin:0 0 8px;">Conversation Context</p><p style="font-size:13px;color:#57534E;line-height:1.7;margin:0;white-space:pre-wrap;">'+lead_data["conversation_snippet"]+'</p></div>' if lead_data.get('conversation_snippet') else ''}
                      <a href="https://lumvi.net/admin/leads?client_id={client_id}"
                         style="display:inline-block;margin-top:24px;padding:11px 22px;
                                background:#B8924A;color:#fff;text-decoration:none;
                                border-radius:9px;font-weight:700;font-size:13.5px;">
                        View All Leads →</a>
                    </div>"""
                )
                _mail.send(msg)
            except Exception as _mail_err:
                current_app.logger.warning(
                    f"[Lead email] failed for {client_id}: {_mail_err}"
                )

        return jsonify({
            'success': True,
            'message': "Thank you! We've received your information and will be in touch soon.",
            'contact_info': contact_info
        })

    except Exception as e:
        current_app.logger.error(f'Error submitting lead: {e}')
        return jsonify({'success': False, 'error': 'Failed to submit lead'}), 500


@leads_bp.route('/api/leads/<client_id>', methods=['GET'])
@login_required
def list_leads(client_id):
    """Return all leads for a client with optional stage/search filter."""
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    stage  = request.args.get('stage', '').strip()
    search = request.args.get('q', '').lower().strip()
    try:
        page     = max(1, int(request.args.get('page', 1)))
        per_page = min(100, max(1, int(request.args.get('per_page', 50))))
    except (ValueError, TypeError):
        return jsonify({'success': False, 'error': 'page and per_page must be integers'}), 400

    # Filtering and search pushed into SQL — no Python-side loops needed
    leads = models.get_leads(client_id, stage=stage, search=search)

    total = len(leads)
    leads = leads[(page - 1) * per_page: page * per_page]
    return jsonify({
        'success': True, 'leads': leads,
        'total': total, 'page': page, 'per_page': per_page
    })


@leads_bp.route('/api/leads/<client_id>/stage-labels', methods=['GET'])
@login_required
def get_stage_labels(client_id):
    """
    Return this client's custom pipeline stage display names (if any),
    configured via /api/admin/customize. Falls back to {} when unset —
    the dashboard already defaults to the standard 6 labels client-side.
    """
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    client = models.get_client_by_id(client_id)
    if not client:
        return jsonify({'success': False, 'error': 'Client not found'}), 404

    config = json.loads(client['branding_settings']) if client.get('branding_settings') else {}
    return jsonify({'success': True, 'stage_labels': config.get('stage_labels', {})})


@leads_bp.route('/api/leads/<client_id>/<int:lead_id>', methods=['PATCH'])
@login_required
def update_lead(client_id, lead_id):
    """
    Update a lead's stage, notes, assigned_to, priority, name, email, phone, or company.
    Fires the outbound webhook on stage change.
    """
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    data = request.json or {}
    valid_stages = {'new', 'contacted', 'qualified', 'proposal', 'closed', 'lost'}

    if 'stage' in data and data['stage'] not in valid_stages:
        return jsonify({'success': False, 'error': f'Invalid stage: {data["stage"]}'}), 400

    existing = models.get_lead_by_id(client_id, lead_id)
    if not existing:
        return jsonify({'success': False, 'error': 'Lead not found'}), 404

    old_stage     = existing.get('stage') or 'new'
    stage_changed = 'stage' in data and data['stage'] != old_stage

    # Require a reason when transitioning TO lost (not on subsequent edits of lost leads)
    if data.get('stage') == 'lost' and old_stage != 'lost':
        if not data.get('lost_reason', '').strip():
            return jsonify({'success': False,
                            'error': 'lost_reason is required when moving a lead to lost'}), 400

    # Require closed_value + outcome_notes when transitioning TO closed
    if data.get('stage') == 'closed' and old_stage != 'closed':
        raw_value = data.get('closed_value')
        if raw_value in (None, ''):
            return jsonify({'success': False,
                            'error': 'closed_value is required when moving a lead to closed'}), 400
        try:
            closed_value = float(raw_value)
        except (TypeError, ValueError):
            return jsonify({'success': False, 'error': 'closed_value must be a number'}), 400
        if not math.isfinite(closed_value) or closed_value < 0:
            return jsonify({'success': False,
                            'error': 'closed_value must be a non-negative number'}), 400
        data['closed_value'] = closed_value  # normalized — never store the raw client value
        if not data.get('outcome_notes', '').strip():
            return jsonify({'success': False,
                            'error': 'outcome_notes is required when moving a lead to closed'}), 400

    if stage_changed:
        action = f"Moved from {old_stage} → {data['stage']}"
    elif 'notes' in data:
        action = 'Notes updated'
    elif 'assigned_to' in data:
        action = f"Assigned to {data.get('assigned_to', '')}"
    else:
        action = 'Updated: ' + ', '.join(k for k in data if k not in ('_actor', '_action'))

    data['_actor']  = current_user.email
    data['_action'] = action

    updated = models.update_lead(client_id, lead_id, data)
    if updated is None:
        return jsonify({'success': False, 'error': 'Lead not found or update failed'}), 404

    if stage_changed:
        _fire_lead_stage_webhook(client_id, updated, old_stage)

    return jsonify({'success': True, 'lead': updated})


@leads_bp.route('/api/leads/<client_id>/<int:lead_id>', methods=['DELETE'])
@login_required
def delete_lead(client_id, lead_id):
    """Permanently delete a lead."""
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    ok = models.delete_lead_by_client(client_id, lead_id)
    if not ok:
        return jsonify({'success': False, 'error': 'Lead not found'}), 404

    current_app.logger.info(
        f'[LeadMgmt] deleted lead={lead_id} client={client_id} user={current_user.email}'
    )
    return jsonify({'success': True})


@leads_bp.route('/api/leads/<client_id>/bulk', methods=['POST'])
@login_required
def bulk_update_leads(client_id):
    """
    Bulk update multiple leads at once.
    Body: { lead_ids: [...], updates: { stage?, assigned_to?, priority? } }
    """
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    data     = request.json or {}
    lead_ids = data.get('lead_ids', [])
    updates  = data.get('updates', {})
    valid_stages = {'new', 'contacted', 'qualified', 'proposal', 'closed', 'lost'}

    if 'stage' in updates and updates['stage'] not in valid_stages:
        return jsonify({'success': False, 'error': 'Invalid stage'}), 400
    if not lead_ids:
        return jsonify({'success': False, 'error': 'No lead_ids provided'}), 400

    # Snapshot old stages BEFORE the bulk write so webhooks carry accurate data.
    old_stages: dict = {}
    if 'stage' in updates:
        for lid in lead_ids:
            existing = models.get_lead_by_id(client_id, lid)
            if existing:
                old_stages[lid] = existing.get('stage') or 'new'

    count = models.bulk_update_leads(client_id, lead_ids, updates, actor=current_user.email)

    if 'stage' in updates:
        for lid in lead_ids:
            lead = models.get_lead_by_id(client_id, lid)
            if lead:
                old = old_stages.get(lid, 'new')
                if old != updates['stage']:          # only fire on a real change
                    _fire_lead_stage_webhook(client_id, lead, old_stage=old)

    return jsonify({'success': True, 'updated': count})




# ── CSV EXPORT ────────────────────────────────────────────────────────────────

@leads_bp.route('/api/leads/<client_id>/export', methods=['GET'])
@login_required
def export_leads(client_id):
    """
    Return all leads for a client as a downloadable CSV file.
    Respects the same stage / search filters as list_leads.
    GET /api/leads/<client_id>/export?stage=new&q=smith
    """
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    stage  = request.args.get('stage', '').strip()
    search = request.args.get('q', '').lower().strip()
    leads  = models.get_leads(client_id, stage=stage, search=search)

    columns = [
        'id', 'name', 'email', 'phone', 'company', 'message',
        'stage', 'priority', 'intent_summary', 'assigned_to', 'notes',
        'lost_reason', 'follow_up_at', 'closed_value', 'outcome_notes',
        'source_url', 'created_at', 'updated_at',
    ]

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(columns)
    for lead in leads:
        writer.writerow([lead.get(col) or '' for col in columns])

    response = make_response(buf.getvalue())
    response.headers['Content-Type']        = 'text/csv; charset=utf-8'
    response.headers['Content-Disposition'] = f'attachment; filename="leads_{client_id}.csv"'
    return response

@leads_bp.route('/api/leads/<client_id>/webhook-inbound', methods=['POST'])
def inbound_lead_webhook(client_id):
    """
    External CRM / Zapier / Make can POST here to update a lead from their side.
    Auth: X-Lumvi-Secret header must match the client's configured inbound_webhook_secret.
    Body: { lead_id, stage?, notes?, assigned_to?, priority? }
    """
    client = models.get_client_by_id(client_id)
    if not client:
        return jsonify({'error': 'Client not found'}), 404

    config          = json.loads(client.get('branding_settings') or '{}')
    expected_secret = config.get('integrations', {}).get('inbound_webhook_secret', '')
    provided_secret = request.headers.get('X-Lumvi-Secret', '')

    # Constant-time comparison prevents timing oracle attacks on the secret.
    if not expected_secret or not hmac.compare_digest(provided_secret, expected_secret):
        return jsonify(
            {'error': 'Unauthorized — invalid or missing X-Lumvi-Secret header'}
        ), 401

    data        = request.json or {}
    lead_id_raw = data.get('lead_id', '')
    try:
        lead_id = int(lead_id_raw)
    except (ValueError, TypeError):
        return jsonify({'error': 'lead_id must be an integer'}), 400

    valid_stages = {'new', 'contacted', 'qualified', 'proposal', 'closed', 'lost'}
    if 'stage' in data and data['stage'] not in valid_stages:
        return jsonify(
            {'error': f'Invalid stage. Must be one of: {", ".join(sorted(valid_stages))}'}
        ), 400

    existing = models.get_lead_by_id(client_id, lead_id)
    if not existing:
        return jsonify({'error': 'Lead not found'}), 404

    old_stage = existing.get('stage') or 'new'
    allowed   = {'stage', 'notes', 'assigned_to', 'priority'}
    updates   = {k: v for k, v in data.items() if k in allowed}
    updates['_actor']  = 'external_webhook'
    updates['_action'] = 'Inbound webhook: ' + ', '.join(
        f"{k}={v}" for k, v in updates.items() if not k.startswith('_')
    )

    updated       = models.update_lead(client_id, lead_id, updates)
    stage_changed = 'stage' in updates and updates['stage'] != old_stage
    if stage_changed and updated:
        _fire_lead_stage_webhook(client_id, updated, old_stage)

    current_app.logger.info(
        f'[InboundWebhook] lead={lead_id} client={client_id} stage_changed={stage_changed}'
    )
    return jsonify({
        'success': True, 'lead_id': lead_id, 'stage': (updated or {}).get('stage')
    })
