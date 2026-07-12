"""
blueprints/inbox.py
-------------------
Human-handoff inbox: ticket listing, detail, status updates, counts, and
the background notification helper that fires when a handoff is created.

Extracted from app.py.  All behaviour is identical to the original; nothing
has been changed except the route registration mechanism (Blueprint vs app).

Dependencies injected at registration time via init_inbox():
  - mail         : Flask-Mail instance from app.py
  - fire_webhook : callable — fire_webhook_event(client_id, event_type, data)

Registration in app.py:
  from blueprints.inbox import inbox_bp, init_inbox
  init_inbox(mail=mail, fire_webhook=fire_webhook_event)
  app.register_blueprint(inbox_bp)
"""

import threading
from datetime import datetime

from flask import Blueprint, jsonify, render_template, request, current_app
from flask_login import current_user, login_required
from flask_mail import Message

import models
from app_utils import sanitize_input

# ── Blueprint ────────────────────────────────────────────────────────────────

inbox_bp = Blueprint('inbox', __name__)

# Injected dependencies — populated by init_inbox() before first request.
_mail         = None
_fire_webhook = None


def init_inbox(mail, fire_webhook):
    """
    Called once in app.py after the Mail instance and webhook dispatcher
    are ready.  Must be called before the first request reaches this blueprint.
    """
    global _mail, _fire_webhook
    _mail         = mail
    _fire_webhook = fire_webhook


# ── Handoff notification (background, non-blocking) ──────────────────────────

def notify_handoff(client_id, client, config, ticket_id, reason,
                   urgency, name, email, summary, method):
    """
    Notify the agency's contact email when a human handoff ticket is created.
    Fires in a background daemon thread — never blocks the chat response.
    Also fires the outbound CRM webhook if one is configured.

    Public so the chat blueprint can call it directly.
    """
    def _send():
        try:
            # FIX: this read the agency-inherited branding_settings.contact
            # blob exclusively — clients.notification_email (the dedicated,
            # validated column that manage_client_users.html actually saves
            # to) was never consulted at all. Whatever an agency configured
            # there had zero effect on where handoff emails went. Falls back
            # to the old contact.email for any client who's never touched
            # notification_email, so nothing breaks for existing clients.
            contact_info  = config.get('contact', {})
            notify_email  = (client or {}).get('notification_email') or contact_info.get('email')
            company_name  = (client or {}).get('company_name', 'your chatbot')
            urgency_label = '🔴 High' if urgency == 'high' else '🟡 Normal'
            customer_label = name or email or 'Unknown visitor'
            inbox_url = f'https://lumvi.net/inbox?client_id={client_id}&ticket={ticket_id}'

            if notify_email and _mail:
                try:
                    sender_info = models.get_email_from_for_client(client_id)
                    msg = Message(
                        subject=f"[{urgency_label}] Handoff needed — {customer_label}",
                        sender=f"{sender_info['name']} <{sender_info['address']}>",
                        recipients=[notify_email],
                        html=f"""
                        <div style="font-family:'DM Sans',sans-serif;max-width:560px;margin:0 auto;
                                    background:#F7F4EF;padding:36px;border-radius:16px;">
                          <h2 style="font-size:20px;font-weight:700;color:#1C1917;margin-bottom:4px;">
                            Human Handoff Requested</h2>
                          <p style="color:#A8A29E;font-size:13px;margin-bottom:24px;">
                            via {company_name} · Ticket
                            <code style="background:#E7E2DA;padding:2px 6px;border-radius:4px;">
                              {ticket_id}</code></p>
                          <table style="width:100%;border-collapse:collapse;margin-bottom:20px;">
                            <tr><td style="padding:10px 0;border-bottom:1px solid #E7E2DA;
                                           font-size:13px;color:#57534E;width:120px;">Customer</td>
                                <td style="padding:10px 0;border-bottom:1px solid #E7E2DA;
                                           font-size:13px;font-weight:600;color:#1C1917;">
                                  {customer_label}</td></tr>
                            {'<tr><td style="padding:10px 0;border-bottom:1px solid #E7E2DA;font-size:13px;color:#57534E;">Email</td><td style="padding:10px 0;border-bottom:1px solid #E7E2DA;font-size:13px;font-weight:600;"><a href="mailto:' + email + '" style="color:#B8924A;">' + email + '</a></td></tr>' if email else ''}
                            <tr><td style="padding:10px 0;border-bottom:1px solid #E7E2DA;
                                           font-size:13px;color:#57534E;">Urgency</td>
                                <td style="padding:10px 0;border-bottom:1px solid #E7E2DA;
                                           font-size:13px;font-weight:600;">{urgency_label}</td></tr>
                            <tr><td style="padding:10px 0;border-bottom:1px solid #E7E2DA;
                                           font-size:13px;color:#57534E;">Trigger</td>
                                <td style="padding:10px 0;border-bottom:1px solid #E7E2DA;
                                           font-size:13px;color:#1C1917;">{method}</td></tr>
                            <tr><td style="padding:10px 0;font-size:13px;color:#57534E;
                                          vertical-align:top;padding-top:14px;">Question</td>
                                <td style="padding:10px 0;padding-top:14px;font-size:13px;
                                           font-style:italic;color:#1C1917;">"{reason[:300]}"</td></tr>
                          </table>
                          {'<div style="background:#fff;border:1px solid #E7E2DA;border-radius:10px;padding:16px;margin-bottom:20px;"><p style="font-size:12px;color:#A8A29E;margin:0 0 8px;font-weight:600;text-transform:uppercase;letter-spacing:0.05em;">Conversation summary</p><pre style="font-size:12px;color:#57534E;white-space:pre-wrap;margin:0;line-height:1.6;">' + summary[:800] + '</pre></div>' if summary else ''}
                          <a href="{inbox_url}"
                             style="display:inline-block;margin-top:4px;padding:11px 22px;
                                    background:#B8924A;color:#fff;text-decoration:none;
                                    border-radius:9px;font-weight:700;font-size:13.5px;">
                            Open Inbox →</a>
                          <p style="font-size:11px;color:#A8A29E;margin-top:20px;">
                            Ticket ID: {ticket_id} · Lumvi Platform</p>
                        </div>"""
                    )
                    _mail.send(msg)
                    current_app.logger.info(
                        f"[Handoff] email sent ticket={ticket_id} to={notify_email}"
                    )
                except Exception as _mail_err:
                    current_app.logger.warning(
                        f"[Handoff] email failed ticket={ticket_id}: {_mail_err}"
                    )

            # Fire outbound CRM webhook via unified dispatcher
            if _fire_webhook:
                _fire_webhook(client_id, 'handoff_created', {
                    'ticket_id':      ticket_id,
                    'urgency':        urgency,
                    'reason':         reason,
                    'customer_name':  name,
                    'customer_email': email,
                    'method':         method,
                })

        except Exception as _outer_err:
            current_app.logger.error(
                f"[Handoff] notify_handoff thread error: {_outer_err}"
            )

    threading.Thread(target=_send, daemon=True).start()


# ── Routes ───────────────────────────────────────────────────────────────────

@inbox_bp.route('/inbox')
@login_required
def inbox_page():
    """Render the human inbox dashboard."""
    clients = models.get_user_clients(current_user.id)
    return render_template('inbox.html', user=current_user, clients=clients)


@inbox_bp.route('/api/inbox/<client_id>', methods=['GET'])
@login_required
def list_inbox_tickets(client_id):
    """
    List tickets for a client, sorted by urgency then time.
    Query params: status (open|in_progress|resolved|all), limit, offset
    """
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    status = request.args.get('status', 'open')
    limit  = min(int(request.args.get('limit',  50)), 200)
    offset = max(int(request.args.get('offset',  0)),   0)

    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        where  = "client_id = %s"
        params = [client_id]
        if status != 'all':
            where  += " AND status = %s"
            params.append(status)

        cursor.execute(
            f"""
            SELECT ticket_id, session_id, reason, customer_email, customer_name,
                   summary, urgency, status, assigned_to, resolution_notes,
                   created_at, updated_at
            FROM human_inbox
            WHERE {where}
            ORDER BY
                CASE urgency
                    WHEN 'urgent' THEN 1 WHEN 'high' THEN 2
                    WHEN 'normal' THEN 3 ELSE 4
                END,
                created_at DESC
            LIMIT %s OFFSET %s
            """,
            params + [limit, offset]
        )
        rows = cursor.fetchall()
        cursor.execute(
            f"SELECT COUNT(*) AS n FROM human_inbox WHERE {where}", params
        )
        total = cursor.fetchone()['n']
        tickets = [
            {
                'ticket_id':        row['ticket_id'],
                'session_id':       row['session_id'],
                'reason':           row['reason'],
                'customer_email':   row['customer_email'],
                'customer_name':    row['customer_name'],
                'summary':          row['summary'],
                'urgency':          row['urgency'],
                'status':           row['status'],
                'assigned_to':      row['assigned_to'],
                'resolution_notes': row['resolution_notes'],
                'created_at':  row['created_at'].isoformat() if row['created_at'] else '',
                'updated_at':  row['updated_at'].isoformat() if row['updated_at'] else '',
            }
            for row in rows
        ]
        return jsonify({
            'success': True, 'tickets': tickets,
            'total': total, 'limit': limit, 'offset': offset,
        }), 200
    except Exception as e:
        current_app.logger.error(f"[Inbox] list error client={client_id}: {e}")
        return jsonify({'success': False, 'error': 'Failed to load tickets'}), 500
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


@inbox_bp.route('/api/inbox/<client_id>/<ticket_id>', methods=['GET'])
@login_required
def get_inbox_ticket(client_id, ticket_id):
    """Return one ticket in full, including the conversation transcript."""
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            """
            SELECT ticket_id, session_id, reason, customer_email, customer_name,
                   summary, urgency, status, assigned_to, resolution_notes,
                   created_at, updated_at
            FROM human_inbox
            WHERE client_id = %s AND ticket_id = %s
            """,
            (client_id, ticket_id)
        )
        row = cursor.fetchone()
        if not row:
            return jsonify({'success': False, 'error': 'Ticket not found'}), 404

        ticket = {
            'ticket_id':        row['ticket_id'],
            'session_id':       row['session_id'],
            'reason':           row['reason'],
            'customer_email':   row['customer_email'],
            'customer_name':    row['customer_name'],
            'summary':          row['summary'],
            'urgency':          row['urgency'],
            'status':           row['status'],
            'assigned_to':      row['assigned_to'],
            'resolution_notes': row['resolution_notes'],
            'created_at':  row['created_at'].isoformat() if row['created_at'] else '',
            'updated_at':  row['updated_at'].isoformat() if row['updated_at'] else '',
        }

        # Pull the full conversation transcript for this session
        transcript = []
        if row['session_id']:
            cursor.execute(
                """
                SELECT user_message, bot_response, timestamp, method
                FROM conversations
                WHERE client_id = %s AND session_id = %s
                ORDER BY timestamp ASC
                LIMIT 100
                """,
                (client_id, row['session_id'])
            )
            for t in cursor.fetchall():
                if t['user_message']:
                    transcript.append({
                        'role': 'user', 'content': t['user_message'],
                        'time': t['timestamp'].isoformat() if t['timestamp'] else '',
                    })
                if t['bot_response']:
                    transcript.append({
                        'role': 'assistant', 'content': t['bot_response'],
                        'method': t.get('method', ''),
                        'time': t['timestamp'].isoformat() if t['timestamp'] else '',
                    })

        ticket['transcript'] = transcript
        return jsonify({'success': True, 'ticket': ticket}), 200
    except Exception as e:
        current_app.logger.error(f"[Inbox] get error ticket={ticket_id}: {e}")
        return jsonify({'success': False, 'error': 'Failed to load ticket'}), 500
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


@inbox_bp.route('/api/inbox/<client_id>/<ticket_id>', methods=['PATCH'])
@login_required
def update_inbox_ticket(client_id, ticket_id):
    """Update status, assigned_to, or resolution_notes on a ticket."""
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    data = request.get_json(force=True) or {}
    allowed_statuses = {'open', 'in_progress', 'resolved'}

    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            "SELECT status FROM human_inbox WHERE client_id = %s AND ticket_id = %s",
            (client_id, ticket_id)
        )
        row = cursor.fetchone()
        if not row:
            return jsonify({'success': False, 'error': 'Ticket not found'}), 404

        updates = {}
        new_status = (data.get('status') or '').lower().strip()
        if new_status:
            if new_status not in allowed_statuses:
                return jsonify({'success': False,
                                'error': f'status must be one of {allowed_statuses}'}), 400
            if row['status'] == 'resolved' and new_status != 'resolved':
                return jsonify({'success': False,
                                'error': 'Resolved tickets cannot be reopened'}), 400
            updates['status'] = new_status
        if 'assigned_to' in data:
            updates['assigned_to'] = sanitize_input(str(data['assigned_to'] or ''), 200)
        if 'resolution_notes' in data:
            updates['resolution_notes'] = sanitize_input(
                str(data['resolution_notes'] or ''), 2000
            )
        if not updates:
            return jsonify({'success': False, 'error': 'No fields to update'}), 400

        updates['updated_at'] = datetime.utcnow()
        set_clause = ', '.join(f"{k} = %s" for k in updates)
        cursor.execute(
            f"UPDATE human_inbox SET {set_clause} WHERE client_id = %s AND ticket_id = %s",
            list(updates.values()) + [client_id, ticket_id]
        )
        conn.commit()
        current_app.logger.info(
            f"[Inbox] ticket={ticket_id} updated by user={current_user.id} "
            f"fields={list(updates.keys())}"
        )
        return jsonify({'success': True, 'ticket_id': ticket_id}), 200
    except Exception as e:
        current_app.logger.error(f"[Inbox] update error ticket={ticket_id}: {e}")
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return jsonify({'success': False, 'error': 'Failed to update ticket'}), 500
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


@inbox_bp.route('/api/inbox/<client_id>/counts', methods=['GET'])
@login_required
def get_inbox_counts(client_id):
    """Open/in_progress/resolved counts — used by the sidebar badge."""
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            "SELECT status, COUNT(*) AS n FROM human_inbox WHERE client_id = %s GROUP BY status",
            (client_id,)
        )
        counts = {'open': 0, 'in_progress': 0, 'resolved': 0}
        for row in cursor.fetchall():
            if row['status'] in counts:
                counts[row['status']] = row['n']
        counts['total_open'] = counts['open'] + counts['in_progress']
        return jsonify({'success': True, 'counts': counts}), 200
    except Exception as e:
        current_app.logger.error(f"[Inbox] counts error client={client_id}: {e}")
        return jsonify({'success': False, 'error': 'Failed to load counts'}), 500
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()
