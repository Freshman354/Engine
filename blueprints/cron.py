"""
blueprints/cron.py
------------------
All scheduled/cron job routes and their worker functions, plus the
admin-triggered variants and training data admin routes.

Extracted from app.py. All behaviour is identical to the original;
nothing has been changed except:
  - Route registration: Blueprint vs app
  - app.logger → current_app.logger
  - Inline `import time as _time` / `import hmac` promoted to module level
  - `__import__('hmac').compare_digest(...)` replaced with direct `hmac.compare_digest(...)`
  - Dependencies injected at registration time via init_cron()

Routes
------
  POST        /api/admin/enforce-subscriptions        admin_enforce_subscriptions
  GET/POST    /cron/enforce-subscriptions             cron_enforce_subscriptions
  GET/POST    /cron/weekly-digest                     cron_weekly_digest
  GET/POST    /cron/cleanup-logs                      cron_cleanup_logs
  GET/POST    /cron/stale-lead-nudge                  cron_stale_lead_nudge
  GET/POST    /cron/follow-up-reminders               cron_follow_up_reminders
  GET         /cron/status                            cron_status
  GET/POST    /cron/agency-overage                    cron_agency_overage
  GET         /api/admin/training/stats               admin_training_stats
  GET         /api/admin/training/export              admin_training_export
  POST        /api/admin/training/assign-splits       admin_assign_splits

Registration in app.py:
  from blueprints.cron import cron_bp, init_cron
  init_cron(
      mail=mail,
      enforce_subscriptions=enforce_subscriptions,
      agency_included_clients=AGENCY_INCLUDED_CLIENTS,
      agency_seat_price=AGENCY_SEAT_PRICE,
  )
  app.register_blueprint(cron_bp)
"""

import hmac
import io
import json
import os
import time

from datetime import datetime

from flask import Blueprint, jsonify, request, current_app
from flask_login import current_user, login_required
from flask_mail import Message as MailMessage

import models

# ── Blueprint ────────────────────────────────────────────────────────────────

cron_bp = Blueprint('cron', __name__)

# Injected dependencies — populated by init_cron() before first request.
_mail                    = None
_enforce_subscriptions   = None
_agency_included_clients = None
_agency_seat_price       = None


def init_cron(mail, enforce_subscriptions, agency_included_clients, agency_seat_price):
    """
    Called once in app.py after all shared objects are ready.
    Must be called before the first request reaches this blueprint.
    """
    global _mail, _enforce_subscriptions, _agency_included_clients, _agency_seat_price
    _mail                    = mail
    _enforce_subscriptions   = enforce_subscriptions
    _agency_included_clients = agency_included_clients
    _agency_seat_price       = agency_seat_price


# ── Shared secret validator ───────────────────────────────────────────────────

def _check_cron_secret():
    """
    Returns (secret_str, error_response_or_None).
    Validates the CRON_SECRET env var and the caller-supplied value.
    Call at the top of every public cron endpoint.
    """
    cron_secret = os.environ.get('CRON_SECRET', '').strip()
    if not cron_secret:
        current_app.logger.error(
            "[Cron] CRON_SECRET env var not set — endpoint disabled for safety."
        )
        return None, (jsonify({'error': 'Cron not configured'}), 503)

    provided = (
        request.args.get('secret', '') or
        (request.get_json(silent=True) or {}).get('secret', '')
    )
    if not hmac.compare_digest(provided, cron_secret):
        current_app.logger.warning(
            f"[Cron] Unauthorized attempt from {request.remote_addr}"
        )
        return None, (jsonify({'error': 'Unauthorized'}), 401)

    return cron_secret, None


# ── Weekly digest helpers ─────────────────────────────────────────────────────

def _build_digest_email_html(business_name: str, questions: list,
                              upgrade_url: str) -> str:
    rows = ''.join(
        f"""<tr>
          <td style="padding:10px 16px;border-bottom:1px solid #F0EBE1;font-size:14px;color:#1C1917;">
            {i}. {q['question']}
          </td>
          <td style="padding:10px 16px;border-bottom:1px solid #F0EBE1;font-size:13px;color:#A8A29E;text-align:right;">
            {q['count']}x this week
          </td>
        </tr>"""
        for i, q in enumerate(questions, 1)
    )
    return f"""
    <div style="font-family:'DM Sans',Arial,sans-serif;max-width:540px;margin:0 auto;background:#F7F4EF;padding:32px 20px;">
      <div style="text-align:center;margin-bottom:24px;">
        <span style="font-size:22px;font-weight:800;color:#1C1917;">Your weekly bot report 🤖</span>
      </div>
      <div style="background:#fff;border:1px solid #E7E2DA;border-radius:16px;overflow:hidden;">
        <div style="background:#1C1917;padding:20px 24px;">
          <p style="color:rgba(255,255,255,0.7);font-size:14px;margin:0;">
            Hi {business_name} team — here are the top questions your chatbot
            <strong style="color:#B8924A;">couldn't answer</strong> this week.
            Adding these to your knowledge base will help convert more visitors.
          </p>
        </div>
        <table style="width:100%;border-collapse:collapse;">
          <thead>
            <tr style="background:#F7F4EF;">
              <th style="padding:10px 16px;font-size:12px;font-weight:700;color:#A8A29E;text-align:left;text-transform:uppercase;letter-spacing:0.05em;">Question</th>
              <th style="padding:10px 16px;font-size:12px;font-weight:700;color:#A8A29E;text-align:right;text-transform:uppercase;letter-spacing:0.05em;">Frequency</th>
            </tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>
        <div style="padding:20px 24px;text-align:center;border-top:1px solid #F0EBE1;">
          <a href="{upgrade_url}" style="display:inline-block;padding:12px 28px;background:#B8924A;color:#fff;border-radius:10px;font-weight:700;font-size:14px;text-decoration:none;">
            Fix these gaps in your bot →
          </a>
          <p style="font-size:12px;color:#A8A29E;margin-top:12px;">
            Log in to your dashboard to add answers to these questions.
          </p>
        </div>
      </div>
      <p style="text-align:center;font-size:11px;color:#A8A29E;margin-top:20px;">
        Lumvi · <a href="{upgrade_url}/unsubscribe-digest" style="color:#A8A29E;">Unsubscribe from weekly digest</a>
      </p>
    </div>
    """


def send_weekly_digest():
    """
    Send each paid client's owner an email listing their top unanswered
    questions from the past 7 days.

    Deduplication: uses get_clients_for_weekly_digest_due() which only
    returns clients whose last_digest_sent_at is NULL or >6 days ago.
    After a successful send, marks the client with mark_digest_sent() so
    a repeated cron call in the same week is a no-op.
    """
    t0 = time.time()

    if not hasattr(models, 'get_clients_for_weekly_digest_due'):
        current_app.logger.warning(
            '[WeeklyDigest] dedup function not found — skipping'
        )
        return {'sent': 0, 'skipped': 0, 'errors': 0}

    clients  = models.get_clients_for_weekly_digest_due()
    sent = skipped = errors = 0
    base_url = os.environ.get('APP_BASE_URL', 'https://lumvi.net')

    for client in clients:
        cid   = client['client_id']
        biz   = client.get('business_name') or 'Your business'
        email = client.get('contact_email') or client.get('owner_email')
        if not email:
            skipped += 1
            continue

        questions = models.get_unanswered_questions_for_email(cid, since_days=7, limit=5)
        if not questions:
            skipped += 1
            continue

        try:
            html = _build_digest_email_html(
                business_name = biz,
                questions     = questions,
                upgrade_url   = f"{base_url}/dashboard?client_id={cid}",
            )
            msg = MailMessage(
                subject    = f"Your bot report: {len(questions)} questions it couldn't answer this week",
                recipients = [email],
                html       = html,
                sender     = os.environ.get('MAIL_DEFAULT_SENDER', 'hello@lumvi.net'),
            )
            if _mail:
                _mail.send(msg)
            models.mark_digest_sent(cid)
            sent += 1
            current_app.logger.info(
                f'[WeeklyDigest] sent to {email} client={cid}'
            )
        except Exception as e:
            current_app.logger.error(f'[WeeklyDigest] failed for {cid}: {e}')
            errors += 1

    duration_ms = int((time.time() - t0) * 1000)
    result = {'sent': sent, 'skipped': skipped, 'errors': errors}
    current_app.logger.info(
        f'[WeeklyDigest] complete — {result} dur={duration_ms}ms'
    )
    models.log_cron_run(
        'weekly_digest', success=(errors == 0), result=result,
        duration_ms=duration_ms, triggered_by='http',
    )
    return result


# ── Stale lead nudge ───────────────────────────────────────────────────────────

# Tunable window — a lead must be at least MIN hours old (and less than MAX
# hours old) and never touched since capture to qualify for one nudge.
STALE_LEAD_MIN_HOURS = 24
STALE_LEAD_MAX_HOURS = 48


def send_stale_lead_nudges():
    """
    Email the business owner about leads that have sat in 'new' for
    24-48 hours with no update (no PATCH has touched them since capture).
    Deduplicated via leads.stale_nudge_sent_at — each lead is nudged once.
    """
    t0 = time.time()
    if not hasattr(models, 'get_stale_new_leads'):
        current_app.logger.warning(
            '[StaleLeadNudge] models.get_stale_new_leads not found — skipping'
        )
        return {'notified': 0, 'skipped': 0, 'errors': 0}

    stale_leads = models.get_stale_new_leads(
        min_hours=STALE_LEAD_MIN_HOURS, max_hours=STALE_LEAD_MAX_HOURS
    )
    notified = skipped = errors = 0
    base_url = os.environ.get('APP_BASE_URL', 'https://lumvi.net')

    for lead in stale_leads:
        cid = lead['client_id']
        client = models.get_client_by_id(cid)
        if not client:
            skipped += 1
            continue

        config       = json.loads(client['branding_settings']) if client.get('branding_settings') else {}
        contact_info = config.get('contact', {})
        recipients   = [
            e.strip() for e in (contact_info.get('email') or '').split(',') if e.strip()
        ]
        if not recipients:
            skipped += 1
            continue

        try:
            sender_info = models.get_email_from_for_client(cid)
            lead_label  = lead.get('name') or lead.get('email') or 'A lead'
            html = f"""
            <div style="font-family:'DM Sans',Arial,sans-serif;max-width:520px;margin:0 auto;
                        background:#F7F4EF;padding:32px;border-radius:16px;">
              <h2 style="font-size:18px;font-weight:700;color:#1C1917;margin-bottom:4px;">
                ⏰ A lead is waiting on you</h2>
              <p style="color:#A8A29E;font-size:13px;margin-bottom:20px;">
                via {client.get('company_name','your chatbot')}</p>
              <div style="background:#fff;border:1px solid #E7E2DA;border-radius:12px;
                          padding:18px;margin-bottom:20px;">
                <p style="font-size:14px;font-weight:700;color:#1C1917;margin:0 0 4px;">
                  {lead_label}</p>
                <p style="font-size:13px;color:#57534E;margin:0;">{lead.get('email','')}</p>
              </div>
              <p style="font-size:13px;color:#57534E;line-height:1.6;">
                This lead has been sitting in <strong>New</strong> for over
                {STALE_LEAD_MIN_HOURS} hours with no follow-up yet.
              </p>
              <a href="{base_url}/admin/leads?client_id={cid}"
                 style="display:inline-block;margin-top:16px;padding:11px 22px;
                        background:#B8924A;color:#fff;text-decoration:none;
                        border-radius:9px;font-weight:700;font-size:13.5px;">
                Follow up now →</a>
            </div>"""
            msg = MailMessage(
                subject    = f"⏰ Lead waiting: {lead_label}",
                sender     = f"{sender_info['name']} <{sender_info['address']}>",
                recipients = recipients,
                html       = html,
            )
            if _mail:
                _mail.send(msg)
            models.mark_stale_nudge_sent(lead['id'])
            notified += 1
            current_app.logger.info(
                f"[StaleLeadNudge] notified client={cid} lead={lead['id']}"
            )
        except Exception as e:
            current_app.logger.error(
                f"[StaleLeadNudge] failed for lead={lead.get('id')}: {e}"
            )
            errors += 1

    duration_ms = int((time.time() - t0) * 1000)
    result = {'notified': notified, 'skipped': skipped, 'errors': errors}
    current_app.logger.info(f'[StaleLeadNudge] complete — {result} dur={duration_ms}ms')
    models.log_cron_run(
        'stale_lead_nudge', success=(errors == 0), result=result,
        duration_ms=duration_ms, triggered_by='http',
    )
    return result


# ── Follow-up reminder ──────────────────────────────────────────────────────────

def send_followup_reminders():
    """
    Email the business owner when a lead's scheduled follow_up_at date/time
    has been reached. Deduplicated via leads.followup_reminder_sent_at — each
    lead is reminded once per scheduled follow-up. Skips leads already in
    'closed' or 'lost' — nothing to follow up on there.
    """
    t0 = time.time()
    if not hasattr(models, 'get_due_follow_ups'):
        current_app.logger.warning(
            '[FollowUpReminder] models.get_due_follow_ups not found — skipping'
        )
        return {'notified': 0, 'skipped': 0, 'errors': 0}

    due_leads = models.get_due_follow_ups()
    notified = skipped = errors = 0
    base_url = os.environ.get('APP_BASE_URL', 'https://lumvi.net')

    for lead in due_leads:
        cid = lead['client_id']
        client = models.get_client_by_id(cid)
        if not client:
            skipped += 1
            continue

        config       = json.loads(client['branding_settings']) if client.get('branding_settings') else {}
        contact_info = config.get('contact', {})
        recipients   = [
            e.strip() for e in (contact_info.get('email') or '').split(',') if e.strip()
        ]
        if not recipients:
            skipped += 1
            continue

        try:
            sender_info = models.get_email_from_for_client(cid)
            lead_label  = lead.get('name') or lead.get('email') or 'A lead'
            html = f"""
            <div style="font-family:'DM Sans',Arial,sans-serif;max-width:520px;margin:0 auto;
                        background:#F7F4EF;padding:32px;border-radius:16px;">
              <h2 style="font-size:18px;font-weight:700;color:#1C1917;margin-bottom:4px;">
                📅 Follow-up reminder</h2>
              <p style="color:#A8A29E;font-size:13px;margin-bottom:20px;">
                via {client.get('company_name','your chatbot')}</p>
              <div style="background:#fff;border:1px solid #E7E2DA;border-radius:12px;
                          padding:18px;margin-bottom:20px;">
                <p style="font-size:14px;font-weight:700;color:#1C1917;margin:0 0 4px;">
                  {lead_label}</p>
                <p style="font-size:13px;color:#57534E;margin:0;">{lead.get('email','')}</p>
              </div>
              <p style="font-size:13px;color:#57534E;line-height:1.6;">
                You scheduled a follow-up with this lead for today.
              </p>
              <a href="{base_url}/admin/leads?client_id={cid}"
                 style="display:inline-block;margin-top:16px;padding:11px 22px;
                        background:#B8924A;color:#fff;text-decoration:none;
                        border-radius:9px;font-weight:700;font-size:13.5px;">
                Open lead →</a>
            </div>"""
            msg = MailMessage(
                subject    = f"📅 Follow-up due: {lead_label}",
                sender     = f"{sender_info['name']} <{sender_info['address']}>",
                recipients = recipients,
                html       = html,
            )
            if _mail:
                _mail.send(msg)
            models.mark_followup_reminder_sent(lead['id'])
            notified += 1
            current_app.logger.info(
                f"[FollowUpReminder] notified client={cid} lead={lead['id']}"
            )
        except Exception as e:
            current_app.logger.error(
                f"[FollowUpReminder] failed for lead={lead.get('id')}: {e}"
            )
            errors += 1

    duration_ms = int((time.time() - t0) * 1000)
    result = {'notified': notified, 'skipped': skipped, 'errors': errors}
    current_app.logger.info(f'[FollowUpReminder] complete — {result} dur={duration_ms}ms')
    models.log_cron_run(
        'followup_reminder', success=(errors == 0), result=result,
        duration_ms=duration_ms, triggered_by='http',
    )
    return result


# ── Agency overage billing ────────────────────────────────────────────────────

def bill_agency_overages():
    """
    Called monthly by /cron/agency-overage.
    For every agency user with more than AGENCY_INCLUDED_CLIENTS clients,
    calculate the extra seats, record a pending payment, and email a receipt.
    """
    t0 = time.time()
    if not hasattr(models, 'get_agency_users_with_overage'):
        current_app.logger.warning(
            '[AgencyOverage] models.get_agency_users_with_overage not found'
        )
        return {'billed': 0, 'skipped': 0, 'total_revenue': 0.0}

    agency_users  = models.get_agency_users_with_overage(_agency_included_clients)
    billed = skipped = 0
    total_revenue   = 0.0

    for u in agency_users:
        user_id      = u['id']
        email        = u['email']
        client_count = int(u['client_count'])
        extra_seats  = client_count - _agency_included_clients
        if extra_seats <= 0:
            skipped += 1
            continue

        amount = round(extra_seats * _agency_seat_price, 2)

        try:
            models.record_payment(
                user_id   = user_id,
                amount    = amount,
                plan_type = 'agency',
                provider  = 'overage',
                currency  = 'USD',
                status    = 'pending',
                notes     = (
                    f"Agency per-seat overage: {extra_seats} extra seat(s) "
                    f"× ${_agency_seat_price}/mo = ${amount:.2f}. "
                    f"Total clients: {client_count} "
                    f"(included: {_agency_included_clients})"
                ),
            )
            total_revenue += amount
            billed        += 1
            current_app.logger.info(
                f'[AgencyOverage] billed user={user_id} email={email} '
                f'extra_seats={extra_seats} amount=${amount}'
            )

            try:
                base_url = os.environ.get('APP_BASE_URL', 'https://lumvi.net')
                html = f"""
                <div style="font-family:Arial,sans-serif;max-width:520px;margin:0 auto;padding:24px;background:#F7F4EF;">
                  <div style="background:#fff;border:1px solid #E7E2DA;border-radius:16px;overflow:hidden;">
                    <div style="background:#1C1917;padding:20px 24px;">
                      <h2 style="color:#B8924A;font-size:18px;margin:0;">Agency Per-Seat Billing Notice</h2>
                    </div>
                    <div style="padding:24px;">
                      <p style="color:#57534E;font-size:14px;line-height:1.6;margin-bottom:16px;">
                        Hi {email},<br><br>
                        Your Lumvi Agency plan currently has <strong>{client_count} chatbots</strong>.
                        Your plan includes {_agency_included_clients} — the additional
                        <strong>{extra_seats} seat(s)</strong> are billed at
                        ${_agency_seat_price:.0f}/mo each.
                      </p>
                      <div style="background:#F7F4EF;border:1px solid #E7E2DA;border-radius:12px;padding:16px;margin-bottom:20px;">
                        <div style="display:flex;justify-content:space-between;margin-bottom:8px;">
                          <span style="color:#A8A29E;font-size:13px;">Included seats</span>
                          <span style="font-weight:700;color:#1C1917;">{_agency_included_clients}</span>
                        </div>
                        <div style="display:flex;justify-content:space-between;margin-bottom:8px;">
                          <span style="color:#A8A29E;font-size:13px;">Extra seats</span>
                          <span style="font-weight:700;color:#1C1917;">{extra_seats} × ${_agency_seat_price:.0f}</span>
                        </div>
                        <div style="display:flex;justify-content:space-between;border-top:1px solid #E7E2DA;padding-top:10px;margin-top:4px;">
                          <span style="color:#A8A29E;font-size:13px;">Overage charge</span>
                          <span style="font-weight:800;color:#B8924A;font-size:16px;">${amount:.2f}/mo</span>
                        </div>
                      </div>
                      <p style="color:#A8A29E;font-size:12px;">
                        This charge will be processed via your payment method on file.
                        To reduce your bill, archive unused chatbots from your
                        <a href="{base_url}/agency/clients" style="color:#B8924A;">Agency dashboard</a>.
                      </p>
                    </div>
                  </div>
                </div>"""
                msg = MailMessage(
                    subject    = f'Lumvi Agency billing: {extra_seats} extra seat(s) — ${amount:.2f}/mo',
                    recipients = [email],
                    html       = html,
                    sender     = os.environ.get('MAIL_DEFAULT_SENDER', 'hello@lumvi.net'),
                )
                if _mail:
                    _mail.send(msg)
            except Exception as mail_err:
                current_app.logger.error(
                    f'[AgencyOverage] email failed for {email}: {mail_err}'
                )

        except Exception as e:
            current_app.logger.error(
                f'[AgencyOverage] billing failed for user={user_id}: {e}'
            )
            skipped += 1

    duration_ms = int((time.time() - t0) * 1000)
    result = {
        'billed':        billed,
        'skipped':       skipped,
        'total_revenue': round(total_revenue, 2),
    }
    current_app.logger.info(
        f'[AgencyOverage] complete — billed={billed} skipped={skipped} '
        f'total_revenue=${total_revenue:.2f} dur={duration_ms}ms'
    )
    models.log_cron_run(
        'agency_overage', success=True, result=result,
        duration_ms=duration_ms, triggered_by='http',
    )
    return result


# ── Routes ───────────────────────────────────────────────────────────────────

@cron_bp.route('/api/admin/enforce-subscriptions', methods=['POST'])
@login_required
def admin_enforce_subscriptions():
    """Admin-only: manually trigger subscription enforcement."""
    user = models.get_user_by_id(current_user.id)
    if not user or not user.get('is_admin'):
        return jsonify({'success': False, 'error': 'Admin only'}), 403
    try:
        downgraded = models.downgrade_expired_users()
        current_app.logger.info(
            f"[Admin] manual downgrade run: {len(downgraded)} users"
        )
        return jsonify({
            'success':          True,
            'downgraded_count': len(downgraded),
            'downgraded_users': [
                {'id': u['id'], 'email': u.get('email'), 'plan': u.get('plan_type')}
                for u in downgraded
            ],
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@cron_bp.route('/cron/enforce-subscriptions', methods=['GET', 'POST'])
def cron_enforce_subscriptions():
    """
    Dyno-restart-safe daily cron endpoint.
    Secured by CRON_SECRET env var.

    Usage:
      GET  /cron/enforce-subscriptions?secret=YOUR_CRON_SECRET
      POST /cron/enforce-subscriptions  (body: {"secret": "YOUR_CRON_SECRET"})
    """
    _, err = _check_cron_secret()
    if err:
        return err

    downgraded = _enforce_subscriptions()
    return jsonify({
        'success':          True,
        'ran_at':           datetime.utcnow().isoformat(),
        'downgraded_count': len(downgraded),
    })


@cron_bp.route('/cron/weekly-digest', methods=['GET', 'POST'])
def cron_weekly_digest():
    """
    Weekly cron endpoint — sends unanswered-questions digest to all paid clients.
    Same CRON_SECRET as /cron/enforce-subscriptions.

    Usage:
      GET  /cron/weekly-digest?secret=YOUR_CRON_SECRET
      POST /cron/weekly-digest  (body: {"secret": "YOUR_CRON_SECRET"})
    """
    _, err = _check_cron_secret()
    if err:
        return err

    result = send_weekly_digest()
    return jsonify({
        'success': True,
        'ran_at':  datetime.utcnow().isoformat(),
        **result,
    })


@cron_bp.route('/cron/cleanup-logs', methods=['GET', 'POST'])
def cron_cleanup_logs():
    """
    Prune old webhook_logs (default >60 days) to keep the DB lean.
    Conversations are NEVER pruned — preserved for LLM fine-tuning.

    Recommended schedule: weekly (e.g. every Sunday at 03:00 UTC).

    Optional params:
      webhook_days  (default 60) — delete webhook_logs older than N days
    """
    _, err = _check_cron_secret()
    if err:
        return err

    body         = request.get_json(silent=True) or {}
    webhook_days = int(
        request.args.get('webhook_days', body.get('webhook_days', 60))
    )
    webhook_days = max(webhook_days, 7)   # never delete less than 7 days

    t0          = time.time()
    deleted     = models.prune_old_logs(webhook_days=webhook_days)
    duration_ms = int((time.time() - t0) * 1000)

    result = {**deleted, 'webhook_days': webhook_days}
    models.log_cron_run(
        'cleanup_logs', success=True, result=result,
        duration_ms=duration_ms, triggered_by='http',
    )
    current_app.logger.info(f'[CleanupLogs] {result} dur={duration_ms}ms')
    return jsonify({
        'success': True,
        'ran_at':  datetime.utcnow().isoformat(),
        **result,
    })


@cron_bp.route('/cron/stale-lead-nudge', methods=['GET', 'POST'])
def cron_stale_lead_nudge():
    """
    Daily cron — emails the business owner about leads stuck in 'new' for
    24-48 hours with no update. Same CRON_SECRET as other cron routes.

    Usage:
      GET  /cron/stale-lead-nudge?secret=YOUR_CRON_SECRET
      POST /cron/stale-lead-nudge  (body: {"secret": "YOUR_CRON_SECRET"})
    """
    _, err = _check_cron_secret()
    if err:
        return err

    result = send_stale_lead_nudges()
    return jsonify({
        'success': True,
        'ran_at':  datetime.utcnow().isoformat(),
        **result,
    })


@cron_bp.route('/cron/follow-up-reminders', methods=['GET', 'POST'])
def cron_follow_up_reminders():
    """
    Frequent cron (recommended hourly) — emails the business owner when a
    lead's scheduled follow_up_at date/time has arrived.

    Usage:
      GET  /cron/follow-up-reminders?secret=YOUR_CRON_SECRET
      POST /cron/follow-up-reminders  (body: {"secret": "YOUR_CRON_SECRET"})
    """
    _, err = _check_cron_secret()
    if err:
        return err

    result = send_followup_reminders()
    return jsonify({
        'success': True,
        'ran_at':  datetime.utcnow().isoformat(),
        **result,
    })


@cron_bp.route('/cron/status', methods=['GET'])
@login_required
def cron_status():
    """
    Returns last-run info for all cron jobs.
    Admin-only.
    """
    if not getattr(current_user, 'is_admin', False):
        return jsonify({'error': 'Admin only'}), 403

    jobs   = ['enforce_subscriptions', 'weekly_digest', 'agency_overage', 'cleanup_logs',
              'stale_lead_nudge', 'followup_reminder']
    status = {}
    for job in jobs:
        last = models.get_cron_last_run(job)
        if last:
            last['ran_at'] = (
                last['ran_at'].isoformat()
                if hasattr(last.get('ran_at'), 'isoformat')
                else str(last.get('ran_at', ''))
            )
        status[job] = last or {'ran_at': None, 'success': None, 'result': None}

    return jsonify({'success': True, 'cron_status': status})


@cron_bp.route('/cron/agency-overage', methods=['GET', 'POST'])
def cron_agency_overage():
    """
    Monthly cron — calculates and records per-seat overage charges.
    Recommended schedule: 1st of every month at 00:05 UTC.
    """
    _, err = _check_cron_secret()
    if err:
        return err

    result = bill_agency_overages()
    return jsonify({
        'success':       True,
        'ran_at':        datetime.utcnow().isoformat(),
        'billed':        result['billed'],
        'skipped':       result['skipped'],
        'total_revenue': result['total_revenue'],
    })


# ── Training data admin routes ────────────────────────────────────────────────

@cron_bp.route('/api/admin/training/stats', methods=['GET'])
@login_required
def admin_training_stats():
    """
    Training sample stats. Admin only.
    Query params:
      client_id — per-client breakdown; omit for global summary.
    """
    if not current_user.is_admin:
        return jsonify({'success': False, 'error': 'Admin only'}), 403
    try:
        from training_collector import get_training_stats
        client_id = request.args.get('client_id')
        stats     = get_training_stats(client_id) if client_id else get_training_stats()
        return jsonify({'success': True, 'stats': stats})
    except Exception as e:
        current_app.logger.error(f'[TrainingAdmin] stats error: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500


@cron_bp.route('/api/admin/training/export', methods=['GET'])
@login_required
def admin_training_export():
    """
    Export training samples as JSONL (Alpaca format) for fine-tuning.
    Query params:
      client_id   — filter to one client (optional; omit for all)
      split       — train / val / test (default: train)
      min_quality — float 0.0–1.0 (default: 0.5)
      limit       — max rows (default: 5000)
    Returns a downloadable .jsonl file.
    """
    if not current_user.is_admin:
        return jsonify({'success': False, 'error': 'Admin only'}), 403
    try:
        from training_collector import export_training_jsonl
        client_id   = request.args.get('client_id')
        split       = request.args.get('split', 'train')
        min_quality = float(request.args.get('min_quality', 0.5))
        limit       = int(request.args.get('limit', 5000))

        jsonl_str = export_training_jsonl(
            client_id   = client_id,
            split       = split,
            min_quality = min_quality,
            limit       = limit,
        )
        filename = (
            f"lumvi_training_{split}_{datetime.utcnow().strftime('%Y%m%d')}.jsonl"
        )
        return current_app.response_class(
            response    = jsonl_str,
            status      = 200,
            mimetype    = 'application/jsonl',
            headers     = {'Content-Disposition': f'attachment; filename={filename}'},
        )
    except Exception as e:
        current_app.logger.error(f'[TrainingAdmin] export error: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500


@cron_bp.route('/api/admin/training/assign-splits', methods=['POST'])
@login_required
def admin_assign_splits():
    """
    Assign train/val/test splits to all unassigned samples for a client.
    Body: { "client_id": "...", "train_pct": 0.8, "val_pct": 0.1 }
    """
    if not current_user.is_admin:
        return jsonify({'success': False, 'error': 'Admin only'}), 403
    try:
        from training_collector import assign_splits
        data      = request.get_json() or {}
        client_id = data.get('client_id')
        train_pct = float(data.get('train_pct', 0.8))
        val_pct   = float(data.get('val_pct', 0.1))
        if not client_id:
            return jsonify({'success': False, 'error': 'client_id required'}), 400
        assign_splits(client_id=client_id, train_pct=train_pct, val_pct=val_pct)
        return jsonify({'success': True, 'message': f'Splits assigned for {client_id}'})
    except Exception as e:
        current_app.logger.error(f'[TrainingAdmin] assign_splits error: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500
