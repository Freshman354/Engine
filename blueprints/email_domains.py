"""
blueprints/email_domains.py
============================
Agency custom email domain API routes.

POST   /api/agency/email-domain          — Register domain with Brevo, get DNS records
GET    /api/agency/email-domain          — Get current domain status + DNS records
POST   /api/agency/email-domain/verify   — Trigger DNS check + Brevo authentication
DELETE /api/agency/email-domain          — Remove domain (Brevo + DB)

All routes require login. The agency must be on pro/agency/enterprise plan
since custom domains are part of the white-label feature set.
"""

from flask import Blueprint, jsonify, request
from flask_login import current_user, login_required

import models
from services.brevo_domains import (
    add_domain,
    authenticate_domain,
    delete_domain  as brevo_delete_domain,
    is_domain_authenticated,
)
from services.dns_verifier import verify_domain_records
from utils import get_logger

logger = get_logger('lumvi.email_domains')

email_domains_bp = Blueprint('email_domains', __name__)

# Plans that include white-label (and therefore custom email domains)
_WHITE_LABEL_PLANS = {'pro', 'agency', 'enterprise'}


def _plan_allows_custom_domain() -> bool:
    user = models.get_user_by_id(current_user.id)
    plan = (user or {}).get('plan_type', 'free')
    return plan in _WHITE_LABEL_PLANS


# ── Register domain ────────────────────────────────────────────────────────────

@email_domains_bp.route('/api/agency/email-domain', methods=['POST'])
@login_required
def register_email_domain():
    """
    Body: { domain, from_name, from_email }

    Calls Brevo to register the domain and returns the DNS records
    the agency needs to add to their registrar. Stores in DB as 'pending'.
    """
    if not _plan_allows_custom_domain():
        return jsonify({
            'success': False,
            'error':   'Custom email domains require Pro or Agency plan',
        }), 403

    data       = request.get_json() or {}
    domain     = (data.get('domain') or '').strip().lower()
    from_name  = (data.get('from_name') or '').strip()
    from_email = (data.get('from_email') or '').strip().lower()

    if not domain:
        return jsonify({'success': False, 'error': 'Domain is required'}), 400
    if not from_email:
        return jsonify({'success': False, 'error': 'From email is required'}), 400
    if '@' not in from_email:
        return jsonify({'success': False, 'error': 'Invalid from email address'}), 400

    # Confirm from_email is on the claimed domain
    email_domain = from_email.split('@')[-1]
    if email_domain != domain:
        return jsonify({
            'success': False,
            'error':   f'From email must be on {domain} (got @{email_domain})',
        }), 400

    # Register with Brevo — get back the DNS records to display
    ok, err, dns = add_domain(domain)
    if not ok:
        logger.warning(
            f'[EmailDomain] Brevo add failed user={current_user.id} '
            f'domain={domain}: {err}'
        )
        return jsonify({'success': False, 'error': f'Brevo error: {err}'}), 502

    # Persist
    saved = models.upsert_agency_domain(
        user_id    = current_user.id,
        domain     = domain,
        from_name  = from_name,
        from_email = from_email,
        spf_host   = dns['spf_host'],
        spf_value  = dns['spf_value'],
        dkim_host  = dns['dkim_host'],
        dkim_value = dns['dkim_value'],
    )
    if not saved:
        return jsonify({'success': False, 'error': 'Database error — try again'}), 500

    logger.info(
        f'[EmailDomain] registered user={current_user.id} '
        f'domain={domain} from={from_email}'
    )
    return jsonify({
        'success':    True,
        'status':     'pending',
        'domain':     domain,
        'from_name':  from_name,
        'from_email': from_email,
        'dns_records': _format_dns_records(dns),
    })


# ── Get status ─────────────────────────────────────────────────────────────────

@email_domains_bp.route('/api/agency/email-domain', methods=['GET'])
@login_required
def get_email_domain():
    """Return current domain record with status and DNS records to display."""
    row = models.get_agency_domain(current_user.id)
    if not row:
        return jsonify({'success': True, 'domain': None})

    return jsonify({
        'success':    True,
        'domain':     row['domain'],
        'from_name':  row['from_name'],
        'from_email': row['from_email'],
        'status':     row['status'],
        'verified_at': str(row['verified_at']) if row.get('verified_at') else None,
        'dns_records': _format_dns_records({
            'spf_host':  row['spf_host'],
            'spf_value': row['spf_value'],
            'dkim_host': row['dkim_host'],
            'dkim_value': row['dkim_value'],
        }),
    })


# ── Verify domain ──────────────────────────────────────────────────────────────

@email_domains_bp.route('/api/agency/email-domain/verify', methods=['POST'])
@login_required
def verify_email_domain():
    """
    Two-step verification:
      1. Our DNS check (Cloudflare DoH) — confirms records are live
      2. Brevo authenticate call — Brevo does its own check + flips status

    We require step 1 to pass before calling step 2, to avoid hitting
    Brevo's API with domains that clearly aren't ready.
    """
    row = models.get_agency_domain(current_user.id)
    if not row:
        return jsonify({'success': False, 'error': 'No domain registered'}), 404

    domain    = row['domain']
    dkim_host = row['dkim_host']
    spf_host  = row['spf_host']

    # Step 1: our DNS check
    dns_ok, dns_msg = verify_domain_records(domain, spf_host, dkim_host)

    if not dns_ok:
        models.set_domain_status(current_user.id, 'failed')
        logger.info(
            f'[EmailDomain] DNS check failed user={current_user.id} '
            f'domain={domain}: {dns_msg}'
        )
        return jsonify({
            'success':   False,
            'status':    'failed',
            'detail':    dns_msg,
            'help':      'DNS records can take up to 48 hours to propagate. '
                         'Check you added them to the correct domain registrar.',
        })

    # Step 2: Brevo authentication
    brevo_ok, brevo_msg = authenticate_domain(domain)

    if brevo_ok or is_domain_authenticated(domain):
        models.set_domain_status(current_user.id, 'verified')
        logger.info(
            f'[EmailDomain] verified user={current_user.id} domain={domain}'
        )
        return jsonify({
            'success': True,
            'status':  'verified',
            'detail':  'Domain verified — lead emails will now come from your domain',
        })

    # DNS is live but Brevo hasn't flipped yet — mark pending, not failed
    models.set_domain_status(current_user.id, 'pending')
    logger.info(
        f'[EmailDomain] DNS ok but Brevo pending user={current_user.id} '
        f'domain={domain}: {brevo_msg}'
    )
    return jsonify({
        'success': False,
        'status':  'pending',
        'detail':  (
            f'DNS records confirmed. Brevo is still processing: {brevo_msg}. '
            f'Try verifying again in a few minutes.'
        ),
    })


# ── Delete domain ──────────────────────────────────────────────────────────────

@email_domains_bp.route('/api/agency/email-domain', methods=['DELETE'])
@login_required
def delete_email_domain():
    """Remove custom domain from DB and Brevo."""
    row = models.get_agency_domain(current_user.id)
    if not row:
        return jsonify({'success': True})

    domain = row['domain']

    # Remove from Brevo (best-effort)
    brevo_delete_domain(domain)

    # Remove from DB
    models.delete_agency_domain(current_user.id)

    logger.info(
        f'[EmailDomain] deleted user={current_user.id} domain={domain}'
    )
    return jsonify({'success': True})


# ── Helper ─────────────────────────────────────────────────────────────────────

def _format_dns_records(dns: dict) -> list:
    """Format DNS records for display in the frontend table."""
    return [
        {
            'type':  'TXT',
            'host':  dns.get('spf_host', ''),
            'value': dns.get('spf_value', ''),
            'label': 'SPF',
            'help':  'Authorises Brevo to send email from your domain',
        },
        {
            'type':  'CNAME',
            'host':  dns.get('dkim_host', ''),
            'value': dns.get('dkim_value', ''),
            'label': 'DKIM',
            'help':  'Enables cryptographic signing of emails from your domain',
        },
    ]
