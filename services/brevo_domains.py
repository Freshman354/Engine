"""
services/brevo_domains.py
=========================
Brevo REST API wrapper for custom sender domain management.

Brevo lets you authenticate a custom domain so emails sent through
your Lumvi/Brevo account appear to come from agency@theirdomain.com,
not support@lumvi.net. This is the proper white-label email solution.

Flow:
  1. add_domain(domain)     → Brevo creates domain record, returns DNS records
  2. Agency adds DNS records to their registrar
  3. authenticate_domain()  → Brevo checks DNS and flips status to verified
  4. Lumvi sends emails with From: agency@theirdomain.com

Requires BREVO_API_KEY in Railway env vars.
Note: different from MAIL_USERNAME/MAIL_PASSWORD which are SMTP credentials.
Get your API key at: https://app.brevo.com/settings/keys/api
"""

import os
from typing import Dict, Optional, Tuple

import requests
from utils import get_logger

logger = get_logger('lumvi.brevo_domains')

_BASE    = 'https://api.brevo.com/v3'
_TIMEOUT = 10


def _headers() -> Dict[str, str]:
    return {
        'accept':       'application/json',
        'content-type': 'application/json',
        'api-key':      os.environ.get('BREVO_API_KEY', ''),
    }


def _api_key_set() -> bool:
    return bool(os.environ.get('BREVO_API_KEY', '').strip())


# ── Domain management ──────────────────────────────────────────────────────────

def add_domain(domain: str) -> Tuple[bool, str, Dict]:
    """
    Register a custom sender domain with Brevo.

    Returns:
        (success, error_message, dns_records)

    dns_records dict shape on success:
        {
            'spf_host':  'yourdomain.com',
            'spf_value': 'v=spf1 include:spf.brevo.com ~all',
            'dkim_host': 'mail._domainkey.yourdomain.com',
            'dkim_value': 'mail._domainkey.yourdomain.com.brevo.com.',
        }
    """
    if not _api_key_set():
        return False, 'BREVO_API_KEY not set', {}
    try:
        r = requests.post(
            f'{_BASE}/senders/domains',
            headers=_headers(),
            json={'name': domain},
            timeout=_TIMEOUT,
        )

        # 201 = created, 400 with "already exists" = we already registered it
        if r.status_code == 400:
            body = r.json() or {}
            if 'already' in str(body.get('message', '')).lower():
                return get_domain_dns_records(domain)
            return False, body.get('message', f'Brevo error {r.status_code}'), {}

        if not r.ok:
            return False, f'Brevo error {r.status_code}: {r.text[:200]}', {}

        data = r.json() or {}
        return _extract_dns_records(domain, data)

    except requests.exceptions.Timeout:
        return False, 'Brevo API timed out', {}
    except Exception as e:
        logger.error(f'[BrevoAPI] add_domain error domain={domain}: {e}')
        return False, str(e), {}


def get_domain_dns_records(domain: str) -> Tuple[bool, str, Dict]:
    """
    Fetch the DNS records Brevo needs for an already-registered domain.
    Called when the domain already exists in Brevo (409 on add).
    """
    if not _api_key_set():
        return False, 'BREVO_API_KEY not set', {}
    try:
        r = requests.get(
            f'{_BASE}/senders/domains/{domain}',
            headers=_headers(),
            timeout=_TIMEOUT,
        )
        if not r.ok:
            return False, f'Brevo error {r.status_code}', {}
        data = r.json() or {}
        return _extract_dns_records(domain, data)
    except Exception as e:
        logger.error(f'[BrevoAPI] get_domain error domain={domain}: {e}')
        return False, str(e), {}


def authenticate_domain(domain: str) -> Tuple[bool, str]:
    """
    Tell Brevo to check the DNS records for a domain and flip status.
    Call this after the agency confirms they've added the records.

    Returns (success, message).
    """
    if not _api_key_set():
        return False, 'BREVO_API_KEY not set'
    try:
        r = requests.put(
            f'{_BASE}/senders/domains/{domain}/authenticate',
            headers=_headers(),
            timeout=_TIMEOUT,
        )
        if r.status_code == 200:
            return True, 'Domain authenticated successfully'
        body = r.json() if r.content else {}
        msg  = body.get('message', f'Brevo status {r.status_code}')
        return False, msg
    except requests.exceptions.Timeout:
        return False, 'Brevo API timed out'
    except Exception as e:
        logger.error(f'[BrevoAPI] authenticate_domain error domain={domain}: {e}')
        return False, str(e)


def delete_domain(domain: str) -> bool:
    """Remove a custom domain from Brevo."""
    if not _api_key_set():
        return False
    try:
        r = requests.delete(
            f'{_BASE}/senders/domains/{domain}',
            headers=_headers(),
            timeout=_TIMEOUT,
        )
        return r.ok or r.status_code == 404
    except Exception as e:
        logger.error(f'[BrevoAPI] delete_domain error domain={domain}: {e}')
        return False


def is_domain_authenticated(domain: str) -> bool:
    """
    Check if a domain is already authenticated in Brevo.
    Used by the DNS re-check cron without triggering authentication.
    """
    if not _api_key_set():
        return False
    try:
        r = requests.get(
            f'{_BASE}/senders/domains/{domain}',
            headers=_headers(),
            timeout=_TIMEOUT,
        )
        if not r.ok:
            return False
        data = r.json() or {}
        return bool(data.get('authenticated'))
    except Exception as e:
        logger.error(f'[BrevoAPI] is_authenticated error domain={domain}: {e}')
        return False


# ── Internal helpers ───────────────────────────────────────────────────────────

def _extract_dns_records(domain: str, data: Dict) -> Tuple[bool, str, Dict]:
    """
    Normalise Brevo's domain response into a clean DNS records dict.
    Brevo's response shape has changed across API versions — handle both.
    """
    records: Dict = {}

    # SPF — always the same value regardless of domain
    spf_record = data.get('spfRecord') or data.get('spf_record') or {}
    records['spf_host']  = spf_record.get('hostName') or domain
    records['spf_value'] = (
        spf_record.get('value')
        or 'v=spf1 include:spf.brevo.com ~all'
    )

    # DKIM — CNAME pointing to Brevo's signing infrastructure
    dkim_record = data.get('dkimRecord') or data.get('dkim_record') or {}
    records['dkim_host']  = (
        dkim_record.get('hostName')
        or f'mail._domainkey.{domain}'
    )
    records['dkim_value'] = (
        dkim_record.get('value')
        or f'mail._domainkey.{domain}.brevo.com.'
    )

    logger.info(f'[BrevoAPI] DNS records extracted for domain={domain}')
    return True, '', records
