"""
services/dns_verifier.py
========================
DNS record verification for custom agency email domains.

Uses Cloudflare's DNS-over-HTTPS API (1.1.1.1) — no additional
Python dependencies required beyond requests.

Two checks are run when an agency claims to have added DNS records:
  1. SPF TXT record on the root domain includes spf.brevo.com
  2. DKIM CNAME record exists at mail._domainkey.<domain>

Both must pass before status is set to 'verified'.
Cloudflare DoH is used rather than the system resolver because:
  - Railway's DNS caches aggressively — new records can take minutes
  - Cloudflare propagates faster than most system resolvers
  - No socket-level access restrictions on Railway
"""

import re
from typing import Tuple

import requests
from utils import get_logger

logger = get_logger('lumvi.dns_verifier')

_DOH_URL = 'https://cloudflare-dns.com/dns-query'
_TIMEOUT  = 8
_HEADERS  = {'Accept': 'application/dns-json'}


# ── Public API ─────────────────────────────────────────────────────────────────

def verify_domain_records(
    domain: str,
    spf_host: str,
    dkim_host: str,
) -> Tuple[bool, str]:
    """
    Run SPF + DKIM checks.

    Returns:
        (verified: bool, detail_message: str)

    detail_message is shown in the UI to help the agency diagnose issues.
    """
    domain = _clean_domain(domain)
    spf_ok,  spf_msg  = _check_spf(spf_host or domain)
    dkim_ok, dkim_msg = _check_dkim(dkim_host or f'mail._domainkey.{domain}')

    if spf_ok and dkim_ok:
        return True, 'SPF and DKIM records verified'

    parts = []
    if not spf_ok:
        parts.append(f'SPF: {spf_msg}')
    if not dkim_ok:
        parts.append(f'DKIM: {dkim_msg}')
    return False, ' · '.join(parts)


def check_spf_only(domain: str) -> Tuple[bool, str]:
    """Check only the SPF record — used for faster status polling."""
    return _check_spf(_clean_domain(domain))


def check_dkim_only(dkim_host: str) -> Tuple[bool, str]:
    """Check only the DKIM CNAME — used for faster status polling."""
    return _check_dkim(dkim_host)


# ── Checks ─────────────────────────────────────────────────────────────────────

def _check_spf(host: str) -> Tuple[bool, str]:
    """
    Verify TXT record on host contains 'include:spf.brevo.com'.
    Accepts any valid SPF record that includes Brevo's sending IPs.
    """
    try:
        r = requests.get(
            _DOH_URL,
            params={'name': host, 'type': 'TXT'},
            headers=_HEADERS,
            timeout=_TIMEOUT,
        )
        if not r.ok:
            return False, f'DNS lookup failed ({r.status_code})'

        answers = r.json().get('Answer', [])
        if not answers:
            return False, f'No TXT record found on {host}'

        for ans in answers:
            value = ans.get('data', '')
            if 'include:spf.brevo.com' in value:
                logger.debug(f'[DNS] SPF OK host={host}')
                return True, 'SPF record found'

        # Found TXT records but none include Brevo
        existing = [a.get('data', '')[:60] for a in answers[:2]]
        return False, (
            f'TXT record exists but missing include:spf.brevo.com. '
            f'Found: {existing}'
        )

    except requests.exceptions.Timeout:
        return False, 'DNS lookup timed out — try again'
    except Exception as e:
        logger.error(f'[DNS] SPF check error host={host}: {e}')
        return False, f'Lookup error: {e}'


def _check_dkim(host: str) -> Tuple[bool, str]:
    """
    Verify CNAME record exists at the DKIM hostname.
    A CNAME to Brevo's signing servers is what Brevo requires.
    """
    try:
        r = requests.get(
            _DOH_URL,
            params={'name': host, 'type': 'CNAME'},
            headers=_HEADERS,
            timeout=_TIMEOUT,
        )
        if not r.ok:
            return False, f'DNS lookup failed ({r.status_code})'

        answers = r.json().get('Answer', [])
        if not answers:
            # Check TXT as fallback (some providers publish DKIM as TXT)
            return _check_dkim_txt_fallback(host)

        target = answers[0].get('data', '')
        if 'brevo.com' in target.lower():
            logger.debug(f'[DNS] DKIM CNAME OK host={host} → {target}')
            return True, 'DKIM CNAME record found'

        # CNAME exists but doesn't point to Brevo
        return False, f'CNAME found but points to {target!r} (expected Brevo)'

    except requests.exceptions.Timeout:
        return False, 'DNS lookup timed out — try again'
    except Exception as e:
        logger.error(f'[DNS] DKIM check error host={host}: {e}')
        return False, f'Lookup error: {e}'


def _check_dkim_txt_fallback(host: str) -> Tuple[bool, str]:
    """Check TXT record as fallback for DKIM (some registrars use TXT, not CNAME)."""
    try:
        r = requests.get(
            _DOH_URL,
            params={'name': host, 'type': 'TXT'},
            headers=_HEADERS,
            timeout=_TIMEOUT,
        )
        answers = (r.json().get('Answer', []) if r.ok else [])
        if answers:
            return True, 'DKIM TXT record found'
        return False, f'No CNAME or TXT record found at {host}'
    except Exception:
        return False, f'No CNAME record found at {host}'


# ── Helpers ────────────────────────────────────────────────────────────────────

def _clean_domain(domain: str) -> str:
    """Strip protocol, www prefix, and trailing slashes."""
    domain = domain.lower().strip()
    domain = re.sub(r'^https?://', '', domain)
    domain = re.sub(r'^www\.', '', domain)
    return domain.rstrip('/')
