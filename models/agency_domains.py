"""
models/agency_domains.py
========================
DB operations for agency custom email domains.

One domain per agency (user_id). Stores the domain name, desired from-email,
Brevo-generated DNS records, and verification status.

Status flow:
  pending → verified      (DNS records confirmed, Brevo authenticated)
  pending → failed        (DNS check ran but records not found)
  verified → pending      (agency changed domain)
"""

from typing import Dict, List, Optional
from .db import get_db
from utils import get_logger

logger = get_logger('lumvi.agency_domains')


# ── Write ──────────────────────────────────────────────────────────────────────

def upsert_agency_domain(
    user_id: int,
    domain: str,
    from_name: str,
    from_email: str,
    spf_host: str,
    spf_value: str,
    dkim_host: str,
    dkim_value: str,
) -> bool:
    """
    Insert or replace the agency's custom email domain record.
    Resets status to 'pending' whenever the domain changes.
    Returns True on success.
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute('''
            INSERT INTO agency_email_domains
                (user_id, domain, from_name, from_email,
                 spf_host, spf_value, dkim_host, dkim_value,
                 status, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'pending', NOW())
            ON CONFLICT (user_id) DO UPDATE SET
                domain      = EXCLUDED.domain,
                from_name   = EXCLUDED.from_name,
                from_email  = EXCLUDED.from_email,
                spf_host    = EXCLUDED.spf_host,
                spf_value   = EXCLUDED.spf_value,
                dkim_host   = EXCLUDED.dkim_host,
                dkim_value  = EXCLUDED.dkim_value,
                status      = 'pending',
                verified_at = NULL,
                last_check_at = NULL
        ''', (user_id, domain, from_name, from_email,
              spf_host, spf_value, dkim_host, dkim_value))
        conn.commit()
        logger.info(f'[AgencyDomain] upserted user={user_id} domain={domain}')
        return True
    except Exception as e:
        if conn:
            try: conn.rollback()
            except Exception: pass
        logger.error(f'[AgencyDomain] upsert error user={user_id}: {e}')
        return False
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def set_domain_status(
    user_id: int,
    status: str,            # 'pending' | 'verified' | 'failed'
) -> bool:
    """Update verification status. Sets verified_at when status=verified."""
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute('''
            UPDATE agency_email_domains
            SET    status        = %s,
                   last_check_at = NOW(),
                   verified_at  = CASE WHEN %s = 'verified' THEN NOW() ELSE verified_at END
            WHERE  user_id = %s
        ''', (status, status, user_id))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        if conn:
            try: conn.rollback()
            except Exception: pass
        logger.error(f'[AgencyDomain] status update error user={user_id}: {e}')
        return False
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def delete_agency_domain(user_id: int) -> bool:
    """Remove agency's custom domain record."""
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute(
            'DELETE FROM agency_email_domains WHERE user_id = %s', (user_id,)
        )
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        if conn:
            try: conn.rollback()
            except Exception: pass
        logger.error(f'[AgencyDomain] delete error user={user_id}: {e}')
        return False
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


# ── Read ───────────────────────────────────────────────────────────────────────

def get_agency_domain(user_id: int) -> Optional[Dict]:
    """
    Return the agency's custom domain record, or None.
    Used in _send_lead_email() to decide whether to use a custom from-address.
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute(
            'SELECT * FROM agency_email_domains WHERE user_id = %s', (user_id,)
        )
        row = cursor.fetchone()
        return dict(row) if row else None
    except Exception as e:
        logger.error(f'[AgencyDomain] get error user={user_id}: {e}')
        return None
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def get_verified_domain_for_client(client_id: str) -> Optional[Dict]:
    """
    Look up a verified custom domain via client → user join.
    Used in _send_lead_email() — returns None if no verified domain.
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute('''
            SELECT aed.*
            FROM   agency_email_domains aed
            JOIN   clients c ON c.user_id = aed.user_id
            WHERE  c.client_id = %s
            AND    aed.status  = 'verified'
            LIMIT  1
        ''', (client_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    except Exception as e:
        logger.error(f'[AgencyDomain] client lookup error client={client_id}: {e}')
        return None
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def get_all_pending_domains() -> List[Dict]:
    """Return all pending domains — used by cron to auto-recheck DNS."""
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute('''
            SELECT * FROM agency_email_domains
            WHERE  status = 'pending'
            ORDER  BY created_at ASC
        ''')
        return [dict(r) for r in cursor.fetchall()]
    except Exception as e:
        logger.error(f'[AgencyDomain] pending list error: {e}')
        return []
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()
