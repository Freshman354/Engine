"""
models/clients.py
-----------------
Client CRUD, ownership verification, suspension, cloning, white-label
settings, agency branding, custom domain DNS, and enriched stats.
"""
import json
import re
import uuid
from datetime import datetime
from .db import get_db

def create_client(user_id, company_name, branding_settings=None, vertical=None):
    """
    Create a new client for a user.
    If the owner is an agency and has agency_branding_settings, those are
    auto-applied to the new client unless branding_settings is explicitly passed.
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        import re as _re
        slug      = _re.sub(r'[^a-z0-9-]', '', company_name.lower().replace(' ', '-'))
        client_id = f"{slug}-{secrets.token_hex(4)}"

        # Auto-inherit agency defaults when nothing is passed
        if branding_settings is None:
            owner = get_user_by_id(user_id)
            agency_raw = (owner or {}).get('agency_branding_settings')
            if agency_raw:
                try:
                    agency_bs = json.loads(agency_raw) if isinstance(agency_raw, str) else agency_raw
                    # Deep-copy and personalise for this client
                    branding_settings = {
                        'branding': dict(agency_bs.get('branding', {})),
                        'bot_settings': dict(agency_bs.get('bot_settings', {})),
                        'contact': dict(agency_bs.get('contact', {})),
                        'integrations': {},
                        'vertical': vertical or agency_bs.get('vertical', 'general'),
                    }
                    # Reset company-specific fields so owner fills them in
                    branding_settings['branding']['company_name'] = company_name
                except Exception:
                    branding_settings = None

        if branding_settings is None:
            branding_settings = {
                'branding': {
                    'company_name': company_name,
                    'primary_color': '#B8924A',
                    'remove_branding': False,
                },
                'bot_settings': {
                    'bot_name': 'Support Assistant',
                    'welcome_message': 'Hi! How can I help you today?',
                },
                'contact': {},
                'integrations': {},
                'vertical': vertical or 'general',
            }

        primary_color = branding_settings.get('branding', {}).get('primary_color')
        welcome_msg   = branding_settings.get('bot_settings', {}).get('welcome_message')
        remove_flag   = bool(branding_settings.get('branding', {}).get('remove_branding', False))

        cursor.execute(
            '''INSERT INTO clients
                   (user_id, client_id, company_name, branding_settings,
                    widget_color, welcome_message, remove_branding)
               VALUES (%s, %s, %s, %s, %s, %s, %s)''',
            (user_id, client_id, company_name,
             json.dumps(branding_settings),
             primary_color, welcome_msg, remove_flag)
        )
        conn.commit()
        return client_id
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


def get_user_clients(user_id):
    """Get all clients for a user. Returns [] on DB error (never raises)."""
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute('SELECT * FROM clients WHERE user_id = %s', (user_id,))
        return [dict(c) for c in cursor.fetchall()]
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[get_user_clients] {e}')
        return []
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


# =====================================================================
# WHITE-LABEL HELPERS
# =====================================================================

_DOMAIN_RE = __import__('re').compile(
    r'^(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}$'
)

# Optional dnspython — used for accurate CNAME-chain DNS verification.
# Falls back to socket if not installed.
try:
    import dns.resolver as _dns_resolver
    import dns.exception as _dns_exception
    _HAS_DNSPYTHON = True
except ImportError:
    _HAS_DNSPYTHON = False

_LUMVI_TARGET = 'lumvi.net'   # canonical CNAME target agencies point to

def is_valid_domain(domain: str) -> bool:
    """Return True if `domain` looks like a valid hostname (no scheme, no path)."""
    if not domain or len(domain) > 253:
        return False
    return bool(_DOMAIN_RE.match(domain.strip()))


def check_domain_dns(domain: str) -> dict:
    """
    Walk the CNAME chain for `domain` and report whether it resolves to lumvi.net.

    Returns:
        {
            'pointed': bool,
            'message': str,          # human-readable status
            'chain':   list[str],    # CNAME hops discovered
        }

    Strategy:
      1. If dnspython is available, walk CNAME records (works correctly with
         Cloudflare-proxied domains where IP comparison fails).
      2. Fallback: compare resolved IPs against lumvi.net IPs via socket.
         This is less accurate but always available.
    """
    import socket

    domain = domain.strip().lower()
    chain  = []

    if _HAS_DNSPYTHON:
        resolver = _dns_resolver.Resolver()
        resolver.timeout  = 3
        resolver.lifetime = 6

        current = domain
        for _ in range(10):   # guard against infinite CNAME loops
            try:
                answers = resolver.resolve(current, 'CNAME')
                target  = str(answers[0].target).rstrip('.')
                chain.append(target)

                if target == _LUMVI_TARGET or target.endswith('.' + _LUMVI_TARGET):
                    return {
                        'pointed': True,
                        'message': f'✓ CNAME chain points to {_LUMVI_TARGET}',
                        'chain':   chain,
                    }
                current = target

            except _dns_exception.DNSException:
                # No CNAME at this hop — stop walking
                break

        # CNAME chain didn't reach lumvi.net; check if A records match
        try:
            lumvi_ips  = {r[4][0] for r in socket.getaddrinfo(_LUMVI_TARGET, None)}
            domain_ips = {r[4][0] for r in socket.getaddrinfo(domain, None)}
            if lumvi_ips & domain_ips:
                return {'pointed': True,  'message': '✓ Domain IP resolves to Lumvi', 'chain': chain}
            if domain_ips:
                return {'pointed': False, 'message': '⏳ Domain resolves but CNAME does not point to lumvi.net — check your DNS', 'chain': chain}
        except socket.gaierror:
            pass

        return {'pointed': False, 'message': '✗ Domain not found — add a CNAME record pointing to lumvi.net', 'chain': chain}

    # ── Fallback: socket IP comparison ───────────────────────────────────────
    # Less accurate for Cloudflare-proxied domains, but always available.
    try:
        lumvi_ips  = {r[4][0] for r in socket.getaddrinfo(_LUMVI_TARGET, None)}
        domain_ips = {r[4][0] for r in socket.getaddrinfo(domain, None)}
        if lumvi_ips & domain_ips:
            return {'pointed': True,  'message': '✓ Domain is pointing to Lumvi', 'chain': chain}
        if domain_ips:
            return {
                'pointed': False,
                'message': (
                    "⏳ Domain resolves but doesn't match Lumvi's IP — if you're using Cloudflare, "
                    "install dnspython on the server for accurate CNAME detection"
                ),
                'chain': chain,
            }
        return {'pointed': False, 'message': '✗ Domain not found — check your DNS records', 'chain': chain}
    except socket.gaierror:
        return {'pointed': False, 'message': '✗ Domain not found — check your DNS records', 'chain': chain}


def get_client_by_custom_domain(domain: str):
    """
    Look up a client whose custom_widget_domain matches `domain`.
    Used by the /widget route to serve white-labelled widgets on custom domains.
    """
    try:
        conn, cursor = get_db()
        cursor.execute(
            'SELECT * FROM clients WHERE custom_widget_domain = %s',
            (domain.lower().strip(),)
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        return dict(row) if row else None
    except Exception:
        return None


def save_white_label_settings(client_id: str, domain: str | None,
                               custom_css: str | None,
                               branded_email_from: str | None) -> None:
    """
    Persist white-label columns for a client.

    Sentinel semantics (per field):
      None        → leave existing value untouched  (field not in request payload)
      ''          → explicitly clear  (set NULL in DB)
      'some.value'→ update to new value

    This lets callers clear a custom domain by passing domain='' rather than
    being trapped by the old COALESCE-always-keeps behaviour.
    """
    _SKIP = object()   # BUG-09 fix: must be defined before _val closure references it

    def _val(v):
        """Convert sentinel: None→skip, ''→NULL, str→str."""
        if v is None:
            return _SKIP
        return None if v == '' else v

    sets, params = [], []
    d  = _val(domain)
    c  = _val(custom_css)
    ef = _val(branded_email_from)

    if d  is not _SKIP: sets.append('custom_widget_domain = %s'); params.append(d)
    if c  is not _SKIP: sets.append('custom_css           = %s'); params.append(c)
    if ef is not _SKIP: sets.append('branded_email_from   = %s'); params.append(ef)

    if not sets:
        return  # nothing to do

    params.append(client_id)
    sql = f"UPDATE clients SET {', '.join(sets)} WHERE client_id = %s"

    try:
        conn, cursor = get_db()
        cursor.execute(sql, params)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[save_white_label_settings] {e}")


def get_email_from_for_client(client_id: str) -> dict:
    """
    Return the branded email sender info for a client.

    Priority:
      1. client.branded_email_from
      2. owner agency_branding_settings.branded_email_from
      3. Lumvi default

    Returns {'name': str, 'address': str}
    """
    DEFAULT = {'name': 'Lumvi', 'address': 'support@lumvi.net'}
    try:
        conn, cursor = get_db()
        cursor.execute(
            '''SELECT c.branded_email_from, c.branding_settings,
                      u.agency_branding_settings, c.company_name
               FROM clients c
               JOIN users u ON u.id = c.user_id
               WHERE c.client_id = %s''',
            (client_id,)
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if not row:
            return DEFAULT

        # 1. Client-level override
        if row.get('branded_email_from'):
            return {'name': row['branded_email_from'], 'address': 'support@lumvi.net'}

        # 2. Agency default
        agency_raw = row.get('agency_branding_settings')
        if agency_raw:
            try:
                ab = json.loads(agency_raw) if isinstance(agency_raw, str) else agency_raw
                agency_from = ab.get('branded_email_from')
                if agency_from:
                    return {'name': agency_from, 'address': 'support@lumvi.net'}
            except Exception:
                pass

        # 3. Use company name as a friendly default
        company = row.get('company_name')
        if company:
            return {'name': company, 'address': 'support@lumvi.net'}

        return DEFAULT
    except Exception:
        return DEFAULT


def save_agency_branding(user_id: int, agency_branding: dict) -> None:
    """Persist the agency-wide branding defaults for a user."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            "UPDATE users SET agency_branding_settings = %s WHERE id = %s",
            (json.dumps(agency_branding), user_id)
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[save_agency_branding] {e}")


def get_agency_branding(user_id: int) -> dict:
    """Return the agency's default branding dict, or {}."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            "SELECT agency_branding_settings FROM users WHERE id = %s", (user_id,)
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        raw = (row or {}).get('agency_branding_settings')
        if raw:
            return json.loads(raw) if isinstance(raw, str) else raw
        return {}
    except Exception:
        return {}

def get_client_by_id(client_id):
    """Get client by client_id. Returns None on missing row or DB error."""
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute('SELECT * FROM clients WHERE client_id = %s', (client_id,))
        client = cursor.fetchone()
        return dict(client) if client else None
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[get_client_by_id] {e}')
        return None
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass

def verify_client_ownership(user_id, client_id):
    """Verify that a user owns a client. Returns False on DB error."""
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute(
            'SELECT id FROM clients WHERE client_id = %s AND user_id = %s',
            (client_id, user_id)
        )
        return cursor.fetchone() is not None
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[verify_client_ownership] {e}')
        return False
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


def delete_client(client_id):
    """
    Cascade-delete a client and all its associated data.
    Order matters for FK constraints — client row must be last.
    """
    conn, cursor = get_db()
    try:
        # FK-constrained tables first
        cursor.execute('DELETE FROM conversations         WHERE client_id = %s', (client_id,))
        cursor.execute('DELETE FROM leads                 WHERE client_id = %s', (client_id,))
        cursor.execute('DELETE FROM faqs                  WHERE client_id = %s', (client_id,))
        # BUG-08 fix: orphaned tables that were previously missed
        cursor.execute('DELETE FROM knowledge_base        WHERE client_id = %s', (client_id,))
        cursor.execute('DELETE FROM faq_embeddings        WHERE client_id = %s', (client_id,))
        cursor.execute('DELETE FROM conversation_summaries WHERE client_id = %s', (client_id,))
        cursor.execute('DELETE FROM chat_sessions          WHERE client_id = %s', (client_id,))
        cursor.execute('DELETE FROM kb_gaps                WHERE client_id = %s', (client_id,))
        cursor.execute('DELETE FROM poor_answers           WHERE client_id = %s', (client_id,))
        cursor.execute('DELETE FROM webhook_configs        WHERE client_id = %s', (client_id,))
        cursor.execute('DELETE FROM webhook_logs           WHERE client_id = %s', (client_id,))
        # Client row last
        cursor.execute('DELETE FROM clients               WHERE client_id = %s', (client_id,))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def toggle_client_suspended(client_id: str, suspend: bool) -> bool:
    """Set is_suspended for a client. Returns True on success."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            "UPDATE clients SET is_suspended = %s WHERE client_id = %s",
            (suspend, client_id)
        )
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[toggle_client_suspended] {e}")
        return False


def clone_client(source_client_id: str, user_id: int, new_name: str) -> str | None:
    """
    Clone a client: copies branding_settings and all FAQs to a new client.
    Returns the new client_id or None on failure.
    """
    import re as _re
    try:
        conn, cursor = get_db()

        # Fetch source
        cursor.execute('SELECT * FROM clients WHERE client_id = %s', (source_client_id,))
        source = cursor.fetchone()
        if not source:
            cursor.close(); conn.close()
            return None

        # Create new client_id
        slug       = _re.sub(r'[^a-z0-9-]', '', new_name.lower().replace(' ', '-'))
        new_cid    = f"{slug}-{secrets.token_hex(4)}"
        bs         = source.get('branding_settings') or '{}'

        cursor.execute(
            '''INSERT INTO clients
                   (user_id, client_id, company_name, branding_settings,
                    widget_color, welcome_message, remove_branding)
               VALUES (%s, %s, %s, %s, %s, %s, %s)''',
            (user_id, new_cid, new_name, bs,
             source.get('widget_color'), source.get('welcome_message'),
             source.get('remove_branding', False))
        )

        # Clone FAQs
        cursor.execute('SELECT * FROM faqs WHERE client_id = %s', (source_client_id,))
        faqs = cursor.fetchall()
        for faq in faqs:
            new_faq_id = f"faq-{secrets.token_hex(4)}"
            cursor.execute(
                '''INSERT INTO faqs
                       (client_id, faq_id, question, answer, triggers, category,
                        quality_score, tags, is_active)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                (new_cid, new_faq_id,
                 faq.get('question', ''), faq.get('answer', ''),
                 faq.get('triggers', '[]'), faq.get('category', 'General'),
                 faq.get('quality_score', 0.0), faq.get('tags', '[]'), True)
            )

        conn.commit()
        cursor.close()
        conn.close()
        return new_cid
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[clone_client] {e}")
        return None


def get_clients_enriched_stats(client_ids: list) -> dict:
    """
    Fetch stats for multiple clients in bulk — one query per metric
    instead of N×4 individual queries. Used by /agency/clients.

    Returns dict keyed by client_id:
    {
        'faqs_count':    int,
        'leads_count':   int,
        'conversations': int,
        'daily_msgs':    int,
        'last_active':   datetime | None,
    }
    """
    if not client_ids:
        return {}

    # Build a default result so every client_id is always present
    result = {
        cid: {
            'faqs_count':    0,
            'leads_count':   0,
            'conversations': 0,
            'daily_msgs':    0,
            'last_active':   None,
        }
        for cid in client_ids
    }

    try:
        conn, cursor = get_db()
        today = datetime.utcnow().strftime('%Y-%m-%d')

        # ── FAQs per client ───────────────────────────────────────────
        cursor.execute(
            """SELECT client_id, COUNT(*) AS cnt
               FROM faqs
               WHERE client_id = ANY(%s) AND is_active = TRUE
               GROUP BY client_id""",
            (client_ids,)
        )
        for row in cursor.fetchall():
            result[row['client_id']]['faqs_count'] = int(row['cnt'])

        # ── Leads per client ──────────────────────────────────────────
        cursor.execute(
            """SELECT client_id, COUNT(*) AS cnt
               FROM leads
               WHERE client_id = ANY(%s)
               GROUP BY client_id""",
            (client_ids,)
        )
        for row in cursor.fetchall():
            result[row['client_id']]['leads_count'] = int(row['cnt'])

        # ── Total conversations + last active ─────────────────────────
        cursor.execute(
            """SELECT client_id,
                      COUNT(*) AS cnt,
                      MAX(timestamp) AS last_ts
               FROM conversations
               WHERE client_id = ANY(%s)
               GROUP BY client_id""",
            (client_ids,)
        )
        for row in cursor.fetchall():
            result[row['client_id']]['conversations'] = int(row['cnt'])
            result[row['client_id']]['last_active']   = row['last_ts']

        # ── Daily messages (today only) ───────────────────────────────
        cursor.execute(
            """SELECT client_id, COUNT(*) AS cnt
               FROM conversations
               WHERE client_id = ANY(%s)
                 AND DATE(timestamp) = %s
               GROUP BY client_id""",
            (client_ids, today)
        )
        for row in cursor.fetchall():
            result[row['client_id']]['daily_msgs'] = int(row['cnt'])

        cursor.close()
        conn.close()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[get_clients_enriched_stats] {e}")

    return result


def get_all_clients() -> list:
    """
    Return every row from the clients table as a list of dicts.

    Used by reindex_all_clients() in ai_helper.py to discover all client_ids
    without needing to know them upfront.

    Each dict contains every column from the clients table. The fields
    reindex_all_clients() uses are:
        client_id  — TEXT UNIQUE — the identifier passed to index_faqs()
        company_name — for logging / progress reporting

    Returns [] on any DB error so the caller can log and continue.
    """
    try:
        conn, cursor = get_db()
        try:
            cursor.execute(
                """
                SELECT client_id, company_name, user_id, created_at
                FROM   clients
                ORDER  BY created_at ASC
                """
            )
            rows = cursor.fetchall()
        finally:
            cursor.close()
            conn.close()

        return [dict(r) for r in rows]

    except Exception as _e:
        import logging as _logging
        _logging.getLogger('lumvi.models').error(
            f"[get_all_clients] DB error: {type(_e).__name__}: {_e}",
            exc_info=True,
        )
        return []


if __name__ == '__main__':
    init_db()
