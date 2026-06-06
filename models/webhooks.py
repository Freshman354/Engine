"""
models/webhooks.py
------------------
Webhook config storage, signing secret management, and delivery log.
"""
import json
import secrets
import uuid
from datetime import datetime
from .db import get_db

def get_webhooks(client_id: str) -> list:
    """Return all webhook configs for a client."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            """SELECT webhook_id, name, url, events, enabled, signing_secret, created_at
               FROM webhook_configs WHERE client_id = %s ORDER BY created_at""",
            (client_id,)
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        result = []
        for r in rows:
            result.append({
                'webhook_id':     r['webhook_id'],
                'name':           r['name'],
                'url':            r['url'],
                'events':         json.loads(r['events'] or '[]'),
                'enabled':        bool(r['enabled']),
                'signing_secret': r['signing_secret'] or '',
                'created_at':     r['created_at'].isoformat() if r['created_at'] else None,
            })
        return result
    except Exception:
        return []


def save_webhooks(client_id: str, webhooks: list) -> int:
    """
    Replace all webhooks for a client. Preserves signing_secret when
    the caller doesn't send one (secret is managed separately).
    Returns count saved.
    """
    if not isinstance(webhooks, list):
        return 0
    conn, cursor = get_db()
    try:
        # Fetch existing secrets so we don't lose them on update
        cursor.execute(
            "SELECT webhook_id, signing_secret FROM webhook_configs WHERE client_id = %s",
            (client_id,)
        )
        existing_secrets = {r['webhook_id']: r['signing_secret'] for r in cursor.fetchall()}

        # Delete removed webhooks
        incoming_ids = [w.get('webhook_id') for w in webhooks if w.get('webhook_id')]
        cursor.execute(
            "DELETE FROM webhook_configs WHERE client_id = %s AND webhook_id <> ALL(%s)",
            (client_id, incoming_ids or ['__none__'])
        )

        # SSRF guard — reject private / loopback / non-HTTP(S) URLs at save time
        import ipaddress
        from urllib.parse import urlparse as _urlparse

        def _is_safe_url(url: str) -> bool:
            try:
                p = _urlparse(url)
                if p.scheme not in ('http', 'https'):
                    return False
                host = p.hostname or ''
                if not host:
                    return False
                if host in ('localhost', 'metadata.google.internal', '169.254.169.254'):
                    return False
                try:
                    addr = ipaddress.ip_address(host)
                    if addr.is_private or addr.is_loopback or addr.is_link_local or addr.is_reserved:
                        return False
                except ValueError:
                    pass   # hostname, not IP — allow
                return True
            except Exception:
                return False

        # Valid event names — reject unknown event types
        _VALID_EVENTS = {
            'lead_captured', 'lead_stage_changed', 'conversation_ended',
            'faq_matched', 'message_sent', 'handoff_created',
        }

        saved = 0
        for wh in webhooks:
            url = (wh.get('url') or '').strip()
            if not url or not _is_safe_url(url):
                import logging
                logging.getLogger(__name__).warning(
                    f'[save_webhooks] SSRF-blocked or invalid URL client={client_id} url={url!r}'
                )
                continue  # skip this webhook — don't save dangerous URLs

            # Sanitise event list against known valid events
            raw_events = wh.get('events') or ['lead_captured']
            if isinstance(raw_events, str):
                try:
                    import json as _json
                    raw_events = _json.loads(raw_events)
                except Exception:
                    raw_events = ['lead_captured']
            clean_events = [e for e in raw_events if e in _VALID_EVENTS] or ['lead_captured']

            wid    = wh.get('webhook_id') or str(uuid.uuid4())
            secret = existing_secrets.get(wid) or _generate_signing_secret()
            cursor.execute(
                """INSERT INTO webhook_configs
                       (client_id, webhook_id, name, url, events, enabled, signing_secret)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (webhook_id) DO UPDATE SET
                       name       = EXCLUDED.name,
                       url        = EXCLUDED.url,
                       events     = EXCLUDED.events,
                       enabled    = EXCLUDED.enabled,
                       updated_at = CURRENT_TIMESTAMP""",
                (
                    client_id, wid,
                    wh.get('name', 'Webhook')[:120],
                    url,
                    json.dumps(clean_events),
                    bool(wh.get('enabled', True)),
                    secret,
                )
            )
            saved += 1
        conn.commit()
        return saved
    except Exception as e:
        conn.rollback()
        import logging
        logging.getLogger(__name__).error(f"[save_webhooks] {e}")
        return 0
    finally:
        cursor.close()
        conn.close()


def get_signing_secret(client_id: str, webhook_id: str) -> str:
    """Return the signing secret for a specific webhook."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            "SELECT signing_secret FROM webhook_configs WHERE client_id = %s AND webhook_id = %s",
            (client_id, webhook_id)
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        return row['signing_secret'] if row else ''
    except Exception:
        return ''


def regenerate_signing_secret(client_id: str, webhook_id: str) -> str:
    """Generate and persist a new signing secret. Returns the new secret."""
    new_secret = _generate_signing_secret()
    try:
        conn, cursor = get_db()
        cursor.execute(
            """UPDATE webhook_configs SET signing_secret = %s, updated_at = CURRENT_TIMESTAMP
               WHERE client_id = %s AND webhook_id = %s""",
            (new_secret, client_id, webhook_id)
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception:
        pass
    return new_secret


def _generate_signing_secret() -> str:
    """32-byte hex signing secret (64 chars)."""
    return secrets.token_hex(32)


def log_webhook_delivery(client_id: str, webhook_id: str, event_type: str,
                         url: str, payload: dict, status_code: int,
                         response_text: str, success: bool, duration_ms: int) -> None:
    """Append one delivery record to webhook_logs."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            """INSERT INTO webhook_logs
                   (client_id, webhook_id, event_type, url, payload,
                    status_code, response, success, duration_ms)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                client_id, webhook_id, event_type, url,
                json.dumps(payload)[:4000],
                status_code,
                (response_text or '')[:1000],
                success,
                duration_ms,
            )
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception:
        pass


def get_webhook_logs(client_id: str, limit: int = 20) -> list:
    """Return latest webhook delivery logs for a client."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            """SELECT l.webhook_id, l.event_type, l.url, l.status_code,
                      l.response, l.success, l.duration_ms, l.fired_at,
                      c.name AS webhook_name
               FROM webhook_logs l
               LEFT JOIN webhook_configs c
                 ON l.webhook_id = c.webhook_id AND l.client_id = c.client_id
               WHERE l.client_id = %s
               ORDER BY l.fired_at DESC
               LIMIT %s""",
            (client_id, limit)
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        result = []
        for r in rows:
            result.append({
                'webhook_id':   r['webhook_id'],
                'webhook_name': r['webhook_name'] or 'Deleted webhook',
                'event_type':   r['event_type'],
                'url':          r['url'],
                'status_code':  r['status_code'],
                'response':     r['response'],
                'success':      bool(r['success']),
                'duration_ms':  r['duration_ms'],
                'fired_at':     r['fired_at'].isoformat() if r['fired_at'] else None,
            })
        return result
    except Exception:
        return []


# =====================================================================
# ADMIN DASHBOARD — SUPPLEMENTAL QUERIES
# Additive only. Pattern: cursor.close()/conn.close() inside try block.
# =====================================================================

_GEMINI_INPUT_PRICE_PER_TOKEN  = 0.075 / 1_000_000
_GEMINI_OUTPUT_PRICE_PER_TOKEN = 0.300 / 1_000_000


def _calc_cost(input_tokens, output_tokens):
    return (
        (input_tokens  or 0) * _GEMINI_INPUT_PRICE_PER_TOKEN +
        (output_tokens or 0) * _GEMINI_OUTPUT_PRICE_PER_TOKEN
    )


