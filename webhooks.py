"""
webhooks.py — Lumvi Platform Webhook Ingestion Layer
=====================================================
Receives inbound webhooks from Shopify, Acuity Scheduling, Calendly,
WooCommerce, and Square, verifies their signatures, normalises the
payloads, and upserts data into Lumvi's orders / appointment_slots /
appointments tables so tools.py can serve real answers to end users.

Architecture
------------
                  Shopify store ──► POST /webhooks/shopify/<client_id>
                  WooCommerce store ► POST /webhooks/woocommerce/<client_id>
                                              │
                  Acuity account ──► POST /webhooks/acuity/<client_id>
                  Calendly account ► POST /webhooks/calendly/<client_id>
                  Square account ──► POST /webhooks/square/<client_id>
                                              │
                                     webhooks.py (this file)
                                              │
                                    verify_signature()
                                    normalise_payload()
                                    upsert into DB
                                              │
                                    tools.py  lookup_order()
                                              check_availability()
                                              book_appointment()

Registration / Setup
--------------------
Each Lumvi client has one row in client_integrations keyed by
(client_id, platform). That row stores:
  • webhook_secret  — used to verify HMAC signatures
  • platform_config — JSON blob for any extra per-platform settings

The merchant sets this up once in the Lumvi dashboard, then pastes
the generated webhook URL into their platform's settings. No ongoing
maintenance needed.

Signature schemes (all verified against each platform's own docs —
see the module-level comment above each _verify_*_signature function
for the exact source):
  • Shopify:     HMAC-SHA256, base64, header X-Shopify-Hmac-Sha256
  • Acuity:      HMAC-SHA256, hex,    header X-Acuity-Signature
  • Calendly:    HMAC-SHA256, hex,    header Calendly-Webhook-Signature
                 (signs "{timestamp}.{body}", not body alone)
  • WooCommerce: HMAC-SHA256, base64, header X-WC-Webhook-Signature
  • Square:      HMAC-SHA256, base64, header x-square-hmacsha256-signature
                 (signs "{notification_url}{body}" — URL matters)

Supported platforms (v1)
  • shopify      — orders/create, orders/updated, orders/cancelled,
                   orders/fulfilled, orders/paid, checkouts/create,
                   checkouts/update (checkouts/* only captured for
                   clients with cart_recovery_enabled=True), app/uninstalled
  • woocommerce  — order.created, order.updated, order.deleted, order.restored
  • acuity       — appointment.scheduled, appointment.rescheduled,
                   appointment.cancelled
  • calendly     — invitee.created, invitee.canceled
  • square       — booking.created, booking.updated

How to add a new platform
  1. Add a verify_<platform>_signature() function
  2. Add a normalise_<platform>_<event>() function
  3. Add a route in register_webhook_routes()
  4. Add platform DDL to migrate_integrations() if needed
  No other files need to change.

Flask usage (in app.py)
-----------------------
    from webhooks import register_webhook_routes, migrate_integrations
    migrate_integrations()          # call once at startup
    register_webhook_routes(app)    # mounts all /webhooks/* routes
"""

import hashlib
import hmac
import json
import logging
import os
import re
from datetime import datetime, timedelta
from functools import wraps

import models
import crypto_utils

logger = logging.getLogger(__name__)


# =====================================================================
# DB MIGRATION
# Creates the client_integrations table that stores one row per
# (client_id, platform) with the webhook secret and config.
# Safe to call on every startup — fully idempotent.
# =====================================================================

def migrate_integrations():
    """
    Create client_integrations table if it doesn't exist.
    Called from app.py alongside migrate_agent_tables() and init_db().
    """
    conn = cursor = None
    try:
        conn, cursor = models.get_db()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS client_integrations (
                id               SERIAL PRIMARY KEY,
                client_id        TEXT        NOT NULL,
                platform         TEXT        NOT NULL,
                webhook_secret   TEXT        NOT NULL,
                platform_config  JSONB       DEFAULT '{}',
                is_active        BOOLEAN     DEFAULT TRUE,
                created_at       TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
                updated_at       TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT client_integrations_uq UNIQUE (client_id, platform)
            )
        ''')

        # Index for fast lookup on every inbound webhook
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_client_integrations_lookup "
            "ON client_integrations (client_id, platform) WHERE is_active = TRUE"
        )

        # Index for shop_domain -> client_id lookup, used by the headless
        # Shopify OAuth install flow (app.py's connect_shopify_callback) to
        # detect a reinstall/reconnect of a shop Lumvi already knows, before
        # a Lumvi session exists to look it up the normal way. shop_domain
        # lives inside platform_config JSONB rather than its own column —
        # no schema change, just an expression index over the existing data.
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_client_integrations_shopify_shop "
            "ON client_integrations ((platform_config->>'shop_domain')) "
            "WHERE platform = 'shopify' AND is_active = TRUE"
        )

        # webhook_log — audit trail. Never used for query logic.
        # Rotated externally (e.g. DELETE WHERE created_at < NOW() - INTERVAL '30 days').
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS webhook_log (
                id           SERIAL      PRIMARY KEY,
                client_id    TEXT        NOT NULL,
                platform     TEXT        NOT NULL,
                event_type   TEXT,
                status       TEXT        NOT NULL,   -- 'ok' | 'sig_fail' | 'error'
                payload_hash TEXT,                   -- SHA-256 of raw body (dedup)
                error_msg    TEXT,
                created_at   TIMESTAMP   DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_webhook_log_client "
            "ON webhook_log (client_id, created_at DESC)"
        )

        # shopify_compliance_requests — durable audit trail + processing
        # state for the three mandatory GDPR topics (customers/data_request,
        # customers/redact, shop/redact). See handle_shopify_compliance_
        # webhook() for why this exists: Shopify's review process tests
        # that these topics actually DO something, not just return 200 —
        # this table is both the mechanism (tracks what still needs
        # processing) and the evidence (a queryable record of what was
        # found/done for any given request).
        #
        # result_summary is deliberately a count/description ("3 order(s)
        # redacted"), never the raw customer data itself — this table is
        # an operational/audit record, not a second copy of the PII it's
        # tracking the handling of.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS shopify_compliance_requests (
                id                    SERIAL      PRIMARY KEY,
                topic                 TEXT        NOT NULL,
                shop_domain           TEXT        NOT NULL,
                client_id             TEXT,
                customer_email        TEXT,
                customer_shopify_id   TEXT,
                status                TEXT        NOT NULL DEFAULT 'received',
                result_summary        TEXT,
                received_at           TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
                processed_at          TIMESTAMP,
                scheduled_for         TIMESTAMP
            )
        ''')

        # Used by process_due_shopify_shop_redactions() (the cron job) to
        # find shop/redact rows whose grace period has elapsed.
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_compliance_requests_due "
            "ON shopify_compliance_requests (status, scheduled_for) "
            "WHERE status = 'scheduled'"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_compliance_requests_client "
            "ON shopify_compliance_requests (client_id)"
        )

        # personal_data_access_log — answers Shopify's Data Protection
        # Details questionnaire question "Do you log access to personal
        # data?" (previously answered No — see the Partner Dashboard
        # rejection this was built in response to). Deliberately does NOT
        # store the raw customer_email/name being accessed — record_ref is
        # a truncated SHA-256 hash instead (see _hash_pii_ref below), so
        # this audit table doesn't become a second live copy of the exact
        # PII it exists to track access to. Still lets you answer "was
        # there unusual access to this specific customer's data" by
        # hashing the same email and comparing, without the log itself
        # being a new place that PII leaks from.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS personal_data_access_log (
                id            SERIAL      PRIMARY KEY,
                client_id     TEXT        NOT NULL,
                data_type     TEXT        NOT NULL,
                purpose       TEXT        NOT NULL,
                accessor      TEXT        NOT NULL DEFAULT 'system',
                record_ref    TEXT,
                accessed_at   TIMESTAMP   DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_personal_data_access_client "
            "ON personal_data_access_log (client_id, accessed_at DESC)"
        )

        conn.commit()
        print('✅ migrate_integrations: client_integrations + webhook_log + '
              'shopify_compliance_requests + personal_data_access_log ready')

    except Exception as e:
        print(f'⚠️  migrate_integrations error: {e}')
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


# =====================================================================
# INTEGRATION CRUD
# Used by the Lumvi dashboard to create/update client integrations.
# =====================================================================

def upsert_integration(client_id: str, platform: str,
                       webhook_secret: str, platform_config: dict = None) -> bool:
    """
    Create or update a client integration row.

    Called from the Lumvi dashboard when a merchant sets up
    a new platform connection. Returns True on success.

    Args:
        client_id:        Lumvi client identifier
        platform:         'shopify' | 'acuity'
        webhook_secret:   The HMAC secret from the platform
        platform_config:  Optional JSON blob (e.g. {'shop_domain': 'mystore.myshopify.com'})

    Fires a '{platform}_connected' analytics event (e.g. 'shopify_connected')
    the moment this integration goes from missing/inactive to active — not
    on every credential/config refresh of an already-active integration —
    so the admin dashboard's activity timeline reflects real connect
    actions, not re-saves.
    """
    conn = cursor = None
    try:
        conn, cursor = models.get_db()

        # Was it active before this call? Determines whether this is a
        # genuine connect/reconnect (fire event) or just a config refresh.
        cursor.execute(
            "SELECT is_active FROM client_integrations WHERE client_id = %s AND platform = %s",
            (client_id, platform)
        )
        _existing = cursor.fetchone()
        _was_active = bool(_existing and _existing.get('is_active'))

        encrypted_config = _encrypt_platform_config(platform_config or {})
        cursor.execute(
            """
            INSERT INTO client_integrations
                (client_id, platform, webhook_secret, platform_config, is_active, updated_at)
            VALUES (%s, %s, %s, %s, TRUE, NOW())
            ON CONFLICT ON CONSTRAINT client_integrations_uq
            DO UPDATE SET
                webhook_secret  = EXCLUDED.webhook_secret,
                platform_config = EXCLUDED.platform_config,
                is_active       = TRUE,
                updated_at      = NOW()
            """,
            (client_id, platform,
             webhook_secret,
             json.dumps(encrypted_config))
        )
        conn.commit()
        logger.info(f'[Integration] upserted client={client_id} platform={platform}')

        if not _was_active:
            try:
                owner_id = models.get_client_owner_id(client_id)
                if owner_id:
                    models.track_event(
                        f'{platform}_connected', user_id=owner_id,
                        metadata={'client_id': client_id},
                    )
            except Exception:
                pass  # analytics logging must never fail the connection itself

        return True
    except Exception as e:
        logger.error(f'[Integration] upsert_integration error: {e}')
        return False
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def get_integration(client_id: str, platform: str) -> dict | None:
    """
    Fetch an active integration row. Returns None if not found.
    Used internally by every webhook handler.
    """
    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            """
            SELECT client_id, platform, webhook_secret, platform_config
            FROM client_integrations
            WHERE client_id = %s AND platform = %s AND is_active = TRUE
            """,
            (client_id, platform)
        )
        row = cursor.fetchone()
        if not row:
            return None
        cfg = row.get('platform_config') or {}
        if isinstance(cfg, str):
            try: cfg = json.loads(cfg)
            except Exception: cfg = {}
        return {
            'client_id':       row['client_id'],
            'platform':        row['platform'],
            'webhook_secret':  row['webhook_secret'],
            'platform_config': _decrypt_platform_config(cfg),
        }
    except Exception as e:
        logger.error(f'[Integration] get_integration error: {e}')
        return None
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


# platform_config sub-keys that are real credentials, not metadata — never
# safe to send back to the browser once saved. get_integration() (above)
# deliberately does NOT use this — it's called server-side only, by webhook
# handlers and by shopify_connect.py/app.py's read-merge-write, which need
# the real values. Only list_integrations() (below), which feeds the
# dashboard UI directly, needs to redact.
_SENSITIVE_CONFIG_KEYS = ('access_token', 'shopify_client_secret', 'consumer_secret')


# Field-level encryption for the values in _SENSITIVE_CONFIG_KEYS, at rest
# in client_integrations.platform_config. Reuses crypto_utils.py's existing
# Fernet setup (already used for client_ext_integrations credentials) rather
# than adding a second encryption scheme — crypto_utils.encrypt_credentials
# takes a dict, so a single value is wrapped/unwrapped in a one-key dict
# rather than adding a new function to crypto_utils.py itself.
#
# _decrypt_secret falls back to returning the value unchanged when it isn't
# a valid Fernet token — this is what makes the migration backward
# compatible with rows written before this change: nothing needs a backfill
# migration, existing plaintext rows just keep working as plaintext until
# the next upsert_integration() call on that row re-encrypts it naturally
# (every write path already does read-merge-write through get_integration/
# list_integrations, so this happens automatically on the next reconnect,
# secret rotation, or config change — not something a caller has to do).

def _encrypt_secret(value: str) -> str:
    if not value:
        return value
    return crypto_utils.encrypt_credentials({'v': value})


def _decrypt_secret(value: str) -> str:
    if not value:
        return value
    decrypted = crypto_utils.decrypt_credentials(value)
    if 'v' in decrypted:
        return decrypted['v']
    return value  # not a Fernet token — legacy plaintext row, pass through


def _encrypt_platform_config(cfg: dict) -> dict:
    cfg = dict(cfg or {})
    for key in _SENSITIVE_CONFIG_KEYS:
        if cfg.get(key):
            cfg[key] = _encrypt_secret(cfg[key])
    return cfg


def _decrypt_platform_config(cfg: dict) -> dict:
    cfg = dict(cfg or {})
    for key in _SENSITIVE_CONFIG_KEYS:
        if cfg.get(key):
            cfg[key] = _decrypt_secret(cfg[key])
    return cfg


def _redact_platform_config(cfg: dict) -> dict:
    """Replace credential values with a has_<key> boolean so the dashboard
    can show "already connected" / drive a rotate flow without the raw
    secret ever reaching the browser."""
    cfg = dict(cfg or {})
    for key in _SENSITIVE_CONFIG_KEYS:
        cfg[f'has_{key}'] = bool(cfg.pop(key, None))
    return cfg


def list_integrations(client_id: str, redact: bool = True) -> list:
    """
    List all active integrations for a client.

    redact=True (default, safe): for anything that reaches the browser —
    e.g. app.py's GET /api/integrations/<client_id> — credential sub-fields
    in platform_config (access_token, client_secret, consumer_secret) are
    replaced with has_<key> booleans. Webhook secrets are always excluded
    entirely (not selected from the DB at all, further down).

    redact=False: for trusted server-side callers that need the real
    credentials to actually call the platform's API — commerce_adapters.py's
    _get_inventory_integration/_get_order_integration, specifically. Never
    pass False anywhere a result might reach the frontend.
    """
    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            """
            SELECT platform, platform_config, is_active, created_at, updated_at
            FROM client_integrations
            WHERE client_id = %s
            ORDER BY platform
            """,
            (client_id,)
        )
        rows = cursor.fetchall()
        result = []
        for row in rows:
            cfg = row.get('platform_config') or {}
            if isinstance(cfg, str):
                try: cfg = json.loads(cfg)
                except Exception: cfg = {}
            # redact=True (dashboard/UI): _redact_platform_config only ever
            # checks truthiness of these fields, and ciphertext is still a
            # non-empty string — no need to decrypt just to redact.
            # redact=False (commerce_adapters.py, trusted server-side callers
            # that need a real usable token): decrypt.
            result.append({
                'platform':        row['platform'],
                'platform_config': _redact_platform_config(cfg) if redact else _decrypt_platform_config(cfg),
                'is_active':       row['is_active'],
                'created_at':      str(row.get('created_at', '')),
                'updated_at':      str(row.get('updated_at', '')),
            })
        return result
    except Exception as e:
        logger.error(f'[Integration] list_integrations error: {e}')
        return []
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def get_first_sync_at(client_id: str, platform: str = 'shopify'):
    """
    Timestamp of the first successfully-processed webhook for this
    client/platform — used by the admin dashboard's user detail view as
    "first sync date". No new table: webhook_log is the existing audit
    trail every webhook handler already writes to.
    Returns None if no successful webhook has ever been logged.
    """
    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            "SELECT MIN(created_at) AS first_sync FROM webhook_log "
            "WHERE client_id = %s AND platform = %s AND status = 'ok'",
            (client_id, platform)
        )
        row = cursor.fetchone()
        return row['first_sync'] if row else None
    except Exception as e:
        logger.error(f'[Integration] get_first_sync_at error: {e}')
        return None
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def get_client_id_by_shopify_shop(shop_domain: str) -> str | None:
    """
    Returns the client_id already connected to this Shopify shop domain, or
    None if Lumvi has never seen this shop before.

    Used by app.py's connect_shopify_callback for a headless (Shopify-
    initiated) install/reinstall, where there's no Lumvi session yet to look
    the client up the normal way (models.get_user_clients). Matches on the
    shop_domain stored inside platform_config JSONB — see the expression
    index added in migrate_integrations() above.

    shop_domain itself isn't a credential, so this reads platform_config
    directly rather than going through get_integration()/_decrypt_platform_config.
    """
    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            """
            SELECT client_id FROM client_integrations
            WHERE platform = 'shopify' AND is_active = TRUE
              AND platform_config->>'shop_domain' = %s
            LIMIT 1
            """,
            (shop_domain,)
        )
        row = cursor.fetchone()
        return row['client_id'] if row else None
    except Exception as e:
        logger.error(f'[Integration] get_client_id_by_shopify_shop error: {e}')
        return None
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def delete_integration(client_id: str, platform: str) -> bool:
    """
    Soft-delete (deactivate) an integration. Webhook events are rejected
    after this.

    Fires a '{platform}_disconnected' analytics event when this actually
    deactivates a previously-active row — covers both the dashboard
    "disconnect" button and the app/uninstalled webhook handler, since
    both call this same function.
    """
    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            "UPDATE client_integrations SET is_active = FALSE, updated_at = NOW() "
            "WHERE client_id = %s AND platform = %s AND is_active = TRUE",
            (client_id, platform)
        )
        _deactivated = cursor.rowcount > 0
        conn.commit()

        if _deactivated:
            try:
                owner_id = models.get_client_owner_id(client_id)
                if owner_id:
                    models.track_event(
                        f'{platform}_disconnected', user_id=owner_id,
                        metadata={'client_id': client_id},
                    )
            except Exception:
                pass

        return True
    except Exception as e:
        logger.error(f'[Integration] delete_integration error: {e}')
        return False
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


# =====================================================================
# AUDIT LOG
# =====================================================================

def _log_webhook(client_id: str, platform: str, event_type: str,
                 status: str, payload_hash: str = None, error_msg: str = None):
    """Write one row to webhook_log. Non-blocking best-effort — never raises."""
    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            """
            INSERT INTO webhook_log
                (client_id, platform, event_type, status, payload_hash, error_msg)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (client_id, platform, event_type, status, payload_hash, error_msg)
        )
        conn.commit()
    except Exception:
        pass
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def _payload_hash(raw_body: bytes) -> str:
    return hashlib.sha256(raw_body).hexdigest()


# =====================================================================
# SHOPIFY SIGNATURE VERIFICATION
# Shopify signs every webhook with HMAC-SHA256 using the webhook secret.
# The digest is base64-encoded and sent in X-Shopify-Hmac-Sha256.
# https://shopify.dev/docs/apps/webhooks/configuration/https#step-5
# =====================================================================

def _verify_shopify_signature(raw_body: bytes, hmac_header: str, secret: str) -> bool:
    """
    Returns True if the HMAC-SHA256 digest of raw_body matches hmac_header.
    Uses hmac.compare_digest to prevent timing attacks.
    """
    import base64
    if not hmac_header or not secret:
        return False
    digest = hmac.new(
        secret.encode('utf-8'),
        raw_body,
        hashlib.sha256
    ).digest()
    expected = base64.b64encode(digest).decode('utf-8')
    return hmac.compare_digest(expected, hmac_header.strip())


# =====================================================================
# ACUITY SIGNATURE VERIFICATION
# Acuity signs webhooks with HMAC-SHA256. The signature is sent in
# X-Acuity-Signature as a hex digest.
# https://developers.acuityscheduling.com/docs/webhooks
# =====================================================================

def _verify_acuity_signature(raw_body: bytes, sig_header: str, secret: str) -> bool:
    """
    Returns True if the HMAC-SHA256 hex digest of raw_body matches sig_header.
    """
    if not sig_header or not secret:
        return False
    expected = hmac.new(
        secret.encode('utf-8'),
        raw_body,
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, sig_header.strip().lower())


# =====================================================================
# CALENDLY SIGNATURE VERIFICATION
# Calendly signs webhooks with HMAC-SHA256. The signature header format
# is "t=<unix_timestamp>,v1=<hex_digest>" and the signed message is
# "<timestamp>.<raw_body>" (NOT raw_body alone — this is the detail
# most implementations get wrong).
# https://developer.calendly.com/api-docs/ZG9jOjM5NjEzOTU3-webhook-signatures
# =====================================================================

def _verify_calendly_signature(raw_body: bytes, sig_header: str, secret: str) -> bool:
    """
    sig_header looks like: "t=1609459200,v1=5257a869e7bcb7fdf..."
    Returns False (not raises) on any malformed header — a webhook with
    a broken signature header is indistinguishable from a forged one.
    """
    if not sig_header or not secret:
        return False
    try:
        parts = dict(p.split('=', 1) for p in sig_header.split(',') if '=' in p)
        timestamp, signature = parts['t'], parts['v1']
    except (KeyError, ValueError):
        return False
    signed_payload = f'{timestamp}.'.encode('utf-8') + raw_body
    expected = hmac.new(secret.encode('utf-8'), signed_payload, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature.strip())


# =====================================================================
# WOOCOMMERCE SIGNATURE VERIFICATION
# WooCommerce signs webhooks with HMAC-SHA256 over the raw request body,
# base64-encoded, sent in X-WC-Webhook-Signature. Topic (e.g.
# "order.created") is sent separately in X-WC-Webhook-Topic.
# https://woocommerce.github.io/code-reference/classes/WC-Webhook.html
# =====================================================================

def _verify_woocommerce_signature(raw_body: bytes, sig_header: str, secret: str) -> bool:
    """Returns True if base64(HMAC-SHA256(raw_body, secret)) matches sig_header."""
    import base64
    if not sig_header or not secret:
        return False
    digest = hmac.new(secret.encode('utf-8'), raw_body, hashlib.sha256).digest()
    expected = base64.b64encode(digest).decode('utf-8')
    return hmac.compare_digest(expected, sig_header.strip())


# =====================================================================
# SQUARE SIGNATURE VERIFICATION
# Square signs webhooks with HMAC-SHA256, base64-encoded, sent in
# x-square-hmacsha256-signature — but unlike every other platform here,
# the signed message is the notification URL CONCATENATED WITH the raw
# body, not the body alone. The notification_url must exactly match
# what's registered in the Square dashboard or every signature check
# fails even with the correct secret.
# https://developer.squareup.com/docs/webhooks/step3validate
# =====================================================================

def _verify_square_signature(raw_body: bytes, sig_header: str, secret: str,
                              notification_url: str) -> bool:
    """Returns True if base64(HMAC-SHA256(notification_url + raw_body, secret)) matches sig_header."""
    import base64
    if not sig_header or not secret or not notification_url:
        return False
    message = notification_url.encode('utf-8') + raw_body
    digest = hmac.new(secret.encode('utf-8'), message, hashlib.sha256).digest()
    expected = base64.b64encode(digest).decode('utf-8')
    return hmac.compare_digest(expected, sig_header.strip())


# =====================================================================
# SHOPIFY NORMALISERS
# Each function accepts the raw Shopify webhook payload dict and
# upserts into Lumvi's orders table using the pattern from tools.py.
# =====================================================================

_SHOPIFY_STATUS_MAP = {
    'pending':    'pending',
    'authorized': 'confirmed',
    'partially_paid': 'confirmed',
    'paid':       'confirmed',
    'partially_refunded': 'processing',
    'refunded':   'refunded',
    'voided':     'cancelled',
}


def _upsert_order(client_id: str, order_data: dict) -> bool:
    """
    Write one normalised order into Lumvi's orders table.
    Uses INSERT … ON CONFLICT DO UPDATE so both new and updated
    orders from webhooks are handled with a single call.
    """
    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            """
            INSERT INTO orders
                (client_id, order_id, customer_email, customer_name,
                 status, items_json, total_amount, currency, notes,
                 created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (client_id, order_id) DO UPDATE SET
                customer_email = EXCLUDED.customer_email,
                customer_name  = EXCLUDED.customer_name,
                status         = EXCLUDED.status,
                items_json     = EXCLUDED.items_json,
                total_amount   = EXCLUDED.total_amount,
                currency       = EXCLUDED.currency,
                notes          = EXCLUDED.notes,
                updated_at     = EXCLUDED.updated_at
            """,
            (
                client_id,
                order_data['order_id'],
                order_data.get('customer_email', ''),
                order_data.get('customer_name', ''),
                order_data.get('status', 'pending'),
                json.dumps(order_data.get('items', [])),
                order_data.get('total_amount'),
                order_data.get('currency', 'USD'),
                order_data.get('notes', ''),
                order_data.get('created_at') or datetime.utcnow(),
                datetime.utcnow(),
            )
        )
        conn.commit()
        return True
    except Exception as e:
        logger.error(f'[Webhook:upsert_order] client={client_id} error: {e}')
        return False
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


# =====================================================================
# SHOPIFY CHECKOUT NORMALISER — cart recovery
# Shopify checkouts/create and checkouts/update webhooks fire as a
# customer fills in the checkout form. Same shape for both topics.
# https://shopify.dev/docs/api/admin-rest/latest/resources/abandoned-checkouts
# =====================================================================

def _normalise_shopify_checkout(payload: dict) -> dict:
    """
    Map a Shopify checkout payload → the kwargs upsert_abandoned_cart() expects.
    """
    customer   = payload.get('customer') or {}
    billing    = payload.get('billing_address') or {}
    email      = (payload.get('email') or customer.get('email') or '').lower().strip()
    first_name = customer.get('first_name') or billing.get('first_name') or ''
    last_name  = customer.get('last_name') or billing.get('last_name') or ''
    name       = f"{first_name} {last_name}".strip() or email

    items = []
    for li in (payload.get('line_items') or []):
        items.append({
            'name':     li.get('title', ''),
            'quantity': li.get('quantity', 1),
            'price':    li.get('price', '0.00'),
        })

    return {
        'checkout_token': payload.get('token'),
        'customer_email': email or None,
        'customer_name':  name or None,
        'cart_total':     payload.get('total_price'),
        'currency':       payload.get('currency', 'USD'),
        'line_items':     items,
        'checkout_url':   payload.get('abandoned_checkout_url'),
    }


def _normalise_shopify_order(payload: dict, client_id: str) -> dict:
    """
    Map a Shopify order payload → Lumvi order dict.

    Shopify order fields used:
      name             → order_id  (e.g. "#1001")
      financial_status → status
      customer         → customer_name, customer_email
      line_items       → items
      total_price      → total_amount
      currency         → currency
      created_at       → created_at
      note             → notes
    """
    # order_id: use Shopify's human-readable order name (#1001) or fall back to id
    order_id = str(payload.get('name') or payload.get('id') or '').strip('#').strip()

    # Customer info
    customer    = payload.get('customer') or {}
    email       = (
        payload.get('email') or
        customer.get('email') or ''
    ).lower().strip()
    first_name  = customer.get('first_name') or ''
    last_name   = customer.get('last_name') or ''
    name        = f"{first_name} {last_name}".strip() or email

    # Status
    fin_status  = (payload.get('financial_status') or 'pending').lower()
    ful_status  = (payload.get('fulfillment_status') or '').lower()
    if ful_status == 'fulfilled':
        status = 'delivered'
    elif ful_status == 'partial':
        status = 'in_transit'
    else:
        status = _SHOPIFY_STATUS_MAP.get(fin_status, 'pending')

    # Cancelled override
    if payload.get('cancelled_at'):
        status = 'cancelled'

    # Line items
    items = []
    for li in (payload.get('line_items') or []):
        items.append({
            'name':     li.get('name') or li.get('title', ''),
            'quantity': li.get('quantity', 1),
            'price':    li.get('price', '0.00'),
            'sku':      li.get('sku', ''),
        })

    # Dates
    raw_created = payload.get('created_at')
    try:
        created_at = datetime.fromisoformat(
            raw_created.replace('Z', '+00:00')
        ) if raw_created else datetime.utcnow()
    except Exception:
        created_at = datetime.utcnow()

    return {
        'order_id':       order_id,
        'customer_email': email,
        'customer_name':  name,
        'status':         status,
        'items':          items,
        'total_amount':   payload.get('total_price'),
        'currency':       payload.get('currency', 'USD'),
        'notes':          payload.get('note') or '',
        'created_at':     created_at,
        # Links this order back to the checkout that produced it, so
        # cart-recovery can mark the matching abandoned_carts row
        # 'recovered' — see mark_cart_recovered() call in
        # handle_shopify_webhook(). Present on Shopify order payloads;
        # None if the order didn't originate from a tracked checkout
        # (e.g. a manually-created draft order).
        'checkout_token': payload.get('checkout_token'),
    }


# =====================================================================
# WOOCOMMERCE NORMALISER
# WooCommerce's REST Order resource shape (same fields whether the
# webhook fires for order.created or order.updated).
# https://woocommerce.github.io/woocommerce-rest-api-docs/#order-properties
# =====================================================================

_WOOCOMMERCE_STATUS_MAP = {
    'pending':     'pending',
    'processing':  'confirmed',
    'on-hold':     'pending',
    'completed':   'delivered',
    'cancelled':   'cancelled',
    'refunded':    'cancelled',
    'failed':      'cancelled',
    'trash':       'cancelled',
}


def _normalise_woocommerce_order(payload: dict, client_id: str) -> dict:
    """
    Map a WooCommerce order payload → Lumvi order dict.

    WooCommerce order fields used:
      id                → order_id
      status            → status
      billing            → customer_name, customer_email
      line_items        → items
      total             → total_amount
      currency          → currency
      date_created      → created_at
      customer_note     → notes
    """
    order_id = str(payload.get('id') or payload.get('number') or '').strip()

    billing    = payload.get('billing') or {}
    email      = (billing.get('email') or '').lower().strip()
    first_name = billing.get('first_name') or ''
    last_name  = billing.get('last_name') or ''
    name       = f'{first_name} {last_name}'.strip() or email

    status = _WOOCOMMERCE_STATUS_MAP.get((payload.get('status') or 'pending').lower(), 'pending')

    items = []
    for li in (payload.get('line_items') or []):
        items.append({
            'name':     li.get('name', ''),
            'quantity': li.get('quantity', 1),
            'price':    li.get('price') or li.get('total', '0.00'),
            'sku':      li.get('sku', ''),
        })

    raw_created = payload.get('date_created') or payload.get('date_created_gmt')
    try:
        created_at = datetime.fromisoformat(raw_created) if raw_created else datetime.utcnow()
    except Exception:
        created_at = datetime.utcnow()

    return {
        'order_id':       order_id,
        'customer_email': email,
        'customer_name':  name,
        'status':         status,
        'items':          items,
        'total_amount':   payload.get('total'),
        'currency':       payload.get('currency', 'USD'),
        'notes':          payload.get('customer_note') or '',
        'created_at':     created_at,
    }


def _verify_shopify_app_signature(raw_body: bytes, hmac_header: str) -> bool:
    """
    Same HMAC scheme as _verify_shopify_signature, but against the app's
    own client secret rather than a stored per-integration one.

    Used only for the mandatory compliance topics (handle_shopify_compliance_
    webhook, below) — those arrive at a shop-scoped but not client_id-scoped
    URL, and may have no client_integrations row to read a secret from at
    all (shop/redact fires up to 48 days after uninstall, by which point
    the row may be long gone).
    """
    secret = os.environ.get('SHOPIFY_APP_CLIENT_SECRET', '')
    if not secret:
        return False
    return _verify_shopify_signature(raw_body, hmac_header, secret)


# Shopify's own compliance deadline is 30 days from receiving the request
# (per https://shopify.dev/docs/apps/build/compliance/privacy-law-compliance).
# This grace period is Lumvi's OWN internal safety window before actually
# executing a shop/redact purge — short enough to comfortably clear that
# 30-day deadline with room to spare, long enough to catch a catastrophic
# bug (wrong client_id, a bad deploy) before an irreversible whole-client
# delete runs. customers/redact (much narrower blast radius — deletes only
# matching order rows) does NOT use this delay; it executes synchronously,
# see handle_shopify_compliance_webhook below.
SHOPIFY_SHOP_REDACT_GRACE_DAYS = 3

# Retention policy: data is purged when a merchant disconnects/uninstalls,
# per the explicit decision to tie retention to account lifecycle rather
# than a fixed calendar window. Longer than SHOPIFY_SHOP_REDACT_GRACE_DAYS
# above on purpose — that one fires off Shopify's own shop/redact webhook,
# 48 hours after uninstall already, driven by Shopify's compliance
# deadline; this one fires immediately on uninstall (see the app/uninstalled
# handling in handle_shopify_webhook), so it needs its own buffer to give a
# merchant who uninstalled by mistake real time to reinstall before their
# data is purged, without that buffer riding on Shopify's separate timeline.
SHOPIFY_UNINSTALL_RETENTION_GRACE_DAYS = 30


def _hash_pii_ref(value: str) -> str:
    """
    Truncated SHA-256 of a PII value (email, phone, etc.), for use as a
    traceable-but-non-reversible reference in personal_data_access_log.
    Not a security control (hashes of low-entropy values like emails are
    guessable) — just keeps the access log from being a second live copy
    of the exact data it's tracking access to. 16 hex chars is enough to
    correlate repeated access to the same value without being mistaken
    for a real secret.
    """
    if not value:
        return ''
    return hashlib.sha256(value.strip().lower().encode('utf-8')).hexdigest()[:16]


def log_personal_data_access(client_id: str, data_type: str, purpose: str,
                             accessor: str = 'system', record_ref: str = None):
    """
    Records one access to personal/protected customer data. Best-effort —
    never raises, never blocks the access it's logging (a logging failure
    shouldn't take down order lookup).

    Args:
        client_id:  whose data was accessed
        data_type:  what kind — e.g. 'shopify_order', 'shopify_customer_email'
        purpose:    why — e.g. 'order_status_lookup', 'gdpr_data_request',
                    'gdpr_redaction'
        accessor:   'system' for automated/AI access (the common case —
                    the chatbot answering "where's my order"), or a
                    specific staff user id for a human dashboard view
        record_ref: an already-hashed reference (see _hash_pii_ref) — pass
                    the hash, not the raw value, this function does not
                    hash it for you (callers should be explicit about
                    what's going into the log, not rely on this function
                    to remember to protect it)
    """
    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            """
            INSERT INTO personal_data_access_log
                (client_id, data_type, purpose, accessor, record_ref)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (client_id, data_type, purpose, accessor, record_ref)
        )
        conn.commit()
    except Exception as e:
        logger.error(f'[PersonalDataAccess] failed to log access client={client_id} '
                     f'data_type={data_type}: {e}')
        if conn:
            try: conn.rollback()
            except Exception: pass
    finally:
        if cursor: cursor.close()
        if conn: conn.close()


def _get_client_owner_email(client_id: str) -> str:
    """Resolves the Lumvi account email that owns this client — used to
    notify a merchant when a customers/data_request export is ready."""
    try:
        client = models.get_client_by_id(client_id)
        if not client:
            return ''
        owner = models.get_user_by_id(client['user_id'])
        return (owner or {}).get('email', '')
    except Exception as e:
        logger.error(f'[Shopify Compliance] could not resolve owner email for client={client_id}: {e}')
        return ''


def _find_orders_by_email(client_id: str, customer_email: str) -> list:
    """
    Orders matching this client + customer email. Deliberately matches on
    email, not Shopify's numeric order IDs from orders_to_redact/
    orders_requested — Lumvi's own orders.order_id column stores Shopify's
    human-readable order NAME ("1001"), not the raw numeric id ("299938")
    those arrays contain (see _normalise_shopify_order: `str(payload.get
    ('name') or payload.get('id') ...)`). Matching against the numeric IDs
    directly would silently match zero rows in the common case — looking
    like a successful, empty redaction rather than the wrong query. Email
    is the field both sides of this actually share.
    """
    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            "SELECT order_id, status, total_amount, currency, items_json, created_at "
            "FROM orders WHERE client_id = %s AND customer_email = %s",
            (client_id, customer_email)
        )
        return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f'[Shopify Compliance] order lookup failed client={client_id}: {e}')
        return []
    finally:
        if cursor: cursor.close()
        if conn: conn.close()


def _redact_orders_by_email(client_id: str, customer_email: str) -> int:
    """Deletes matching order rows. Returns the number of rows removed."""
    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            "DELETE FROM orders WHERE client_id = %s AND customer_email = %s",
            (client_id, customer_email)
        )
        count = cursor.rowcount
        conn.commit()
        return count
    except Exception as e:
        logger.error(f'[Shopify Compliance] order redaction failed client={client_id}: {e}')
        if conn:
            try: conn.rollback()
            except Exception: pass
        return 0
    finally:
        if cursor: cursor.close()
        if conn: conn.close()


def hard_delete_shopify_integration(client_id: str) -> bool:
    """
    Permanently removes client_integrations and webhook_log rows for this
    client. NOT a soft delete — see delete_integration() for that (used by
    app/uninstalled). Only ever called by process_due_shopify_shop_
    redactions(), below.

    This exists as its own function because models.delete_client() does
    NOT cover either table: client_integrations has no FK relationship to
    clients at all (checked directly — its CREATE TABLE has
    `client_id TEXT NOT NULL` with no REFERENCES clause), and neither
    table is in delete_client's explicit per-table DELETE list. Without
    this, a shop/redact "complete" deletion would leave the encrypted
    Shopify access token and shop_domain sitting in the database
    indefinitely — exactly the kind of gap Shopify's review process tests
    for.
    """
    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        cursor.execute('DELETE FROM client_integrations WHERE client_id = %s', (client_id,))
        cursor.execute('DELETE FROM webhook_log WHERE client_id = %s', (client_id,))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f'[Shopify] hard_delete_shopify_integration failed client={client_id}: {e}')
        if conn:
            try: conn.rollback()
            except Exception: pass
        return False
    finally:
        if cursor: cursor.close()
        if conn: conn.close()


def _record_compliance_request(topic: str, shop_domain: str, client_id: str = None,
                               customer_email: str = None, customer_shopify_id: str = None,
                               status: str = 'received', scheduled_for=None):
    """Insert one row into shopify_compliance_requests. Returns the new row's id, or None on failure."""
    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            """
            INSERT INTO shopify_compliance_requests
                (topic, shop_domain, client_id, customer_email, customer_shopify_id, status, scheduled_for)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (topic, shop_domain, client_id, customer_email, customer_shopify_id, status, scheduled_for)
        )
        row = cursor.fetchone()
        conn.commit()
        return row['id'] if row else None
    except Exception as e:
        logger.error(f'[Shopify Compliance] failed to record request: {e}')
        if conn:
            try: conn.rollback()
            except Exception: pass
        return None
    finally:
        if cursor: cursor.close()
        if conn: conn.close()


def _complete_compliance_request(request_id, status: str, result_summary: str):
    """Marks a compliance request row processed. No-ops if request_id is None
    (the row failed to insert in the first place — nothing to update)."""
    if not request_id:
        return
    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            "UPDATE shopify_compliance_requests SET status=%s, result_summary=%s, processed_at=NOW() WHERE id=%s",
            (status, result_summary, request_id)
        )
        conn.commit()
    except Exception as e:
        logger.error(f'[Shopify Compliance] failed to update request id={request_id}: {e}')
        if conn:
            try: conn.rollback()
            except Exception: pass
    finally:
        if cursor: cursor.close()
        if conn: conn.close()


def get_due_shopify_shop_redactions() -> list:
    """shop/redact rows whose grace period has elapsed — used by the cron job."""
    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            "SELECT id, client_id, shop_domain FROM shopify_compliance_requests "
            "WHERE status = 'scheduled' AND scheduled_for <= NOW()"
        )
        return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f'[Shopify Compliance] get_due_shopify_shop_redactions failed: {e}')
        return []
    finally:
        if cursor: cursor.close()
        if conn: conn.close()


def process_due_shopify_shop_redactions() -> dict:
    """
    Called by a cron job (blueprints/cron.py's /cron/shopify-redactions,
    mirroring cron_hard_delete_accounts' exact pattern for scheduled user
    deletion). Performs the actual shop/redact purge for every request
    whose grace period (SHOPIFY_SHOP_REDACT_GRACE_DAYS) has elapsed:
    models.delete_client() for the comprehensive per-table cascade
    (conversations, leads, faqs, knowledge_base, orders, etc.), plus
    hard_delete_shopify_integration() for the two tables that function
    doesn't cover.

    Note on models.delete_client()'s contract, since it's easy to get
    wrong: it does NOT return a success boolean — it returns None
    implicitly on success and RE-RAISES on failure (see its `except
    Exception: conn.rollback(); raise`). Treating its return value as a
    truthy success flag would silently treat every successful call as a
    failure. It's called inside its own try/except here specifically
    because of that.
    """
    due = get_due_shopify_shop_redactions()
    processed = 0
    failed = 0
    for row in due:
        client_id = row['client_id']
        if not client_id:
            # Already-unresolvable shop at request time — nothing to
            # delete. Mark it done rather than leaving it stuck in
            # 'scheduled' forever with nothing that will ever process it.
            _complete_compliance_request(row['id'], 'completed', 'No matching client — nothing to redact')
            processed += 1
            continue
        try:
            models.delete_client(client_id)
            client_ok = True
        except Exception as e:
            logger.error(f'[Shopify Compliance] delete_client failed client={client_id}: {e}')
            client_ok = False

        integration_ok = hard_delete_shopify_integration(client_id)

        if client_ok and integration_ok:
            _complete_compliance_request(row['id'], 'completed',
                'Client data (conversations, leads, FAQs, orders, knowledge base) and '
                'Shopify integration credentials permanently deleted')
            processed += 1
            logger.info(f'[Shopify Compliance] shop/redact completed client={client_id}')
        else:
            _complete_compliance_request(row['id'], 'failed',
                f'delete_client_ok={client_ok} hard_delete_integration_ok={integration_ok}')
            failed += 1
            logger.error(f'[Shopify Compliance] shop/redact INCOMPLETE client={client_id} '
                         f'delete_client_ok={client_ok} hard_delete_integration_ok={integration_ok} '
                         f'— needs manual follow-up')
    return {'processed': processed, 'failed': failed, 'total_due': len(due)}


def handle_shopify_compliance_webhook(raw_body: bytes, hmac_header: str, topic: str,
                                      mail=None) -> tuple[dict, int]:
    """
    Handles Shopify's three mandatory compliance topics: customers/data_request,
    customers/redact, shop/redact — required for any Shopify app, listed or
    custom-distributed. See https://shopify.dev/docs/apps/build/compliance/privacy-law-compliance.

    These are NOT registered per-shop via webhookSubscriptionCreate the way
    orders/checkouts/app-uninstalled are (see ShopifyAdapter.register_webhooks
    in commerce_adapters.py) — Shopify doesn't support that for these three;
    they're configured once, app-wide, in the Partner Dashboard's "Compliance
    webhooks" section, pointing at the single URL register_webhook_routes
    mounts below. That Partner Dashboard step is a manual one-time setup
    action, not something this function or its caller can do.

    Every request is recorded in shopify_compliance_requests regardless of
    outcome — durable, queryable evidence of what was found/done, which is
    what Shopify's review process specifically checks for (a 200 response
    that didn't actually do anything is documented as the single most
    common first-submission rejection reason).

    Payload shapes (confirmed against Shopify's own docs, not assumed):
      customers/data_request: {shop_domain, customer: {id, email, phone},
                                orders_requested: [...], data_request: {id}}
      customers/redact:       {shop_domain, customer: {id, email, phone},
                                orders_to_redact: [...]}
      shop/redact:            {shop_domain}  — no customer, shop-scoped

    mail is optional (Flask-Mail's Mail instance, passed through from
    app.py via register_webhook_routes) — used only for the best-effort
    customers/data_request notification email. Every mail.send() call is
    wrapped so a failed/unconfigured email never affects the webhook
    response — Shopify only needs the 200 and the durable record above;
    email is a delivery-convenience layer on top; the merchant/Prosper can
    always retrieve the same data directly via the orders table or the
    shopify_compliance_requests audit row.
    """
    phash = _payload_hash(raw_body)

    if not _verify_shopify_app_signature(raw_body, hmac_header):
        logger.warning(f'[Shopify Compliance] signature verification failed topic={topic}')
        return {'error': 'Invalid signature'}, 401

    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError:
        return {'error': 'Invalid JSON'}, 400

    shop_domain = (payload.get('shop_domain') or '').strip().lower()
    client_id = get_client_id_by_shopify_shop(shop_domain) if shop_domain else None
    customer = payload.get('customer') or {}
    customer_email = (customer.get('email') or '').strip().lower() or None
    customer_shopify_id = str(customer.get('id')) if customer.get('id') else None

    logger.info(f'[Shopify Compliance] {topic} shop={shop_domain} client={client_id or "unresolved"}')

    if not client_id:
        # Shop already fully offboarded (or never actually connected — a
        # test ping). Nothing to act on, but still a legitimate Shopify
        # request that must be acknowledged — Shopify must not get a
        # failure response just because the shop is long gone.
        _record_compliance_request(topic, shop_domain, None, customer_email,
                                   customer_shopify_id, status='completed')
        return {'status': 'ok'}, 200

    # ── customers/data_request ──────────────────────────────────────────
    if topic == 'customers/data_request':
        request_id = _record_compliance_request(topic, shop_domain, client_id,
                                                 customer_email, customer_shopify_id)
        if not customer_email:
            _complete_compliance_request(request_id, 'completed',
                'No customer email in payload — nothing to compile')
            return {'status': 'ok'}, 200

        orders = _find_orders_by_email(client_id, customer_email)
        log_personal_data_access(client_id, 'shopify_order', 'gdpr_data_request',
                                 record_ref=_hash_pii_ref(customer_email))
        summary = f'{len(orders)} order(s) found'
        _complete_compliance_request(request_id, 'completed', summary)
        logger.info(f'[Shopify Compliance] data_request client={client_id} {summary}')

        if mail and orders:
            try:
                from flask_mail import Message as MailMessage
                owner_email = _get_client_owner_email(client_id)
                if owner_email:
                    order_lines = '\n'.join(
                        f"- Order {o['order_id']}: {o['status']}, "
                        f"{o['total_amount']} {o['currency']}, placed {o['created_at']}"
                        for o in orders
                    )
                    msg = MailMessage(
                        subject=f'Shopify customer data request — {shop_domain}',
                        recipients=[owner_email],
                        body=(f"Shopify sent a customer data request for {customer_email} "
                              f"on {shop_domain}.\n\n{summary}:\n\n{order_lines}\n\n"
                              f"You are required to provide this to the customer if requested."),
                        sender=os.environ.get('MAIL_DEFAULT_SENDER', 'hello@lumvi.net'),
                    )
                    mail.send(msg)
                    logger.info(f'[Shopify Compliance] data_request email sent to {owner_email}')
            except Exception as e:
                # Best-effort — the compiled data is already durably
                # queryable (orders table + the audit row above) even if
                # this notification email fails.
                logger.error(f'[Shopify Compliance] data_request email failed client={client_id}: {e}')

        return {'status': 'ok'}, 200

    # ── customers/redact ─────────────────────────────────────────────────
    if topic == 'customers/redact':
        request_id = _record_compliance_request(topic, shop_domain, client_id,
                                                 customer_email, customer_shopify_id)
        if not customer_email:
            _complete_compliance_request(request_id, 'completed',
                'No customer email in payload — nothing to redact')
            return {'status': 'ok'}, 200

        redacted_count = _redact_orders_by_email(client_id, customer_email)
        log_personal_data_access(client_id, 'shopify_order', 'gdpr_redaction',
                                 record_ref=_hash_pii_ref(customer_email))
        summary = f'{redacted_count} order(s) redacted'
        _complete_compliance_request(request_id, 'completed', summary)
        logger.info(f'[Shopify Compliance] customers/redact client={client_id} {summary}')
        return {'status': 'ok'}, 200

    # ── shop/redact ──────────────────────────────────────────────────────
    if topic == 'shop/redact':
        scheduled_for = datetime.utcnow() + timedelta(days=SHOPIFY_SHOP_REDACT_GRACE_DAYS)
        request_id = _record_compliance_request(topic, shop_domain, client_id,
                                                 status='scheduled', scheduled_for=scheduled_for)
        logger.info(f'[Shopify Compliance] shop/redact scheduled client={client_id} '
                    f'for={scheduled_for.isoformat()}')
        return {'status': 'ok'}, 200

    # Unknown/future compliance topic — acknowledge, don't fail the
    # webhook, but make sure it's visible rather than silently dropped.
    logger.warning(f'[Shopify Compliance] unrecognized compliance topic={topic} shop={shop_domain}')
    _record_compliance_request(topic, shop_domain, client_id, customer_email,
                               customer_shopify_id, status='completed')
    return {'status': 'ok'}, 200


def handle_shopify_webhook(client_id: str, raw_body: bytes, hmac_header: str,
                           topic: str, shop_domain_header: str = '') -> tuple[dict, int]:
    """
    Verify and process one inbound Shopify webhook.

    Args:
        client_id:           From the URL path parameter
        raw_body:            request.get_data() — raw bytes before any parsing
        hmac_header:         request.headers.get('X-Shopify-Hmac-Sha256')
        topic:                request.headers.get('X-Shopify-Topic')
                              e.g. 'orders/create', 'orders/updated', 'orders/cancelled'
        shop_domain_header:  request.headers.get('X-Shopify-Shop-Domain') — sent on
                              every Shopify webhook topic, confirmed directly by a
                              Shopify developer (not assumed): always the
                              *.myshopify.com domain, unaffected by custom storefront
                              domains. See the shop-domain cross-check below for why
                              this parameter exists.

    Returns:
        (response_dict, http_status_code)

    SECURITY — shop-domain cross-check (fixes a cross-tenant replay gap):
    webhook_secret is now the same SHOPIFY_APP_CLIENT_SECRET for every
    Shopify-connected client (see upsert_integration's callers — this was a
    deliberate Phase 0 fix, since that's genuinely what Shopify signs
    app-registered webhooks with). A side effect nobody caught at the time:
    HMAC verification alone no longer proves a webhook belongs to the
    client_id in the URL — only that Shopify signed it for *some* shop on
    this app. Any legitimately-signed webhook, replayed against a
    *different* client's URL, would still pass. The check below closes
    that: after the signature verifies, the shop the payload actually came
    from (per Shopify's own header, not anything in the body a forged
    request could set) must match what's on file for this specific
    client_id, or the request is rejected before any processing —
    including before JSON parsing, so a replayed-but-mismatched payload
    never reaches order/checkout persistence at all.
    """
    phash = _payload_hash(raw_body)

    # 1. Load integration config
    integration = get_integration(client_id, 'shopify')
    if not integration:
        logger.warning(f'[Shopify] no integration found for client={client_id}')
        _log_webhook(client_id, 'shopify', topic, 'error', phash,
                     'Integration not configured')
        return {'error': 'Integration not configured'}, 404

    # 2. Verify signature
    if not _verify_shopify_signature(raw_body, hmac_header, integration['webhook_secret']):
        logger.warning(f'[Shopify] signature verification failed client={client_id}')
        _log_webhook(client_id, 'shopify', topic, 'sig_fail', phash,
                     'HMAC signature mismatch')
        return {'error': 'Invalid signature'}, 401

    # 2b. Verify the shop this webhook actually came from matches this
    # client's registration — see the security note in this function's
    # docstring. Deliberately strict: no fuzzy matching, no fallback if the
    # header is missing (fails closed, not open) — a webhook with a valid
    # signature but no shop-domain header, or a mismatched one, is treated
    # identically to a forged one, because from this client's perspective
    # that's exactly what it is.
    expected_shop = ((integration.get('platform_config') or {}).get('shop_domain') or '').strip().lower()
    actual_shop = (shop_domain_header or '').strip().lower()
    if not actual_shop or actual_shop != expected_shop:
        logger.error(f'[Shopify SECURITY] shop-domain mismatch client={client_id} '
                     f'expected={expected_shop!r} got={actual_shop!r} topic={topic} — '
                     f'rejected as a possible cross-tenant replay')
        _log_webhook(client_id, 'shopify', topic, 'shop_mismatch', phash,
                     f'X-Shopify-Shop-Domain mismatch: expected={expected_shop!r} got={actual_shop!r}')
        return {'error': 'Shop domain does not match this integration'}, 401

    # 3. Parse payload
    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError as e:
        _log_webhook(client_id, 'shopify', topic, 'error', phash, f'JSON parse error: {e}')
        return {'error': 'Invalid JSON'}, 400

    # 4. Route by topic
    topic = (topic or '').lower().strip()

    # app/uninstalled arrives on this same endpoint/secret as everything
    # else Shopify sends this app for this shop — no separate route needed.
    # Deactivates the integration the same way the dashboard's manual
    # "disconnect" button already does (delete_integration is a soft
    # delete — is_active=FALSE, not a data wipe); a later reinstall of the
    # same shop reactivates this same row rather than creating a new one.
    if topic == 'app/uninstalled':
        ok = delete_integration(client_id, 'shopify')
        logger.info(f'[Shopify] app/uninstalled client={client_id} deactivated={ok}')
        _log_webhook(client_id, 'shopify', topic, 'ok' if ok else 'error', phash)

        # Retention: schedule the full purge now, tied to this observed
        # lifecycle event, rather than waiting on Shopify's separate
        # shop/redact compliance webhook (which needs its own Partner
        # Dashboard configuration and has no guarantee of ever arriving if
        # that's not set up — see the production readiness follow-up for
        # why relying on it alone left a real gap). Reuses the exact same
        # scheduled-deletion pathway shop/redact already uses
        # (process_due_shopify_shop_redactions) — same table, same cron
        # job, just a second trigger for it. A merchant who reinstalls
        # within the grace period is unaffected: reinstalling reactivates
        # the same client_integrations row rather than creating a new one
        # (see get_client_id_by_shopify_shop), and if a shop/redact row is
        # still 'scheduled' at that point it just processes against a
        # client that's active again — delete_client() on an active
        # client is exactly what a merchant who WANTED to leave should get,
        # so this only matters if they explicitly meant to disconnect.
        # SHOPIFY_UNINSTALL_RETENTION_GRACE_DAYS is intentionally longer
        # than SHOPIFY_SHOP_REDACT_GRACE_DAYS (Shopify's compliance-driven
        # 3-day window): this trigger fires immediately on uninstall, not
        # 48 hours later like Shopify's own shop/redact, so a longer
        # window here gives a merchant who uninstalled by mistake more
        # real time to reinstall before their data is purged.
        shop_domain = ((integration or {}).get('platform_config') or {}).get('shop_domain', '')
        scheduled_for = datetime.utcnow() + timedelta(days=SHOPIFY_UNINSTALL_RETENTION_GRACE_DAYS)
        _record_compliance_request('shop/redact', shop_domain, client_id,
                                   status='scheduled', scheduled_for=scheduled_for)
        logger.info(f'[Shopify] retention purge scheduled client={client_id} '
                    f'for={scheduled_for.isoformat()} (uninstall-triggered)')

        return {'status': 'ok'}, 200

    order_topics    = {'orders/create', 'orders/updated', 'orders/cancelled',
                        'orders/fulfilled', 'orders/paid'}
    checkout_topics = {'checkouts/create', 'checkouts/update'}
    supported_topics = order_topics | checkout_topics

    if topic not in supported_topics:
        # Return 200 to prevent Shopify from retrying non-order topics
        _log_webhook(client_id, 'shopify', topic, 'ok', phash, 'topic ignored')
        return {'status': 'ignored', 'topic': topic}, 200

    # 5a. Checkout topics — abandoned-cart capture for cart recovery.
    # Only tracked for clients who've turned the feature on (plan-gated at
    # the point cart_recovery_enabled is set — see client_settings.py).
    if topic in checkout_topics:
        try:
            client = models.get_client_by_id(client_id)
            if not client or not client.get('cart_recovery_enabled'):
                _log_webhook(client_id, 'shopify', topic, 'ok', phash,
                             'cart recovery not enabled for this client')
                return {'status': 'ignored', 'topic': topic}, 200

            checkout_data = _normalise_shopify_checkout(payload)
            if not checkout_data.get('checkout_token'):
                raise ValueError('Could not extract checkout token from payload')

            result = models.upsert_abandoned_cart(client_id, **checkout_data)
            if not result.get('success'):
                raise RuntimeError(result.get('error', 'upsert_abandoned_cart failed'))

            log_personal_data_access(client_id, 'shopify_checkout', 'cart_recovery',
                                     record_ref=_hash_pii_ref(checkout_data.get('customer_email') or ''))

            logger.info(
                f'[Shopify] {topic} → cart={checkout_data["checkout_token"]} client={client_id}'
            )
            _log_webhook(client_id, 'shopify', topic, 'ok', phash)
            return {'status': 'ok', 'checkout_token': checkout_data['checkout_token']}, 200

        except Exception as e:
            logger.error(f'[Shopify] checkout processing error client={client_id} topic={topic}: {e}')
            _log_webhook(client_id, 'shopify', topic, 'error', phash, str(e))
            return {'error': 'Processing failed'}, 500

    # 5b. Order topics — order upsert, plus close out any abandoned cart
    # this order completed (prevents a recovery email going out for a
    # cart that already converted).
    try:
        order_data = _normalise_shopify_order(payload, client_id)
        if not order_data.get('order_id'):
            raise ValueError('Could not extract order_id from payload')

        success = _upsert_order(client_id, order_data)
        if not success:
            raise RuntimeError('DB upsert failed')

        # Access logging: this is the primary ONGOING access channel for
        # customer order data once Protected Customer Data access is
        # approved (the compliance-request paths above are the occasional
        # GDPR-driven ones). Logged as 'system' — this is automated sync,
        # not a specific staff member choosing to look at a record.
        log_personal_data_access(client_id, 'shopify_order', 'order_sync',
                                 record_ref=_hash_pii_ref(order_data.get('customer_email', '')))

        if order_data.get('checkout_token'):
            recovery = models.mark_cart_recovered(
                client_id, order_data['checkout_token'],
                order_id=order_data.get('order_id'),
                revenue=order_data.get('total_amount'),
            )
            # first_time guards against Shopify's own webhook redelivery;
            # is_recovery guards against crediting Lumvi for a sale that
            # completed before any recovery email went out (see
            # mark_cart_recovered's docstring — V1 "recovered" definition).
            if recovery.get('is_recovery') and recovery.get('first_time') and recovery.get('cart_id'):
                models.create_recovery_notification(
                    client_id, recovery['cart_id'],
                    order_id=order_data.get('order_id'),
                    revenue=order_data.get('total_amount'),
                )

        logger.info(
            f'[Shopify] {topic} → order={order_data["order_id"]} '
            f'status={order_data["status"]} client={client_id}'
        )
        _log_webhook(client_id, 'shopify', topic, 'ok', phash)
        return {'status': 'ok', 'order_id': order_data['order_id']}, 200

    except Exception as e:
        logger.error(f'[Shopify] processing error client={client_id} topic={topic}: {e}')
        _log_webhook(client_id, 'shopify', topic, 'error', phash, str(e))
        return {'error': 'Processing failed'}, 500


def handle_woocommerce_webhook(client_id: str, raw_body: bytes,
                               sig_header: str, topic: str) -> tuple[dict, int]:
    """
    Verify and process one inbound WooCommerce webhook.

    Args:
        client_id:   From the URL path parameter
        raw_body:    request.get_data()
        sig_header:  request.headers.get('X-WC-Webhook-Signature')
        topic:       request.headers.get('X-WC-Webhook-Topic')
                     e.g. 'order.created', 'order.updated'

    Returns:
        (response_dict, http_status_code)
    """
    phash = _payload_hash(raw_body)

    integration = get_integration(client_id, 'woocommerce')
    if not integration:
        logger.warning(f'[WooCommerce] no integration found for client={client_id}')
        _log_webhook(client_id, 'woocommerce', topic, 'error', phash,
                     'Integration not configured')
        return {'error': 'Integration not configured'}, 404

    if not _verify_woocommerce_signature(raw_body, sig_header, integration['webhook_secret']):
        logger.warning(f'[WooCommerce] signature verification failed client={client_id}')
        _log_webhook(client_id, 'woocommerce', topic, 'sig_fail', phash,
                     'HMAC signature mismatch')
        return {'error': 'Invalid signature'}, 401

    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError as e:
        _log_webhook(client_id, 'woocommerce', topic, 'error', phash, f'JSON parse error: {e}')
        return {'error': 'Invalid JSON'}, 400

    # WooCommerce also sends a "webhook_id" ping with no real order data
    # when a webhook is first created — accept it without processing.
    if not payload.get('id') and not payload.get('line_items'):
        _log_webhook(client_id, 'woocommerce', topic, 'ok', phash, 'ping/test payload')
        return {'status': 'ok', 'note': 'ping received'}, 200

    topic = (topic or '').lower().strip()
    supported_topics = {'order.created', 'order.updated', 'order.deleted', 'order.restored'}
    if topic not in supported_topics:
        _log_webhook(client_id, 'woocommerce', topic, 'ok', phash, 'topic ignored')
        return {'status': 'ignored', 'topic': topic}, 200

    try:
        order_data = _normalise_woocommerce_order(payload, client_id)
        if not order_data.get('order_id'):
            raise ValueError('Could not extract order_id from payload')

        success = _upsert_order(client_id, order_data)
        if not success:
            raise RuntimeError('DB upsert failed')

        logger.info(
            f'[WooCommerce] {topic} → order={order_data["order_id"]} '
            f'status={order_data["status"]} client={client_id}'
        )
        _log_webhook(client_id, 'woocommerce', topic, 'ok', phash)
        return {'status': 'ok', 'order_id': order_data['order_id']}, 200

    except Exception as e:
        logger.error(f'[WooCommerce] processing error client={client_id} topic={topic}: {e}')
        _log_webhook(client_id, 'woocommerce', topic, 'error', phash, str(e))
        return {'error': 'Processing failed'}, 500


# =====================================================================
# ACUITY NORMALISERS
# Acuity sends appointment data for scheduled, rescheduled, cancelled.
# We normalise into appointment_slots + appointments tables.
# =====================================================================

def _upsert_appointment_slot(client_id: str, slot_data: dict) -> bool:
    """
    Ensure an appointment_slots row exists for this datetime/service combo.
    Acuity doesn't have an explicit slot concept — we synthesise one from
    the appointment datetime so check_availability can surface it.
    """
    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            """
            INSERT INTO appointment_slots
                (slot_id, client_id, slot_datetime, service_type, duration_minutes,
                 capacity, booked_count)
            VALUES (%s, %s, %s, %s, %s, 1, 0)
            ON CONFLICT (slot_id) DO UPDATE SET
                slot_datetime    = EXCLUDED.slot_datetime,
                service_type     = EXCLUDED.service_type,
                duration_minutes = EXCLUDED.duration_minutes
            """,
            (
                slot_data['slot_id'],
                client_id,
                slot_data['slot_datetime'],
                slot_data.get('service_type', 'general'),
                slot_data.get('duration_minutes', 30),
            )
        )
        conn.commit()
        return True
    except Exception as e:
        logger.error(f'[Webhook:upsert_slot] client={client_id} error: {e}')
        return False
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def _upsert_appointment(client_id: str, appt_data: dict) -> bool:
    """
    Write one normalised Acuity appointment into Lumvi's appointments table.
    Also increments / decrements booked_count on the slot row.
    """
    conn = cursor = None
    try:
        conn, cursor = models.get_db()

        is_cancelled = appt_data.get('status') == 'cancelled'

        # Upsert the appointment record
        cursor.execute(
            """
            INSERT INTO appointments
                (booking_id, client_id, slot_id, customer_name, customer_email,
                 customer_phone, notes, status, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (booking_id) DO UPDATE SET
                slot_id        = EXCLUDED.slot_id,
                customer_name  = EXCLUDED.customer_name,
                customer_email = EXCLUDED.customer_email,
                customer_phone = EXCLUDED.customer_phone,
                notes          = EXCLUDED.notes,
                status         = EXCLUDED.status,
                updated_at     = NOW()
            RETURNING (xmax = 0) AS is_insert
            """,
            (
                appt_data['booking_id'],
                client_id,
                appt_data['slot_id'],
                appt_data.get('customer_name', ''),
                appt_data.get('customer_email', ''),
                appt_data.get('customer_phone', ''),
                appt_data.get('notes', ''),
                appt_data.get('status', 'confirmed'),
                appt_data.get('created_at') or datetime.utcnow(),
            )
        )
        row       = cursor.fetchone()
        is_insert = row['is_insert'] if row else True

        # Adjust booked_count on the slot
        if is_insert and not is_cancelled:
            # New confirmed booking → increment
            cursor.execute(
                """
                UPDATE appointment_slots
                SET booked_count = booked_count + 1
                WHERE slot_id = %s AND client_id = %s
                """,
                (appt_data['slot_id'], client_id)
            )
        elif is_cancelled:
            # Cancellation → decrement (floor at 0)
            cursor.execute(
                """
                UPDATE appointment_slots
                SET booked_count = GREATEST(booked_count - 1, 0)
                WHERE slot_id = %s AND client_id = %s
                """,
                (appt_data['slot_id'], client_id)
            )

        conn.commit()
        return True

    except Exception as e:
        logger.error(f'[Webhook:upsert_appointment] client={client_id} error: {e}')
        if conn:
            try: conn.rollback()
            except Exception: pass
        return False
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def _normalise_acuity_appointment(payload: dict, client_id: str,
                                  event_type: str) -> tuple[dict, dict]:
    """
    Map an Acuity appointment payload → (slot_data, appointment_data).

    Acuity appointment fields used:
      id            → booking_id  ("acuity_{id}")
      datetime      → slot_datetime
      type          → service_type
      duration      → duration_minutes
      firstName + lastName → customer_name
      email         → customer_email
      phone         → customer_phone
      notes         → notes
      createdAt     → created_at
    """
    appt_id       = str(payload.get('id') or '')
    booking_id    = f'acuity_{appt_id}'
    slot_id       = f'acuity_slot_{appt_id}'   # one slot per booking for Acuity

    # Datetime
    raw_dt = payload.get('datetime') or payload.get('date') or ''
    try:
        slot_datetime = datetime.fromisoformat(
            raw_dt.replace('Z', '+00:00')
        ) if raw_dt else datetime.utcnow()
    except Exception:
        slot_datetime = datetime.utcnow()

    # Created at
    raw_created = payload.get('createdAt') or payload.get('created_at') or ''
    try:
        created_at = datetime.fromisoformat(
            raw_created.replace('Z', '+00:00')
        ) if raw_created else datetime.utcnow()
    except Exception:
        created_at = datetime.utcnow()

    # Customer
    first  = payload.get('firstName') or payload.get('first_name') or ''
    last   = payload.get('lastName')  or payload.get('last_name')  or ''
    name   = f'{first} {last}'.strip()
    email  = (payload.get('email') or '').lower().strip()
    phone  = (payload.get('phone') or '').strip()
    notes  = (payload.get('notes') or payload.get('note') or '').strip()

    # Service / duration
    service_type     = (payload.get('type') or 'general').strip().lower()
    duration_minutes = int(payload.get('duration') or 30)

    # Status
    if event_type == 'appointment.cancelled':
        status = 'cancelled'
    elif event_type == 'appointment.rescheduled':
        status = 'confirmed'
    else:
        status = 'confirmed'

    slot_data = {
        'slot_id':          slot_id,
        'slot_datetime':    slot_datetime,
        'service_type':     service_type,
        'duration_minutes': duration_minutes,
    }

    appt_data = {
        'booking_id':     booking_id,
        'slot_id':        slot_id,
        'customer_name':  name,
        'customer_email': email,
        'customer_phone': phone,
        'notes':          notes,
        'status':         status,
        'created_at':     created_at,
    }

    return slot_data, appt_data


# =====================================================================
# CALENDLY NORMALISER
# Calendly's v2 webhook payload nests everything under "payload".
# invitee.created / invitee.canceled are the two events we handle.
# https://developer.calendly.com/api-docs/b92768854bc06-invitee-payload
# =====================================================================

def _normalise_calendly_appointment(payload: dict, client_id: str,
                                    event_type: str) -> tuple[dict, dict]:
    """
    Map a Calendly webhook payload → (slot_data, appointment_data).

    Calendly fields used (all under payload['payload']):
      uri                        → booking_id (last URI path segment)
      calendar_event.start_time  → slot_datetime
      calendar_event.end_time    → used to compute duration_minutes
      event_type.name            → service_type
      name                       → customer_name
      email                      → customer_email
      created_at                 → created_at
    """
    inner = payload.get('payload') or {}

    # Calendly resource URIs look like https://api.calendly.com/scheduled_events/{uuid}/invitees/{uuid}
    invitee_uri = inner.get('uri') or ''
    invitee_id  = invitee_uri.rstrip('/').split('/')[-1] or _payload_hash(json.dumps(inner).encode())[:16]
    booking_id  = f'calendly_{invitee_id}'
    slot_id     = f'calendly_slot_{invitee_id}'

    cal_event = inner.get('calendar_event') or {}
    raw_start = cal_event.get('start_time') or ''
    raw_end   = cal_event.get('end_time') or ''
    try:
        slot_datetime = datetime.fromisoformat(raw_start.replace('Z', '+00:00')) if raw_start else datetime.utcnow()
    except Exception:
        slot_datetime = datetime.utcnow()

    duration_minutes = 30
    try:
        if raw_start and raw_end:
            start_dt = datetime.fromisoformat(raw_start.replace('Z', '+00:00'))
            end_dt   = datetime.fromisoformat(raw_end.replace('Z', '+00:00'))
            duration_minutes = max(int((end_dt - start_dt).total_seconds() / 60), 1)
    except Exception:
        pass

    raw_created = inner.get('created_at') or ''
    try:
        created_at = datetime.fromisoformat(raw_created.replace('Z', '+00:00')) if raw_created else datetime.utcnow()
    except Exception:
        created_at = datetime.utcnow()

    name  = (inner.get('name') or '').strip()
    email = (inner.get('email') or '').lower().strip()
    service_type = ((inner.get('event_type') or {}).get('name') or 'general').strip().lower()

    status = 'cancelled' if event_type == 'invitee.canceled' else 'confirmed'

    slot_data = {
        'slot_id':          slot_id,
        'slot_datetime':    slot_datetime,
        'service_type':     service_type,
        'duration_minutes': duration_minutes,
    }
    appt_data = {
        'booking_id':     booking_id,
        'slot_id':        slot_id,
        'customer_name':  name,
        'customer_email': email,
        'customer_phone': '',   # Calendly's webhook payload doesn't include phone
        'notes':          '',
        'status':         status,
        'created_at':     created_at,
    }
    return slot_data, appt_data


# =====================================================================
# SQUARE NORMALISER
# Square's booking.created/booking.updated events nest the booking under
# data.object.booking. Square only provides customer_id in the webhook
# (not name/email/phone) — resolving that to a real name would require a
# separate Customers API call, which this webhook-only integration
# deliberately doesn't make. customer_name is left blank rather than
# guessed; tools.py callers should treat that as "known, but nameless".
# https://developer.squareup.com/reference/square/bookings-api/webhooks/booking.created
# =====================================================================

_SQUARE_CANCELLED_STATUSES = {'CANCELLED_BY_SELLER', 'CANCELLED_BY_CUSTOMER', 'DECLINED'}


def _normalise_square_booking(payload: dict, client_id: str) -> tuple[dict, dict]:
    booking = (((payload.get('data') or {}).get('object') or {}).get('booking')) or {}

    booking_id = str(booking.get('id') or '')
    slot_id    = f'square_slot_{booking_id}'

    raw_start = booking.get('start_at') or ''
    try:
        slot_datetime = datetime.fromisoformat(raw_start.replace('Z', '+00:00')) if raw_start else datetime.utcnow()
    except Exception:
        slot_datetime = datetime.utcnow()

    segments = booking.get('appointment_segments') or []
    duration_minutes = int(segments[0].get('duration_minutes', 30)) if segments else 30
    service_type = 'general'   # service_variation_id is a Catalog API reference, not a readable name

    raw_created = booking.get('created_at') or ''
    try:
        created_at = datetime.fromisoformat(raw_created.replace('Z', '+00:00')) if raw_created else datetime.utcnow()
    except Exception:
        created_at = datetime.utcnow()

    status = 'cancelled' if (booking.get('status') or '').upper() in _SQUARE_CANCELLED_STATUSES else 'confirmed'

    slot_data = {
        'slot_id':          slot_id,
        'slot_datetime':    slot_datetime,
        'service_type':     service_type,
        'duration_minutes': duration_minutes,
    }
    appt_data = {
        'booking_id':     f'square_{booking_id}',
        'slot_id':        slot_id,
        'customer_name':  '',   # see module note above — Square doesn't send this in the webhook
        'customer_email': '',
        'customer_phone': '',
        'notes':          (booking.get('customer_note') or '').strip(),
        'status':         status,
        'created_at':     created_at,
    }
    return slot_data, appt_data


def handle_acuity_webhook(client_id: str, raw_body: bytes,
                          sig_header: str, event_type: str) -> tuple[dict, int]:
    """
    Verify and process one inbound Acuity webhook.

    Args:
        client_id:    From the URL path parameter
        raw_body:     request.get_data()
        sig_header:   request.headers.get('X-Acuity-Signature')
        event_type:   request.form.get('action') or request.json.get('action')
                      e.g. 'appointment.scheduled', 'appointment.rescheduled',
                           'appointment.cancelled'

    Returns:
        (response_dict, http_status_code)
    """
    phash = _payload_hash(raw_body)

    # 1. Load integration config
    integration = get_integration(client_id, 'acuity')
    if not integration:
        logger.warning(f'[Acuity] no integration found for client={client_id}')
        _log_webhook(client_id, 'acuity', event_type, 'error', phash,
                     'Integration not configured')
        return {'error': 'Integration not configured'}, 404

    # 2. Verify signature
    if not _verify_acuity_signature(raw_body, sig_header, integration['webhook_secret']):
        logger.warning(f'[Acuity] signature verification failed client={client_id}')
        _log_webhook(client_id, 'acuity', event_type, 'sig_fail', phash,
                     'HMAC signature mismatch')
        return {'error': 'Invalid signature'}, 401

    # 3. Parse payload — Acuity sends form-encoded or JSON depending on event
    try:
        if raw_body.startswith(b'{'):
            payload = json.loads(raw_body)
        else:
            from urllib.parse import parse_qs
            qs = parse_qs(raw_body.decode('utf-8'))
            # parse_qs gives lists — flatten to single values
            payload = {k: v[0] if len(v) == 1 else v for k, v in qs.items()}
            # Acuity may also nest appointment JSON under 'appointment'
            if 'appointment' in payload and isinstance(payload['appointment'], str):
                try:
                    payload = json.loads(payload['appointment'])
                except Exception:
                    pass
    except Exception as e:
        _log_webhook(client_id, 'acuity', event_type, 'error', phash,
                     f'Parse error: {e}')
        return {'error': 'Invalid payload'}, 400

    # 4. Route by event type
    supported_events = {
        'appointment.scheduled',
        'appointment.rescheduled',
        'appointment.cancelled',
    }

    if event_type not in supported_events:
        _log_webhook(client_id, 'acuity', event_type, 'ok', phash, 'event ignored')
        return {'status': 'ignored', 'event': event_type}, 200

    # 5. Normalise and upsert
    try:
        slot_data, appt_data = _normalise_acuity_appointment(
            payload, client_id, event_type
        )

        if not appt_data.get('booking_id'):
            raise ValueError('Could not extract booking_id from payload')

        # For rescheduled: we synthesise a new slot, so the old slot's
        # booked_count will be decremented by the 'cancelled' path on the
        # OLD booking_id. Acuity sends a cancellation event for the old
        # appointment automatically before the rescheduled event, so
        # booked_count stays accurate without special-casing here.

        _upsert_appointment_slot(client_id, slot_data)
        success = _upsert_appointment(client_id, appt_data)

        if not success:
            raise RuntimeError('DB upsert failed')

        logger.info(
            f'[Acuity] {event_type} → booking={appt_data["booking_id"]} '
            f'status={appt_data["status"]} client={client_id}'
        )
        _log_webhook(client_id, 'acuity', event_type, 'ok', phash)
        return {'status': 'ok', 'booking_id': appt_data['booking_id']}, 200

    except Exception as e:
        logger.error(f'[Acuity] processing error client={client_id} event={event_type}: {e}')
        _log_webhook(client_id, 'acuity', event_type, 'error', phash, str(e))
        return {'error': 'Processing failed'}, 500


def handle_calendly_webhook(client_id: str, raw_body: bytes,
                            sig_header: str) -> tuple[dict, int]:
    """
    Verify and process one inbound Calendly webhook.

    Args:
        client_id:   From the URL path parameter
        raw_body:    request.get_data()
        sig_header:  request.headers.get('Calendly-Webhook-Signature')

    Returns:
        (response_dict, http_status_code)
    """
    phash = _payload_hash(raw_body)

    integration = get_integration(client_id, 'calendly')
    if not integration:
        logger.warning(f'[Calendly] no integration found for client={client_id}')
        _log_webhook(client_id, 'calendly', None, 'error', phash,
                     'Integration not configured')
        return {'error': 'Integration not configured'}, 404

    if not _verify_calendly_signature(raw_body, sig_header, integration['webhook_secret']):
        logger.warning(f'[Calendly] signature verification failed client={client_id}')
        _log_webhook(client_id, 'calendly', None, 'sig_fail', phash,
                     'HMAC signature mismatch')
        return {'error': 'Invalid signature'}, 401

    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError as e:
        _log_webhook(client_id, 'calendly', None, 'error', phash, f'JSON parse error: {e}')
        return {'error': 'Invalid JSON'}, 400

    event_type = (payload.get('event') or '').strip()
    supported_events = {'invitee.created', 'invitee.canceled'}
    if event_type not in supported_events:
        # routing_form_submission.created and anything else — acknowledge, don't process
        _log_webhook(client_id, 'calendly', event_type, 'ok', phash, 'event ignored')
        return {'status': 'ignored', 'event': event_type}, 200

    try:
        slot_data, appt_data = _normalise_calendly_appointment(payload, client_id, event_type)
        if not appt_data.get('booking_id'):
            raise ValueError('Could not extract booking_id from payload')

        _upsert_appointment_slot(client_id, slot_data)
        success = _upsert_appointment(client_id, appt_data)
        if not success:
            raise RuntimeError('DB upsert failed')

        logger.info(
            f'[Calendly] {event_type} → booking={appt_data["booking_id"]} '
            f'status={appt_data["status"]} client={client_id}'
        )
        _log_webhook(client_id, 'calendly', event_type, 'ok', phash)
        return {'status': 'ok', 'booking_id': appt_data['booking_id']}, 200

    except Exception as e:
        logger.error(f'[Calendly] processing error client={client_id} event={event_type}: {e}')
        _log_webhook(client_id, 'calendly', event_type, 'error', phash, str(e))
        return {'error': 'Processing failed'}, 500


def handle_square_webhook(client_id: str, raw_body: bytes,
                          sig_header: str, notification_url: str) -> tuple[dict, int]:
    """
    Verify and process one inbound Square webhook.

    Args:
        client_id:          From the URL path parameter
        raw_body:            request.get_data()
        sig_header:           request.headers.get('x-square-hmacsha256-signature')
        notification_url:    the FULL webhook URL exactly as registered in the
                              Square dashboard — required because Square signs
                              (url + body), not body alone. See
                              _verify_square_signature's module note.

    Returns:
        (response_dict, http_status_code)
    """
    phash = _payload_hash(raw_body)

    integration = get_integration(client_id, 'square')
    if not integration:
        logger.warning(f'[Square] no integration found for client={client_id}')
        _log_webhook(client_id, 'square', None, 'error', phash,
                     'Integration not configured')
        return {'error': 'Integration not configured'}, 404

    if not _verify_square_signature(raw_body, sig_header, integration['webhook_secret'], notification_url):
        logger.warning(f'[Square] signature verification failed client={client_id}')
        _log_webhook(client_id, 'square', None, 'sig_fail', phash,
                     'HMAC signature mismatch')
        return {'error': 'Invalid signature'}, 401

    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError as e:
        _log_webhook(client_id, 'square', None, 'error', phash, f'JSON parse error: {e}')
        return {'error': 'Invalid JSON'}, 400

    event_type = (payload.get('type') or '').strip()
    supported_events = {'booking.created', 'booking.updated'}
    if event_type not in supported_events:
        _log_webhook(client_id, 'square', event_type, 'ok', phash, 'event ignored')
        return {'status': 'ignored', 'event': event_type}, 200

    try:
        slot_data, appt_data = _normalise_square_booking(payload, client_id)
        if not appt_data.get('booking_id') or appt_data['booking_id'] == 'square_':
            raise ValueError('Could not extract booking id from payload')

        _upsert_appointment_slot(client_id, slot_data)
        success = _upsert_appointment(client_id, appt_data)
        if not success:
            raise RuntimeError('DB upsert failed')

        logger.info(
            f'[Square] {event_type} → booking={appt_data["booking_id"]} '
            f'status={appt_data["status"]} client={client_id}'
        )
        _log_webhook(client_id, 'square', event_type, 'ok', phash)
        return {'status': 'ok', 'booking_id': appt_data['booking_id']}, 200

    except Exception as e:
        logger.error(f'[Square] processing error client={client_id} event={event_type}: {e}')
        _log_webhook(client_id, 'square', event_type, 'error', phash, str(e))
        return {'error': 'Processing failed'}, 500


# =====================================================================
# FLASK ROUTE REGISTRATION
# Call register_webhook_routes(app) once in app.py.
# All webhook routes are mounted under /webhooks/.
# =====================================================================

def register_webhook_routes(app, mail=None):
    """
    Mount all webhook receiver routes onto the Flask app.

    Call once in app.py after creating the Flask app:
        from webhooks import register_webhook_routes, migrate_integrations
        migrate_integrations()
        register_webhook_routes(app, mail=mail)

    mail (optional): a Flask-Mail Mail instance, already bound to this app
    (app.py creates it via `mail = Mail(app)`). Used only by the
    customers/data_request compliance handler to notify the merchant their
    export is ready — best-effort, never blocks or fails the webhook
    response if omitted or if sending fails. See handle_shopify_compliance_
    webhook's docstring.

    Routes mounted:
        POST /webhooks/shopify/<client_id>
        POST /webhooks/shopify/compliance        (mandatory GDPR topics — see
                                                    handle_shopify_compliance_webhook)
        POST /webhooks/acuity/<client_id>
        GET  /webhooks/health                   (uptime check, no auth)

    Dashboard / management routes:
        POST   /api/integrations/<client_id>            upsert integration
        GET    /api/integrations/<client_id>            list integrations
        DELETE /api/integrations/<client_id>/<platform> deactivate integration
    """
    from flask import request, jsonify

    # ── Webhook receivers ─────────────────────────────────────────────

    @app.route('/webhooks/shopify/<client_id>', methods=['POST'])
    def shopify_webhook(client_id):
        raw_body     = request.get_data()
        hmac_header  = request.headers.get('X-Shopify-Hmac-Sha256', '')
        topic        = request.headers.get('X-Shopify-Topic', '')
        shop_domain  = request.headers.get('X-Shopify-Shop-Domain', '')
        result, status = handle_shopify_webhook(client_id, raw_body, hmac_header, topic, shop_domain)
        return jsonify(result), status

    @app.route('/webhooks/shopify/compliance', methods=['POST'])
    def shopify_compliance_webhook():
        raw_body    = request.get_data()
        hmac_header = request.headers.get('X-Shopify-Hmac-Sha256', '')
        topic       = request.headers.get('X-Shopify-Topic', '')
        result, status = handle_shopify_compliance_webhook(raw_body, hmac_header, topic, mail=mail)
        return jsonify(result), status

    @app.route('/webhooks/acuity/<client_id>', methods=['POST'])
    def acuity_webhook(client_id):
        raw_body   = request.get_data()
        sig_header = request.headers.get('X-Acuity-Signature', '')
        # Acuity sends event type in the body or as a query param
        try:
            event_type = (
                request.args.get('action') or
                request.json.get('action') or
                request.form.get('action') or
                'appointment.scheduled'
            )
        except Exception:
            event_type = 'appointment.scheduled'
        result, status = handle_acuity_webhook(client_id, raw_body, sig_header, event_type)
        return jsonify(result), status

    @app.route('/webhooks/calendly/<client_id>', methods=['POST'])
    def calendly_webhook(client_id):
        raw_body   = request.get_data()
        sig_header = request.headers.get('Calendly-Webhook-Signature', '')
        result, status = handle_calendly_webhook(client_id, raw_body, sig_header)
        return jsonify(result), status

    @app.route('/webhooks/woocommerce/<client_id>', methods=['POST'])
    def woocommerce_webhook(client_id):
        raw_body   = request.get_data()
        sig_header = request.headers.get('X-WC-Webhook-Signature', '')
        topic      = request.headers.get('X-WC-Webhook-Topic', '')
        result, status = handle_woocommerce_webhook(client_id, raw_body, sig_header, topic)
        return jsonify(result), status

    @app.route('/webhooks/square/<client_id>', methods=['POST'])
    def square_webhook(client_id):
        raw_body   = request.get_data()
        sig_header = request.headers.get('x-square-hmacsha256-signature', '')
        # Square signs (notification_url + body) using the exact URL
        # registered in the Square dashboard. Built from APP_BASE_URL
        # (same env var used elsewhere for webhook_url) rather than
        # request.url — this app has no ProxyFix/X-Forwarded-Proto
        # trust configured, so behind Render's reverse proxy request.url
        # could report http:// instead of https://, which would silently
        # fail every signature check even with the correct secret.
        base_url = os.environ.get('APP_BASE_URL', 'https://app.lumvi.ai').rstrip('/')
        notification_url = f'{base_url}/webhooks/square/{client_id}'
        result, status = handle_square_webhook(client_id, raw_body, sig_header, notification_url)
        return jsonify(result), status

    @app.route('/webhooks/health', methods=['GET'])
    def webhook_health():
        return jsonify({'status': 'ok', 'service': 'lumvi-webhooks'}), 200

    # NOTE: dashboard-management routes (create/list/delete integration) are
    # NOT registered here. They previously were, at these exact same URLs
    # (/api/integrations/<client_id> POST/GET, /api/integrations/<client_id>/
    # <platform> DELETE) — with no @login_required or ownership check at
    # all. app.py registers its own, properly-secured versions of these
    # same routes (create_platform_integration, list_platform_integrations,
    # delete_platform_integration — see app.py) using the exact same
    # upsert_integration/list_integrations/delete_integration functions
    # from this file. Because Flask/Werkzeug dispatches to the FIRST
    # registered matching route for identical URL+method pairs, and this
    # function used to run before app.py's route definitions, the
    # unauthenticated versions here were silently shadowing app.py's
    # secured ones — anyone who knew or guessed a client_id could create,
    # view, or delete another merchant's integration with zero login.
    # Removed rather than fixed-in-place so there is exactly one
    # implementation of each route, not two that can drift apart again.

    logger.info('[Webhooks] Routes registered: /webhooks/shopify, /webhooks/acuity, /webhooks/health')


# =====================================================================
# ONBOARDING INSTRUCTIONS
# Returned to the dashboard so the merchant knows exactly what to paste
# into their platform's settings.
# =====================================================================

def _onboarding_instructions(platform: str, webhook_url: str) -> dict:
    """
    Human-readable setup instructions for the merchant.
    Returned as part of the POST /api/integrations response.
    """
    if platform == 'shopify':
        return {
            'title': 'Connect Shopify',
            'steps': [
                'In the Shopify admin, go to Settings → Notifications',
                'Scroll to the bottom and click "Create webhook"',
                f'Paste this URL: {webhook_url}',
                'Set Format to JSON',
                'Subscribe to: orders/create, orders/updated, orders/cancelled',
                'If you plan to turn on cart recovery, also subscribe to: '
                'checkouts/create, checkouts/update',
                'Copy the "Webhook signing secret" shown after saving',
                'Paste the signing secret back into the Lumvi dashboard',
            ],
            'note': (
                'Lumvi only stores order ID, status, customer name/email, '
                'items, and total. No payment details are ever stored. '
                'Checkout data is only captured if cart recovery is turned '
                'on for your store.'
            ),
        }

    if platform == 'acuity':
        return {
            'title': 'Connect Acuity Scheduling',
            'steps': [
                'In Acuity, go to Integrations → Webhooks',
                'Click "Add webhook"',
                f'Paste this URL: {webhook_url}',
                'Check: appointment.scheduled, appointment.rescheduled, appointment.cancelled',
                'Copy the secret key shown after saving',
                'Paste the secret key back into the Lumvi dashboard',
            ],
            'note': (
                'Lumvi stores appointment time, service type, and customer '
                'contact details. No payment or personal health info is stored.'
            ),
        }

    if platform == 'calendly':
        return {
            'title': 'Connect Calendly',
            'steps': [
                'In Calendly, go to Integrations → Webhooks (requires a paid Calendly plan)',
                'Click "Create Webhook Subscription"',
                f'Paste this URL: {webhook_url}',
                'Subscribe to: invitee.created, invitee.canceled',
                'Copy the signing key shown after saving',
                'Paste the signing key back into the Lumvi dashboard',
            ],
            'note': (
                'Lumvi stores the scheduled time, event type, and the '
                "invitee's name and email. Calendly's webhook payload does "
                'not include phone number.'
            ),
        }

    if platform == 'woocommerce':
        return {
            'title': 'Connect WooCommerce',
            'steps': [
                'In WordPress admin, go to WooCommerce → Settings → Advanced → Webhooks',
                'Click "Add webhook"',
                f'Set Delivery URL to: {webhook_url}',
                'Set Topic to: Order created (add a second webhook for Order updated if you want status changes tracked)',
                'Set Secret to any password you choose',
                'Paste that same secret into the Lumvi dashboard',
            ],
            'note': (
                'Lumvi only stores order ID, status, customer name/email, '
                'line items, and total. No payment card details are ever stored.'
            ),
        }

    if platform == 'square':
        return {
            'title': 'Connect Square',
            'steps': [
                'In the Square Developer Dashboard, open your application',
                'Go to Webhooks → Subscriptions, click "Add Subscription"',
                f'Set Notification URL to exactly: {webhook_url}',
                'Subscribe to: booking.created, booking.updated',
                'Copy the Signature Key shown after saving',
                'Paste the signature key back into the Lumvi dashboard',
            ],
            'note': (
                'Lumvi stores the appointment time and any note left by the '
                'customer. Square only sends a customer reference ID in the '
                "webhook, not their name or email — the customer's identity "
                "isn't available unless you look them up in Square directly. "
                'The Notification URL must match exactly (including https://) '
                'or Square signature checks will fail.'
            ),
        }

    return {}
