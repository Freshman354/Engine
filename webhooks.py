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

The agency sets this up once in the Lumvi dashboard. The small
business owner then pastes the generated webhook URL into their
platform's settings. No ongoing maintenance needed.

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
  • shopify      — orders/created, orders/updated, orders/cancelled
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
from datetime import datetime
from functools import wraps

import models

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

        conn.commit()
        print('✅ migrate_integrations: client_integrations + webhook_log ready')

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

    Called from the Lumvi agency dashboard when an operator sets up
    a new platform connection. Returns True on success.

    Args:
        client_id:        Lumvi client identifier
        platform:         'shopify' | 'acuity'
        webhook_secret:   The HMAC secret from the platform
        platform_config:  Optional JSON blob (e.g. {'shop_domain': 'mystore.myshopify.com'})
    """
    conn = cursor = None
    try:
        conn, cursor = models.get_db()
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
             json.dumps(platform_config or {}))
        )
        conn.commit()
        logger.info(f'[Integration] upserted client={client_id} platform={platform}')
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
            'platform_config': cfg,
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
            result.append({
                'platform':        row['platform'],
                'platform_config': _redact_platform_config(cfg) if redact else cfg,
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


def delete_integration(client_id: str, platform: str) -> bool:
    """Soft-delete (deactivate) an integration. Webhook events are rejected after this."""
    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            "UPDATE client_integrations SET is_active = FALSE, updated_at = NOW() "
            "WHERE client_id = %s AND platform = %s",
            (client_id, platform)
        )
        conn.commit()
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


def handle_shopify_webhook(client_id: str, raw_body: bytes,
                           hmac_header: str, topic: str) -> tuple[dict, int]:
    """
    Verify and process one inbound Shopify webhook.

    Args:
        client_id:    From the URL path parameter
        raw_body:     request.get_data() — raw bytes before any parsing
        hmac_header:  request.headers.get('X-Shopify-Hmac-Sha256')
        topic:        request.headers.get('X-Shopify-Topic')
                      e.g. 'orders/created', 'orders/updated', 'orders/cancelled'

    Returns:
        (response_dict, http_status_code)
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

    # 3. Parse payload
    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError as e:
        _log_webhook(client_id, 'shopify', topic, 'error', phash, f'JSON parse error: {e}')
        return {'error': 'Invalid JSON'}, 400

    # 4. Route by topic
    topic = (topic or '').lower().strip()
    supported_topics = {'orders/created', 'orders/updated', 'orders/cancelled',
                        'orders/fulfilled', 'orders/paid'}

    if topic not in supported_topics:
        # Return 200 to prevent Shopify from retrying non-order topics
        _log_webhook(client_id, 'shopify', topic, 'ok', phash, 'topic ignored')
        return {'status': 'ignored', 'topic': topic}, 200

    # 5. Normalise and upsert
    try:
        order_data = _normalise_shopify_order(payload, client_id)
        if not order_data.get('order_id'):
            raise ValueError('Could not extract order_id from payload')

        success = _upsert_order(client_id, order_data)
        if not success:
            raise RuntimeError('DB upsert failed')

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

def register_webhook_routes(app):
    """
    Mount all webhook receiver routes onto the Flask app.

    Call once in app.py after creating the Flask app:
        from webhooks import register_webhook_routes, migrate_integrations
        migrate_integrations()
        register_webhook_routes(app)

    Routes mounted:
        POST /webhooks/shopify/<client_id>
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
        raw_body    = request.get_data()
        hmac_header = request.headers.get('X-Shopify-Hmac-Sha256', '')
        topic       = request.headers.get('X-Shopify-Topic', '')
        result, status = handle_shopify_webhook(client_id, raw_body, hmac_header, topic)
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
    # view, or delete another agency's integration with zero login.
    # Removed rather than fixed-in-place so there is exactly one
    # implementation of each route, not two that can drift apart again.

    logger.info('[Webhooks] Routes registered: /webhooks/shopify, /webhooks/acuity, /webhooks/health')


# =====================================================================
# ONBOARDING INSTRUCTIONS
# Returned to the dashboard so the agency knows exactly what to paste
# into the platform settings for their client.
# =====================================================================

def _onboarding_instructions(platform: str, webhook_url: str) -> dict:
    """
    Human-readable setup instructions for the agency operator.
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
                'Subscribe to: orders/created, orders/updated, orders/cancelled',
                'Copy the "Webhook signing secret" shown after saving',
                'Paste the signing secret back into the Lumvi dashboard',
            ],
            'note': (
                'Lumvi only stores order ID, status, customer name/email, '
                'items, and total. No payment details are ever stored.'
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
