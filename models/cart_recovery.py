"""
models/cart_recovery.py
========================
DB helpers for the "cart recovery automation" feature (ai_growth/ai_scale
plan flag 'cart_recovery' — see PLAN_LIMITS in app.py).

Flow:
  1. A Shopify checkouts/create or checkouts/update webhook comes in for a
     client with cart_recovery_enabled=True. webhooks.py's topic dispatch
     calls upsert_abandoned_cart() with the checkout payload. NOT this
     module's job to verify the webhook signature or decide whether the
     client has cart_recovery enabled — that belongs to whatever calls in
     here, same as every other inbound-webhook handler.
  2. blueprints/cron.py's /cron/cart-recovery job calls
     get_carts_due_for_recovery_email() (default: abandoned >1h, no email
     sent yet) and sends one recovery email per cart from
     notifications@lumvi.net, with Reply-To set to a unique per-cart
     address at the dedicated inbound-parsing subdomain
     (cart-{id}@reply.lumvi.net). Marks each with mark_recovery_email_sent().
  3. If the customer replies, Brevo's inbound parsing webhook POSTs to
     blueprints/inbound_email.py, which calls
     get_cart_by_reply_local_part() to find which cart/client the reply
     belongs to, then forwards it to that client's notification_email.
  4. Whenever a matching order comes in for a client with pending
     abandoned carts (webhooks.py's orders/create handler), it should call
     mark_cart_recovered() — NOT implemented in this pass, see the
     accompanying chat message for why.

No conversion/order-matching is done automatically by cron — the send job
only checks status='pending' at send time, so this table needs SOMETHING
external (webhooks.py's orders/create handler) to ever mark a cart
'recovered', or it will re-appear if a future re-send pass is added.
Currently there's only ONE send per cart (see get_carts_due_for_recovery_email's
docstring) so this gap has no user-visible effect yet, but flagging it
here for whoever wires orders/create next.
"""
import json
import secrets

from .db import get_db


def upsert_abandoned_cart(client_id: str, checkout_token: str,
                           customer_email: str = None, customer_name: str = None,
                           cart_total=None, currency: str = None,
                           line_items: list = None, checkout_url: str = None,
                           platform: str = 'shopify') -> dict:
    """
    Insert or refresh an abandoned checkout row. Shopify sends
    checkouts/update repeatedly as the customer fills in the form
    (email added, shipping address added, etc.) — each one should call
    this again with the same checkout_token so the row reflects the
    latest state without creating duplicates.

    Generates a unique reply_local_part (e.g. 'cart-a1b2c3') on first
    insert only — an UPDATE never touches it, so the same reply address
    keeps working across the checkout's whole lifecycle.

    Returns {success, cart_id} or {success: False, error}.
    """
    try:
        conn, cursor = get_db()
        cursor.execute(
            'SELECT id FROM abandoned_carts WHERE client_id = %s AND checkout_token = %s',
            (client_id, checkout_token)
        )
        row = cursor.fetchone()

        if row:
            cart_id = row['id']
            cursor.execute(
                '''
                UPDATE abandoned_carts
                SET customer_email = COALESCE(%s, customer_email),
                    customer_name  = COALESCE(%s, customer_name),
                    cart_total     = COALESCE(%s, cart_total),
                    currency       = COALESCE(%s, currency),
                    line_items     = COALESCE(%s, line_items),
                    checkout_url   = COALESCE(%s, checkout_url)
                WHERE id = %s
                ''',
                (customer_email, customer_name, cart_total, currency,
                 json.dumps(line_items) if line_items is not None else None,
                 checkout_url, cart_id)
            )
        else:
            reply_local_part = f'cart-{secrets.token_hex(4)}'  # e.g. 'cart-9f3a1c02'
            cursor.execute(
                '''
                INSERT INTO abandoned_carts
                    (client_id, platform, checkout_token, customer_email,
                     customer_name, cart_total, currency, line_items,
                     checkout_url, reply_local_part)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                ''',
                (client_id, platform, checkout_token, customer_email,
                 customer_name, cart_total, currency,
                 json.dumps(line_items) if line_items is not None else None,
                 checkout_url, reply_local_part)
            )
            cart_id = cursor.fetchone()['id']

        conn.commit()
        cursor.close()
        conn.close()
        return {'success': True, 'cart_id': cart_id}
    except Exception as e:
        return {'success': False, 'error': str(e)}


def mark_cart_recovered(client_id: str, checkout_token: str) -> bool:
    """
    Call when a matching order comes in (webhooks.py's orders/create
    handler — not yet wired, see module docstring). Prevents a recovery
    email going out for a cart that already converted.
    """
    try:
        conn, cursor = get_db()
        cursor.execute(
            '''
            UPDATE abandoned_carts
            SET status = 'recovered', recovered_at = NOW()
            WHERE client_id = %s AND checkout_token = %s AND status != 'recovered'
            ''',
            (client_id, checkout_token)
        )
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception:
        return False


def get_carts_due_for_recovery_email(delay_hours: int = 1, limit: int = 200) -> list:
    """
    Carts abandoned at least delay_hours ago, still 'pending', with a
    known customer_email, that haven't had a recovery email sent yet.

    Single-touch only for this pass — recovery_email_sent_at IS NULL is
    the whole condition, so once sent a cart is never re-emailed even if
    it's still 'pending' days later. A second/third follow-up touch would
    need an extra column (e.g. touch_count) and a different WHERE clause —
    flagging as a clear extension point, not building it speculatively now.

    Joins clients for cart_recovery_enabled and notification_email so the
    cron job doesn't need a second query per cart.
    """
    try:
        conn, cursor = get_db()
        cursor.execute(
            '''
            SELECT ac.*, c.cart_recovery_enabled, c.notification_email,
                   c.business_name
            FROM abandoned_carts ac
            JOIN clients c ON c.client_id = ac.client_id
            WHERE ac.status = 'pending'
              AND ac.customer_email IS NOT NULL
              AND ac.recovery_email_sent_at IS NULL
              AND ac.abandoned_at <= NOW() - (%s * INTERVAL '1 hour')
              AND c.cart_recovery_enabled = TRUE
            ORDER BY ac.abandoned_at ASC
            LIMIT %s
            ''',
            (delay_hours, limit)
        )
        rows = cursor.fetchall() or []
        cursor.close()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def mark_recovery_email_sent(cart_id: int) -> bool:
    try:
        conn, cursor = get_db()
        cursor.execute(
            "UPDATE abandoned_carts SET recovery_email_sent_at = NOW(), status = 'sent' WHERE id = %s",
            (cart_id,)
        )
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception:
        return False


def get_cart_by_reply_local_part(local_part: str) -> dict:
    """
    Looks up which cart (and which client's notification_email to forward
    to) a reply address like 'cart-9f3a1c02@reply.lumvi.net' belongs to —
    local_part is everything before the @.

    Returns the cart row merged with the client's notification_email and
    business_name, or None if the local part doesn't match any cart.
    """
    try:
        conn, cursor = get_db()
        cursor.execute(
            '''
            SELECT ac.*, c.notification_email, c.business_name
            FROM abandoned_carts ac
            JOIN clients c ON c.client_id = ac.client_id
            WHERE ac.reply_local_part = %s
            ''',
            (local_part,)
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        return dict(row) if row else None
    except Exception:
        return None


def increment_reply_forwarded(cart_id: int) -> bool:
    try:
        conn, cursor = get_db()
        cursor.execute(
            'UPDATE abandoned_carts SET reply_forwarded_count = reply_forwarded_count + 1 WHERE id = %s',
            (cart_id,)
        )
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception:
        return False
