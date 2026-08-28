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
     claim_carts_for_recovery_email() (default: abandoned >1h, no email
     sent yet), which atomically claims a batch (status → 'sending',
     via SELECT ... FOR UPDATE SKIP LOCKED so two concurrent cron runs
     can never claim the same row — see migrate_cart_recovery_attribution's
     docstring for why this exists) and sends one recovery email per cart
     from notifications@lumvi.net, with Reply-To set to a unique per-cart
     address at the dedicated inbound-parsing subdomain
     (cart-{id}@reply.lumvi.net). A successful send calls
     mark_recovery_email_sent() (status → 'sent'); a failed send calls
     revert_recovery_email_claim() so the cart goes back to 'pending' and
     can be retried next run.
  3. If the customer replies, Brevo's inbound parsing webhook POSTs to
     blueprints/inbound_email.py, which calls
     get_cart_by_reply_local_part() to find which cart/client the reply
     belongs to, then forwards it to that client's notification_email.
  4. Whenever a matching order comes in (webhooks.py's orders/create
     handler), it calls mark_cart_recovered() with the completed order's
     id and actual total_amount. This sets status='recovered',
     recovered_at, recovered_order_id, recovered_revenue — and, on a
     genuine first-time transition only (never on a duplicate/redelivered
     webhook), the caller creates a recovery_notifications row via
     create_recovery_notification() for the merchant's in-app feed.

get_carts_due_for_recovery_email() (read-only, no claiming) is kept
below for any future read-only "what's pending" view — the live send
path uses claim_carts_for_recovery_email() instead.
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


def mark_cart_recovered(client_id: str, checkout_token: str,
                         order_id: str = None, revenue=None) -> dict:
    """
    Call when a matching order comes in (webhooks.py's orders/create
    handler). Records what an abandoned checkout converted to — but
    only counts it as a LUMVI recovery if a recovery email actually
    went out first.

    V1 definition of "Lumvi recovered a cart" (product decision):
      1. shopper abandoned checkout
      2. Lumvi successfully sent the recovery email
      3. shopper completed that same checkout/order afterward
      4. Lumvi matched the order back to the abandoned checkout
    If the order completes before step 2 ever happens
    (recovery_email_sent_at IS NULL), the sale is real but not
    attributable to Lumvi — it must not inflate recovered_carts,
    recovery_rate, or recovered_revenue, and must not trigger a
    merchant "Lumvi recovered a sale" notification.

    Rather than leave that cart's status at 'pending' (which would let
    the cron job claim it later and email a recovery message to a
    customer who already bought — a real regression this guards
    against), it transitions to a distinct terminal status,
    'converted_early'. This is the smallest change that satisfies both
    constraints: analytics/notifications only ever look at
    status='recovered', and claim_carts_for_recovery_email's WHERE
    clause only ever matches 'pending'/stale-'sending', so
    'converted_early' is excluded from both without adding a column or
    a new table — status was already a free-text VARCHAR(20) with no
    CHECK constraint limiting its values.

    order_id/revenue are recorded in EITHER case (useful data either
    way — a full picture of what happened to the cart — even though
    only the 'recovered' case counts toward Lumvi's own metrics).

    Idempotency (WHERE status NOT IN ('recovered', 'converted_early')):
    once a cart reaches either terminal state, a duplicate/redelivered
    order webhook matches zero rows on any later call — revenue is
    never double-counted and neither terminal state can flip into the
    other after the fact.

    Returns {'recovered': bool, 'first_time': bool, 'is_recovery': bool,
    'cart_id': int|None}.
      recovered   — a cart row exists and reached a terminal state
                    (either this call or a previous one).
      first_time  — THIS call was the one that caused the transition
                    (false on a duplicate webhook).
      is_recovery — the transition (whichever call caused it) was to
                    'recovered', not 'converted_early'. Only
                    first_time AND is_recovery together should trigger
                    a merchant notification.
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute(
            '''
            UPDATE abandoned_carts
            SET status = CASE
                    WHEN recovery_email_sent_at IS NOT NULL THEN 'recovered'
                    ELSE 'converted_early'
                END,
                recovered_at = NOW(),
                recovered_order_id = %s,
                recovered_revenue = %s
            WHERE client_id = %s AND checkout_token = %s
              AND status NOT IN ('recovered', 'converted_early')
            RETURNING id, (recovery_email_sent_at IS NOT NULL) AS is_recovery
            ''',
            (order_id, revenue, client_id, checkout_token)
        )
        row = cursor.fetchone()
        first_time = row is not None
        cart_id = row['id'] if row else None
        is_recovery = bool(row['is_recovery']) if row else False

        if not first_time:
            # Either already terminal (duplicate webhook) or no cart row
            # exists for this checkout_token at all — look up which, and
            # what it actually resolved to, purely for the caller's
            # logging/return shape. Behaviour doesn't depend on this.
            cursor.execute(
                'SELECT id, status FROM abandoned_carts WHERE client_id = %s AND checkout_token = %s',
                (client_id, checkout_token)
            )
            existing = cursor.fetchone()
            cart_id = existing['id'] if existing else None
            is_recovery = bool(existing and existing['status'] == 'recovered')

        conn.commit()
        return {
            'recovered': cart_id is not None,
            'first_time': first_time,
            'is_recovery': is_recovery,
            'cart_id': cart_id,
        }
    except Exception:
        if conn:
            try: conn.rollback()
            except Exception: pass
        return {'recovered': False, 'first_time': False, 'is_recovery': False, 'cart_id': None}
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


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


def claim_carts_for_recovery_email(delay_hours: int = 1, limit: int = 200,
                                    stale_claim_minutes: int = 15) -> list:
    """
    Atomically SELECT-and-claim carts due for a recovery email, in one
    statement, so two overlapping cron calls can never claim the same
    row (production-readiness audit, Problem 2).

    A claimed row's status flips 'pending' -> 'sending' and
    recovery_send_claimed_at is set, in the SAME UPDATE that selects it
    (via a FOR UPDATE SKIP LOCKED subquery) — there is no separate
    SELECT-then-UPDATE window for a second call to race into.

    Eligible rows are either:
      - status='pending' and never claimed before, same criteria as the
        old get_carts_due_for_recovery_email(), or
      - status='sending' but claimed more than stale_claim_minutes ago —
        treated as an abandoned claim (the process that claimed it
        crashed before finishing) and safe to retry.

    The caller MUST, for every returned row, eventually call either
    mark_recovery_email_sent(cart_id) on success or
    revert_recovery_email_claim(cart_id) on failure — a claimed row that
    gets neither will sit in 'sending' until the stale-claim window
    passes, then be retried automatically.
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute(
            '''
            UPDATE abandoned_carts
            SET status = 'sending', recovery_send_claimed_at = NOW()
            WHERE id IN (
                SELECT ac.id
                FROM abandoned_carts ac
                JOIN clients c ON c.client_id = ac.client_id
                WHERE ac.customer_email IS NOT NULL
                  AND ac.recovery_email_sent_at IS NULL
                  AND ac.abandoned_at <= NOW() - (%s * INTERVAL '1 hour')
                  AND c.cart_recovery_enabled = TRUE
                  AND (
                        ac.status = 'pending'
                        OR (ac.status = 'sending'
                            AND ac.recovery_send_claimed_at <= NOW() - (%s * INTERVAL '1 minute'))
                      )
                ORDER BY ac.abandoned_at ASC
                LIMIT %s
                FOR UPDATE OF ac SKIP LOCKED
            )
            RETURNING *
            ''',
            (delay_hours, stale_claim_minutes, limit)
        )
        rows = cursor.fetchall() or []
        conn.commit()

        if not rows:
            return []

        # Attach business_name/notification_email the way the old
        # read-only query did (email template + notifications need it),
        # via a second small lookup keyed by the claimed rows' client_ids.
        client_ids = tuple({r['client_id'] for r in rows})
        cursor.execute(
            'SELECT client_id, business_name, notification_email FROM clients WHERE client_id = ANY(%s)',
            (list(client_ids),)
        )
        client_info = {r['client_id']: r for r in cursor.fetchall()}

        result = []
        for r in rows:
            d = dict(r)
            info = client_info.get(d['client_id'], {})
            d['business_name'] = info.get('business_name')
            d['notification_email'] = info.get('notification_email')
            result.append(d)
        return result
    except Exception:
        if conn:
            try: conn.rollback()
            except Exception: pass
        return []
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


def revert_recovery_email_claim(cart_id: int) -> bool:
    """
    Send attempt failed after claiming — put the cart back to 'pending'
    (clearing the claim timestamp) so the next cron run retries it
    immediately rather than waiting out the full stale-claim window.
    """
    try:
        conn, cursor = get_db()
        cursor.execute(
            "UPDATE abandoned_carts SET status = 'pending', recovery_send_claimed_at = NULL "
            "WHERE id = %s AND status = 'sending'",
            (cart_id,)
        )
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception:
        return False


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


# ── Merchant in-app recovery notification ──────────────────────────────────

def create_recovery_notification(client_id: str, cart_id: int,
                                  order_id: str = None, revenue=None) -> bool:
    """
    One row per successfully recovered cart, for the merchant's in-app
    feed. Call ONLY when mark_cart_recovered() reported first_time=True
    — the UNIQUE(cart_id) constraint is a second, independent guard
    against a duplicate (a redelivered order webhook, or a reclaimed
    'sending' row resolving twice) ever producing two notifications for
    the same cart, via INSERT ... ON CONFLICT DO NOTHING.

    Message text is deliberately factual, per the "How Lumvi helped"
    scope for this pass — no causal claim about WHY the customer
    returned, since nothing upstream records that.
    """
    revenue_str = f"${float(revenue):.2f}" if revenue is not None else "an"
    message = (
        f"Lumvi recovered a sale — a customer returned after a recovery "
        f"email and completed {revenue_str} order."
        if revenue is not None else
        "Lumvi recovered a sale — a customer returned after a recovery email and completed their order."
    )
    try:
        conn, cursor = get_db()
        cursor.execute(
            '''
            INSERT INTO recovery_notifications (client_id, cart_id, order_id, revenue, message)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (cart_id) DO NOTHING
            ''',
            (client_id, cart_id, order_id, revenue, message)
        )
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception:
        return False


def get_recovery_notifications(client_id: str, limit: int = 20, unread_only: bool = False) -> list:
    """Most recent recovery notifications for a client's in-app feed."""
    try:
        conn, cursor = get_db()
        query = 'SELECT * FROM recovery_notifications WHERE client_id = %s'
        params = [client_id]
        if unread_only:
            query += ' AND read_at IS NULL'
        query += ' ORDER BY created_at DESC LIMIT %s'
        params.append(limit)
        cursor.execute(query, tuple(params))
        rows = cursor.fetchall() or []
        cursor.close()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def mark_recovery_notification_read(notification_id: int, client_id: str) -> bool:
    """client_id filter is the tenant-isolation check — a notification
    id alone isn't enough to prove ownership."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            'UPDATE recovery_notifications SET read_at = NOW() WHERE id = %s AND client_id = %s AND read_at IS NULL',
            (notification_id, client_id)
        )
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception:
        return False


# ── Recovery analytics ──────────────────────────────────────────────────────

def get_recovery_analytics(client_id: str, start_date=None, end_date=None) -> dict:
    """
    Real, stored-data-only recovery metrics for one client.

    Definitions (documented explicitly — no metric is silently defined):

      Recovery emails sent
        COUNT of carts for which Lumvi successfully recorded a recovery
        email as sent (recovery_email_sent_at IS NOT NULL).

      Recovered carts
        COUNT of those emailed carts that subsequently completed the
        matched checkout/order (status='recovered' — see
        mark_cart_recovered's docstring for the exact V1 definition: an
        order that completed BEFORE any recovery email went out is
        status='converted_early', a real sale, but explicitly excluded
        here — it is not a Lumvi recovery).

      Recovery rate
        recovered carts / recovery emails sent, as a percentage. Both
        counts are the SAME cohort, not two independently-windowed
        counts: see cohort note below.

      Recovered revenue
        SUM of recovered_revenue (the completed order's actual total,
        captured by mark_cart_recovered at the moment of recovery) over
        recovered emailed carts. V1 recovered revenue is the Shopify
        order total recorded at attribution time and is NOT adjusted
        for later refunds — it is not "net revenue" and does not claim
        to reflect refunds. Refund handling is a possible V1.1.

      Average recovered order
        recovered revenue / recovered carts. None when recovered carts
        is 0, so callers don't render a misleading $0.00.

    COHORT (Decision 4): both recovery_attempts and recovered_carts are
    windowed by the SAME column, recovery_email_sent_at — not by
    recovered_at. A cart emailed Aug 31 and recovered Sep 2 belongs to
    the August cohort in full: it counts in August's numerator AND
    denominator, never split across two periods. This needed no new
    schema — recovery_email_sent_at was already sufticient to represent
    "which cohort does this cart belong to", since (per Decision 1)
    status can only reach 'recovered' when recovery_email_sent_at is
    already set.

    DATE RANGE (Decision 3): half-open interval
    [start_date, end_date) — start_date is inclusive, end_date is
    exclusive, avoiding end-of-day boundary bugs. Either bound may be
    omitted (None) for an open-ended range on that side; passing both
    is expected once a frontend date picker exists ("this month" =
    start_date=month start, end_date=next month start).

    CURRENCY (Decision 2): there is no canonical currency field on the
    clients table anywhere in the schema (checked — only per-row
    currency exists, on abandoned_carts/orders, populated per-webhook
    from Shopify's own payload). Rather than invent one, or silently
    sum potentially-different currencies into one misleading number,
    recovered_revenue/avg_recovered_order_value are broken out
    per-currency. For the overwhelmingly common single-currency-store
    case this is one entry and behaves like a normal total; a
    multi-currency store (Shopify Markets) gets a clearly separated
    breakdown instead of a blended, meaningless figure. No FX
    conversion of any kind is performed.
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()

        def _window(column):
            clause, params = f"{column} IS NOT NULL", []
            if start_date is not None:
                clause += f" AND {column} >= %s"
                params.append(start_date)
            if end_date is not None:
                clause += f" AND {column} < %s"
                params.append(end_date)
            return clause, params

        sent_clause, sent_params = _window('recovery_email_sent_at')
        cursor.execute(
            f"SELECT COUNT(*) AS n FROM abandoned_carts WHERE client_id = %s AND {sent_clause}",
            tuple([client_id] + sent_params)
        )
        recovery_attempts = int((cursor.fetchone() or {}).get('n', 0))

        # Same cohort column (recovery_email_sent_at), same window, plus
        # the recovered-status filter — this is the fix for Decision 4.
        cursor.execute(
            f"SELECT COUNT(*) AS n, COALESCE(currency, 'UNKNOWN') AS currency, "
            f"COALESCE(SUM(recovered_revenue), 0) AS revenue "
            f"FROM abandoned_carts "
            f"WHERE client_id = %s AND status = 'recovered' AND {sent_clause} "
            f"GROUP BY COALESCE(currency, 'UNKNOWN')",
            tuple([client_id] + sent_params)
        )
        by_currency_rows = cursor.fetchall() or []

        recovered_carts = sum(int(r['n']) for r in by_currency_rows)
        revenue_by_currency = {
            r['currency']: round(float(r['revenue'] or 0), 2) for r in by_currency_rows
        }
        carts_by_currency = {r['currency']: int(r['n']) for r in by_currency_rows}

        recovery_rate = round(100 * recovered_carts / recovery_attempts, 1) if recovery_attempts > 0 else 0

        multi_currency = len(revenue_by_currency) > 1
        if not revenue_by_currency:
            recovered_revenue = 0.0
            avg_recovered_order_value = None
        elif not multi_currency:
            (only_currency, recovered_revenue), = revenue_by_currency.items()
            n = carts_by_currency[only_currency]
            avg_recovered_order_value = round(recovered_revenue / n, 2) if n > 0 else None
        else:
            # More than one currency present in the window — do NOT
            # collapse into one blended number (Decision 2). Callers
            # must use recovered_revenue_by_currency instead.
            recovered_revenue = None
            avg_recovered_order_value = None

        return {
            'recovery_attempts':            recovery_attempts,
            'recovered_carts':               recovered_carts,
            'recovery_rate':                 recovery_rate,
            'recovered_revenue':             recovered_revenue,
            'avg_recovered_order_value':     avg_recovered_order_value,
            'multi_currency':                multi_currency,
            'recovered_revenue_by_currency': revenue_by_currency,
            'recovered_carts_by_currency':   carts_by_currency,
        }
    except Exception:
        return {
            'recovery_attempts': 0, 'recovered_carts': 0, 'recovery_rate': 0,
            'recovered_revenue': 0, 'avg_recovered_order_value': None,
            'multi_currency': False, 'recovered_revenue_by_currency': {},
            'recovered_carts_by_currency': {},
        }
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass
