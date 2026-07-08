"""
tools.py — Lumvi AI Agent Tool Definitions (System 1)
======================================================
Each tool is a plain Python function that:
  1. Accepts sanitised string arguments
  2. Opens a DB connection with models.get_db(), closes it in a finally block
  3. Returns a dict: {success: bool, ...data fields...}
  4. NEVER raises — always catches and returns {success: False, error: str}

The generate_response_agent() method in ai_helper.py calls these functions
inside the ReAct loop: Reason → Act (call tool) → Observe (read result) → repeat.

Tool registry at the bottom of this file is the single source of truth for
which tools are available and what their Gemini function-call schemas look like.
"""

import re
import uuid
import logging
from datetime import datetime

import models  # same import pattern as app.py

logger = logging.getLogger(__name__)


# =====================================================================
# INPUT SANITISATION
# Mirrors sanitize_input() in app.py — kept local so tools.py has no
# circular import on app.py.
# =====================================================================

def _sanitize(text, max_length=200):
    """Strip HTML tags, collapse whitespace, truncate."""
    if not text or not isinstance(text, str):
        return ""
    text = re.sub(r'<[^>]+>', '', text)
    text = text[:max_length]
    text = ' '.join(text.split())
    return text.strip()


def _is_valid_email(text):
    # FIX: was r'...[A-Z|a-z]{2,}\b' — a character class containing a
    # literal pipe character. Inside [...] the '|' isn't an OR operator,
    # so this was three alternatives (A-Z, the '|' character itself, a-z)
    # rather than the intended case-insensitive letter match. Harmless in
    # practice since this is a presence check, not extraction, but worth
    # fixing since it's a tell for the same copy-paste regex elsewhere.
    return bool(re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', text))


def get_order_management_url(client_id: str) -> str | None:
    """
    Thin wrapper around commerce_adapters.get_order_management_url — see
    that function for the actual client_integrations lookup. Kept as a
    separate function here (rather than calling commerce_adapters
    directly inline) so the import-failure fallback lives in one place.
    Public (no leading underscore) since intent.py's
    order_cancellation_redirect_message() calls this directly, matching
    how get_external_booking_info() is called for book_appointment.
    """
    try:
        from commerce_adapters import get_order_management_url as _get_url
        return _get_url(client_id)
    except Exception as e:
        logger.error(f'[Tool] could not check order_management_url: {e}')
        return None


# =====================================================================
# TOOL 1 — lookup_order
# Looks up an order by order_id (and optionally email) for a given client.
# Expects an `orders` table. If the table doesn't exist for a client,
# returns a graceful not-found rather than crashing.
# =====================================================================

def lookup_order(client_id: str, order_id: str, customer_email: str = "") -> dict:
    """
    Look up an order by order_id for a client.

    Args:
        client_id:       Lumvi client identifier
        order_id:        The order reference (e.g. "ORD-12345")
        customer_email:  Optional — narrows to orders belonging to this email

    Returns:
        {success, order: {id, status, items, total, created_at, ...}} or
        {success: False, error: str}
    """
    client_id     = _sanitize(client_id, 50)
    order_id      = _sanitize(order_id, 100)
    customer_email = _sanitize(customer_email, 200)

    if not order_id:
        return {'success': False, 'error': 'order_id is required'}

    # FIX: this used to query Lumvi's own `orders` table exclusively —
    # which is only ever as fresh as the last inbound webhook (see
    # webhooks.py) and can't see anything that happened before the
    # integration was configured. For a white-label SaaS, "accurate" has
    # to mean the client's actual store, not our copy of it. Try their
    # live Shopify/WooCommerce connection first, via commerce_adapters.py.
    try:
        from commerce_adapters import lookup_order_live
        live = lookup_order_live(client_id, order_id, customer_email)
    except Exception as e:
        logger.error(f'[Tool:lookup_order] commerce_adapters import/call failed: {e}')
        live = None

    if live is not None and live.resolved:
        # A resolved live lookup is authoritative either way — a genuine
        # "not found" from the client's real store is more accurate than
        # anything Lumvi's own copy could say, so this does NOT fall
        # through to the internal table below.
        if live.order is None:
            return {
                'success': False,
                'error': f'Order {order_id} not found.',
                'order_id': order_id,
            }
        o = live.order
        return {
            'success': True,
            'order': {
                'id':                 o.id,
                'status':             o.status,
                'total_amount':       o.total_amount,
                'currency':           o.currency,
                'updated_at':         o.updated_at,
                'financial_status':   o.financial_status,
                'fulfillment_status': o.fulfillment_status,
            },
        }

    if live is not None and not live.resolved and live.error != 'no_adapter_connected':
        # A live adapter IS configured but the call itself failed
        # (timeout/auth/rate-limit) — don't silently fall back to a
        # possibly-stale internal copy and risk presenting wrong data as
        # if it were current; that would quietly reintroduce the exact
        # inaccuracy this whole change is meant to fix. Say so honestly.
        logger.warning(f'[Tool:lookup_order] live lookup failed client={client_id} error={live.error}')
        return {
            'success': False,
            'error': "I'm having trouble checking that right now — please try again in a moment.",
        }

    # No live order-read connection for this client (live is None, or
    # live.error == 'no_adapter_connected') — fall back to Lumvi's own
    # webhook-synced copy. Still useful, just not guaranteed current for
    # anything that happened before the integration was configured.
    conn = cursor = None
    try:
        conn, cursor = models.get_db()

        # The orders table is client-scoped — client_id is always included in WHERE
        query = '''
            SELECT id, client_id, order_id, customer_email, customer_name,
                   status, items_json, total_amount, currency,
                   created_at, updated_at, notes
            FROM orders
            WHERE client_id = %s AND order_id = %s
        '''
        params = [client_id, order_id]

        if customer_email and _is_valid_email(customer_email):
            query += ' AND customer_email = %s'
            params.append(customer_email.lower())

        cursor.execute(query, params)
        row = cursor.fetchone()

        if not row:
            return {
                'success': False,
                'error': f'Order {order_id} not found.',
                'order_id': order_id
            }

        import json as _json
        items = []
        try:
            items = _json.loads(row.get('items_json') or '[]')
        except Exception:
            pass

        return {
            'success': True,
            'order': {
                'id':              row.get('order_id'),
                'status':          row.get('status', 'unknown'),
                'customer_name':   row.get('customer_name', ''),
                'customer_email':  row.get('customer_email', ''),
                'items':           items,
                'total_amount':    row.get('total_amount'),
                'currency':        row.get('currency', 'USD'),
                'created_at':      str(row.get('created_at', '')),
                'updated_at':      str(row.get('updated_at', '')),
                'notes':           row.get('notes', '')
            }
        }

    except Exception as e:
        logger.error(f'[Tool:lookup_order] client={client_id} order={order_id} error: {e}')
        return {'success': False, 'error': 'Could not retrieve order details at this time.'}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# =====================================================================
# TOOL 2 — cancel_order
# Marks an order as cancelled if it is in a cancellable state.
# =====================================================================

# Orders in these statuses can be cancelled.
_CANCELLABLE_STATUSES = {'pending', 'confirmed', 'processing'}

def cancel_order(client_id: str, order_id: str, customer_email: str, reason: str = "") -> dict:
    """
    Cancel an order. Only succeeds if the order is in a cancellable state
    (pending / confirmed / processing) and belongs to the given customer email.

    Args:
        client_id:       Lumvi client identifier
        order_id:        The order reference
        customer_email:  Must match the order's customer email (ownership check)
        reason:          Optional cancellation reason

    Returns:
        {success, message} or {success: False, error: str}
    """
    client_id      = _sanitize(client_id, 50)
    order_id       = _sanitize(order_id, 100)
    customer_email = _sanitize(customer_email, 200).lower()
    reason         = _sanitize(reason, 500)

    if not order_id:
        return {'success': False, 'error': 'order_id is required'}
    if not customer_email or not _is_valid_email(customer_email):
        return {'success': False, 'error': 'A valid customer email is required to cancel an order.'}

    # FIX / DESIGN CHANGE: cancel_order used to either write straight to
    # Lumvi's internal orders table, or (as of the last pass) submit a
    # cancellation *request* with staff notification. Cancellation and
    # refunds are now redirected instead — the same pattern already used
    # for book_appointment when a client's on Acuity/Calendly/Square:
    # point the customer at the business's real self-service page rather
    # than have Lumvi attempt the mutation itself. This sidesteps real
    # money/inventory risk from an autonomous chatbot action entirely, and
    # keeps order tracking/lookup (read-only, via commerce_adapters.py
    # above) as the one thing the chat widget actually executes directly.
    redirect_url = get_order_management_url(client_id)
    if redirect_url:
        return {
            'success': True,
            'redirect_url': redirect_url,
            'message': (
                f"You can cancel order {order_id} and see refund options directly "
                f"here: {redirect_url}"
            ),
        }

    conn = cursor = None
    try:
        conn, cursor = models.get_db()

        # First: verify the order exists and belongs to this customer
        cursor.execute(
            '''
            SELECT id, status FROM orders
            WHERE client_id = %s AND order_id = %s AND customer_email = %s
            ''',
            (client_id, order_id, customer_email)
        )
        row = cursor.fetchone()

        if not row:
            return {
                'success': False,
                'error': f'Order {order_id} not found or does not belong to {customer_email}.'
            }

        current_status = (row.get('status') or '').lower()

        if current_status not in _CANCELLABLE_STATUSES:
            return {
                'success': False,
                'error': (
                    f'Order {order_id} cannot be cancelled because it is already '
                    f'"{current_status}". Please contact our support team for assistance.'
                ),
                'current_status': current_status
            }

        # FIX: this used to set status straight to 'cancelled' and tell the
        # customer it was done. But cancel_order only writes to Lumvi's own
        # synced copy of the order — there is no outbound call to Shopify/
        # WooCommerce's API here, so the REAL order on the business's actual
        # platform is untouched. Telling a customer "cancelled" when the
        # business's system of record still shows it active/shipping is a
        # false confirmation that can cause real harm (e.g. it still ships).
        # Recording it as a *request* is honest about what actually
        # happened, and it won't fight the next inbound webhook sync either
        # — if Shopify pushes a status update before staff act on this, that
        # update correctly overwrites it (see _upsert_order in webhooks.py).
        now = datetime.utcnow()
        cursor.execute(
            '''
            UPDATE orders
            SET status = %s, notes = %s, updated_at = %s
            WHERE client_id = %s AND order_id = %s
            ''',
            (
                'cancellation_requested',
                f'Cancellation requested via chatbot at {now.isoformat()}. Reason: {reason}' if reason
                    else f'Cancellation requested via chatbot at {now.isoformat()}.',
                now,
                client_id,
                order_id
            )
        )
        conn.commit()

        logger.info(f'[Tool:cancel_order] client={client_id} order={order_id} cancellation requested by {customer_email}')
        return {
            'success': True,
            'message': (
                f"I've submitted a cancellation request for order {order_id}. "
                f"Our team will confirm this with you by email shortly — it "
                f"isn't final until they do."
            ),
            'order_id':         order_id,
            'previous_status':  current_status,
            'new_status':       'cancellation_requested'
        }

    except Exception as e:
        logger.error(f'[Tool:cancel_order] client={client_id} order={order_id} error: {e}')
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return {'success': False, 'error': 'Could not cancel the order at this time. Please try again.'}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# =====================================================================
# EXTERNAL BOOKING REDIRECT
# Acuity/Calendly/Square only push CONFIRMED/CANCELLED booking events via
# webhook — none of them push "here's my open availability". That means
# Lumvi's own appointment_slots table can never reflect real availability
# for a client on one of these platforms; the only slots it ever contains
# for them are synthesized from confirmed bookings, and those are born
# already at capacity. For those clients, the accurate thing to do is
# point the customer at the business's real booking page rather than
# pretend to check or reserve availability Lumvi doesn't actually have.
# =====================================================================

_CALENDAR_PLATFORMS = ('calendly', 'acuity', 'square')


def get_external_booking_info(client_id: str) -> dict | None:
    """
    Return {'booking_url', 'platform'} for the client's active Acuity/
    Calendly/Square integration, if one is configured with a booking_url
    in its platform_config. Returns None if not applicable — e.g. no
    integration on any of these platforms, or one exists but the agency
    hasn't entered a public booking page URL for it (platform_config is a
    free-form JSON blob set from the Lumvi dashboard; booking_url isn't
    guaranteed to be present).
    """
    try:
        from webhooks import get_integration
    except Exception:
        return None
    for platform in _CALENDAR_PLATFORMS:
        integration = get_integration(client_id, platform)
        if integration:
            url = (integration.get('platform_config') or {}).get('booking_url')
            if url:
                return {'booking_url': url, 'platform': platform}
    return None


# =====================================================================
# TOOL 3 — check_availability
# Checks open appointment/booking slots for a client.
# =====================================================================

def check_availability(client_id: str, date: str = "", service_type: str = "") -> dict:
    """
    Check available appointment slots for a client.

    If the client has a real booking page configured for Acuity/Calendly/
    Square, that's returned directly instead of querying Lumvi's internal
    slots — see get_external_booking_info() above for why. Falls back to
    Lumvi's own appointment_slots table for clients with no such
    integration (e.g. businesses managing slots directly through the
    Lumvi dashboard with no external scheduling tool).

    Args:
        client_id:    Lumvi client identifier
        date:         ISO date string YYYY-MM-DD. Defaults to today if empty.
        service_type: Optional filter (e.g. "consultation", "viewing", "follow_up")

    Returns:
        {success, slots: [{slot_id, datetime, service_type, duration_minutes}], date}
        or {success, booking_url, platform, message} when redirecting.
    """
    client_id    = _sanitize(client_id, 50)
    date         = _sanitize(date, 20)
    service_type = _sanitize(service_type, 100)

    redirect = get_external_booking_info(client_id)
    if redirect:
        return {
            'success':     True,
            'booking_url': redirect['booking_url'],
            'platform':    redirect['platform'],
            'message': (
                f"You can see live availability and book directly here: "
                f"{redirect['booking_url']}"
            ),
        }

    # Validate / default date
    if date:
        try:
            target_date = datetime.strptime(date, '%Y-%m-%d').date()
        except ValueError:
            return {'success': False, 'error': f'Invalid date format: "{date}". Use YYYY-MM-DD.'}
    else:
        target_date = datetime.utcnow().date()

    conn = cursor = None
    try:
        conn, cursor = models.get_db()

        query = '''
            SELECT slot_id, slot_datetime, service_type, duration_minutes, capacity, booked_count
            FROM appointment_slots
            WHERE client_id = %s
              AND DATE(slot_datetime) = %s
              AND booked_count < capacity
              AND slot_datetime > NOW()
            ORDER BY slot_datetime ASC
            LIMIT 10
        '''
        params = [client_id, str(target_date)]

        if service_type:
            query = '''
                SELECT slot_id, slot_datetime, service_type, duration_minutes, capacity, booked_count
                FROM appointment_slots
                WHERE client_id = %s
                  AND DATE(slot_datetime) = %s
                  AND service_type = %s
                  AND booked_count < capacity
                  AND slot_datetime > NOW()
                ORDER BY slot_datetime ASC
                LIMIT 10
            '''
            params = [client_id, str(target_date), service_type]

        cursor.execute(query, params)
        rows = cursor.fetchall()

        slots = []
        for row in rows:
            slots.append({
                'slot_id':          row.get('slot_id'),
                'datetime':         str(row.get('slot_datetime', '')),
                'service_type':     row.get('service_type', 'general'),
                'duration_minutes': row.get('duration_minutes', 30),
                'spots_left':       (row.get('capacity', 1) - row.get('booked_count', 0))
            })

        if not slots:
            return {
                'success': True,
                'slots':   [],
                'date':    str(target_date),
                'message': f'No available slots found for {target_date}. Try a different date.'
            }

        return {
            'success': True,
            'slots':   slots,
            'date':    str(target_date),
            'count':   len(slots)
        }

    except Exception as e:
        logger.error(f'[Tool:check_availability] client={client_id} date={date} error: {e}')
        return {'success': False, 'error': 'Could not check availability at this time.'}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# =====================================================================
# TOOL 4 — book_appointment
# Books a specific slot. Atomically increments booked_count.
# =====================================================================

def book_appointment(
    client_id:        str,
    slot_id:          str,
    customer_name:    str,
    customer_email:   str,
    customer_phone:   str = "",
    notes:            str = ""
) -> dict:
    """
    Book an appointment slot for a customer.

    Args:
        client_id:       Lumvi client identifier
        slot_id:         The slot_id returned by check_availability
        customer_name:   Customer's full name
        customer_email:  Customer's email (used as booking identifier)
        customer_phone:  Optional phone number
        notes:           Optional notes from the customer

    Returns:
        {success, booking_id, confirmation_message, slot_datetime, service_type}
    """
    client_id      = _sanitize(client_id, 50)
    slot_id        = _sanitize(slot_id, 100)
    customer_name  = _sanitize(customer_name, 200)
    customer_email = _sanitize(customer_email, 200).lower()
    customer_phone = _sanitize(customer_phone, 50)
    notes          = _sanitize(notes, 1000)

    if not slot_id:
        return {'success': False, 'error': 'slot_id is required'}
    if not customer_name:
        return {'success': False, 'error': 'customer_name is required'}
    if not customer_email or not _is_valid_email(customer_email):
        return {'success': False, 'error': 'A valid customer email is required to book an appointment.'}

    # Defense in depth: check_availability redirects clients on Acuity/
    # Calendly/Square to their real booking page instead of ever handing
    # out a slot_id from this system, so this shouldn't normally be
    # reachable for them — but refuse outright rather than create a
    # booking that exists only in Lumvi and nowhere on their real calendar.
    redirect = get_external_booking_info(client_id)
    if redirect:
        return {
            'success': False,
            'error': (
                f"Please book directly here to make sure it's reflected on "
                f"our real calendar: {redirect['booking_url']}"
            ),
        }

    conn = cursor = None
    try:
        conn, cursor = models.get_db()

        # Lock the slot row to prevent double-booking (SELECT FOR UPDATE)
        cursor.execute(
            '''
            SELECT slot_id, slot_datetime, service_type, duration_minutes, capacity, booked_count
            FROM appointment_slots
            WHERE client_id = %s AND slot_id = %s
            FOR UPDATE
            ''',
            (client_id, slot_id)
        )
        slot = cursor.fetchone()

        if not slot:
            return {'success': False, 'error': f'Slot {slot_id} not found.'}

        capacity     = slot.get('capacity', 1)
        booked_count = slot.get('booked_count', 0)

        if booked_count >= capacity:
            return {
                'success': False,
                'error':   'Sorry, this slot is now fully booked. Please choose another time.'
            }

        # Create the booking record
        booking_id = f'bk_{uuid.uuid4().hex[:10]}'
        now        = datetime.utcnow()

        # FIX: this used to insert with status='confirmed' and tell the
        # customer it was booked. But book_appointment only writes to
        # Lumvi's own shadow copy — there's no outbound call to Acuity's/
        # Calendly's/Square's API here, so nothing actually appears on the
        # business's real calendar. A customer told "you're booked" when
        # the business has no idea the appointment exists is a false
        # confirmation risk (no-shows, double-booking, missed appointments).
        # 'pending_confirmation' is honest about what actually happened.
        # booked_count is still incremented — that's just a soft hold so
        # this slot isn't offered to two chat customers at once while staff
        # process the request, independent of the status wording.
        cursor.execute(
            '''
            INSERT INTO appointments
                (booking_id, client_id, slot_id, customer_name, customer_email,
                 customer_phone, notes, status, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''',
            (booking_id, client_id, slot_id, customer_name, customer_email,
             customer_phone, notes, 'pending_confirmation', now)
        )

        # Atomically increment booked_count
        cursor.execute(
            'UPDATE appointment_slots SET booked_count = booked_count + 1 WHERE slot_id = %s',
            (slot_id,)
        )

        conn.commit()

        slot_dt      = str(slot.get('slot_datetime', ''))
        service_type = slot.get('service_type', 'appointment')

        logger.info(
            f'[Tool:book_appointment] client={client_id} booking={booking_id} '
            f'slot={slot_id} customer={customer_email} status=pending_confirmation'
        )

        return {
            'success':              True,
            'booking_id':           booking_id,
            'slot_datetime':        slot_dt,
            'service_type':         service_type,
            'duration_minutes':     slot.get('duration_minutes', 30),
            'confirmation_message': (
                f"I've requested a {service_type} for {slot_dt} "
                f"(reference: {booking_id}). Our team will confirm this "
                f"with you at {customer_email} shortly — it isn't final "
                f"until they do."
            )
        }

    except Exception as e:
        logger.error(f'[Tool:book_appointment] client={client_id} slot={slot_id} error: {e}')
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return {'success': False, 'error': 'Could not complete the booking at this time. Please try again.'}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# =====================================================================
# TOOL 5 — escalate_to_human
# Creates a human inbox ticket and flags the conversation for review.
# Mirrors the webhook pattern from app.py — fires non-blocking if needed.
# =====================================================================

def escalate_to_human(
    client_id:       str,
    session_id:      str,
    reason:          str,
    customer_email:  str = "",
    customer_name:   str = "",
    summary:         str = "",
    urgency:         str = "normal"
) -> dict:
    """
    Escalate a conversation to a human agent. Creates an inbox ticket.

    Args:
        client_id:       Lumvi client identifier
        session_id:      Current conversation session ID
        reason:          Why the escalation is happening
        customer_email:  Customer's email if known
        customer_name:   Customer's name if known
        summary:         Short summary of the conversation so far
        urgency:         "low" | "normal" | "high" | "urgent"

    Returns:
        {success, ticket_id, message}
    """
    client_id      = _sanitize(client_id, 50)
    session_id     = _sanitize(session_id, 100)
    reason         = _sanitize(reason, 500)
    customer_email = _sanitize(customer_email, 200).lower()
    customer_name  = _sanitize(customer_name, 200)
    summary        = _sanitize(summary, 2000)
    urgency        = _sanitize(urgency, 20).lower()

    valid_urgencies = {'low', 'normal', 'high', 'urgent'}
    if urgency not in valid_urgencies:
        urgency = 'normal'

    if not reason:
        return {'success': False, 'error': 'A reason for escalation is required.'}

    conn = cursor = None
    try:
        conn, cursor = models.get_db()

        ticket_id = f'tkt_{uuid.uuid4().hex[:10]}'
        now       = datetime.utcnow()

        cursor.execute(
            '''
            INSERT INTO human_inbox
                (ticket_id, client_id, session_id, reason, customer_email,
                 customer_name, summary, urgency, status, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''',
            (ticket_id, client_id, session_id, reason, customer_email,
             customer_name, summary, urgency, 'open', now, now)
        )
        conn.commit()

        logger.info(
            f'[Tool:escalate_to_human] client={client_id} ticket={ticket_id} '
            f'urgency={urgency} session={session_id}'
        )

        return {
            'success':   True,
            'ticket_id': ticket_id,
            'message': (
                "I've flagged this conversation for our support team. "
                "A team member will follow up with you"
                + (f" at {customer_email}" if customer_email else "")
                + " as soon as possible. Is there anything else I can help with in the meantime?"
            )
        }

    except Exception as e:
        logger.error(f'[Tool:escalate_to_human] client={client_id} session={session_id} error: {e}')
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return {
            'success': False,
            'error':   'Could not create support ticket at this time. Please contact us directly.'
        }
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# =====================================================================
# TOOL 6 — search_knowledge_base
# Semantic/keyword search over the client's knowledge base (FAQs).
# The heavy embedding search is done in ai_helper.py — this tool performs
# a fast DB full-text / ILIKE fallback that works without Gemini.
# In the agent loop, ai_helper will prefer its own vector search; this
# tool gives the agent an explicit "search KB" action it can reason about.
# =====================================================================

def search_knowledge_base(client_id: str, query: str, limit: int = 5) -> dict:
    """
    Search the client's knowledge base for entries matching the query.

    Args:
        client_id:  Lumvi client identifier
        query:      The search string
        limit:      Maximum number of results to return (capped at 10)

    Returns:
        {success, results: [{id, question, answer, category, score}], count}
    """
    client_id = _sanitize(client_id, 50)
    query     = _sanitize(query, 300)
    limit     = min(int(limit) if str(limit).isdigit() else 5, 10)

    if not query:
        return {'success': False, 'error': 'A search query is required.'}

    conn = cursor = None
    try:
        conn, cursor = models.get_db()

        # FIX: was a single ILIKE '%<entire query>%', which required the
        # WHOLE query string to appear verbatim in the question/answer —
        # almost never true for natural-language phrasing. "do you have a
        # refund policy for returns" will never literally appear in
        # "What is your refund policy?", so this tool rarely matched
        # anything real. Tokenize into significant words and match on ANY
        # of them via LIKE ANY(), scoring by whether question or answer hit.
        # Still intentionally simple — ai_helper's embedding search is the
        # primary relevance engine; this tool is the agent's explicit action.
        _stopwords = {
            'the', 'a', 'an', 'is', 'are', 'was', 'were', 'do', 'does', 'did',
            'have', 'has', 'had', 'you', 'your', 'my', 'me', 'to', 'of', 'in',
            'on', 'at', 'for', 'with', 'and', 'or', 'if', 'it', 'this', 'that',
            'can', 'will', 'would', 'could', 'should', 'what', 'when', 'where',
            'how', 'why', 'about',
        }
        words = [w for w in re.findall(r"[a-zA-Z']{3,}", query.lower()) if w not in _stopwords]
        if not words:
            words = [query.lower()]
        words    = words[:8]  # cap — a very long query shouldn't build an unbounded clause
        patterns = [f'%{w}%' for w in words]

        cursor.execute(
            '''
            SELECT id, question, answer, category,
                   CASE
                       WHEN LOWER(question) LIKE ANY(%s) THEN 2
                       WHEN LOWER(answer)   LIKE ANY(%s) THEN 1
                       ELSE 0
                   END AS relevance_score
            FROM knowledge_base
            WHERE client_id = %s
              AND is_active = TRUE
              AND (
                  LOWER(question) LIKE ANY(%s)
                  OR LOWER(answer)   LIKE ANY(%s)
              )
            ORDER BY relevance_score DESC, question ASC
            LIMIT %s
            ''',
            (patterns, patterns, client_id, patterns, patterns, limit)
        )
        rows = cursor.fetchall()

        results = []
        for row in rows:
            results.append({
                'id':       row.get('id'),
                'question': row.get('question', ''),
                'answer':   row.get('answer', ''),
                'category': row.get('category', 'general'),
                'score':    row.get('relevance_score', 0)
            })

        logger.info(f'[Tool:search_knowledge_base] client={client_id} query="{query}" found={len(results)}')

        return {
            'success': True,
            'results': results,
            'count':   len(results),
            'query':   query
        }

    except Exception as e:
        logger.error(f'[Tool:search_knowledge_base] client={client_id} query="{query}" error: {e}')
        return {'success': False, 'error': 'Knowledge base search failed.'}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# =====================================================================
# TOOL REGISTRY
# This is the single source of truth consumed by generate_response_agent()
# in ai_helper.py to build the Gemini function-calling schema.
#
# Each entry:
#   name        — matches the Python function name above
#   function    — the actual callable
#   description — what the agent reads to decide when to use this tool
#   parameters  — Gemini-compatible JSON Schema for the function's args
# =====================================================================

TOOL_REGISTRY = [
    {
        'name':        'lookup_order',
        'function':    lookup_order,
        'description': (
            'Look up the status and details of a customer order by order ID. '
            'Use this when a customer asks about their order status, shipping, '
            'or delivery. Always ask for order_id first; customer_email is optional '
            'but improves security.'
        ),
        'parameters': {
            'type': 'object',
            'properties': {
                'order_id': {
                    'type':        'string',
                    'description': 'The order reference number (e.g. ORD-12345)'
                },
                'customer_email': {
                    'type':        'string',
                    'description': 'Customer email address (optional, for verification)'
                }
            },
            'required': ['order_id']
        }
    },
    {
        'name':        'cancel_order',
        'function':    cancel_order,
        'description': (
            'Cancel a customer order. Only works for orders in pending, confirmed, '
            'or processing status. Requires both order_id and customer_email for '
            'ownership verification. Always confirm the cancellation intent with '
            'the customer before calling this tool.'
        ),
        'parameters': {
            'type': 'object',
            'properties': {
                'order_id': {
                    'type':        'string',
                    'description': 'The order reference number to cancel'
                },
                'customer_email': {
                    'type':        'string',
                    'description': 'Customer email address (required for ownership verification)'
                },
                'reason': {
                    'type':        'string',
                    'description': 'Optional reason for cancellation provided by the customer'
                }
            },
            'required': ['order_id', 'customer_email']
        }
    },
    {
        'name':        'check_availability',
        'function':    check_availability,
        'description': (
            'Check available appointment or booking slots for a specific date. '
            'Use this when a customer wants to book an appointment, meeting, '
            'property viewing, or consultation. If no date is provided, defaults '
            'to today.'
        ),
        'parameters': {
            'type': 'object',
            'properties': {
                'date': {
                    'type':        'string',
                    'description': 'Date to check availability for, in YYYY-MM-DD format'
                },
                'service_type': {
                    'type':        'string',
                    'description': 'Optional type of appointment (e.g. consultation, viewing, follow_up)'
                }
            },
            'required': []
        }
    },
    {
        'name':        'book_appointment',
        'function':    book_appointment,
        'description': (
            'Book a specific appointment slot for a customer. '
            'Always call check_availability first to get valid slot_ids. '
            'Requires customer name and email. Confirm the slot details '
            'with the customer before calling this tool.'
        ),
        'parameters': {
            'type': 'object',
            'properties': {
                'slot_id': {
                    'type':        'string',
                    'description': 'The slot_id from check_availability results'
                },
                'customer_name': {
                    'type':        'string',
                    'description': 'Customer\'s full name'
                },
                'customer_email': {
                    'type':        'string',
                    'description': 'Customer\'s email address'
                },
                'customer_phone': {
                    'type':        'string',
                    'description': 'Customer\'s phone number (optional)'
                },
                'notes': {
                    'type':        'string',
                    'description': 'Any notes or special requests from the customer (optional)'
                }
            },
            'required': ['slot_id', 'customer_name', 'customer_email']
        }
    },
    {
        'name':        'escalate_to_human',
        'function':    escalate_to_human,
        'description': (
            'Escalate the conversation to a human support agent by creating an inbox ticket. '
            'Use this when: the customer is frustrated or the issue cannot be resolved, '
            'the query is outside your knowledge, the customer explicitly asks for a human, '
            'or the situation involves a complaint, refund dispute, or sensitive matter. '
            'Provide a clear reason and summary so the human agent has context.'
        ),
        'parameters': {
            'type': 'object',
            'properties': {
                'session_id': {
                    'type':        'string',
                    'description': 'Current conversation session ID'
                },
                'reason': {
                    'type':        'string',
                    'description': 'Why this conversation needs human attention'
                },
                'customer_email': {
                    'type':        'string',
                    'description': 'Customer\'s email address if known'
                },
                'customer_name': {
                    'type':        'string',
                    'description': 'Customer\'s name if known'
                },
                'summary': {
                    'type':        'string',
                    'description': 'A brief summary of the conversation and issue for the agent'
                },
                'urgency': {
                    'type':        'string',
                    'description': 'Urgency level: low, normal, high, or urgent',
                    'enum':        ['low', 'normal', 'high', 'urgent']
                }
            },
            'required': ['session_id', 'reason']
        }
    },
    {
        'name':        'search_knowledge_base',
        'function':    search_knowledge_base,
        'description': (
            'Search the client\'s knowledge base (FAQs and articles) for information '
            'relevant to the customer\'s query. Use this when the initial context '
            'doesn\'t contain a clear answer and you need to look up specific information. '
            'Returns the most relevant Q&A entries.'
        ),
        'parameters': {
            'type': 'object',
            'properties': {
                'query': {
                    'type':        'string',
                    'description': 'The search query — use keywords from the customer\'s question'
                },
                'limit': {
                    'type':        'integer',
                    'description': 'Max number of results to return (default 5, max 10)'
                }
            },
            'required': ['query']
        }
    }
]


# =====================================================================
# CONVENIENCE HELPERS used by generate_response_agent() in ai_helper.py
# =====================================================================

def get_tool_schemas_for_gemini() -> list:
    """
    Return the list of Gemini-compatible function declarations.
    Pass the result directly to the `tools` parameter of the Gemini API call.

    Example:
        import google.genai as genai
        tools = get_tool_schemas_for_gemini()
        model.generate_content(prompt, tools=tools)
    """
    return [
        {
            'function_declarations': [
                {
                    'name':        t['name'],
                    'description': t['description'],
                    'parameters':  t['parameters']
                }
                for t in TOOL_REGISTRY
            ]
        }
    ]


def dispatch_tool_call(client_id: str, tool_name: str, tool_args: dict) -> dict:
    """
    Execute a tool by name, injecting client_id automatically.
    Called by the ReAct loop in generate_response_agent().

    Args:
        client_id:  Always injected — tools must never trust user-supplied client_id
        tool_name:  The function name from TOOL_REGISTRY
        tool_args:  Dict of arguments as returned by Gemini function calling

    Returns:
        The tool's result dict, always with at least {success: bool}
    """
    tool_map = {t['name']: t['function'] for t in TOOL_REGISTRY}

    if tool_name not in tool_map:
        logger.warning(f'[Tool:dispatch] Unknown tool requested: "{tool_name}"')
        return {'success': False, 'error': f'Tool "{tool_name}" is not available.'}

    # Always override client_id from the verified server-side value
    safe_args = dict(tool_args)
    safe_args['client_id'] = _sanitize(client_id, 50)

    logger.info(f'[Tool:dispatch] tool={tool_name} client={client_id} args={list(safe_args.keys())}')

    try:
        result = tool_map[tool_name](**safe_args)
        return result if isinstance(result, dict) else {'success': False, 'error': 'Unexpected tool response format.'}
    except TypeError as e:
        # Wrong arguments — log clearly for debugging
        logger.error(f'[Tool:dispatch] {tool_name} argument error: {e}')
        return {'success': False, 'error': f'Tool call failed due to invalid arguments: {e}'}
    except Exception as e:
        logger.error(f'[Tool:dispatch] {tool_name} unexpected error: {e}')
        return {'success': False, 'error': 'Tool execution failed unexpectedly.'}


# =====================================================================
# DB MIGRATION — called from app.py startup block alongside other migrate_* calls
# Creates the four tables that tools.py depends on.
# =====================================================================

def migrate_agent_tables():
    """
    Create tables required by System 1 agent tools if they don't exist.
    Safe to call multiple times (IF NOT EXISTS).

    Called from app.py startup:
        from tools import migrate_agent_tables
        migrate_agent_tables()
    """
    conn = cursor = None
    try:
        conn, cursor = models.get_db()

        # orders — used by lookup_order and cancel_order
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS orders (
                id               SERIAL PRIMARY KEY,
                client_id        TEXT NOT NULL,
                order_id         TEXT NOT NULL,
                customer_email   TEXT,
                customer_name    TEXT,
                status           TEXT NOT NULL DEFAULT 'pending',
                items_json       TEXT,
                total_amount     NUMERIC(12, 2),
                currency         TEXT DEFAULT 'USD',
                notes            TEXT,
                created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (client_id, order_id)
            )
        ''')

        # appointment_slots — used by check_availability and book_appointment
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS appointment_slots (
                id               SERIAL PRIMARY KEY,
                slot_id          TEXT NOT NULL UNIQUE,
                client_id        TEXT NOT NULL,
                slot_datetime    TIMESTAMP NOT NULL,
                service_type     TEXT DEFAULT 'general',
                duration_minutes INTEGER DEFAULT 30,
                capacity         INTEGER DEFAULT 1,
                booked_count     INTEGER DEFAULT 0,
                created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # appointments — used by book_appointment
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS appointments (
                id               SERIAL PRIMARY KEY,
                booking_id       TEXT NOT NULL UNIQUE,
                client_id        TEXT NOT NULL,
                slot_id          TEXT NOT NULL,
                customer_name    TEXT,
                customer_email   TEXT,
                customer_phone   TEXT,
                notes            TEXT,
                status           TEXT DEFAULT 'confirmed',
                created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # human_inbox — used by escalate_to_human
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS human_inbox (
                id               SERIAL PRIMARY KEY,
                ticket_id        TEXT NOT NULL UNIQUE,
                client_id        TEXT NOT NULL,
                session_id       TEXT,
                reason           TEXT,
                customer_email   TEXT,
                customer_name    TEXT,
                summary          TEXT,
                urgency          TEXT DEFAULT 'normal',
                status           TEXT DEFAULT 'open',
                assigned_to      TEXT,
                resolution_notes TEXT,
                created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Add session_id to conversations if it doesn't exist yet.
        # Needed by all three systems to group messages into a session.
        try:
            cursor.execute('''
                ALTER TABLE conversations
                ADD COLUMN IF NOT EXISTS session_id TEXT
            ''')
        except Exception:
            pass  # Column already exists — safe to ignore

        conn.commit()
        print('✅ Agent tables migrated (orders, appointment_slots, appointments, human_inbox, conversations.session_id)')

    except Exception as e:
        print(f'⚠️  migrate_agent_tables error: {e}')
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
