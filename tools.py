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
    return bool(re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text))


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

        # Update to cancelled
        now = datetime.utcnow()
        cursor.execute(
            '''
            UPDATE orders
            SET status = %s, notes = %s, updated_at = %s
            WHERE client_id = %s AND order_id = %s
            ''',
            (
                'cancelled',
                f'Cancelled via chatbot at {now.isoformat()}. Reason: {reason}' if reason
                    else f'Cancelled via chatbot at {now.isoformat()}.',
                now,
                client_id,
                order_id
            )
        )
        conn.commit()

        logger.info(f'[Tool:cancel_order] client={client_id} order={order_id} cancelled by {customer_email}')
        return {
            'success': True,
            'message': (
                f'Order {order_id} has been successfully cancelled. '
                'A confirmation will be sent to your email shortly.'
            ),
            'order_id':         order_id,
            'previous_status':  current_status,
            'new_status':       'cancelled'
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
# TOOL 3 — check_availability
# Checks open appointment/booking slots for a client.
# =====================================================================

def check_availability(client_id: str, date: str = "", service_type: str = "") -> dict:
    """
    Check available appointment slots for a client.

    Args:
        client_id:    Lumvi client identifier
        date:         ISO date string YYYY-MM-DD. Defaults to today if empty.
        service_type: Optional filter (e.g. "consultation", "viewing", "follow_up")

    Returns:
        {success, slots: [{slot_id, datetime, service_type, duration_minutes}], date}
    """
    client_id    = _sanitize(client_id, 50)
    date         = _sanitize(date, 20)
    service_type = _sanitize(service_type, 100)

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

        cursor.execute(
            '''
            INSERT INTO appointments
                (booking_id, client_id, slot_id, customer_name, customer_email,
                 customer_phone, notes, status, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''',
            (booking_id, client_id, slot_id, customer_name, customer_email,
             customer_phone, notes, 'confirmed', now)
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
            f'slot={slot_id} customer={customer_email}'
        )

        return {
            'success':              True,
            'booking_id':           booking_id,
            'slot_datetime':        slot_dt,
            'service_type':         service_type,
            'duration_minutes':     slot.get('duration_minutes', 30),
            'confirmation_message': (
                f"Your {service_type} has been booked for {slot_dt}. "
                f"Booking reference: {booking_id}. "
                f"A confirmation will be sent to {customer_email}."
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

        # Use PostgreSQL ILIKE for case-insensitive partial matching.
        # This is intentionally simple — ai_helper's embedding search is the
        # primary relevance engine. This tool is the agent's explicit action.
        like_pattern = f'%{query}%'

        cursor.execute(
            '''
            SELECT id, question, answer, category,
                   CASE
                       WHEN LOWER(question) LIKE LOWER(%s) THEN 2
                       WHEN LOWER(answer)   LIKE LOWER(%s) THEN 1
                       ELSE 0
                   END AS relevance_score
            FROM knowledge_base
            WHERE client_id = %s
              AND is_active = TRUE
              AND (
                  LOWER(question) LIKE LOWER(%s)
                  OR LOWER(answer)   LIKE LOWER(%s)
              )
            ORDER BY relevance_score DESC, question ASC
            LIMIT %s
            ''',
            (like_pattern, like_pattern, client_id, like_pattern, like_pattern, limit)
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
