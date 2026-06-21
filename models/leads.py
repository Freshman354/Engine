"""
models/leads.py
---------------
Lead capture, CRM pipeline (stages, notes, assignment), bulk operations,
and lead-based admin queries.
"""
import json
import uuid
from datetime import datetime
from .db import get_db

def save_lead(client_id, lead_data):
    """
    Save a lead for a client. Returns True on success, False on failure.

    Duplicate handling: if a lead with the same email already exists for
    this client (case-insensitive match), the existing row is enriched
    instead of inserting a new one — submission_count is incremented and
    any newly-provided non-blank fields overwrite the old ones (blank
    fields on the new submission never clobber existing data).
    """
    conn, cursor = get_db()
    try:
        # Serialize custom_fields dict to JSON string for TEXT column
        custom_fields = lead_data.get('custom_fields')
        if isinstance(custom_fields, dict):
            custom_fields = json.dumps(custom_fields)

        cursor.execute(
            'SELECT id FROM leads WHERE client_id = %s AND LOWER(email) = LOWER(%s)',
            (client_id, lead_data['email'])
        )
        existing = cursor.fetchone()

        if existing:
            cursor.execute(
                '''UPDATE leads
                   SET name                 = COALESCE(NULLIF(%s, ''), name),
                       phone                = COALESCE(NULLIF(%s, ''), phone),
                       company              = COALESCE(NULLIF(%s, ''), company),
                       message              = COALESCE(NULLIF(%s, ''), message),
                       custom_fields        = COALESCE(%s, custom_fields),
                       conversation_snippet = COALESCE(NULLIF(%s, ''), conversation_snippet),
                       source_url           = COALESCE(NULLIF(%s, ''), source_url),
                       intent_summary       = COALESCE(NULLIF(%s, ''), intent_summary),
                       submission_count     = COALESCE(submission_count, 1) + 1,
                       updated_at           = NOW()
                   WHERE id = %s''',
                (
                    lead_data.get('name', ''),
                    lead_data.get('phone', ''),
                    lead_data.get('company', ''),
                    lead_data.get('message', ''),
                    custom_fields,
                    lead_data.get('conversation_snippet', ''),
                    lead_data.get('source_url', ''),
                    lead_data.get('intent_summary', ''),
                    existing['id']
                )
            )
        else:
            cursor.execute(
                '''INSERT INTO leads (client_id, name, email, phone, company, message, custom_fields, conversation_snippet, source_url, intent_summary, priority)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                (
                    client_id,
                    lead_data['name'],
                    lead_data['email'],
                    lead_data.get('phone', ''),
                    lead_data.get('company', ''),
                    lead_data.get('message', ''),
                    custom_fields,
                    lead_data.get('conversation_snippet', ''),
                    lead_data.get('source_url', ''),
                    lead_data.get('intent_summary', ''),
                    lead_data.get('priority') or 'high'
                )
            )
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        import logging
        logging.getLogger(__name__).error(f"[save_lead] Failed for client {client_id}: {e}")
        return False
    finally:
        cursor.close()
        conn.close()


def get_lead_by_id(client_id, lead_id):
    """
    Fetch a single lead by its integer primary-key id.
    Returns a dict or None.
    """
    conn, cursor = get_db()
    try:
        cursor.execute(
            'SELECT * FROM leads WHERE id = %s AND client_id = %s',
            (lead_id, client_id)
        )
        row = cursor.fetchone()
        if not row:
            return None
        result = dict(row)
        if result.get('created_at'):
            result['created_at'] = result['created_at'].isoformat()
        if result.get('updated_at'):
            result['updated_at'] = result['updated_at'].isoformat()
        if result.get('follow_up_at'):
            result['follow_up_at'] = result['follow_up_at'].isoformat()
        if result.get('closed_value') is not None:
            result['closed_value'] = float(result['closed_value'])
        if result.get('custom_fields') and isinstance(result['custom_fields'], str):
            try:
                result['custom_fields'] = json.loads(result['custom_fields'])
            except Exception:
                pass
        if result.get('activity_log') and isinstance(result['activity_log'], str):
            try:
                result['activity_log'] = json.loads(result['activity_log'])
            except Exception:
                result['activity_log'] = []
        return result
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[get_lead_by_id] {e}")
        return None
    finally:
        cursor.close()
        conn.close()


def update_lead(client_id, lead_id, updates: dict):
    """
    Update allowed fields on a lead row. Appends to activity_log.
    updates keys: stage, notes, assigned_to, priority, name, email, phone, company
    Returns the updated lead dict, or None on failure.
    """
    allowed = {'stage', 'notes', 'assigned_to', 'priority', 'name', 'email', 'phone', 'company',
               'lost_reason', 'follow_up_at', 'closed_value', 'outcome_notes'}
    clean   = {k: v for k, v in updates.items() if k in allowed}
    if not clean:
        return get_lead_by_id(client_id, lead_id)

    conn, cursor = get_db()
    try:
        # Load existing activity_log
        cursor.execute(
            'SELECT activity_log FROM leads WHERE id = %s AND client_id = %s',
            (lead_id, client_id)
        )
        row = cursor.fetchone()
        if not row:
            return None

        existing_log = []
        if row['activity_log']:
            try:
                existing_log = json.loads(row['activity_log'])
            except Exception:
                existing_log = []

        new_entry = {
            'ts':     __import__('datetime').datetime.utcnow().isoformat() + 'Z',
            'user':   updates.get('_actor', 'system'),
            'action': updates.get('_action', 'Updated: ' + ', '.join(clean.keys())),
        }
        existing_log.append(new_entry)

        set_clauses = ', '.join(f"{k} = %s" for k in clean)
        if 'follow_up_at' in clean:
            # A (re)scheduled follow-up date should get its own reminder —
            # without this, get_due_follow_ups()'s dedup flag would
            # permanently block any reminder after the very first one ever sent.
            set_clauses += ", followup_reminder_sent_at = NULL"
        set_clauses += ", activity_log = %s, updated_at = NOW()"
        values = list(clean.values()) + [json.dumps(existing_log), lead_id, client_id]

        cursor.execute(
            f"UPDATE leads SET {set_clauses} WHERE id = %s AND client_id = %s",
            values
        )
        conn.commit()
        return get_lead_by_id(client_id, lead_id)
    except Exception as e:
        conn.rollback()
        import logging
        logging.getLogger(__name__).error(f"[update_lead] {e}")
        return None
    finally:
        cursor.close()
        conn.close()


def delete_lead_by_client(client_id, lead_id):
    """
    Delete a single lead, enforcing client_id ownership.
    Returns True on success, False if not found or error.
    """
    conn, cursor = get_db()
    try:
        cursor.execute(
            'DELETE FROM leads WHERE id = %s AND client_id = %s',
            (lead_id, client_id)
        )
        deleted = cursor.rowcount
        conn.commit()
        return deleted > 0
    except Exception as e:
        conn.rollback()
        import logging
        logging.getLogger(__name__).error(f"[delete_lead_by_client] {e}")
        return False
    finally:
        cursor.close()
        conn.close()


def bulk_update_leads(client_id, lead_ids: list, updates: dict, actor: str = 'system'):
    """
    Update multiple leads at once. Returns count of rows updated.
    updates keys: stage, assigned_to, priority
    """
    allowed = {'stage', 'assigned_to', 'priority'}
    clean   = {k: v for k, v in updates.items() if k in allowed}
    if not clean or not lead_ids:
        return 0

    conn, cursor = get_db()
    updated = 0
    try:
        for lead_id in lead_ids:
            cursor.execute(
                'SELECT activity_log FROM leads WHERE id = %s AND client_id = %s',
                (lead_id, client_id)
            )
            row = cursor.fetchone()
            if not row:
                continue
            existing_log = []
            if row['activity_log']:
                try:
                    existing_log = json.loads(row['activity_log'])
                except Exception:
                    pass
            existing_log.append({
                'ts':     __import__('datetime').datetime.utcnow().isoformat() + 'Z',
                'user':   actor,
                'action': 'Bulk update: ' + ', '.join(f"{k}={v}" for k, v in clean.items()),
            })
            set_clauses = ', '.join(f"{k} = %s" for k in clean)
            set_clauses += ", activity_log = %s, updated_at = NOW()"
            values = list(clean.values()) + [json.dumps(existing_log), lead_id, client_id]
            cursor.execute(
                f"UPDATE leads SET {set_clauses} WHERE id = %s AND client_id = %s",
                values
            )
            updated += cursor.rowcount
        conn.commit()
        return updated
    except Exception as e:
        conn.rollback()
        import logging
        logging.getLogger(__name__).error(f"[bulk_update_leads] {e}")
        return 0
    finally:
        cursor.close()
        conn.close()


def get_leads(client_id, stage=None, search=None):
    """
    Get leads for a client, newest first.
    stage  — filter to a single pipeline stage (SQL-side, COALESCE handles legacy NULLs)
    search — case-insensitive substring match on name, email, or company (SQL ILIKE)
    Returns [] on failure.
    """
    try:
        conn, cursor = get_db()
        query  = "SELECT * FROM leads WHERE client_id = %s"
        params = [client_id]
        if stage:
            query += " AND COALESCE(stage, 'new') = %s"
            params.append(stage)
        if search:
            term   = '%' + search + '%'
            query += " AND (name ILIKE %s OR email ILIKE %s OR company ILIKE %s)"
            params.extend([term, term, term])
        query += " ORDER BY created_at DESC"
        cursor.execute(query, params)
        leads = cursor.fetchall()
        cursor.close()
        conn.close()
        result = []
        for lead in leads:
            row = dict(lead)
            # Serialize datetime fields so JSON / frontend fmtDate() works
            if row.get('created_at'):
                row['created_at'] = row['created_at'].isoformat()
            if row.get('updated_at'):
                row['updated_at'] = row['updated_at'].isoformat()
            if row.get('follow_up_at'):
                row['follow_up_at'] = row['follow_up_at'].isoformat()
            if row.get('closed_value') is not None:
                row['closed_value'] = float(row['closed_value'])
            # Deserialize custom_fields back to dict if stored as JSON string
            if row.get('custom_fields') and isinstance(row['custom_fields'], str):
                try:
                    row['custom_fields'] = json.loads(row['custom_fields'])
                except Exception:
                    pass
            # Deserialize activity_log back to list
            if row.get('activity_log') and isinstance(row['activity_log'], str):
                try:
                    row['activity_log'] = json.loads(row['activity_log'])
                except Exception:
                    row['activity_log'] = []
            # Default stage for legacy rows that pre-date the migration
            if not row.get('stage'):
                row['stage'] = 'new'
            result.append(row)
        return result
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[get_leads] {e}")
        return []


# =====================================================================
# AFFILIATE FUNCTIONS
# =====================================================================

def get_all_leads_admin(limit=500, client_id_filter=None, search=None):
    """Leads across all clients for admin view."""
    conn, cursor = get_db()
    query = '''SELECT l.*, c.company_name, u.email as owner_email
               FROM leads l
               LEFT JOIN clients c ON l.client_id = c.client_id
               LEFT JOIN users u ON c.user_id = u.id
               WHERE 1=1'''
    params = []
    if client_id_filter:
        query += ' AND l.client_id = %s'
        params.append(client_id_filter)
    if search:
        query += ' AND (l.name ILIKE %s OR l.email ILIKE %s)'
        params.extend(['%' + search + '%', '%' + search + '%'])
    query += ' ORDER BY l.created_at DESC LIMIT %s'
    params.append(limit)
    cursor.execute(query, params)
    rows = [dict(r) for r in cursor.fetchall()]
    cursor.close()
    conn.close()
    for r in rows:
        if r.get('created_at'):
            r['created_at'] = r['created_at'].isoformat()
    return rows


def admin_delete_lead(lead_id):
    """Delete a single lead by id."""
    conn, cursor = get_db()
    cursor.execute('DELETE FROM leads WHERE id = %s', (lead_id,))
    conn.commit()
    cursor.close()
    conn.close()


# =====================================================================
# AGENCY PER-SEAT OVERAGE
# =====================================================================

