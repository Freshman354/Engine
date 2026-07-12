"""
blueprints/client_settings.py
==============================
POST /api/client/settings

Saves per-client lead delivery configuration from manage_client_users.html.
Each save button posts one field at a time:

  { client_id, notification_email }   → UPDATE clients SET notification_email
  { client_id, notification_phone }   → UPDATE clients SET notification_phone
  { client_id, webhook_url }          → upsert into webhook_configs via save_webhooks()

Ownership is enforced on every request — agencies can only edit
clients that belong to their account.
"""

import re
from flask import Blueprint, jsonify, request
from flask_login import current_user, login_required

import models
from models.db import get_db
from utils import get_logger

logger = get_logger('lumvi.client_settings')

client_settings_bp = Blueprint('client_settings', __name__)

# ── Validation helpers ────────────────────────────────────────────────────────

_EMAIL_RE  = re.compile(r'^[\w.%+\-]+@[\w.\-]+\.[a-zA-Z]{2,}$')
_PHONE_RE  = re.compile(r'^[+\d][\d\s\-().]{6,19}$')
_HTTPS_RE  = re.compile(r'^https://.{4,}')

ALLOWED_FIELDS = {'notification_email', 'notification_phone', 'webhook_url'}


def _validate_field(name: str, value: str):
    """
    Returns (clean_value, error_string).
    clean_value is None on validation failure.
    Allows empty string — that clears the field.
    """
    v = (value or '').strip()

    if v == '':
        return v, None   # clearing a field is always allowed

    if name == 'notification_email':
        if not _EMAIL_RE.match(v):
            return None, 'Invalid email address'
        return v.lower(), None

    if name == 'notification_phone':
        digits = re.sub(r'[^\d]', '', v)
        if len(digits) < 7:
            return None, 'Phone number too short'
        if not _PHONE_RE.match(v):
            return None, 'Invalid phone number format'
        return v, None

    if name == 'webhook_url':
        if not _HTTPS_RE.match(v):
            return None, 'Webhook URL must start with https://'
        return v, None

    return v, None


# ── Route ─────────────────────────────────────────────────────────────────────

@client_settings_bp.route('/api/client/settings', methods=['POST'])
@login_required
def update_client_settings():
    """
    Accepts a partial update — only the field(s) present in the body
    are written. Unknown fields are silently ignored.

    Response: { success: true } | { success: false, error: '...' }
    """
    data = request.get_json(silent=True) or {}

    client_id = (data.get('client_id') or '').strip()
    if not client_id:
        return jsonify({'success': False, 'error': 'client_id is required'}), 400

    # Ownership check — agency can only edit their own clients
    if not models.verify_client_ownership(current_user.id, client_id):
        logger.warning(
            f'[ClientSettings] ownership check failed '
            f'user={current_user.id} client={client_id}'
        )
        return jsonify({'success': False, 'error': 'Client not found'}), 404

    # Collect fields to update
    updates = {}
    webhook_url = None

    for field in ALLOWED_FIELDS:
        if field not in data:
            continue
        clean, err = _validate_field(field, data[field])
        if err:
            return jsonify({'success': False, 'error': err}), 400
        if field == 'webhook_url':
            webhook_url = clean   # handled separately via webhook_configs
        else:
            updates[field] = clean

    if not updates and webhook_url is None:
        return jsonify({'success': False, 'error': 'No valid fields provided'}), 400

    # FIX: notification_email used to be freely agency-editable text with no
    # ownership guarantee at all — any string, no verification. If this
    # client has a designated primary contact (a client_user who holds their
    # own portal login — see models.get_primary_contact), that person's
    # email is the source of truth going forward and the agency can no
    # longer overwrite it directly here. Change who the primary contact is
    # via /api/client-users/set-primary instead.
    if 'notification_email' in updates:
        primary = models.get_primary_contact(client_id)
        if primary:
            return jsonify({
                'success': False,
                'error': (
                    "This client has a designated primary contact — "
                    "notification email is managed from Client Users, "
                    "not set directly."
                ),
            }), 409
            # 409 Conflict: the request is well-formed, but this field is no
            # longer agency-owned — distinct from a 400 (bad input) or a 403
            # (route in general is unauthorized), since neither describes
            # "this specific field has moved out of your control."

    # ── Write notification columns to clients table ────────────────────────────
    if updates:
        if not _write_client_columns(client_id, updates):
            return jsonify({'success': False, 'error': 'Database error — try again'}), 500

    # ── Write webhook_url to webhook_configs ──────────────────────────────────
    # save_webhooks() replaces all configs for this client, so we fetch
    # existing ones first to avoid wiping any non-lead-delivery webhooks.
    if webhook_url is not None:
        if not _upsert_lead_webhook(client_id, webhook_url):
            return jsonify({'success': False, 'error': 'Failed to save webhook — try again'}), 500

    logger.info(
        f'[ClientSettings] updated client={client_id} '
        f'user={current_user.id} fields={list(updates.keys())}'
        + (f' webhook_url={bool(webhook_url)}' if webhook_url is not None else '')
    )
    return jsonify({'success': True})


# ── DB helpers ────────────────────────────────────────────────────────────────

def _write_client_columns(client_id: str, fields: dict) -> bool:
    """
    UPDATE clients SET notification_email = %s ... WHERE client_id = %s.
    Only writes columns that are in ALLOWED_FIELDS minus webhook_url.
    """
    safe_fields = {
        k: v for k, v in fields.items()
        if k in ('notification_email', 'notification_phone')
    }
    if not safe_fields:
        return True

    set_clause = ', '.join(f'{col} = %s' for col in safe_fields)
    values     = list(safe_fields.values()) + [client_id]

    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute(
            f'UPDATE clients SET {set_clause} WHERE client_id = %s',
            values,
        )
        conn.commit()
        return True
    except Exception as e:
        if conn:
            try: conn.rollback()
            except Exception: pass
        logger.error(f'[ClientSettings] DB write error client={client_id}: {e}')
        return False
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


def _upsert_lead_webhook(client_id: str, url: str) -> bool:
    """
    Merge a 'lead_captured' webhook into the client's webhook_configs.
    Fetches existing webhooks first so non-lead-delivery configs are
    preserved. Passes through save_webhooks() which has SSRF protection.

    If url is empty string, removes the lead_delivery webhook.
    """
    try:
        existing = models.get_webhooks(client_id) or []

        # Strip any existing lead_delivery webhook
        LEAD_WH_ID = f'lead_delivery_{client_id}'
        others = [w for w in existing if w.get('webhook_id') != LEAD_WH_ID]

        if url:
            # Upsert the lead delivery webhook
            others.append({
                'webhook_id': LEAD_WH_ID,
                'name':       'Lead Delivery',
                'url':        url,
                'events':     ['lead_captured'],
                'enabled':    True,
            })

        saved = models.save_webhooks(client_id, others)
        # save_webhooks returns count saved; -1 or exception = failure
        return saved >= 0

    except Exception as e:
        logger.error(f'[ClientSettings] webhook upsert error client={client_id}: {e}')
        return False
