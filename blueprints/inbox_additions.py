"""
blueprints/inbox_additions.py
------------------------------
Tier 1 inbox routes.  Register this blueprint in app.py with:

    from blueprints.inbox_additions import inbox_additions_bp
    app.register_blueprint(inbox_additions_bp)

These routes sit alongside the existing inbox_bp routes and share the
same /api/inbox/* prefix.  They are split into a separate file so they
can be dropped in without touching the existing inbox.py.
"""

from flask          import Blueprint, jsonify, request
from flask_login    import login_required, current_user
import models

inbox_additions_bp = Blueprint('inbox_additions', __name__)


# ── CSAT ─────────────────────────────────────────────────────────────────────

@inbox_additions_bp.route('/api/csat', methods=['POST'])
def submit_csat_route():
    """Public — widget submits rating after conversation ends."""
    data       = request.get_json() or {}
    client_id  = data.get('client_id',  '').strip()
    session_id = data.get('session_id', '').strip()
    rating_raw = data.get('rating')

    if not client_id or not session_id:
        return jsonify({'success': False, 'error': 'Missing client_id or session_id'}), 400
    try:
        rating = int(rating_raw)
    except (TypeError, ValueError):
        return jsonify({'success': False, 'error': 'rating must be 1 or -1'}), 400

    ok = models.submit_csat(client_id, session_id, rating)
    return jsonify({'success': ok})


# ── CONVERSATION STATUS ───────────────────────────────────────────────────────

@inbox_additions_bp.route('/api/inbox/session/<session_id>/status', methods=['GET'])
def get_session_status_route(session_id):
    """
    Public — polled by the widget every 3s.
    Returns current status + agent_typing flag in a single call.
    """
    client_id = request.args.get('client_id', '').strip()
    if not client_id:
        return jsonify({'success': False, 'error': 'client_id required'}), 400

    status       = models.get_session_status(client_id, session_id)
    agent_typing = models.get_agent_typing(client_id, session_id)
    return jsonify({
        'success':      True,
        'status':       status,
        'agent_typing': agent_typing,
    })


@inbox_additions_bp.route('/api/inbox/session/<session_id>/status', methods=['POST'])
@login_required
def set_session_status_route(session_id):
    """Inbox agent — transition session to a new status."""
    data      = request.get_json() or {}
    client_id = data.get('client_id', '').strip()
    status    = data.get('status',    '').strip()

    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    ok = models.set_session_status(client_id, session_id, status)
    return jsonify({'success': ok})


# ── TYPING INDICATOR ─────────────────────────────────────────────────────────

@inbox_additions_bp.route('/api/inbox/typing', methods=['POST'])
@login_required
def agent_typing():
    """
    Inbox agent — fires on textarea keydown (debounced 800 ms client-side).
    Stores a timestamp in session_data JSONB; expires naturally after 4s.
    """
    data       = request.get_json() or {}
    client_id  = data.get('client_id',  '').strip()
    session_id = data.get('session_id', '').strip()

    if not client_id or not session_id:
        return jsonify({'success': False}), 400
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False}), 403

    models.set_agent_typing(client_id, session_id)
    return jsonify({'success': True})


# ── CONVERSATION TAGS ────────────────────────────────────────────────────────

@inbox_additions_bp.route('/api/inbox/tags', methods=['GET'])
@login_required
def list_tags():
    """Return the tag library for a client."""
    client_id = request.args.get('client_id', '').strip()
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False}), 403
    return jsonify({'success': True, 'tags': models.get_client_tags(client_id)})


@inbox_additions_bp.route('/api/inbox/tags', methods=['POST'])
@login_required
def create_tag():
    """Create a new tag in the client tag library."""
    data      = request.get_json() or {}
    client_id = data.get('client_id', '').strip()
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False}), 403

    tag = models.create_tag(
        client_id,
        name  = data.get('name',  '').strip(),
        color = data.get('color', '#6366f1').strip(),
    )
    return jsonify({'success': bool(tag), 'tag': tag})


@inbox_additions_bp.route('/api/inbox/tags/<int:tag_id>', methods=['DELETE'])
@login_required
def delete_tag(tag_id):
    """Delete a tag and all its session associations (CASCADE)."""
    client_id = request.args.get('client_id', '').strip()
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False}), 403
    return jsonify({'success': models.delete_tag(client_id, tag_id)})


@inbox_additions_bp.route('/api/inbox/session/<session_id>/tags', methods=['GET'])
@login_required
def get_session_tags(session_id):
    """Return all tags currently applied to a session."""
    return jsonify({'success': True, 'tags': models.get_session_tags(session_id)})


@inbox_additions_bp.route('/api/inbox/session/<session_id>/tags', methods=['POST'])
@login_required
def apply_tag_to_session(session_id):
    """Apply an existing tag to a session."""
    data      = request.get_json() or {}
    client_id = data.get('client_id', '').strip()
    tag_id    = data.get('tag_id')
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False}), 403
    try:
        ok = models.apply_tag(session_id, int(tag_id), client_id)
    except (TypeError, ValueError):
        return jsonify({'success': False, 'error': 'invalid tag_id'}), 400
    return jsonify({'success': ok})


@inbox_additions_bp.route('/api/inbox/session/<session_id>/tags/<int:tag_id>', methods=['DELETE'])
@login_required
def remove_tag_from_session(session_id, tag_id):
    """Remove a tag from a session."""
    return jsonify({'success': models.remove_tag(session_id, tag_id)})


# ── PROACTIVE TRIGGERS ───────────────────────────────────────────────────────

@inbox_additions_bp.route('/api/admin/triggers', methods=['GET'])
@login_required
def list_triggers():
    """Return all active proactive triggers for a client."""
    client_id = request.args.get('client_id', '').strip()
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False}), 403
    return jsonify({'success': True, 'triggers': models.get_proactive_triggers(client_id)})


@inbox_additions_bp.route('/api/admin/triggers', methods=['POST'])
@login_required
def create_trigger():
    """Create a new proactive trigger rule."""
    data      = request.get_json() or {}
    client_id = data.get('client_id', '').strip()
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False}), 403

    new_id = models.save_proactive_trigger(
        client_id     = client_id,
        name          = data.get('name',          '').strip(),
        trigger_type  = data.get('trigger_type',  '').strip(),
        trigger_value = data.get('trigger_value', '').strip(),
        message       = data.get('message',       '').strip(),
    )
    return jsonify({'success': bool(new_id), 'id': new_id})


@inbox_additions_bp.route('/api/admin/triggers/<int:trigger_id>', methods=['DELETE'])
@login_required
def delete_trigger(trigger_id):
    """Delete a proactive trigger."""
    client_id = request.args.get('client_id', '').strip()
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False}), 403
    return jsonify({'success': models.delete_proactive_trigger(client_id, trigger_id)})
