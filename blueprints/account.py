"""
blueprints/account.py
----------------------
Agency self-service account settings: editable business profile,
email/password changes, notification preferences, and the account
deletion flow (soft delete with a grace period; permanent removal is
handled separately by blueprints/cron.py::cron_hard_delete_accounts).

Routes
------
  GET         /settings                          settings_page
  PATCH       /api/account/profile                update_profile
  PATCH       /api/account/email                  update_email
  PATCH       /api/account/password               update_password
  PATCH       /api/account/notifications           update_notifications
  POST        /api/account/request-deletion       request_deletion
  POST        /api/account/cancel-deletion        cancel_deletion

Registration in app.py:
  from blueprints.account import account_bp
  app.register_blueprint(account_bp)
"""

from flask import Blueprint, jsonify, render_template, request
from flask_login import current_user, login_required, logout_user

import models

account_bp = Blueprint('account', __name__)


@account_bp.route('/settings')
@login_required
def settings_page():
    user = models.get_user_by_id(current_user.id) or {}
    pending_deletion = models.get_pending_deletion(current_user.id)
    notification_prefs = models.get_notification_prefs(current_user.id)
    return render_template(
        'settings.html',
        user=user,
        pending_deletion=pending_deletion,
        notification_prefs=notification_prefs,
    )


@account_bp.route('/api/account/profile', methods=['PATCH'])
@login_required
def update_profile():
    data = request.get_json(force=True) or {}
    ok = models.update_user_profile(
        current_user.id,
        company_name=data.get('company_name'),
        logo_url=data.get('logo_url'),
        contact_phone=data.get('contact_phone'),
    )
    if not ok:
        return jsonify({'success': False, 'error': 'Could not save profile.'}), 500
    return jsonify({'success': True}), 200


@account_bp.route('/api/account/email', methods=['PATCH'])
@login_required
def update_email():
    data = request.get_json(force=True) or {}
    password = data.get('current_password', '')
    new_email = data.get('new_email', '')

    # Require current password before changing login email — this is the
    # credential an attacker with a hijacked session would NOT have.
    user = models.get_user_by_id(current_user.id)
    if not user or not models.verify_user(user['email'], password):
        return jsonify({'success': False, 'error': 'Current password is incorrect.'}), 403

    result = models.update_user_email(current_user.id, new_email)
    if not result.get('success'):
        return jsonify(result), 400
    return jsonify({'success': True}), 200


@account_bp.route('/api/account/password', methods=['PATCH'])
@login_required
def update_password():
    data = request.get_json(force=True) or {}
    current_password = data.get('current_password', '')
    new_password = data.get('new_password', '')

    if len(new_password) < 8:
        return jsonify({'success': False, 'error': 'New password must be at least 8 characters.'}), 400

    user = models.get_user_by_id(current_user.id)
    if not user or not models.verify_user(user['email'], current_password):
        return jsonify({'success': False, 'error': 'Current password is incorrect.'}), 403

    models.update_user_password(current_user.id, new_password)
    return jsonify({'success': True}), 200


@account_bp.route('/api/account/notifications', methods=['PATCH'])
@login_required
def update_notifications():
    data = request.get_json(force=True) or {}
    allowed = {'weekly_digest', 'lead_alerts', 'billing_alerts'}
    prefs = {k: v for k, v in data.items() if k in allowed}
    ok = models.update_notification_prefs(current_user.id, prefs)
    if not ok:
        return jsonify({'success': False, 'error': 'Could not save preferences.'}), 500
    return jsonify({'success': True, 'notification_prefs': models.get_notification_prefs(current_user.id)}), 200


@account_bp.route('/api/account/request-deletion', methods=['POST'])
@login_required
def request_deletion():
    """
    Starts the soft-delete grace period. Requires current password.

    Also flags the subscription to cancel at period end (same DB-level
    effect as the existing "Cancel Subscription" button in billing.py) so
    it won't silently renew during the grace period. NOTE: this does NOT
    call the Flutterwave/PayPal cancel API immediately — that logic is
    ~80 lines inline in blueprints/billing.py::cancel_subscription using
    injected dependencies (mail, subscription-status helper) not available
    here. If the account is deleted before the current billing period
    ends, the external subscription is stopped by billing.py's existing
    downgrade_expired_users() cron path when the period lapses, same as
    any other cancellation today — not an instant refund/stop.
    """
    data = request.get_json(force=True) or {}
    password = data.get('current_password', '')
    reason = data.get('reason', '')

    user = models.get_user_by_id(current_user.id)
    if not user or not models.verify_user(user['email'], password):
        return jsonify({'success': False, 'error': 'Current password is incorrect.'}), 403

    # Flag the subscription to cancel at period end — mirrors the existing
    # standalone "Cancel Subscription" behavior. Does not call the external
    # payment provider's API immediately (see docstring above).
    if user.get('subscription_id'):
        try:
            models.cancel_user_subscription(current_user.id)
        except Exception:
            pass  # deletion should still proceed even if this fails

    result = models.request_account_deletion(current_user.id, reason=reason)
    if not result.get('success'):
        return jsonify(result), 500
    return jsonify(result), 200


@account_bp.route('/api/account/cancel-deletion', methods=['POST'])
@login_required
def cancel_deletion():
    ok = models.cancel_account_deletion(current_user.id)
    if not ok:
        return jsonify({'success': False, 'error': 'Could not cancel deletion.'}), 500
    return jsonify({'success': True}), 200
