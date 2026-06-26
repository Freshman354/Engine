"""
blueprints/billing.py
---------------------
Upgrade page, Flutterwave payment (callback + server webhook),
subscription cancellation, and affiliate programme routes.

Extracted from app.py. All behaviour is identical to the original;
nothing has been changed except:
  - Route registration: Blueprint vs app
  - app.logger → current_app.logger
  - Inline stdlib imports (time, base64) promoted to module level
  - PLAN_PRICES_FLW moved here — it is billing-only data
  - Dependencies injected at registration time via init_billing()

Routes
------
  GET         /upgrade                                upgrade_page
  GET         /payment/flutterwave/callback           flutterwave_callback
  POST        /payment/flutterwave/webhook            flutterwave_webhook
  GET/POST    /subscription/cancel                    cancel_subscription
  GET/POST    /become-affiliate                       become_affiliate
  GET         /affiliate-dashboard                    affiliate_dashboard

Registration in app.py:
  from blueprints.billing import billing_bp, init_billing, PLAN_PRICES_FLW
  init_billing(mail=mail, get_subscription_status=get_subscription_status)
  app.register_blueprint(billing_bp)
"""

import base64
import os
import time

import requests as _requests
from flask import (Blueprint, flash, jsonify, redirect,
                   render_template, request, current_app, url_for)
from flask_login import current_user, login_required
from flask_mail import Message

import models

# ── Blueprint ────────────────────────────────────────────────────────────────

billing_bp = Blueprint('billing', __name__)

# Injected dependencies — populated by init_billing() before first request.
_mail                   = None
_get_subscription_status = None


def init_billing(mail, get_subscription_status):
    """
    Called once in app.py after all shared objects are ready.
    Must be called before the first request reaches this blueprint.
    """
    global _mail, _get_subscription_status
    _mail                    = mail
    _get_subscription_status = get_subscription_status


# ── Pricing table ─────────────────────────────────────────────────────────────
# Kept here rather than app.py — only billing routes reference it.
# Import PLAN_PRICES_FLW in app.py if any non-blueprint code needs it.

PLAN_PRICES_FLW = {
    'solo':    {'monthly': 19.00,  'annual': 190.00},
    'starter': {'monthly': 49.00,  'annual': 490.00},
    'pro':     {'monthly': 99.00,  'annual': 990.00},
    'growth':  {'monthly': 149.00, 'annual': 1490.00},
    'agency':  {'monthly': 299.00, 'annual': 2990.00},
}


# ── Routes ───────────────────────────────────────────────────────────────────

@billing_bp.route('/upgrade')
@login_required
def upgrade_page():
    return render_template(
        'upgrade.html',
        user=current_user,
        flw_public_key=os.environ.get('FLW_PUBLIC_KEY', '')
    )


@billing_bp.route('/payment/flutterwave/callback')
@login_required
def flutterwave_callback():
    """
    Flutterwave redirects here after payment (plan upgrades AND seat overages).

    tx_ref formats:
      Plan upgrade:  lumvi_{plan}_{cycle}_{user_id}_{timestamp}
      Seat overage:  lumvi_overage_{user_id}_{timestamp}

    Fixes:
    - FW-001: Duplicate check before subscription update
    - FW-002: Remove USD-only currency guard on amount validation
    - FW-004: Validate via Flutterwave signature
    - FW-008: Retry logic for verify API
    - FW-009: Handle overage tx_ref (was crashing with 'unknown plan')
    - FW-010: Fix url_for('dashboard') → url_for('auth.dashboard')
    """
    status         = request.args.get('status', '')
    tx_ref         = request.args.get('tx_ref', '')
    transaction_id = request.args.get('transaction_id', '')

    if status != 'successful':
        flash("Payment was not completed. Please try again.", 'error')
        return redirect(url_for('billing.upgrade_page'))

    if not transaction_id:
        flash("Invalid payment reference. Contact support@lumvi.net.", 'error')
        return redirect(url_for('billing.upgrade_page'))

    flw_secret = os.environ.get('FLW_SECRET_KEY', '')
    if not flw_secret:
        current_app.logger.error("FLW_SECRET_KEY not set")
        flash("Payment configuration error. Contact support@lumvi.net.", 'error')
        return redirect(url_for('billing.upgrade_page'))

    # Verify with Flutterwave API — retry up to 3 times with exponential backoff (FW-008)
    flw_data   = None
    verify_url = f"https://api.flutterwave.com/v3/transactions/{transaction_id}/verify"
    headers    = {"Authorization": f"Bearer {flw_secret}"}

    for attempt in range(3):
        try:
            resp = _requests.get(verify_url, headers=headers, timeout=15)
            resp.raise_for_status()
            flw_data = resp.json()
            break
        except Exception as e:
            current_app.logger.warning(
                f"Flutterwave verify attempt {attempt + 1}/3 failed: {e}"
            )
            if attempt == 2:
                current_app.logger.error(
                    f"Flutterwave verify error after 3 attempts: {e}"
                )
                flash("Could not verify payment. Contact support@lumvi.net.", 'error')
                return redirect(url_for('billing.upgrade_page'))
            time.sleep(2 ** attempt)  # 1s, 2s backoff

    if not flw_data or flw_data.get('status') != 'success':
        flash("Payment verification failed. Contact support@lumvi.net.", 'error')
        return redirect(url_for('billing.upgrade_page'))

    txn = flw_data.get('data', {})
    if txn.get('status') != 'successful':
        flash("Payment not successful. Please try again.", 'error')
        return redirect(url_for('billing.upgrade_page'))

    paid_amount    = float(txn.get('amount', 0))
    paid_currency  = txn.get('currency', 'USD')
    txn_created_at = txn.get('created_at')

    # ── FW-009: Detect overage payments by tx_ref prefix ─────────────────────
    parts = tx_ref.split('_')
    is_overage = (len(parts) >= 2 and parts[1] == 'overage')

    if is_overage:
        # tx_ref format: lumvi_overage_{user_id}_{timestamp}
        # Duplicate check
        try:
            conn, cursor = models.get_db()
            cursor.execute(
                "SELECT id FROM payments WHERE reference = %s LIMIT 1",
                (str(transaction_id),)
            )
            already_processed = cursor.fetchone()
            cursor.close()
            conn.close()
            if already_processed:
                current_app.logger.warning(
                    f"Overage callback: duplicate txn {transaction_id} for user {current_user.id}"
                )
                flash("This payment has already been processed.", 'info')
                return redirect(url_for('auth.dashboard'))
        except Exception as e:
            current_app.logger.error(f"Overage duplicate check failed: {e}")

        models.record_payment(
            current_user.id, paid_amount, 'agency',
            provider='flutterwave_overage',
            reference=str(transaction_id),
            notes=f"Seat overage payment — tx_ref={tx_ref}",
            payment_date=txn_created_at,
        )

        # Clear the overdue hold so the user can add clients again
        if hasattr(models, 'mark_overage_paid'):
            models.mark_overage_paid(current_user.id, reference=str(transaction_id))

        models.track_event(
            'overage_paid', user_id=current_user.id,
            metadata={'amount': paid_amount, 'tx_ref': tx_ref},
        )
        current_app.logger.info(
            f"Overage payment OK: user={current_user.id} amount={paid_amount} txn={transaction_id}"
        )
        flash("Overage invoice paid — thank you! You can now add more chatbots.", 'success')
        return redirect(url_for('auth.dashboard'))  # FW-010 fix

    # ── Standard plan-upgrade payment ─────────────────────────────────────────
    plan  = None
    cycle = 'monthly'
    try:
        plan  = parts[1].lower() if len(parts) > 1 else None
        if len(parts) > 2 and parts[2] in ('monthly', 'annual'):
            cycle = parts[2].lower()
    except Exception:
        pass

    if plan not in PLAN_PRICES_FLW:
        current_app.logger.error(
            f"Flutterwave: unknown plan in tx_ref '{tx_ref}'"
        )
        flash("Could not determine plan. Contact support@lumvi.net.", 'error')
        return redirect(url_for('billing.upgrade_page'))

    is_annual    = (cycle == 'annual')
    expected_amt = PLAN_PRICES_FLW[plan]['annual'] if is_annual else PLAN_PRICES_FLW[plan]['monthly']

    if paid_amount < expected_amt:
        current_app.logger.error(
            f"Flutterwave amount mismatch: expected {expected_amt}, "
            f"got {paid_amount} {paid_currency} (tx {transaction_id})"
        )
        flash("Payment amount mismatch. Contact support@lumvi.net.", 'error')
        return redirect(url_for('billing.upgrade_page'))

    # FW-001: Duplicate check before subscription update
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            "SELECT id FROM payments WHERE reference = %s LIMIT 1",
            (str(transaction_id),)
        )
        already_processed = cursor.fetchone()
        cursor.close()
        conn.close()

        if already_processed:
            current_app.logger.warning(
                f"Flutterwave callback: duplicate txn {transaction_id} "
                f"for user {current_user.id}"
            )
            flash("This payment has already been processed.", 'info')
            return redirect(url_for('auth.dashboard'))  # FW-010 fix
    except Exception as e:
        current_app.logger.error(f"Flutterwave duplicate check failed: {e}")
        # Continue — don't block the user

    models.update_user_subscription(
        user_id=current_user.id,
        plan_type=plan,
        billing_provider='flutterwave',
        subscription_id=str(transaction_id),
        is_annual=is_annual
    )
    models.record_payment(
        current_user.id, paid_amount, plan,
        provider='flutterwave',
        reference=str(transaction_id),
        notes=f"{'Annual' if is_annual else 'Monthly'} — {cycle}",
        payment_date=txn_created_at
    )
    models.track_event(
        'plan_upgrade', user_id=current_user.id,
        metadata={
            'plan': plan, 'provider': 'flutterwave',
            'cycle': cycle, 'amount': paid_amount, 'tx_ref': tx_ref,
        }
    )

    current_app.logger.info(
        f"Flutterwave upgrade OK: user={current_user.id} "
        f"plan={plan} cycle={cycle} txn={transaction_id}"
    )
    flash(
        f"Payment successful! You are now on the "
        f"{plan.capitalize()} plan ({cycle} billing).",
        'success'
    )
    return redirect(url_for('auth.dashboard'))  # FW-010 fix
    tx_ref         = request.args.get('tx_ref', '')
    transaction_id = request.args.get('transaction_id', '')

    if status != 'successful':
        flash("Payment was not completed. Please try again.", 'error')
        return redirect(url_for('billing.upgrade_page'))

    if not transaction_id:
        flash("Invalid payment reference. Contact support@lumvi.net.", 'error')
        return redirect(url_for('billing.upgrade_page'))

    flw_secret = os.environ.get('FLW_SECRET_KEY', '')
    if not flw_secret:
        current_app.logger.error("FLW_SECRET_KEY not set")
        flash("Payment configuration error. Contact support@lumvi.net.", 'error')
        return redirect(url_for('billing.upgrade_page'))

    # Verify with Flutterwave API — retry up to 3 times with exponential backoff (FW-008)
    flw_data   = None
    verify_url = f"https://api.flutterwave.com/v3/transactions/{transaction_id}/verify"
    headers    = {"Authorization": f"Bearer {flw_secret}"}

    for attempt in range(3):
        try:
            resp = _requests.get(verify_url, headers=headers, timeout=15)
            resp.raise_for_status()
            flw_data = resp.json()
            break
        except Exception as e:
            current_app.logger.warning(
                f"Flutterwave verify attempt {attempt + 1}/3 failed: {e}"
            )
            if attempt == 2:
                current_app.logger.error(
                    f"Flutterwave verify error after 3 attempts: {e}"
                )
                flash("Could not verify payment. Contact support@lumvi.net.", 'error')
                return redirect(url_for('billing.upgrade_page'))
            time.sleep(2 ** attempt)  # 1s, 2s backoff

    if not flw_data or flw_data.get('status') != 'success':
        flash("Payment verification failed. Contact support@lumvi.net.", 'error')
        return redirect(url_for('billing.upgrade_page'))

    txn = flw_data.get('data', {})
    if txn.get('status') != 'successful':
        flash("Payment not successful. Please try again.", 'error')
        return redirect(url_for('billing.upgrade_page'))

@billing_bp.route('/payment/flutterwave/webhook', methods=['POST'])
def flutterwave_webhook():
    """
    Flutterwave server-to-server webhook (backup).
    Set webhook URL in Flutterwave dashboard:
        https://lumvi.net/payment/flutterwave/webhook
    Set FLW_WEBHOOK_HASH env var to the secret hash from the dashboard.

    Fixes:
    - FW-003: Validate user_id exists before upgrade
    - FW-005: Enforce webhook auth (required at startup + re-checked here)
    - FW-006: Duplicate check before recording payment
    - FW-007: Log extracted tx_ref fields before validation
    """
    flw_hash     = os.environ.get('FLW_WEBHOOK_HASH', '')
    request_hash = request.headers.get('verif-hash', '')

    if not flw_hash or request_hash != flw_hash:
        current_app.logger.warning(
            f"Flutterwave webhook: invalid hash (got '{request_hash[:20]}...')"
        )
        return jsonify({'error': 'Unauthorized'}), 401

    payload = request.json or {}
    event   = payload.get('event', '')

    if event != 'charge.completed':
        return jsonify({'status': 'ignored'}), 200

    data           = payload.get('data', {})
    txn_status     = data.get('status', '')
    tx_ref         = data.get('tx_ref', '')
    txn_id         = str(data.get('id', ''))
    amount         = float(data.get('amount', 0))
    currency       = data.get('currency', 'USD')
    txn_created_at = data.get('created_at')

    if txn_status != 'successful':
        return jsonify({'status': 'not successful'}), 200

    # Parse plan + cycle + user_id from tx_ref
    # Format: lumvi_{plan}_{cycle}_{user_id}_{ts}  OR  legacy lumvi_{plan}_{user_id}_{ts}
    # Overage:  lumvi_overage_{user_id}_{ts}
    parts   = tx_ref.split('_')
    plan    = None
    cycle   = 'monthly'
    user_id = None

    try:
        plan = parts[1].lower() if len(parts) >= 2 else None
    except (IndexError, ValueError) as e:
        current_app.logger.error(f"Flutterwave webhook: bad tx_ref '{tx_ref}' — {e}")
        return jsonify({'status': 'bad tx_ref'}), 200

    # ── Overage payment path ──────────────────────────────────────────────────
    if plan == 'overage':
        # lumvi_overage_{user_id}_{ts}
        try:
            user_id = int(parts[2]) if len(parts) >= 3 else None
        except (IndexError, ValueError):
            user_id = None

        if not user_id:
            current_app.logger.error(
                f"Flutterwave webhook (overage): no user_id in tx_ref '{tx_ref}'"
            )
            return jsonify({'status': 'bad tx_ref'}), 200

        # Duplicate check
        try:
            conn, cursor = models.get_db()
            cursor.execute(
                "SELECT id FROM payments WHERE reference = %s LIMIT 1", (txn_id,)
            )
            already = cursor.fetchone()
            cursor.close(); conn.close()
            if already:
                current_app.logger.info(f"Webhook overage: already processed txn {txn_id}")
                return jsonify({'status': 'already processed'}), 200
        except Exception as e:
            current_app.logger.error(f"Webhook overage duplicate check failed: {e}")

        user = models.get_user_by_id(user_id)
        if not user:
            current_app.logger.error(f"Webhook overage: user {user_id} not found (txn {txn_id})")
            return jsonify({'status': 'user not found'}), 200

        models.record_payment(
            user_id, amount, 'agency',
            provider='flutterwave_overage',
            reference=txn_id,
            notes=f"Seat overage webhook — tx_ref={tx_ref}",
            payment_date=txn_created_at,
        )

        if hasattr(models, 'mark_overage_paid'):
            models.mark_overage_paid(user_id, reference=txn_id)

        models.track_event(
            'overage_paid', user_id=user_id,
            metadata={'amount': amount, 'tx_ref': tx_ref, 'source': 'webhook'},
        )
        current_app.logger.info(
            f"Webhook overage payment OK: user={user_id} amount={amount} txn={txn_id}"
        )
        return jsonify({'status': 'ok'}), 200

    # ── Standard plan-upgrade path ────────────────────────────────────────────
    try:
        if len(parts) > 2 and parts[2] in ('monthly', 'annual'):
            cycle   = parts[2].lower()
            user_id = int(parts[3]) if len(parts) >= 4 else None
        else:
            cycle   = 'monthly'
            user_id = int(parts[2]) if len(parts) >= 3 else None
    except (IndexError, ValueError) as e:
        current_app.logger.error(
            f"Flutterwave webhook: bad tx_ref '{tx_ref}' — {e}"
        )
        return jsonify({'status': 'bad tx_ref'}), 200

    # FW-007: Log extracted fields before validation
    current_app.logger.info(
        f"Flutterwave webhook parsing: plan={plan} cycle={cycle} "
        f"user_id={user_id} txn_id={txn_id}"
    )

    if plan not in PLAN_PRICES_FLW:
        current_app.logger.error(
            f"Flutterwave webhook: unknown plan '{plan}' (tx_ref='{tx_ref}')"
        )
        return jsonify({'status': 'unknown plan'}), 200

    if not user_id:
        current_app.logger.error(
            f"Flutterwave webhook: no user_id in tx_ref '{tx_ref}'"
        )
        return jsonify({'status': 'no user_id'}), 200

    # FW-003: Validate user exists before upgrading
    user = models.get_user_by_id(user_id)
    if not user:
        current_app.logger.error(
            f"Flutterwave webhook: user {user_id} does not exist (txn {txn_id})"
        )
        return jsonify({'status': 'user not found'}), 200

    is_annual    = (cycle == 'annual')
    expected_amt = PLAN_PRICES_FLW[plan]['annual'] if is_annual else PLAN_PRICES_FLW[plan]['monthly']

    if amount < expected_amt:
        current_app.logger.error(
            f"[Webhook] Amount mismatch for user={user_id} plan={plan}: "
            f"expected {expected_amt}, got {amount} {currency} tx_ref='{tx_ref}'"
        )
        return jsonify({'status': 'amount mismatch'}), 200

    # FW-006: Duplicate check before recording payment
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            "SELECT id FROM payments WHERE reference = %s LIMIT 1", (txn_id,)
        )
        already_processed = cursor.fetchone()
        cursor.close()
        conn.close()

        if already_processed:
            current_app.logger.info(
                f"Flutterwave webhook: already processed txn {txn_id}"
            )
            return jsonify({'status': 'already processed'}), 200
    except Exception as e:
        current_app.logger.error(
            f"Flutterwave webhook duplicate check failed: {e}"
        )
        return jsonify({'status': 'db error'}), 200

    models.update_user_subscription(
        user_id=user_id,
        plan_type=plan,
        billing_provider='flutterwave',
        subscription_id=txn_id,
        is_annual=is_annual
    )
    models.record_payment(
        user_id, amount, plan,
        provider='flutterwave',
        reference=txn_id,
        notes=f"{'Annual' if is_annual else 'Monthly'} webhook",
        payment_date=txn_created_at
    )
    models.track_event(
        'plan_upgrade', user_id=user_id,
        metadata={
            'plan': plan, 'provider': 'flutterwave_webhook',
            'cycle': cycle, 'amount': amount, 'tx_ref': tx_ref,
        }
    )

    current_app.logger.info(
        f"Flutterwave webhook upgrade OK: user={user_id} "
        f"plan={plan} cycle={cycle} txn={txn_id}"
    )
    return jsonify({'status': 'ok'}), 200


@billing_bp.route('/subscription/cancel', methods=['GET', 'POST'])
@login_required
def cancel_subscription():
    """Allow users to cancel their subscription at the end of the current period."""
    if request.method == 'POST':
        success = models.cancel_user_subscription(current_user.id)

        if success:
            user = models.get_user_by_id(current_user.id)

            # Notify Flutterwave to stop future charges
            if (user and user.get('subscription_id')
                    and user.get('billing_provider') == 'flutterwave'):
                try:
                    flw_secret = os.environ.get('FLW_SECRET_KEY')
                    if flw_secret:
                        cancel_url = (
                            f"https://api.flutterwave.com/v3/subscriptions"
                            f"/{user['subscription_id']}/cancel"
                        )
                        _requests.put(
                            cancel_url,
                            headers={"Authorization": f"Bearer {flw_secret}"},
                            timeout=10
                        )
                except Exception as _e:
                    current_app.logger.warning(
                        f"Flutterwave cancel API call failed: {_e}"
                    )

            # Notify PayPal to stop future charges
            elif (user and user.get('subscription_id')
                      and user.get('billing_provider') == 'paypal'):
                try:
                    paypal_client_id     = os.environ.get('PAYPAL_CLIENT_ID', '')
                    paypal_client_secret = os.environ.get('PAYPAL_CLIENT_SECRET', '')
                    paypal_mode          = os.environ.get('PAYPAL_MODE', 'sandbox')
                    paypal_base = (
                        'https://api-m.paypal.com' if paypal_mode == 'live'
                        else 'https://api-m.sandbox.paypal.com'
                    )

                    credentials = base64.b64encode(
                        f"{paypal_client_id}:{paypal_client_secret}".encode()
                    ).decode()
                    token_resp = _requests.post(
                        f"{paypal_base}/v1/oauth2/token",
                        headers={
                            "Authorization": f"Basic {credentials}",
                            "Content-Type":  "application/x-www-form-urlencoded",
                        },
                        data="grant_type=client_credentials",
                        timeout=10
                    )
                    access_token = token_resp.json().get('access_token')

                    if access_token:
                        _requests.post(
                            f"{paypal_base}/v1/billing/subscriptions"
                            f"/{user['subscription_id']}/cancel",
                            headers={
                                "Authorization": f"Bearer {access_token}",
                                "Content-Type":  "application/json",
                            },
                            json={"reason": "Cancelled by user via Lumvi dashboard"},
                            timeout=10
                        )
                        current_app.logger.info(
                            f"[Cancel] PayPal subscription cancelled "
                            f"for user {current_user.id}"
                        )
                except Exception as _e:
                    current_app.logger.warning(
                        f"PayPal cancel API call failed: {_e}"
                    )

            models.track_event('subscription_cancelled', user_id=current_user.id)

            # Send cancellation confirmation email
            try:
                _user_fresh = models.get_user_by_id(current_user.id)
                _sub_info   = (
                    _get_subscription_status(_user_fresh)
                    if _user_fresh and _get_subscription_status
                    else {}
                )
                _expires     = _sub_info.get('expires_at')
                _access_ends = (
                    _expires.strftime('%B %d, %Y')
                    if _expires and hasattr(_expires, 'strftime')
                    else 'the end of your current billing period'
                )
                if _mail:
                    _cancel_msg = Message(
                        subject="Your Lumvi subscription has been cancelled",
                        sender="Lumvi <support@lumvi.net>",
                        recipients=[current_user.email],
                        html=f"""
                        <div style="font-family:'DM Sans',sans-serif;max-width:520px;margin:0 auto;
                                    background:#F7F4EF;padding:36px;border-radius:16px;">
                          <h2 style="font-size:20px;font-weight:700;color:#1C1917;margin-bottom:8px;">
                            Subscription Cancelled</h2>
                          <p style="color:#57534E;font-size:14px;line-height:1.6;margin-bottom:16px;">
                            Your Lumvi subscription has been cancelled. You will retain full access
                            until <strong>{_access_ends}</strong>. After that, your account will
                            revert to the free plan automatically — no further charges will be made.</p>
                          <p style="color:#57534E;font-size:14px;line-height:1.6;margin-bottom:24px;">
                            Changed your mind? You can resubscribe at any time from your
                            <a href="https://lumvi.net/upgrade" style="color:#B8924A;">upgrade page</a>.
                            Your data and clients will be waiting for you.</p>
                          <p style="color:#A8A29E;font-size:12px;">
                            Questions? Contact
                            <a href="mailto:support@lumvi.net" style="color:#B8924A;">support@lumvi.net</a>.
                          </p>
                        </div>"""
                    )
                    _mail.send(_cancel_msg)
            except Exception as _mail_err:
                current_app.logger.warning(
                    f"[Cancel] confirmation email failed: {_mail_err}"
                )

            flash(
                "Your subscription has been cancelled. You will retain access "
                "until the end of your current billing period.",
                'success'
            )
            return redirect(url_for('auth.dashboard'))
        else:
            flash(
                "Could not cancel subscription. Please contact support@lumvi.net.",
                'error'
            )
            return redirect(url_for('billing.cancel_subscription'))

    # GET — show confirmation page
    user     = models.get_user_by_id(current_user.id)
    sub_info = (
        _get_subscription_status(user)
        if user and _get_subscription_status
        else {'status': 'free'}
    )
    return render_template('cancel_subscription.html', user=user, sub_status=sub_info)


@billing_bp.route('/become-affiliate', methods=['GET', 'POST'])
@login_required
def become_affiliate():
    existing = models.get_affiliate_by_user_id(current_user.id)
    if existing:
        return redirect(url_for('billing.affiliate_dashboard'))

    if request.method == 'POST':
        payment_email = request.form.get('payment_email')
        affiliate     = models.create_affiliate(current_user.id, payment_email)
        if affiliate:
            return redirect(url_for('billing.affiliate_dashboard'))
        else:
            return "Error creating affiliate account", 500

    return render_template('become-affiliate.html')


@billing_bp.route('/affiliate-dashboard')
@login_required
def affiliate_dashboard():
    affiliate = models.get_affiliate_by_user_id(current_user.id)
    if not affiliate:
        return redirect(url_for('billing.become_affiliate'))

    stats       = models.get_affiliate_stats(affiliate['id'])
    commissions = models.get_affiliate_commissions(affiliate['id'])
    return render_template(
        'affiliate-dashboard.html', stats=stats, commissions=commissions
    )
