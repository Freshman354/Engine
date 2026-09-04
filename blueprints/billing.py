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
  - REMOVED: /agency/seat-prices, /agency/buy-seat, and the seat/
    overage tx_ref branches in flutterwave_callback/flutterwave_webhook
    — obsolete agency-business-model pricing logic, see the billing
    cleanup report for what this touches elsewhere (models/billing.py,
    cron.py)

Routes
------
  GET         /upgrade                                upgrade_page
  GET         /billing/shopify/return                 shopify_pricing_return
  GET         /payment/flutterwave/callback           flutterwave_callback
  POST        /payment/flutterwave/webhook            flutterwave_webhook
  GET/POST    /subscription/cancel                    cancel_subscription
  GET/POST    /become-affiliate                       become_affiliate
  GET         /affiliate-dashboard                    affiliate_dashboard

Registration in app.py:
  from blueprints.billing import billing_bp, init_billing, PLAN_PRICES_FLW
  init_billing(mail=mail, get_subscription_status=get_subscription_status, User=User)
  app.register_blueprint(billing_bp)

Shopify App Pricing (added — see shopify_billing.py for the Partner API
client this blueprint calls into):
  - upgrade_page() now detects which billing rail a user is on
    (_resolve_billing_rail()) and redirects Shopify-rail users straight to
    Shopify's own hosted plan-selection page instead of rendering
    upgrade.html — Flutterwave users' rendering of that template is
    completely unchanged.
  - shopify_pricing_return() is the welcome-link target configured for all
    three plans in the Partner Dashboard; confirms subscription state via
    the Partner API and writes it through the existing
    models.update_user_subscription(billing_provider='shopify_app_pricing').
  - Subscription-change webhooks don't exist for Shopify App Pricing (none
    since April 28 2026) — see app.py's reconcile_shopify_subscriptions()
    (daily cron, blueprints/cron.py) for the safety net that catches
    cancellations/freezes done entirely inside Shopify admin.
"""

import base64
import os
import time

import requests as _requests
from flask import (Blueprint, flash, jsonify, redirect,
                   render_template, request, current_app, url_for, session)
from flask_login import current_user, login_required, login_user
from flask_mail import Message

import models
import webhooks as _webhooks
import shopify_billing
from bot_protection import get_client_ip

# ── Blueprint ────────────────────────────────────────────────────────────────

billing_bp = Blueprint('billing', __name__)

# Injected dependencies — populated by init_billing() before first request.
_mail                   = None
_get_subscription_status = None
_User                    = None  # flask-login UserMixin wrapper — same class
                                  # app.py's connect_shopify_callback uses to
                                  # log a merchant in with no password step.
                                  # Injected rather than imported directly to
                                  # avoid a circular import (app.py imports
                                  # this blueprint).


def init_billing(mail, get_subscription_status, User):
    """
    Called once in app.py after all shared objects are ready.
    Must be called before the first request reaches this blueprint.
    """
    global _mail, _get_subscription_status, _User
    _mail                    = mail
    _get_subscription_status = get_subscription_status
    _User                    = User


def _resolve_billing_rail(user) -> str:
    """
    Returns 'shopify_app_pricing' or 'flutterwave' — which billing rail a
    given user's upgrade/plan-change click should go through.

    - If the user already has an explicit billing_provider on record (not
      the 'manual' default every user starts with — i.e. they've
      subscribed via one rail before, whether or not they're currently
      back on the free plan after a cancellation), keep using that same
      rail. Never re-derive once a real provider is on record: a merchant
      who later connects/disconnects a Shopify integration shouldn't be
      bounced to a different checkout for a plan they already pay for
      elsewhere.
    - Otherwise (never subscribed via either rail yet — still on
      billing_provider's 'manual' default) derive it from whether any of
      the user's clients has an active Shopify integration. A Shopify App
      Store headless install already lands here with no billing_provider
      set (see models/users.py::create_or_link_shopify_user) — this is
      the only place that fact feeds into billing routing; everywhere
      else in the app, a Shopify-provisioned free user and a
      directly-signed-up free user are identical.
    """
    provider = (getattr(user, 'billing_provider', None) or 'manual').lower()
    if provider in ('shopify_app_pricing', 'flutterwave'):
        return provider

    for client in models.get_user_clients(user.id):
        if _webhooks.get_integration(client['client_id'], 'shopify'):
            return 'shopify_app_pricing'
    return 'flutterwave'


# ── Pricing table ─────────────────────────────────────────────────────────────
# Kept here rather than app.py — only billing routes reference it.
# Import PLAN_PRICES_FLW in app.py if any non-blueprint code needs it.

PLAN_PRICES_FLW = {
    # ── Grandfathered — existing subscribers only, no longer sold ──────────
    # These four keys are NOT on the new /upgrade page and must not be
    # removed: renewal webhooks for existing subscribers keep sending the
    # SAME tx_ref (and therefore the same plan key) every billing cycle
    # (native Flutterwave recurring charges reuse the original tx_ref).
    # Removing a key here would make flutterwave_webhook reject a paying
    # customer's renewal as "unknown plan" and silently fail to extend
    # their subscription_expires_at.
    'solo':    {'monthly': 19.00,  'annual': 190.00},
    'starter': {'monthly': 49.00,  'annual': 490.00},
    'pro':     {'monthly': 99.00,  'annual': 990.00},
    'growth':  {'monthly': 149.00, 'annual': 1490.00},
    # 'agency' deliberately removed — no existing subscribers remain (all
    # migrated to ai_scale by migrate_ai_employee_plan_rename()), and it's
    # not sold anymore either. If it's ever needed again for a manual
    # grandfather case, re-add it here first.

    # ── "AI Employee for Shopify & WooCommerce" — sold on /upgrade ─────────
    # Annual total = monthly * 10 (2 months free), matching the page's JS
    # exactly — this dict IS the server-side enforcement of that formula,
    # used to validate the amount actually paid in both flutterwave_callback
    # and flutterwave_webhook.
    'ai_starter': {'monthly': 29.00,  'annual': 290.00},
    'ai_growth':  {'monthly': 79.00,  'annual': 790.00},
    'ai_scale':   {'monthly': 199.00, 'annual': 1990.00},
}


# ── Routes ───────────────────────────────────────────────────────────────────

@billing_bp.route('/upgrade')
@login_required
def upgrade_page():
    # Rail detection — the actual gap Phase 0 surfaced: this route used to
    # be unconditionally Flutterwave, which would show Shopify-App-Store
    # merchants a non-compliant checkout. A Shopify-App-Pricing user gets
    # sent straight to Shopify's OWN hosted plan-selection page (Shopify
    # hosts it — Lumvi has no template of its own to render for it, and
    # none is needed); a Flutterwave user's experience below is completely
    # unchanged, byte-for-byte, including for existing grandfathered
    # solo/starter/pro/growth subscribers.
    if _resolve_billing_rail(current_user) == 'shopify_app_pricing':
        integration = None
        for client in models.get_user_clients(current_user.id):
            integration = _webhooks.get_integration(client['client_id'], 'shopify')
            if integration:
                break
        shop_domain = (integration or {}).get('platform_config', {}).get('shop_domain', '')
        redirect_url = shopify_billing.pricing_plans_url(shop_domain)
        if redirect_url:
            return redirect(redirect_url)
        # Fails safe rather than 500s or stranding the merchant: falls
        # through to the Flutterwave page below so there's still SOME
        # upgrade path, and logs loudly since this means either
        # SHOPIFY_APP_STORE_HANDLE isn't configured or this user's Shopify
        # integration is missing/inactive despite being on this rail —
        # both need investigating, neither should be silent.
        current_app.logger.error(
            f'[Billing] user={current_user.id} is on the shopify_app_pricing rail '
            f'but pricing_plans_url() returned None (missing SHOPIFY_APP_STORE_HANDLE '
            f'or no active shop_domain) — falling back to the Flutterwave page so the '
            f'merchant is not stuck; this should be investigated.'
        )

    def _parse_plan_ids(env_var_name):
        """Parse 'ai_starter:<id>,ai_growth:<id>,ai_scale:<id>' into
        {'ai_starter': '<id>', ...} with env validation logging.
        Only the 3 new self-serve tiers get recurring Payment Plan IDs here —
        grandfathered solo/starter/pro/growth subscribers keep whatever plan
        ID they were already on; this route no longer sells to them."""
        raw = os.environ.get(env_var_name, '')
        if not raw:
            current_app.logger.error(
                f"[Billing] ENV VAR MISSING: {env_var_name} is not set — "
                f"/upgrade will fail to render"
            )
            return {}
        result = {}
        plans = ['ai_starter', 'ai_growth', 'ai_scale']
        for pair in raw.split(','):
            pair = pair.strip()
            if ':' not in pair:
                current_app.logger.warning(
                    f"[Billing] {env_var_name}: malformed entry '{pair}' (expected plan:id)"
                )
                continue
            plan, plan_id = pair.split(':', 1)
            plan = plan.strip().lower()
            if plan not in plans:
                current_app.logger.warning(
                    f"[Billing] {env_var_name}: unknown plan '{plan}' in entry '{pair}'"
                )
            else:
                result[plan] = plan_id.strip()
        missing = [p for p in plans if p not in result]
        if missing:
            current_app.logger.error(
                f"[Billing] {env_var_name}: missing plan IDs for: {missing}"
            )
        else:
            current_app.logger.info(
                f"[Billing] {env_var_name}: all {len(plans)} plan IDs present — {list(result.keys())}"
            )
        return result

    return render_template(
        'upgrade.html',
        user=current_user,
        flw_public_key=os.environ.get('FLW_PUBLIC_KEY', ''),
        FLW_PLAN_IDS_MONTHLY=_parse_plan_ids('FLW_PLAN_IDS_MONTHLY'),
        FLW_PLAN_IDS_ANNUAL=_parse_plan_ids('FLW_PLAN_IDS_ANNUAL'),
    )


@billing_bp.route('/billing/shopify/return')
def shopify_pricing_return():
    """
    Shopify redirects here after a merchant approves or changes a plan on
    Shopify's hosted plan-selection page (the 'welcome link', configured
    per-plan in the Partner Dashboard, same URL for all three tiers).

    NOT @login_required: per Shopify's current docs, for an app with no
    embedded App Home landing page (Lumvi's case — the dashboard is a
    standalone web app, not a Shopify Admin iframe), both `plan_handle`
    and `shop` are appended to this URL regardless of session state. A
    merchant who changes their plan from inside Shopify admin itself,
    days later, on a different device than the one they installed from,
    may have no Lumvi session at all when they land here — this mirrors
    app.py's connect_shopify_callback, which resolves the same way for
    its own headless-install branch via the same
    get_client_id_by_shopify_shop() lookup, and logs the merchant in the
    same way (no password step) rather than bouncing them to a login wall.

    plan_handle is used only to decide whether it's worth calling the
    Partner API at all — it is NOT trusted as the source of truth for
    what plan_type to set. Shopify's own guidance is to always confirm
    via activeSubscription after a redirect; this queries it every time.
    """
    shop        = (request.args.get('shop') or '').strip().lower()
    plan_handle = (request.args.get('plan_handle') or '').strip()

    if not shop:
        current_app.logger.error('[Shopify Billing] return handler hit with no shop param')
        return redirect(url_for('auth.login'))

    client_id = _webhooks.get_client_id_by_shopify_shop(shop)
    if not client_id:
        current_app.logger.error(f'[Shopify Billing] return handler: unknown shop={shop}')
        return redirect(url_for('auth.login'))

    owner_client = models.get_client_by_id(client_id)
    owner_user   = models.get_user_by_id(owner_client['user_id']) if owner_client else None
    if not owner_user:
        current_app.logger.error(
            f'[Shopify Billing] return handler: shop={shop} maps to client={client_id} '
            f'but its owning user was not found'
        )
        return redirect(url_for('auth.login'))

    integration = _webhooks.get_integration(client_id, 'shopify')
    access_token = (integration or {}).get('platform_config', {}).get('access_token', '')
    if not access_token:
        current_app.logger.error(
            f'[Shopify Billing] return handler: no Shopify access_token on file for '
            f'client={client_id} shop={shop} — cannot resolve shop GID'
        )
        return redirect(url_for('auth.dashboard'))

    shop_gid = shopify_billing.fetch_shop_gid(shop, access_token)
    if not shop_gid:
        current_app.logger.error(f'[Shopify Billing] return handler: could not resolve '
                                  f'shop GID for shop={shop} client={client_id}')
        return redirect(url_for('auth.dashboard'))

    sub, sub_error = shopify_billing.get_active_subscription(shop_gid)
    if not sub:
        # Two different reasons land here (see get_active_subscription's
        # docstring): a real "no subscription" (merchant backed out of the
        # plan page, or hasn't picked one yet) vs. the Partner API call
        # itself failing. Neither is grounds to change plan_type — but
        # they're worth logging differently so a Partner API problem is
        # distinguishable from ordinary merchant behavior after the fact.
        if sub_error:
            current_app.logger.error(
                f'[Shopify Billing] return handler: could not confirm subscription for '
                f'shop={shop} client={client_id} plan_handle={plan_handle!r} '
                f'({sub_error}) — leaving plan_type unchanged'
            )
        else:
            current_app.logger.warning(
                f'[Shopify Billing] return handler: no active subscription found for '
                f'shop={shop} client={client_id} plan_handle={plan_handle!r} — leaving '
                f'plan_type unchanged'
            )
    else:
        plan_type = shopify_billing.plan_handle_to_plan_type(sub['plan_handle'])
        if not plan_type:
            current_app.logger.error(
                f'[Shopify Billing] return handler: unmapped plan_handle='
                f'{sub["plan_handle"]!r} for shop={shop} client={client_id} — check '
                f'SHOPIFY_APP_PRICING_PLAN_HANDLES'
            )
        else:
            models.update_user_subscription(
                user_id=owner_user['id'],
                plan_type=plan_type,
                billing_provider='shopify_app_pricing',
                subscription_id=shop_gid,
                is_annual=(sub['billing_period'] == 'ANNUAL'),
            )
            flash(f'You are now on the {plan_type.replace("ai_", "").title()} plan.', 'success')

    # Log the merchant in regardless of outcome above — same no-password
    # mechanism app.py's connect_shopify_callback uses for its headless
    # branches, including the _user_cache key that session relies on
    # elsewhere. Re-fetch in case update_user_subscription just changed
    # this row.
    current_owner_user = models.get_user_by_id(owner_user['id']) or owner_user
    login_user(_User(current_owner_user), remember=True)
    session.permanent = True
    session['_user_cache'] = dict(current_owner_user)

    return redirect(url_for('auth.dashboard'))


@billing_bp.route('/payment/flutterwave/callback')
@login_required
def flutterwave_callback():
    """
    Flutterwave redirects here after payment (plan upgrades).

    tx_ref format:  lumvi_{plan}_{cycle}_{user_id}_{timestamp}

    Fixes:
    - FW-001: Duplicate check before subscription update
    - FW-002: Remove USD-only currency guard on amount validation
    - FW-004: Validate via Flutterwave signature
    - FW-008: Retry logic for verify API
    - FW-010: Fix url_for('dashboard') → url_for('auth.dashboard')

    REMOVED: seat-purchase and seat-overage tx_ref handling (lumvi_seat_*,
    lumvi_overage_*). Both belonged to the agency/white-label business
    model — no plan can grant more than 1 connected store anymore, so
    there's nothing left to buy extra seats or overage for. See the
    accompanying billing-cleanup report for what else this touches
    (models/billing.py's seat functions, cron.py's seat cron jobs).
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

    parts = tx_ref.split('_')

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
        },
        ip_address=get_client_ip(),
        user_agent=request.headers.get('User-Agent', ''),
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

    # ── Detect tx_ref type ────────────────────────────────────────────────────
    parts   = tx_ref.split('_')
    plan    = None
    cycle   = 'monthly'
    user_id = None

    try:
        plan = parts[1].lower() if len(parts) >= 2 else None
    except (IndexError, ValueError) as e:
        current_app.logger.error(f"Flutterwave webhook: bad tx_ref '{tx_ref}' — {e}")
        return jsonify({'status': 'bad tx_ref'}), 200

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


# =====================================================================
# AGENCY SEAT SUBSCRIPTION ROUTES — REMOVED
# Both routes gated on current_user.plan_type == 'agency', which can
# never be true again (migrate_ai_employee_plan_rename() moved every
# 'agency' account to 'ai_scale', and no plan grants more than 1 store
# anymore — there's nothing left to buy seats for). Removed rather than
# left in place: they were already unreachable (always 403) before this
# edit, this just removes the dead code along with the dead data path.
# =====================================================================


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

            models.track_event('subscription_cancelled', user_id=current_user.id,
                               ip_address=get_client_ip(),
                               user_agent=request.headers.get('User-Agent', ''))

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