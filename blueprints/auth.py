"""
blueprints/auth.py
------------------
Authentication, account management, dashboard, onboarding,
and client creation routes.

Extracted from app.py. All behaviour is identical to the original;
nothing has been changed except:
  - Route registration: Blueprint vs app
  - app.logger → current_app.logger
  - Inline imports promoted to module level
  - Dependencies injected at registration time via init_auth()

Routes
------
  GET/POST    /signup                         signup
  GET/POST    /forgot-password                forgot_password
  GET/POST    /reset-password/<token>         reset_password
  GET         /auth/google                    google_login
  GET         /auth/google/callback           google_callback
  GET/POST    /login                          login
  GET         /logout                         logout
  GET         /dashboard                      dashboard
  GET         /onboarding                     onboarding
  POST        /api/onboarding/complete        onboarding_complete
  POST        /api/onboarding/skip            onboarding_skip
  POST        /create-client                  create_client

Registration in app.py:
  from blueprints.auth import auth_bp, init_auth
  init_auth(
      mail=mail,
      google_oauth=google_oauth,
      plan_limits=PLAN_LIMITS,
      valid_verticals=VALID_VERTICALS,
      get_subscription_status=get_subscription_status,
      send_welcome_email=send_welcome_email,
      agency_included_clients=AGENCY_INCLUDED_CLIENTS,
      agency_seat_price=AGENCY_SEAT_PRICE,
      User=User,
  )
  app.register_blueprint(auth_bp)
"""

import json
import secrets
from datetime import datetime, timedelta

from flask import (Blueprint, flash, jsonify, redirect,
                   render_template, request, current_app, session, url_for)
from flask_login import (current_user, login_required,
                         login_user, logout_user)
from flask_mail import Message

import models

# ── Blueprint ────────────────────────────────────────────────────────────────

auth_bp = Blueprint('auth', __name__)

# Injected dependencies — populated by init_auth() before first request.
_mail                    = None
_google_oauth            = None
_plan_limits             = None
_valid_verticals         = None
_get_subscription_status = None
_send_welcome_email      = None
_agency_included_clients = None
_agency_seat_price       = None
_User                    = None


def init_auth(mail, google_oauth, plan_limits, valid_verticals,
              get_subscription_status, send_welcome_email,
              agency_included_clients, agency_seat_price, User):
    """
    Called once in app.py after all shared objects are ready.
    Must be called before the first request reaches this blueprint.
    """
    global _mail, _google_oauth, _plan_limits, _valid_verticals, \
            _get_subscription_status, _send_welcome_email, \
            _agency_included_clients, _agency_seat_price, _User
    _mail                    = mail
    _google_oauth            = google_oauth
    _plan_limits             = plan_limits
    _valid_verticals         = valid_verticals
    _get_subscription_status = get_subscription_status
    _send_welcome_email      = send_welcome_email
    _agency_included_clients = agency_included_clients
    _agency_seat_price       = agency_seat_price
    _User                    = User


# ── Auth routes ───────────────────────────────────────────────────────────────

@auth_bp.route('/signup', methods=['GET', 'POST'])
def signup():
    if current_user.is_authenticated:
        return redirect(url_for('auth.dashboard'))

    referral_code = request.args.get('ref')
    plan_param    = request.args.get('plan', '').lower()

    if request.method == 'POST':
        email            = request.form.get('email', '').strip().lower()
        password         = request.form.get('password', '')
        confirm_password = request.form.get('confirm_password', '')
        plan_from_form   = (
            request.form.get('plan_param') or
            request.form.get('plan_select') or
            request.args.get('plan') or
            'free'
        ).lower().strip()

        PAID_PLANS  = ('solo', 'starter', 'pro', 'growth', 'agency', 'enterprise')
        valid_plans = ('free',) + PAID_PLANS
        if plan_from_form not in valid_plans:
            plan_from_form = 'free'

        if password != confirm_password:
            return render_template('signup.html', error='Passwords do not match',
                                   referral_code=referral_code, plan_param=plan_from_form)
        if len(password) < 6:
            return render_template('signup.html', error='Password must be at least 6 characters',
                                   referral_code=referral_code, plan_param=plan_from_form)

        intended_plan = plan_from_form
        user_id       = models.create_user(email, password, 'free')

        if user_id is None:
            return render_template('signup.html',
                                   error='An account with that email already exists',
                                   referral_code=referral_code, plan_param=plan_from_form)

        if referral_code:
            affiliate = models.get_affiliate_by_code(referral_code)
            if affiliate:
                models.create_referral(affiliate['id'], user_id, referral_code)
                current_app.logger.info(f'Referral tracked: {referral_code} -> {email}')

        user_data      = models.get_user_by_id(user_id)
        user           = _User(user_data)
        session.permanent = True
        login_user(user, remember=True)
        models.track_event('signup', user_id=user_id,
                           metadata={'email': email, 'plan': intended_plan})
        if _send_welcome_email:
            _send_welcome_email(email)

        current_app.logger.info(f'New signup: {email} | intended plan: {intended_plan}')

        if intended_plan in PAID_PLANS:
            return redirect(url_for('billing.upgrade_page') + f'?plan={intended_plan}')
        return redirect(url_for('auth.dashboard'))

    return render_template('signup.html', referral_code=referral_code, plan_param=plan_param)


@auth_bp.route('/forgot-password', methods=['GET', 'POST'])
def forgot_password():
    if current_user.is_authenticated:
        return redirect(url_for('auth.dashboard'))

    if request.method == 'POST':
        email     = request.form.get('email', '').strip().lower()
        user_data = models.get_user_by_email(email)
        success_msg = "If that email is registered, you'll receive a reset link shortly."

        if user_data:
            token      = secrets.token_urlsafe(32)
            expires_at = datetime.utcnow() + timedelta(hours=1)
            models.save_password_reset_token(user_data['id'], token, expires_at)
            reset_url  = url_for('auth.reset_password', token=token, _external=True)
            try:
                msg = Message(
                    subject    = "Reset your Lumvi password",
                    sender     = "Lumvi <support@lumvi.net>",
                    recipients = [email],
                    html       = f"""
                    <div style="font-family:Inter,sans-serif;max-width:480px;margin:0 auto;background:#0f172a;color:#f8fafc;padding:40px;border-radius:16px;">
                        <div style="text-align:center;margin-bottom:32px;">
                            <div style="display:inline-block;background:linear-gradient(135deg,#6366f1,#a78bfa);border-radius:12px;padding:12px 20px;font-size:24px;font-weight:800;margin-bottom:12px;">⚡ Lumvi</div>
                        </div>
                        <h2 style="margin:0 0 12px;font-size:22px;font-weight:700;">Reset your password</h2>
                        <p style="color:#94a3b8;margin:0 0 28px;line-height:1.6;">
                            We received a request to reset the password for your Lumvi account.
                            Click the button below to set a new password. This link expires in <strong style="color:#f8fafc;">1 hour</strong>.
                        </p>
                        <a href="{reset_url}" style="display:block;text-align:center;background:linear-gradient(135deg,#6366f1,#7c3aed);color:white;text-decoration:none;padding:14px 28px;border-radius:10px;font-weight:700;font-size:15px;margin-bottom:24px;">
                            Reset My Password →
                        </a>
                        <p style="color:#475569;font-size:13px;margin:0;line-height:1.6;">
                            If you didn't request this, you can safely ignore this email — your password won't change.<br><br>
                            Or copy this link: <span style="color:#6366f1;">{reset_url}</span>
                        </p>
                    </div>"""
                )
                if _mail:
                    _mail.send(msg)
            except Exception as e:
                current_app.logger.error(
                    f"Password reset email failed: {type(e).__name__}: {e}"
                )

        return render_template('forgot_password.html', success=success_msg)

    return render_template('forgot_password.html')


@auth_bp.route('/reset-password/<token>', methods=['GET', 'POST'])
def reset_password(token):
    if current_user.is_authenticated:
        return redirect(url_for('auth.dashboard'))

    token_data = models.get_password_reset_token(token)
    if not token_data:
        return render_template('reset_password.html',
                               error="This reset link is invalid or has already been used.")

    expires_at = token_data['expires_at']
    if isinstance(expires_at, str):
        try:
            expires_at = datetime.strptime(expires_at, '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            expires_at = datetime.strptime(expires_at, '%Y-%m-%d %H:%M:%S')

    if datetime.utcnow() > expires_at:
        models.delete_password_reset_token(token)
        return render_template('reset_password.html',
                               error="This reset link has expired. Please request a new one.")

    if request.method == 'POST':
        password         = request.form.get('password', '')
        confirm_password = request.form.get('confirm_password', '')

        if len(password) < 6:
            return render_template('reset_password.html', token=token,
                                   error="Password must be at least 6 characters.")
        if password != confirm_password:
            return render_template('reset_password.html', token=token,
                                   error="Passwords don't match.")

        models.update_user_password(token_data['user_id'], password)
        models.delete_password_reset_token(token)
        models.track_event('password_reset', user_id=token_data['user_id'])
        return render_template('reset_password.html',
                               success="Password updated! You can now log in.")

    return render_template('reset_password.html', token=token)


@auth_bp.route('/auth/google')
def google_login():
    redirect_uri = url_for('auth.google_callback', _external=True)
    return _google_oauth.authorize_redirect(redirect_uri)


@auth_bp.route('/auth/google/callback')
def google_callback():
    try:
        token     = _google_oauth.authorize_access_token()
        userinfo  = token.get('userinfo') or _google_oauth.userinfo()
        google_id = userinfo['sub']
        email     = userinfo.get('email', '').lower().strip()

        if not email:
            flash('Google sign-in failed: no email returned.', 'error')
            return redirect(url_for('auth.login'))

        user_data = models.create_or_link_google_user(google_id, email)
        if not user_data:
            flash('Google sign-in failed. Please try again.', 'error')
            return redirect(url_for('auth.login'))

        is_new = user_data.get('plan_type') == 'free'
        user   = _User(user_data)
        login_user(user, remember=True)
        session.permanent = True
        session['_user_cache'] = dict(user_data)

        if is_new and _send_welcome_email:
            try:
                _send_welcome_email(email)
            except Exception:
                pass

        return redirect(url_for('auth.dashboard'))

    except Exception as e:
        current_app.logger.error(f'[Google OAuth] {e}')
        flash('Google sign-in failed. Please try again.', 'error')
        return redirect(url_for('auth.login'))


@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('auth.dashboard'))

    if request.method == 'POST':
        email     = request.form.get('email')
        password  = request.form.get('password')
        user_data = models.verify_user(email, password)

        if user_data:
            if not user_data.get('is_admin') and _get_subscription_status:
                sub = _get_subscription_status(user_data)
                if sub['status'] == 'expired':
                    models.downgrade_single_user(user_data['id'])
                    user_data['plan_type'] = 'free'
                    current_app.logger.info(
                        f"Auto-downgraded user {user_data['id']} to free on login"
                    )

            user = _User(user_data)
            session.pop('_user_cache', None)
            session.permanent = True
            login_user(user, remember=True)
            models.track_event('login', user_id=user_data['id'],
                               metadata={'email': email})

            fresh = models.get_user_by_id(user_data['id'])
            sub   = _get_subscription_status(fresh) if _get_subscription_status else {}
            session['sub_status'] = sub.get('status', 'free')

            return redirect(url_for('auth.dashboard'))

        return render_template('login.html', error='Invalid email or password')

    return render_template('login.html')


@auth_bp.route('/logout')
@login_required
def logout():
    session.pop('_user_cache', None)
    logout_user()
    return redirect(url_for('auth.login'))


# ── Dashboard & onboarding ────────────────────────────────────────────────────

@auth_bp.route('/dashboard')
@login_required
def dashboard():
    clients = models.get_user_clients(current_user.id)
    for client in clients:
        if client['branding_settings']:
            client['branding_settings'] = json.loads(client['branding_settings'])

    fresh_user = models.get_user_by_id(current_user.id)

    if fresh_user and not fresh_user.get('is_admin') and _get_subscription_status:
        sub = _get_subscription_status(fresh_user)
        if sub['status'] == 'expired':
            models.downgrade_single_user(fresh_user['id'])
            fresh_user = models.get_user_by_id(fresh_user['id'])
            current_app.logger.info(
                f"[Dashboard] Auto-downgraded user {fresh_user['id']} to free."
            )

    if (fresh_user and
            not fresh_user.get('onboarding_completed') and
            not fresh_user.get('is_admin') and
            len(clients) == 0):
        return redirect(url_for('auth.onboarding'))

    plan_type    = (fresh_user or {}).get('plan_type', current_user.plan_type)
    plan_limits  = _plan_limits.get(plan_type, _plan_limits['free'])
    client_limit = plan_limits['clients']
    client_count = len(clients)
    slots_display = 'Unlimited' if client_limit >= 999999 else str(client_limit)
    limit_reached = False if client_limit >= 999999 else client_count >= client_limit

    agency_extra_seats  = (max(0, client_count - _agency_included_clients)
                           if plan_type == 'agency' else 0)
    agency_overage_cost = agency_extra_seats * _agency_seat_price
    agency_overage_label = (
        f"+{agency_extra_seats} extra seat{'s' if agency_extra_seats != 1 else ''} "
        f"× ${_agency_seat_price:.0f}/mo = ${agency_overage_cost:.0f}/mo billed next cycle"
        if agency_extra_seats > 0 else ''
    )

    has_payment_method = bool(
        (fresh_user or {}).get('subscription_id') and
        (fresh_user or {}).get('subscription_status', 'active') in ('active', 'trialing')
    )

    sub_status = session.pop('sub_status', None)
    sub_info   = (_get_subscription_status(fresh_user)
                  if fresh_user and _get_subscription_status
                  else {'status': 'free'})

    return render_template(
        'dashboard_enterprise.html',
        user                     = current_user,
        clients                  = clients,
        plan_type                = plan_type,
        plan_limits              = plan_limits,
        client_count             = client_count,
        client_limit             = client_limit,
        slots_display            = slots_display,
        limit_reached            = limit_reached,
        sub_status               = sub_info['status'],
        sub_expires_at           = sub_info.get('expires_at'),
        sub_grace_ends_at        = sub_info.get('grace_ends_at'),
        agency_extra_seats       = agency_extra_seats,
        agency_overage_cost      = agency_overage_cost,
        agency_overage_label     = agency_overage_label,
        agency_included_clients  = _agency_included_clients,
        agency_seat_price        = _agency_seat_price,
        has_payment_method       = has_payment_method,
    )


@auth_bp.route('/onboarding')
@login_required
def onboarding():
    fresh_user = models.get_user_by_id(current_user.id)
    if fresh_user and fresh_user.get('onboarding_completed'):
        return redirect(url_for('auth.dashboard'))
    plan_type = (fresh_user or {}).get('plan_type', current_user.plan_type)
    return render_template('onboarding.html', user=current_user, plan_type=plan_type)


@auth_bp.route('/api/onboarding/complete', methods=['POST'])
@login_required
def onboarding_complete():
    try:
        models.mark_onboarding_complete(current_user.id)
        current_app.logger.info(
            f"[Onboarding] User {current_user.id} completed onboarding."
        )
        return jsonify({'success': True, 'redirect': url_for('auth.dashboard')})
    except Exception as e:
        current_app.logger.error(f"[Onboarding] complete error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@auth_bp.route('/api/onboarding/skip', methods=['POST'])
@login_required
def onboarding_skip():
    models.mark_onboarding_complete(current_user.id)
    return jsonify({'success': True, 'redirect': url_for('auth.dashboard')})


# ── Client creation ───────────────────────────────────────────────────────────

@auth_bp.route('/create-client', methods=['POST'])
@login_required
def create_client():
    try:
        company_name = request.form.get('company_name')
        vertical     = request.form.get('vertical', 'general')
        if _valid_verticals and vertical not in _valid_verticals:
            vertical = 'general'

        if not company_name:
            return jsonify({'success': False, 'error': 'Company name is required'}), 400

        user         = models.get_user_by_id(current_user.id)
        plan_type    = user['plan_type']
        plan_limit   = _plan_limits.get(plan_type, _plan_limits['free'])['clients']

        plan_upgrade_hints = {
            'free':    'Solo: 1 chatbot $19/mo | Starter: 3 chatbots | Pro: 10 chatbots | Agency: Unlimited',
            'solo':    'Starter: 3 chatbots | Pro: 10 chatbots | Agency: Unlimited',
            'starter': 'Pro: 10 chatbots | Agency: Unlimited',
            'pro':     'Agency: Unlimited chatbots at $299/mo',
        }
        upgrade_hint = plan_upgrade_hints.get(plan_type, 'Upgrade to add more chatbots')

        _lock_conn, _lock_cursor = models.get_db()
        try:
            _lock_cursor.execute("SELECT pg_advisory_lock(%s)", (current_user.id,))

            current_clients = models.get_user_clients(current_user.id)
            client_count    = len(current_clients)

            is_agency_overage = (
                plan_type == 'agency' and
                client_count >= _agency_included_clients
            )
            extra_seats  = (max(0, client_count - _agency_included_clients + 1)
                            if is_agency_overage else 0)
            overage_cost = extra_seats * _agency_seat_price

            if is_agency_overage:
                _user_data  = models.get_user_by_id(current_user.id)
                _sub_id     = (_user_data or {}).get('subscription_id')
                _sub_status = (_user_data or {}).get('subscription_status', 'active')
                if not _sub_id or _sub_status in ('cancelled', 'past_due'):
                    _lock_cursor.execute("SELECT pg_advisory_unlock(%s)", (current_user.id,))
                    _lock_cursor.close()
                    _lock_conn.close()
                    _err = (
                        "A saved payment method is required to add extra seats. "
                        "Please update your billing details on the upgrade page."
                    )
                    _is_xhr = (
                        request.headers.get('X-Requested-With') == 'XMLHttpRequest' or
                        request.headers.get('Accept', '').startswith('application/json')
                    )
                    if _is_xhr:
                        return jsonify({
                            'success': False, 'error': _err, 'upgrade_url': '/upgrade'
                        }), 402
                    return redirect(url_for('billing.upgrade_page'))

            if client_count >= plan_limit and not is_agency_overage:
                _lock_cursor.execute("SELECT pg_advisory_unlock(%s)", (current_user.id,))
                _lock_cursor.close()
                _lock_conn.close()
                _is_xhr = (
                    request.headers.get('X-Requested-With') == 'XMLHttpRequest' or
                    request.headers.get('Accept', '').startswith('application/json')
                )
                if _is_xhr:
                    return jsonify({
                        'success': False,
                        'error': (
                            f'Plan limit reached. You can have '
                            f'{plan_limit} chatbot{"s" if plan_limit != 1 else ""} '
                            f'on your {plan_type} plan. Upgrade to add more.'
                        ),
                        'upgrade_url': '/upgrade',
                    }), 403
                return f'''<!DOCTYPE html>
<html>
<head><title>Plan Limit Reached</title>
<link href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,700&family=DM+Sans:wght@400;600;700&display=swap" rel="stylesheet">
<style>
*{{box-sizing:border-box;margin:0;padding:0;}}
body{{font-family:'DM Sans',sans-serif;background:#F7F4EF;min-height:100vh;
  display:flex;align-items:center;justify-content:center;padding:20px;}}
.card{{background:#fff;border:1px solid #E7E2DA;border-radius:20px;padding:48px;
  max-width:480px;text-align:center;box-shadow:0 4px 24px rgba(0,0,0,0.06);}}
h1{{font-family:'Fraunces',serif;font-size:26px;font-weight:800;color:#1C1917;margin-bottom:12px;}}
p{{color:#57534E;margin-bottom:16px;line-height:1.65;font-size:15px;}}
.info{{background:rgba(184,146,74,0.1);border:1px solid rgba(184,146,74,0.25);
  border-radius:12px;padding:16px;margin-bottom:20px;color:#9A7A3A;font-size:13.5px;line-height:1.7;}}
.btn{{display:inline-block;padding:12px 24px;border-radius:10px;font-weight:700;
  text-decoration:none;margin:5px;font-size:14px;transition:all 0.2s;}}
.btn-gold{{background:#B8924A;color:#fff;}}
.btn-gold:hover{{background:#9A7A3A;}}
.btn-ghost{{background:transparent;color:#57534E;border:1.5px solid #E7E2DA;}}
</style></head>
<body>
<div class="card">
  <h1>Chatbot Limit Reached</h1>
  <p>You've reached the maximum number of chatbots for your current plan.</p>
  <div class="info">
    <strong>Plan:</strong> {plan_type.title()}<br>
    <strong>Chatbots:</strong> {client_count} / {plan_limit if plan_limit < 999999 else "Unlimited"}<br>
    <strong>Status:</strong> Limit Reached
  </div>
  <p style="font-size:13px;color:#A8A29E;">{upgrade_hint}</p>
  <a href="/upgrade" class="btn btn-gold">Upgrade Plan →</a>
  <a href="/dashboard" class="btn btn-ghost">← Back</a>
</div>
</body></html>''', 403

            client_id = models.create_client(
                current_user.id, company_name, vertical=vertical
            )

        finally:
            try:
                _lock_cursor.execute("SELECT pg_advisory_unlock(%s)", (current_user.id,))
                _lock_conn.commit()
            except Exception:
                pass
            try:
                _lock_cursor.close()
                _lock_conn.close()
            except Exception:
                pass

        current_app.logger.info(
            f"[CreateClient] Created {client_id} for user {current_user.id}"
        )

        if is_agency_overage and client_id:
            try:
                models.record_agency_overage_seat(
                    user_id   = current_user.id,
                    client_id = client_id,
                    seat_num  = client_count + 1,
                )
                current_app.logger.info(
                    f"[AgencyOverage] user={current_user.id} seat={client_count+1} "
                    f"extra_cost=${overage_cost:.2f}/mo client={client_id}"
                )
            except Exception as _ov_err:
                current_app.logger.error(
                    f"[AgencyOverage] Failed to record seat: {_ov_err}"
                )

        _is_xhr = (
            request.headers.get('X-Requested-With') == 'XMLHttpRequest' or
            request.headers.get('Accept', '').startswith('application/json')
        )
        if _is_xhr:
            return jsonify({'success': True, 'client_id': client_id})
        return redirect(url_for('auth.dashboard'))

    except Exception as e:
        current_app.logger.error(f'Error creating client: {e}')
        _is_xhr = (
            request.headers.get('X-Requested-With') == 'XMLHttpRequest' or
            request.headers.get('Accept', '').startswith('application/json')
        )
        if _is_xhr:
            return jsonify({'success': False, 'error': str(e)}), 500
        return redirect(url_for('auth.dashboard'))
