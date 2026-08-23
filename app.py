"""
app.py
------
Lumvi — Flask application entry point.

This file is responsible for wiring only:
  - Flask app creation and configuration
  - Extension initialisation (Mail, Login, Limiter, CORS, OAuth)
  - Shared helper definitions (log_conversation, fire_webhook_event, etc.)
  - Blueprint registration and dependency injection
  - DB migrations on startup
  - Subscription enforcement on startup
  - Static/widget/legal routes and error handlers

All business logic lives in blueprints/ and services/.
"""

# ── Standard library ─────────────────────────────────────────────────────────
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import shutil
import threading
import uuid
import warnings as _warnings
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, TimeoutError as _FuturesTimeout
from datetime import datetime, timedelta
from functools import wraps
from io import StringIO
from logging.handlers import RotatingFileHandler

# ── Third-party ───────────────────────────────────────────────────────────────
import requests
from authlib.integrations.flask_client import OAuth as _OAuth
from dotenv import load_dotenv
from flask import (Flask, flash, jsonify, redirect, render_template,
                   request, session, url_for)
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_login import (LoginManager, UserMixin, current_user,
                         login_required, login_user, logout_user)
from flask_mail import Mail, Message
from paypalrestsdk import Payment, configure

# ── Local ─────────────────────────────────────────────────────────────────────
import cache_utils
import commerce_adapters
import models
import shopify_connect
import webhooks as _webhooks
from ai_helper import get_ai_helper
from app_utils import sanitize_input
from bot_protection import register_bot_protection
from config import Config

load_dotenv()

# ── Thread pools ──────────────────────────────────────────────────────────────
# Defined before app creation so blueprints can receive them via init_*().
_dns_executor = ThreadPoolExecutor(max_workers=4,  thread_name_prefix='dns-check')
_wh_executor  = ThreadPoolExecutor(max_workers=8,  thread_name_prefix='wh-deliver')

# ═══════════════════════════════════════════════════════════════════════════════
# APP CREATION
# ═══════════════════════════════════════════════════════════════════════════════

app = Flask(__name__)

# ── Trust the deploy platform's reverse proxy (Render/Railway/etc. terminate
# TLS in front of this app — Flask sees plain http internally otherwise).
# Without this, url_for(..., _external=True) builds http:// URLs even though
# the site is actually served over https — which is exactly what breaks
# Google OAuth (Error 400: redirect_uri_mismatch — Google's console has the
# https:// redirect URI registered, but google_login() was generating the
# http:// version). x_proto=1 trusts one hop of X-Forwarded-Proto; x_host=1
# does the same for X-Forwarded-Host. Bump these if there's more than one
# proxy hop in front of the app.
from werkzeug.middleware.proxy_fix import ProxyFix  # noqa: E402
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

# ── Required env vars — crash at startup rather than silently misconfigure ────

_secret = os.environ.get('SECRET_KEY')
if not _secret:
    raise RuntimeError(
        'SECRET_KEY environment variable is not set. '
        'Generate one with: python -c "import secrets; print(secrets.token_hex(32))"'
    )
app.config['SECRET_KEY'] = _secret

_admin_secret = os.environ.get('ADMIN_SECRET')
if not _admin_secret:
    raise RuntimeError(
        'ADMIN_SECRET environment variable is not set. '
        'Generate one with: python -c "import secrets; print(secrets.token_hex(32))"'
    )

_flw_webhook_hash = os.environ.get('FLW_WEBHOOK_HASH')
if not _flw_webhook_hash:
    raise RuntimeError(
        'FLW_WEBHOOK_HASH environment variable is not set. '
        'Get this from Flutterwave dashboard → Settings → Webhook → Secret Hash'
    )

# ── Request / session config ──────────────────────────────────────────────────

app.config['MAX_CONTENT_LENGTH']       = 8 * 1024 * 1024   # 8 MB
app.config['PERMANENT_SESSION_LIFETIME']  = timedelta(days=30)
app.config['SESSION_COOKIE_SECURE']       = True
app.config['SESSION_COOKIE_HTTPONLY']     = True
app.config['SESSION_COOKIE_SAMESITE']     = 'Lax'
app.config['PREFERRED_URL_SCHEME']        = 'https'
app.config['SESSION_COOKIE_NAME']         = 'lumvi_session'
app.config['REMEMBER_COOKIE_DURATION']    = timedelta(days=30)
app.config['REMEMBER_COOKIE_SECURE']      = True
app.config['REMEMBER_COOKIE_HTTPONLY']    = True

# ── Logging ───────────────────────────────────────────────────────────────────

if not os.path.exists('logs'):
    os.makedirs('logs')

file_handler = RotatingFileHandler(
    'logs/chatbot.log', maxBytes=10_240_000, backupCount=10
)
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
))
file_handler.setLevel(logging.INFO)
app.logger.addHandler(file_handler)
app.logger.setLevel(logging.INFO)
app.logger.info('Lumvi startup')

# ═══════════════════════════════════════════════════════════════════════════════
# EXTENSIONS
# ═══════════════════════════════════════════════════════════════════════════════

# ── Flask-Mail ────────────────────────────────────────────────────────────────

app.config['MAIL_SERVER']            = os.environ.get('MAIL_SERVER', 'smtp-relay.brevo.com')
app.config['MAIL_PORT']              = int(os.environ.get('MAIL_PORT', 587))
app.config['MAIL_USE_TLS']           = True
app.config['MAIL_USE_SSL']           = False
app.config['MAIL_USERNAME']          = os.environ.get('MAIL_USERNAME', '')
# FIX: smtplib (what Flask-Mail wraps) has no default timeout — an SMTP
# connection that hangs at any stage (connect, TLS handshake, auth, DATA)
# just waits indefinitely, no exception ever raised, nothing ever logged.
# This is why the original signup timeout produced zero error-log output:
# it wasn't failing, it was silently stuck. 20s is generous for a normal
# SMTP round-trip; long enough to not false-positive on a slow-but-working
# connection, short enough that a hung one surfaces (and gets logged) fast.
app.config['MAIL_TIMEOUT']           = 20
app.config['MAIL_PASSWORD']          = os.environ.get('MAIL_PASSWORD', '')
app.config['MAIL_DEFAULT_SENDER']    = 'Lumvi <support@lumvi.net>'
app.config['MAIL_MAX_EMAILS']        = None
app.config['MAIL_ASCII_ATTACHMENTS'] = False
mail = Mail(app)

# Startup config check — MAIL_USERNAME/MAIL_PASSWORD and INBOUND_EMAIL_SECRET
# all default to '' when unset, so a missing one causes silent failures deep
# inside a cron job or webhook handler rather than an obvious error at boot.
# Checked together here (even though INBOUND_EMAIL_SECRET is consumed by
# inbound_email.py, registered later) so there's one place that shouts at
# startup instead of three separate first-request failures.
if not app.config['MAIL_USERNAME'] or not app.config['MAIL_PASSWORD']:
    app.logger.warning(
        '[Startup] MAIL_USERNAME/MAIL_PASSWORD not set — outbound email '
        '(signup, password reset, cart recovery, etc.) will fail silently '
        'until these are configured.'
    )
if not os.environ.get('INBOUND_EMAIL_SECRET', '').strip():
    app.logger.warning(
        '[Startup] INBOUND_EMAIL_SECRET not set — the cart recovery reply '
        'forwarding endpoint (/webhooks/inbound-email/<secret>) is disabled.'
    )

# ── Google OAuth ──────────────────────────────────────────────────────────────

_oauth       = _OAuth(app)
google_oauth = _oauth.register(
    name             = 'google',
    client_id        = os.environ.get('GOOGLE_CLIENT_ID'),
    client_secret    = os.environ.get('GOOGLE_CLIENT_SECRET'),
    server_metadata_url = 'https://accounts.google.com/.well-known/openid-configuration',
    client_kwargs    = {'scope': 'openid email profile'},
)

# ── PayPal ────────────────────────────────────────────────────────────────────

configure({
    'mode':          os.getenv('PAYPAL_MODE', 'sandbox'),
    'client_id':     os.getenv('PAYPAL_CLIENT_ID'),
    'client_secret': os.getenv('PAYPAL_CLIENT_SECRET'),
})

# ── Flask-Login ───────────────────────────────────────────────────────────────

login_manager           = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'auth.login'


class User(UserMixin):
    def __init__(self, user_data):
        self.id        = user_data['id']
        self.email     = user_data['email']
        self.plan_type = user_data['plan_type']
        self.is_admin  = bool(user_data.get('is_admin', False))


@login_manager.user_loader
def load_user(user_id):
    # PERF FIX: cache user dict in session to avoid a DB round-trip on every
    # authenticated request. Cache is busted on login and logout.
    try:
        uid    = int(user_id)
        cached = session.get('_user_cache')
        if cached and isinstance(cached, dict) and cached.get('id') == uid:
            return User(cached)
        user_data = models.get_user_by_id(uid)
        if user_data:
            session['_user_cache'] = dict(user_data)
            return User(user_data)
        return None
    except Exception as e:
        app.logger.error(f'[load_user] {e}')
        return None

# ── CORS ──────────────────────────────────────────────────────────────────────

CORS(app, resources={
    r'/api/*': {
        'origins':      '*',
        'methods':      ['GET', 'POST', 'OPTIONS'],
        'allow_headers': ['Content-Type'],
        'max_age':      3600,
    },
    r'/widget': {
        'origins': '*',
        'methods': ['GET'],
        'max_age': 3600,
    },
})

# ── Bot protection ───────────────────────────────────────────────────────────
# Real browsers (including the embeddable widget's own JS running in an end
# customer's browser) pass through untouched. Only requests that
# self-identify as a bot are checked further — see bot_protection.py for the
# full design and why this is a secondary layer, not the primary one
# (Cloudflare Bot Fight Mode, enabled in the dashboard, is).
register_bot_protection(app)

# ── Flask-Limiter ─────────────────────────────────────────────────────────────

_limiter_storage = os.environ.get('REDIS_URL', 'memory://')
if _limiter_storage == 'memory://':
    _warnings.warn(
        'REDIS_URL not set — rate limiter is in-memory. '
        'This resets on restart and breaks under multiple workers.',
        RuntimeWarning,
    )

limiter = Limiter(
    app         = app,
    key_func    = get_remote_address,
    default_limits  = ['200 per day', '50 per hour'],
    storage_uri = _limiter_storage,
)

# ── AI helper ─────────────────────────────────────────────────────────────────

ai_helper = get_ai_helper(Config.GEMINI_API_KEY, Config.GEMINI_MODEL)
if ai_helper and ai_helper.enabled:
    app.logger.info('✅ Gemini AI initialized')
    print('✅ AI Helper ENABLED — using Gemini for smart matching')
else:
    print('❌ AI Helper DISABLED — set GEMINI_API_KEY to enable.')

USE_AI = Config.USE_AI

# ═══════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════════

PLAN_LIMITS = {
    # ── Lumvi: AI Employee for Shopify & WooCommerce ──────────────────────
    # These are the ONLY plans the product now supports: Free, Starter,
    # Growth, Scale. Free is a first-class plan (not a placeholder) — it's
    # the plan every new signup lands on (see models.create_user /
    # models.create_or_link_google_user) and the plan expired subscriptions
    # fall back to (see models.downgrade_expired_users).
    # The obsolete white-label/agency-era tiers (solo, starter[old $49
    # tier], pro, growth[old $149 tier], agency, enterprise) have been
    # removed — that product no longer exists, and no production accounts
    # remain on any of them (migrate_ai_employee_plan_rename moves any
    # stragglers onto the nearest current tier).
    #
    #   clients                 — number of connected stores allowed on the
    #                             account. NOTE: "clients" is the underlying
    #                             models.py/DB field name inherited from the
    #                             old agency architecture (one row per
    #                             managed business); in the new product it
    #                             means "connected store", not "agency's
    #                             customer". Renaming the field itself is a
    #                             models.py change — out of scope here.
    #   faqs_per_client         — effectively unlimited (999) on every tier.
    #                             Vestigial: kept only because
    #                             blueprints/faqs.py still reads this key.
    #   messages_per_day        — effectively unlimited (999999) on every
    #                             tier. Superseded by conversations_per_month
    #                             below; kept only because blueprints/chat.py
    #                             still reads this key for the daily_limit
    #                             path in log_conversation().
    #   conversations_per_month — monthly cap, checked in blueprints/chat.py.
    #                             None = unlimited. Counts distinct
    #                             conversations, not raw messages (see
    #                             models.get_monthly_conversation_count).
    #   integrations_limit      — max connected platforms, checked in
    #                             create_platform_integration() below.
    #                             None = unlimited.
    #   product_recommendations — gates the recommend_products AI tool
    #                             (tools.py).
    #   cart_recovery           — gates the abandoned-checkout recovery
    #                             feature (Shopify only for now).
    #   lead_capture            — gates the lead-collection trigger in
    #                             blueprints/chat.py.
    #   api_access              — reserved. No external developer API
    #                             exists yet to gate — this flag is a
    #                             placeholder for when one ships.
    #
    # White-label branding has been removed entirely as a product concept —
    # there is no 'white_label' key any more. See remove_branding handling
    # in save_customization() below, which is now hardcoded to False.
    # Paid dict keys stay 'ai_starter'/'ai_growth'/'ai_scale' — NOT the
    # plainer 'starter'/'growth'/'scale' the plan names suggest. 'free'
    # is the one key that IS the plain plan_type value (see
    # models.create_user / models.create_or_link_google_user). A one-time
    # migration (migrate_ai_employee_plan_rename, see the migrations list
    # below) moves every obsolete plan_type (solo, old starter, pro, old
    # growth, agency, enterprise) onto the nearest current tier. Renaming
    # any of these keys would silently drop every affected user to the
    # .get(..., PLAN_LIMITS['free']) fallback the next time they load a
    # page. 'Free' / 'Starter' / 'Growth' / 'Scale' are the display names
    # shown to users (see admin_set_plan()'s form and any upgrade/billing
    # pages) — the storage values are free/ai_starter/ai_growth/ai_scale.
    'free': {
        'clients': 1, 'faqs_per_client': 50, 'messages_per_day': 999999,
        'conversations_per_month': 50, 'grace_conversations': 0,
        'integrations_limit': 1,
        'product_recommendations': False, 'cart_recovery': False,
        'lead_capture': False, 'api_access': False,
        'analytics': True, 'analytics_level': 'basic',
        'customization': False,
        'webhooks': True, 'priority_support': False,
        'agentic_actions': False,
    },
    'ai_starter': {
        'clients': 1, 'faqs_per_client': 999, 'messages_per_day': 999999,
        'conversations_per_month': 1000, 'grace_conversations': 20,
        'integrations_limit': 1,
        'product_recommendations': False, 'cart_recovery': False,
        'lead_capture': False, 'api_access': False,
        'analytics': True, 'analytics_level': 'basic',
        'customization': True,
        'webhooks': True, 'priority_support': False,
        'agentic_actions': False,
    },
    'ai_growth': {
        'clients': 1, 'faqs_per_client': 999, 'messages_per_day': 999999,
        'conversations_per_month': 5000, 'grace_conversations': 50,
        'integrations_limit': 5,  # "Shopify + WooCommerce + additional integrations"
        'product_recommendations': True, 'cart_recovery': True,
        'lead_capture': True, 'api_access': False,
        'analytics': True, 'analytics_level': 'advanced',
        'customization': True,
        'webhooks': True, 'priority_support': True,
        'agentic_actions': True,
    },
    'ai_scale': {
        'clients': 1, 'faqs_per_client': 999, 'messages_per_day': 999999,
        'conversations_per_month': None,   # unlimited
        'grace_conversations': 0,          # moot — cap is unlimited, grace never triggers
        'integrations_limit': None,        # unlimited
        'product_recommendations': True, 'cart_recovery': True,
        'lead_capture': True, 'api_access': True,
        'analytics': True, 'analytics_level': 'advanced',
        'customization': True,
        'webhooks': True, 'priority_support': True,
        'agentic_actions': True,
    },
}

# TODO(legacy, out of scope): AGENCY_INCLUDED_CLIENTS / AGENCY_SEAT_PRICE are
# per-seat billing constants from the old agency/reseller architecture. They
# no longer correspond to any active plan above. agency.py and auth.py no
# longer need them (removed along with the agency business model — see the
# billing/dashboard cleanup reports). blueprints/cron.py is now the only
# remaining consumer (its seat-billing cron jobs, themselves obsolete but
# out of scope for this pass — see cron.py follow-up notes). Delete this
# constant once cron.py's seat/overage cron jobs are retired too.
AGENCY_INCLUDED_CLIENTS = 20
AGENCY_SEAT_PRICE       = 15.00   # USD per extra client per month

# ── Vertical system prompts (imported by chat blueprint via injection) ─────────
# NOTE: VERTICAL_PROMPTS is ~180 lines. It lives here for now and will move to
# services/faq_service.py in the next refactor phase alongside the FAQ helpers.
from vertical_prompts import VERTICAL_PROMPTS   # noqa: E402  (single-concern module)
VALID_VERTICALS = set(VERTICAL_PROMPTS.keys())

# ── Keyword matcher constants ─────────────────────────────────────────────────

STOP_WORDS = {
    'a', 'an', 'the', 'this', 'that', 'these', 'those',
    'i', 'me', 'my', 'we', 'our', 'you', 'your', 'it', 'its',
    'he', 'she', 'they', 'them', 'their',
    'is', 'are', 'was', 'were', 'be', 'been', 'being',
    'do', 'does', 'did', 'have', 'has', 'had',
    'can', 'could', 'will', 'would', 'should', 'shall', 'may', 'might',
    'go', 'get', 'got', 'make', 'made',
    'in', 'on', 'at', 'by', 'for', 'with', 'about',
    'to', 'from', 'into', 'through', 'before', 'after',
    'of', 'off', 'out', 'over', 'under', 'and', 'or', 'but',
    'if', 'as', 'than', 'because', 'while',
    'just', 'also', 'too', 'very', 'really', 'quite', 'already',
    'still', 'ever', 'never', 'always', 'often',
    'please', 'thanks', 'thank', 'hello', 'hi', 'hey',
    'what', 'where', 'which', 'who', 'why', 'how',
    'any', 'all', 'some', 'more', 'most', 'many', 'much',
    'no', 'not', 'nor', 'there', 'per', 'each',
}

GENERIC_TAGS = {
    'information', 'info', 'details', 'learn',
    'business', 'service', 'services', 'product', 'products',
    'use', 'used', 'using', 'need', 'want', 'like', 'work',
    'platform', 'system', 'tool', 'website', 'site', 'account',
    'help', 'support', 'contact', 'team', 'company', 'client',
    'way', 'ways', 'option', 'options', 'type', 'types', 'kind',
}

# ═══════════════════════════════════════════════════════════════════════════════
# SHARED HELPERS
# These functions are injected into blueprints via init_*() calls below.
# They live here because they depend on app-level objects (mail, app.logger,
# the _wh_executor pool) that can't be imported without a circular dependency.
# They will migrate to services/ in the next refactor phase.
# ═══════════════════════════════════════════════════════════════════════════════

def send_welcome_email(email: str) -> None:
    """
    Send a branded welcome email to a new Lumvi user.

    FIX: fires in a background daemon thread now — this used to run
    synchronously inside the signup()/google_callback() request itself.
    mail.send() is a blocking SMTP call with no explicit timeout; if the
    mail server is slow, rate-limiting, or unreachable, the whole signup
    request hangs until Cloudflare's origin timeout and the new user gets
    a 524 instead of an account (this is exactly what was reported —
    signup via email timing out). Safe to background with no app-context
    handling needed: `mail = Mail(app)` above uses direct init (stores
    `self.app` internally), not the `init_app()` deferred pattern, so
    mail.send() doesn't touch current_app. Same reasoning applies to the
    app.logger calls below — they're the real `app` object, not the
    current_app proxy.
    """
    def _send():
        try:
            msg = Message(
                subject    = "Welcome to Lumvi — your AI Employee is ready",
                sender     = "Lumvi <support@lumvi.net>",
                recipients = [email],
                html       = """
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Welcome to Lumvi</title></head>
<body style="margin:0;padding:0;background:#F7F4EF;font-family:Georgia,'Times New Roman',serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#F7F4EF;padding:48px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" style="max-width:560px;width:100%;">

        <!-- Header -->
        <tr><td style="padding:0 40px 32px;text-align:center;">
          <span style="font-family:Arial,Helvetica,sans-serif;font-size:20px;font-weight:700;color:#1E1E1E;letter-spacing:0.5px;">LUMVI</span>
          <div style="margin-top:18px;">
            <span style="font-family:Arial,Helvetica,sans-serif;font-size:11px;font-weight:700;letter-spacing:2px;color:#B79349;">&mdash; MEET YOUR AI EMPLOYEE</span>
          </div>
        </td></tr>

        <!-- Main card -->
        <tr><td style="background:#FFFFFF;border:1px solid #EAE4D9;border-radius:4px;padding:48px 44px;">

          <h1 style="margin:0 0 20px;font-family:Georgia,'Times New Roman',serif;font-size:28px;font-weight:400;color:#1E1E1E;line-height:1.3;text-align:center;">
            Your store just hired<br/>its best employee.
          </h1>

          <p style="margin:0 0 32px;font-family:Arial,Helvetica,sans-serif;color:#4A4A4A;font-size:15px;line-height:1.7;text-align:center;">
            Welcome to Lumvi. It works like your best hire &mdash; it just doesn't sleep.
            From today, your Shopify or WooCommerce store has someone answering customers,
            tracking orders, recommending products, and handling returns around the clock.
          </p>

          <!-- Divider -->
          <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:32px;">
            <tr><td style="border-top:1px solid #EAE4D9;font-size:1px;line-height:1px;">&nbsp;</td></tr>
          </table>

          <!-- Steps -->
          <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:36px;">
            <tr>
              <td width="36" valign="top" style="padding-bottom:22px;">
                <span style="display:inline-block;width:24px;height:24px;border:1px solid #B79349;border-radius:50%;text-align:center;line-height:24px;font-family:Arial,Helvetica,sans-serif;font-size:12px;font-weight:700;color:#B79349;">1</span>
              </td>
              <td valign="top" style="padding-bottom:22px;padding-left:8px;">
                <strong style="font-family:Arial,Helvetica,sans-serif;font-size:14px;font-weight:700;color:#1E1E1E;">Connect your store</strong>
                <p style="margin:6px 0 0;font-family:Arial,Helvetica,sans-serif;font-size:13px;color:#767676;line-height:1.6;">Link your Shopify or WooCommerce store in a couple of clicks &mdash; no developer needed.</p>
              </td>
            </tr>
            <tr>
              <td width="36" valign="top" style="padding-bottom:22px;">
                <span style="display:inline-block;width:24px;height:24px;border:1px solid #B79349;border-radius:50%;text-align:center;line-height:24px;font-family:Arial,Helvetica,sans-serif;font-size:12px;font-weight:700;color:#B79349;">2</span>
              </td>
              <td valign="top" style="padding-bottom:22px;padding-left:8px;">
                <strong style="font-family:Arial,Helvetica,sans-serif;font-size:14px;font-weight:700;color:#1E1E1E;">Teach it your business</strong>
                <p style="margin:6px 0 0;font-family:Arial,Helvetica,sans-serif;font-size:13px;color:#767676;line-height:1.6;">Add your FAQs, policies, and product details so it sounds like you.</p>
              </td>
            </tr>
            <tr>
              <td width="36" valign="top">
                <span style="display:inline-block;width:24px;height:24px;border:1px solid #B79349;border-radius:50%;text-align:center;line-height:24px;font-family:Arial,Helvetica,sans-serif;font-size:12px;font-weight:700;color:#B79349;">3</span>
              </td>
              <td valign="top" style="padding-left:8px;">
                <strong style="font-family:Arial,Helvetica,sans-serif;font-size:14px;font-weight:700;color:#1E1E1E;">Put it to work</strong>
                <p style="margin:6px 0 0;font-family:Arial,Helvetica,sans-serif;font-size:13px;color:#767676;line-height:1.6;">Embed one line of code and your AI Employee starts serving customers immediately.</p>
              </td>
            </tr>
          </table>

          <!-- CTA -->
          <table width="100%" cellpadding="0" cellspacing="0">
            <tr><td align="center" style="padding-bottom:8px;">
              <a href="https://lumvi.net/dashboard" style="display:inline-block;background:#1E1E1E;color:#F7F4EF;text-decoration:none;padding:16px 40px;border-radius:2px;font-family:Arial,Helvetica,sans-serif;font-weight:700;font-size:13px;letter-spacing:1px;text-transform:uppercase;">
                Start Free Trial
              </a>
            </td></tr>
          </table>

        </td></tr>

        <!-- Footer -->
        <tr><td style="padding:32px 40px 0;text-align:center;">
          <p style="margin:0;font-family:Arial,Helvetica,sans-serif;color:#9A9A9A;font-size:12px;line-height:1.7;">
            Questions? Reply to this email or reach us at <a href="mailto:support@lumvi.net" style="color:#B79349;text-decoration:none;">support@lumvi.net</a>
          </p>
          <p style="margin:12px 0 0;font-family:Arial,Helvetica,sans-serif;color:#C4C4C4;font-size:11px;">
            &copy; 2026 Lumvi &middot; <a href="https://lumvi.net" style="color:#C4C4C4;text-decoration:none;">lumvi.net</a>
          </p>
        </td></tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""
            )
            mail.send(msg)
            app.logger.info(f'Welcome email sent to {email}')
        except Exception as e:
            app.logger.error(f'Welcome email failed for {email}: {type(e).__name__}: {e}')

    threading.Thread(target=_send, daemon=True).start()


def get_subscription_status(user: dict) -> dict:
    """
    Returns subscription info for a user.
    status: 'active' | 'cancelling' | 'grace' | 'expired'
    Admins are always treated as active.
    """
    if user.get('is_admin'):
        return {'status': 'active', 'expires_at': None, 'grace_ends_at': None}

    # NOTE: 'enterprise' no longer exists under the Free/Starter/Growth/
    # Scale architecture (see PLAN_LIMITS above). 'free' IS a real current
    # plan again, but doesn't need a special case here — free accounts are
    # never given a subscription_expires_at value, so they fall straight
    # into the `if not expires_at` branch just below and read as
    # permanently active, same as admin-granted accounts (admin_set_plan()).
    expires_at    = user.get('subscription_expires_at')
    grace_ends_at = user.get('grace_period_ends_at')

    if not expires_at:
        return {'status': 'active', 'expires_at': None, 'grace_ends_at': None}

    now = datetime.utcnow()

    def _parse(dt):
        if isinstance(dt, str):
            try:   return datetime.strptime(dt, '%Y-%m-%d %H:%M:%S.%f')
            except ValueError:
                   return datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
        return dt

    expires_at    = _parse(expires_at)
    grace_ends_at = _parse(grace_ends_at) if grace_ends_at else None

    if now < expires_at:
        if user.get('cancel_at_period_end'):
            return {'status': 'cancelling', 'expires_at': expires_at,
                    'grace_ends_at': grace_ends_at}
        return {'status': 'active', 'expires_at': expires_at,
                'grace_ends_at': grace_ends_at}
    elif grace_ends_at and now < grace_ends_at:
        return {'status': 'grace', 'expires_at': expires_at,
                'grace_ends_at': grace_ends_at}
    else:
        return {'status': 'expired', 'expires_at': expires_at,
                'grace_ends_at': grace_ends_at}


def enforce_subscriptions() -> list:
    """
    Downgrade all non-admin users whose grace period has ended.
    Safe to call multiple times — idempotent SQL WHERE clause.
    Logs every run to cron_runs for auditability.
    """
    import time as _time
    t0 = _time.time()
    try:
        now = datetime.utcnow()
        app.logger.info(
            f'[Scheduler] enforce_subscriptions running at {now.isoformat()}'
        )
        downgraded = models.downgrade_expired_users()
        for u in downgraded:
            app.logger.info(
                f"[Scheduler] Downgraded user {u['id']} ({u.get('email')}) → free"
            )
        app.logger.info(f'[Scheduler] Total downgraded: {len(downgraded)}')
        duration_ms = int((_time.time() - t0) * 1000)
        models.log_cron_run(
            'enforce_subscriptions', success=True,
            result={'downgraded_count': len(downgraded)},
            duration_ms=duration_ms,
        )
        return downgraded
    except Exception as e:
        app.logger.error(f'[Scheduler] enforce_subscriptions error: {e}')
        models.log_cron_run(
            'enforce_subscriptions', success=False,
            result={'error': str(e)}, duration_ms=0,
        )
        return []


# ── Client owner plan cache ───────────────────────────────────────────────────
# 60-second TTL avoids 2 extra DB round-trips on every chat message.
# Thread-safe: double-checked locking pattern prevents stale overwrites.

_client_owner_cache: dict      = {}
_client_owner_cache_lock       = threading.Lock()


def _get_cached_client_owner(client_id: str):
    """Return client owner dict from cache, falling back to DB on miss/expiry."""
    with _client_owner_cache_lock:
        entry = _client_owner_cache.get(client_id)
        if entry:
            owner, expires_at = entry
            if datetime.utcnow() < expires_at:
                return owner
    # Cache miss — query outside the lock to avoid holding it during a slow DB call
    owner = models.get_client_owner(client_id)
    if owner:
        with _client_owner_cache_lock:
            existing = _client_owner_cache.get(client_id)
            if not existing or datetime.utcnow() >= existing[1]:
                _client_owner_cache[client_id] = (
                    owner, datetime.utcnow() + timedelta(seconds=60)
                )
    return owner


# ── Keyword FAQ matcher ───────────────────────────────────────────────────────
# Used only when AI is disabled or the RAG pipeline fails.
# Will move to services/faq_service.py in the next refactor phase.

def extract_keywords(text: str) -> list:
    words = re.findall(r'\b[a-z]+\b', text.lower())
    return [w for w in words if w not in STOP_WORDS and len(w) >= 3]


def compute_tag_weights(faqs_list: list) -> dict:
    tag_frequency = Counter()
    for faq in faqs_list:
        for tag in faq.get('triggers', []):
            tag_frequency[tag.lower()] += 1
        for kw in extract_keywords(faq.get('question', '')):
            tag_frequency[kw] += 1
    return {
        tag: (0.05 if tag in GENERIC_TAGS else round(1.0 / freq, 3))
        for tag, freq in tag_frequency.items()
    }


def find_best_match(user_query: str, faqs_list: list,
                    confidence_threshold: float = 0.68):
    if not user_query or not faqs_list:
        return None, 0.0
    query_keywords = extract_keywords(user_query)
    if not query_keywords:
        return None, 0.0

    query_kw_set = set(query_keywords)
    tag_weights  = compute_tag_weights(faqs_list)
    best_faq, best_score = None, 0.0

    for faq in faqs_list:
        raw_tags  = [t.lower().strip() for t in faq.get('triggers', [])]
        all_tags  = set(raw_tags + extract_keywords(faq.get('question', '')))
        matched   = query_kw_set.intersection(all_tags)
        if not matched:
            continue
        raw_score    = sum(tag_weights.get(t, 0.3) for t in matched)
        max_possible = sum(tag_weights.get(t, 0.3) for t in all_tags)
        normalized   = raw_score / max_possible if max_possible > 0 else 0.0
        coverage     = len(matched) / len(query_kw_set)
        final_score  = normalized * 0.7 + coverage * 0.3
        if final_score > best_score:
            best_score = final_score
            best_faq   = faq

    if best_score < confidence_threshold:
        app.logger.info(
            f"[Matcher] Low confidence ({best_score:.2f}) for: '{user_query}'"
        )
        return None, 0.0

    app.logger.info(
        f"[Matcher] Matched '{best_faq.get('question')}' | score: {best_score:.2f}"
    )
    return best_faq, round(best_score, 2)


# ── Unified webhook dispatcher ────────────────────────────────────────────────

def _is_safe_webhook_url(url: str) -> bool:
    import ipaddress
    from urllib.parse import urlparse
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ('http', 'https'):
            return False
        host = parsed.hostname or ''
        if not host:
            return False
        if host in ('localhost', 'metadata.google.internal', '169.254.169.254'):
            return False
        try:
            addr = ipaddress.ip_address(host)
            if (addr.is_private or addr.is_loopback or
                    addr.is_link_local or addr.is_reserved):
                return False
        except ValueError:
            pass   # hostname, not IP — allow
        return True
    except Exception:
        return False


def _deliver_one(webhook_url: str, payload: dict, signing_secret: str,
                 event_type: str, client_id: str, webhook_id: str) -> None:
    """Deliver one webhook — up to 3 attempts with exponential back-off."""
    import time, hmac as _hmac, hashlib

    body_bytes = json.dumps(payload, separators=(',', ':')).encode()
    headers    = {
        'Content-Type':     'application/json',
        'X-Lumvi-Event':    event_type,
        'X-Lumvi-Delivery': str(uuid.uuid4()),
        'User-Agent':       'Lumvi-Webhooks/1.0',
    }
    if signing_secret:
        sig = _hmac.new(signing_secret.encode(), body_bytes, hashlib.sha256).hexdigest()
        headers['X-Lumvi-Signature'] = f'sha256={sig}'

    delays, last_exc, status, resp_txt, duration = [0, 0.5, 2.0], None, 0, '', 0
    for attempt, delay in enumerate(delays, start=1):
        if delay:
            time.sleep(delay)
        t0 = time.time()
        try:
            resp     = requests.post(webhook_url, data=body_bytes,
                                     headers=headers, timeout=10)
            duration = int((time.time() - t0) * 1000)
            status   = resp.status_code
            resp_txt = resp.text[:500]
            if 200 <= status < 300:
                app.logger.info(
                    f'[Webhook] ✓ event={event_type} client={client_id} '
                    f'wh={webhook_id} attempt={attempt} status={status}'
                )
                models.log_webhook_delivery(
                    client_id=client_id, webhook_id=webhook_id,
                    event_type=event_type, url=webhook_url,
                    payload=payload, status_code=status,
                    response_text=resp_txt, success=True,
                    duration_ms=duration,
                )
                return
            app.logger.warning(
                f'[Webhook] non-2xx event={event_type} status={status} '
                f'attempt={attempt}'
            )
        except Exception as exc:
            duration = int((time.time() - t0) * 1000)
            last_exc = exc
            app.logger.warning(
                f'[Webhook] error event={event_type} attempt={attempt}: {exc}'
            )

    models.log_webhook_delivery(
        client_id=client_id, webhook_id=webhook_id,
        event_type=event_type, url=webhook_url,
        payload=payload, status_code=status,
        response_text=resp_txt or str(last_exc or 'Failed after 3 attempts'),
        success=False, duration_ms=duration,
    )
    app.logger.error(
        f'[Webhook] ✗ all attempts failed event={event_type} '
        f'client={client_id} wh={webhook_id}'
    )


def fire_webhook_event(client_id: str, event_type: str, data: dict) -> None:
    """Dispatch an outbound webhook event — non-blocking, thread-pooled."""
    if client_id == 'demo':
        return
    try:
        webhooks = models.get_webhooks(client_id)
        if not webhooks:
            return
        payload = {
            'event':     event_type,
            'client_id': client_id,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'data':      data,
        }
        fired = 0
        for wh in webhooks:
            if not wh.get('enabled'):
                continue
            subscribed = wh.get('events') or []
            if isinstance(subscribed, str):
                try:    subscribed = json.loads(subscribed)
                except Exception: subscribed = []
            if event_type not in subscribed:
                continue
            url = (wh.get('url') or '').strip()
            if not url or not _is_safe_webhook_url(url):
                if url:
                    app.logger.warning(
                        f'[Webhook] SSRF-blocked url wh={wh.get("webhook_id")}'
                    )
                continue
            _wh_executor.submit(
                _deliver_one,
                url, payload, wh.get('signing_secret', ''),
                event_type, client_id, wh.get('webhook_id', ''),
            )
            fired += 1
        if fired:
            app.logger.info(
                f'[Webhook] dispatched event={event_type} '
                f'client={client_id} webhooks={fired}'
            )
    except Exception as exc:
        app.logger.error(f'[Webhook] fire_webhook_event error: {exc}')


def notify_webhook(client_id: str, lead_data: dict) -> None:
    """Deprecated shim — delegates to fire_webhook_event."""
    fire_webhook_event(client_id, 'lead_captured', lead_data)


def log_conversation(client_id, user_message, bot_response,
                     matched=False, method='unknown',
                     session_id=None, daily_limit=None, monthly_limit=None,
                     page_url=None, referrer=None,
                     utm_source=None, utm_medium=None,
                     utm_campaign=None) -> bool:
    """
    Insert a conversation row.
    When daily_limit is supplied, the INSERT is wrapped in a CTE that
    re-counts today's rows atomically — prevents races at the cap boundary.
    monthly_limit does the same thing for the Starter/Growth/Scale
    'conversations_per_month' gate — counts DISTINCT conversations
    (session_id, or id::text as a fallback for session-less rows) in the
    current calendar month rather than raw rows today. Only one of
    daily_limit/monthly_limit should be passed at a time — every current
    plan uses monthly_limit; daily_limit is kept only for callers still
    passing messages_per_day-style caps (see PLAN_LIMITS note on
    'messages_per_day' being vestigial). If both are given, daily_limit
    takes precedence.
    Returns True if the row was inserted, False if the cap was already reached.
    Page-context fields (page_url, referrer, utm_*) are written on the first
    message of a session; subsequent messages carry the same values.
    """
    try:
        conn, cursor = models.get_db()
        if daily_limit is not None and daily_limit < 999999:
            cursor.execute(
                '''
                WITH today_count AS (
                    SELECT COUNT(*) AS cnt FROM conversations
                    WHERE  client_id = %s AND DATE(timestamp) = CURRENT_DATE
                )
                INSERT INTO conversations
                    (client_id, user_message, bot_response, matched, method,
                     session_id, page_url, referrer,
                     utm_source, utm_medium, utm_campaign)
                SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                FROM   today_count
                WHERE  cnt < %s
                ''',
                (client_id,
                 client_id, user_message, bot_response,
                 matched, method, session_id or None,
                 page_url, referrer, utm_source, utm_medium, utm_campaign,
                 daily_limit)
            )
            inserted = cursor.rowcount > 0
        elif monthly_limit is not None:
            cursor.execute(
                '''
                WITH month_count AS (
                    SELECT COUNT(DISTINCT COALESCE(session_id, id::text)) AS cnt
                    FROM conversations
                    WHERE  client_id = %s
                      AND  timestamp >= date_trunc('month', CURRENT_DATE)
                )
                INSERT INTO conversations
                    (client_id, user_message, bot_response, matched, method,
                     session_id, page_url, referrer,
                     utm_source, utm_medium, utm_campaign)
                SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                FROM   month_count
                WHERE  cnt < %s
                ''',
                (client_id,
                 client_id, user_message, bot_response,
                 matched, method, session_id or None,
                 page_url, referrer, utm_source, utm_medium, utm_campaign,
                 monthly_limit)
            )
            inserted = cursor.rowcount > 0
        else:
            cursor.execute(
                '''
                INSERT INTO conversations
                    (client_id, user_message, bot_response, matched, method,
                     session_id, page_url, referrer,
                     utm_source, utm_medium, utm_campaign)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''',
                (client_id, user_message, bot_response,
                 matched, method, session_id or None,
                 page_url, referrer, utm_source, utm_medium, utm_campaign)
            )
            inserted = True

        conn.commit()
        cursor.close()
        conn.close()

        if inserted:
            app.logger.info(
                f'✅ Logged conversation for {client_id} session={session_id}'
            )
        else:
            app.logger.info(
                f'[Limit] atomic insert blocked for {client_id} '
                f'(daily_limit={daily_limit}, monthly_limit={monthly_limit})'
            )
        return inserted
    except Exception as e:
        app.logger.error(f'❌ Error logging conversation: {e}')
        return True   # fail-open: never block the chat response on a log error


# ── notify_handoff / notify_usage_threshold — kept here so chat.py can
# receive them via injection. Implementations live in blueprints/inbox.py.
# We import them here so they can be passed into init_chat() by name.
from blueprints.inbox import notify_handoff, notify_usage_threshold   # noqa: E402

# ═══════════════════════════════════════════════════════════════════════════════
# DATABASE MIGRATIONS (startup, idempotent)
# ═══════════════════════════════════════════════════════════════════════════════

try:
    models.init_db()

    _optional_migrations = [
        'migrate_clients_table', 'migrate_faqs_table',
        'migrate_faq_to_knowledge_base', 'migrate_subscription_expiry',
        'migrate_to_recurring_subscriptions', 'migrate_conversation_features',
        'migrate_knowledge_base', 'migrate_faqs_import_tracking',
        'migrate_business_knowledge',
        'migrate_webhooks', 'migrate_white_label',
        'migrate_client_status', 'migrate_onboarding', 'migrate_cron_tables',
        'migrate_api_usage_log', 'migrate_kb_gaps', 'migrate_lead_pipeline',
        'migrate_agency_seat_billing', 'migrate_payments_unique_reference',
        'migrate_google_oauth',
        # Tier 1 features
        'migrate_page_context', 'migrate_csat',
        'migrate_conversation_status', 'migrate_conversation_tags',
        'migrate_proactive_triggers',
        'migrate_lead_extra_fields',
        'migrate_lead_duplicate_tracking',
        'migrate_lead_outcome_tracking',
        'migrate_lead_nudge_tracking',
        'migrate_lead_intent_summary',
        'migrate_overage_tracking',   # agency $15/seat recurring billing columns
        'migrate_seat_subscriptions', # agency per-seat purchase subscriptions table
        'migrate_lead_delivery',      # Gap 3 — notification_email/phone/name on clients
        'migrate_agency_email_domains', # white-label custom email domain
        'migrate_external_integrations',       # client_ext_integrations + actions + audit log (agentic external tool calls)
        'migrate_account_profile',             # profile fields + soft/hard delete on users
        'migrate_system_settings',             # generic key-value store for live admin toggles
        'migrate_ai_employee_plan_rename',     # Shopify/WooCommerce pivot: agency -> ai_scale (one-time)
        'migrate_cart_recovery',               # abandoned_carts table + clients.cart_recovery_enabled
        'migrate_usage_notifications',         # usage_notifications table + clients.ai_unavailable_mode/human_support_contact
        'migrate_admin_activity_tracking',     # admin dashboard: users activity cols + analytics_events IP/UA
    ]
    for _fn in _optional_migrations:
        if hasattr(models, _fn):
            getattr(models, _fn)()

    # Drop legacy plan_type check constraint if present
    try:
        _c, _cur = models.get_db()
        _cur.execute('ALTER TABLE users DROP CONSTRAINT IF EXISTS users_plan_type_check')
        _c.commit(); _cur.close(); _c.close()
    except Exception:
        pass

    # Training data tables
    try:
        from training_collector import migrate_training_tables
        migrate_training_tables()
    except Exception as _e:
        print(f'⚠️  migrate_training_tables: {_e}')

    # Agent tool tables
    try:
        from tools import migrate_agent_tables
        migrate_agent_tables()
    except Exception as _e:
        print(f'⚠️  migrate_agent_tables: {_e}')

    # Platform webhook ingestion tables
    try:
        _webhooks.migrate_integrations()
    except Exception as _e:
        print(f'⚠️  webhooks.migrate_integrations: {_e}')

    # Conversations table + session_id index
    try:
        _c, _cur = models.get_db()
        _cur.execute('''
            CREATE TABLE IF NOT EXISTS conversations (
                id           SERIAL    PRIMARY KEY,
                client_id    TEXT      NOT NULL,
                user_message TEXT      NOT NULL,
                bot_response TEXT      NOT NULL,
                matched      BOOLEAN   DEFAULT FALSE,
                method       TEXT,
                session_id   TEXT,
                timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        _cur.execute(
            'CREATE INDEX IF NOT EXISTS idx_conversations_client_session '
            'ON conversations (client_id, session_id) WHERE session_id IS NOT NULL'
        )
        _c.commit(); _cur.close(); _c.close()
    except Exception as _e:
        print(f'⚠️  conversations migration: {_e}')

    print('✅ Database initialized/migrated successfully!')

    # ── Startup enforcement — single-worker guard via pg_try_advisory_lock ────
    def _startup_enforce():
        try:
            with app.app_context():
                _sc, _scur = models.get_db()
                try:
                    _scur.execute('SELECT pg_try_advisory_lock(13370001)')
                    row      = _scur.fetchone()
                    acquired = list(row.values())[0] if row else False
                except Exception:
                    acquired = True
                    _sc = _scur = None

                if not acquired:
                    app.logger.info(
                        '[Startup] enforcement skipped — another worker running it'
                    )
                    if _sc:
                        try: _scur.close(); _sc.close()
                        except Exception: pass
                    return

                try:
                    enforce_subscriptions()
                    app.logger.info('[Startup] subscription enforcement complete')
                finally:
                    if _sc:
                        try:
                            _scur.execute('SELECT pg_advisory_unlock(13370001)')
                            _sc.commit(); _scur.close(); _sc.close()
                        except Exception:
                            pass
        except Exception as _e:
            app.logger.error(f'[Startup] enforcement error: {_e}')

    threading.Timer(10.0, _startup_enforce).start()
    print('✅ Startup enforcement scheduled (T+10s, single-worker guard active)')

except Exception as e:
    print(f'⚠️  Database initialization error: {e}')

# ═══════════════════════════════════════════════════════════════════════════════
# BLUEPRINT REGISTRATION
# ═══════════════════════════════════════════════════════════════════════════════

# Admin blueprint (pre-existing — no init_* needed)
from admin_routes import admin_bp
app.register_blueprint(admin_bp)

# Inbox
from blueprints.inbox import inbox_bp, init_inbox
init_inbox(mail=mail, fire_webhook=fire_webhook_event)
app.register_blueprint(inbox_bp)

# Leads
from blueprints.leads import leads_bp, init_leads, submit_lead as _submit_lead_view
init_leads(
    mail=mail, limiter=limiter,
    fire_webhook=fire_webhook_event,
    notify_webhook=notify_webhook,
    log_conversation=log_conversation,
    ai_helper=ai_helper,
)
app.register_blueprint(leads_bp)
limiter.limit('10 per hour')(_submit_lead_view)

# FAQs
from blueprints.faqs import faqs_bp, init_faqs
init_faqs(
    app=app,
    plan_limits=PLAN_LIMITS,
    ai_helper=ai_helper,
    extract_keywords=extract_keywords,
)
app.register_blueprint(faqs_bp)

# Business Knowledge (Phase 1) — additive, isolated from the FAQ blueprint
# above; not wired into live retrieval yet (Phase 2).
from blueprints.business_knowledge import business_knowledge_bp, init_business_knowledge
init_business_knowledge(app=app, ai_helper=ai_helper)
app.register_blueprint(business_knowledge_bp)

# Billing
# NOTE: billing.py's PLAN_PRICES_FLW (a legacy pricing dict, duplicate of
# the pricing represented by PLAN_LIMITS above) is no longer imported here —
# it was unused in this file. It still lives in blueprints/billing.py;
# TODO(legacy, out of scope): update billing.py's own pricing table to only
# price Starter/Growth/Scale, since that's a change to billing.py.
from blueprints.billing import billing_bp, init_billing
init_billing(mail=mail, get_subscription_status=get_subscription_status)
app.register_blueprint(billing_bp)

# Agency (stripped down — single-store analytics/dashboard only, see
# blueprints/agency.py's module docstring for what was removed and why)
from blueprints.agency import agency_bp, init_agency
init_agency(plan_limits=PLAN_LIMITS)
app.register_blueprint(agency_bp)

# Account settings (profile, email, password, notifications, deletion)
from blueprints.account import account_bp
app.register_blueprint(account_bp)

# Auth
from blueprints.auth import auth_bp, init_auth
init_auth(
    mail=mail,
    google_oauth=google_oauth,
    plan_limits=PLAN_LIMITS,
    valid_verticals=VALID_VERTICALS,
    get_subscription_status=get_subscription_status,
    send_welcome_email=send_welcome_email,
    User=User,
)
app.register_blueprint(auth_bp)

# Cron
from blueprints.cron import cron_bp, init_cron
init_cron(
    mail=mail,
    enforce_subscriptions=enforce_subscriptions,
    agency_included_clients=AGENCY_INCLUDED_CLIENTS,
    agency_seat_price=AGENCY_SEAT_PRICE,
)
app.register_blueprint(cron_bp)

# Chat (registered last — depends on notify_handoff from inbox_bp)
from blueprints.chat import (chat_bp, init_chat,
                               chat as _chat_view,
                               chat_rate as _chat_rate_view)
init_chat(
    limiter=limiter,
    ai_helper=ai_helper,
    plan_limits=PLAN_LIMITS,
    vertical_prompts=VERTICAL_PROMPTS,
    log_conversation=log_conversation,
    find_best_match=find_best_match,
    get_cached_client_owner=_get_cached_client_owner,
    fire_webhook=fire_webhook_event,
    notify_handoff=notify_handoff,
    notify_usage_threshold=notify_usage_threshold,
)
app.register_blueprint(chat_bp)
limiter.limit('30 per minute')(_chat_view)
limiter.limit('20 per minute')(_chat_rate_view)

# ── Tier 1 feature routes (CSAT, status, typing, tags, triggers) ──────────────
from blueprints.inbox_additions import inbox_additions_bp
app.register_blueprint(inbox_additions_bp)

from blueprints.email_domains   import email_domains_bp
app.register_blueprint(email_domains_bp)

from blueprints.client_settings import client_settings_bp
app.register_blueprint(client_settings_bp)

# Inbound email (Brevo inbound parsing — cart recovery reply forwarding)
from blueprints.inbound_email import inbound_email_bp, init_inbound_email
init_inbound_email(mail=mail)
app.register_blueprint(inbound_email_bp)

# Blog (Knowledge Hub) — no dependencies, same shape as account_bp
from blueprints.blog import blog_bp
app.register_blueprint(blog_bp)

# Platform webhook routes (Shopify, Acuity)
_webhooks.register_webhook_routes(app, mail=mail)

# ═══════════════════════════════════════════════════════════════════════════════
# AFTER-REQUEST HOOK
# ═══════════════════════════════════════════════════════════════════════════════

@app.after_request
def allow_widget_embedding(response):
    response.headers.pop('X-Frame-Options', None)
    response.headers['Content-Security-Policy'] = 'frame-ancestors *'
    origin = request.headers.get('Origin')
    if origin:
        path = request.path
        if path.startswith('/api/') or path.startswith('/widget'):
            response.headers['Access-Control-Allow-Origin']      = origin
            response.headers['Access-Control-Allow-Credentials'] = 'true'
            response.headers['Access-Control-Allow-Methods']     = (
                'GET, POST, PUT, DELETE, OPTIONS'
            )
            response.headers['Access-Control-Allow-Headers'] = (
                'Content-Type, Authorization, X-Requested-With'
            )
    return response

# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES THAT STAY IN app.py
# Simple pages with no business logic — not worth a blueprint.
# ═══════════════════════════════════════════════════════════════════════════════

@app.route('/api/config', methods=['GET'])
def get_config():
    try:
        client_id = request.args.get('client_id', 'default')
        client    = models.get_client_by_id(client_id)
        if not client:
            return jsonify({'success': False, 'error': 'Client not found'}), 404
        branding = (json.loads(client['branding_settings'])
                    if client['branding_settings'] else {})

        # Business hours — server-side tz check, widget receives a simple bool
        bh_status = models.check_business_hours(client_id)
        # Proactive triggers — widget evaluates time/URL rules client-side
        triggers  = models.get_proactive_triggers(client_id)

        return jsonify({'success': True, 'config': {
            'client_id':    client_id,
            'branding':     branding.get('branding', {}),
            'contact':      branding.get('contact', {}),
            'bot_settings': branding.get('bot_settings', {}),
            'is_online':    bh_status['is_open'],
            'offline_msg':  bh_status['offline_message'],
            'triggers':     triggers,
        }})
    except Exception as e:
        app.logger.error(f'Error getting config: {e}')
        return jsonify({'success': False, 'error': 'Failed to load configuration'}), 500


@app.route('/api/shopify/resolve-client', methods=['GET'])
def shopify_resolve_client():
    """
    shop domain -> client_id, for the Theme App Embed's bootstrap script
    (extensions/lumvi-chat-embed/blocks/*.liquid) to find its client_id
    with zero merchant configuration — the block only ever renders
    {{ shop.permanent_domain }}, which Shopify provides automatically, so
    there's nothing for the merchant to paste or pick.

    Once resolved, the bootstrap script creates widget.js's own <script>
    tag with data-client-id set, which is exactly how the manual embed
    already worked — see widget.js's currentScript/data-client-id
    resolution. widget.js itself needed no changes for this migration.

    client_id isn't sensitive at this exposure level — it's already
    visible in the storefront's rendered page source via that same script
    tag, same as it always has been with the manual snippet. No new
    disclosure here, just automating what a merchant used to copy/paste.

    No rate limit — matches /api/config just above, the same kind of
    cheap, public, read-only lookup already called on every widget load.
    """
    shop = (request.args.get('shop') or '').strip().lower()
    if not shop:
        return jsonify({'success': False, 'error': 'Missing shop parameter'}), 400

    client_id = _webhooks.get_client_id_by_shopify_shop(shop)
    if not client_id:
        # Not an error the merchant can act on from here — most likely the
        # embed is turned on in a theme but the OAuth install hasn't
        # completed (or was uninstalled). The bootstrap script just skips
        # rendering the widget rather than showing a broken chat button.
        return jsonify({'success': False, 'error': 'No connected Lumvi account for this shop'}), 404

    return jsonify({'success': True, 'client_id': client_id})


@app.route('/api/user/info')
@login_required
def user_info():
    return jsonify({
        'success':   True,
        'plan_type': current_user.plan_type,
        'email':     current_user.email,
        'id':        current_user.id,
    })


@app.route('/demo')
def demo_page():
    return render_template('demo.html')


@app.route('/support')
@login_required
def support_page():
    plan        = current_user.plan_type
    plan_limits = PLAN_LIMITS.get(plan, PLAN_LIMITS['free'])
    has_priority = plan_limits['priority_support']
    return render_template(
        'support.html',
        user         = current_user,
        has_priority = has_priority,
        response_sla = '< 4 hours' if has_priority else '1-2 business days',
        badge        = 'Priority Support' if has_priority else 'Standard Support',
    )


@app.route('/terms')
def terms():
    return render_template('terms.html')


@app.route('/privacy-policy')
def privacy_policy():
    return render_template('privacy-policy.html')


@app.route('/refund-policy')
def refund_policy():
    return render_template('refund-policy.html')


# ═══════════════════════════════════════════════════════════════════════════════
# ERROR HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════

@app.errorhandler(413)
def request_too_large(e):
    app.logger.warning(f'[413] Request too large: {request.path}')
    if request.path.startswith('/api/'):
        return jsonify({
            'success': False,
            'error':   (
                'Request too large. Avatar images are auto-compressed in the browser '
                '— if you see this error please try a smaller file.'
            ),
        }), 413
    return 'Request too large (max 8 MB)', 413


# ═══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    # Production: gunicorn app:app --workers 4 --worker-class gevent --bind 0.0.0.0:$PORT
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)

# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES — MISSING FROM FIRST PASS (all 33 added here)
# ═══════════════════════════════════════════════════════════════════════════════

# ── Root + widget ─────────────────────────────────────────────────────────────

@app.route('/')
def index():
    if current_user.is_authenticated:
        return redirect(url_for('auth.dashboard'))
    return redirect(url_for('landing_page'))


@app.route('/landing')
def landing_page():
    return render_template('landing-professional.html')


@app.route('/static/widget.js')
def serve_widget_js_custom_domain():
    from flask import send_from_directory
    host = request.host.split(':')[0].lower()
    if host in ('lumvi.net', 'www.lumvi.net', 'localhost', '127.0.0.1'):
        return send_from_directory(app.static_folder, 'widget.js',
                                   mimetype='application/javascript')
    client = models.get_client_by_custom_domain(host)
    if not client:
        app.logger.warning(f'[WidgetJS] Unregistered custom domain blocked: {host}')
        return jsonify({'error': 'Unknown domain'}), 403
    app.logger.info(
        f'[WidgetJS] Serving widget.js for custom domain: {host} '
        f'client={client["client_id"]}'
    )
    response = send_from_directory(app.static_folder, 'widget.js',
                                   mimetype='application/javascript')
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Cache-Control'] = 'public, max-age=300'
    return response


@app.route('/widget')
def widget():
    client_id = request.args.get('client_id', '').strip()
    client    = None

    if client_id and client_id != 'demo':
        client = models.get_client_by_id(client_id)

    if not client:
        host = request.host.split(':')[0].lower()
        if host and host not in ('lumvi.net', 'www.lumvi.net', 'localhost', '127.0.0.1'):
            client = models.get_client_by_custom_domain(host)
            if client:
                client_id = client['client_id']
                app.logger.info(
                    f'[Widget] Custom domain match: host={host} client={client_id}'
                )

    if not client:
        client = {
            'client_id':        'demo',
            'company_name':     'Demo Company',
            'widget_color':     '#B8924A',
            'bot_name':         'Support',
            'bot_avatar':       '',
            'tagline':          'Typically replies instantly',
            'welcome_message':  'Hi! How can I help you today?',
            'fallback_message': '',
            'quick_replies':    ['What are your hours?', 'Pricing info', 'Contact us'],
            'remove_branding':  0,
            'logo_url':         '',
            'custom_css':       '',
            'contact':          {},
            'lead_triggers':    ['contact', 'sales', 'demo', 'speak', 'talk', 'human', 'agent'],
            'lead_q3':          '',
            'lead_q4':          '',
            'widget_theme':     'lumvi',
            'widget_font':      'dm_sans',
            'bubble_style':     'rounded',
            'header_color':     '',
        }
    else:
        client             = dict(client)
        branding_settings  = json.loads(client.get('branding_settings') or '{}')
        bot_settings       = branding_settings.get('bot_settings', {})
        branding           = branding_settings.get('branding', {})
        contact            = branding_settings.get('contact', {})

        client['bot_name']          = bot_settings.get('bot_name')         or client.get('company_name') or 'Support'
        client['bot_avatar']        = bot_settings.get('bot_avatar')        or ''
        client['bot_avatar_url']    = bot_settings.get('bot_avatar_url')    or ''
        client['tagline']           = bot_settings.get('status_line')       or 'Typically replies instantly'
        client['welcome_message']   = bot_settings.get('welcome_message')   or client.get('welcome_message') or 'Hi! How can I help you today?'
        client['fallback_message']  = bot_settings.get('fallback_message')  or ''
        client['quick_replies']     = [r for r in (bot_settings.get('quick_replies') or []) if r and str(r).strip()]
        client['featured_product']  = bot_settings.get('featured_product')  or {}
        client['lead_q3']           = bot_settings.get('lead_q3', '').strip()
        client['lead_q4']           = bot_settings.get('lead_q4', '').strip()
        client['widget_color']      = branding.get('primary_color')         or client.get('widget_color') or '#B8924A'
        client['remove_branding']   = branding.get('remove_branding',       client.get('remove_branding', 0))
        client['logo_url']          = branding.get('logo')                  or branding.get('logo_url') or ''
        client['custom_css']        = client.get('custom_css')              or ''
        client['contact']           = contact
        client['branding_settings'] = branding_settings
        # Color Mode (light/dark/auto) is the only mode picker exposed in
        # customize.html. Map it straight onto widget_theme — chat.html's
        # existing theme system already has 'light' and 'dark' branches;
        # 'auto' is a thin addition that starts from 'light' and lets a
        # prefers-color-scheme media query swap in 'dark's values.
        _cm = branding.get('color_mode', 'light')
        client['widget_theme']      = _cm if _cm in ('light', 'dark', 'auto') else 'light'
        client['widget_font']       = branding.get('widget_font',   'dm_sans')
        client['bubble_style']      = branding.get('bubble_style',  'rounded')
        client['corner_radius']     = branding.get('corner_radius', 'balanced')
        client['header_color']      = branding.get('header_color',  '')
        client['bubble_radius']     = branding.get('bubble_radius')
        client['bot_bubble_color']  = branding.get('bot_bubble_color',  '')
        client['user_bubble_color'] = branding.get('user_bubble_color', '')
        client['lead_triggers']     = branding_settings.get('bot_settings', {}).get(
            'lead_triggers', ['contact', 'sales', 'demo', 'speak', 'talk', 'human', 'agent']
        )

        # Business hours — check server-side so the widget gets a simple bool
        _bh = models.check_business_hours(client_id)
        client['is_online']   = _bh['is_open']
        client['offline_msg'] = _bh['offline_message']

    return render_template('chat.html', client=client)


# ── Health + admin ops ────────────────────────────────────────────────────────

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status':    'healthy',
        'timestamp': datetime.now().isoformat(),
        'version':   '1.0.0',
    })


@app.route('/admin/leads')
@login_required
def admin_leads():
    client_id = request.args.get('client_id')
    if not client_id:
        return 'Client ID required', 400
    if not models.verify_client_ownership(current_user.id, client_id):
        return 'Unauthorized', 403
    leads  = models.get_leads(client_id)
    client = models.get_client_by_id(client_id)
    return render_template('admin.html', leads=leads,
                           client_id=client_id, client=client)


@app.route('/api/admin/cache-stats')
@login_required
def cache_stats_endpoint():
    client_id = request.args.get('client_id', '')
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    stats = cache_utils.cache_stats(client_id)
    return jsonify({'success': True, **stats})


@app.route('/api/admin/cache-invalidate', methods=['POST'])
@login_required
def cache_invalidate_endpoint():
    data      = request.get_json() or {}
    client_id = data.get('client_id', '')
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    new_version = cache_utils.invalidate(client_id)
    app.logger.info(
        f'[Cache] Manual invalidation: client={client_id} '
        f'new_version={new_version} by user={current_user.id}'
    )
    return jsonify({'success': True, 'new_kb_version': new_version})


@app.route('/api/admin/backup', methods=['POST'])
def trigger_backup():
    import hmac
    auth_token  = request.headers.get('X-Admin-Token') or ''
    admin_token = os.environ.get('ADMIN_TOKEN', '')
    if not admin_token:
        app.logger.error('[Backup] ADMIN_TOKEN env var not set — endpoint disabled.')
        return jsonify({'success': False, 'error': 'Backup not configured'}), 503
    if not hmac.compare_digest(auth_token.encode(), admin_token.encode()):
        app.logger.warning(f'[Backup] Unauthorized attempt from {request.remote_addr}')
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401
    try:
        clients_dir = 'clients'
        if os.path.exists(clients_dir):
            for cid in os.listdir(clients_dir):
                if os.path.isdir(os.path.join(clients_dir, cid)):
                    pass  # backup_client_data(cid) — re-enable if needed
        return jsonify({
            'success':   True,
            'message':   'Backup completed',
            'timestamp': datetime.now().isoformat(),
        })
    except Exception as e:
        app.logger.error(f'[Backup] error: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/admin/reindex', methods=['POST'])
def trigger_reindex():
    import hmac
    auth_token  = request.headers.get('X-Admin-Token') or ''
    admin_token = os.environ.get('ADMIN_TOKEN', '')
    if not admin_token:
        app.logger.error('[Reindex] ADMIN_TOKEN env var not set — endpoint disabled.')
        return jsonify({'success': False, 'error': 'Not configured'}), 503
    if not hmac.compare_digest(auth_token.encode(), admin_token.encode()):
        app.logger.warning(f'[Reindex] Unauthorized attempt from {request.remote_addr}')
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401
    try:
        helper  = get_ai_helper(Config.GEMINI_API_KEY, Config.GEMINI_MODEL)
        results = helper.reindex_all_clients()
        succeeded   = {c: n for c, n in results.items() if n >= 0}
        failed      = {c: n for c, n in results.items() if n < 0}
        total_emb   = sum(succeeded.values())
        app.logger.info(
            f'[Reindex] complete — {len(succeeded)} OK, '
            f'{len(failed)} failed, {total_emb} embeddings stored'
        )
        return jsonify({
            'success':           len(failed) == 0,
            'clients_ok':        len(succeeded),
            'clients_failed':    len(failed),
            'failed_ids':        list(failed.keys()),
            'total_embeddings':  total_emb,
            'results':           results,
        })
    except Exception as e:
        app.logger.exception(f'[Reindex] fatal error: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/admin/set-plan', methods=['GET', 'POST'])
def admin_set_plan():
    ADMIN_SECRET = os.environ.get('ADMIN_SECRET', '')
    error = success = None
    if request.method == 'POST':
        secret      = request.form.get('secret')
        email       = request.form.get('email', '').strip().lower()
        plan        = request.form.get('plan', '').strip().lower()
        grace_days_raw = request.form.get('grace_days', '30').strip()
        valid_plans = ['ai_starter', 'ai_growth', 'ai_scale']
        if secret != ADMIN_SECRET:
            error = 'Invalid admin secret.'
        elif not email:
            error = 'Email is required.'
        elif plan not in valid_plans:
            error = f'Invalid plan. Must be one of: {", ".join(valid_plans)}'
        else:
            try:
                grace_days = int(grace_days_raw)
            except ValueError:
                grace_days = 30
            user = models.get_user_by_email(email)
            if not user:
                error = f'No user found with email: {email}'
            else:
                conn, cursor = models.get_db()
                if grace_days == 0:
                    # grace_days=0 is an explicit, deliberate permanent grant
                    # (no more free/enterprise tiers to special-case here —
                    # every plan is a paid subscription now).
                    cursor.execute(
                        '''UPDATE users SET plan_type = %s, upgraded_at = CURRENT_TIMESTAMP,
                               grace_period_ends_at = NULL, subscription_expires_at = NULL
                           WHERE email = %s''',
                        (plan, email)
                    )
                else:
                    # Was previously a bare `SET plan_type = %s` with no expiry
                    # at all — the downgrade cron (downgrade_expired_users)
                    # only acts on users with grace_period_ends_at or
                    # subscription_expires_at set in the past, so a manually
                    # granted plan with neither set became silently permanent
                    # regardless of intent. Defaults to 30 days now instead.
                    cursor.execute(
                        '''UPDATE users SET plan_type = %s, upgraded_at = CURRENT_TIMESTAMP,
                               grace_period_ends_at = CURRENT_TIMESTAMP + make_interval(days => %s)
                           WHERE email = %s''',
                        (plan, grace_days, email)
                    )
                conn.commit(); cursor.close(); conn.close()
                _plan_display = {'free': 'Free', 'ai_starter': 'Starter', 'ai_growth': 'Growth', 'ai_scale': 'Scale'}.get(plan, plan)
                success = f'✅ {email} updated to {_plan_display} plan.' + (
                    ' (permanent — no auto-downgrade)' if grace_days == 0
                    else f' (auto-downgrades after {grace_days} days unless renewed)'
                )

    return f'''<!DOCTYPE html>
<html>
<head><title>Lumvi Admin — Set Plan</title>
<style>
  body{{font-family:-apple-system,sans-serif;background:#0f172a;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:20px;}}
  .card{{background:#1e293b;border:1px solid rgba(255,255,255,.1);border-radius:16px;padding:40px;max-width:460px;width:100%;color:#f8fafc;}}
  h1{{font-size:22px;font-weight:800;margin-bottom:6px;}}
  p{{color:#64748b;font-size:14px;margin-bottom:28px;}}
  label{{display:block;font-size:13px;font-weight:600;color:#94a3b8;margin-bottom:6px;text-transform:uppercase;letter-spacing:.04em;}}
  input,select{{width:100%;padding:10px 14px;background:#0f172a;border:1px solid rgba(255,255,255,.1);border-radius:8px;color:#f8fafc;font-size:14px;margin-bottom:16px;}}
  button{{width:100%;padding:12px;background:#06b6d4;color:#0f172a;border:none;border-radius:8px;font-weight:800;font-size:15px;cursor:pointer;margin-top:4px;}}
  .success{{background:rgba(16,185,129,.15);border:1px solid rgba(16,185,129,.3);color:#34d399;padding:12px 16px;border-radius:8px;margin-bottom:20px;font-size:14px;}}
  .error{{background:rgba(239,68,68,.15);border:1px solid rgba(239,68,68,.3);color:#f87171;padding:12px 16px;border-radius:8px;margin-bottom:20px;font-size:14px;}}
  .warning{{color:#fbbf24;font-size:12px;margin-top:16px;text-align:center;}}
</style></head>
<body><div class="card">
  <h1>Admin — Set User Plan</h1>
  <p>Update any user account to a different plan tier.</p>
  {"<div class='success'>" + success + "</div>" if success else ""}
  {"<div class='error'>" + error + "</div>" if error else ""}
  <form method="POST">
    <label>Admin Secret</label>
    <input type="password" name="secret" placeholder="Enter admin secret" required>
    <label>User Email</label>
    <input type="email" name="email" placeholder="user@example.com" required>
    <label>New Plan</label>
    <select name="plan">
      <option value="free">Free</option>
      <option value="ai_starter">Starter</option>
      <option value="ai_growth">Growth</option>
      <option value="ai_scale">Scale</option>
    </select>
    <label>Auto-downgrade after</label>
    <select name="grace_days">
      <option value="7">7 days</option>
      <option value="30" selected>30 days</option>
      <option value="90">90 days</option>
      <option value="0">Never (permanent — use deliberately)</option>
    </select>
    <button type="submit">Update Plan</button>
  </form>
  <p class="warning">⚠️ Keep this URL private.</p>
</div></body></html>'''


@app.route('/admin/init-db-production', methods=['GET', 'POST'])
def init_db_production():
    if request.method == 'POST':
        secret = request.form.get('secret')
        if secret == 'your-secret-password-here':
            models.init_db()
            try:
                models.migrate_clients_table()
            except Exception as e:
                app.logger.warning(f'Clients migration helper failed: {e}')
            conn, cursor = models.get_db()
            conn.commit(); cursor.close(); conn.close()
            return '✅ Database initialized!'
        return '❌ Invalid secret'
    return '''<form method="POST">
        <input type="password" name="secret" placeholder="Admin secret">
        <button type="submit">Initialize DB</button>
    </form>'''


# ── Customize ─────────────────────────────────────────────────────────────────

@app.route('/customize')
@login_required
def customize_page():
    client_id = request.args.get('client_id')
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return 'Unauthorized', 403
    fresh_user  = models.get_user_by_id(current_user.id)
    plan_type   = (fresh_user or {}).get('plan_type', current_user.plan_type)
    plan_limits = PLAN_LIMITS.get(plan_type, PLAN_LIMITS['free'])
    if not plan_limits['customization']:
        return render_template('customize_upgrade.html',
                               user=current_user, plan_type=plan_type), 403
    client = models.get_client_by_id(client_id)
    branding_settings = {}
    if client and client.get('branding_settings'):
        try:
            branding_settings = json.loads(client['branding_settings'])
        except Exception:
            branding_settings = {}
    return render_template(
        'customize.html',
        user            = current_user,
        client_id       = client_id,
        client          = client,
        branding        = branding_settings,
        plan_type       = plan_type,
        plan_limits     = plan_limits,
        has_webhooks    = plan_limits.get('webhooks', False),
        has_analytics   = plan_limits.get('analytics', False),
        # TODO(legacy, out of scope): templates/customize.html may still
        # reference a `has_white_label` variable for white-label UI that no
        # longer applies to any plan — Jinja treats a missing var as falsy,
        # so this doesn't break rendering, but the template's white-label
        # section should be deleted there.
    )


@app.route('/api/admin/customize', methods=['POST'])
@login_required
def save_customization():
    try:
        data      = request.json
        client_id = data.get('client_id')
        if not client_id:
            return jsonify({'success': False, 'error': 'Client ID required'}), 400
        if not models.verify_client_ownership(current_user.id, client_id):
            return jsonify({'success': False, 'error': 'Unauthorized'}), 403
        client = models.get_client_by_id(client_id)
        if not client:
            return jsonify({'success': False, 'error': 'Client not found'}), 404

        fresh_user  = models.get_user_by_id(current_user.id)
        fresh_plan  = (fresh_user or {}).get('plan_type', 'ai_starter')
        plan_limits = PLAN_LIMITS.get(fresh_plan, PLAN_LIMITS['free'])

        incoming_integrations = data.get('integrations', {})
        if plan_limits['webhooks']:
            integrations = incoming_integrations
        else:
            integrations = {}
            if incoming_integrations.get('webhook_url'):
                app.logger.info(
                    f'[Limit] Webhook URL stripped for user {current_user.id} '
                    f'on plan "{fresh_plan}"'
                )

        incoming_vertical = data.get('vertical', 'general')
        vertical = incoming_vertical if incoming_vertical in VALID_VERTICALS else 'general'

        _hex_re         = re.compile(r'^#[0-9A-Fa-f]{6}$')
        incoming_branding = data.get('branding', {})
        _br_raw = incoming_branding.get('bubble_radius')
        if _br_raw is not None:
            try:
                incoming_branding['bubble_radius'] = max(0, min(22, int(_br_raw)))
            except (TypeError, ValueError):
                incoming_branding.pop('bubble_radius', None)
        for _ck in ('bot_bubble_color', 'user_bubble_color'):
            _v = str(incoming_branding.get(_ck, '')).strip()
            incoming_branding[_ck] = _v if _hex_re.match(_v) else ''

        branding_settings = {
            'branding':     incoming_branding,
            'contact':      data.get('contact', {}),
            'bot_settings': data.get('bot_settings', {}),
            'integrations': integrations,
            'vertical':     vertical,
        }
        branding_settings['contact'].setdefault('address', '')

        # Custom pipeline stage names — agency can rename the 6 fixed stage
        # keys to match the client's business (e.g. 'qualified' -> 'Site Visit
        # Booked'). Keys are restricted to the known stage set; values capped
        # at 30 chars. The underlying stage key stored on each lead never
        # changes — this only affects display labels.
        incoming_stage_labels = data.get('stage_labels', {})
        valid_stage_keys = {'new', 'contacted', 'qualified', 'proposal', 'closed', 'lost'}
        stage_labels = {}
        if isinstance(incoming_stage_labels, dict):
            for _sk, _sv in incoming_stage_labels.items():
                if _sk in valid_stage_keys:
                    _label = str(_sv).strip()[:30]
                    if _label:
                        stage_labels[_sk] = _label
        branding_settings['stage_labels'] = stage_labels

        # Availability schedule — null means "always online" (enforcement disabled)
        incoming_bh = data.get('business_hours')
        if incoming_bh and isinstance(incoming_bh, dict) and incoming_bh.get('timezone'):
            branding_settings['business_hours'] = {
                'timezone':       str(incoming_bh.get('timezone', 'UTC'))[:64],
                'offline_message':str(incoming_bh.get('offline_message', ''))[:300],
                'schedule':       incoming_bh.get('schedule', {}),
            }
        else:
            # Explicitly remove — user turned it off
            branding_settings.pop('business_hours', None)

        raw_qr = branding_settings['bot_settings'].get('quick_replies') or []
        branding_settings['bot_settings']['quick_replies'] = [
            r for r in raw_qr if r and str(r).strip()
        ]

        # White-label branding removal was an agency/enterprise-only feature
        # under the old architecture — no current plan (Starter/Growth/
        # Scale) offers it, so this is now always False regardless of what
        # the client sends.
        # TODO(legacy, out of scope): the `remove_branding` column on the
        # clients table (models.py) and any related UI in customize.html
        # are dead weight now and candidates for a future migration to drop.
        remove_branding = False
        branding_settings['branding']['remove_branding'] = remove_branding

        conn   = models.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''UPDATE clients SET branding_settings=%s, company_name=%s,
               widget_color=%s, welcome_message=%s, remove_branding=%s
               WHERE client_id=%s AND user_id=%s''',
            (
                json.dumps(branding_settings),
                data.get('branding', {}).get('company_name'),
                data.get('branding', {}).get('primary_color'),
                data.get('bot_settings', {}).get('welcome_message'),
                remove_branding,
                client_id,
                current_user.id,
            )
        )
        conn.commit(); cursor.close(); conn.close()
        app.logger.info(f'Customization saved for client: {client_id}')
        return jsonify({'success': True, 'message': 'Customization saved successfully'})
    except Exception as e:
        app.logger.error(f'Error saving customization: {e}')
        return jsonify({'success': False, 'error': 'Failed to save customization'}), 500


# ── Cart Recovery ────────────────────────────────────────────────────────────

@app.route('/cart-recovery')
@login_required
def cart_recovery_page():
    client_id = request.args.get('client_id')
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return 'Unauthorized', 403
    fresh_user  = models.get_user_by_id(current_user.id)
    plan_type   = (fresh_user or {}).get('plan_type', current_user.plan_type)
    plan_limits = PLAN_LIMITS.get(plan_type, PLAN_LIMITS['free'])
    client      = models.get_client_by_id(client_id) or {}
    return render_template(
        'cart_recovery.html',
        user        = current_user,
        client_id   = client_id,
        client      = client,
        plan_type   = plan_type,
        plan_limits = plan_limits,
    )


# ── Webhooks (outbound config) ────────────────────────────────────────────────

@app.route('/api/admin/webhooks', methods=['GET'])
@login_required
def get_webhooks_route():
    client_id = request.args.get('client_id', '')
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    if not PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free']).get('webhooks'):
        return jsonify({'success': False, 'error': 'Webhooks are not available on your current plan'}), 403
    return jsonify({'success': True, 'webhooks': models.get_webhooks(client_id)})


@app.route('/api/admin/webhooks', methods=['POST'])
@login_required
def save_webhooks_route():
    data      = request.json or {}
    client_id = data.get('client_id', '')
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    if not PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free']).get('webhooks'):
        return jsonify({'success': False, 'error': 'Webhooks are not available on your current plan'}), 403
    webhooks = data.get('webhooks', [])
    if len(webhooks) > 10:
        return jsonify({'success': False, 'error': 'Maximum 10 webhooks per client'}), 400
    count = models.save_webhooks(client_id, webhooks)
    app.logger.info(
        f'[Webhooks] Saved {count} webhooks client={client_id} user={current_user.id}'
    )
    return jsonify({'success': True, 'saved': count})


@app.route('/api/admin/webhooks/regenerate-secret', methods=['POST'])
@login_required
def regenerate_webhook_secret():
    data       = request.json or {}
    client_id  = data.get('client_id', '')
    webhook_id = data.get('webhook_id', '')
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    if not PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free']).get('webhooks'):
        return jsonify({'success': False, 'error': 'Webhooks are not available on your current plan'}), 403
    new_secret = models.regenerate_signing_secret(client_id, webhook_id)
    return jsonify({'success': True, 'signing_secret': new_secret})


@app.route('/api/admin/webhooks/logs', methods=['GET'])
@login_required
def get_webhook_logs_route():
    client_id = request.args.get('client_id', '')
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    if not PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free']).get('webhooks'):
        return jsonify({'success': False, 'error': 'Webhooks are not available on your current plan'}), 403
    return jsonify({'success': True, 'logs': models.get_webhook_logs(client_id, limit=20)})


@app.route('/api/admin/webhooks/test', methods=['POST'])
@login_required
def test_webhook():
    import time, hmac as _hmac, hashlib
    data        = request.json or {}
    webhook_url = data.get('webhook_url', '').strip()
    client_id   = data.get('client_id', '')
    webhook_id  = data.get('webhook_id', '')
    event_type  = data.get('event_type', 'test')
    if not webhook_url:
        return jsonify({'success': False, 'error': 'No webhook URL provided'}), 400
    if not PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free']).get('webhooks'):
        return jsonify({'success': False, 'error': 'Webhooks are not available on your current plan'}), 403
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    ts = datetime.utcnow().isoformat() + 'Z'
    SAMPLE_PAYLOADS = {
        'lead_captured':      {'event': 'lead_captured',      'client_id': client_id, 'timestamp': ts, 'data': {'name': 'Jane Smith', 'email': 'jane@example.com', 'phone': '+1 555 000 0000', 'company': 'Acme Corp'}},
        'conversation_ended': {'event': 'conversation_ended', 'client_id': client_id, 'timestamp': ts, 'data': {'session_id': 'sess_abc123', 'message_count': 6, 'resolved': True}},
        'faq_matched':        {'event': 'faq_matched',        'client_id': client_id, 'timestamp': ts, 'data': {'question': 'What are your business hours?', 'confidence': 0.94}},
        'message_sent':       {'event': 'message_sent',       'client_id': client_id, 'timestamp': ts, 'data': {'role': 'user', 'content': 'Do you offer refunds?'}},
        'test':               {'event': 'test',               'client_id': client_id, 'timestamp': ts, 'data': {'message': 'This is a test delivery from Lumvi.', 'sent_by': current_user.email}},
    }
    payload        = SAMPLE_PAYLOADS.get(event_type, SAMPLE_PAYLOADS['test'])
    signing_secret = models.get_signing_secret(client_id, webhook_id) if webhook_id else ''
    headers        = {
        'Content-Type':     'application/json',
        'X-Lumvi-Event':    event_type,
        'X-Lumvi-Delivery': str(uuid.uuid4()),
    }
    if signing_secret:
        body_bytes = json.dumps(payload).encode()
        sig = _hmac.new(signing_secret.encode(), body_bytes, hashlib.sha256).hexdigest()
        headers['X-Lumvi-Signature'] = f'sha256={sig}'

    t0 = time.time()
    try:
        resp        = requests.post(webhook_url, json=payload, headers=headers, timeout=10)
        duration_ms = int((time.time() - t0) * 1000)
        success     = 200 <= resp.status_code < 300
        resp_body   = resp.text[:500]
        if webhook_id:
            models.log_webhook_delivery(
                client_id=client_id, webhook_id=webhook_id,
                event_type=event_type, url=webhook_url,
                payload=payload, status_code=resp.status_code,
                response_text=resp_body, success=success, duration_ms=duration_ms,
            )
        return jsonify({
            'success':       success,
            'status_code':   resp.status_code,
            'duration_ms':   duration_ms,
            'response_body': resp_body,
            'payload_sent':  payload,
            'message':       f'HTTP {resp.status_code} · {duration_ms}ms',
        })
    except requests.exceptions.Timeout:
        return jsonify({'success': False, 'error': 'Request timed out (>10s)', 'payload_sent': payload})
    except requests.exceptions.ConnectionError:
        return jsonify({'success': False, 'error': 'Could not connect — check the URL', 'payload_sent': payload})
    except Exception as e:
        app.logger.error(f'[test-webhook] {e}')
        return jsonify({'success': False, 'error': str(e), 'payload_sent': payload})


# ── Onboarding store-connect ───────────────────────────────────────────────
# Shopify: real OAuth install flow (authorization_code grant against a
# centrally-registered Lumvi Partner/Dev-Dashboard app — see SHOPIFY_APP_
# CLIENT_ID/SECRET below). Replaces the old client_credentials/custom-app
# flow — Shopify stopped letting merchants create new legacy custom apps
# as of Jan 1 2026, so that path is a dead end for anyone new.
# WooCommerce: still the manual key-entry flow (no centrally-registered
# app needed — WooCommerce's REST API keys are just per-site credentials).
#
# This same flow now serves two entry points, not just one:
#   1. Logged-in dashboard user clicking "Connect Shopify" (original use
#      case — return_to=dashboard|onboarding, current_user already set).
#   2. Shopify itself initiating an install with no Lumvi session at all —
#      the merchant found/installed the app from Shopify (App Store listing
#      or a direct install link) rather than starting from Lumvi's own
#      onboarding wizard. There is no user_id to attach a client to yet.
# connect_shopify_callback() branches on current_user.is_authenticated to
# tell these apart, rather than splitting into two routes/callback URLs —
# Shopify's OAuth redirect_uri is registered once in the Partner Dashboard,
# so a single callback that handles both cases is less to keep in sync than
# two callbacks that must independently be kept correct.
#
# Both create the client record AND the platform credentials, since at
# this point in onboarding no client exists yet — except when the user
# already has one (reconnecting, or switching platforms), in which case
# both reuse it rather than creating a second one. Every plan caps at 1
# connected store; creating a second client here would silently violate
# that and orphan the first one.

SHOPIFY_APP_CLIENT_ID     = os.environ.get('SHOPIFY_APP_CLIENT_ID', '')
# NOTE: an earlier version of this code used a separate
# SHOPIFY_EXTENSION_UUID env var here, sourced from the "UID" field shown
# in the Shopify Dev Dashboard's extension Versions page. That UID is a
# build-time identifier Shopify CLI uses internally to map extension code
# to Shopify's records during `shopify app deploy` — it is NOT the value
# the activateAppId deep link needs. Confirmed against Shopify's current
# docs: the deep link format is activateAppId={api_key}/{handle}, where
# api_key is the app's own client ID — i.e. SHOPIFY_APP_CLIENT_ID above.
# See build_embed_activation_urls below.
SHOPIFY_APP_CLIENT_SECRET = os.environ.get('SHOPIFY_APP_CLIENT_SECRET', '')
SHOPIFY_APP_SCOPES        = 'read_products,read_orders'
SHOPIFY_API_VERSION       = '2026-07'  # matches commerce_adapters.ShopifyAdapter.API_VERSION


def build_embed_activation_urls(shop: str) -> dict:
    """
    Single canonical source for the two URLs every post-connect surface
    (onboarding, dashboard, integrations page) needs to prompt a merchant
    to activate the Lumvi theme app embed. Returns:

      {'activate_embed_url': str | None, 'manual_activation_url': str}

    Deep link straight to the theme editor with the chat embed block
    already toggled on and previewed — app embed blocks are OFF by
    default for every app on every store (Shopify platform behavior, not
    something this app can change), so there's still one merchant click
    after OAuth: Save. This link is how Shopify's own docs recommend
    closing that gap — see "App embed block deep linking" at
    https://shopify.dev/docs/apps/build/online-store/theme-app-extensions/configuration

    {handle} is the extension handle as registered with Shopify —
    "lumvi-chat-embed", confirmed in the Dev Dashboard's Versions page —
    not the block's liquid filename. The identifier before it is the
    app's own client ID (SHOPIFY_APP_CLIENT_ID), per Shopify's
    activateAppId={api_key}/{handle} format — NOT the extension "uid"
    shown on the Dev Dashboard's Versions page, which is a build-time
    identifier Shopify CLI uses only to map extension code during
    `shopify app deploy` and has no runtime meaning here.

    Fails safe: if SHOPIFY_APP_CLIENT_ID isn't configured, omit the deep
    link entirely rather than send merchants to a broken URL. The
    merchant still successfully connected — they just get the manual
    fallback instead of a broken "Activate" button.

    KNOWN PLATFORM ISSUE: multiple Shopify community threads (2025-2026)
    report this exact activateAppId deep link intermittently failing on
    Shopify's side regardless of whether api_key or an extension UUID is
    used — sometimes redirecting to context=theme instead of context=apps,
    or showing "App embed does not exist" even with correct values. No
    official fix as of this writing. So this link is treated as a
    best-effort shortcut, never the only path — manual_activation_url is
    always populated so every calling page's UI can fall back immediately
    if the merchant reports the deep link didn't work (see
    static/js/embed-activation.js, the single shared front-end module
    that consumes these two values).
    """
    activate_embed_url = None
    manual_activation_url = f'https://{shop}/admin/themes/current/editor?context=apps'

    if SHOPIFY_APP_CLIENT_ID:
        activate_embed_url = (
            f'https://{shop}/admin/themes/current/editor'
            f'?context=apps&template=index&activateAppId={SHOPIFY_APP_CLIENT_ID}/chat-embed'
        )
    else:
        app.logger.warning('[Shopify OAuth] SHOPIFY_APP_CLIENT_ID not set — omitting '
                           'activate_embed_url, falling back to manual instructions only')

    return {
        'activate_embed_url': activate_embed_url,
        'manual_activation_url': manual_activation_url,
    }


def _get_or_create_client_for_user(user_id, company_name, vertical='ecommerce'):
    """
    Returns the user's existing client_id if they already have one
    (every plan caps at 1 store — reconnecting or switching platforms
    should reuse the same client row, not create a second one), else
    creates a new client and returns its id.
    """
    existing = models.get_user_clients(user_id)
    if existing:
        return existing[0]['client_id']
    return models.create_client(user_id, company_name, vertical=vertical)


def _fetch_shopify_shop_email(shop_domain: str, access_token: str) -> str:
    """
    Fetch the store's contact email via the Admin API, right after a
    headless install's token exchange — used to find-or-create the Lumvi
    account that owns this store (see models.create_or_link_shopify_user).
    Returns '' on any failure; the caller falls back to a synthetic
    placeholder rather than blocking account creation on it.
    """
    try:
        resp = requests.get(
            f'https://{shop_domain}/admin/api/{SHOPIFY_API_VERSION}/shop.json',
            headers={'X-Shopify-Access-Token': access_token},
            timeout=10,
        )
        if resp.status_code != 200:
            return ''
        shop_data = (resp.json() or {}).get('shop', {}) or {}
        return (shop_data.get('email') or shop_data.get('customer_email') or '').strip().lower()
    except requests.exceptions.RequestException as e:
        app.logger.warning(f'[Shopify OAuth] shop.json fetch failed for {shop_domain}: {e}')
        return ''


def _register_shopify_webhooks_background(shop_domain: str, access_token: str,
                                          client_id: str, webhook_url: str):
    """
    Runs in a background thread — see the call site in
    connect_shopify_callback for why (W3 fix: registering 8 topics
    synchronously inside the OAuth callback added 1.5-4s of sequential
    Shopify API calls to the redirect the merchant is waiting on;
    Shopify's own guidance is that a merchant should land in the app UI
    immediately after approving, not wait on it).

    Safe to background: this only makes outbound HTTPS calls to Shopify's
    API (via ShopifyAdapter.register_webhooks -> requests.post) — no
    Flask request context, no app context, no DB access, so nothing
    request-scoped needs to survive past the point this thread starts.
    app.logger is a plain, thread-safe logging.Logger and works fine
    called this way (current_app.logger would NOT — that needs an active
    app context this thread doesn't have, which is why this uses the
    module-level `app` object directly instead).

    webhook_url must already be resolved (via url_for) by the caller,
    before this function is invoked — url_for() itself needs a request
    context, which a spawned thread doesn't have.
    """
    try:
        adapter = commerce_adapters.ShopifyAdapter({'shop_domain': shop_domain, 'access_token': access_token})
        webhook_results = adapter.register_webhooks(webhook_url)
        failed_topics = [t for t, success in webhook_results.items() if not success]
        if failed_topics:
            app.logger.warning(f'[Shopify OAuth] webhook registration incomplete shop={shop_domain} '
                               f'client={client_id} failed_topics={failed_topics}')
        else:
            app.logger.info(f'[Shopify OAuth] webhooks registered shop={shop_domain} client={client_id}')
    except Exception as e:
        app.logger.error(f'[Shopify OAuth] webhook registration failed shop={shop_domain} client={client_id}: {e}')


def _shopify_oauth_error_redirect(message: str, return_to: str, authenticated: bool, client_id: str = None):
    """
    Routes a Shopify OAuth failure to a safe destination.

    A logged-in user's flow keeps the existing behaviour — bounce back to
    wherever they started (dashboard, onboarding, or now integrations.html
    for an agency user connecting a specific managed client), with ?error=
    for that page's own JS to display. A headless (Shopify-initiated)
    install has no session yet, so those pages would just redirect to
    login and the error message would be lost — flash it and land on the
    login page instead, the same pattern blueprints/auth.py's
    google_callback already uses for its own OAuth failures.

    client_id (when known) is threaded back into the integrations.html
    redirect so the right client stays selected on error — that page reads
    ?client_id= as a deep-link preselect already (see its DOMContentLoaded
    handler), this just reuses it.
    """
    if not authenticated:
        flash(message, 'error')
        return redirect(url_for('auth.login'))
    if return_to == 'integrations':
        from urllib.parse import urlencode
        url = url_for('integrations_page')
        params = {'error': message}
        if client_id:
            params['client_id'] = client_id
        return redirect(f'{url}?{urlencode(params)}')
    return_endpoint = 'auth.dashboard' if return_to == 'dashboard' else 'auth.onboarding'
    return redirect(url_for(return_endpoint, error=message))


@app.route('/connect/shopify/install')
def connect_shopify_install():
    """
    Step 1 of the OAuth flow — redirect the merchant's browser to
    Shopify's install/consent screen. Shopify redirects back to
    connect_shopify_callback() below after they approve.

    No longer @login_required — this is now Lumvi's Shopify App URL, which
    Shopify itself opens (App Store listing, or a direct install link) with
    no Lumvi session in play at all, not just a link a logged-in dashboard
    user clicks. current_user.is_authenticated (checked here and again in
    the callback) is what tells the two cases apart; see the module comment
    above _get_or_create_client_for_user for why one shared route handles
    both instead of splitting into two.

    return_to=dashboard|onboarding|integrations controls where a logged-in
    user lands afterward — set by whichever page's "Connect" button
    initiated this (dashboard_enterprise.html's reconnect modal, onboarding.html's
    wizard, or integrations.html for an agency user managing several
    clients), so someone connecting later doesn't get bounced somewhere
    they didn't come from. Meaningless for a headless install (nobody's
    logged in yet) — _shopify_oauth_error_redirect and the callback's
    success path both check is_authenticated independently rather than
    trusting return_to to imply it.

    client_id (optional) lets an authenticated user target a SPECIFIC
    client they already own — integrations.html's client selector passes
    this, since an agency user can own several. Without it, the
    authenticated branch falls back to _get_or_create_client_for_user
    (their first/only client) — correct for dashboard_enterprise.html and
    onboarding.html, which only ever operate on "the current user's one
    store", but wrong for integrations.html's multi-client case. Ownership
    is verified here (models.verify_client_ownership) rather than trusted
    from the query string, and a bad client_id fails loudly rather than
    silently falling back to "their first client" — connecting the wrong
    agency client's store to Shopify is the kind of mistake that should be
    visible immediately, not guessed around.

    shop_domain (existing param, from onboarding/dashboard/integrations'
    own forms) and shop (Shopify's standard param name, used when Shopify
    itself opens this URL) are both accepted — same validation either way.
    """
    authenticated = current_user.is_authenticated
    return_to_raw = request.args.get('return_to')
    return_to = return_to_raw if return_to_raw in ('dashboard', 'integrations') else 'onboarding'

    requested_client_id = (request.args.get('client_id') or '').strip()
    verified_client_id = None
    if authenticated and requested_client_id:
        if models.verify_client_ownership(current_user.id, requested_client_id):
            verified_client_id = requested_client_id
        else:
            app.logger.warning(f'[Shopify OAuth] user={current_user.id} requested '
                               f'client_id={requested_client_id} they do not own')
            return _shopify_oauth_error_redirect("That client wasn't found on your account.", return_to, authenticated)

    shop_domain = (request.args.get('shop_domain') or request.args.get('shop') or '').strip().lower()
    if not shop_domain:
        return _shopify_oauth_error_redirect("Please enter your store's domain first.", return_to, authenticated, verified_client_id)
    if not shop_domain.endswith('.myshopify.com'):
        shop_domain = f'{shop_domain}.myshopify.com'
    if not re.match(r'^[a-z0-9][a-z0-9\-]*\.myshopify\.com$', shop_domain):
        return _shopify_oauth_error_redirect("That doesn't look like a valid Shopify store domain.", return_to, authenticated, verified_client_id)

    if not SHOPIFY_APP_CLIENT_ID:
        app.logger.error('[Shopify OAuth] SHOPIFY_APP_CLIENT_ID not set')
        return _shopify_oauth_error_redirect('Shopify connection is not configured yet. Contact support@lumvi.net.', return_to, authenticated, verified_client_id)

    # CSRF protection — Shopify echoes this back on callback; must match
    # what we stored in the session for this same browser/user.
    state = secrets.token_urlsafe(32)
    session['shopify_oauth_state']     = state
    session['shopify_oauth_shop']      = shop_domain
    session['shopify_oauth_return_to'] = return_to
    session['shopify_oauth_client_id'] = verified_client_id

    redirect_uri = url_for('connect_shopify_callback', _external=True)
    authorize_url = (
        f'https://{shop_domain}/admin/oauth/authorize'
        f'?client_id={SHOPIFY_APP_CLIENT_ID}'
        f'&scope={SHOPIFY_APP_SCOPES}'
        f'&redirect_uri={requests.utils.quote(redirect_uri, safe="")}'
        f'&state={state}'
    )
    return redirect(authorize_url)


@app.route('/connect/shopify/callback')
def connect_shopify_callback():
    """
    Step 2 — Shopify redirects here after the merchant approves (or
    denies) the install. Verifies the request actually came from
    Shopify (HMAC) and matches the install we started (state), exchanges
    the one-time code for a permanent access token, resolves which Lumvi
    account owns it (see the account-resolution block below), and saves
    the connection.

    No longer @login_required, for the same reason connect_shopify_install
    no longer is — see that route's docstring.
    """
    authenticated = current_user.is_authenticated

    shop  = (request.args.get('shop') or '').strip().lower()
    code  = request.args.get('code')
    state = request.args.get('state')
    hmac_param = request.args.get('hmac')

    expected_state = session.pop('shopify_oauth_state', None)
    expected_shop  = session.pop('shopify_oauth_shop', None)
    return_to      = session.pop('shopify_oauth_return_to', 'onboarding')
    target_client_id = session.pop('shopify_oauth_client_id', None)  # set only by integrations.html's client-scoped connect — see connect_shopify_install

    if not code or not shop:
        return _shopify_oauth_error_redirect('Shopify did not return the expected connection details. Please try again.', return_to, authenticated, target_client_id)
    if not expected_state or not secrets.compare_digest(state or '', expected_state):
        app.logger.warning(f'[Shopify OAuth] state mismatch for shop={shop}')
        return _shopify_oauth_error_redirect('This connection link expired or was already used. Please try connecting again.', return_to, authenticated, target_client_id)
    if expected_shop and shop != expected_shop:
        app.logger.warning(f'[Shopify OAuth] shop mismatch: expected={expected_shop} got={shop}')
        return _shopify_oauth_error_redirect('Something looked off with that connection. Please try again.', return_to, authenticated, target_client_id)

    # Verify Shopify actually signed this callback — without this, anyone
    # could forge a request to this URL with an arbitrary shop/code.
    # urlencode(sorted(...)) matches Shopify's documented algorithm exactly
    # (percent-encodes values) rather than a naive string join — today's
    # params (shop/code/state/timestamp) wouldn't need escaping in practice,
    # but this is the actually-documented-correct form, not a guess.
    from urllib.parse import urlencode
    params = request.args.to_dict()
    params.pop('hmac', None)
    sorted_params = urlencode(sorted(params.items()))
    computed_hmac = hmac.new(
        SHOPIFY_APP_CLIENT_SECRET.encode(), sorted_params.encode(), hashlib.sha256
    ).hexdigest()
    if not hmac_param or not hmac.compare_digest(computed_hmac, hmac_param):
        app.logger.error(f'[Shopify OAuth] HMAC verification failed for shop={shop}')
        return _shopify_oauth_error_redirect('Could not verify that connection request. Please try again.', return_to, authenticated, target_client_id)

    # Exchange the one-time code for a permanent access token.
    try:
        resp = requests.post(
            f'https://{shop}/admin/oauth/access_token',
            json={
                'client_id':     SHOPIFY_APP_CLIENT_ID,
                'client_secret': SHOPIFY_APP_CLIENT_SECRET,
                'code':          code,
            },
            timeout=10,
        )
        if resp.status_code != 200:
            raise ValueError(f'token exchange returned http_{resp.status_code}')
        access_token = resp.json().get('access_token')
        if not access_token:
            raise ValueError('token exchange response had no access_token')
    except Exception as e:
        app.logger.error(f'[Shopify OAuth] token exchange failed for shop={shop}: {e}')
        return _shopify_oauth_error_redirect("Couldn't complete the connection with Shopify. Please try again.", return_to, authenticated, target_client_id)

    # ── Account resolution ──────────────────────────────────────────────
    # Four cases:
    #  1. A logged-in user connecting a SPECIFIC client they already own —
    #     integrations.html's client selector (an agency user can own
    #     several clients; see connect_shopify_install's docstring for why
    #     this needs its own case rather than falling through to #2).
    #  2. A logged-in dashboard/onboarding user connecting/reconnecting
    #     their own (single) store — unchanged from before.
    #  3. A headless install of a shop Lumvi already knows (reinstall, or
    #     the merchant re-approving after uninstalling) — reuse that same
    #     client and log in whichever user already owns it, rather than
    #     creating a duplicate account for the same store.
    #  4. A headless install of a brand-new shop — find-or-create a Lumvi
    #     account by the store's contact email (mirrors
    #     create_or_link_google_user's shape) and log them straight in,
    #     no password step at install time.
    try:
        company_name = shop.replace('.myshopify.com', '').replace('-', ' ').title()

        if authenticated and target_client_id:
            client_id = target_client_id  # ownership already verified in connect_shopify_install
        elif authenticated:
            client_id = _get_or_create_client_for_user(current_user.id, company_name)
        else:
            existing_client_id = _webhooks.get_client_id_by_shopify_shop(shop)
            if existing_client_id:
                owner_client = models.get_client_by_id(existing_client_id)
                owner_user   = models.get_user_by_id(owner_client['user_id']) if owner_client else None
                if not owner_user:
                    raise RuntimeError(f'shop={shop} maps to client={existing_client_id} but its owning user was not found')
                login_user(User(owner_user), remember=True)
                session.permanent = True
                session['_user_cache'] = dict(owner_user)
                client_id = existing_client_id
            else:
                shop_email = _fetch_shopify_shop_email(shop, access_token)
                user_data  = models.create_or_link_shopify_user(shop_email)
                if not user_data:
                    raise RuntimeError('create_or_link_shopify_user returned None')
                login_user(User(user_data), remember=True)
                session.permanent = True
                session['_user_cache'] = dict(user_data)
                client_id = _get_or_create_client_for_user(user_data['id'], company_name)

        # Same per-plan integration cap create_platform_integration() (the
        # legacy manual-connect endpoint) already enforces — see the
        # PLAN_LIMITS comment block above ('integrations_limit': 1 on
        # free/starter, 5 on growth, None/unlimited on scale). Reconnecting
        # Shopify itself never counts against the cap, only connecting a
        # genuinely NEW platform to a client that's already at its limit —
        # same rule create_platform_integration uses.
        #
        # Deliberately NOT using create_platform_integration's exact
        # current_count expression here: despite list_integrations()'s
        # docstring ("List all active integrations"), it doesn't actually
        # filter is_active — every row counts, including ones deactivated
        # by a prior disconnect or (as of this migration) an app/uninstalled
        # webhook. That's a latent bug in the legacy endpoint's identical
        # check too (not fixed here — out of scope for this specific fix),
        # but it matters more now: app/uninstalled actively creates
        # deactivated rows on every Shopify uninstall, so a merchant
        # reinstalling their OWN store could otherwise be wrongly blocked
        # by their own prior (inactive) connection. Counting only
        # is_active rows avoids inheriting that here.
        existing = _webhooks.get_integration(client_id, 'shopify')
        if not existing:
            plan_limits = PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free'])
            integrations_limit = plan_limits.get('integrations_limit')
            if integrations_limit is not None:
                active_count = sum(1 for i in _webhooks.list_integrations(client_id) if i['is_active'])
                if active_count >= integrations_limit:
                    return _shopify_oauth_error_redirect(
                        f'Your plan allows up to {integrations_limit} connected integration(s). '
                        f'Upgrade to connect more.', return_to, authenticated, target_client_id)

        merged_config = dict((existing or {}).get('platform_config') or {})
        merged_config.update({
            'shop_domain':          shop,
            'access_token':         access_token,
            'order_lookup_enabled': True,
            'inventory_enabled':    True,
        })
        # OAuth access tokens don't need a client_id/client_secret pair —
        # drop any stale ones from a prior client-credentials connection.
        merged_config.pop('shopify_client_id', None)
        merged_config.pop('shopify_client_secret', None)

        # webhook_secret: SHOPIFY_APP_CLIENT_SECRET, not a random per-client
        # value. Once webhooks are auto-registered (next phase), Shopify
        # signs every webhook it sends this app with the app's OWN client
        # secret — a single value shared across every merchant's store, not
        # something scoped per shop. _verify_shopify_signature() (webhooks.py)
        # already compares against whatever's stored here, so storing the
        # app secret is the only change needed to make that check correct;
        # a random secret would make every auto-registered webhook fail
        # verification. (client_integrations.webhook_secret is NOT NULL, so
        # this column still needs a value even though it isn't merchant-
        # specific for Shopify the way it is for the other platforms.)
        ok = _webhooks.upsert_integration(client_id, 'shopify', SHOPIFY_APP_CLIENT_SECRET, merged_config)
        if not ok:
            raise RuntimeError('upsert_integration returned False')
    except Exception as e:
        app.logger.error(f'[Shopify OAuth] save failed for shop={shop}: {e}')
        return _shopify_oauth_error_redirect('Connected to Shopify, but saving the connection failed. Please try again.', return_to, authenticated, target_client_id)

    # Auto-register webhooks — the actual point of this migration (no more
    # "go paste this URL into Settings → Notifications"). Two things
    # changed here versus the original Phase 2 version, both addressing
    # findings from the production readiness audit:
    #
    #  - W4: order/checkout/app-uninstalled topics are now ALSO declared
    #    in shopify.app.toml's [[webhooks.subscriptions]] blocks, which
    #    Shopify auto-manages on install/uninstall with zero app code —
    #    that's the primary mechanism now, per Shopify's current
    #    recommendation for a topic set that's fixed and known at build
    #    time (it never varies per merchant). This call is a self-healing
    #    backup: if the TOML-declared subscriptions aren't active for some
    #    reason (e.g. deployed before the TOML config was verified against
    #    a real CLI scaffold — see that file's own caveat), this still
    #    ensures coverage. Calling it in ADDITION to the TOML declarations
    #    is harmless — Shopify reports an already-exists userError for
    #    anything already covered, which register_webhooks() already
    #    treats as success, not a failure to retry or alert on.
    #  - W3: this used to run synchronously inside this request — 8
    #    sequential Shopify API calls, 1.5-4s added to the redirect the
    #    merchant is waiting on. Now backgrounded (daemon thread): the
    #    merchant lands in the app UI immediately, registration finishes a
    #    moment later. Deliberately not awaited/joined — a registration
    #    hiccup shouldn't undo an otherwise-successful connection, and
    #    search/order-lookup already work via direct API calls with no
    #    webhooks involved at all either way. url_for() is resolved here,
    #    before handing off to the thread, since it needs this request's
    #    context and the background thread won't have one.
    webhook_url = url_for('shopify_webhook', client_id=client_id, _external=True)
    threading.Thread(
        target=_register_shopify_webhooks_background,
        args=(shop, access_token, client_id, webhook_url),
        daemon=True,
    ).start()

    app.logger.info(f'[Shopify OAuth] connected shop={shop} client={client_id} user={current_user.id} headless={not authenticated}')

    embed_urls = build_embed_activation_urls(shop)

    if return_to == 'dashboard':
        return redirect(url_for('auth.dashboard', connected=1, **embed_urls))
    if return_to == 'integrations':
        return redirect(url_for('integrations_page', client_id=client_id, connected=1, **embed_urls))
    return redirect(url_for('auth.onboarding', step=2, connected=1, client_id=client_id, **embed_urls))



@app.route('/connect/woocommerce/manual', methods=['POST'])
@login_required
def connect_woocommerce_manual():
    data            = request.json or {}
    store_url       = (data.get('store_url') or '').strip()
    consumer_key    = (data.get('consumer_key') or '').strip()
    consumer_secret = (data.get('consumer_secret') or '').strip()

    if not store_url or not consumer_key or not consumer_secret:
        return jsonify({'success': False, 'error': 'Store URL and both API keys are required.'}), 400
    if not store_url.startswith('http'):
        store_url = f'https://{store_url}'
    store_url = store_url.rstrip('/')

    # Verify the keys actually work against this store's REST API before
    # creating anything — same reasoning as the Shopify check above.
    try:
        resp = requests.get(
            f'{store_url}/wp-json/wc/v3/products',
            params={'per_page': 1},
            auth=(consumer_key, consumer_secret),
            timeout=10,
        )
        if resp.status_code == 401:
            return jsonify({'success': False, 'error': "Those API keys were rejected — please double-check them."}), 400
        if resp.status_code != 200:
            return jsonify({
                'success': False,
                'error': f"Couldn't reach the WooCommerce REST API at that URL (HTTP {resp.status_code}).",
            }), 400
    except requests.exceptions.RequestException:
        return jsonify({
            'success': False,
            'error': "Couldn't reach that store. Check the URL and try again.",
        }), 400

    try:
        from urllib.parse import urlparse
        host = urlparse(store_url).netloc or store_url
        company_name = host.replace('www.', '').split('.')[0].replace('-', ' ').title()
        client_id = _get_or_create_client_for_user(current_user.id, company_name)

        ok = _webhooks.upsert_integration(
            client_id, 'woocommerce',
            secrets.token_hex(32),  # webhook_secret for inbound order webhooks
            {
                'store_url':        store_url,
                'consumer_key':     consumer_key,
                'consumer_secret':  consumer_secret,
                'order_lookup_enabled': True,
                'inventory_enabled':    True,
            },
        )
        if not ok:
            return jsonify({'success': False, 'error': 'Could not save connection.'}), 500
        return jsonify({'success': True, 'client_id': client_id})
    except Exception as e:
        app.logger.error(f'[connect_woocommerce_manual] {e}')
        return jsonify({'success': False, 'error': 'Something went wrong saving your store. Please try again.'}), 500


# ── Platform integrations (Shopify / Acuity inbound webhooks) ─────────────────

@app.route('/integrations')
@login_required
def integrations_page():
    fresh_user  = models.get_user_by_id(current_user.id)
    plan_type   = (fresh_user or {}).get('plan_type', current_user.plan_type)
    plan_limits = PLAN_LIMITS.get(plan_type, PLAN_LIMITS['free'])
    if not plan_limits.get('webhooks'):
        return redirect(url_for('auth.dashboard') + '?upgrade=webhooks')
    clients  = models.get_user_clients(current_user.id)
    base_url = os.environ.get('APP_BASE_URL', 'https://app.lumvi.ai')
    return render_template(
        'integrations.html',
        user=current_user, plan_type=plan_type,
        plan_limits=plan_limits, clients=clients, base_url=base_url,
    )


@app.route('/api/integrations/<client_id>', methods=['POST'])
@login_required
def create_platform_integration(client_id):
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    plan_limits = PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free'])
    if not plan_limits.get('webhooks'):
        return jsonify({'success': False, 'error': 'Webhooks are not available on your current plan'}), 403
    data     = request.get_json(force=True) or {}
    platform = (data.get('platform') or '').lower().strip()
    secret   = (data.get('webhook_secret') or '').strip()
    config   = data.get('platform_config') or {}
    enable_agent_actions = bool(data.get('enable_agent_actions'))
    if platform not in ('shopify', 'acuity', 'calendly', 'woocommerce', 'square'):
        return jsonify({'success': False, 'error': 'platform must be shopify, acuity, calendly, woocommerce, or square'}), 400
    if not secret:
        return jsonify({'success': False, 'error': 'webhook_secret is required'}), 400
    if enable_agent_actions and not plan_limits.get('agentic_actions'):
        # Same gate /api/agent-actions/<client_id>/integrations POST uses —
        # this route can now also provision a client_ext_integrations row,
        # so it shouldn't be a side-door around that plan check.
        return jsonify({'success': False, 'error': 'Agent actions require the Growth or Scale plan'}), 403

    # Starter/Growth/Scale count-based integration cap (1 / 5 / unlimited —
    # see upgrade-shopify.html). Only counts against the cap if this is a
    # NEW platform for this client —
    # rotating the secret on an already-connected platform doesn't add to
    # the count.
    integrations_limit = plan_limits.get('integrations_limit')
    if integrations_limit is not None and not _webhooks.get_integration(client_id, platform):
        current_count = len(_webhooks.list_integrations(client_id))
        if current_count >= integrations_limit:
            return jsonify({
                'success': False,
                'error': f'Your plan allows up to {integrations_limit} connected integration(s). Upgrade to connect more.'
            }), 403

    base_url    = os.environ.get('APP_BASE_URL', 'https://app.lumvi.ai')
    webhook_url = f'{base_url}/webhooks/{platform}/{client_id}'

    # Shopify with client_id+client_secret also feeds commerce_adapters.py's
    # live order/inventory reads (tools.lookup_order / tools.search_products)
    # via a client-credentials-grant token that's fetched/refreshed
    # automatically. See shopify_connect.py.
    if platform == 'shopify' and config.get('shopify_client_id') and config.get('shopify_client_secret'):
        result = shopify_connect.connect_shopify(
            client_id=client_id,
            shop_domain=config.get('shop_domain', ''),
            shopify_client_id=config.get('shopify_client_id', ''),
            shopify_client_secret=config.get('shopify_client_secret', ''),
            webhook_secret=secret,
            enable_order_lookup=bool(config.get('order_lookup_enabled', True)),
            enable_inventory=bool(config.get('inventory_enabled', True)),
            enable_agent_actions=enable_agent_actions,
        )
        if not result.get('success'):
            if 'error' in result:
                # Bad input caught before anything was written (e.g. blank
                # shop_domain/client_id/client_secret) — same 400 the old
                # path used for a blank webhook_secret.
                return jsonify({'success': False, 'error': result['error']}), 400
            error = '; '.join(result.get('errors', [])) or 'Failed to save integration'
            return jsonify({'success': False, 'error': error}), 500
        app.logger.info(
            f'[Integration] user={current_user.id} connected platform=shopify client={client_id} '
            f'agent_actions_integration={result.get("agent_actions_integration_id")}'
        )
        return jsonify({
            'success':                      True,
            'platform':                     platform,
            'webhook_url':                  webhook_url,
            'instructions':                 _webhooks._onboarding_instructions(platform, webhook_url),
            'agent_actions_integration_id': result.get('agent_actions_integration_id'),
            'actions_created':              result.get('actions_created', 0),
            'warnings':                     result.get('errors', []),
        }), 200

    # Every other platform — and Shopify with no access_token (webhook
    # sync only, no live order/inventory reads) — merges with any existing
    # platform_config rather than replacing it outright. upsert_integration's
    # ON CONFLICT does a full replace, not a merge — openEditModal's
    # "rotate secret" flow deliberately sends an EMPTY platform_config (it
    # only touches the secret), so without this merge every secret
    # rotation would silently wipe out an existing access_token /
    # consumer_key / consumer_secret.
    existing      = _webhooks.get_integration(client_id, platform)
    merged_config = dict((existing or {}).get('platform_config') or {})
    merged_config.update(config)
    ok = _webhooks.upsert_integration(client_id, platform, secret, merged_config)
    if not ok:
        return jsonify({'success': False, 'error': 'Failed to save integration'}), 500
    app.logger.info(
        f'[Integration] user={current_user.id} connected platform={platform} client={client_id}'
    )
    return jsonify({
        'success':      True,
        'platform':     platform,
        'webhook_url':  webhook_url,
        'instructions': _webhooks._onboarding_instructions(platform, webhook_url),
    }), 200


@app.route('/api/integrations/<client_id>', methods=['GET'])
@login_required
def list_platform_integrations(client_id):
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    if not PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free']).get('webhooks'):
        return jsonify({'success': False, 'error': 'Webhooks are not available on your current plan'}), 403
    integrations = _webhooks.list_integrations(client_id)
    base_url     = os.environ.get('APP_BASE_URL', 'https://app.lumvi.ai')
    for i in integrations:
        i['webhook_url'] = f'{base_url}/webhooks/{i["platform"]}/{client_id}'
    return jsonify({'success': True, 'integrations': integrations}), 200


@app.route('/api/integrations/<client_id>/<platform>', methods=['DELETE'])
@login_required
def delete_platform_integration(client_id, platform):
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    if not PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free']).get('webhooks'):
        return jsonify({'success': False, 'error': 'Webhooks are not available on your current plan'}), 403
    if platform not in ('shopify', 'acuity', 'calendly', 'woocommerce', 'square'):
        return jsonify({'success': False, 'error': 'Unknown platform'}), 400
    ok = _webhooks.delete_integration(client_id, platform)
    if not ok:
        return jsonify({'success': False, 'error': 'Failed to deactivate integration'}), 500
    app.logger.info(
        f'[Integration] user={current_user.id} disconnected platform={platform} client={client_id}'
    )
    return jsonify({'success': True, 'platform': platform}), 200


@app.route('/api/integrations/<client_id>/log', methods=['GET'])
@login_required
def get_integration_log(client_id):
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    if not PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free']).get('webhooks'):
        return jsonify({'success': False, 'error': 'Webhooks are not available on your current plan'}), 403
    limit    = min(int(request.args.get('limit', 20)), 100)
    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            '''SELECT platform, event_type, status, payload_hash, error_msg, created_at
               FROM webhook_log WHERE client_id = %s
               ORDER BY created_at DESC LIMIT %s''',
            (client_id, limit)
        )
        rows = cursor.fetchall()
        log  = [{
            'platform':   r['platform'],
            'event_type': r['event_type'],
            'status':     r['status'],
            'ref':        (r.get('payload_hash') or '')[:8] or '—',
            'error':      r.get('error_msg'),
            'time':       str(r.get('created_at', '')),
        } for r in rows]
        return jsonify({'success': True, 'log': log}), 200
    except Exception as e:
        app.logger.error(f'[IntegrationLog] error: {e}')
        return jsonify({'success': False, 'error': 'Failed to load log'}), 500
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


# ── Agent Actions (agency-configured external system integrations) ────────────
# SECURITY: every route below takes client_id (checked against the caller via
# verify_client_ownership) AND a separate integration_id/action_id from the
# URL or JSON body. Those two are independent — verify_client_ownership alone
# does NOT prove the integration/action actually belongs to that client_id.
# Without the checks below, an authenticated agency user could keep their own
# client_id (passes ownership) while substituting another agency's
# integration_id/action_id and read, modify, delete, or fire a real request
# against a completely different agency's connected system.

def _integration_belongs_to_client(integration_id: str, client_id: str) -> bool:
    return any(i['integration_id'] == integration_id for i in models.get_integrations(client_id))


def _action_belongs_to_client(action_id, client_id: str) -> bool:
    action = models.get_action_by_id(action_id)
    if not action:
        return False
    return _integration_belongs_to_client(action['integration_id'], client_id)


# Distinct from the platform integrations above: those receive INBOUND
# webhooks from Shopify/Acuity. This lets the chatbot make OUTBOUND calls
# to a client's own external system (their booking API, CRM, etc.) —
# see models/integrations.py, pipeline/integration_adapter.py,
# pipeline/stages/agent_actions.py.

@app.route('/agent-actions')
@login_required
def agent_actions_page():
    fresh_user  = models.get_user_by_id(current_user.id)
    plan_type   = (fresh_user or {}).get('plan_type', current_user.plan_type)
    plan_limits = PLAN_LIMITS.get(plan_type, PLAN_LIMITS['free'])
    if not plan_limits.get('agentic_actions'):
        return redirect(url_for('auth.dashboard') + '?upgrade=agentic_actions')
    clients = models.get_user_clients(current_user.id)
    return render_template(
        'agent_actions.html',
        user=current_user, plan_type=plan_type,
        plan_limits=plan_limits, clients=clients,
    )


@app.route('/api/agent-actions/<client_id>/integrations', methods=['GET'])
@login_required
def list_agent_integrations(client_id):
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    if not PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free']).get('agentic_actions'):
        return jsonify({'success': False, 'error': 'Agent actions require the Growth or Scale plan'}), 403
    integrations = models.get_integrations(client_id)
    all_actions  = models.get_actions_for_client(client_id)
    for i in integrations:
        i['actions'] = [a for a in all_actions if a['integration_id'] == i['integration_id']]
    return jsonify({'success': True, 'integrations': integrations}), 200


@app.route('/api/agent-actions/<client_id>/integrations', methods=['POST'])
@login_required
def create_agent_integration(client_id):
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    if not PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free']).get('agentic_actions'):
        return jsonify({'success': False, 'error': 'Agent actions require the Growth or Scale plan'}), 403
    data = request.get_json(force=True) or {}
    result = models.create_integration(
        client_id=client_id,
        name=data.get('name', ''),
        base_url=data.get('base_url', ''),
        auth_type=data.get('auth_type', ''),
        credentials=data.get('credentials') or {},
        created_by_agency_user_id=current_user.id,
    )
    if not result.get('success'):
        return jsonify(result), 400
    app.logger.info(
        f'[AgentActions] user={current_user.id} created integration={result["integration_id"]} client={client_id}'
    )
    return jsonify(result), 200


@app.route('/api/agent-actions/<client_id>/integrations/<integration_id>', methods=['PATCH'])
@login_required
def update_agent_integration(client_id, integration_id):
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    if not _integration_belongs_to_client(integration_id, client_id):
        return jsonify({'success': False, 'error': 'Integration not found'}), 404
    if not PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free']).get('agentic_actions'):
        return jsonify({'success': False, 'error': 'Agent actions require the Growth or Scale plan'}), 403
    data = request.get_json(force=True) or {}
    if 'credentials' in data:
        ok = models.update_integration_credentials(integration_id, data['credentials'] or {})
        if not ok:
            return jsonify({'success': False, 'error': 'Failed to update credentials'}), 500
    if 'active' in data:
        ok = models.set_integration_active(integration_id, bool(data['active']))
        if not ok:
            return jsonify({'success': False, 'error': 'Failed to update status'}), 500
    return jsonify({'success': True}), 200


@app.route('/api/agent-actions/<client_id>/integrations/<integration_id>', methods=['DELETE'])
@login_required
def delete_agent_integration(client_id, integration_id):
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    if not _integration_belongs_to_client(integration_id, client_id):
        return jsonify({'success': False, 'error': 'Integration not found'}), 404
    if not PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free']).get('agentic_actions'):
        return jsonify({'success': False, 'error': 'Agent actions require the Growth or Scale plan'}), 403
    ok = models.delete_integration(integration_id)
    if not ok:
        return jsonify({'success': False, 'error': 'Failed to delete integration'}), 500
    app.logger.info(f'[AgentActions] user={current_user.id} deleted integration={integration_id}')
    return jsonify({'success': True}), 200


@app.route('/api/agent-actions/<client_id>/integrations/<integration_id>/actions', methods=['POST'])
@login_required
def create_agent_action(client_id, integration_id):
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    if not _integration_belongs_to_client(integration_id, client_id):
        return jsonify({'success': False, 'error': 'Integration not found'}), 404
    if not PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free']).get('agentic_actions'):
        return jsonify({'success': False, 'error': 'Agent actions require the Growth or Scale plan'}), 403
    data = request.get_json(force=True) or {}

    # CORRECTION to the fix I made here last time: I had this backwards.
    # Having now seen pipeline/stages/agent_actions.py's _check_spend_cap(),
    # max_auto_amount is NOT an auto-approval bypass — it's the opposite: a
    # ceiling that forces escalation to a human when an amount exceeds it
    # ("a confirmation prompt is not real oversight for an above-cap
    # amount; only a human is" — that's the actual code comment). Nulling
    # it out, as I did before, doesn't reduce risk — it just removes that
    # ceiling, which only matters at all once requires_confirmation=False.
    #
    # The real lever for "nothing executes without a human/agent process
    # ready to back it up" is requires_confirmation — that's what gates
    # whether execute_client_action() ever fires with zero interaction.
    # It already defaults to True, but nothing stopped an agency (or a
    # future dashboard change) from setting it False. Forcing it here
    # closes that regardless of amount or cap — every external action now
    # unconditionally asks the live customer before doing anything.
    #
    # max_auto_amount/amount_param stay disabled too, but for a different,
    # more specific reason now: even the escalate-to-human path this
    # enables (_escalation_result in agent_actions.py) promises "someone
    # from the team will take care of this" — which isn't true yet if
    # there's no one actually watching the agent-actions log for these.
    # Re-enable both once that process exists; each is a one-line change.
    result = models.add_action(
        integration_id=integration_id,
        action_name=data.get('action_name', ''),
        http_method=data.get('http_method', ''),
        endpoint_path=data.get('endpoint_path', ''),
        param_mapping=data.get('param_mapping') or {},
        response_mapping=data.get('response_mapping') or {},
        requires_confirmation=True,
        description=data.get('description', ''),
        amount_param=None,
        max_auto_amount=None,
    )
    if not result.get('success'):
        return jsonify(result), 400
    app.logger.info(
        f'[AgentActions] user={current_user.id} added action={data.get("action_name")} '
        f'integration={integration_id} client={client_id}'
    )
    return jsonify(result), 200


@app.route('/api/agent-actions/<client_id>/actions/<int:action_id>', methods=['DELETE'])
@login_required
def delete_agent_action(client_id, action_id):
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    if not _action_belongs_to_client(action_id, client_id):
        return jsonify({'success': False, 'error': 'Action not found'}), 404
    if not PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free']).get('agentic_actions'):
        return jsonify({'success': False, 'error': 'Agent actions require the Growth or Scale plan'}), 403
    ok = models.delete_action(action_id)
    if not ok:
        return jsonify({'success': False, 'error': 'Failed to delete action'}), 500
    return jsonify({'success': True}), 200


@app.route('/api/agent-actions/<client_id>/test', methods=['POST'])
@login_required
def test_agent_action(client_id):
    """Fire a single action with sample params so the agency can verify the
    mapping before going live — mirrors the existing /api/test-webhook pattern."""
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    if not PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free']).get('agentic_actions'):
        return jsonify({'success': False, 'error': 'Agent actions require the Growth or Scale plan'}), 403
    data      = request.get_json(force=True) or {}
    action_id = data.get('action_id')
    params    = data.get('params') or {}
    if not action_id:
        return jsonify({'success': False, 'error': 'action_id is required'}), 400
    if not _action_belongs_to_client(action_id, client_id):
        return jsonify({'success': False, 'error': 'Action not found'}), 404
    from pipeline.integration_adapter import execute_client_action
    result = execute_client_action(
        action_id=action_id, params=params,
        client_id=client_id, session_id=f'test:{current_user.id}',
    )
    return jsonify(result), 200


@app.route('/api/agent-actions/<client_id>/log', methods=['GET'])
@login_required
def get_agent_action_log(client_id):
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    if not PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free']).get('agentic_actions'):
        return jsonify({'success': False, 'error': 'Agent actions require the Growth or Scale plan'}), 403
    limit = min(int(request.args.get('limit', 50)), 100)
    log   = models.get_action_log(client_id, limit=limit)
    return jsonify({'success': True, 'log': log}), 200


@app.route('/api/agent-actions/overview', methods=['GET'])
@login_required
def get_agent_actions_overview():
    """Cross-client rollup — every client this agency owns, one call."""
    if not PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free']).get('agentic_actions'):
        return jsonify({'success': False, 'error': 'Agent actions require the Growth or Scale plan'}), 403
    clients = models.get_user_clients(current_user.id)
    client_ids = [c['client_id'] for c in clients]
    stats = {s['client_id']: s for s in models.get_agency_integration_overview(client_ids)}
    overview = [{
        'client_id': c['client_id'],
        'company_name': c.get('company_name') or c['client_id'],
        **stats.get(c['client_id'], {
            'integration_count': 0, 'active_integration_count': 0,
            'action_count': 0, 'last_action_at': None, 'failures_last_7_days': 0,
        }),
    } for c in clients]
    return jsonify({'success': True, 'overview': overview}), 200



# FIX: this route had no @app.route(...) decorator at all — Flask never
# registered it, so any request to it (whatever URL the frontend's
# "choose a template" UI calls) returned a 404. Every sibling route in
# this section follows /api/agent-actions/... ; this one's docstring says
# "No client_id needed", matching the pattern already used by
# get_agent_actions_overview() just above it (/api/agent-actions/overview
# — also no client_id). Named consistently with that.
@app.route('/api/agent-actions/templates', methods=['GET'])
@login_required
def list_agent_action_templates():
    """No client_id needed — templates are generic, same list for every agency."""
    from integration_templates import list_templates
    return jsonify({'success': True, 'templates': list_templates()}), 200


@app.route('/api/agent-actions/<client_id>/apply-template', methods=['POST'])
@login_required
def apply_agent_action_template(client_id):
    """
    Creates one integration plus its full starter action set from a
    template in a single call, instead of an agency hand-configuring
    param_mapping/response_mapping per action from scratch.
    """
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    if not PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free']).get('agentic_actions'):
        return jsonify({'success': False, 'error': 'Agent actions require the Growth or Scale plan'}), 403

    from integration_templates import get_template
    data = request.get_json(force=True) or {}
    platform = data.get('platform', '')
    template = get_template(platform)
    if not template:
        return jsonify({'success': False, 'error': 'Unknown template.'}), 400

    base_url = (data.get('base_url') or template['base_url']).strip()
    if '{' in base_url:
        return jsonify({'success': False, 'error': 'Base URL still has a placeholder in it — fill in the real address.'}), 400

    integration_result = models.create_integration(
        client_id=client_id,
        name=data.get('name') or template['name'],
        base_url=base_url,
        auth_type=template['auth_type'],
        credentials=data.get('credentials') or {},
        created_by_agency_user_id=current_user.id,
    )
    if not integration_result.get('success'):
        return jsonify(integration_result), 400

    integration_id = integration_result['integration_id']
    created, failed = [], []
    for action in template['actions']:
        result = models.add_action(
            integration_id=integration_id,
            action_name=action['action_name'],
            http_method=action['http_method'],
            endpoint_path=action['endpoint_path'],
            param_mapping=action['param_mapping'],
            response_mapping=action.get('response_mapping') or {},
            # Same correction as create_agent_action() above — forced True
            # regardless of what the template suggests, until there's a
            # real process behind either confirmation path.
            requires_confirmation=True,
            description=action.get('description', ''),
            amount_param=None,
            max_auto_amount=None,  # see create_agent_action() for why both stay disabled
        )
        if result.get('success'):
            created.append(action['action_name'])
        else:
            failed.append({'action_name': action['action_name'], 'error': result.get('error')})

    app.logger.info(
        f'[AgentActions] user={current_user.id} applied template={platform} '
        f'client={client_id} integration={integration_id} created={len(created)} failed={len(failed)}'
    )
    return jsonify({
        'success': True, 'integration_id': integration_id,
        'actions_created': created, 'actions_failed': failed,
    }), 200




@app.route('/analytics')
@login_required
def analytics_page():
    fresh_user  = models.get_user_by_id(current_user.id)
    plan_type   = (fresh_user or {}).get('plan_type', current_user.plan_type)
    plan_limits = PLAN_LIMITS.get(plan_type, PLAN_LIMITS['free'])
    is_admin    = bool((fresh_user or {}).get('is_admin', False))
    if not plan_limits['analytics'] and not is_admin:
        return render_template('analytics_upgrade.html',
                               user=current_user, plan_type=plan_type), 403
    clients   = models.get_user_clients(current_user.id)
    client_id = request.args.get('client_id')
    if not client_id and clients:
        client_id = clients[0]['client_id']
    for c in clients:
        if c.get('branding_settings'):
            try:
                c['branding_settings'] = json.loads(c['branding_settings'])
            except Exception:
                c['branding_settings'] = {}
    # No plan supports more than one connected store anymore (see 'clients'
    # in PLAN_LIMITS above — free/ai_starter/ai_growth/ai_scale are all
    # capped at 1). is_agency below is now vestigial for every real account
    # — plan_type == 'ai_scale' can never actually have >1 client to switch
    # between — kept only for is_admin's sake and because templates/
    # analytics.html still reads the `is_agency` kwarg name. Candidate to
    # simplify to just `is_agency = is_admin` once that template is updated.
    is_agency = plan_type == 'ai_scale' or is_admin
    return render_template(
        'analytics.html',
        clients     = clients,
        client_id   = client_id,
        plan_type   = plan_type,
        plan_limits = plan_limits,
        is_agency   = is_agency,
        user        = current_user,
    )


# ── Simple page routes ────────────────────────────────────────────────────────

@app.route('/sales')
def sales_page():
    return render_template('sales-page.html')


@app.route('/help-center')
@login_required
def help_center_page():
    client_id = request.args.get('client_id', '')
    return redirect(url_for('faqs.article_manager_page', client_id=client_id))


@app.route('/thank-you')
def thank_you_page():
    return render_template('thank-you.html')


# ── Inbound webhook — lead retrieval ──────────────────────────────────────────

@app.route('/api/webhook/lead', methods=['POST'])
def webhook_new_lead():
    try:
        import hmac as _hmac
        _wh_secret = os.environ.get('WEBHOOK_SECRET', '').strip()
        if not _wh_secret:
            return jsonify({'error': 'Webhook not configured'}), 503
        _provided = request.headers.get('X-Webhook-Secret', '')
        if not _hmac.compare_digest(_provided, _wh_secret):
            return jsonify({'error': 'Unauthorized'}), 401
        data      = request.json or {}
        client_id = data.get('client_id')
        leads     = (models.get_leads(client_id) or [])[:10]
        return jsonify({'success': True, 'leads': leads})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
