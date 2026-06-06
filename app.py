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
Target: ~500 lines. Route count in this file: 7.
"""

# ── Standard library ─────────────────────────────────────────────────────────
import json
import logging
import os
import re
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
import models
import webhooks as _webhooks
from ai_helper import get_ai_helper
from app_utils import sanitize_input
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
app.config['MAIL_PASSWORD']          = os.environ.get('MAIL_PASSWORD', '')
app.config['MAIL_DEFAULT_SENDER']    = 'Lumvi <support@lumvi.net>'
app.config['MAIL_MAX_EMAILS']        = None
app.config['MAIL_ASCII_ATTACHMENTS'] = False
mail = Mail(app)

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
#
# Two distinct policies:
#   1. Authenticated dashboard routes (/api/user/*, /api/config, etc.)
#      — specific origins only, credentials allowed.
#      Add your domain(s) to ALLOWED_ORIGINS in Railway env vars.
#   2. Widget chat routes (/api/chat, /widget)
#      — any origin (embedded on customer websites), NO credentials.
#      Browsers reject Allow-Credentials: true + wildcard origin (CORS spec).

_ALLOWED_ORIGINS = [
    o.strip()
    for o in os.environ.get(
        'ALLOWED_ORIGINS',
        'https://lumvi.net,https://app.lumvi.net'
    ).split(',')
    if o.strip()
]

CORS(app, resources={
    # Authenticated dashboard API — known origins, credentials OK
    r'/api/user/*': {
        'origins':              _ALLOWED_ORIGINS,
        'methods':              ['GET', 'POST', 'OPTIONS'],
        'allow_headers':        ['Content-Type', 'Authorization'],
        'supports_credentials': True,
        'max_age':              3600,
    },
    # Public widget/chat API — any origin, no credentials
    r'/api/*': {
        'origins':       '*',
        'methods':       ['GET', 'POST', 'OPTIONS'],
        'allow_headers': ['Content-Type'],
        'max_age':       3600,
    },
    r'/widget': {
        'origins': '*',
        'methods': ['GET'],
        'max_age': 3600,
    },
})

# ── Flask-Limiter ─────────────────────────────────────────────────────────────

_limiter_storage = os.environ.get('REDIS_URL', 'memory://')
if _limiter_storage == 'memory://':
    _warnings.warn(
        'REDIS_URL not set — rate limiter is in-memory. '
        'Limits reset on every deploy/restart. '
        'Set REDIS_URL in Railway environment variables for persistent limiting.',
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
    'free': {
        'clients': 1, 'faqs_per_client': 5, 'messages_per_day': 50,
        'analytics': False, 'analytics_level': 'none',
        'customization': False, 'white_label': False,
        'webhooks': False, 'priority_support': False,
    },
    'solo': {
        'clients': 1, 'faqs_per_client': 999, 'messages_per_day': 999999,
        'analytics': True, 'analytics_level': 'basic',
        'customization': True, 'white_label': False,
        'webhooks': False, 'priority_support': False,
    },
    'starter': {
        'clients': 3, 'faqs_per_client': 999, 'messages_per_day': 2000,
        'analytics': True, 'analytics_level': 'basic',
        'customization': True, 'white_label': False,
        'webhooks': False, 'priority_support': False,
    },
    'pro': {
        'clients': 10, 'faqs_per_client': 999, 'messages_per_day': 999999,
        'analytics': True, 'analytics_level': 'full',
        'customization': True, 'white_label': False,
        'webhooks': True, 'priority_support': True,
    },
    'agency': {
        'clients': 999999, 'faqs_per_client': 999, 'messages_per_day': 999999,
        'analytics': True, 'analytics_level': 'full',
        'customization': True, 'white_label': True,
        'webhooks': True, 'priority_support': True,
    },
    'enterprise': {
        'clients': 999999, 'faqs_per_client': 999, 'messages_per_day': 999999,
        'analytics': True, 'analytics_level': 'full',
        'customization': True, 'white_label': True,
        'webhooks': True, 'priority_support': True,
    },
}

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
    """Send a branded welcome email to a new Lumvi user."""
    try:
        msg = Message(
            subject    = "Welcome to Lumvi — your AI chatbot is ready 🚀",
            sender     = "Lumvi <support@lumvi.net>",
            recipients = [email],
            html       = """
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#0f172a;font-family:'Inter',Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0f172a;padding:40px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" style="max-width:560px;width:100%;">
        <tr><td style="background:linear-gradient(135deg,#6366f1 0%,#7c3aed 50%,#a78bfa 100%);border-radius:16px 16px 0 0;padding:36px 40px;text-align:center;">
          <div style="display:inline-block;background:rgba(255,255,255,0.15);border-radius:12px;padding:10px 20px;margin-bottom:16px;">
            <span style="font-size:26px;font-weight:900;color:#ffffff;letter-spacing:-0.5px;">&#9889; Lumvi</span>
          </div>
          <h1 style="margin:0;font-size:24px;font-weight:800;color:#ffffff;line-height:1.3;">
            You're all set &mdash; let's build your first chatbot!
          </h1>
        </td></tr>
        <tr><td style="background:#1e293b;padding:36px 40px;">
          <p style="margin:0 0 20px;color:#94a3b8;font-size:15px;line-height:1.7;">
            Hey there &#128075; &mdash; welcome to Lumvi! You're now part of a growing group of agencies and businesses using AI chatbots to capture leads and answer questions automatically.
          </p>
          <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:28px;">
            <tr><td style="padding:14px 16px;background:rgba(99,102,241,0.08);border:1px solid rgba(99,102,241,0.2);border-radius:12px;">
              <strong style="font-size:14px;font-weight:700;color:#c7d2fe;">1. Create your first chatbot</strong>
              <p style="margin:6px 0 0;font-size:13px;color:#64748b;line-height:1.6;">Go to your dashboard and click "Create New Chatbot".</p>
            </td></tr>
            <tr><td style="height:10px;"></td></tr>
            <tr><td style="padding:14px 16px;background:rgba(99,102,241,0.08);border:1px solid rgba(99,102,241,0.2);border-radius:12px;">
              <strong style="font-size:14px;font-weight:700;color:#c7d2fe;">2. Add your FAQs</strong>
              <p style="margin:6px 0 0;font-size:13px;color:#64748b;line-height:1.6;">Upload a CSV or PDF, or add them manually in the FAQ Manager.</p>
            </td></tr>
            <tr><td style="height:10px;"></td></tr>
            <tr><td style="padding:14px 16px;background:rgba(99,102,241,0.08);border:1px solid rgba(99,102,241,0.2);border-radius:12px;">
              <strong style="font-size:14px;font-weight:700;color:#c7d2fe;">3. Embed on your website</strong>
              <p style="margin:6px 0 0;font-size:13px;color:#64748b;line-height:1.6;">Copy the one-line embed code from your dashboard and paste it into any website.</p>
            </td></tr>
          </table>
          <table width="100%" cellpadding="0" cellspacing="0">
            <tr><td align="center" style="padding:8px 0 28px;">
              <a href="https://lumvi.net/dashboard" style="display:inline-block;background:linear-gradient(135deg,#6366f1,#7c3aed);color:#ffffff;text-decoration:none;padding:15px 36px;border-radius:10px;font-weight:800;font-size:15px;">
                Go to My Dashboard &rarr;
              </a>
            </td></tr>
          </table>
          <p style="margin:0;color:#475569;font-size:13px;line-height:1.7;border-top:1px solid rgba(255,255,255,0.06);padding-top:20px;">
            Questions? Reply to this email or reach us at <a href="mailto:support@lumvi.net" style="color:#818cf8;text-decoration:none;">support@lumvi.net</a>.
          </p>
        </td></tr>
        <tr><td style="background:#0f172a;border-radius:0 0 16px 16px;padding:20px 40px;text-align:center;">
          <p style="margin:0;color:#334155;font-size:12px;">
            &copy; 2025 Lumvi &middot; <a href="https://lumvi.net" style="color:#475569;text-decoration:none;">lumvi.net</a>
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


def get_subscription_status(user: dict) -> dict:
    """
    Returns subscription info for a user.
    status: 'active' | 'cancelling' | 'grace' | 'expired' | 'free'
    Admins are always treated as active.
    """
    if user.get('is_admin'):
        return {'status': 'active', 'expires_at': None, 'grace_ends_at': None}

    plan = user.get('plan_type', 'free')
    if plan in ('free', 'enterprise'):
        return {'status': 'free', 'expires_at': None, 'grace_ends_at': None}

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
                     session_id=None, daily_limit=None) -> bool:
    """
    Insert a conversation row.
    When daily_limit is supplied, the INSERT is wrapped in a CTE that
    re-counts today's rows atomically — prevents races at the cap boundary.
    Returns True if the row was inserted, False if the cap was already reached.
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
                    (client_id, user_message, bot_response, matched, method, session_id)
                SELECT %s, %s, %s, %s, %s, %s
                FROM   today_count
                WHERE  cnt < %s
                ''',
                (client_id,
                 client_id, user_message, bot_response,
                 matched, method, session_id or None,
                 daily_limit)
            )
            inserted = cursor.rowcount > 0
        else:
            cursor.execute(
                '''
                INSERT INTO conversations
                    (client_id, user_message, bot_response, matched, method, session_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                ''',
                (client_id, user_message, bot_response,
                 matched, method, session_id or None)
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
                f'(daily_limit={daily_limit})'
            )
        return inserted
    except Exception as e:
        app.logger.error(f'❌ Error logging conversation: {e}')
        return True   # fail-open: never block the chat response on a log error


# ── notify_handoff — kept here so chat.py can receive it via injection ────────
# The actual implementation lives in blueprints/inbox.py (notify_handoff).
# We import it here so it can be passed into init_chat() by name.
from blueprints.inbox import notify_handoff   # noqa: E402

# ═══════════════════════════════════════════════════════════════════════════════
# DATABASE MIGRATIONS (startup, idempotent)
# ═══════════════════════════════════════════════════════════════════════════════

try:
    models.init_db()

    _optional_migrations = [
        'migrate_clients_table', 'migrate_faqs_table',
        'migrate_faq_to_knowledge_base', 'migrate_subscription_expiry',
        'migrate_to_recurring_subscriptions', 'migrate_conversation_features',
        'migrate_knowledge_base', 'migrate_webhooks', 'migrate_white_label',
        'migrate_client_status', 'migrate_onboarding', 'migrate_cron_tables',
        'migrate_api_usage_log', 'migrate_kb_gaps', 'migrate_lead_pipeline',
        'migrate_agency_seat_billing', 'migrate_payments_unique_reference',
        'migrate_google_oauth',
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

    # ── Startup enforcement — single worker, no advisory lock needed ──────────
    def _startup_enforce():
        try:
            with app.app_context():
                enforce_subscriptions()
                app.logger.info('[Startup] subscription enforcement complete')
        except Exception as _e:
            app.logger.error(f'[Startup] enforcement error: {_e}')

    threading.Timer(10.0, _startup_enforce).start()
    print('✅ Startup enforcement scheduled (T+10s)')

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

# Billing
from blueprints.billing import billing_bp, init_billing, PLAN_PRICES_FLW
init_billing(mail=mail, get_subscription_status=get_subscription_status)
app.register_blueprint(billing_bp)

# Agency
from blueprints.agency import agency_bp, init_agency
init_agency(
    mail=mail,
    plan_limits=PLAN_LIMITS,
    dns_executor=_dns_executor,
    futures_timeout=_FuturesTimeout,
)
app.register_blueprint(agency_bp)

# Auth
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
)
app.register_blueprint(chat_bp)
limiter.limit('30 per minute')(_chat_view)
limiter.limit('20 per minute')(_chat_rate_view)

# Platform webhook routes (Shopify, Acuity)
_webhooks.register_webhook_routes(app)

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
            response.headers['Access-Control-Allow-Origin']  = origin
            response.headers['Access-Control-Allow-Methods'] = (
                'GET, POST, PUT, DELETE, OPTIONS'
            )
            response.headers['Access-Control-Allow-Headers'] = (
                'Content-Type, Authorization, X-Requested-With'
            )
            # Allow credentials only for known origins on authenticated routes.
            # Browsers block Allow-Credentials: true + wildcard origin (CORS spec).
            if origin in _ALLOWED_ORIGINS and path.startswith('/api/user/'):
                response.headers['Access-Control-Allow-Credentials'] = 'true'
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
        return jsonify({'success': True, 'config': {
            'client_id':   client_id,
            'branding':    branding.get('branding', {}),
            'contact':     branding.get('contact', {}),
            'bot_settings': branding.get('bot_settings', {}),
        }})
    except Exception as e:
        app.logger.error(f'Error getting config: {e}')
        return jsonify({'success': False, 'error': 'Failed to load configuration'}), 500


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


@app.route('/')
def index():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    return redirect(url_for('landing_page'))


@app.route('/landing')
def landing_page():
    return render_template('landing-professional.html')


@app.route('/static/widget.js')
def serve_widget_js_custom_domain():
    """
    Serve widget.js when requested via a white-label custom domain.

    When an agency sets chat.theiragency.com as their custom domain, the embed
    code points widget.js at https://chat.theiragency.com/static/widget.js.
    Flask's normal static file handler doesn't check the Host header, so this
    route explicitly serves the file regardless of which domain the request
    arrived on — as long as that domain is registered as a custom_widget_domain.
    """
    import os
    from flask import send_from_directory

    host = request.host.split(':')[0].lower()

    # Allow the real lumvi.net origin through without extra checks
    if host in ('lumvi.net', 'www.lumvi.net', 'localhost', '127.0.0.1'):
        return send_from_directory(app.static_folder, 'widget.js',
                                   mimetype='application/javascript')

    # Verify the requesting host is a registered custom domain
    client = models.get_client_by_custom_domain(host)
    if not client:
        app.logger.warning(f"[WidgetJS] Unregistered custom domain blocked: {host}")
        return jsonify({'error': 'Unknown domain'}), 403

    app.logger.info(f"[WidgetJS] Serving widget.js for custom domain: {host} client={client['client_id']}")
    response = send_from_directory(app.static_folder, 'widget.js',
                                   mimetype='application/javascript')
    # Allow cross-origin from any domain (widget embeds always cross-origin)
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Cache-Control'] = 'public, max-age=300'  # 5 min cache
    return response


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
    # Development only. Production Procfile:
    # web: gunicorn app:app --workers 1 --worker-class gevent --worker-connections 100 --timeout 120 --bind 0.0.0.0:$PORT
    # On Railway Starter plan (512 MB), also add: --max-requests 500 --max-requests-jitter 50
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)
