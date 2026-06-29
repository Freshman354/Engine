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
                     session_id=None, daily_limit=None,
                     page_url=None, referrer=None,
                     utm_source=None, utm_medium=None,
                     utm_campaign=None) -> bool:
    """
    Insert a conversation row.
    When daily_limit is supplied, the INSERT is wrapped in a CTE that
    re-counts today's rows atomically — prevents races at the cap boundary.
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
    agency_included_clients=AGENCY_INCLUDED_CLIENTS,
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

# ── Tier 1 feature routes (CSAT, status, typing, tags, triggers) ──────────────
from blueprints.inbox_additions import inbox_additions_bp
app.register_blueprint(inbox_additions_bp)

from blueprints.email_domains   import email_domains_bp
app.register_blueprint(email_domains_bp)

from blueprints.client_settings import client_settings_bp
app.register_blueprint(client_settings_bp)

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
        client['tagline']           = branding.get('tagline')               or 'Typically replies instantly'
        client['welcome_message']   = bot_settings.get('welcome_message')   or client.get('welcome_message') or 'Hi! How can I help you today?'
        client['fallback_message']  = bot_settings.get('fallback_message')  or ''
        client['quick_replies']     = [r for r in (bot_settings.get('quick_replies') or []) if r and str(r).strip()]
        client['lead_q3']           = bot_settings.get('lead_q3', '').strip()
        client['lead_q4']           = bot_settings.get('lead_q4', '').strip()
        client['widget_color']      = branding.get('primary_color')         or client.get('widget_color') or '#B8924A'
        client['remove_branding']   = branding.get('remove_branding',       client.get('remove_branding', 0))
        client['logo_url']          = branding.get('logo')                  or branding.get('logo_url') or ''
        client['custom_css']        = client.get('custom_css')              or ''
        client['contact']           = contact
        client['branding_settings'] = branding_settings
        client['widget_theme']      = branding.get('widget_theme',  'lumvi')
        client['widget_font']       = branding.get('widget_font',   'dm_sans')
        client['bubble_style']      = branding.get('bubble_style',  'rounded')
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
        valid_plans = ['free', 'solo', 'starter', 'pro', 'growth', 'agency', 'enterprise']
        if secret != ADMIN_SECRET:
            error = 'Invalid admin secret.'
        elif not email:
            error = 'Email is required.'
        elif plan not in valid_plans:
            error = f'Invalid plan. Must be one of: {", ".join(valid_plans)}'
        else:
            user = models.get_user_by_email(email)
            if not user:
                error = f'No user found with email: {email}'
            else:
                conn, cursor = models.get_db()
                cursor.execute(
                    'UPDATE users SET plan_type = %s WHERE email = %s', (plan, email)
                )
                conn.commit(); cursor.close(); conn.close()
                success = f'✅ {email} updated to {plan.capitalize()} plan.'

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
      <option value="solo">Solo ($19/mo)</option>
      <option value="starter">Starter ($49/mo)</option>
      <option value="pro">Pro ($99/mo)</option>
      <option value="agency">Agency ($299/mo)</option>
      <option value="enterprise">Enterprise</option>
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
        has_white_label = plan_limits.get('white_label', False),
        has_analytics   = plan_limits.get('analytics', False),
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
        fresh_plan  = (fresh_user or {}).get('plan_type', 'free')
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

        remove_branding = False
        if fresh_plan in ('agency', 'enterprise'):
            remove_branding = bool(data.get('remove_branding'))
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


# ── Webhooks (outbound config) ────────────────────────────────────────────────

@app.route('/api/admin/webhooks', methods=['GET'])
@login_required
def get_webhooks_route():
    client_id = request.args.get('client_id', '')
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    if not PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free']).get('webhooks'):
        return jsonify({'success': False, 'error': 'Webhooks require Pro or Agency plan'}), 403
    return jsonify({'success': True, 'webhooks': models.get_webhooks(client_id)})


@app.route('/api/admin/webhooks', methods=['POST'])
@login_required
def save_webhooks_route():
    data      = request.json or {}
    client_id = data.get('client_id', '')
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    if not PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free']).get('webhooks'):
        return jsonify({'success': False, 'error': 'Webhooks require Pro or Agency plan'}), 403
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
        return jsonify({'success': False, 'error': 'Webhooks require Pro or Agency plan'}), 403
    new_secret = models.regenerate_signing_secret(client_id, webhook_id)
    return jsonify({'success': True, 'signing_secret': new_secret})


@app.route('/api/admin/webhooks/logs', methods=['GET'])
@login_required
def get_webhook_logs_route():
    client_id = request.args.get('client_id', '')
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    if not PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free']).get('webhooks'):
        return jsonify({'success': False, 'error': 'Webhooks require Pro or Agency plan'}), 403
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
        return jsonify({'success': False, 'error': 'Webhooks require Pro or Agency plan'}), 403
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
    if not PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free']).get('webhooks'):
        return jsonify({'success': False, 'error': 'Webhooks require Pro or Agency plan'}), 403
    data     = request.get_json(force=True) or {}
    platform = (data.get('platform') or '').lower().strip()
    secret   = (data.get('webhook_secret') or '').strip()
    config   = data.get('platform_config') or {}
    if platform not in ('shopify', 'acuity'):
        return jsonify({'success': False, 'error': 'platform must be shopify or acuity'}), 400
    if not secret:
        return jsonify({'success': False, 'error': 'webhook_secret is required'}), 400
    ok = _webhooks.upsert_integration(client_id, platform, secret, config)
    if not ok:
        return jsonify({'success': False, 'error': 'Failed to save integration'}), 500
    base_url    = os.environ.get('APP_BASE_URL', 'https://app.lumvi.ai')
    webhook_url = f'{base_url}/webhooks/{platform}/{client_id}'
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
        return jsonify({'success': False, 'error': 'Webhooks require Pro or Agency plan'}), 403
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
        return jsonify({'success': False, 'error': 'Webhooks require Pro or Agency plan'}), 403
    if platform not in ('shopify', 'acuity'):
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
        return jsonify({'success': False, 'error': 'Webhooks require Pro or Agency plan'}), 403
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


# ── Analytics ─────────────────────────────────────────────────────────────────

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
    is_agency = plan_type in ('pro', 'agency', 'enterprise') or is_admin
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
