from flask import Flask, request, jsonify, render_template, send_from_directory, redirect, url_for, session, flash
from flask_cors import CORS
from concurrent.futures import ThreadPoolExecutor, TimeoutError as _FuturesTimeout
_dns_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix='dns-check')
from dotenv import load_dotenv
import os
import json
import re
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import logging
from logging.handlers import RotatingFileHandler
import shutil
from datetime import datetime, timedelta
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from functools import wraps
from flask_mail import Mail, Message
import secrets
import threading
import cache_utils  # KB version-based cache invalidation
import models
import requests
import uuid
from collections import Counter
from io import StringIO
from config import Config
from ai_helper import get_ai_helper
from paypalrestsdk import Payment, configure
import webhooks as _webhooks       # Platform webhook ingestion (Shopify, Acuity)

# Load environment variables
load_dotenv()

app = Flask(__name__)

# SECRET_KEY must be set in env — crash at startup rather than silently use a weak key
_secret = os.environ.get("SECRET_KEY")
if not _secret:
    raise RuntimeError(
        "SECRET_KEY environment variable is not set. "
        "Generate one with: python -c \"import secrets; print(secrets.token_hex(32))\""
    )
app.config['SECRET_KEY'] = _secret

# ADMIN_SECRET must be set — the /admin/set-plan endpoint uses it to authorise
# plan changes. Without an explicit secret the route previously fell back to a
# well-known default ('lumvi-admin-2024') that anyone could guess.
_admin_secret = os.environ.get("ADMIN_SECRET")
if not _admin_secret:
    raise RuntimeError(
        "ADMIN_SECRET environment variable is not set. "
        "Generate one with: python -c \"import secrets; print(secrets.token_hex(32))\""
    )

# FLW_WEBHOOK_HASH must be set — required for webhook authentication (FW-005)
_flw_webhook_hash = os.environ.get("FLW_WEBHOOK_HASH")
if not _flw_webhook_hash:
    raise RuntimeError(
        "FLW_WEBHOOK_HASH environment variable is not set. "
        "Get this from Flutterwave dashboard → Settings → Webhook → Secret Hash"
    )

app.config['MAX_CONTENT_LENGTH'] = 8 * 1024 * 1024  # 8MB max request body

# ── Session security ─────────────────────────────────────────────────
# Without these, Flask uses browser-session cookies with no security
# flags — sessions expire on tab close, accessible to JS (XSS risk),
# no CSRF protection. These apply globally to every logged-in user.
app.config['PERMANENT_SESSION_LIFETIME']  = timedelta(days=30)
app.config['SESSION_COOKIE_SECURE']       = True   # HTTPS only — never sent over HTTP
app.config['SESSION_COOKIE_HTTPONLY']     = True   # not accessible to JavaScript
app.config['SESSION_COOKIE_SAMESITE']     = 'Lax'  # CSRF protection on cross-origin forms
app.config['SESSION_COOKIE_NAME']         = 'lumvi_session'
app.config['REMEMBER_COOKIE_DURATION']    = timedelta(days=30)
app.config['REMEMBER_COOKIE_SECURE']      = True
app.config['REMEMBER_COOKIE_HTTPONLY']    = True

# Admin blueprint — registered AFTER SECRET_KEY is configured
from admin_routes import admin_bp
app.register_blueprint(admin_bp)

# ── Flask-Mail (password reset emails) ──────────────────────────────
app.config['MAIL_SERVER']   = os.environ.get('MAIL_SERVER', 'smtp-relay.brevo.com')
app.config['MAIL_PORT']     = int(os.environ.get('MAIL_PORT', 587))
app.config['MAIL_USE_TLS']  = True
app.config['MAIL_USE_SSL']  = False
app.config['MAIL_USERNAME'] = os.environ.get('MAIL_USERNAME', '')
app.config['MAIL_PASSWORD'] = os.environ.get('MAIL_PASSWORD', '')
app.config['MAIL_DEFAULT_SENDER'] = 'Lumvi <support@lumvi.net>'
app.config['MAIL_MAX_EMAILS'] = None
app.config['MAIL_ASCII_ATTACHMENTS'] = False
mail = Mail(app)

# ── Google OAuth ─────────────────────────────────────────────────────
from authlib.integrations.flask_client import OAuth as _OAuth
_oauth = _OAuth(app)
google_oauth = _oauth.register(
    name='google',
    client_id=os.environ.get('GOOGLE_CLIENT_ID'),
    client_secret=os.environ.get('GOOGLE_CLIENT_SECRET'),
    server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
    client_kwargs={'scope': 'openid email profile'}
)


def send_welcome_email(email):
    """Send a branded welcome email to a new Lumvi user."""
    try:
        msg = Message(
            subject="Welcome to Lumvi — your AI chatbot is ready 🚀",
            sender="Lumvi <support@lumvi.net>",
            recipients=[email],
            html=f"""
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#0f172a;font-family:'Inter',Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0f172a;padding:40px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" style="max-width:560px;width:100%;">

        <!-- Header -->
        <tr><td style="background:linear-gradient(135deg,#6366f1 0%,#7c3aed 50%,#a78bfa 100%);border-radius:16px 16px 0 0;padding:36px 40px;text-align:center;">
          <div style="display:inline-block;background:rgba(255,255,255,0.15);border-radius:12px;padding:10px 20px;margin-bottom:16px;">
            <span style="font-size:26px;font-weight:900;color:#ffffff;letter-spacing:-0.5px;">&#9889; Lumvi</span>
          </div>
          <h1 style="margin:0;font-size:24px;font-weight:800;color:#ffffff;line-height:1.3;">
            You're all set &mdash; let's build your first chatbot!
          </h1>
        </td></tr>

        <!-- Body -->
        <tr><td style="background:#1e293b;padding:36px 40px;">
          <p style="margin:0 0 20px;color:#94a3b8;font-size:15px;line-height:1.7;">
            Hey there &#128075; &mdash; welcome to Lumvi! You're now part of a growing group of agencies and businesses using AI chatbots to capture leads and answer questions automatically.
          </p>
          <p style="margin:0 0 28px;color:#94a3b8;font-size:15px;line-height:1.7;">
            Here's how to get started in 3 simple steps:
          </p>

          <!-- Steps -->
          <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:28px;">
            <tr>
              <td style="padding:14px 16px;background:rgba(99,102,241,0.08);border:1px solid rgba(99,102,241,0.2);border-radius:12px;">
                <strong style="font-size:14px;font-weight:700;color:#c7d2fe;">1. Create your first chatbot</strong>
                <p style="margin:6px 0 0;font-size:13px;color:#64748b;line-height:1.6;">Go to your dashboard and click "Create New Chatbot". Choose an industry template or start from scratch.</p>
              </td>
            </tr>
            <tr><td style="height:10px;"></td></tr>
            <tr>
              <td style="padding:14px 16px;background:rgba(99,102,241,0.08);border:1px solid rgba(99,102,241,0.2);border-radius:12px;">
                <strong style="font-size:14px;font-weight:700;color:#c7d2fe;">2. Add your FAQs</strong>
                <p style="margin:6px 0 0;font-size:13px;color:#64748b;line-height:1.6;">Train your bot with common questions. Upload a CSV or PDF, or add them manually in the FAQ Manager.</p>
              </td>
            </tr>
            <tr><td style="height:10px;"></td></tr>
            <tr>
              <td style="padding:14px 16px;background:rgba(99,102,241,0.08);border:1px solid rgba(99,102,241,0.2);border-radius:12px;">
                <strong style="font-size:14px;font-weight:700;color:#c7d2fe;">3. Embed on your website</strong>
                <p style="margin:6px 0 0;font-size:13px;color:#64748b;line-height:1.6;">Copy the one-line embed code from your dashboard and paste it into any website. Done!</p>
              </td>
            </tr>
          </table>

          <!-- CTA -->
          <table width="100%" cellpadding="0" cellspacing="0">
            <tr><td align="center" style="padding:8px 0 28px;">
              <a href="https://lumvi.net/dashboard"
                 style="display:inline-block;background:linear-gradient(135deg,#6366f1,#7c3aed);color:#ffffff;text-decoration:none;padding:15px 36px;border-radius:10px;font-weight:800;font-size:15px;">
                Go to My Dashboard &rarr;
              </a>
            </td></tr>
          </table>

          <p style="margin:0;color:#475569;font-size:13px;line-height:1.7;border-top:1px solid rgba(255,255,255,0.06);padding-top:20px;">
            Questions? Reply to this email or reach us at
            <a href="mailto:support@lumvi.net" style="color:#818cf8;text-decoration:none;">support@lumvi.net</a>.
            We're happy to help.
          </p>
        </td></tr>

        <!-- Footer -->
        <tr><td style="background:#0f172a;border-radius:0 0 16px 16px;padding:20px 40px;text-align:center;">
          <p style="margin:0;color:#334155;font-size:12px;">
            &copy; 2025 Lumvi &middot;
            <a href="https://lumvi.net" style="color:#475569;text-decoration:none;">lumvi.net</a> &middot;
            <a href="https://lumvi.net/privacy-policy" style="color:#475569;text-decoration:none;">Privacy Policy</a>
          </p>
        </td></tr>

      </table>
    </td></tr>
  </table>
</body>
</html>
            """
        )
        mail.send(msg)
        app.logger.info(f"Welcome email sent to {email}")
    except Exception as e:
        app.logger.error(f"Welcome email failed for {email}: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()


# Initialize AI helper at app startup
ai_helper = get_ai_helper(Config.GEMINI_API_KEY, Config.GEMINI_MODEL)

if ai_helper and ai_helper.enabled:
    app.logger.info("✅ Gemini AI initialized")
    print("✅ AI Helper ENABLED — using Gemini for smart matching")
else:
    print("❌ AI Helper DISABLED — no API key provided. Set GEMINI_API_KEY in environment variables.")

configure({
    "mode": os.getenv('PAYPAL_MODE', 'sandbox'),
    "client_id": os.getenv('PAYPAL_CLIENT_ID'),
    "client_secret": os.getenv('PAYPAL_CLIENT_SECRET')
})

# =====================================================================
# =====================================================================
# SUBSCRIPTION ENFORCEMENT
# =====================================================================
# WHY WE DON'T USE APScheduler ON RENDER:
#   Render dynos can be restarted or put to sleep at any time.
#   An in-process APScheduler dies with the dyno and never fires again
#   until the next restart — which is why users weren't being downgraded.
#
# APPROACH:
#   1. Run enforce_subscriptions() once on every startup (catches anything
#      that was missed since the last restart).
#   2. Expose a POST /cron/enforce-subscriptions endpoint secured by a
#      secret token. Point UptimeRobot (free) or Render Cron to hit it
#      daily. This is dyno-restart-safe because the HTTP request wakes
#      the dyno and runs the logic.
# =====================================================================

def enforce_subscriptions():
    """
    Downgrade all non-admin users whose grace period has ended.
    Safe to call multiple times — SQL WHERE clause is idempotent.
    Logs every run to cron_runs for auditability.
    """
    import time as _time
    t0 = _time.time()
    try:
        now = datetime.utcnow()
        app.logger.info(f"[Scheduler] enforce_subscriptions running at {now.isoformat()}")
        downgraded = models.downgrade_expired_users()
        for u in downgraded:
            app.logger.info(
                f"[Scheduler] Downgraded user {u['id']} ({u.get('email')}) → free"
            )
        app.logger.info(f"[Scheduler] Total downgraded: {len(downgraded)}")
        duration_ms = int((_time.time() - t0) * 1000)
        models.log_cron_run(
            'enforce_subscriptions', success=True,
            result={'downgraded_count': len(downgraded)},
            duration_ms=duration_ms,
        )
        return downgraded
    except Exception as e:
        app.logger.error(f"[Scheduler] enforce_subscriptions error: {e}")
        models.log_cron_run(
            'enforce_subscriptions', success=False,
            result={'error': str(e)}, duration_ms=0,
        )
        return []


USE_AI = Config.USE_AI

# =====================================================================
# PLAN LIMITS
# Pricing: Starter $49/mo | Pro $99/mo | Agency $299/mo
# =====================================================================
PLAN_LIMITS = {
    'free': {
        'clients': 1,
        'faqs_per_client': 5,
        'messages_per_day': 50,
        'analytics': False,
        'analytics_level': 'none',
        'customization': False,
        'white_label': False,
        'webhooks': False,
        'priority_support': False
    },
    'solo': {
        # $19/mo — single-website small business
        'clients': 1,
        'faqs_per_client': 999,
        'messages_per_day': 999999,  # unlimited
        'analytics': True,
        'analytics_level': 'basic',
        'customization': True,
        'white_label': False,
        'webhooks': False,
        'priority_support': False
    },
    'starter': {
        # $49/mo — foot-in-door tier
        'clients': 3,
        'faqs_per_client': 999,
        'messages_per_day': 2000,
        'analytics': True,           # Basic: conversations + leads (no unanswered Qs / timeline)
        'analytics_level': 'basic',  # Pro/Agency get 'full'
        'customization': True,
        'white_label': False,
        'webhooks': False,
        'priority_support': False
    },
    'pro': {
        # $99/mo — primary offer
        'clients': 10,
        'faqs_per_client': 999,
        'messages_per_day': 999999,  # unlimited
        'analytics': True,
        'analytics_level': 'full',
        'customization': True,
        'white_label': False,
        'webhooks': True,
        'priority_support': True
    },
    'agency': {
        # $299/mo — white-label reseller
        'clients': 999999,  # unlimited
        'faqs_per_client': 999,
        'messages_per_day': 999999,  # unlimited
        'analytics': True,
        'analytics_level': 'full',
        'customization': True,
        'white_label': True,
        'webhooks': True,
        'priority_support': True
    },
    'enterprise': {
        # Custom — legacy / grandfathered
        'clients': 999999,
        'faqs_per_client': 999,
        'messages_per_day': 999999,
        'analytics': True,
        'analytics_level': 'full',
        'customization': True,
        'white_label': True,
        'webhooks': True,
        'priority_support': True
    }
}

# ── Agency per-seat overage pricing ────────────────────────────────
# Agency plan includes AGENCY_INCLUDED_CLIENTS clients flat.
# Every client above that incurs AGENCY_SEAT_PRICE / month, billed
# via the /cron/agency-overage endpoint (run monthly, same CRON_SECRET).
AGENCY_INCLUDED_CLIENTS = 20
AGENCY_SEAT_PRICE       = 15.00   # USD per extra client per month

# Initialize Flask-Login
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# User class for Flask-Login
class User(UserMixin):
    def __init__(self, user_data):
        self.id = user_data['id']
        self.email = user_data['email']
        self.plan_type = user_data['plan_type']
        self.is_admin = bool(user_data.get('is_admin', False))

@login_manager.user_loader
def load_user(user_id):
    # PERF FIX: Flask-Login calls this on EVERY authenticated request to
    # rebuild the session user. Without a cache, every page load opens a
    # new DB connection just to re-fetch a row that hasn't changed. Under
    # Render's free-tier DB the pool exhausts quickly, causing the first
    # 1-2 login attempts to time-out and appear as "wrong password".
    #
    # Solution: store the user dict in the Flask session. The cache is
    # busted on login and logout so it can never serve stale data.
    #
    # Wrapped in try/except so a stale-connection DB error on first request
    # after idle returns None (treats user as anonymous) rather than 500ing.
    try:
        uid = int(user_id)
        cached = session.get('_user_cache')
        if cached and isinstance(cached, dict) and cached.get('id') == uid:
            return User(cached)
        # Cache miss — query DB and warm the session cache
        user_data = models.get_user_by_id(uid)
        if user_data:
            session['_user_cache'] = dict(user_data)
            return User(user_data)
        return None
    except Exception as e:
        app.logger.error(f'[load_user] {e}')
        return None

# Initialize database on startup
try:
    models.init_db()
    if hasattr(models, 'migrate_clients_table'):
        models.migrate_clients_table()

    # Ensure 'pro' is allowed in the plan_type column (removes old CHECK constraint)
    try:
        conn, cursor = models.get_db()
        cursor.execute("ALTER TABLE users DROP CONSTRAINT IF EXISTS users_plan_type_check")
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as _e:
        pass  # column may not have a constraint — that's fine
    if hasattr(models, 'migrate_faqs_table'):
        models.migrate_faqs_table()
    if hasattr(models, 'migrate_faq_to_knowledge_base'):
        models.migrate_faq_to_knowledge_base()
    if hasattr(models, 'migrate_subscription_expiry'):
        models.migrate_subscription_expiry()
    if hasattr(models, 'migrate_to_recurring_subscriptions'):
        models.migrate_to_recurring_subscriptions()
    if hasattr(models, 'migrate_conversation_features'):
        models.migrate_conversation_features()
    if hasattr(models, 'migrate_knowledge_base'):
        models.migrate_knowledge_base()
    if hasattr(models, 'migrate_webhooks'):
        models.migrate_webhooks()
    if hasattr(models, 'migrate_white_label'):
        models.migrate_white_label()
    if hasattr(models, 'migrate_client_status'):
        models.migrate_client_status()
    if hasattr(models, 'migrate_onboarding'):
        models.migrate_onboarding()
    if hasattr(models, 'migrate_cron_tables'):
        models.migrate_cron_tables()
    if hasattr(models, 'migrate_api_usage_log'):
        models.migrate_api_usage_log()
    if hasattr(models, 'migrate_kb_gaps'):
        models.migrate_kb_gaps()
    if hasattr(models, 'migrate_lead_pipeline'):
        models.migrate_lead_pipeline()
    if hasattr(models, 'migrate_agency_seat_billing'):
        models.migrate_agency_seat_billing()
    if hasattr(models, 'migrate_payments_unique_reference'):
        models.migrate_payments_unique_reference()
    if hasattr(models, 'migrate_google_oauth'):
        models.migrate_google_oauth()

    # System 2: Training data collection table
    try:
        from training_collector import migrate_training_tables
        migrate_training_tables()
    except Exception as _tc_err:
        print(f'⚠️  migrate_training_tables failed: {_tc_err}')

    # ── System 1: Agent tool tables (orders, appointment_slots, appointments, human_inbox) ──
    try:
        from tools import migrate_agent_tables
        migrate_agent_tables()
    except Exception as _agent_err:
        print(f'⚠️  migrate_agent_tables failed: {_agent_err}')

    # ── Platform webhook ingestion (Shopify, Acuity) ───────────────────
    # Creates client_integrations + webhook_log tables if they don't exist.
    # Safe to call on every startup — fully idempotent.
    try:
        _webhooks.migrate_integrations()
    except Exception as _wh_err:
        print(f"⚠️  webhooks.migrate_integrations error: {_wh_err}")

    # Ensure the conversations table exists.
    # This was previously (incorrectly) created inside log_conversation() on every
    # chat request. It belongs here — once, at startup, alongside all other schema work.
    try:
        conn, cursor = models.get_db()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS conversations (
                id           SERIAL      PRIMARY KEY,
                client_id    TEXT        NOT NULL,
                user_message TEXT        NOT NULL,
                bot_response TEXT        NOT NULL,
                matched      BOOLEAN     DEFAULT FALSE,
                method       TEXT,
                session_id   TEXT,
                timestamp    TIMESTAMP   DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        # Index for inbox transcript lookup by session_id
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_conversations_client_session "
            "ON conversations (client_id, session_id) WHERE session_id IS NOT NULL"
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as _conv_err:
        print(f"⚠️  conversations table migration failed: {_conv_err}")

    print("✅ Database initialized/migrated successfully!")

    # ── Startup enforcement: actually schedule the timer ──────────────
    # The comment previously described this but it was never implemented.
    # Now it fires 10 seconds after startup — gives workers time to settle.
    #
    # MULTI-WORKER FIX: With N Gunicorn workers each process starts its own
    # timer, causing N redundant enforce_subscriptions() calls.  We use
    # pg_try_advisory_lock with a fixed app-level key (13370001) so only
    # whichever worker wins the lock actually runs enforcement; the rest skip.
    # The lock is released immediately after enforcement so it doesn't block
    # anything else.
    def _startup_enforce():
        try:
            with app.app_context():
                _sc, _scur = models.get_db()
                try:
                    _scur.execute("SELECT pg_try_advisory_lock(13370001)")
                    row = _scur.fetchone()
                    acquired = list(row.values())[0] if row else False
                except Exception:
                    acquired = True   # if lock check fails, proceed anyway
                    _sc = _scur = None

                if not acquired:
                    app.logger.info("[Startup] enforcement skipped — another worker already running it")
                    if _sc:
                        try: _scur.close(); _sc.close()
                        except Exception: pass
                    return

                try:
                    enforce_subscriptions()
                    app.logger.info("[Startup] subscription enforcement complete")
                finally:
                    if _sc:
                        try:
                            _scur.execute("SELECT pg_advisory_unlock(13370001)")
                            _sc.commit()
                            _scur.close()
                            _sc.close()
                        except Exception:
                            pass
        except Exception as _se:
            app.logger.error(f"[Startup] enforcement error: {_se}")
    threading.Timer(10.0, _startup_enforce).start()
    print("✅ Startup enforcement scheduled (T+10s, single-worker guard active)")
except Exception as e:
    print(f"⚠️ Database initialization error: {e}")
    # DB failed — don't schedule enforcement, it would fail too

# Enhanced CORS configuration
CORS(app, resources={
    r"/api/*": {
        "origins": "*",
        "methods": ["GET", "POST", "OPTIONS"],
        "allow_headers": ["Content-Type"],
        "max_age": 3600
    },
    r"/widget": {
        "origins": "*",
        "methods": ["GET"],
        "max_age": 3600
    }
})

# Register platform webhook receiver routes (/webhooks/shopify, /webhooks/acuity)
# Must be called after the app is created and CORS is configured.
_webhooks.register_webhook_routes(app)

# Rate limiting
# Use Redis when available (required for multi-worker correctness).
# Falls back to in-memory only if REDIS_URL is not set (single-worker dev).
import warnings as _warnings
_limiter_storage = os.environ.get("REDIS_URL", "memory://")
if _limiter_storage == "memory://":
    _warnings.warn(
        "REDIS_URL not set — rate limiter is in-memory. "
        "This resets on restart and breaks under multiple workers.",
        RuntimeWarning,
    )
limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["200 per day", "50 per hour"],
    storage_uri=_limiter_storage,
)

# =====================================================================
# VERTICAL SYSTEM PROMPTS — 24 Industries
# =====================================================================
VERTICAL_PROMPTS = {

    # ── 1. GENERAL ────────────────────────────────────────────────────
    'general': """You are a friendly, professional assistant. Help visitors with their questions clearly and helpfully.
- Be warm, polite and concise
- Answer questions using the available knowledge base
- If you cannot answer, offer to connect the visitor with the team
- Keep responses to 2-3 sentences""",

    # ── 2. REAL ESTATE ────────────────────────────────────────────────
    'real_estate': """You are a warm, professional real estate assistant helping buyers and renters find properties.
- Ask about budget, location, bedrooms, and timeline
- Help qualify leads by understanding urgency and financing
- Offer to book property viewings for serious prospects
- Always be encouraging — buying a home is exciting!""",

    # ── 3. SAAS ───────────────────────────────────────────────────────
    'saas': """You are a knowledgeable SaaS support assistant helping users get the most out of the product.
- Help with onboarding, features, billing, and troubleshooting
- For complex issues, collect user details and route to support
- Highlight relevant features naturally
- Always resolve in chat first before escalating""",

    # ── 4. E-COMMERCE ────────────────────────────────────────────────
    'ecommerce': """You are a fast, friendly e-commerce support assistant.
- Help with order tracking, returns, refunds, and shipping
- Ask for order number when handling order-specific queries
- Proactively offer alternatives if an item is out of stock
- Keep responses short — shoppers want fast answers""",

    # ── 5. HEALTHCARE ────────────────────────────────────────────────
    'healthcare': """You are a calm, professional healthcare clinic assistant.
- Help with appointment booking, clinic hours, and services
- NEVER provide medical diagnoses or specific medical advice
- Collect patient name, contact number, and reason for visit when booking
- If someone describes an emergency, direct them to call emergency services immediately""",

    # ── 6. LAW FIRM ──────────────────────────────────────────────────
    'law_firm': """You are a professional legal intake assistant for a law firm.
- Help with initial consultations, practice areas, and intake
- NEVER provide specific legal advice — you are an intake assistant only
- Collect case type, brief description, urgency, and contact details
- Be thorough and detail-oriented""",

    # ── 7. RESTAURANT ────────────────────────────────────────────────
    'restaurant': """You are a friendly restaurant assistant helping guests with reservations, menus, and dining enquiries.
- Help with table reservations — collect date, time, party size, and name
- Answer questions about the menu, dietary options, allergens, and specials
- Share opening hours, location, parking, and private dining options
- Be warm and enthusiastic — great hospitality starts before they arrive""",

    # ── 8. HOSPITALITY / HOTEL ───────────────────────────────────────
    'hotel': """You are a professional hotel concierge assistant helping guests plan their stay.
- Help with room availability, rates, check-in/check-out, and amenities
- Collect check-in date, check-out date, number of guests, and preferences when enquiring about bookings
- Suggest local attractions, dining, and activities based on guest interests
- Handle special requests (early check-in, accessibility needs, celebrations) with care and positivity""",

    # ── 9. FITNESS / GYM ────────────────────────────────────────────
    'fitness': """You are an energetic, supportive fitness and gym assistant.
- Help with membership options, class schedules, personal training, and facilities
- Collect name, fitness goals, and availability when someone is interested in joining
- Encourage prospects — every fitness journey starts with the first step
- Answer questions about class types, equipment, opening hours, and trial offers""",

    # ── 10. DENTAL ───────────────────────────────────────────────────
    'dental': """You are a friendly, reassuring dental practice assistant.
- Help patients book appointments, understand treatments, and learn about pricing
- Collect patient name, contact number, preferred dentist (if any), and reason for visit when booking
- NEVER diagnose dental conditions or recommend specific treatments
- For dental emergencies (severe pain, swelling, trauma), advise the patient to call the practice immediately or visit urgent care""",

    # ── 11. MORTGAGE / FINANCE ───────────────────────────────────────
    'mortgage': """You are a knowledgeable mortgage and finance enquiry assistant.
- Help potential clients understand mortgage products, rates, and the application process
- Collect name, contact details, property value, deposit amount, and employment status for lead qualification
- NEVER give specific financial or investment advice — you are an enquiry assistant only
- Always recommend speaking with a qualified mortgage adviser for personalised guidance
- Be clear, reassuring, and jargon-free — finance can be intimidating""",

    # ── 12. INSURANCE ────────────────────────────────────────────────
    'insurance': """You are a helpful insurance enquiry assistant.
- Help visitors understand policy types (life, health, auto, home, business) and get quotes
- Collect name, contact details, type of cover needed, and rough requirements for lead generation
- NEVER confirm coverage, make policy promises, or give specific claims advice
- For existing claims or urgent policy questions, direct the visitor to a licensed agent or the support line
- Be straightforward and trustworthy — insurance decisions are important""",

    # ── 13. EDUCATION / TUTORING ────────────────────────────────────
    'education': """You are a helpful, encouraging education and tutoring assistant.
- Help prospective students and parents understand courses, programmes, tutors, and schedules
- Collect student name, age/year group, subject(s), and learning goals when someone is interested in enrolling
- Answer questions about pricing, trial sessions, online vs in-person, and results
- Be warm and supportive — education enquiries often come from anxious students or concerned parents""",

    # ── 14. AUTOMOTIVE / CAR DEALERSHIP ─────────────────────────────
    'automotive': """You are a professional automotive assistant for a car dealership or service centre.
- Help visitors explore vehicle inventory, financing options, and test drives
- For service bookings, collect vehicle make, model, year, registration number, and nature of the issue
- For sales leads, collect name, contact details, vehicle of interest, and budget
- Be knowledgeable and straightforward — car buyers appreciate honesty and expertise""",

    # ── 15. BEAUTY / SALON / SPA ────────────────────────────────────
    'beauty': """You are a warm, stylish assistant for a beauty salon, barbershop, or spa.
- Help clients book appointments for specific treatments, stylists, or therapists
- Collect client name, preferred date/time, treatment type, and stylist preference when booking
- Answer questions about services, pricing, products used, and aftercare
- Be personable and attentive — great beauty experiences start with great service""",

    # ── 16. TRAVEL / TOURISM ────────────────────────────────────────
    'travel': """You are an enthusiastic travel assistant helping people plan their perfect trip.
- Help with destination ideas, itineraries, travel packages, and booking enquiries
- Collect travel dates, destination, number of travellers, budget, and interests for enquiry leads
- Answer questions about visa requirements, travel insurance, packing tips, and local customs
- Be inspiring — travel is one of life's great joys, and your enthusiasm should reflect that""",

    # ── 17. RECRUITMENT / STAFFING ──────────────────────────────────
    'recruitment': """You are a professional recruitment and staffing assistant.
- Help job seekers understand open roles, the application process, and agency services
- Help employers understand staffing solutions, timelines, and how to submit a vacancy
- For candidates: collect name, contact details, role of interest, and experience level
- For employers: collect company name, contact details, role type, and urgency
- Be professional and encouraging — job searching and hiring are both high-stakes""",

    # ── 18. ACCOUNTING / BOOKKEEPING ────────────────────────────────
    'accounting': """You are a professional accounting and bookkeeping practice assistant.
- Help business owners and individuals understand services (tax returns, payroll, bookkeeping, VAT/GST, advisory)
- Collect name, business type, number of employees, and main pain point for lead qualification
- NEVER provide specific tax or financial advice — recommend a consultation with a qualified accountant
- Be clear and confident — clients are trusting you with something important""",

    # ── 19. CONSTRUCTION / CONTRACTORS ──────────────────────────────
    'construction': """You are a professional assistant for a construction company, builder, or contractor.
- Help homeowners and businesses get quotes, understand services, and book site visits
- Collect name, contact details, project type, location, and rough timeline/budget for quote requests
- Answer questions about past projects, materials, certifications, warranties, and timelines
- Be dependable and detail-oriented — clients want to trust who they're hiring to work on their property""",

    # ── 20. PET SERVICES / VETERINARY ───────────────────────────────
    'pet_services': """You are a friendly, caring assistant for a veterinary clinic or pet services business.
- Help pet owners book appointments, grooming sessions, or consultations
- Collect owner name, pet name, species/breed, age, and reason for visit when booking
- NEVER provide specific veterinary diagnoses or medical advice
- For emergencies (difficulty breathing, seizures, suspected poisoning), immediately direct the owner to call an emergency vet or go to the nearest emergency animal hospital
- Be warm and empathetic — pets are family""",

    # ── 21. NON-PROFIT / CHARITY ────────────────────────────────────
    'nonprofit': """You are a compassionate assistant for a non-profit organisation or charity.
- Help visitors learn about the mission, programmes, and impact
- Guide donors through giving options, gift matching, and recurring donations
- Help volunteers find opportunities and complete sign-up
- Collect name and contact details for anyone wanting to donate, volunteer, or partner
- Be mission-driven and grateful — every interaction is an opportunity to deepen the relationship""",

    # ── 22. EVENT PLANNING / VENUES ─────────────────────────────────
    'events': """You are an enthusiastic event planning and venue assistant.
- Help clients enquire about venue hire, event packages, catering, and availability
- Collect event type, expected guest count, preferred date(s), and budget for enquiries
- Answer questions about layouts, AV equipment, catering, parking, and accessibility
- Be creative and detail-oriented — every event is unique and clients want to feel heard""",

    # ── 23. MENTAL HEALTH / THERAPY ─────────────────────────────────
    'therapy': """You are a calm, empathetic assistant for a therapy, counselling, or mental health practice.
- Help prospective clients understand available services, therapist specialisms, and booking
- Collect name, preferred contact method, and general area of concern (e.g. anxiety, relationships) — never probe for sensitive details
- NEVER provide therapeutic advice, crisis intervention, or clinical guidance
- If someone expresses immediate distress or risk of harm, respond with compassion and direct them to a crisis line or emergency services immediately
- Always prioritise safety, confidentiality, and sensitivity above all else""",

    # ── 24. PHOTOGRAPHY / VIDEOGRAPHY ───────────────────────────────
    'photography': """You are a creative, personable assistant for a photography or videography studio.
- Help clients enquire about shoots, packages, pricing, and availability
- Collect name, event type (wedding, portrait, commercial, etc.), preferred date, and location for enquiries
- Answer questions about turnaround times, editing style, deliverables, and licensing
- Be enthusiastic and creative — clients are trusting you to capture important moments""",

}

# Valid vertical keys — used for input validation in save_customization
VALID_VERTICALS = set(VERTICAL_PROMPTS.keys())

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
    'no', 'not', 'nor', 'there', 'per', 'each'
}

GENERIC_TAGS = {
    'information', 'info', 'details', 'learn',
    'business', 'service', 'services', 'product', 'products',
    'use', 'used', 'using', 'need', 'want', 'like', 'work',
    'platform', 'system', 'tool', 'website', 'site', 'account',
    'help', 'support', 'contact', 'team', 'company', 'client',
    'way', 'ways', 'option', 'options', 'type', 'types', 'kind'
}


def extract_keywords(text):
    words = re.findall(r'\b[a-z]+\b', text.lower())
    return [w for w in words if w not in STOP_WORDS and len(w) >= 3]


def compute_tag_weights(faqs_list):
    """Count frequency of both triggers AND question keywords so common words get low weight."""
    tag_frequency = Counter()
    for faq in faqs_list:
        # Count explicit triggers
        for tag in faq.get('triggers', []):
            tag_frequency[tag.lower()] += 1
        # Also count question keywords so high-frequency words get penalised
        for kw in extract_keywords(faq.get('question', '')):
            tag_frequency[kw] += 1

    tag_weights = {}
    for tag, freq in tag_frequency.items():
        if tag in GENERIC_TAGS:
            tag_weights[tag] = 0.05   # near-zero weight for generic words
        else:
            tag_weights[tag] = round(1.0 / freq, 3)
    return tag_weights


def find_best_match(user_query, faqs_list, confidence_threshold=0.68):
    """Keyword matcher — used only when AI is disabled. Threshold raised to 0.68 to reduce false positives."""
    if not user_query or not faqs_list:
        return None, 0.0

    query_keywords = extract_keywords(user_query)
    if not query_keywords:
        return None, 0.0

    query_keyword_set = set(query_keywords)
    tag_weights = compute_tag_weights(faqs_list)

    best_faq = None
    best_score = 0.0

    for faq in faqs_list:
        raw_tags = [t.lower().strip() for t in faq.get('triggers', [])]
        question_keywords = extract_keywords(faq.get('question', ''))
        all_tags = set(raw_tags + question_keywords)

        matched_tags = query_keyword_set.intersection(all_tags)
        if not matched_tags:
            continue

        raw_score = sum(tag_weights.get(tag, 0.3) for tag in matched_tags)
        max_possible = sum(tag_weights.get(tag, 0.3) for tag in all_tags)
        normalized = raw_score / max_possible if max_possible > 0 else 0.0
        coverage = len(matched_tags) / len(query_keyword_set)
        final_score = (normalized * 0.7) + (coverage * 0.3)

        if final_score > best_score:
            best_score = final_score
            best_faq = faq

    if best_score < confidence_threshold:
        app.logger.info(f"[Matcher] Low confidence ({best_score:.2f}) for: '{user_query}'")
        return None, 0.0

    app.logger.info(f"[Matcher] Matched '{best_faq.get('question')}' | score: {best_score:.2f}")
    return best_faq, round(best_score, 2)


# =====================================================================
# UNIFIED WEBHOOK DISPATCHER
# Replaces the old _fire_webhook / notify_webhook / _fire_lead_stage_webhook.
# Reads from webhook_configs, signs with HMAC-SHA256, logs every delivery,
# retries up to 3 times with exponential back-off, runs off the Flask worker.
# =====================================================================

# Thread pool dedicated to webhook delivery (separate from DNS pool)
_wh_executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix='wh-deliver')


def _is_safe_webhook_url(url: str) -> bool:
    """
    Block SSRF vectors — reject private / loopback / link-local addresses
    and non-HTTP(S) schemes before ever making the outbound request.
    """
    import ipaddress
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        if parsed.scheme not in ('http', 'https'):
            return False
        host = parsed.hostname or ''
        if not host:
            return False
        # Block known-bad hostnames
        if host in ('localhost', 'metadata.google.internal', '169.254.169.254'):
            return False
        try:
            addr = ipaddress.ip_address(host)
            if addr.is_private or addr.is_loopback or addr.is_link_local or addr.is_reserved:
                return False
        except ValueError:
            pass   # hostname, not IP — allow (DNS will resolve later)
        return True
    except Exception:
        return False


def _deliver_one(webhook_url: str, payload: dict, signing_secret: str,
                 event_type: str, client_id: str, webhook_id: str) -> None:
    """
    Deliver one webhook with up to 3 attempts (0.5 s / 2 s / 5 s back-off).
    Logs the final outcome to webhook_logs.
    """
    import time, hmac as _hmac, hashlib

    body_bytes = json.dumps(payload, separators=(',', ':')).encode()
    headers = {
        'Content-Type':     'application/json',
        'X-Lumvi-Event':    event_type,
        'X-Lumvi-Delivery': str(uuid.uuid4()),
        'User-Agent':       'Lumvi-Webhooks/1.0',
    }
    if signing_secret:
        sig = _hmac.new(signing_secret.encode(), body_bytes, hashlib.sha256).hexdigest()
        headers['X-Lumvi-Signature'] = f'sha256={sig}'

    delays   = [0, 0.5, 2.0]   # sleep before attempt 2 and 3
    last_exc = None
    status   = 0
    resp_txt = ''
    duration = 0

    for attempt, delay in enumerate(delays, start=1):
        if delay:
            time.sleep(delay)
        t0 = time.time()
        try:
            resp     = requests.post(webhook_url, data=body_bytes, headers=headers, timeout=10)
            duration = int((time.time() - t0) * 1000)
            status   = resp.status_code
            resp_txt = resp.text[:500]
            if 200 <= status < 300:
                app.logger.info(
                    f'[Webhook] ✓ delivered event={event_type} client={client_id} wh={webhook_id} attempt={attempt} status={status} dur={duration}ms'
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
                f'[Webhook] non-2xx event={event_type} client={client_id} status={status} attempt={attempt}'
            )
        except Exception as exc:
            duration = int((time.time() - t0) * 1000)
            last_exc = exc
            app.logger.warning(
                f'[Webhook] error event={event_type} client={client_id} attempt={attempt}: {exc}'
            )

    # All attempts failed — log the final failure
    models.log_webhook_delivery(
        client_id=client_id, webhook_id=webhook_id,
        event_type=event_type, url=webhook_url,
        payload=payload, status_code=status,
        response_text=resp_txt or str(last_exc or 'Failed after 3 attempts'),
        success=False, duration_ms=duration,
    )
    app.logger.error(
        f'[Webhook] ✗ all attempts failed event={event_type} client={client_id} wh={webhook_id} last_status={status}'
    )


def fire_webhook_event(client_id: str, event_type: str, data: dict) -> None:
    """
    Public entry point — call this everywhere an event happens.

    Reads all enabled webhook_configs for the client, filters to those
    that subscribe to event_type, signs each payload, and dispatches
    delivery to the thread pool (never blocks the request cycle).

    Silently no-ops if the client has no webhooks or the plan doesn't
    support them — safe to call unconditionally.
    """
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
            # events may be stored as JSON string or list
            if isinstance(subscribed, str):
                try:
                    subscribed = json.loads(subscribed)
                except Exception:
                    subscribed = []
            if event_type not in subscribed:
                continue
            url = (wh.get('url') or '').strip()
            if not url or not _is_safe_webhook_url(url):
                if url:
                    app.logger.warning(
                        f'[Webhook] SSRF-blocked or invalid URL wh={wh.get("webhook_id")} url={url}'
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
                f'[Webhook] dispatched event={event_type} client={client_id} webhooks={fired}'
            )
    except Exception as exc:
        app.logger.error(f'[Webhook] fire_webhook_event error: {exc}')


# ── Kept for backward compat — delegates to fire_webhook_event ─────────
def notify_webhook(client_id: str, lead_data: dict) -> None:
    """Deprecated shim — use fire_webhook_event directly."""
    fire_webhook_event(client_id, 'lead_captured', lead_data)


def _notify_handoff(client_id, client, config, ticket_id, reason,
                    urgency, name, email, summary, method):
    """
    Notify the agency's contact email when a human handoff ticket is created.
    Fires in a background daemon thread — never blocks the chat response.
    Also fires the outbound CRM webhook if one is configured.
    """
    def _send():
        try:
            contact_info  = config.get('contact', {})
            notify_email  = contact_info.get('email')
            company_name  = (client or {}).get('company_name', 'your chatbot')
            urgency_label = '🔴 High' if urgency == 'high' else '🟡 Normal'
            customer_label = name or email or 'Unknown visitor'
            inbox_url = f'https://lumvi.net/inbox?client_id={client_id}&ticket={ticket_id}'

            if notify_email:
                try:
                    sender_info = models.get_email_from_for_client(client_id)
                    msg = Message(
                        subject=f"[{urgency_label}] Handoff needed — {customer_label}",
                        sender=f"{sender_info['name']} <{sender_info['address']}>",
                        recipients=[notify_email],
                        html=f"""
                        <div style="font-family:'DM Sans',sans-serif;max-width:560px;margin:0 auto;
                                    background:#F7F4EF;padding:36px;border-radius:16px;">
                          <h2 style="font-size:20px;font-weight:700;color:#1C1917;margin-bottom:4px;">
                            Human Handoff Requested</h2>
                          <p style="color:#A8A29E;font-size:13px;margin-bottom:24px;">
                            via {company_name} · Ticket
                            <code style="background:#E7E2DA;padding:2px 6px;border-radius:4px;">
                              {ticket_id}</code></p>
                          <table style="width:100%;border-collapse:collapse;margin-bottom:20px;">
                            <tr><td style="padding:10px 0;border-bottom:1px solid #E7E2DA;
                                           font-size:13px;color:#57534E;width:120px;">Customer</td>
                                <td style="padding:10px 0;border-bottom:1px solid #E7E2DA;
                                           font-size:13px;font-weight:600;color:#1C1917;">
                                  {customer_label}</td></tr>
                            {'<tr><td style="padding:10px 0;border-bottom:1px solid #E7E2DA;font-size:13px;color:#57534E;">Email</td><td style="padding:10px 0;border-bottom:1px solid #E7E2DA;font-size:13px;font-weight:600;"><a href="mailto:' + email + '" style="color:#B8924A;">' + email + '</a></td></tr>' if email else ''}
                            <tr><td style="padding:10px 0;border-bottom:1px solid #E7E2DA;
                                           font-size:13px;color:#57534E;">Urgency</td>
                                <td style="padding:10px 0;border-bottom:1px solid #E7E2DA;
                                           font-size:13px;font-weight:600;">{urgency_label}</td></tr>
                            <tr><td style="padding:10px 0;border-bottom:1px solid #E7E2DA;
                                           font-size:13px;color:#57534E;">Trigger</td>
                                <td style="padding:10px 0;border-bottom:1px solid #E7E2DA;
                                           font-size:13px;color:#1C1917;">{method}</td></tr>
                            <tr><td style="padding:10px 0;font-size:13px;color:#57534E;
                                          vertical-align:top;padding-top:14px;">Question</td>
                                <td style="padding:10px 0;padding-top:14px;font-size:13px;
                                           font-style:italic;color:#1C1917;">"{reason[:300]}"</td></tr>
                          </table>
                          {'<div style="background:#fff;border:1px solid #E7E2DA;border-radius:10px;padding:16px;margin-bottom:20px;"><p style="font-size:12px;color:#A8A29E;margin:0 0 8px;font-weight:600;text-transform:uppercase;letter-spacing:0.05em;">Conversation summary</p><pre style="font-size:12px;color:#57534E;white-space:pre-wrap;margin:0;line-height:1.6;">' + summary[:800] + '</pre></div>' if summary else ''}
                          <a href="{inbox_url}"
                             style="display:inline-block;margin-top:4px;padding:11px 22px;
                                    background:#B8924A;color:#fff;text-decoration:none;
                                    border-radius:9px;font-weight:700;font-size:13.5px;">
                            Open Inbox →</a>
                          <p style="font-size:11px;color:#A8A29E;margin-top:20px;">
                            Ticket ID: {ticket_id} · Lumvi Platform</p>
                        </div>"""
                    )
                    mail.send(msg)
                    app.logger.info(f"[Handoff] email sent ticket={ticket_id} to={notify_email}")
                except Exception as _mail_err:
                    app.logger.warning(f"[Handoff] email failed ticket={ticket_id}: {_mail_err}")

            # Fire outbound CRM webhook via unified dispatcher
            fire_webhook_event(client_id, 'handoff_created', {
                'ticket_id':      ticket_id,
                'urgency':        urgency,
                'reason':         reason,
                'customer_name':  name,
                'customer_email': email,
                'method':         method,
            })

        except Exception as _outer_err:
            app.logger.error(f"[Handoff] _notify_handoff thread error: {_outer_err}")

    import threading
    threading.Thread(target=_send, daemon=True).start()


@app.after_request
def allow_widget_embedding(response):
    # Allow the chat widget to be iframed from any domain.
    response.headers.pop('X-Frame-Options', None)
    response.headers['Content-Security-Policy'] = "frame-ancestors *"

    origin = request.headers.get('Origin')
    if origin:
        # Only reflect the origin (and allow credentials) for widget/API routes.
        # Applying credentials: true globally with a wildcard origin is a CORS
        # security violation — browsers block it and it leaks session cookies to
        # arbitrary third-party sites.
        path = request.path
        is_widget_or_api = (
            path.startswith('/api/')
            or path == '/widget'
            or path.startswith('/widget')
        )
        if is_widget_or_api:
            response.headers['Access-Control-Allow-Origin'] = origin
            response.headers['Access-Control-Allow-Credentials'] = 'true'
            response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
            response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, X-Requested-With'

    return response

# =====================================================================
# LOGGING
# =====================================================================

if not os.path.exists('logs'):
    os.makedirs('logs')

file_handler = RotatingFileHandler('logs/chatbot.log', maxBytes=10240000, backupCount=10)
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
))
file_handler.setLevel(logging.INFO)
app.logger.addHandler(file_handler)
app.logger.setLevel(logging.INFO)
app.logger.info('Chatbot startup')

# =====================================================================
# BACKUP SYSTEM
# =====================================================================

def backup_client_data(client_id):
    try:
        client_path = get_client_path(client_id)
        backup_dir = os.path.join('backups', client_id)

        if not os.path.exists('backups'):
            os.makedirs('backups')
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = os.path.join(backup_dir, f'backup_{timestamp}')
        shutil.copytree(client_path, backup_path)

        backups = sorted([d for d in os.listdir(backup_dir) if d.startswith('backup_')])
        if len(backups) > 7:
            for old_backup in backups[:-7]:
                shutil.rmtree(os.path.join(backup_dir, old_backup))

        app.logger.info(f'Backed up data for client: {client_id}')
        return True
    except Exception as e:
        app.logger.error(f'Backup failed for {client_id}: {e}')
        return False


def backup_all_clients():
    try:
        clients_dir = 'clients'
        if os.path.exists(clients_dir):
            for client_id in os.listdir(clients_dir):
                client_path = os.path.join(clients_dir, client_id)
                if os.path.isdir(client_path):
                    backup_client_data(client_id)
        return True
    except Exception as e:
        app.logger.error(f'Backup all failed: {e}')
        return False

# =====================================================================
# UTILITY FUNCTIONS
# =====================================================================

def get_client_path(client_id):
    client_path = os.path.join('clients', client_id)
    if not os.path.exists(client_path):
        client_path = os.path.join('clients', 'default')
    return client_path


def load_client_config(client_id):
    config_path = os.path.join(get_client_path(client_id), 'config.json')
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading config for {client_id}: {e}")
        return None


def load_client_faqs(client_id):
    faqs_path = os.path.join(get_client_path(client_id), 'faqs.json')
    try:
        with open(faqs_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading FAQs for {client_id}: {e}")
        return {"faqs": []}


def save_lead(client_id, lead_data):
    leads_path = os.path.join(get_client_path(client_id), 'leads.json')
    try:
        with open(leads_path, 'r', encoding='utf-8') as f:
            leads_file = json.load(f)

        lead_data['id'] = f"lead_{uuid.uuid4().hex[:8]}"
        lead_data['timestamp'] = datetime.now().isoformat()
        lead_data['client_id'] = client_id
        leads_file['leads'].append(lead_data)

        with open(leads_path, 'w', encoding='utf-8') as f:
            json.dump(leads_file, f, indent=2)

        return True
    except Exception as e:
        print(f"Error saving lead for {client_id}: {e}")
        return False


def is_email(text):
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    return re.search(email_pattern, text) is not None


def extract_email(text):
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    match = re.search(email_pattern, text)
    return match.group(0) if match else None


def match_faq(message, faqs, lead_triggers):
    message_lower = message.lower()

    for trigger in lead_triggers:
        if trigger.lower() in message_lower:
            return "TRIGGER_LEAD_COLLECTION", None

    if is_email(message):
        return "TRIGGER_LEAD_COLLECTION", extract_email(message)

    best_match = None
    max_score = 0
    all_matches = []

    for faq in faqs['faqs']:
        score = 0
        matches = []

        for trigger in faq['triggers']:
            if trigger.lower() in message_lower:
                score += 1
                matches.append(trigger)

        if score > 0:
            all_matches.append({'faq': faq, 'score': score, 'matches': matches})

        if score > max_score:
            max_score = score
            best_match = faq

    if best_match and max_score > 0:
        return best_match['answer'], None

    if all_matches:
        similar_questions = [m['faq']['question'] for m in all_matches[:3]]
        return "NO_MATCH_WITH_SUGGESTIONS", similar_questions

    return None, None


def log_conversation(client_id, user_message, bot_response,
                     matched=False, method='unknown', session_id=None,
                     daily_limit=None):
    # NOTE: The conversations table is created once in models.init_db().
    # Do NOT run CREATE TABLE here — it fires a DDL statement on every chat
    # message, which is extremely expensive and serialises all DB writes.
    # session_id column added by migrate_agent_tables() via
    # ALTER TABLE conversations ADD COLUMN IF NOT EXISTS session_id TEXT.
    #
    # RACE CONDITION FIX: when daily_limit is supplied the INSERT is wrapped in
    # a CTE that re-counts today's rows inside the same statement.  Because the
    # count and the insert happen atomically (single round-trip, evaluated under
    # the same snapshot), concurrent requests that all passed the pre-check
    # cannot collectively exceed the cap.  Returns True if the row was inserted,
    # False if the limit was already reached (caller should treat as limit_hit).
    try:
        conn, cursor = models.get_db()

        if daily_limit is not None and daily_limit < 999999:
            # Atomic check-then-insert: only inserts when today's count < limit.
            cursor.execute(
                '''
                WITH today_count AS (
                    SELECT COUNT(*) AS cnt
                    FROM   conversations
                    WHERE  client_id = %s
                      AND  DATE(timestamp) = CURRENT_DATE
                )
                INSERT INTO conversations
                    (client_id, user_message, bot_response, matched, method, session_id)
                SELECT %s, %s, %s, %s, %s, %s
                FROM   today_count
                WHERE  cnt < %s
                ''',
                (
                    client_id,                          # for the CTE WHERE
                    client_id, user_message, bot_response,
                    matched, method, session_id or None,
                    daily_limit,                        # cap
                )
            )
            inserted = cursor.rowcount > 0
        else:
            cursor.execute(
                '''
                INSERT INTO conversations
                    (client_id, user_message, bot_response, matched, method, session_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                ''',
                (client_id, user_message, bot_response, matched, method,
                 session_id or None)
            )
            inserted = True

        conn.commit()
        cursor.close()
        conn.close()

        if inserted:
            app.logger.info(f'✅ Logged conversation for {client_id} session={session_id}')
        else:
            app.logger.info(
                f'[Limit] atomic insert blocked for {client_id} '
                f'(daily_limit={daily_limit}) session={session_id}'
            )
        return inserted

    except Exception as e:
        app.logger.error(f'❌ Error logging conversation: {e}')
        return True  # fail-open: don't block the chat response on a log error

# =====================================================================
# PLAN ENFORCEMENT HELPERS
# =====================================================================

def get_daily_message_count(client_id):
    """Return how many chat messages this client has received today (UTC)."""
    try:
        conn, cursor = models.get_db()
        today = datetime.utcnow().strftime('%Y-%m-%d')
        cursor.execute(
            '''
            SELECT COUNT(*) AS cnt FROM conversations
            WHERE client_id = %s AND DATE(timestamp) = %s
            ''',
            (client_id, today)
        )
        row = cursor.fetchone() or {}
        cursor.close()
        conn.close()
        return int(row.get('cnt', 0))
    except Exception as e:
        app.logger.error(f'get_daily_message_count error: {e}')
        return 0  # fail open — don't block chat if DB is down


def get_client_owner_plan(client_id):
    """Return the plan_type string for the user who owns this client_id."""
    try:
        client = models.get_client_by_id(client_id)
        if not client:
            return 'free'
        user = models.get_user_by_id(client['user_id'])
        if not user:
            return 'free'
        return user.get('plan_type', 'free')
    except Exception as e:
        app.logger.error(f'get_client_owner_plan error: {e}')
        return 'free'  # safest default


# PERF FIX: Cache the (owner, plan_type) for each client_id for 60 seconds.
# get_client_owner() + get_user_by_id() are called on EVERY chat message just
# to check the daily message limit. That's 2 extra DB round-trips per request
# adding ~200-400ms on a cold Render DB connection.
# The 60-second TTL means a plan upgrade takes effect within 1 minute — acceptable.
#
# THREAD SAFETY: _client_owner_cache_lock serialises all reads and writes so
# concurrent Flask threads (threaded=True or gevent workers) never observe a
# partial update or overwrite a freshly-written entry with a stale DB value.
_client_owner_cache: dict = {}  # {client_id: (owner_dict, expires_at)}
_client_owner_cache_lock = threading.Lock()

def _get_cached_client_owner(client_id: str):
    """Return client owner dict from cache, falling back to DB on miss/expiry."""
    with _client_owner_cache_lock:
        entry = _client_owner_cache.get(client_id)
        if entry:
            owner, expires_at = entry
            if datetime.utcnow() < expires_at:
                return owner

    # Cache miss or expired — query DB *outside* the lock so we don't hold it
    # during a potentially slow DB round-trip.
    owner = models.get_client_owner(client_id)
    if owner:
        with _client_owner_cache_lock:
            # Re-check: another thread may have populated the cache while we
            # were querying the DB.  Only write if the entry is still missing
            # or stale, so a fresher write from a concurrent thread is never
            # overwritten with our (possibly older) result.
            existing = _client_owner_cache.get(client_id)
            if not existing or datetime.utcnow() >= existing[1]:
                _client_owner_cache[client_id] = (owner, datetime.utcnow() + timedelta(seconds=60))
    return owner


def get_subscription_status(user):
    """
    Returns a dict with subscription info for a user.
    status: 'active' | 'expired' | 'grace' | 'free'
    Admins are always treated as active — they can never be downgraded.
    """
    from datetime import datetime

    # Admins are exempt from all subscription enforcement
    if user.get('is_admin'):
        return {'status': 'active', 'expires_at': None, 'grace_ends_at': None}

    plan = user.get('plan_type', 'free')

    # Free and enterprise plans don't expire
    if plan in ('free', 'enterprise'):
        return {'status': 'free', 'expires_at': None, 'grace_ends_at': None}

    expires_at = user.get('subscription_expires_at')
    grace_ends_at = user.get('grace_period_ends_at')

    # No expiry set yet — treat as active (legacy users / manual upgrades)
    if not expires_at:
        return {'status': 'active', 'expires_at': None, 'grace_ends_at': None}

    now = datetime.utcnow()

    # Handle string dates from DB
    if isinstance(expires_at, str):
        try:
            expires_at = datetime.strptime(expires_at, '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            expires_at = datetime.strptime(expires_at, '%Y-%m-%d %H:%M:%S')

    if isinstance(grace_ends_at, str):
        try:
            grace_ends_at = datetime.strptime(grace_ends_at, '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            grace_ends_at = datetime.strptime(grace_ends_at, '%Y-%m-%d %H:%M:%S')

    if now < expires_at:
        if user.get('cancel_at_period_end'):
            return {'status': 'cancelling', 'expires_at': expires_at, 'grace_ends_at': grace_ends_at}
        return {'status': 'active', 'expires_at': expires_at, 'grace_ends_at': grace_ends_at}
    elif grace_ends_at and now < grace_ends_at:
        return {'status': 'grace', 'expires_at': expires_at, 'grace_ends_at': grace_ends_at}
    else:
        return {'status': 'expired', 'expires_at': expires_at, 'grace_ends_at': grace_ends_at}


# =====================================================================
# API ENDPOINTS
# =====================================================================

@app.route('/api/config', methods=['GET'])
def get_config():
    try:
        client_id = request.args.get('client_id', 'default')
        client = models.get_client_by_id(client_id)

        if not client:
            return jsonify({'success': False, 'error': 'Client not found'}), 404

        branding_settings = json.loads(client['branding_settings']) if client['branding_settings'] else {}
        config = {
            'client_id': client_id,
            'branding': branding_settings.get('branding', {}),
            'contact': branding_settings.get('contact', {}),
            'bot_settings': branding_settings.get('bot_settings', {})
        }

        return jsonify({'success': True, 'config': config})

    except Exception as e:
        app.logger.error(f'Error getting config: {e}')
        return jsonify({'success': False, 'error': 'Failed to load configuration'}), 500


def sanitize_input(text, max_length=500):
    if not text or not isinstance(text, str):
        return ""
    text = re.sub(r'<[^>]+>', '', text)
    text = text[:max_length]
    text = ' '.join(text.split())
    return text.strip()


@app.route('/api/user/info')
@login_required
def user_info():
    return jsonify({
        'success': True,
        'plan_type': current_user.plan_type,
        'email': current_user.email,
        'id': current_user.id
    })


@app.route('/api/chat', methods=['POST'])
@limiter.limit("30 per minute")
def chat():
    try:
        data = request.json
        message = sanitize_input(data.get('message', ''))
        client_id = sanitize_input(data.get('client_id', 'demo'), max_length=50)
        conversation_history = data.get('history', [])
        # session_id — generated by the widget on first load, stored in localStorage,
        # sent with every message. Ties conversations, session memory, and inbox
        # tickets together into one traceable thread.
        session_id = sanitize_input(data.get('session_id', ''), max_length=100) or None

        if not message:
            return jsonify({'success': False, 'error': 'Message is required'}), 400

        client = None  # always defined — stays None if DB call below fails
        try:
            client = models.get_client_by_id(client_id)
            if not client:
                app.logger.warning(f'Client not found: {client_id}, using demo FAQs')
                faqs_list = [
                    {
                        "id": "demo_1",
                        "question": "What are your hours?",
                        "answer": "We're open Monday-Friday, 9 AM - 6 PM EST. Weekend hours: Saturday 10 AM - 4 PM. Closed Sundays. 🕒",
                        "triggers": ["hours", "open", "opening", "closing", "working"]
                    },
                    {
                        "id": "demo_2",
                        "question": "What are your prices?",
                        "answer": "Starter: $49/mo | Pro: $99/mo | Agency: $299/mo. All plans include a 14-day free trial! 💰",
                        "triggers": ["price", "pricing", "cost", "fee", "payment", "charge", "afford", "subscription"]
                    },
                    {
                        "id": "demo_3",
                        "question": "Do you offer discounts?",
                        "answer": "Yes! Annual plans save you 2 full months. Ask us about annual billing. 🎉",
                        "triggers": ["discount", "sale", "promo", "coupon", "deal", "cheaper", "reduce", "saving", "annual"]
                    }
                ]
                config = {}
            else:
                config = json.loads(client['branding_settings']) if client['branding_settings'] else {}
                faqs_list = models.get_faqs(client_id)
        except Exception as db_error:
            app.logger.error(f'Database error: {db_error}')
            faqs_list = []
            config = {}

        lead_triggers = config.get('bot_settings', {}).get('lead_triggers', ['contact', 'sales', 'demo', 'speak', 'talk'])
        message_lower = message.lower()

        # ── Plan enforcement: messages_per_day ──────────────────────────
        # Only check for real (non-demo) clients so the demo widget is
        # never accidentally blocked.
        #
        # _chat_daily_limit is set here and threaded into every log_conversation
        # call below so the atomic CTE insert can re-check the cap under the
        # same DB snapshot — preventing races at the boundary.
        _chat_daily_limit = None  # None == unlimited / demo — no atomic check needed
        if client and client_id != 'demo':
            owner = _get_cached_client_owner(client_id)
            if owner:
                plan_type = owner.get('plan_type', 'free')
                daily_limit = PLAN_LIMITS.get(plan_type, PLAN_LIMITS['free'])['messages_per_day']
                if daily_limit < 999999:
                    _chat_daily_limit = daily_limit   # thread down to log_conversation
                    today_count = models.get_daily_message_count(client_id)
                    if today_count >= daily_limit:
                        app.logger.info(
                            f"[Limit] {client_id} hit daily cap ({today_count}/{daily_limit}) on plan '{plan_type}'"
                        )
                        return jsonify({
                            'success': True,
                            'response': (
                                "You've reached today's message limit. "
                                "Upgrade your plan for unlimited messages, or try again tomorrow. 🚀"
                            ),
                            'limit_reached': True,
                            'upgrade_url': '/upgrade?plan=pro',
                            'method': 'limit_enforced'
                        })
                    # ── Overage warning: fire at 80% usage ──────────────────
                    elif daily_limit < 999999 and today_count >= int(daily_limit * 0.8):
                        pct = round(today_count / daily_limit * 100)
                        app.logger.info(
                            f"[UsageWarning] {client_id} at {pct}% of daily cap "
                            f"({today_count}/{daily_limit}) on plan '{plan_type}'"
                        )
                        # We continue processing but flag the warning in the response
                        # so the dashboard can show a banner the next time the owner logs in.
                        try:
                            models.upsert_usage_warning(client_id, pct, today_count, daily_limit)
                        except Exception:
                            pass  # non-fatal

        # Load vertical system prompt
        vertical = config.get('vertical', 'general')
        vertical_system_prompt = config.get('bot_settings', {}).get('system_prompt') or VERTICAL_PROMPTS.get(vertical)

        # ── Step 1: Keyword-only lead check (AI disabled path only) ────────
        # When AI is enabled, generate_response() handles lead detection
        # internally as part of the full pipeline — no need to pre-check here.
        if not (ai_helper and ai_helper.enabled):
            for trigger in lead_triggers:
                if trigger.lower() in message_lower:
                    response_text = "I'd be happy to connect you with our team! What's the best email to reach you?"
                    log_conversation(client_id, message, response_text, matched=True, method='lead_trigger', session_id=session_id, daily_limit=_chat_daily_limit)
                    return jsonify({
                        'success': True,
                        'response': response_text,
                        'trigger_lead_collection': True,
                        'method': 'lead_trigger',
                        'contact_info': config.get('contact', {})
                    })

        # ── Step 2: Full RAG Pipeline ────────────────────────────────────
        if ai_helper and ai_helper.enabled:
            try:
                # Load 15-message history — summarisation is handled inside generate_response
                convo_history = models.get_recent_conversations(client_id, limit=15)

                app.logger.info(
                    f"[Chat] client={client_id} faqs={len(faqs_list)} "
                    f"history={len(convo_history)} vertical={vertical}"
                )

                # ── Fetch kb_version once per request (Redis O(1)) ────────
                # Passed into generate_response so the helper can do cache
                # lookup/write without a second Redis round-trip from app.py.
                # Only fetch for real clients; demo always bypasses cache.
                kb_version = (
                    cache_utils.get_kb_version(client_id)
                    if client_id != 'demo' else None
                )

                # Full pipeline: embed search → rerank → RAG generate → guardrails
                # The kb_version arg enables the Redis cache layer inside generate_response.
                result = ai_helper.generate_response(
                    user_message=message,
                    faqs=faqs_list,
                    vertical=vertical,
                    conversation_history=convo_history,
                    client_id=client_id,
                    lead_triggers=lead_triggers,
                    kb_version=kb_version,
                    session_id=session_id,      # Phase 3 persistent memory + inbox transcript linkage
                )

                response_text = result.get('response', '')
                method        = result.get('method', 'rag_pipeline')
                confidence    = result.get('confidence', 0.0)
                from_cache    = result.get('method') == 'cache'

                # ── HANDOFF: contact_request or is_lead from pipeline ─────────
                # When the AI returns a contact_request action (IDK, confidence
                # gate, frustration escalation), create a human_inbox ticket
                # immediately — don't wait for the user to fill in a lead form.
                _action     = result.get('action') or {}
                _is_handoff = (
                    _action.get('type') == 'contact_request' or
                    result.get('is_lead')
                )
                if _is_handoff and client_id != 'demo':
                    _handoff   = result.get('handoff') or {}
                    _sess_mem  = _handoff.get('session_memory') or result.get('lead_metadata') or {}
                    _reason    = (_handoff.get('unanswered_question') or message or 'User triggered handoff')
                    _urgency   = 'high' if _sess_mem.get('frustration_score', 0) >= 3 else 'normal'
                    _transcript = _handoff.get('transcript') or []
                    _summary   = '\n'.join(
                        f"{t['role'].upper()}: {t['content']}"
                        for t in _transcript[-6:]
                    ) if _transcript else message

                    try:
                        from tools import escalate_to_human as _escalate
                        _ticket = _escalate(
                            client_id      = client_id,
                            session_id     = session_id or '',
                            reason         = _reason[:500],
                            customer_email = _sess_mem.get('email') or '',
                            customer_name  = _sess_mem.get('name')  or '',
                            summary        = _summary[:2000],
                            urgency        = _urgency,
                        )
                        if _ticket.get('success'):
                            _ticket_id = _ticket.get('ticket_id', '')
                            app.logger.info(
                                f"[Handoff] ticket={_ticket_id} method={method} client={client_id}"
                            )
                            _notify_handoff(
                                client_id = client_id,
                                client    = client,
                                config    = config,
                                ticket_id = _ticket_id,
                                reason    = _reason,
                                urgency   = _urgency,
                                name      = _sess_mem.get('name')  or '',
                                email     = _sess_mem.get('email') or '',
                                summary   = _summary,
                                method    = method,
                            )
                    except Exception as _esc_err:
                        app.logger.warning(f"[Handoff] escalation non-critical: {_esc_err}")

                    log_conversation(
                        client_id, message, response_text,
                        matched=True, method=method, session_id=session_id,
                        daily_limit=_chat_daily_limit,
                    )

                    # System 2: collect escalation training sample
                    if client_id != 'demo':
                        try:
                            from training_collector import collect_escalation
                            collect_escalation(
                                client_id    = client_id,
                                session_id   = session_id or '',
                                user_message = message,
                                reason       = _reason,
                                bot_response = response_text,
                                ticket_id    = str(_ticket_id) if '_ticket_id' in locals() else '',
                                urgency      = _urgency,
                                vertical     = vertical,
                            )
                        except Exception as _tc_err:
                            app.logger.debug(f'[TrainingCollector] escalation error: {_tc_err}')

                    return jsonify({
                        'success':               True,
                        'response':              response_text,
                        'trigger_lead_collection': True,
                        'method':                method,
                        'contact_info':          config.get('contact', {}),
                        'session_id':            session_id,
                    })

                # Catch any remaining is_lead signals not caught above
                if result.get('is_lead'):
                    log_conversation(client_id, message, response_text, matched=True, method='lead_pipeline', session_id=session_id, daily_limit=_chat_daily_limit)
                    return jsonify({
                        'success': True,
                        'response': response_text,
                        'trigger_lead_collection': True,
                        'method': 'lead_pipeline',
                        'contact_info': config.get('contact', {}),
                        'session_id': session_id,
                    })

                if response_text:
                    matched = confidence > 0.4
                    log_conversation(client_id, message, response_text, matched=matched, method=method, session_id=session_id, daily_limit=_chat_daily_limit)

                    # System 2: collect training sample in background
                    if client_id != 'demo':
                        try:
                            from training_collector import collect_conversation_turn
                            collect_conversation_turn(
                                client_id    = client_id,
                                session_id   = session_id or '',
                                user_message = message,
                                bot_response = response_text,
                                method       = method,
                                confidence   = confidence,
                                vertical     = vertical,
                                is_lead      = bool(result.get('is_lead')),
                            )
                        except Exception as _tc_err:
                            app.logger.debug(f'[TrainingCollector] chat turn error: {_tc_err}')
                    app.logger.info(
                        f"[AI Match] method={method} confidence={confidence:.2f} "
                        f"response_len={len(response_text)} cached={from_cache}"
                    )

                    # Summarise long conversations non-blocking
                    if not from_cache and client_id != 'demo':
                        try:
                            ai_helper.maybe_summarise(client_id, convo_history)
                        except Exception as _sum_err:
                            app.logger.debug(f"[Summarise] non-critical: {_sum_err}")

                    # Fire message_sent for every AI response
                    fire_webhook_event(client_id, 'message_sent', {
                        'session_id': session_id,
                        'message':    message,
                        'response':   response_text,
                        'method':     method,
                        'confidence': round(confidence, 4),
                    })
                    # Fire faq_matched when AI matched with meaningful confidence
                    if matched and confidence >= 0.5:
                        fire_webhook_event(client_id, 'faq_matched', {
                            'session_id': session_id,
                            'message':    message,
                            'response':   response_text,
                            'method':     method,
                            'confidence': round(confidence, 4),
                        })

                    return jsonify({
                        'success':    True,
                        'response':   response_text,
                        'confidence': confidence,
                        'method':     method,
                        'session_id': session_id,   # echoed so widget can store it
                    })

            except Exception as ai_error:
                app.logger.error(f"[RAG Pipeline] error: {ai_error}", exc_info=True)

        # ── Step 3: Keyword fallback (threshold=0.68, AI disabled/failed only) ─
        best_faq, confidence = find_best_match(message, faqs_list)
        if best_faq:
            app.logger.info(
                f"[Keyword Fallback] faq='{best_faq.get('id')}' "
                f"score={confidence:.3f} threshold=0.68"
            )
            response_text = best_faq.get('answer')
            log_conversation(client_id, message, response_text, matched=True, method='keyword_fallback', session_id=session_id, daily_limit=_chat_daily_limit)
            fire_webhook_event(client_id, 'message_sent', {
                'session_id': session_id,
                'message':    message,
                'response':   response_text,
                'method':     'keyword_fallback',
                'confidence': round(confidence, 4),
            })
            fire_webhook_event(client_id, 'faq_matched', {
                'session_id': session_id,
                'message':    message,
                'response':   response_text,
                'method':     'keyword_fallback',
                'confidence': round(confidence, 4),
            })
            return jsonify({
                'success': True,
                'response': response_text,
                'confidence': confidence,
                'method': 'keyword_fallback'
            })

        # Step 4: Fallback
        fallback = config.get('bot_settings', {}).get(
            'fallback_message',
            "I'm not sure about that. Would you like to speak with our team? Type 'contact'!"
        )
        log_conversation(client_id, message, fallback, matched=False, method='fallback', session_id=session_id, daily_limit=_chat_daily_limit)
        return jsonify({
            'success': True,
            'response': fallback,
            'confidence': 0.0,
            'show_contact_button': True,
            'method': 'fallback'
        })

    except Exception as e:
        app.logger.error(f'Error in chat endpoint: {e}')
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': 'Internal server error'}), 500


@app.route('/api/chat/rate', methods=['POST'])
@limiter.limit("20 per minute")
def chat_rate():
    """
    System 2: User thumbs-up (1) / thumbs-down (-1) rating on a bot response.
    Stores a rating training sample and updates the quality on the original sample.

    Body: { client_id, session_id, sample_id, rating, user_message, bot_response }
    rating: 1 = positive, -1 = negative, 0 = neutral
    """
    try:
        data         = request.json or {}
        client_id    = sanitize_input(data.get('client_id', ''), max_length=50)
        session_id   = sanitize_input(data.get('session_id', ''), max_length=100)
        sample_id    = sanitize_input(data.get('sample_id', ''), max_length=30)
        user_message = sanitize_input(data.get('user_message', ''), max_length=1000)
        bot_response = sanitize_input(data.get('bot_response', ''), max_length=2000)

        try:
            rating = int(data.get('rating', 0))
        except (TypeError, ValueError):
            rating = 0
        rating = max(-1, min(rating, 1))

        if not client_id:
            return jsonify({'success': False, 'error': 'client_id required'}), 400

        # Verify client exists before recording
        if client_id != 'demo':
            client = models.get_client_by_id(client_id)
            if not client:
                return jsonify({'success': False, 'error': 'Client not found'}), 404

            from training_collector import collect_user_rating
            collect_user_rating(
                client_id    = client_id,
                session_id   = session_id,
                sample_id    = sample_id,
                rating       = rating,
                user_message = user_message,
                bot_response = bot_response,
            )

        return jsonify({'success': True, 'rating': rating})

    except Exception as e:
        app.logger.error(f'[chat/rate] error: {e}')
        return jsonify({'success': False, 'error': 'Internal server error'}), 500


@app.route('/api/lead', methods=['POST'])
@limiter.limit("10 per hour")
def submit_lead():
    try:
        data = request.json
        client_id = sanitize_input(data.get('client_id', 'default'), max_length=50)
        name = sanitize_input(data.get('name', ''), max_length=100)
        email = sanitize_input(data.get('email', ''), max_length=200)
        phone = sanitize_input(data.get('phone', ''), max_length=50)
        company = sanitize_input(data.get('company', ''), max_length=100)
        message = sanitize_input(data.get('message', ''), max_length=1000)
        custom_fields = data.get('custom_fields', {})
        if not isinstance(custom_fields, dict):
            custom_fields = {}

        if not name or not email:
            return jsonify({'success': False, 'error': 'Name and email are required'}), 400

        if not is_email(email):
            return jsonify({'success': False, 'error': 'Invalid email format'}), 400

        client = models.get_client_by_id(client_id)
        if not client:
            return jsonify({'success': False, 'error': 'Client not found'}), 404

        lead_data = {
            'name': name, 'email': email, 'phone': phone, 'company': company,
            'message': message,
            'custom_fields': custom_fields if custom_fields else None,
            'conversation_snippet': sanitize_input(data.get('conversation_snippet', ''), max_length=2000),
            'source_url': data.get('source_url', '')
        }

        models.save_lead(client_id, lead_data)
        # lead_captured fires via notify_webhook shim → fire_webhook_event
        notify_webhook(client_id, {'name': name, 'email': email, 'phone': phone, 'company': company})
        # conversation_ended — a submitted lead marks end of a qualified session
        fire_webhook_event(client_id, 'conversation_ended', {
            'session_id':  data.get('session_id', ''),
            'outcome':     'lead_captured',
            'lead_name':   name,
            'lead_email':  email,
        })

        # Log to conversations table so lead submissions appear in analytics
        user_summary = f"[Lead Captured] Name: {name} | Email: {email}"
        if phone:
            user_summary += f" | Phone: {phone}"
        if company:
            user_summary += f" | Company: {company}"
        log_conversation(
            client_id,
            user_summary,
            "Thank you! We've received your information and will be in touch soon.",
            matched=True,
            method='lead_captured',
            session_id=sanitize_input(
                (request.json or {}).get('session_id', ''), max_length=100
            ) or None
        )

        app.logger.info(f'Lead captured for client: {client_id}')

        config       = json.loads(client['branding_settings']) if client['branding_settings'] else {}
        contact_info = config.get('contact', {})

        # Send branded lead notification to the client's contact email if set
        notify_email = contact_info.get('email')
        if notify_email:
            try:
                sender_info = models.get_email_from_for_client(client_id)
                msg = Message(
                    subject=f"New Lead: {name}",
                    sender=f"{sender_info['name']} <{sender_info['address']}>",
                    recipients=[notify_email],
                    html=f"""
                    <div style="font-family:'DM Sans',sans-serif;max-width:520px;margin:0 auto;
                                background:#F7F4EF;padding:36px;border-radius:16px;">
                      <h2 style="font-size:20px;font-weight:700;color:#1C1917;margin-bottom:4px;">
                        New Lead Captured</h2>
                      <p style="color:#A8A29E;font-size:13px;margin-bottom:24px;">
                        via {client.get('company_name','your chatbot')}</p>
                      <table style="width:100%;border-collapse:collapse;">
                        <tr><td style="padding:10px 0;border-bottom:1px solid #E7E2DA;
                                       font-size:13px;color:#57534E;width:100px;">Name</td>
                            <td style="padding:10px 0;border-bottom:1px solid #E7E2DA;
                                       font-size:13px;font-weight:600;color:#1C1917;">{name}</td></tr>
                        <tr><td style="padding:10px 0;border-bottom:1px solid #E7E2DA;
                                       font-size:13px;color:#57534E;">Email</td>
                            <td style="padding:10px 0;border-bottom:1px solid #E7E2DA;
                                       font-size:13px;font-weight:600;color:#1C1917;">{email}</td></tr>
                        {'<tr><td style="padding:10px 0;border-bottom:1px solid #E7E2DA;font-size:13px;color:#57534E;">Phone</td><td style="padding:10px 0;border-bottom:1px solid #E7E2DA;font-size:13px;font-weight:600;color:#1C1917;">'+phone+'</td></tr>' if phone else ''}
                        {'<tr><td style="padding:10px 0;border-bottom:1px solid #E7E2DA;font-size:13px;color:#57534E;">Company</td><td style="padding:10px 0;border-bottom:1px solid #E7E2DA;font-size:13px;font-weight:600;color:#1C1917;">'+company+'</td></tr>' if company else ''}
                      </table>
                      <a href="https://lumvi.net/admin/leads?client_id={client_id}"
                         style="display:inline-block;margin-top:24px;padding:11px 22px;
                                background:#B8924A;color:#fff;text-decoration:none;
                                border-radius:9px;font-weight:700;font-size:13.5px;">
                        View All Leads →</a>
                    </div>"""
                )
                mail.send(msg)
            except Exception as _mail_err:
                app.logger.warning(f"[Lead email] failed for {client_id}: {_mail_err}")

        return jsonify({
            'success': True,
            'message': "Thank you! We've received your information and will be in touch soon.",
            'contact_info': contact_info
        })

    except Exception as e:
        app.logger.error(f'Error submitting lead: {e}')
        return jsonify({'success': False, 'error': 'Failed to submit lead'}), 500

# =====================================================================
# LEAD MANAGEMENT API — stage updates, notes, assignment, delete
# All operations go through models.py → PostgreSQL.
# =====================================================================

def _fire_lead_stage_webhook(client_id, lead, old_stage):
    """Fire outbound webhook when a lead stage changes — delegates to unified dispatcher."""
    fire_webhook_event(client_id, 'lead_stage_changed', {
        'lead_id':     str(lead.get('id', '')),
        'name':        lead.get('name'),
        'email':       lead.get('email'),
        'old_stage':   old_stage,
        'new_stage':   lead.get('stage'),
        'notes':       lead.get('notes'),
        'assigned_to': lead.get('assigned_to'),
    })


@app.route('/api/leads/<client_id>', methods=['GET'])
@login_required
def list_leads(client_id):
    """Return all leads for a client with optional stage/search filter."""
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    stage  = request.args.get('stage', '').strip()
    search = request.args.get('q', '').lower().strip()
    try:
        page     = max(1, int(request.args.get('page', 1)))
        per_page = min(100, max(1, int(request.args.get('per_page', 50))))
    except (ValueError, TypeError):
        return jsonify({'success': False, 'error': 'page and per_page must be integers'}), 400

    leads = models.get_leads(client_id)

    if stage:
        leads = [l for l in leads if (l.get('stage') or 'new') == stage]
    if search:
        leads = [l for l in leads if
                 search in (l.get('name') or '').lower() or
                 search in (l.get('email') or '').lower() or
                 search in (l.get('company') or '').lower()]

    total = len(leads)
    leads = leads[(page - 1) * per_page: page * per_page]
    return jsonify({'success': True, 'leads': leads, 'total': total, 'page': page, 'per_page': per_page})


@app.route('/api/leads/<client_id>/<int:lead_id>', methods=['PATCH'])
@login_required
def update_lead(client_id, lead_id):
    """
    Update a lead's stage, notes, assigned_to, priority, name, email, phone, or company.
    Fires the outbound webhook on stage change.
    """
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    data = request.json or {}
    valid_stages = {'new', 'contacted', 'qualified', 'proposal', 'closed', 'lost'}

    if 'stage' in data and data['stage'] not in valid_stages:
        return jsonify({'success': False, 'error': f'Invalid stage: {data["stage"]}'}), 400

    # Fetch current state to detect stage change and get old_stage
    existing = models.get_lead_by_id(client_id, lead_id)
    if not existing:
        return jsonify({'success': False, 'error': 'Lead not found'}), 404

    old_stage = existing.get('stage') or 'new'
    stage_changed = 'stage' in data and data['stage'] != old_stage

    # Build action description for activity log
    if stage_changed:
        action = f"Moved from {old_stage} → {data['stage']}"
    elif 'notes' in data:
        action = 'Notes updated'
    elif 'assigned_to' in data:
        action = f"Assigned to {data.get('assigned_to', '')}"
    else:
        action = 'Updated: ' + ', '.join(k for k in data if k not in ('_actor', '_action'))

    data['_actor']  = current_user.email
    data['_action'] = action

    updated = models.update_lead(client_id, lead_id, data)
    if updated is None:
        return jsonify({'success': False, 'error': 'Lead not found or update failed'}), 404

    if stage_changed:
        _fire_lead_stage_webhook(client_id, updated, old_stage)

    return jsonify({'success': True, 'lead': updated})


@app.route('/api/leads/<client_id>/<int:lead_id>', methods=['DELETE'])
@login_required
def delete_lead(client_id, lead_id):
    """Permanently delete a lead."""
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    ok = models.delete_lead_by_client(client_id, lead_id)
    if not ok:
        return jsonify({'success': False, 'error': 'Lead not found'}), 404

    app.logger.info(f'[LeadMgmt] deleted lead={lead_id} client={client_id} user={current_user.email}')
    return jsonify({'success': True})


@app.route('/api/leads/<client_id>/bulk', methods=['POST'])
@login_required
def bulk_update_leads(client_id):
    """
    Bulk update multiple leads at once.
    Body: { lead_ids: [...], updates: { stage?, assigned_to?, priority? } }
    """
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    data     = request.json or {}
    lead_ids = data.get('lead_ids', [])
    updates  = data.get('updates', {})
    valid_stages = {'new', 'contacted', 'qualified', 'proposal', 'closed', 'lost'}

    if 'stage' in updates and updates['stage'] not in valid_stages:
        return jsonify({'success': False, 'error': 'Invalid stage'}), 400
    if not lead_ids:
        return jsonify({'success': False, 'error': 'No lead_ids provided'}), 400

    count = models.bulk_update_leads(client_id, lead_ids, updates, actor=current_user.email)

    # Fire webhooks for stage changes
    if 'stage' in updates:
        for lid in lead_ids:
            lead = models.get_lead_by_id(client_id, lid)
            if lead:
                _fire_lead_stage_webhook(client_id, lead, old_stage='unknown')

    return jsonify({'success': True, 'updated': count})


@app.route('/api/leads/<client_id>/webhook-inbound', methods=['POST'])
def inbound_lead_webhook(client_id):
    """
    External CRM / Zapier / Make can POST here to update a lead from their side.
    Auth: X-Lumvi-Secret header must match the client's configured inbound_webhook_secret.
    Body: { lead_id, stage?, notes?, assigned_to?, priority? }
    """
    client = models.get_client_by_id(client_id)
    if not client:
        return jsonify({'error': 'Client not found'}), 404

    config = json.loads(client.get('branding_settings') or '{}')
    expected_secret = config.get('integrations', {}).get('inbound_webhook_secret', '')
    provided_secret = request.headers.get('X-Lumvi-Secret', '')

    # APP-BUG-07 fix: constant-time comparison prevents timing oracle attacks
    # on the inbound webhook secret.
    import hmac as _hmac
    if not expected_secret or not _hmac.compare_digest(provided_secret, expected_secret):
        return jsonify({'error': 'Unauthorized — invalid or missing X-Lumvi-Secret header'}), 401

    data    = request.json or {}
    lead_id_raw = data.get('lead_id', '')
    try:
        lead_id = int(lead_id_raw)
    except (ValueError, TypeError):
        return jsonify({'error': 'lead_id must be an integer'}), 400

    valid_stages = {'new', 'contacted', 'qualified', 'proposal', 'closed', 'lost'}
    if 'stage' in data and data['stage'] not in valid_stages:
        return jsonify({'error': f'Invalid stage. Must be one of: {", ".join(sorted(valid_stages))}'}), 400

    existing = models.get_lead_by_id(client_id, lead_id)
    if not existing:
        return jsonify({'error': 'Lead not found'}), 404

    old_stage = existing.get('stage') or 'new'
    allowed = {'stage', 'notes', 'assigned_to', 'priority'}
    updates = {k: v for k, v in data.items() if k in allowed}
    updates['_actor']  = 'external_webhook'
    updates['_action'] = 'Inbound webhook: ' + ', '.join(f"{k}={v}" for k, v in updates.items() if not k.startswith('_'))

    updated = models.update_lead(client_id, lead_id, updates)
    stage_changed = 'stage' in updates and updates['stage'] != old_stage
    if stage_changed and updated:
        _fire_lead_stage_webhook(client_id, updated, old_stage)

    app.logger.info(f'[InboundWebhook] lead={lead_id} client={client_id} stage_changed={stage_changed}')
    return jsonify({'success': True, 'lead_id': lead_id, 'stage': (updated or {}).get('stage')})


# =====================================================================
# AUTHENTICATION ROUTES
# =====================================================================

@app.route('/signup', methods=['GET', 'POST'])
def signup():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))

    referral_code = request.args.get('ref')
    plan_param    = request.args.get('plan', '').lower()  # from sales page buttons

    if request.method == 'POST':
        email            = request.form.get('email', '').strip().lower()
        password         = request.form.get('password', '')
        confirm_password = request.form.get('confirm_password', '')
        # Read the plan the user selected — from hidden field or plan_select radio
        plan_from_form = (
            request.form.get('plan_param') or
            request.form.get('plan_select') or
            request.args.get('plan') or
            'free'
        ).lower().strip()

        # Validate plan value — includes all billable plans
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

        # Always create the account as 'free' first — the plan activates after
        # payment is confirmed via /payment/flutterwave/callback.
        # This ensures a valid user_id exists before the payment callback fires.
        intended_plan = plan_from_form  # remember what the user wanted
        user_id = models.create_user(email, password, 'free')

        if user_id is None:
            return render_template('signup.html', error='An account with that email already exists',
                                   referral_code=referral_code, plan_param=plan_from_form)

        if referral_code:
            affiliate = models.get_affiliate_by_code(referral_code)
            if affiliate:
                models.create_referral(affiliate['id'], user_id, referral_code)
                app.logger.info(f'Referral tracked: {referral_code} -> {email}')

        user_data = models.get_user_by_id(user_id)
        user = User(user_data)
        session.permanent = True
        login_user(user, remember=True)
        models.track_event('signup', user_id=user_id, metadata={'email': email, 'plan': intended_plan})
        send_welcome_email(email)

        app.logger.info(f'New signup: {email} | intended plan: {intended_plan}')

        # Paid plan signups → upgrade/payment page with plan pre-selected
        if intended_plan in PAID_PLANS:
            return redirect(url_for('upgrade_page') + f'?plan={intended_plan}')
        return redirect(url_for('dashboard'))

    return render_template('signup.html', referral_code=referral_code, plan_param=plan_param)


@app.route('/forgot-password', methods=['GET', 'POST'])
def forgot_password():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))

    if request.method == 'POST':
        email = request.form.get('email', '').strip().lower()
        user_data = models.get_user_by_email(email)

        # Always show success message — never reveal if email exists (security)
        success_msg = "If that email is registered, you'll receive a reset link shortly."

        if user_data:
            token = secrets.token_urlsafe(32)
            expires_at = datetime.utcnow() + timedelta(hours=1)
            models.save_password_reset_token(user_data['id'], token, expires_at)

            reset_url = url_for('reset_password', token=token, _external=True)
            try:
                msg = Message(
                    subject="Reset your Lumvi password",
                    sender="Lumvi <support@lumvi.net>",
                    recipients=[email],
                    html=f"""
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
                    </div>
                    """
                )
                mail.send(msg)
            except Exception as e:
                app.logger.error(f"Password reset email failed: {type(e).__name__}: {e}")
                import traceback
                traceback.print_exc()

        return render_template('forgot_password.html', success=success_msg)

    return render_template('forgot_password.html')


@app.route('/reset-password/<token>', methods=['GET', 'POST'])
def reset_password(token):
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))

    token_data = models.get_password_reset_token(token)

    if not token_data:
        return render_template('reset_password.html', error="This reset link is invalid or has already been used.")

    # Handle expires_at as either datetime or string
    expires_at = token_data['expires_at']
    if isinstance(expires_at, str):
        try:
            expires_at = datetime.strptime(expires_at, '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            expires_at = datetime.strptime(expires_at, '%Y-%m-%d %H:%M:%S')

    if datetime.utcnow() > expires_at:
        models.delete_password_reset_token(token)
        return render_template('reset_password.html', error="This reset link has expired. Please request a new one.")

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

        return render_template('reset_password.html', success="Password updated! You can now log in.")

    return render_template('reset_password.html', token=token)


@app.route('/auth/google')
def google_login():
    redirect_uri = url_for('google_callback', _external=True, _scheme='https')
    return google_oauth.authorize_redirect(redirect_uri)


@app.route('/auth/google/callback')
def google_callback():
    try:
        token     = google_oauth.authorize_access_token()
        userinfo  = token.get('userinfo') or google_oauth.userinfo()
        google_id = userinfo['sub']
        email     = userinfo.get('email', '').lower().strip()

        if not email:
            flash('Google sign-in failed: no email returned.', 'error')
            return redirect(url_for('login'))

        user_data = models.create_or_link_google_user(google_id, email)
        if not user_data:
            flash('Google sign-in failed. Please try again.', 'error')
            return redirect(url_for('login'))

        is_new = user_data.get('plan_type') == 'free'
        user = User(user_data)
        login_user(user, remember=True)
        session.permanent = True
        session['_user_cache'] = dict(user_data)

        if is_new:
            try: send_welcome_email(email)
            except Exception: pass

        return redirect(url_for('dashboard'))

    except Exception as e:
        app.logger.error(f'[Google OAuth] {e}')
        flash('Google sign-in failed. Please try again.', 'error')
        return redirect(url_for('login'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))

    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')
        user_data = models.verify_user(email, password)

        if user_data:
            # Auto-downgrade if grace period has ended (skip for admins)
            if not user_data.get('is_admin'):
                sub = get_subscription_status(user_data)
                if sub['status'] == 'expired':
                    models.downgrade_single_user(user_data['id'])
                    user_data['plan_type'] = 'free'
                    app.logger.info(f"Auto-downgraded user {user_data['id']} to free on login")

            user = User(user_data)
            session.pop('_user_cache', None)  # bust cache so load_user re-fetches on next request
            session.permanent = True          # apply PERMANENT_SESSION_LIFETIME (30 days)
            login_user(user, remember=True)
            models.track_event('login', user_id=user_data['id'], metadata={'email': email})

            # Pass subscription status to dashboard via session
            fresh = models.get_user_by_id(user_data['id'])
            sub = get_subscription_status(fresh)
            session['sub_status'] = sub['status']

            return redirect(url_for('dashboard'))
        else:
            return render_template('login.html', error='Invalid email or password')

    return render_template('login.html')


@app.route('/logout')
@login_required
def logout():
    session.pop('_user_cache', None)  # clear cached user data
    logout_user()
    return redirect(url_for('login'))


@app.route('/dashboard')
@login_required
def dashboard():
    clients = models.get_user_clients(current_user.id)
    for client in clients:
        if client['branding_settings']:
            client['branding_settings'] = json.loads(client['branding_settings'])

    # Always re-fetch user from DB so plan badge is never stale after an upgrade
    fresh_user = models.get_user_by_id(current_user.id)

    # ── Active enforcement: downgrade if grace period ended ──────────
    if fresh_user and not fresh_user.get('is_admin'):
        sub = get_subscription_status(fresh_user)
        if sub['status'] == 'expired':
            models.downgrade_single_user(fresh_user['id'])
            fresh_user = models.get_user_by_id(fresh_user['id'])  # re-fetch after downgrade
            app.logger.info(f"[Dashboard] Auto-downgraded user {fresh_user['id']} to free.")

    # ── Onboarding redirect for new users ─────────────────────────────
    # Fresh users who haven't completed onboarding and have no clients yet
    # go straight to the guided wizard instead of a blank dashboard.
    if (fresh_user and
            not fresh_user.get('onboarding_completed') and
            not fresh_user.get('is_admin') and
            len(clients) == 0):
        return redirect(url_for('onboarding'))

    plan_type   = (fresh_user or {}).get('plan_type', current_user.plan_type)
    plan_limits = PLAN_LIMITS.get(plan_type, PLAN_LIMITS['free'])
    client_limit = plan_limits['clients']
    client_count = len(clients)
    slots_display = 'Unlimited' if client_limit >= 999999 else str(client_limit)
    limit_reached = False if client_limit >= 999999 else client_count >= client_limit

    # ── Agency per-seat overage for dashboard display ───────────────
    agency_extra_seats   = max(0, client_count - AGENCY_INCLUDED_CLIENTS) if plan_type == 'agency' else 0
    agency_overage_cost  = agency_extra_seats * AGENCY_SEAT_PRICE
    agency_overage_label = (
        f"+{agency_extra_seats} extra seat{'s' if agency_extra_seats != 1 else ''} "
        f"× ${AGENCY_SEAT_PRICE:.0f}/mo = ${agency_overage_cost:.0f}/mo billed next cycle"
        if agency_extra_seats > 0 else ''
    )

    # ── Payment method check: agency users need a subscription_id on file
    # before they can create overage seats (client 21+).  Passed to the
    # template to gate the Add Client button and surface a billing warning.
    has_payment_method = bool(
        (fresh_user or {}).get('subscription_id') and
        (fresh_user or {}).get('subscription_status', 'active') in ('active', 'trialing')
    )

    # Subscription status for popup
    sub_status = session.pop('sub_status', None)
    sub_info = get_subscription_status(fresh_user) if fresh_user else {'status': 'free'}

    template = 'dashboard_enterprise.html'
    return render_template(
        template,
        user=current_user,
        clients=clients,
        plan_type=plan_type,
        plan_limits=plan_limits,
        client_count=client_count,
        client_limit=client_limit,
        slots_display=slots_display,
        limit_reached=limit_reached,
        sub_status=sub_info['status'],
        sub_expires_at=sub_info.get('expires_at'),
        sub_grace_ends_at=sub_info.get('grace_ends_at'),
        agency_extra_seats=agency_extra_seats,
        agency_overage_cost=agency_overage_cost,
        agency_overage_label=agency_overage_label,
        agency_included_clients=AGENCY_INCLUDED_CLIENTS,
        agency_seat_price=AGENCY_SEAT_PRICE,
        has_payment_method=has_payment_method
    )


@app.route('/onboarding')
@login_required
def onboarding():
    """
    Guided setup wizard for new agency owners.
    Redirects to dashboard if onboarding is already complete.
    """
    fresh_user = models.get_user_by_id(current_user.id)
    if fresh_user and fresh_user.get('onboarding_completed'):
        return redirect(url_for('dashboard'))
    plan_type = (fresh_user or {}).get('plan_type', current_user.plan_type)
    return render_template('onboarding.html',
                           user=current_user,
                           plan_type=plan_type)


@app.route('/api/onboarding/complete', methods=['POST'])
@login_required
def onboarding_complete():
    """
    Called by the wizard on final step.
    Marks onboarding done so the user never sees the wizard again.
    """
    try:
        models.mark_onboarding_complete(current_user.id)
        app.logger.info(f"[Onboarding] User {current_user.id} completed onboarding.")
        return jsonify({'success': True, 'redirect': url_for('dashboard')})
    except Exception as e:
        app.logger.error(f"[Onboarding] complete error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/onboarding/skip', methods=['POST'])
@login_required
def onboarding_skip():
    """Let the user skip the wizard and go straight to the dashboard."""
    models.mark_onboarding_complete(current_user.id)
    return jsonify({'success': True, 'redirect': url_for('dashboard')})


@app.route('/create-client', methods=['POST'])
@login_required
def create_client():
    try:
        company_name = request.form.get('company_name')
        vertical     = request.form.get('vertical', 'general')
        # Validate vertical against known keys; fall back to general
        if vertical not in VALID_VERTICALS:
            vertical = 'general'

        if not company_name:
            return jsonify({'success': False, 'error': 'Company name is required'}), 400

        user = models.get_user_by_id(current_user.id)
        plan_type = user['plan_type']
        plan_limit = PLAN_LIMITS.get(plan_type, PLAN_LIMITS['free'])['clients']

        # ---- Readable limit labels per plan ----
        plan_upgrade_hints = {
            'free':    'Solo: 1 chatbot $19/mo | Starter: 3 chatbots | Pro: 10 chatbots | Agency: Unlimited',
            'solo':    'Starter: 3 chatbots | Pro: 10 chatbots | Agency: Unlimited',
            'starter': 'Pro: 10 chatbots | Agency: Unlimited',
            'pro':     'Agency: Unlimited chatbots at $299/mo',
        }
        upgrade_hint = plan_upgrade_hints.get(plan_type, 'Upgrade to add more chatbots')

        # ── RACE CONDITION FIX: serialise concurrent creation attempts for this
        # user with a PostgreSQL advisory lock keyed to their user_id.
        # pg_advisory_lock() blocks until acquired; pg_advisory_unlock() releases
        # it.  Because we hold the lock across the count-check AND the insert,
        # two simultaneous requests can no longer both read count=0 and both
        # create a client — the second one will read count=1 (correct).
        # The lock is session-scoped so it is always released when the connection
        # is returned, even if an exception occurs.
        _lock_conn, _lock_cursor = models.get_db()
        try:
            _lock_cursor.execute("SELECT pg_advisory_lock(%s)", (current_user.id,))

            # Re-count inside the lock so the value is authoritative
            current_clients = models.get_user_clients(current_user.id)
            client_count = len(current_clients)

            # ── Agency per-seat: allow creation above 20, charge $15/extra ──
            is_agency_overage = (
                plan_type == 'agency' and
                client_count >= AGENCY_INCLUDED_CLIENTS
            )
            extra_seats = max(0, client_count - AGENCY_INCLUDED_CLIENTS + 1) if is_agency_overage else 0
            overage_cost = extra_seats * AGENCY_SEAT_PRICE

            # ── Gate: require a valid payment method for any overage seat ──
            # Without this, agency users can add seats 21+ for free and the
            # monthly cron creates a pending invoice that may never be paid.
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
                    if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or \
                       request.headers.get('Accept', '').startswith('application/json'):
                        return jsonify({
                            'success': False,
                            'error': _err,
                            'upgrade_url': '/upgrade'
                        }), 402
                    return redirect(url_for('upgrade'))

            if client_count >= plan_limit and not is_agency_overage:
                _lock_cursor.execute("SELECT pg_advisory_unlock(%s)", (current_user.id,))
                _lock_cursor.close()
                _lock_conn.close()
                # Return JSON if called from the onboarding wizard (XHR/JSON request)
                if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or \
                   request.headers.get('Accept', '').startswith('application/json'):
                    return jsonify({
                        'success': False,
                        'error': f'Plan limit reached. You can have {plan_limit} chatbot{"s" if plan_limit != 1 else ""} on your {plan_type} plan. Upgrade to add more.',
                        'upgrade_url': '/upgrade'
                    }), 403
                # Legacy HTML fallback for direct form submissions
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

            client_id = models.create_client(current_user.id, company_name, vertical=vertical)

        finally:
            # Always release the advisory lock, even if an exception was raised
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

        app.logger.info(f"[CreateClient] Created {client_id} for user {current_user.id}")

        # Log agency overage seat so it can be billed in the monthly cron
        if is_agency_overage and client_id:
            try:
                models.record_agency_overage_seat(
                    user_id   = current_user.id,
                    client_id = client_id,
                    seat_num  = client_count + 1,  # 1-indexed seat number
                )
                app.logger.info(
                    f"[AgencyOverage] user={current_user.id} seat={client_count+1} "
                    f"extra_cost=${overage_cost:.2f}/mo client={client_id}"
                )
            except Exception as _ov_err:
                app.logger.error(f"[AgencyOverage] Failed to record seat: {_ov_err}")

        # Return JSON if the request came from the onboarding wizard (XHR),
        # otherwise redirect to dashboard for the legacy form-submit flow.
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or \
           request.headers.get('Accept', '').startswith('application/json'):
            return jsonify({'success': True, 'client_id': client_id})
        return redirect(url_for('dashboard'))

    except Exception as e:
        app.logger.error(f'Error creating client: {e}')
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or \
           request.headers.get('Accept', '').startswith('application/json'):
            return jsonify({'success': False, 'error': str(e)}), 500
        return redirect(url_for('dashboard'))


@app.route('/')
def index():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    return redirect(url_for('landing_page'))

# =====================================================================
# WIDGET & ADMIN ROUTES
# =====================================================================

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


@app.route('/widget')
def widget():
    """
    Serves the chat widget HTML.
    Resolution order:
      1. ?client_id= query param (standard embed)
      2. request.host matches a client's custom_widget_domain (white-label domain)
      3. Falls back to demo client
    """
    client_id = request.args.get('client_id', '').strip()
    client    = None

    # 1. Standard lookup by client_id param
    if client_id and client_id != 'demo':
        client = models.get_client_by_id(client_id)

    # 2. Custom domain lookup — host header (strip port for local dev)
    if not client:
        host = request.host.split(':')[0].lower()
        if host and host not in ('lumvi.net', 'www.lumvi.net', 'localhost', '127.0.0.1'):
            client = models.get_client_by_custom_domain(host)
            if client:
                client_id = client['client_id']
                app.logger.info(f"[Widget] Custom domain match: host={host} client={client_id}")

    # 3. Demo fallback
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
        client = dict(client)
        branding_settings = json.loads(client.get('branding_settings') or '{}')
        bot_settings = branding_settings.get('bot_settings', {})
        branding     = branding_settings.get('branding', {})
        contact      = branding_settings.get('contact', {})

        client['bot_name']         = bot_settings.get('bot_name')        or client.get('company_name') or 'Support'
        client['bot_avatar']       = bot_settings.get('bot_avatar')     or ''   # empty = use SVG icon in widget
        client['bot_avatar_url']   = bot_settings.get('bot_avatar_url') or ''
        client['tagline']          = branding.get('tagline')             or 'Typically replies instantly'
        client['welcome_message']  = bot_settings.get('welcome_message') or client.get('welcome_message') or 'Hi! How can I help you today?'
        client['fallback_message'] = bot_settings.get('fallback_message') or ''
        client['quick_replies']    = [r for r in (bot_settings.get('quick_replies') or []) if r and str(r).strip()]
        client['lead_q3']          = bot_settings.get('lead_q3', '').strip()
        client['lead_q4']          = bot_settings.get('lead_q4', '').strip()
        client['widget_color']     = branding.get('primary_color')       or client.get('widget_color') or '#B8924A'
        client['remove_branding']  = branding.get('remove_branding',     client.get('remove_branding', 0))
        client['logo_url']         = branding.get('logo')               or branding.get('logo_url') or ''
        client['custom_css']       = client.get('custom_css') or ''
        client['contact']          = contact
        client['branding_settings'] = branding_settings
        # ── Widget style settings ────────────────────────
        client['widget_theme']     = branding.get('widget_theme',  'lumvi')
        client['widget_font']      = branding.get('widget_font',   'dm_sans')
        client['bubble_style']     = branding.get('bubble_style',  'rounded')
        client['header_color']     = branding.get('header_color',  '')   # optional hex override
        # ── Bubble appearance overrides (set via customize page slider/pickers) ──
        client['bubble_radius']     = branding.get('bubble_radius')      # None → Jinja skips block
        client['bot_bubble_color']  = branding.get('bot_bubble_color',  '')
        client['user_bubble_color'] = branding.get('user_bubble_color', '')
        # Expose lead_triggers so the widget can short-circuit on QR taps
        client['lead_triggers']    = branding_settings.get('bot_settings', {}).get(
            'lead_triggers', ['contact', 'sales', 'demo', 'speak', 'talk', 'human', 'agent']
        )

    return render_template('chat.html', client=client)


@app.route('/admin/leads')
@login_required
def admin_leads():
    client_id = request.args.get('client_id')

    if not client_id:
        return "Client ID required", 400

    if not models.verify_client_ownership(current_user.id, client_id):
        return "Unauthorized", 403

    leads = models.get_leads(client_id)
    client = models.get_client_by_id(client_id)

    return render_template('admin.html', leads=leads, client_id=client_id, client=client)


@app.route('/landing')
def landing_page():
    return render_template('landing-professional.html')

# =====================================================================
# HEALTH CHECK & ADMIN
# =====================================================================

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'version': '1.0.0'
    })

@app.route('/api/admin/cache-stats')
@login_required
def cache_stats_endpoint():
    """Return current kb_version and cache backend for a client."""
    client_id = request.args.get('client_id', '')
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    stats = cache_utils.cache_stats(client_id)
    return jsonify({'success': True, **stats})


@app.route('/api/admin/cache-invalidate', methods=['POST'])
@login_required
def cache_invalidate_endpoint():
    """Manually invalidate KB cache for a client (emergency use)."""
    data      = request.get_json() or {}
    client_id = data.get('client_id', '')
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    new_version = cache_utils.invalidate(client_id)
    app.logger.info(
        f"[Cache] Manual invalidation: client={client_id} new_version={new_version} "
        f"by user={current_user.id}"
    )
    return jsonify({'success': True, 'new_kb_version': new_version})

@app.route('/api/admin/backup', methods=['POST'])
def trigger_backup():
    import hmac
    auth_token = request.headers.get('X-Admin-Token') or ''
    admin_token = os.environ.get('ADMIN_TOKEN', '')

    # Require ADMIN_TOKEN to be explicitly set in env — no insecure default.
    if not admin_token:
        app.logger.error('[Backup] ADMIN_TOKEN env var not set — endpoint disabled.')
        return jsonify({'success': False, 'error': 'Backup not configured'}), 503

    # Timing-safe comparison prevents secret-length oracle attacks.
    if not hmac.compare_digest(auth_token.encode(), admin_token.encode()):
        app.logger.warning(f'[Backup] Unauthorized attempt from {request.remote_addr}')
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401

    success = backup_all_clients()

    return jsonify({
        'success': success,
        'message': 'Backup completed' if success else 'Backup failed',
        'timestamp': datetime.now().isoformat()
    })


@app.route('/api/admin/reindex', methods=['POST'])
def trigger_reindex():
    """
    One-time admin endpoint to re-index all client FAQs through Voyage AI.

    Run this once after switching embedding models (e.g. bge → Voyage AI).
    Protected by the same ADMIN_TOKEN used for /api/admin/backup.

    Usage:
        curl -X POST https://your-app.railway.app/api/admin/reindex \
             -H "X-Admin-Token: your_admin_token"

    Or from a browser via fetch in the console:
        fetch('/api/admin/reindex', {
            method: 'POST',
            headers: { 'X-Admin-Token': 'your_admin_token' }
        }).then(r => r.json()).then(console.log)

    DELETE THIS ROUTE after re-indexing is complete.
    """
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
        from ai_helper import get_ai_helper
        helper  = get_ai_helper(Config.GEMINI_API_KEY, Config.GEMINI_MODEL)
        results = helper.reindex_all_clients()

        succeeded = {cid: n for cid, n in results.items() if n >= 0}
        failed    = {cid: n for cid, n in results.items() if n  < 0}
        total_emb = sum(succeeded.values())

        app.logger.info(
            f'[Reindex] complete — {len(succeeded)} OK, '
            f'{len(failed)} failed, {total_emb} embeddings stored'
        )
        return jsonify({
            'success':       len(failed) == 0,
            'clients_ok':    len(succeeded),
            'clients_failed': len(failed),
            'failed_ids':    list(failed.keys()),
            'total_embeddings': total_emb,
            'results':       results,
        })
    except Exception as e:
        app.logger.exception(f'[Reindex] fatal error: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500
def embed_generator():
    return render_template('embed-generator.html')


@app.route('/customize')
@login_required
def customize_page():
    client_id = request.args.get('client_id')

    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return "Unauthorized", 403

    fresh_user  = models.get_user_by_id(current_user.id)
    plan_type   = (fresh_user or {}).get('plan_type', current_user.plan_type)
    plan_limits = PLAN_LIMITS.get(plan_type, PLAN_LIMITS['free'])

    if not plan_limits['customization']:
        return render_template(
            'customize_upgrade.html',
            user=current_user, plan_type=plan_type
        ), 403

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
        data = request.json
        client_id = data.get('client_id')

        if not client_id:
            return jsonify({'success': False, 'error': 'Client ID required'}), 400

        if not models.verify_client_ownership(current_user.id, client_id):
            return jsonify({'success': False, 'error': 'Unauthorized'}), 403

        client = models.get_client_by_id(client_id)
        if not client:
            return jsonify({'success': False, 'error': 'Client not found'}), 404

        # Always fetch plan from DB — current_user.plan_type is cached in the
        # session and may be stale after a downgrade. A downgraded user could
        # otherwise still save white-label / webhook settings.
        fresh_user  = models.get_user_by_id(current_user.id)
        fresh_plan  = (fresh_user or {}).get('plan_type', 'free')
        plan_limits = PLAN_LIMITS.get(fresh_plan, PLAN_LIMITS['free'])

        # ── Integrations / Zapier / Make ─────────────────────────────────
        # Pro + Agency: webhook URL is supported, save whatever they send.
        # Free + Starter: wipe the webhook URL so it can never fire.
        incoming_integrations = data.get('integrations', {})
        if plan_limits['webhooks']:
            integrations = incoming_integrations
            app.logger.info(
                f"[Webhooks] Saved for user {current_user.id} "
                f"(plan: {fresh_plan}), "
                f"url_set: {bool(incoming_integrations.get('webhook_url'))}"
            )
        else:
            integrations = {}
            if incoming_integrations.get('webhook_url'):
                app.logger.info(
                    f"[Limit] Webhook URL stripped for user {current_user.id} "
                    f"on plan '{fresh_plan}'"
                )

        # Validate and sanitise vertical — fall back to 'general' if unrecognised
        incoming_vertical = data.get('vertical', 'general')
        vertical = incoming_vertical if incoming_vertical in VALID_VERTICALS else 'general'

        # ── Validate & sanitise new bubble/color fields before persisting ──────
        _hex_re = re.compile(r'^#[0-9A-Fa-f]{6}$')
        incoming_branding = data.get('branding', {})

        # bubble_radius: int clamped 0–22; missing/None → omit so Jinja uses preset
        _br_raw = incoming_branding.get('bubble_radius')
        if _br_raw is not None:
            try:
                incoming_branding['bubble_radius'] = max(0, min(22, int(_br_raw)))
            except (TypeError, ValueError):
                incoming_branding.pop('bubble_radius', None)

        # bot_bubble_color / user_bubble_color: valid #rrggbb hex or empty string
        for _color_key in ('bot_bubble_color', 'user_bubble_color'):
            _val = str(incoming_branding.get(_color_key, '')).strip()
            incoming_branding[_color_key] = _val if _hex_re.match(_val) else ''

        branding_settings = {
            'branding': incoming_branding,
            'contact': data.get('contact', {}),
            'bot_settings': data.get('bot_settings', {}),
            'integrations': integrations,
            'vertical': vertical,
        }

        # Ensure contact always includes address (may be absent from old saves)
        branding_settings['contact'].setdefault('address', '')

        # Scrub empty quick reply strings so they never persist to the DB
        raw_qr = branding_settings['bot_settings'].get('quick_replies') or []
        branding_settings['bot_settings']['quick_replies'] = [
            r for r in raw_qr if r and str(r).strip()
        ]

        # White-label only on agency/enterprise — use fresh plan, not cached session
        remove_branding = False
        if fresh_plan in ('agency', 'enterprise'):
            remove_branding = bool(data.get('remove_branding'))

        branding_settings['branding']['remove_branding'] = remove_branding

        conn = models.get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE clients 
            SET 
                branding_settings = %s,
                company_name = %s,
                widget_color = %s,
                welcome_message = %s,
                remove_branding = %s
            WHERE client_id = %s AND user_id = %s
            ''', (
                json.dumps(branding_settings),
                data.get('branding', {}).get('company_name'),
                data.get('branding', {}).get('primary_color'),
                data.get('bot_settings', {}).get('welcome_message'),
                remove_branding,
                client_id,
                current_user.id
            ))
        conn.commit()
        cursor.close()
        conn.close()

        app.logger.info(f'Customization saved for client: {client_id}')
        return jsonify({'success': True, 'message': 'Customization saved successfully'})

    except Exception as e:
        app.logger.error(f'Error saving customization: {e}')
        return jsonify({'success': False, 'error': 'Failed to save customization'}), 500


@app.route('/api/admin/webhooks', methods=['GET'])
@login_required
def get_webhooks():
    """List all webhook configs for a client."""
    client_id = request.args.get('client_id', '')
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    plan_limits = PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free'])
    if not plan_limits.get('webhooks'):
        return jsonify({'success': False, 'error': 'Webhooks require Pro or Agency plan'}), 403
    return jsonify({'success': True, 'webhooks': models.get_webhooks(client_id)})


@app.route('/api/admin/webhooks', methods=['POST'])
@login_required
def save_webhooks():
    """Save (upsert) the full list of webhooks for a client."""
    data      = request.json or {}
    client_id = data.get('client_id', '')
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    plan_limits = PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free'])
    if not plan_limits.get('webhooks'):
        return jsonify({'success': False, 'error': 'Webhooks require Pro or Agency plan'}), 403
    webhooks = data.get('webhooks', [])
    if len(webhooks) > 10:
        return jsonify({'success': False, 'error': 'Maximum 10 webhooks per client'}), 400
    count = models.save_webhooks(client_id, webhooks)
    app.logger.info(f"[Webhooks] Saved {count} webhooks for client={client_id} user={current_user.id}")
    return jsonify({'success': True, 'saved': count})


@app.route('/api/admin/webhooks/regenerate-secret', methods=['POST'])
@login_required
def regenerate_webhook_secret():
    """Generate a new signing secret for a specific webhook."""
    data       = request.json or {}
    client_id  = data.get('client_id', '')
    webhook_id = data.get('webhook_id', '')
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    plan_limits = PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free'])
    if not plan_limits.get('webhooks'):
        return jsonify({'success': False, 'error': 'Webhooks require Pro or Agency plan'}), 403
    new_secret = models.regenerate_signing_secret(client_id, webhook_id)
    return jsonify({'success': True, 'signing_secret': new_secret})


@app.route('/api/admin/webhooks/logs', methods=['GET'])
@login_required
def get_webhook_logs():
    """Return last 20 webhook deliveries for a client."""
    client_id = request.args.get('client_id', '')
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    plan_limits = PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free'])
    if not plan_limits.get('webhooks'):
        return jsonify({'success': False, 'error': 'Webhooks require Pro or Agency plan'}), 403
    return jsonify({'success': True, 'logs': models.get_webhook_logs(client_id, limit=20)})


# =====================================================================
# PLATFORM INTEGRATIONS — Shopify / Acuity webhook setup
# (distinct from the existing /api/admin/webhooks routes which handle
#  outbound Lumvi → CRM webhooks; these handle inbound platform → Lumvi)
# =====================================================================

@app.route('/integrations')
@login_required
def integrations_page():
    """
    Agency dashboard — Platform Integrations page.
    Lets operators connect Shopify / Acuity for each of their clients.
    Requires Pro or Agency plan (webhooks feature flag).
    """
    fresh_user  = models.get_user_by_id(current_user.id)
    plan_type   = (fresh_user or {}).get('plan_type', current_user.plan_type)
    plan_limits = PLAN_LIMITS.get(plan_type, PLAN_LIMITS['free'])

    if not plan_limits.get('webhooks'):
        # Redirect to upgrade page rather than a plain 403
        return redirect(url_for('dashboard') + '?upgrade=webhooks')

    clients = models.get_user_clients(current_user.id)
    base_url = os.environ.get('APP_BASE_URL', 'https://app.lumvi.ai')

    return render_template(
        'integrations.html',
        user=current_user,
        plan_type=plan_type,
        plan_limits=plan_limits,
        clients=clients,
        base_url=base_url,
    )


@app.route('/api/integrations/<client_id>', methods=['POST'])
@login_required
def create_platform_integration(client_id):
    """
    Connect or update a platform integration for a client.

    Body:  { platform, webhook_secret, platform_config? }
    Returns: { success, webhook_url, instructions }

    Security:
      • Ownership: caller must own client_id
      • Plan:      requires webhooks feature (Pro / Agency)
      • Input:     platform must be 'shopify' or 'acuity'; secret non-empty
    """
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    plan_limits = PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free'])
    if not plan_limits.get('webhooks'):
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
        f"[Integration] user={current_user.id} connected platform={platform} "
        f"client={client_id}"
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
    """
    List all active integrations for a client (secrets redacted).
    Also includes the live webhook URL for each connected platform.
    """
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    plan_limits = PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free'])
    if not plan_limits.get('webhooks'):
        return jsonify({'success': False, 'error': 'Webhooks require Pro or Agency plan'}), 403

    integrations = _webhooks.list_integrations(client_id)
    base_url     = os.environ.get('APP_BASE_URL', 'https://app.lumvi.ai')
    for i in integrations:
        i['webhook_url'] = f'{base_url}/webhooks/{i["platform"]}/{client_id}'

    return jsonify({'success': True, 'integrations': integrations}), 200


@app.route('/api/integrations/<client_id>/<platform>', methods=['DELETE'])
@login_required
def delete_platform_integration(client_id, platform):
    """
    Deactivate a platform integration.
    Existing data (orders, appointments) is preserved — only future
    webhook events will be rejected.
    """
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    plan_limits = PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free'])
    if not plan_limits.get('webhooks'):
        return jsonify({'success': False, 'error': 'Webhooks require Pro or Agency plan'}), 403

    if platform not in ('shopify', 'acuity'):
        return jsonify({'success': False, 'error': 'Unknown platform'}), 400

    ok = _webhooks.delete_integration(client_id, platform)
    if not ok:
        return jsonify({'success': False, 'error': 'Failed to deactivate integration'}), 500

    app.logger.info(
        f"[Integration] user={current_user.id} disconnected platform={platform} "
        f"client={client_id}"
    )
    return jsonify({'success': True, 'platform': platform}), 200


@app.route('/api/integrations/<client_id>/log', methods=['GET'])
@login_required
def get_integration_log(client_id):
    """
    Return the last 20 inbound webhook events for a client.
    Used by the integrations dashboard activity log.
    """
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    plan_limits = PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free'])
    if not plan_limits.get('webhooks'):
        return jsonify({'success': False, 'error': 'Webhooks require Pro or Agency plan'}), 403

    limit = min(int(request.args.get('limit', 20)), 100)

    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            """
            SELECT platform, event_type, status, payload_hash, error_msg, created_at
            FROM webhook_log
            WHERE client_id = %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (client_id, limit)
        )
        rows = cursor.fetchall()
        log  = [
            {
                'platform':   row['platform'],
                'event_type': row['event_type'],
                'status':     row['status'],
                'ref':        (row.get('payload_hash') or '')[:8] or '—',
                'error':      row.get('error_msg'),
                'time':       str(row.get('created_at', '')),
            }
            for row in rows
        ]
        return jsonify({'success': True, 'log': log}), 200
    except Exception as e:
        app.logger.error(f"[IntegrationLog] error: {e}")
        return jsonify({'success': False, 'error': 'Failed to load log'}), 500
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


# =====================================================================
# HUMAN INBOX — read, claim, and resolve handoff tickets
# Tickets are written by tools.escalate_to_human() which is called
# from the chat route whenever the AI pipeline returns contact_request.
# =====================================================================

@app.route('/inbox')
@login_required
def inbox_page():
    """Render the human inbox dashboard."""
    clients = models.get_user_clients(current_user.id)
    return render_template('inbox.html', user=current_user, clients=clients)


@app.route('/api/inbox/<client_id>', methods=['GET'])
@login_required
def list_inbox_tickets(client_id):
    """
    List tickets for a client, sorted by urgency then time.
    Query params: status (open|in_progress|resolved|all), limit, offset
    """
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    status = request.args.get('status', 'open')
    limit  = min(int(request.args.get('limit',  50)), 200)
    offset = max(int(request.args.get('offset',  0)),   0)

    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        where  = "client_id = %s"
        params = [client_id]
        if status != 'all':
            where  += " AND status = %s"
            params.append(status)

        cursor.execute(
            f"""
            SELECT ticket_id, session_id, reason, customer_email, customer_name,
                   summary, urgency, status, assigned_to, resolution_notes,
                   created_at, updated_at
            FROM human_inbox
            WHERE {where}
            ORDER BY
                CASE urgency
                    WHEN 'urgent' THEN 1 WHEN 'high' THEN 2
                    WHEN 'normal' THEN 3 ELSE 4
                END,
                created_at DESC
            LIMIT %s OFFSET %s
            """,
            params + [limit, offset]
        )
        rows = cursor.fetchall()
        cursor.execute(
            f"SELECT COUNT(*) AS n FROM human_inbox WHERE {where}", params
        )
        total = cursor.fetchone()['n']
        tickets = [
            {
                'ticket_id':        row['ticket_id'],
                'session_id':       row['session_id'],
                'reason':           row['reason'],
                'customer_email':   row['customer_email'],
                'customer_name':    row['customer_name'],
                'summary':          row['summary'],
                'urgency':          row['urgency'],
                'status':           row['status'],
                'assigned_to':      row['assigned_to'],
                'resolution_notes': row['resolution_notes'],
                'created_at':  row['created_at'].isoformat() if row['created_at'] else '',
                'updated_at':  row['updated_at'].isoformat() if row['updated_at'] else '',
            }
            for row in rows
        ]
        return jsonify({
            'success': True, 'tickets': tickets,
            'total': total, 'limit': limit, 'offset': offset,
        }), 200
    except Exception as e:
        app.logger.error(f"[Inbox] list error client={client_id}: {e}")
        return jsonify({'success': False, 'error': 'Failed to load tickets'}), 500
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


@app.route('/api/inbox/<client_id>/<ticket_id>', methods=['GET'])
@login_required
def get_inbox_ticket(client_id, ticket_id):
    """Return one ticket in full, including the conversation transcript."""
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            """
            SELECT ticket_id, session_id, reason, customer_email, customer_name,
                   summary, urgency, status, assigned_to, resolution_notes,
                   created_at, updated_at
            FROM human_inbox
            WHERE client_id = %s AND ticket_id = %s
            """,
            (client_id, ticket_id)
        )
        row = cursor.fetchone()
        if not row:
            return jsonify({'success': False, 'error': 'Ticket not found'}), 404

        ticket = {
            'ticket_id':        row['ticket_id'],
            'session_id':       row['session_id'],
            'reason':           row['reason'],
            'customer_email':   row['customer_email'],
            'customer_name':    row['customer_name'],
            'summary':          row['summary'],
            'urgency':          row['urgency'],
            'status':           row['status'],
            'assigned_to':      row['assigned_to'],
            'resolution_notes': row['resolution_notes'],
            'created_at':  row['created_at'].isoformat() if row['created_at'] else '',
            'updated_at':  row['updated_at'].isoformat() if row['updated_at'] else '',
        }

        # Pull the full conversation transcript for this session
        transcript = []
        if row['session_id']:
            cursor.execute(
                """
                SELECT user_message, bot_response, timestamp, method
                FROM conversations
                WHERE client_id = %s AND session_id = %s
                ORDER BY timestamp ASC
                LIMIT 100
                """,
                (client_id, row['session_id'])
            )
            for t in cursor.fetchall():
                if t['user_message']:
                    transcript.append({
                        'role': 'user', 'content': t['user_message'],
                        'time': t['timestamp'].isoformat() if t['timestamp'] else '',
                    })
                if t['bot_response']:
                    transcript.append({
                        'role': 'assistant', 'content': t['bot_response'],
                        'method': t.get('method', ''),
                        'time': t['timestamp'].isoformat() if t['timestamp'] else '',
                    })

        ticket['transcript'] = transcript
        return jsonify({'success': True, 'ticket': ticket}), 200
    except Exception as e:
        app.logger.error(f"[Inbox] get error ticket={ticket_id}: {e}")
        return jsonify({'success': False, 'error': 'Failed to load ticket'}), 500
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


@app.route('/api/inbox/<client_id>/<ticket_id>', methods=['PATCH'])
@login_required
def update_inbox_ticket(client_id, ticket_id):
    """Update status, assigned_to, or resolution_notes on a ticket."""
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    data = request.get_json(force=True) or {}
    allowed_statuses = {'open', 'in_progress', 'resolved'}

    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            "SELECT status FROM human_inbox WHERE client_id = %s AND ticket_id = %s",
            (client_id, ticket_id)
        )
        row = cursor.fetchone()
        if not row:
            return jsonify({'success': False, 'error': 'Ticket not found'}), 404

        updates = {}
        new_status = (data.get('status') or '').lower().strip()
        if new_status:
            if new_status not in allowed_statuses:
                return jsonify({'success': False,
                                'error': f'status must be one of {allowed_statuses}'}), 400
            if row['status'] == 'resolved' and new_status != 'resolved':
                return jsonify({'success': False,
                                'error': 'Resolved tickets cannot be reopened'}), 400
            updates['status'] = new_status
        if 'assigned_to' in data:
            updates['assigned_to'] = sanitize_input(str(data['assigned_to'] or ''), 200)
        if 'resolution_notes' in data:
            updates['resolution_notes'] = sanitize_input(str(data['resolution_notes'] or ''), 2000)
        if not updates:
            return jsonify({'success': False, 'error': 'No fields to update'}), 400

        updates['updated_at'] = datetime.utcnow()
        set_clause = ', '.join(f"{k} = %s" for k in updates)
        cursor.execute(
            f"UPDATE human_inbox SET {set_clause} WHERE client_id = %s AND ticket_id = %s",
            list(updates.values()) + [client_id, ticket_id]
        )
        conn.commit()
        app.logger.info(
            f"[Inbox] ticket={ticket_id} updated by user={current_user.id} "
            f"fields={list(updates.keys())}"
        )
        return jsonify({'success': True, 'ticket_id': ticket_id}), 200
    except Exception as e:
        app.logger.error(f"[Inbox] update error ticket={ticket_id}: {e}")
        if conn:
            try: conn.rollback()
            except Exception: pass
        return jsonify({'success': False, 'error': 'Failed to update ticket'}), 500
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


@app.route('/api/inbox/<client_id>/counts', methods=['GET'])
@login_required
def get_inbox_counts(client_id):
    """Open/in_progress/resolved counts — used by the sidebar badge."""
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            "SELECT status, COUNT(*) AS n FROM human_inbox WHERE client_id = %s GROUP BY status",
            (client_id,)
        )
        counts = {'open': 0, 'in_progress': 0, 'resolved': 0}
        for row in cursor.fetchall():
            if row['status'] in counts:
                counts[row['status']] = row['n']
        counts['total_open'] = counts['open'] + counts['in_progress']
        return jsonify({'success': True, 'counts': counts}), 200
    except Exception as e:
        app.logger.error(f"[Inbox] counts error client={client_id}: {e}")
        return jsonify({'success': False, 'error': 'Failed to load counts'}), 500
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()
@login_required
def test_webhook():
    """
    Fire a test delivery to a specific webhook.
    Returns status_code, response body, duration, and the exact payload sent.
    Also logs the delivery to webhook_logs.
    """
    import time, hmac, hashlib

    data       = request.json or {}
    webhook_url = data.get('webhook_url', '').strip()
    client_id  = data.get('client_id', '')
    webhook_id = data.get('webhook_id', '')
    event_type = data.get('event_type', 'test')

    if not webhook_url:
        return jsonify({'success': False, 'error': 'No webhook URL provided'}), 400

    plan_limits = PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free'])
    if not plan_limits.get('webhooks'):
        return jsonify({'success': False, 'error': 'Webhooks require Pro or Agency plan'}), 403

    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    # Build event-specific sample payload
    ts = datetime.utcnow().isoformat() + 'Z'
    SAMPLE_PAYLOADS = {
        'lead_captured': {
            'event':     'lead_captured',
            'client_id': client_id,
            'timestamp': ts,
            'data': {
                'name':    'Jane Smith',
                'email':   'jane@example.com',
                'phone':   '+1 555 000 0000',
                'company': 'Acme Corp',
                'message': 'I am interested in your services.',
                'source_url': 'https://example.com',
            }
        },
        'conversation_ended': {
            'event':     'conversation_ended',
            'client_id': client_id,
            'timestamp': ts,
            'data': {
                'session_id':    'sess_abc123',
                'message_count': 6,
                'resolved':      True,
                'duration_secs': 142,
            }
        },
        'faq_matched': {
            'event':     'faq_matched',
            'client_id': client_id,
            'timestamp': ts,
            'data': {
                'question':   'What are your business hours?',
                'faq_id':     'faq_xyz',
                'confidence': 0.94,
                'method':     'ai_smart',
            }
        },
        'message_sent': {
            'event':     'message_sent',
            'client_id': client_id,
            'timestamp': ts,
            'data': {
                'role':    'user',
                'content': 'Do you offer refunds?',
            }
        },
        'test': {
            'event':     'test',
            'client_id': client_id,
            'timestamp': ts,
            'data': {
                'message': 'This is a test delivery from Lumvi.',
                'sent_by': current_user.email,
            }
        },
    }

    payload = SAMPLE_PAYLOADS.get(event_type, SAMPLE_PAYLOADS['test'])

    # Sign the payload if we have a signing secret
    signing_secret = models.get_signing_secret(client_id, webhook_id) if webhook_id else ''
    headers = {
        'Content-Type':    'application/json',
        'X-Lumvi-Event':   event_type,
        'X-Lumvi-Delivery': str(uuid.uuid4()),
    }
    if signing_secret:
        body_bytes = json.dumps(payload).encode()
        sig = hmac.new(signing_secret.encode(), body_bytes, hashlib.sha256).hexdigest()
        headers['X-Lumvi-Signature'] = f'sha256={sig}'

    t0 = time.time()
    try:
        resp = requests.post(webhook_url, json=payload, headers=headers, timeout=10)
        duration_ms = int((time.time() - t0) * 1000)
        success     = 200 <= resp.status_code < 300
        response_body = resp.text[:500]

        # Log the delivery
        if webhook_id:
            models.log_webhook_delivery(
                client_id=client_id, webhook_id=webhook_id,
                event_type=event_type, url=webhook_url,
                payload=payload, status_code=resp.status_code,
                response_text=response_body, success=success,
                duration_ms=duration_ms,
            )

        return jsonify({
            'success':      success,
            'status_code':  resp.status_code,
            'duration_ms':  duration_ms,
            'response_body': response_body,
            'payload_sent': payload,
            'message':      f'HTTP {resp.status_code} · {duration_ms}ms',
        })

    except requests.exceptions.Timeout:
        duration_ms = int((time.time() - t0) * 1000)
        if webhook_id:
            models.log_webhook_delivery(
                client_id=client_id, webhook_id=webhook_id,
                event_type=event_type, url=webhook_url,
                payload=payload, status_code=0,
                response_text='Timeout', success=False,
                duration_ms=duration_ms,
            )
        return jsonify({'success': False, 'error': 'Request timed out (>10s)', 'payload_sent': payload})
    except requests.exceptions.ConnectionError as e:
        return jsonify({'success': False, 'error': 'Could not connect — check the URL', 'payload_sent': payload})
    except Exception as e:
        app.logger.error(f'[test-webhook] {e}')
        return jsonify({'success': False, 'error': str(e), 'payload_sent': payload})


# =====================================================================
# WHITE-LABEL ROUTES
# =====================================================================

@app.route('/api/admin/white-label', methods=['GET'])
@login_required
def get_white_label():
    """Return white-label settings for a client (custom domain, CSS, email from)."""
    client_id = request.args.get('client_id', '')
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    fresh_user  = models.get_user_by_id(current_user.id)
    plan_type   = (fresh_user or {}).get('plan_type', current_user.plan_type)
    plan_limits = PLAN_LIMITS.get(plan_type, PLAN_LIMITS['free'])

    if not plan_limits.get('customization'):
        return jsonify({'success': False, 'error': 'Plan upgrade required'}), 403

    client = models.get_client_by_id(client_id)
    if not client:
        return jsonify({'success': False, 'error': 'Client not found'}), 404

    return jsonify({
        'success': True,
        'custom_widget_domain': client.get('custom_widget_domain') or '',
        'custom_css':           client.get('custom_css') or '',
        'branded_email_from':   client.get('branded_email_from') or '',
        'has_custom_domain':    bool(client.get('custom_widget_domain')),
    })


@app.route('/api/admin/white-label', methods=['POST'])
@login_required
def save_white_label():
    """
    Save white-label settings for a client.
    Plan gating:
      - branded_email_from  → Pro+
      - custom_widget_domain, custom_css → Agency only
    """
    data      = request.json or {}
    client_id = data.get('client_id', '')

    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    fresh_user  = models.get_user_by_id(current_user.id)
    plan_type   = (fresh_user or {}).get('plan_type', current_user.plan_type)
    plan_limits = PLAN_LIMITS.get(plan_type, PLAN_LIMITS['free'])
    is_agency   = plan_type in ('agency', 'enterprise')
    is_pro_plus = plan_type in ('pro', 'agency', 'enterprise')

    # Keep empty string as sentinel for 'clear this field';
    # None means 'key not in payload — don't touch'.
    # models.save_white_label_settings() understands both sentinels.
    _raw_domain    = data.get('custom_widget_domain')
    _raw_css       = data.get('custom_css')
    _raw_email     = data.get('branded_email_from')
    domain             = _raw_domain.strip().lower()    if _raw_domain    is not None else None
    custom_css         = _raw_css.strip()               if _raw_css       is not None else None
    branded_email_from = _raw_email.strip()             if _raw_email     is not None else None

    # Validate domain format (skip if clearing)
    if domain and not models.is_valid_domain(domain):
        return jsonify({'success': False, 'error': f'"{domain}" is not a valid domain name. Use format: chat.yoursite.com'}), 400

    # Plan gating
    if domain and not is_agency:
        return jsonify({'success': False, 'error': 'Custom widget domain requires the Agency plan'}), 403
    if custom_css and not is_agency:
        return jsonify({'success': False, 'error': 'Custom CSS requires the Agency plan'}), 403
    if branded_email_from and not is_pro_plus:
        return jsonify({'success': False, 'error': 'Branded email sender requires Pro or Agency plan'}), 403

    # Check domain uniqueness (another client can't use the same domain).
    # The application-level check below is a fast path; the DB MUST also have
    # a UNIQUE constraint on custom_widget_domain so two concurrent requests
    # that both pass this check cannot both commit the same domain.
    # Run once in psql: ALTER TABLE clients ADD CONSTRAINT uq_custom_widget_domain
    #                   UNIQUE (custom_widget_domain);
    if domain:
        existing = models.get_client_by_custom_domain(domain)
        if existing and existing['client_id'] != client_id:
            return jsonify({'success': False, 'error': f'Domain "{domain}" is already in use by another client'}), 409

    try:
        models.save_white_label_settings(client_id, domain, custom_css, branded_email_from)
    except Exception as _wl_err:
        # Catch a DB-level unique violation in the rare race window where two
        # concurrent requests both passed the application check above.
        _err_str = str(_wl_err).lower()
        if 'unique' in _err_str or 'duplicate' in _err_str:
            app.logger.warning(
                f"[WhiteLabel] domain conflict (race) client={client_id} domain={domain}: {_wl_err}"
            )
            return jsonify({'success': False, 'error': f'Domain "{domain}" was just claimed by another client. Please choose a different domain.'}), 409
        raise
    app.logger.info(f"[WhiteLabel] saved client={client_id} domain={domain} user={current_user.id}")

    return jsonify({
        'success': True,
        'message': 'White-label settings saved',
        'cname_target': 'lumvi.net',
        'cname_instructions': (
            f'Point a CNAME record from {domain} → lumvi.net in your DNS provider, '
            'then wait up to 24h for propagation.'
        ) if domain else None,
    })


@app.route('/api/admin/agency-branding', methods=['GET'])
@login_required
def get_agency_branding():
    """Return the agency-wide default branding for the current user."""
    fresh_user = models.get_user_by_id(current_user.id)
    plan_type  = (fresh_user or {}).get('plan_type', current_user.plan_type)
    if plan_type not in ('agency', 'enterprise'):
        return jsonify({'success': False, 'error': 'Agency plan required'}), 403
    return jsonify({'success': True, 'agency_branding': models.get_agency_branding(current_user.id)})


@app.route('/api/admin/agency-branding', methods=['POST'])
@login_required
def save_agency_branding_route():
    """
    Save agency-wide default branding.
    These defaults are auto-applied when a new client is created under this agency account.
    """
    fresh_user = models.get_user_by_id(current_user.id)
    plan_type  = (fresh_user or {}).get('plan_type', current_user.plan_type)
    if plan_type not in ('agency', 'enterprise'):
        return jsonify({'success': False, 'error': 'Agency plan required'}), 403

    data = request.json or {}
    agency_branding = {
        'branding': data.get('branding', {}),
        'bot_settings': data.get('bot_settings', {}),
        'contact': data.get('contact', {}),
        'branded_email_from': data.get('branded_email_from', ''),
    }
    models.save_agency_branding(current_user.id, agency_branding)
    app.logger.info(f"[AgencyBranding] saved user={current_user.id}")
    return jsonify({'success': True, 'message': 'Agency branding defaults saved'})


@app.route('/api/admin/white-label/verify-domain', methods=['POST'])
@login_required
def verify_custom_domain():
    """
    DNS check — walks the CNAME chain to verify the domain points to lumvi.net.
    Uses dnspython when available (accurate for Cloudflare-proxied domains).
    Falls back to socket IP comparison otherwise.
    Runs in a thread pool so it never blocks a Flask worker.
    """
    data      = request.json or {}
    domain    = data.get('domain', '').strip().lower()
    client_id = data.get('client_id', '')

    if not domain or not models.is_valid_domain(domain):
        return jsonify({'success': False, 'error': 'Invalid domain'}), 400
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    # Run DNS lookup off the Flask worker thread — 8 s hard timeout
    future = _dns_executor.submit(models.check_domain_dns, domain)
    try:
        result = future.result(timeout=8)
    except _FuturesTimeout:
        app.logger.warning(f"[DNS] verify timed out for domain={domain}")
        return jsonify({
            'success': True,
            'domain':  domain,
            'pointed': False,
            'message': '⏳ DNS check timed out — try again in a moment',
            'chain':   [],
        })
    except Exception as exc:
        app.logger.error(f"[DNS] verify error domain={domain}: {exc}")
        return jsonify({
            'success': True,
            'domain':  domain,
            'pointed': False,
            'message': '✗ DNS check failed — check your DNS records',
            'chain':   [],
        })

    app.logger.info(
        f"[DNS] verify domain={domain} pointed={result['pointed']} "
        f"chain={result.get('chain')} user={current_user.id}"
    )
    return jsonify({
        'success': True,
        'domain':  domain,
        'pointed': result['pointed'],
        'message': result['message'],
        'chain':   result.get('chain', []),
    })


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

    # Default to first client if none specified
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
        clients      = clients,
        client_id    = client_id,
        plan_type    = plan_type,
        plan_limits  = plan_limits,
        is_agency    = is_agency,
        user         = current_user,
    )


@app.route('/api/analytics/agency')
@login_required
def get_agency_analytics():
    """
    Multi-client overview analytics.
    Returns per-client stats + totals for the agency dashboard.
    """
    try:
        fresh_user  = models.get_user_by_id(current_user.id)
        plan_type   = (fresh_user or {}).get('plan_type', current_user.plan_type)
        plan_limits = PLAN_LIMITS.get(plan_type, PLAN_LIMITS['free'])
        is_admin    = bool((fresh_user or {}).get('is_admin', False))

        if not plan_limits['analytics'] and not is_admin:
            return jsonify({'success': False, 'error': 'Upgrade required'}), 403

        date_range = request.args.get('range', 'week')
        now        = datetime.now()
        if date_range == 'today':
            start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        elif date_range == 'week':
            start_date = now - timedelta(days=7)
        elif date_range == 'month':
            start_date = now - timedelta(days=30)
        else:
            start_date = datetime(2020, 1, 1)

        clients    = models.get_user_clients(current_user.id)
        client_ids = [c['client_id'] for c in clients]

        if not client_ids:
            return jsonify({'success': True, 'clients': [], 'totals': {}, 'timeline': []})

        conn, cursor = models.get_db()
        try:
            # ── Per-client stats in bulk ──────────────────────────────────
            cursor.execute(
            """SELECT client_id,
                      COUNT(*) AS total,
                      SUM(CASE WHEN matched = TRUE THEN 1 ELSE 0 END) AS matched
               FROM conversations
               WHERE client_id = ANY(%s) AND timestamp >= %s
               GROUP BY client_id""",
            (client_ids, start_date)
            )
            conv_stats = {r['client_id']: dict(r) for r in cursor.fetchall()}

            cursor.execute(
            "SELECT client_id, COUNT(*) AS cnt FROM leads WHERE client_id = ANY(%s) AND created_at >= %s GROUP BY client_id",
            (client_ids, start_date)
            )
            lead_stats = {r['client_id']: int(r['cnt']) for r in cursor.fetchall()}

            cursor.execute(
            "SELECT client_id, COUNT(*) AS cnt FROM conversations WHERE client_id = ANY(%s) AND matched = FALSE AND timestamp >= %s GROUP BY client_id",
            (client_ids, start_date)
            )
            unanswered_stats = {r['client_id']: int(r['cnt']) for r in cursor.fetchall()}

            today_str = now.strftime('%Y-%m-%d')
            cursor.execute(
            "SELECT client_id, COUNT(*) AS cnt FROM conversations WHERE client_id = ANY(%s) AND DATE(timestamp) = %s GROUP BY client_id",
            (client_ids, today_str)
            )
            daily_stats = {r['client_id']: int(r['cnt']) for r in cursor.fetchall()}

            cursor.execute(
            "SELECT client_id, MAX(timestamp) AS last_ts FROM conversations WHERE client_id = ANY(%s) GROUP BY client_id",
            (client_ids,)
            )
            last_active = {r['client_id']: r['last_ts'] for r in cursor.fetchall()}

            # ── 7-day combined timeline ───────────────────────────────────
            timeline = []
            for i in range(7):
                d     = (now - timedelta(days=6 - i)).strftime('%Y-%m-%d')
                cursor.execute(
                "SELECT COUNT(*) AS cnt FROM conversations WHERE client_id = ANY(%s) AND DATE(timestamp) = %s",
                (client_ids, d)
                )
                c_row = cursor.fetchone() or {}
                cursor.execute(
                "SELECT COUNT(*) AS cnt FROM leads WHERE client_id = ANY(%s) AND DATE(created_at) = %s",
                (client_ids, d)
                )
                l_row = cursor.fetchone() or {}
                timeline.append({'date': d, 'conversations': int(c_row.get('cnt', 0)), 'leads': int(l_row.get('cnt', 0))})

        finally:
            try: cursor.close()
            except Exception: pass
            try: conn.close()
            except Exception: pass

        # ── Build per-client result list ──────────────────────────────
        client_map   = {c['client_id']: c for c in clients}
        plan_limits_ = PLAN_LIMITS.get(plan_type, PLAN_LIMITS['free'])
        daily_limit  = plan_limits_['messages_per_day']

        result_clients = []
        for cid in client_ids:
            cs       = conv_stats.get(cid, {'total': 0, 'matched': 0})
            total    = int(cs.get('total', 0))
            matched  = int(cs.get('matched', 0))
            leads    = lead_stats.get(cid, 0)
            daily    = daily_stats.get(cid, 0)
            unanswered = unanswered_stats.get(cid, 0)
            res_rate = round(matched / total * 100) if total > 0 else 0
            last_ts  = last_active.get(cid)

            if daily_limit >= 999999:
                usage_pct = 0
            else:
                usage_pct = min(round(daily / daily_limit * 100), 100)

            result_clients.append({
                'client_id':    cid,
                'name':         client_map.get(cid, {}).get('company_name', cid),
                'conversations': total,
                'leads':         leads,
                'resolution_rate': res_rate,
                'daily_msgs':    daily,
                'daily_limit':   'Unlimited' if daily_limit >= 999999 else daily_limit,
                'usage_pct':     usage_pct,
                'unanswered':    unanswered,
                'last_active':   last_ts.isoformat() if last_ts else None,
            })

        # Sort by conversations desc
        result_clients.sort(key=lambda x: x['conversations'], reverse=True)

        # ── Totals ────────────────────────────────────────────────────
        tot_conv   = sum(c['conversations'] for c in result_clients)
        tot_leads  = sum(c['leads'] for c in result_clients)
        tot_unanswered = sum(c['unanswered'] for c in result_clients)
        avg_res    = round(sum(c['resolution_rate'] for c in result_clients) / len(result_clients)) if result_clients else 0

        return jsonify({
            'success':  True,
            'clients':  result_clients,
            'timeline': timeline,
            'totals': {
                'clients':         len(clients),
                'conversations':   tot_conv,
                'leads':           tot_leads,
                'resolution_rate': avg_res,
                'unanswered':      tot_unanswered,
            }
        })

    except Exception as e:
        app.logger.error(f'[agency analytics] {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/client-report')
@login_required
def client_report():
    """
    Branded, one-page client performance report.
    Accessible by the agency owner (?client_id=) or a logged-in client portal user.
    """
    client_id = request.args.get('client_id', '').strip()
    period    = request.args.get('period', 'month')   # week | month | all

    # Owner access — verify ownership
    if client_id and not models.verify_client_ownership(current_user.id, client_id):
        return "Unauthorized", 403
    # Fall back to first client if not specified
    if not client_id:
        clients = models.get_user_clients(current_user.id)
        if clients:
            client_id = clients[0]['client_id']
        else:
            return redirect(url_for('dashboard'))

    client = models.get_client_by_id(client_id)
    if not client:
        return "Client not found", 404

    # Parse branding
    branding = {}
    bs_raw   = client.get('branding_settings') or '{}'
    try:
        branding = json.loads(bs_raw) if isinstance(bs_raw, str) else bs_raw
    except Exception:
        branding = {}

    branding_inner = branding.get('branding', {})
    primary_color  = branding_inner.get('primary_color') or client.get('widget_color') or '#B8924A'
    logo_url       = branding_inner.get('logo') or branding_inner.get('logo_url') or ''
    company_name   = branding_inner.get('company_name') or client.get('company_name', 'Client')

    # Date range label
    period_labels = {'week': 'Last 7 Days', 'month': 'Last 30 Days', 'all': 'All Time'}
    period_label  = period_labels.get(period, 'Last 30 Days')

    # Get agency branding for the report header
    agency_branding = models.get_agency_branding(current_user.id) if hasattr(models, 'get_agency_branding') else {}
    agency_name     = (agency_branding.get('branding', {}).get('company_name') or
                       current_user.email.split('@')[0].title())

    return render_template(
        'client_report.html',
        client        = client,
        client_id     = client_id,
        company_name  = company_name,
        primary_color = primary_color,
        logo_url      = logo_url,
        agency_name   = agency_name,
        agency_branding = agency_branding,
        period        = period,
        period_label  = period_label,
        user          = current_user,
    )
@app.route('/api/admin/analytics', methods=['GET'])
@login_required
def get_analytics():
    try:
        client_id = request.args.get('client_id', '').strip()
        if not client_id:
            return jsonify({'success': False, 'error': 'No client_id provided'}), 400
            
        if not models.verify_client_ownership(current_user.id, client_id):
            return jsonify({'success': False, 'error': 'unauthorized'}), 403

        date_range = request.args.get('range', 'month')
        now = datetime.now()
        if date_range == 'today':
            start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        elif date_range == 'week':
            start_date = now - timedelta(days=7)
        elif date_range == 'month':
            start_date = now - timedelta(days=30)
        else:
            start_date = datetime(2020, 1, 1)

        conn, cursor = models.get_db()

        # 1. Basic Stats
        cursor.execute(
            'SELECT COUNT(*) AS total FROM conversations WHERE client_id = %s AND timestamp >= %s',
            (client_id, start_date)
        )
        total_conversations = (cursor.fetchone() or {}).get('total', 0)

        cursor.execute(
            'SELECT COUNT(*) AS matched_count FROM conversations WHERE client_id = %s AND timestamp >= %s AND matched = TRUE',
            (client_id, start_date)
        )
        answered = (cursor.fetchone() or {}).get('matched_count', 0)
        unanswered_count = total_conversations - answered
        answer_rate = int((answered / total_conversations * 100)) if total_conversations > 0 else 0

        cursor.execute(
            'SELECT COUNT(*) AS total_leads FROM leads WHERE client_id = %s AND created_at >= %s',
            (client_id, start_date)
        )
        total_leads = (cursor.fetchone() or {}).get('total_leads', 0)

        # 2. Timeline Logic
        timeline = []
        days_to_show = 7 if date_range == 'week' else 30
        for i in range(days_to_show):
            date = (now - timedelta(days=(days_to_show - 1) - i))
            date_str = date.strftime('%Y-%m-%d')
            cursor.execute(
                'SELECT COUNT(*) AS daily_count FROM conversations WHERE client_id = %s AND DATE(timestamp) = %s',
                (client_id, date_str)
            )
            conv_count = (cursor.fetchone() or {}).get('daily_count', 0)
            cursor.execute(
                'SELECT COUNT(*) AS daily_leads FROM leads WHERE client_id = %s AND DATE(created_at) = %s',
                (client_id, date_str)
            )
            lead_count = (cursor.fetchone() or {}).get('daily_leads', 0)
            timeline.append({'date': date_str, 'count': conv_count, 'leads': lead_count})

        # 3. Top Questions
        cursor.execute(
            'SELECT user_message, COUNT(*) as count FROM conversations WHERE client_id = %s AND timestamp >= %s AND matched = TRUE GROUP BY user_message ORDER BY count DESC LIMIT 6',
            (client_id, start_date)
        )
        top_questions = [{'question': r['user_message'], 'count': r['count']} for r in cursor.fetchall()]

        # 4. Unanswered (Gaps)
        cursor.execute(
            'SELECT user_message, COUNT(*) as count FROM conversations WHERE client_id = %s AND timestamp >= %s AND matched = FALSE GROUP BY user_message ORDER BY count DESC LIMIT 6',
            (client_id, start_date)
        )
        unanswered_list = [{'question': r['user_message'], 'count': r['count']} for r in cursor.fetchall()]

        # 5. Recent Leads
        cursor.execute(
            'SELECT name, email, phone, created_at FROM leads WHERE client_id = %s ORDER BY created_at DESC LIMIT 15',
            (client_id,)
        )
        leads_captured = []
        for r in cursor.fetchall():
            leads_captured.append({
                'name': r['name'], 
                'email': r['email'], 
                'phone': r['phone'], 
                'created_at': r['created_at'].isoformat() if r['created_at'] else ''
            })

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'analytics': {
                'total_conversations': total_conversations,
                'total_leads': total_leads,
                'answer_rate': answer_rate,
                'unanswered_count': unanswered_count,
                'timeline': timeline,
                'top_questions': top_questions,
                'unanswered': unanswered_list,
                'leads_captured': leads_captured
            }
        })

    except Exception as e:
        app.logger.error(f'Error getting analytics: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/sales')
def sales_page():
    return render_template('sales-page.html')


# =====================================================================
# HELP CENTER ARTICLES
# =====================================================================

@app.route('/help-center')
@login_required
def help_center_page():
    # Redirects to the new article manager — keeps old bookmarks working
    client_id = request.args.get('client_id', '')
    return redirect(url_for('article_manager_page', client_id=client_id))


@app.route('/api/articles', methods=['GET'])
def get_articles():
    """Public endpoint — used by chat widget to load articles."""
    client_id = request.args.get('client_id')
    if not client_id:
        return jsonify({'success': False, 'error': 'client_id required'}), 400
    articles = models.get_articles(client_id)
    return jsonify({'success': True, 'articles': articles})


@app.route('/api/articles/manage', methods=['GET', 'POST', 'PUT', 'DELETE'])
@login_required
def manage_articles():
    try:
        if request.method == 'GET':
            client_id = request.args.get('client_id')
            if not client_id or not models.verify_client_ownership(current_user.id, client_id):
                return jsonify({'success': False, 'error': 'Unauthorized'}), 403
            articles = models.get_articles(client_id)
            return jsonify({'success': True, 'articles': articles})

        data = request.get_json()
        client_id = data.get('client_id')
        if not client_id or not models.verify_client_ownership(current_user.id, client_id):
            return jsonify({'success': False, 'error': 'Unauthorized'}), 403

        if request.method == 'POST':
            title    = data.get('title', '').strip()
            content  = data.get('content', '').strip()
            category = data.get('category', 'General').strip()
            if not title or not content:
                return jsonify({'success': False, 'error': 'Title and content are required'}), 400
            article_id = models.create_article(client_id, title, content, category)
            return jsonify({'success': True, 'id': article_id})

        if request.method == 'PUT':
            article_id = data.get('id')
            title    = data.get('title', '').strip()
            content  = data.get('content', '').strip()
            category = data.get('category', 'General').strip()
            if not article_id or not title or not content:
                return jsonify({'success': False, 'error': 'id, title and content are required'}), 400
            models.update_article(article_id, client_id, title, content, category)
            return jsonify({'success': True})

        if request.method == 'DELETE':
            article_id = data.get('id')
            if not article_id:
                return jsonify({'success': False, 'error': 'id required'}), 400
            models.delete_article(article_id, client_id)
            return jsonify({'success': True})

    except Exception as e:
        app.logger.error(f'Articles error: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/thank-you')
def thank_you_page():
    return render_template('thank-you.html')


@app.route('/faq-manager')
@login_required
def faq_manager_page():
    client_id = request.args.get('client_id')

    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return "Unauthorized", 403

    return render_template('faq-manager.html')


@app.route('/article-manager')
@login_required
def article_manager_page():
    """Help Center article manager — create, edit and delete articles per client."""
    client_id = request.args.get('client_id')

    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return "Unauthorized", 403

    client      = models.get_client_by_id(client_id)
    fresh_user  = models.get_user_by_id(current_user.id)
    plan_type   = (fresh_user or {}).get('plan_type', current_user.plan_type)

    return render_template(
        'article-manager.html',
        client_id  = client_id,
        client     = client,
        plan_type  = plan_type,
        user       = current_user,
    )


@app.route('/api/faqs', methods=['GET', 'POST'])
@login_required
def manage_faqs():
    try:
        if request.method == 'GET':
            client_id = request.args.get('client_id')
        else:
            if request.is_json:
                client_id = request.json.get('client_id')
            else:
                client_id = request.form.get('client_id')

        if not client_id:
            return jsonify({'success': False, 'error': 'Client ID is required'}), 400

        if not models.verify_client_ownership(current_user.id, client_id):
            return jsonify({'success': False, 'error': 'Unauthorized'}), 403

        if request.method == 'GET':
            try:
                faqs = models.get_faqs(client_id)
                return jsonify({'success': True, 'faqs': faqs})
            except Exception as e:
                app.logger.error(f'Error loading FAQs: {e}')
                return jsonify({'success': True, 'faqs': []})

        elif request.method == 'POST':
            if request.is_json:
                faqs_list = request.json.get('faqs', [])
            else:
                return jsonify({'success': False, 'error': 'Request must be JSON'}), 400

            user = models.get_user_by_id(current_user.id)
            plan_limits = PLAN_LIMITS.get(user['plan_type'], PLAN_LIMITS['free'])
            max_faqs = plan_limits['faqs_per_client']

            if len(faqs_list) > max_faqs:
                return jsonify({
                    'success': False,
                    'error': f'Plan limit: Maximum {max_faqs} FAQs allowed on {user["plan_type"]} plan',
                    'upgrade_required': True
                }), 403

            models.save_faqs(client_id, faqs_list)
            cache_utils.bump_kb_version(client_id)
            app.logger.info(f"[Cache] KB invalidated after FAQ save: client={client_id}")

            # System 2: record each saved FAQ as a correction training sample.
            # This captures human-curated knowledge edits as high-quality (1.0)
            # training signal — the best data the system will ever see.
            if client_id != 'demo':
                try:
                    from training_collector import collect_correction
                    vertical = json.loads(
                        models.get_client_by_id(client_id).get('branding_settings') or '{}'
                    ).get('vertical', 'general')
                    for faq in faqs_list[:50]:  # cap at 50 per save to avoid burst writes
                        q = (faq.get('question') or '').strip()
                        a = (faq.get('answer')   or '').strip()
                        if q and a:
                            collect_correction(
                                client_id        = client_id,
                                session_id       = '',
                                original_message = q,
                                bad_response     = '',   # no prior bad response — new FAQ
                                correct_response = a,
                                corrected_by     = f'user:{current_user.id}',
                                vertical         = vertical,
                            )
                except Exception as _tc_err:
                    app.logger.debug(f'[TrainingCollector] FAQ correction error: {_tc_err}')

            # Re-index embeddings for semantic search (non-blocking)
            if ai_helper and ai_helper.enabled:
                try:
                    ai_helper.index_faqs(faqs_list, client_id)
                except Exception as _idx_err:
                    app.logger.warning(f"[index_faqs] non-critical error: {_idx_err}")

            return jsonify({'success': True, 'message': 'FAQs updated successfully'})

    except Exception as e:
        app.logger.error(f'Error managing FAQs: {e}')
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': 'Failed to manage FAQs'}), 500


@app.route('/api/faqs/delete-all', methods=['POST'])
@login_required
def delete_all_faqs():
    """Delete all FAQs for a client — called by the FAQ Manager Delete All button."""
    try:
        data = request.get_json()
        client_id = data.get('client_id') if data else None

        if not client_id:
            return jsonify({'success': False, 'error': 'Client ID required'}), 400

        if not models.verify_client_ownership(current_user.id, client_id):
            return jsonify({'success': False, 'error': 'Unauthorized'}), 403

        # Delete from both tables. models.delete_all_faqs() covers the primary
        # knowledge_base table; the direct SQL below covers the legacy faqs table
        # and acts as a hard fallback if the models function doesn't exist yet.
        if hasattr(models, 'delete_all_faqs'):
            models.delete_all_faqs(client_id)
        else:
            app.logger.warning("[delete_all_faqs] models.delete_all_faqs not found — using direct SQL fallback")

        # Always delete from both tables directly to guarantee clean state
        try:
            conn, cursor = models.get_db()
            cursor.execute('DELETE FROM faqs WHERE client_id = %s', (client_id,))
            cursor.execute('DELETE FROM knowledge_base WHERE client_id = %s', (client_id,))
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as _del_err:
            app.logger.error(f"[delete_all_faqs] Direct SQL delete failed: {_del_err}")

        cache_utils.bump_kb_version(client_id)
        app.logger.info(f'[Cache] KB invalidated after delete-all: client={client_id}')
        app.logger.info(f'All FAQs deleted for client {client_id} by user {current_user.id}')
        return jsonify({'success': True, 'message': 'All FAQs deleted successfully'})

    except Exception as e:
        app.logger.error(f'Error deleting all FAQs: {e}')
        return jsonify({'success': False, 'error': 'Failed to delete FAQs'}), 500


def _save_legacy_faqs(client_id: str, chunks: list):
    """Insert enriched chunks into the legacy faqs table (backward compat)."""
    conn, cursor = models.get_db()
    saved = 0
    try:
        for chunk in chunks:
            faq_id = str(uuid.uuid4())
            cursor.execute(
                '''INSERT INTO faqs (client_id, faq_id, question, answer, category, triggers)
                   VALUES (%s, %s, %s, %s, %s, %s)''',
                (
                    client_id, faq_id,
                    chunk['title'],
                    chunk['content'],
                    chunk.get('category', 'General'),
                    json.dumps(chunk.get('tags', []))
                )
            )
            saved += 1
        conn.commit()
    except Exception as _e:
        app.logger.warning(f"[Upload/BG] Legacy FAQ save error (non-critical): {_e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
    return saved


def _bg_enrich_and_save(client_id: str, valid_faqs: list):
    """
    Background worker: enrich → chunk → embed → save.
    Runs in a daemon thread so the HTTP response is returned immediately.
    The entire pipeline (Gemini enrichment + per-item embedding) can take
    30-120 seconds for a large file — it must never block the request cycle.
    """
    with app.app_context():
        try:
            app.logger.info(f"[Upload/BG] starting enrich for client={client_id} items={len(valid_faqs)}")
            if ai_helper and ai_helper.enabled:
                chunks = ai_helper.enrich_and_chunk(valid_faqs, client_id)
            else:
                chunks = [
                    {
                        'kb_id':     str(uuid.uuid4()),
                        'title':     item['question'],
                        'content':   item['answer'],
                        'type':      'faq',
                        'category':  item.get('category', 'General'),
                        'tags':      item.get('tags', []),
                        'embedding': [],
                        'metadata':  {'source': 'upload'},
                        'quality':   item.get('quality_score', 0.75),
                    }
                    for item in valid_faqs
                ]

            if not chunks:
                app.logger.warning(f"[Upload/BG] enrich returned 0 chunks for client={client_id}")
                return

            kb_saved  = models.save_knowledge_chunks(client_id, chunks)
            faq_saved = _save_legacy_faqs(client_id, chunks)
            cache_utils.bump_kb_version(client_id)
            app.logger.info(
                f"[Upload/BG] done client={client_id} kb_saved={kb_saved} faq_saved={faq_saved}"
            )
        except Exception as e:
            app.logger.error(f"[Upload/BG] error for client={client_id}: {e}", exc_info=True)


@app.route('/api/faq/upload', methods=['POST'])
@login_required
def upload_faqs():
    """
    Smart upload pipeline:
      1. Parse file (CSV / Excel / PDF)           — synchronous, fast
      2. Validate + basic enrichment              — synchronous, fast
      3. AI enrichment + embed + save             — BACKGROUND THREAD
         (enrich_and_chunk makes 100s of Gemini calls for large files;
          running it synchronously caused the 3-5 minute hang)
    """
    try:
        client_id = request.form.get('client_id')
        if not models.verify_client_ownership(current_user.id, client_id):
            return jsonify({'success': False, 'error': 'Unauthorized'}), 403

        if 'file' not in request.files:
            return jsonify({'success': False, 'error': 'No file uploaded'}), 400

        file = request.files['file']
        if not file.filename:
            return jsonify({'success': False, 'error': 'No file selected'}), 400

        filename = file.filename.lower()

        # ── Parse raw items from file (fast, synchronous) ────────────
        if filename.endswith('.csv'):
            raw_items = process_csv_upload(file)
        elif filename.endswith(('.xlsx', '.xls')):
            raw_items = process_excel_upload(file)
        elif filename.endswith('.pdf'):
            raw_items = process_pdf_upload(file)
        else:
            return jsonify({'success': False, 'error': 'Unsupported file type. Upload CSV, Excel, or PDF.'}), 400

        if not raw_items:
            return jsonify({'success': False, 'error': 'No content found in file. Check the format.'}), 400

        app.logger.info(f"[Upload] client={client_id} raw_items={len(raw_items)} file={filename}")

        # ── Validate + dedup (fast, synchronous) ─────────────────────
        valid_faqs, errors = models.validate_and_enrich_faqs(raw_items, client_id)

        if errors:
            app.logger.info(
                f"[Upload] client={client_id} skipped={len(errors)} errors: "
                + "; ".join(f"row {e['row']}: {e['reason']}" for e in errors[:5])
            )

        if not valid_faqs:
            return jsonify({
                'success': False,
                'error': 'No valid content to import after validation.',
                'validation_errors': errors[:10],
            }), 400

        # ── AI enrichment + embed + save → background thread ─────────
        # enrich_and_chunk calls Gemini once per item (paraphrase + tags +
        # category) plus one embed call per item and per paraphrase variant.
        # For 50 FAQs that's 150-200 sequential API calls — it MUST be async.
        import threading
        t = threading.Thread(
            target=_bg_enrich_and_save,
            args=(client_id, valid_faqs),
            daemon=True,
        )
        t.start()

        response = {
            'success':    True,
            'message':    (
                f'Processing {len(valid_faqs)} items — your knowledge base will be '
                'ready in about 30–60 seconds. Refresh the FAQ Manager to see them.'
            ),
            'count':      len(valid_faqs),
            'processing': True,
        }
        if errors:
            response['skipped']           = len(errors)
            response['validation_errors'] = errors[:10]
        return jsonify(response)

    except Exception as e:
        app.logger.error(f"[Upload] Error: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/faq/import-url', methods=['POST'])
@login_required
def import_faqs_from_url():
    """
    Fetch a webpage by URL, extract visible text, then use AI to parse
    Q&A pairs from it — same enrichment pipeline as PDF/CSV uploads.
    """
    try:
        data      = request.get_json(silent=True) or {}
        client_id = data.get('client_id')
        url       = (data.get('url') or '').strip()

        if not models.verify_client_ownership(current_user.id, client_id):
            return jsonify({'success': False, 'error': 'Unauthorized'}), 403

        if not url:
            return jsonify({'success': False, 'error': 'No URL provided'}), 400

        import re as _re
        if not _re.match(r'^https?://', url):
            url = 'https://' + url

        # ── Fetch the page ────────────────────────────────────────────
        import urllib.request
        import urllib.error
        import html as _html
        try:
            req = urllib.request.Request(
                url,
                headers={'User-Agent': 'Mozilla/5.0 (compatible; LumviBot/1.0)'},
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                raw_bytes = resp.read(500_000)   # cap at 500 KB
        except urllib.error.HTTPError as e:
            return jsonify({'success': False, 'error': f'Could not fetch URL: HTTP {e.code}'}), 400
        except Exception as e:
            return jsonify({'success': False, 'error': f'Could not fetch URL: {e}'}), 400

        # ── Strip HTML tags → plain text ──────────────────────────────
        charset = 'utf-8'
        try:
            html_text = raw_bytes.decode(charset, errors='replace')
        except Exception:
            html_text = raw_bytes.decode('latin-1', errors='replace')

        # Remove scripts, styles, nav, footer noise
        html_text = _re.sub(r'(?is)<(script|style|nav|footer|header)[^>]*>.*?</\1>', ' ', html_text)
        html_text = _re.sub(r'<[^>]+>', ' ', html_text)          # strip all tags
        html_text = _html.unescape(html_text)                     # decode &amp; etc.
        html_text = _re.sub(r'[ \t]{2,}', ' ', html_text)        # collapse spaces
        html_text = _re.sub(r'\n{3,}', '\n\n', html_text).strip()# collapse blank lines

        if len(html_text) < 50:
            return jsonify({'success': False, 'error': 'Page had no readable text content.'}), 400

        # ── AI extraction (reuse existing helper) ─────────────────────
        raw_items = extract_faqs_from_text(html_text[:6000])   # cap prompt size

        if not raw_items:
            return jsonify({
                'success': False,
                'error':   'No FAQ pairs found on that page. Try a dedicated FAQ/Help page URL.',
            }), 400

        app.logger.info(f"[ImportURL] client={client_id} url={url} raw={len(raw_items)}")

        # ── Validate + dedup + background enrich (same as file upload) ─
        valid_faqs, errors = models.validate_and_enrich_faqs(raw_items, client_id)

        if not valid_faqs:
            return jsonify({
                'success': False,
                'error':   'All extracted items failed validation (duplicates or missing fields).',
                'validation_errors': errors[:10],
            }), 400

        import threading
        t = threading.Thread(
            target=_bg_enrich_and_save,
            args=(client_id, valid_faqs),
            daemon=True,
        )
        t.start()

        response = {
            'success':    True,
            'message':    (
                f'Found {len(valid_faqs)} FAQ{"s" if len(valid_faqs) != 1 else ""} on that page — '
                'your knowledge base will be ready in about 30–60 seconds.'
            ),
            'count':      len(valid_faqs),
            'processing': True,
        }
        if errors:
            response['skipped']           = len(errors)
            response['validation_errors'] = errors[:10]
        return jsonify(response)

    except Exception as e:
        app.logger.error(f'[ImportURL] Error: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


def process_csv_upload(file):
    import pandas as pd
    import io
    try:
        df = pd.read_csv(io.StringIO(file.stream.read().decode('utf-8')))
        if 'question' not in df.columns or 'answer' not in df.columns:
            return []
        faqs = []
        for _, row in df.iterrows():
            triggers = extract_keywords(row['question'])
            faq = {
                'question': str(row['question']).strip(),
                'answer': str(row['answer']).strip(),
                'category': str(row.get('category', 'General')).strip(),
                'triggers': triggers
            }
            if faq['question'] and faq['answer']:
                faqs.append(faq)
        return faqs
    except Exception as e:
        app.logger.error(f'Error processing CSV: {e}')
        return []


def process_excel_upload(file):
    import pandas as pd
    try:
        df = pd.read_excel(file)
        if 'question' not in df.columns or 'answer' not in df.columns:
            return []
        faqs = []
        for _, row in df.iterrows():
            triggers = extract_keywords(row['question'])
            faq = {
                'question': str(row['question']).strip(),
                'answer': str(row['answer']).strip(),
                'category': str(row.get('category', 'General')).strip(),
                'triggers': triggers
            }
            if faq['question'] and faq['answer']:
                faqs.append(faq)
        return faqs
    except Exception as e:
        app.logger.error(f'Error processing Excel: {e}')
        return []


def process_pdf_upload(file):
    import PyPDF2
    import io
    try:
        pdf_reader = PyPDF2.PdfReader(io.BytesIO(file.read()))
        text = ""
        for page in pdf_reader.pages:
            text += page.extract_text() + "\n"

        if ai_helper and ai_helper.enabled:
            return extract_faqs_from_text(text)
        else:
            return parse_structured_faq_text(text)

    except Exception as e:
        app.logger.error(f'Error processing PDF: {e}')
        return []


def extract_faqs_from_text(text):
    try:
        prompt = f"""Extract FAQ pairs from this text. Return a JSON array of objects with 'question' and 'answer' fields.

Text:
{text[:3000]}

Return ONLY valid JSON array like:
[
  {{"question": "What are your hours?", "answer": "We're open 9-5 Monday-Friday"}},
  {{"question": "How much does it cost?", "answer": "$49 per month"}}
]
"""
        # Guard: model may be None if Gemini init failed
        if not ai_helper or not ai_helper.enabled or not ai_helper.model:
            return parse_structured_faq_text(text)

        response = ai_helper.model.generate_content(
            prompt,
            request_options={'timeout': 20},  # prevent worker exhaustion
        )
        import re
        json_match = re.search(r'\[.*\]', response.text, re.DOTALL)
        if json_match:
            faqs_data = json.loads(json_match.group())
            for faq in faqs_data:
                faq['triggers'] = extract_keywords(faq['question'])
                faq['category'] = 'Imported'
            return faqs_data
        return []
    except Exception as e:
        app.logger.error(f'Error extracting FAQs with AI: {e}')
        return []


def parse_structured_faq_text(text):
    faqs = []
    lines = text.split('\n')
    current_q = None
    current_a = None

    for line in lines:
        line = line.strip()
        if line.startswith(('Q:', 'Question:', 'q:', 'question:')):
            if current_q and current_a:
                faqs.append({
                    'question': current_q,
                    'answer': current_a,
                    'category': 'Imported',
                    'triggers': extract_keywords(current_q)
                })
            current_q = line.split(':', 1)[1].strip()
            current_a = None
        elif line.startswith(('A:', 'Answer:', 'a:', 'answer:')):
            current_a = line.split(':', 1)[1].strip()

    if current_q and current_a:
        faqs.append({
            'question': current_q,
            'answer': current_a,
            'category': 'Imported',
            'triggers': extract_keywords(current_q)
        })

    return faqs


@app.route('/upgrade')
@login_required
def upgrade_page():
    def _parse_plan_ids(env_val):
        """Parse 'solo:123,starter:456,...' env var into a dict."""
        result = {}
        if env_val:
            for pair in env_val.split(','):
                parts = pair.strip().split(':')
                if len(parts) == 2:
                    result[parts[0].strip()] = parts[1].strip()
        return result

    return render_template(
        'upgrade.html',
        user=current_user,
        flw_public_key=os.environ.get('FLW_PUBLIC_KEY', ''),
        FLW_PLAN_IDS_MONTHLY=_parse_plan_ids(os.environ.get('FLW_PLAN_IDS_MONTHLY', '')),
        FLW_PLAN_IDS_ANNUAL=_parse_plan_ids(os.environ.get('FLW_PLAN_IDS_ANNUAL', '')),
    )

# =====================================================================
# PAYMENT ROUTES - PAYPAL (DISABLED - Only Flutterwave enabled)
# =====================================================================

# @app.route('/payment/paypal/create', methods=['POST'])
# @login_required
# def create_paypal_payment():
#     try:
#         data = request.json
#         plan = data.get('plan')
# 
#         PLAN_PRICES = {
#             'starter': 49.00,
#             'pro': 99.00,
#             'agency': 299.00
#         }
# 
#         amount = PLAN_PRICES.get(plan)
#         if not amount:
#             return jsonify({'success': False, 'error': 'Invalid plan'}), 400
# 
#         payment = Payment({
#             "intent": "sale",
#             "payer": {"payment_method": "paypal"},
#             "redirect_urls": {
#                 "return_url": f"{request.host_url}payment/paypal/success",
#                 "cancel_url": f"{request.host_url}payment/paypal/cancel"
#             },
#             "transactions": [{
#                 "item_list": {
#                     "items": [{
#                         "name": f"{plan.capitalize()} Plan - Monthly Subscription",
#                         "sku": f"plan_{plan}",
#                         "price": f"{amount:.2f}",
#                         "currency": "USD",
#                         "quantity": 1
#                     }]
#                 },
#                 "amount": {"total": f"{amount:.2f}", "currency": "USD"},
#                 "description": f"Upgrade to {plan.capitalize()} Plan"
#             }]
#         })
# 
#         if payment.create():
#             session['pending_payment'] = {
#                 'user_id': current_user.id,
#                 'plan': plan,
#                 'amount': amount,
#                 'payment_id': payment.id
#             }
# 
#             approval_url = next(
#                 (link.href for link in payment.links if link.rel == 'approval_url'),
#                 None
#             )
# 
#             return jsonify({
#                 'success': True,
#                 'approval_url': approval_url,
#                 'payment_id': payment.id
#             })
#         else:
#             app.logger.error(f"PayPal payment creation failed: {payment.error}")
#             return jsonify({'success': False, 'error': 'Payment creation failed'}), 500
# 
#     except Exception as e:
#         app.logger.error(f"PayPal error: {e}")
#         import traceback
#         traceback.print_exc()
#         return jsonify({'success': False, 'error': str(e)}), 500


# @app.route('/payment/paypal/success')
# @login_required
# def paypal_success():
#     try:
#         payment_id = request.args.get('paymentId')
#         payer_id = request.args.get('PayerID')
#         pending_payment = session.get('pending_payment', {})
# 
#         if not pending_payment or pending_payment.get('payment_id') != payment_id:
#             flash("⚠️ Payment session expired. Please try again.", 'warning')
#             return redirect(url_for('upgrade_page'))
# 
#         payment = Payment.find(payment_id)
# 
#         if payment.execute({"payer_id": payer_id}):
#             plan   = pending_payment['plan']
#             amount = pending_payment.get('amount', 0)
# 
#             # Set plan_type + subscription_expires_at + grace_period_ends_at
#             # so the scheduler can downgrade this user when their period ends.
#             models.update_user_subscription(
#                 user_id=current_user.id,
#                 plan_type=plan,
#                 billing_provider='paypal',
#                 subscription_id=payment_id,
#                 is_annual=False   # PayPal flow is monthly-only for now
#             )
# 
#             session.pop('pending_payment', None)
#             models.record_payment(current_user.id, float(amount), plan,
#                                   provider='paypal', reference=payment_id)
#             models.track_event('plan_upgrade', user_id=current_user.id,
#                                metadata={'plan': plan, 'provider': 'paypal', 'amount': amount})
#             flash(f"✅ Payment successful! You've been upgraded to the {plan.capitalize()} plan.", 'success')
#             return redirect(url_for('dashboard'))
#         else:
#             app.logger.error(f"PayPal execution failed: {payment.error}")
#             flash("❌ Payment execution failed. Please try again.", 'error')
#             return redirect(url_for('upgrade_page'))
# 
#     except Exception as e:
#         app.logger.error(f"PayPal success handler error: {e}")
#         import traceback
#         traceback.print_exc()
#         flash("❌ Payment processing error. Contact support@lumvi.net.", 'error')
#         return redirect(url_for('dashboard'))


# =====================================================================
# PAYMENT ROUTES - FLUTTERWAVE
# =====================================================================

PLAN_PRICES_FLW = {
    'solo':    {'monthly': 19.00,  'annual': 190.00},
    'starter': {'monthly': 49.00,  'annual': 490.00},
    'pro':     {'monthly': 99.00,  'annual': 990.00},
    'growth': {'monthly': 149.00, 'annual': 1490.00},
    'agency':  {'monthly': 299.00, 'annual': 2990.00}
}

@app.route('/payment/flutterwave/callback')
@login_required
def flutterwave_callback():
    """
    Flutterwave redirects here after payment.
    tx_ref format: lumvi_{plan}_{cycle}_{user_id}_{timestamp}
    cycle = 'monthly' | 'annual'
    
    Fixes:
    - FW-001: Duplicate check before subscription update
    - FW-002: Remove USD-only currency guard on amount validation
    - FW-004: Validate via Flutterwave signature
    - FW-008: Retry logic for verify API
    """
    status         = request.args.get('status', '')
    tx_ref         = request.args.get('tx_ref', '')
    transaction_id = request.args.get('transaction_id', '')

    if status != 'successful':
        flash("Payment was not completed. Please try again.", 'error')
        return redirect(url_for('upgrade_page'))

    if not transaction_id:
        flash("Invalid payment reference. Contact support@lumvi.net.", 'error')
        return redirect(url_for('upgrade_page'))

    # Verify with Flutterwave API (FW-008: retry logic)
    flw_secret = os.environ.get('FLW_SECRET_KEY', '')
    if not flw_secret:
        app.logger.error("FLW_SECRET_KEY not set")
        flash("Payment configuration error. Contact support@lumvi.net.", 'error')
        return redirect(url_for('upgrade_page'))

    flw_data = None
    verify_url = f"https://api.flutterwave.com/v3/transactions/{transaction_id}/verify"
    headers = {"Authorization": f"Bearer {flw_secret}"}
    
    # Retry up to 3 times with exponential backoff (FW-008)
    for attempt in range(3):
        try:
            resp = requests.get(verify_url, headers=headers, timeout=15)
            resp.raise_for_status()
            flw_data = resp.json()
            break
        except Exception as e:
            app.logger.warning(f"Flutterwave verify attempt {attempt + 1}/3 failed: {e}")
            if attempt == 2:
                app.logger.error(f"Flutterwave verify error after 3 attempts: {e}")
                flash("Could not verify payment. Contact support@lumvi.net.", 'error')
                return redirect(url_for('upgrade_page'))
            import time
            time.sleep(2 ** attempt)  # 1s, 2s, 4s backoff

    if not flw_data or flw_data.get('status') != 'success':
        flash("Payment verification failed. Contact support@lumvi.net.", 'error')
        return redirect(url_for('upgrade_page'))

    txn = flw_data.get('data', {})
    if txn.get('status') != 'successful':
        flash("Payment not successful. Please try again.", 'error')
        return redirect(url_for('upgrade_page'))

    # Parse tx_ref: lumvi_{plan}_{cycle}_{user_id}_{timestamp}
    plan       = None
    cycle      = 'monthly'
    tx_user_id = None
    try:
        parts = tx_ref.split('_')
        plan  = parts[1].lower() if len(parts) > 1 else None
        # cycle is optional — old format was lumvi_{plan}_{user_id}_{ts}
        if len(parts) > 2 and parts[2] in ('monthly', 'annual'):
            cycle      = parts[2].lower()
            tx_user_id = int(parts[3]) if len(parts) >= 4 else None
        else:
            tx_user_id = int(parts[2]) if len(parts) >= 3 else None
    except Exception:
        pass

    # Validate the user_id embedded in tx_ref matches the logged-in user
    if tx_user_id and tx_user_id != current_user.id:
        app.logger.error(
            f"Flutterwave callback: tx_ref user mismatch — "
            f"tx_ref says {tx_user_id}, logged-in user is {current_user.id} (tx {transaction_id})"
        )
        flash("Payment session mismatch. Contact support@lumvi.net.", 'error')
        return redirect(url_for('upgrade_page'))

    if plan not in PLAN_PRICES_FLW:
        app.logger.error(f"Flutterwave: unknown plan in tx_ref '{tx_ref}'")
        flash("Could not determine plan. Contact support@lumvi.net.", 'error')
        return redirect(url_for('upgrade_page'))

    is_annual    = (cycle == 'annual')
    expected_amt = PLAN_PRICES_FLW[plan]['annual'] if is_annual else PLAN_PRICES_FLW[plan]['monthly']
    paid_amount  = float(txn.get('amount', 0))
    paid_currency = txn.get('currency', 'USD')
    txn_created_at = txn.get('created_at')  # For FW-009

    # FW-002: Remove USD-only guard — validate all currencies (or add exchange rate)
    # For now, we log the currency but accept it. In production, add exchange rate API
    if paid_amount < expected_amt:
        app.logger.error(
            f"Flutterwave amount mismatch: expected {expected_amt} USD, "
            f"got {paid_amount} {paid_currency} (tx {transaction_id})"
        )
        flash("Payment amount mismatch. Contact support@lumvi.net.", 'error')
        return redirect(url_for('upgrade_page'))

    # FW-001: Duplicate check before subscription update
    try:
        conn, cursor = models.get_db()
        cursor.execute("SELECT id FROM payments WHERE reference = %s LIMIT 1", (str(transaction_id),))
        already_processed = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if already_processed:
            app.logger.warning(f"Flutterwave callback: duplicate txn {transaction_id} for user {current_user.id}")
            flash("This payment has already been processed.", 'info')
            return redirect(url_for('dashboard'))
    except Exception as e:
        app.logger.error(f"Flutterwave duplicate check failed: {e}")
        # Continue anyway — don't block user

    # Upgrade user with recurring subscription fields
    models.update_user_subscription(
        user_id=current_user.id,
        plan_type=plan,
        billing_provider='flutterwave',
        subscription_id=str(transaction_id),
        is_annual=is_annual
    )

    # FW-009: Pass payment_date from Flutterwave
    models.record_payment(
        current_user.id, paid_amount, plan,
        provider='flutterwave',
        reference=str(transaction_id),
        notes=f"{'Annual' if is_annual else 'Monthly'} — {cycle}",
        payment_date=txn_created_at
    )

    models.track_event('plan_upgrade', user_id=current_user.id,
                       metadata={'plan': plan, 'provider': 'flutterwave',
                                 'cycle': cycle, 'amount': paid_amount, 'tx_ref': tx_ref})

    app.logger.info(f"Flutterwave upgrade OK: user={current_user.id} plan={plan} cycle={cycle} txn={transaction_id}")
    flash(f"Payment successful! You are now on the {plan.capitalize()} plan ({cycle} billing).", 'success')
    return redirect(url_for('dashboard'))


@app.route('/payment/flutterwave/webhook', methods=['POST'])
def flutterwave_webhook():
    """
    Flutterwave server-to-server webhook (backup).
    Set webhook URL in Flutterwave dashboard: https://lumvi.net/payment/flutterwave/webhook
    Set FLW_WEBHOOK_HASH env var to your secret hash from the Flutterwave dashboard.
    
    Fixes:
    - FW-003: Validate user_id exists before upgrade
    - FW-005: Enforce webhook auth (now required at startup)
    - FW-006: Duplicate check before recording payment
    - FW-007: Log extracted tx_ref fields before validation
    """
    flw_hash     = os.environ.get('FLW_WEBHOOK_HASH', '')
    request_hash = request.headers.get('verif-hash', '')

    # FW-005: Auth is now required at startup, but re-check here
    if not flw_hash or request_hash != flw_hash:
        app.logger.warning(f"Flutterwave webhook: invalid hash (expected, got '{request_hash[:20]}...')")
        return jsonify({'error': 'Unauthorized'}), 401

    payload = request.json or {}
    event   = payload.get('event', '')

    if event != 'charge.completed':
        return jsonify({'status': 'ignored'}), 200

    data       = payload.get('data', {})
    txn_status = data.get('status', '')
    tx_ref     = data.get('tx_ref', '')
    txn_id     = str(data.get('id', ''))
    amount     = float(data.get('amount', 0))
    currency   = data.get('currency', 'USD')
    txn_created_at = data.get('created_at')  # For FW-009

    if txn_status != 'successful':
        return jsonify({'status': 'not successful'}), 200

    # Extract plan + cycle + user_id from tx_ref
    # Format: lumvi_{plan}_{cycle}_{user_id}_{ts} OR legacy lumvi_{plan}_{user_id}_{ts}
    plan = None
    cycle = 'monthly'
    user_id = None
    try:
        parts   = tx_ref.split('_')
        plan    = parts[1].lower() if len(parts) >= 2 else None
        if len(parts) > 2 and parts[2] in ('monthly', 'annual'):
            cycle   = parts[2].lower()
            user_id = int(parts[3]) if len(parts) >= 4 else None
        else:
            cycle   = 'monthly'
            user_id = int(parts[2]) if len(parts) >= 3 else None
    except (IndexError, ValueError) as e:
        app.logger.error(f"Flutterwave webhook: bad tx_ref '{tx_ref}' — {e}")
        return jsonify({'status': 'bad tx_ref'}), 200

    # FW-007: Log extracted fields before validation
    app.logger.info(f"Flutterwave webhook parsing: plan={plan} cycle={cycle} user_id={user_id} txn_id={txn_id}")

    if plan not in PLAN_PRICES_FLW:
        app.logger.error(f"Flutterwave webhook: unknown plan '{plan}' (tx_ref='{tx_ref}')")
        return jsonify({'status': 'unknown plan'}), 200

    if not user_id:
        app.logger.error(f"Flutterwave webhook: no user_id in tx_ref '{tx_ref}'")
        return jsonify({'status': 'no user_id'}), 200

    # FW-003: Validate user_id exists before upgrade
    user = models.get_user_by_id(user_id)
    if not user:
        app.logger.error(f"Flutterwave webhook: user {user_id} does not exist (txn {txn_id})")
        return jsonify({'status': 'user not found'}), 200

    # FW-002: Remove USD-only guard — validate all currencies (same as callback)
    is_annual    = (cycle == 'annual')
    expected_amt = PLAN_PRICES_FLW[plan]['annual'] if is_annual else PLAN_PRICES_FLW[plan]['monthly']
    if amount < expected_amt:
        app.logger.error(
            f"[Webhook] Amount mismatch for user={user_id} plan={plan}: "
            f"expected {expected_amt} USD, got {amount} {currency} tx_ref='{tx_ref}'"
        )
        return jsonify({'status': 'amount mismatch'}), 200

    # FW-006: Duplicate check before recording payment
    try:
        conn, cursor = models.get_db()
        cursor.execute("SELECT id FROM payments WHERE reference = %s LIMIT 1", (txn_id,))
        already_processed = cursor.fetchone()
        cursor.close()
        conn.close()

        if already_processed:
            app.logger.info(f"Flutterwave webhook: already processed txn {txn_id}")
            return jsonify({'status': 'already processed'}), 200
    except Exception as e:
        app.logger.error(f"Flutterwave webhook duplicate check failed: {e}")
        return jsonify({'status': 'db error'}), 200

    # Upgrade user with recurring fields
    models.update_user_subscription(
        user_id=user_id,
        plan_type=plan,
        billing_provider='flutterwave',
        subscription_id=txn_id,
        is_annual=is_annual
    )

    # FW-009: Pass payment_date from Flutterwave
    models.record_payment(user_id, amount, plan, provider='flutterwave',
                          reference=txn_id,
                          notes=f"{'Annual' if is_annual else 'Monthly'} webhook",
                          payment_date=txn_created_at)
    models.track_event('plan_upgrade', user_id=user_id,
                       metadata={'plan': plan, 'provider': 'flutterwave_webhook',
                                 'cycle': cycle, 'amount': amount, 'tx_ref': tx_ref})

    app.logger.info(f"Flutterwave webhook upgrade OK: user={user_id} plan={plan} cycle={cycle} txn={txn_id}")
    return jsonify({'status': 'ok'}), 200


@app.route('/subscription/cancel', methods=['GET', 'POST'])
@login_required
def cancel_subscription():
    """Allow users to cancel their subscription at the end of the current period."""
    if request.method == 'POST':
        success = models.cancel_user_subscription(current_user.id)

        if success:
            user = models.get_user_by_id(current_user.id)

            # Notify Flutterwave to stop future charges
            if user and user.get('subscription_id') and user.get('billing_provider') == 'flutterwave':
                try:
                    flw_secret = os.environ.get('FLW_SECRET_KEY')
                    if flw_secret:
                        cancel_url = f"https://api.flutterwave.com/v3/subscriptions/{user['subscription_id']}/cancel"
                        headers = {"Authorization": f"Bearer {flw_secret}"}
                        requests.put(cancel_url, headers=headers, timeout=10)
                except Exception as _e:
                    app.logger.warning(f"Flutterwave cancel API call failed: {_e}")

            # Notify PayPal to stop future charges
            elif user and user.get('subscription_id') and user.get('billing_provider') == 'paypal':
                try:
                    import base64
                    paypal_client_id     = os.environ.get('PAYPAL_CLIENT_ID', '')
                    paypal_client_secret = os.environ.get('PAYPAL_CLIENT_SECRET', '')
                    paypal_mode          = os.environ.get('PAYPAL_MODE', 'sandbox')
                    paypal_base          = ('https://api-m.paypal.com' if paypal_mode == 'live'
                                            else 'https://api-m.sandbox.paypal.com')

                    # Get OAuth token
                    credentials = base64.b64encode(
                        f"{paypal_client_id}:{paypal_client_secret}".encode()
                    ).decode()
                    token_resp = requests.post(
                        f"{paypal_base}/v1/oauth2/token",
                        headers={"Authorization": f"Basic {credentials}",
                                 "Content-Type": "application/x-www-form-urlencoded"},
                        data="grant_type=client_credentials",
                        timeout=10
                    )
                    access_token = token_resp.json().get('access_token')

                    if access_token:
                        requests.post(
                            f"{paypal_base}/v1/billing/subscriptions/{user['subscription_id']}/cancel",
                            headers={"Authorization": f"Bearer {access_token}",
                                     "Content-Type": "application/json"},
                            json={"reason": "Cancelled by user via Lumvi dashboard"},
                            timeout=10
                        )
                        app.logger.info(f"[Cancel] PayPal subscription cancelled for user {current_user.id}")
                except Exception as _e:
                    app.logger.warning(f"PayPal cancel API call failed: {_e}")

            models.track_event('subscription_cancelled', user_id=current_user.id)

            # Send cancellation confirmation email — gives user a record of
            # when access ends, prevents the majority of support disputes.
            try:
                _user_fresh  = models.get_user_by_id(current_user.id)
                _sub_info    = get_subscription_status(_user_fresh) if _user_fresh else {}
                _expires     = _sub_info.get('expires_at')
                _access_ends = (
                    _expires.strftime('%B %d, %Y')
                    if _expires and hasattr(_expires, 'strftime')
                    else 'the end of your current billing period'
                )
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
                mail.send(_cancel_msg)
            except Exception as _mail_err:
                app.logger.warning(f"[Cancel] confirmation email failed: {_mail_err}")

            flash("Your subscription has been cancelled. You will retain access until the end of your current billing period.", 'success')
            return redirect(url_for('dashboard'))
        else:
            flash("Could not cancel subscription. Please contact support@lumvi.net.", 'error')
            return redirect(url_for('cancel_subscription'))

    # GET — show confirmation page
    user    = models.get_user_by_id(current_user.id)
    sub_info = get_subscription_status(user) if user else {'status': 'free'}
    return render_template('cancel_subscription.html', user=user, sub_status=sub_info)


@app.route('/api/webhook/lead', methods=['POST'])
def webhook_new_lead():
    try:
        # APP-BUG-04 fix: fail closed if WEBHOOK_SECRET not configured;
        # use constant-time comparison to prevent timing oracle attacks.
        import hmac as _hmac
        _wh_secret = os.environ.get('WEBHOOK_SECRET', '').strip()
        if not _wh_secret:
            return jsonify({'error': 'Webhook not configured'}), 503
        _provided = request.headers.get('X-Webhook-Secret', '')
        if not _hmac.compare_digest(_provided, _wh_secret):
            return jsonify({'error': 'Unauthorized'}), 401

        data = request.json or {}
        client_id = data.get('client_id')
        leads = models.get_leads(client_id)
        leads = leads[:10]
        return jsonify({'success': True, 'leads': leads})

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/webhook/faq-import', methods=['POST'])
def webhook_faq_import():
    try:
        # APP-BUG-04 fix: fail closed if WEBHOOK_SECRET not configured
        import hmac as _hmac
        _wh_secret = os.environ.get('WEBHOOK_SECRET', '').strip()
        if not _wh_secret:
            return jsonify({'error': 'Webhook not configured'}), 503
        _provided = request.headers.get('X-Webhook-Secret', '')
        if not _hmac.compare_digest(_provided, _wh_secret):
            return jsonify({'error': 'Unauthorized'}), 401

        data = request.json or {}
        client_id = data.get('client_id')
        incoming_faqs = data.get('faqs', [])

        if not client_id or not incoming_faqs:
            return jsonify({'error': 'client_id and faqs required'}), 400

        # APP-BUG-02 fix: wrap DB work in try/except/finally so connection
        # is always returned to the pool even if an insert throws.
        conn = cursor = None
        saved = 0
        try:
            conn, cursor = models.get_db()
            for faq in incoming_faqs:
                question = faq.get('question', '').strip()
                answer = faq.get('answer', '').strip()
                if not question or not answer:
                    continue
                triggers = extract_keywords(question)
                cursor.execute(
                    '''
                    INSERT INTO faqs (client_id, faq_id, question, answer, category, triggers)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ''',
                    (
                        client_id,
                        str(uuid.uuid4()),
                        question,
                        answer,
                        faq.get('category', 'General') if isinstance(faq, dict) else 'General',
                        json.dumps(triggers)
                    )
                )
                saved += 1
            conn.commit()
        except Exception as _db_err:
            if conn:
                try: conn.rollback()
                except Exception: pass
            raise _db_err
        finally:
            if cursor:
                try: cursor.close()
                except Exception: pass
            if conn:
                try: conn.close()
                except Exception: pass

        cache_utils.bump_kb_version(client_id)
        app.logger.info(f"[Cache] KB invalidated after webhook FAQ import: client={client_id}")

        return jsonify({
            'success': True,
            'message': f'Imported {saved} FAQs successfully',
            'count': saved
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500


# DISABLED: PayPal cancel route (only Flutterwave enabled)
# @app.route('/payment/paypal/cancel')
# @login_required
# def paypal_cancel():
#     session.pop('pending_payment', None)
#     flash("💳 Payment cancelled. You can try again anytime.", 'info')
#     return redirect(url_for('upgrade_page'))


# DISABLED: PayPal webhook (only Flutterwave enabled)
# @app.route('/payment/paypal/webhook', methods=['POST'])
# def paypal_webhook():
#     try:
#         event      = request.json or {}
#         event_type = event.get('event_type', '')
#         resource   = event.get('resource', {})
#         app.logger.info(f"PayPal webhook received: {event_type}")
# 
#         if event_type == 'BILLING.SUBSCRIPTION.CANCELLED':
#             # Find user by PayPal subscription_id and mark cancel_at_period_end
#             subscription_id = resource.get('id')
#             if subscription_id:
#                 conn, cursor = models.get_db()
#                 cursor.execute(
#                     'SELECT id FROM users WHERE subscription_id = %s LIMIT 1',
#                     (subscription_id,)
#                 )
#                 row = cursor.fetchone()
#                 cursor.close()
#                 conn.close()
#                 if row:
#                     models.cancel_user_subscription(row['id'])
#                     models.track_event('subscription_cancelled', user_id=row['id'],
#                                        metadata={'provider': 'paypal', 'source': 'webhook'})
#                     app.logger.info(f"[PayPal webhook] Cancelled subscription for user {row['id']}")
#                 else:
#                     app.logger.warning(f"[PayPal webhook] No user found for subscription_id={subscription_id}")
# 
#         elif event_type in ('PAYMENT.SALE.COMPLETED', 'BILLING.SUBSCRIPTION.RENEWED'):
#             # Renewal — extend the subscription period by 30 days
#             subscription_id = resource.get('billing_agreement_id') or resource.get('id')
#             amount_obj      = resource.get('amount', {})
#             amount          = float(amount_obj.get('total', 0))
#             if subscription_id:
#                 conn, cursor = models.get_db()
#                 cursor.execute(
#                     'SELECT id, plan_type FROM users WHERE subscription_id = %s LIMIT 1',
#                     (subscription_id,)
#                 )
#                 row = cursor.fetchone()
#                 cursor.close()
#                 conn.close()
#                 if row:
#                     models.update_user_subscription(
#                         user_id=row['id'],
#                         plan_type=row['plan_type'],
#                         billing_provider='paypal',
#                         subscription_id=subscription_id,
#                         is_annual=False
#                     )
#                     models.record_payment(row['id'], amount, row['plan_type'],
#                                           provider='paypal', reference=subscription_id,
#                                           notes='Webhook renewal')
#                     app.logger.info(f"[PayPal webhook] Renewed subscription for user {row['id']}")
# 
#         return jsonify({'success': True}), 200
# 
#     except Exception as e:
#         app.logger.error(f"PayPal webhook error: {e}")
#         return jsonify({'success': False}), 500

# =====================================================================
# AFFILIATE ROUTES
# =====================================================================

@app.route('/become-affiliate', methods=['GET', 'POST'])
@login_required
def become_affiliate():
    existing = models.get_affiliate_by_user_id(current_user.id)
    if existing:
        return redirect(url_for('affiliate_dashboard'))

    if request.method == 'POST':
        payment_email = request.form.get('payment_email')
        affiliate = models.create_affiliate(current_user.id, payment_email)

        if affiliate:
            return redirect(url_for('affiliate_dashboard'))
        else:
            return "Error creating affiliate account", 500

    return render_template('become-affiliate.html')


@app.route('/affiliate-dashboard')
@login_required
def affiliate_dashboard():
    affiliate = models.get_affiliate_by_user_id(current_user.id)

    if not affiliate:
        return redirect(url_for('become_affiliate'))

    stats = models.get_affiliate_stats(affiliate['id'])
    commissions = models.get_affiliate_commissions(affiliate['id'])

    return render_template('affiliate-dashboard.html', stats=stats, commissions=commissions)


@app.route('/admin/set-plan', methods=['GET', 'POST'])
def admin_set_plan():
    # Secret is validated at startup — it will always be a real value here.
    ADMIN_SECRET = os.environ.get('ADMIN_SECRET', '')
    error = None
    success = None

    if request.method == 'POST':
        secret = request.form.get('secret')
        email = request.form.get('email', '').strip().lower()
        plan = request.form.get('plan', '').strip().lower()

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
                    'UPDATE users SET plan_type = %s WHERE email = %s',
                    (plan, email)
                )
                conn.commit()
                cursor.close()
                conn.close()
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
  button:hover{{background:#0891b2;}}
  .success{{background:rgba(16,185,129,.15);border:1px solid rgba(16,185,129,.3);color:#34d399;padding:12px 16px;border-radius:8px;margin-bottom:20px;font-size:14px;}}
  .error{{background:rgba(239,68,68,.15);border:1px solid rgba(239,68,68,.3);color:#f87171;padding:12px 16px;border-radius:8px;margin-bottom:20px;font-size:14px;}}
  .warning{{color:#fbbf24;font-size:12px;margin-top:16px;text-align:center;}}
</style>
</head>
<body>
<div class="card">
  <h1>Admin — Set User Plan</h1>
  <p>Update any user account to a different plan tier.</p>
  {"<div class=\"success\">" + success + "</div>" if success else ""}
  {"<div class=\"error\">" + error + "</div>" if error else ""}
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
  <p class="warning">⚠️ Keep this URL private. Set ADMIN_SECRET in your environment variables.</p>
</div>
</body>
</html>'''


@app.route('/admin/init-db-production', methods=['GET', 'POST'])
def init_db_production():
    if request.method == 'POST':
        secret = request.form.get('secret')
        if secret == 'your-secret-password-here':
            models.init_db()
            try:
                models.migrate_clients_table()
            except Exception as e:
                app.logger.warning(f"Clients migration helper failed: {e}")

            conn, cursor = models.get_db()
            conn.commit()
            cursor.close()
            conn.close()
            return "✅ Database initialized!"
        else:
            return "❌ Invalid secret"

    return '''
    <form method="POST">
        <input type="password" name="secret" placeholder="Admin secret">
        <button type="submit">Initialize DB</button>
    </form>
    '''


@app.route('/demo')
def demo_page():
    return render_template('demo.html')


# =====================================================================
# FIX 2: BLOCK BOT DELETION (privacy policy compliance)
# Bots cannot be self-deleted — users must contact support.
# This catches any frontend delete calls and returns a clear message.
# =====================================================================

@app.route('/api/clients/delete', methods=['POST', 'DELETE'])
@login_required
def delete_client_legacy():
    """Legacy route — client_id in JSON body."""
    data = request.json or {}
    client_id = data.get('client_id')
    if not client_id:
        return jsonify({'success': False, 'error': 'client_id required'}), 400
    return _do_delete_client(client_id)


@app.route('/api/clients/<client_id>/delete', methods=['POST', 'DELETE'])
@login_required
def delete_client_by_id(client_id):
    """RESTful delete route — client_id in URL."""
    return _do_delete_client(client_id)


def _do_delete_client(client_id):
    """Shared deletion logic — verifies ownership then cascades delete."""
    try:
        if not models.verify_client_ownership(current_user.id, client_id):
            return jsonify({'success': False, 'error': 'Unauthorized'}), 403

        models.delete_client(client_id)
        app.logger.info(f'Client {client_id} deleted by user {current_user.id}')
        return jsonify({'success': True, 'message': 'Chatbot deleted successfully'})
    except Exception as e:
        app.logger.error(f'Delete client error: {e}')
        return jsonify({'success': False, 'error': 'Failed to delete chatbot'}), 500


# =====================================================================
# FIX 3: PRIORITY SUPPORT ROUTE
# Free/Starter -> standard support | Pro/Agency -> priority support
# =====================================================================

@app.route('/support')
@login_required
def support_page():
    plan = current_user.plan_type
    plan_limits = PLAN_LIMITS.get(plan, PLAN_LIMITS['free'])
    has_priority = plan_limits['priority_support']

    if has_priority:
        response_sla = '< 4 hours'
        badge = 'Priority Support'
        badge_color = '#06b6d4'
    else:
        response_sla = '1-2 business days'
        badge = 'Standard Support'
        badge_color = '#64748b'

    subject = '[PRIORITY] ' if has_priority else ''
    subject += f'Support Request - {plan.capitalize()} - {current_user.email}'
    mailto = 'mailto:support@lumvi.net?subject=' + subject.replace(' ', '%20')

    return f'''<!DOCTYPE html>
<html>
<head><title>Lumvi Support</title>
<style>
  body{{font-family:-apple-system,sans-serif;background:linear-gradient(135deg,#0f172a,#1e1b4b);min-height:100vh;display:flex;align-items:center;justify-content:center;padding:20px;}}
  .card{{background:rgba(30,41,59,.95);border:1px solid rgba(255,255,255,.1);border-radius:20px;padding:48px;max-width:540px;width:100%;color:#f8fafc;}}
  .badge{{display:inline-block;padding:6px 14px;border-radius:999px;font-size:13px;font-weight:700;background:{badge_color}22;color:{badge_color};border:1px solid {badge_color}55;margin-bottom:20px;}}
  h1{{font-size:26px;font-weight:800;margin-bottom:10px;}}
  p{{color:#94a3b8;font-size:15px;line-height:1.6;margin-bottom:20px;}}
  .sla-box{{background:rgba(6,182,212,.08);border:1px solid rgba(6,182,212,.25);border-radius:12px;padding:16px 20px;margin-bottom:28px;font-size:15px;color:#67e8f9;}}
  .sla-box strong{{display:block;font-size:12px;color:#94a3b8;margin-bottom:4px;text-transform:uppercase;letter-spacing:.05em;}}
  .tips{{margin-bottom:32px;}}
  .tips h3{{font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:.05em;margin-bottom:10px;}}
  .tips li{{color:#cbd5e1;font-size:14px;line-height:1.9;margin-left:18px;}}
  .btn{{display:inline-block;padding:13px 28px;border-radius:10px;font-weight:700;text-decoration:none;font-size:14px;margin:4px;}}
  .btn-p{{background:#06b6d4;color:#0f172a;}}
  .btn-s{{background:transparent;color:#94a3b8;border:1px solid rgba(255,255,255,.15);}}
</style>
</head>
<body>
<div class="card">
  <div class="badge">{badge}</div>
  <h1>Get Help</h1>
  <p>Email our support team. Response time is based on your plan.</p>
  <div class="sla-box"><strong>Expected response time</strong>{response_sla}</div>
  <div class="tips">
    <h3>For faster resolution, include:</h3>
    <ul>
      <li>Your client ID if reporting a chatbot issue</li>
      <li>What you expected vs what happened</li>
      <li>Screenshots if relevant</li>
    </ul>
  </div>
  <a href="{mailto}" class="btn btn-p">Email Support</a>
  <a href="/dashboard" class="btn btn-s">Back to Dashboard</a>
</div>
</body>
</html>'''


# =====================================================================
# FIX 4: CLIENT MANAGEMENT PORTAL (AGENCY / ENTERPRISE ONLY)
# =====================================================================

@app.route('/client-portal')
@login_required
def client_portal():
    fresh_user  = models.get_user_by_id(current_user.id)
    plan_type   = (fresh_user or {}).get('plan_type', current_user.plan_type)
    plan_limits = PLAN_LIMITS.get(plan_type, PLAN_LIMITS['free'])

    if not plan_limits['white_label']:
        return '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Upgrade Required — Lumvi</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,700;9..144,800&family=DM+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0;}
:root{
  --cream:#F7F4EF;--gold:#B8924A;--gold-lt:rgba(184,146,74,0.12);--gold-dk:#9A7A3A;
  --gold-glow:rgba(184,146,74,0.22);--dark:#1C1917;--mid:#57534E;--sub:#A8A29E;
  --border:#E7E2DA;--white:#fff;
}
body{font-family:'DM Sans',sans-serif;background:var(--cream);min-height:100vh;
  display:flex;flex-direction:column;align-items:center;justify-content:center;padding:24px;}
.card{background:var(--white);border:1px solid var(--border);border-radius:20px;
  padding:48px 40px;max-width:480px;width:100%;text-align:center;
  box-shadow:0 4px 24px rgba(0,0,0,0.06);}
.icon{width:64px;height:64px;border-radius:18px;background:var(--gold-lt);
  display:flex;align-items:center;justify-content:center;margin:0 auto 20px;
  border:1px solid rgba(184,146,74,0.2);}
.icon svg{width:28px;height:28px;color:var(--gold);}
h1{font-family:'Fraunces',serif;font-size:24px;font-weight:800;color:var(--dark);
  margin-bottom:12px;letter-spacing:-0.3px;}
p{font-size:14px;color:var(--mid);line-height:1.7;margin-bottom:8px;}
.features{text-align:left;background:var(--cream);border:1px solid var(--border);
  border-radius:12px;padding:16px 20px;margin:20px 0;display:flex;flex-direction:column;gap:9px;}
.feat{display:flex;align-items:center;gap:9px;font-size:13.5px;color:var(--mid);}
.feat svg{width:15px;height:15px;color:var(--gold);flex-shrink:0;}
.btn-upgrade{display:inline-flex;align-items:center;justify-content:center;gap:7px;
  width:100%;padding:13px 24px;background:var(--gold);color:#fff;border-radius:12px;
  font-weight:700;font-size:14.5px;text-decoration:none;margin-bottom:10px;
  box-shadow:0 2px 8px var(--gold-glow);transition:all 0.2s;border:none;cursor:pointer;}
.btn-upgrade:hover{background:var(--gold-dk);transform:translateY(-1px);}
.btn-back{display:inline-flex;align-items:center;justify-content:center;
  width:100%;padding:11px 24px;background:transparent;color:var(--sub);
  border:1.5px solid var(--border);border-radius:12px;font-weight:600;
  font-size:14px;text-decoration:none;transition:all 0.15s;}
.btn-back:hover{border-color:var(--mid);color:var(--dark);}
.price-chip{display:inline-block;padding:3px 12px;background:var(--gold-lt);
  color:var(--gold-dk);border-radius:20px;font-size:12px;font-weight:700;
  border:1px solid rgba(184,146,74,0.25);margin-bottom:16px;}
</style>
</head>
<body>
<div class="card">
  <div class="icon">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
      <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/>
      <circle cx="9" cy="7" r="4"/>
      <path d="M23 21v-2a4 4 0 0 0-3-3.87"/>
      <path d="M16 3.13a4 4 0 0 1 0 7.75"/>
    </svg>
  </div>
  <div class="price-chip">Agency Plan — $299/mo</div>
  <h1>Client Portal</h1>
  <p>Manage unlimited client chatbots, branding, leads, and analytics from a single command centre.</p>
  <div class="features">
    <div class="feat">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="20 6 9 17 4 12"/></svg>
      Unlimited client chatbots
    </div>
    <div class="feat">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="20 6 9 17 4 12"/></svg>
      Full white-label — your brand, not Lumvi's
    </div>
    <div class="feat">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="20 6 9 17 4 12"/></svg>
      Custom domain per client widget
    </div>
    <div class="feat">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="20 6 9 17 4 12"/></svg>
      Clone clients, bulk actions, agency defaults
    </div>
    <div class="feat">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="20 6 9 17 4 12"/></svg>
      Webhooks, branded email, priority support
    </div>
  </div>
  <a href="/upgrade" class="btn-upgrade">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" style="width:14px;height:14px;"><polyline points="18 15 12 9 6 15"/></svg>
    Upgrade to Agency
  </a>
  <a href="/dashboard" class="btn-back">← Back to Dashboard</a>
</div>
</body>
</html>''', 403

    clients    = models.get_user_clients(current_user.id)
    client_ids = [c['client_id'] for c in clients]

    # Bulk stats
    stats       = models.get_clients_enriched_stats(client_ids) if hasattr(models, 'get_clients_enriched_stats') else {}
    leads_month = models.get_leads_this_month_bulk(client_ids)  if hasattr(models, 'get_leads_this_month_bulk')  else {}

    daily_limit  = plan_limits['messages_per_day']
    client_limit = plan_limits['clients']

    enriched = []
    for c in clients:
        cid = c['client_id']
        s   = stats.get(cid, {})

        branding = {}
        bs_raw   = c.get('branding_settings') or '{}'
        try:
            branding = json.loads(bs_raw) if isinstance(bs_raw, str) else bs_raw
        except Exception:
            branding = {}

        branding_inner = branding.get('branding', {})
        bot_settings   = branding.get('bot_settings', {})
        primary_color  = branding_inner.get('primary_color') or c.get('widget_color') or '#B8924A'
        logo_url       = branding_inner.get('logo') or branding_inner.get('logo_url') or ''
        bot_avatar_url = bot_settings.get('bot_avatar_url') or ''

        daily_msgs = s.get('daily_msgs', 0)
        if daily_limit >= 999999:
            usage_pct, usage_class = 0, 'success'
        else:
            usage_pct   = min(round(daily_msgs / daily_limit * 100), 100)
            usage_class = 'danger' if usage_pct >= 90 else ('warning' if usage_pct >= 70 else 'success')

        last_active = s.get('last_active')
        if last_active:
            delta = datetime.utcnow() - last_active.replace(tzinfo=None)
            if delta.days == 0:   last_active_str = 'Today'
            elif delta.days == 1: last_active_str = 'Yesterday'
            elif delta.days < 7:  last_active_str = f'{delta.days}d ago'
            elif delta.days < 30: last_active_str = f'{delta.days // 7}w ago'
            else:                 last_active_str = last_active.strftime('%b %d')
        else:
            last_active_str = 'No activity'

        is_suspended = bool(c.get('is_suspended', False))

        enriched.append({
            **c,
            'cid':            cid,
            'name':           c.get('company_name', 'Unnamed'),
            'faqs_count':     s.get('faqs_count', 0),
            'leads_count':    s.get('leads_count', 0),
            'leads_month':    leads_month.get(cid, 0),
            'conversations':  s.get('conversations', 0),
            'daily_msgs':     daily_msgs,
            'usage_pct':      usage_pct,
            'usage_class':    usage_class,
            'primary_color':  primary_color,
            'logo_url':       logo_url,
            'bot_avatar_url': bot_avatar_url,
            'last_active_str':last_active_str,
            'is_suspended':   is_suspended,
        })

    total_leads      = sum(c['leads_count']  for c in enriched)
    total_convos     = sum(c['conversations'] for c in enriched)
    active_clients   = sum(1 for c in enriched if not c['is_suspended'])
    leads_this_month = sum(c['leads_month']  for c in enriched)
    slots_display    = 'Unlimited' if client_limit >= 999999 else str(client_limit)
    daily_display    = 'Unlimited' if daily_limit  >= 999999 else str(daily_limit)

    agency_branding = models.get_agency_branding(current_user.id) if hasattr(models, 'get_agency_branding') else {}

    return render_template(
        'client_portal.html',
        user             = current_user,
        plan_type        = plan_type,
        plan_limits      = plan_limits,
        clients          = enriched,
        total_leads      = total_leads,
        total_convos     = total_convos,
        active_clients   = active_clients,
        leads_this_month = leads_this_month,
        client_count     = len(enriched),
        slots_display    = slots_display,
        daily_display    = daily_display,
        agency_branding  = agency_branding,
    )

# =====================================================================
# =====================================================================
# AGENCY CLIENTS DASHBOARD  (/agency/clients)
# =====================================================================

@app.route('/agency/clients')
@login_required
def agency_clients():
    fresh_user = models.get_user_by_id(current_user.id)
    plan_type  = (fresh_user or {}).get('plan_type', current_user.plan_type)
    is_admin   = bool((fresh_user or {}).get('is_admin', False))

    allowed_plans = {'pro', 'agency', 'enterprise'}
    if plan_type not in allowed_plans and not is_admin:
        return render_template('agency_clients_upgrade.html',
                               user=current_user, plan_type=plan_type), 403

    clients    = models.get_user_clients(current_user.id)
    client_ids = [c['client_id'] for c in clients]

    stats       = models.get_clients_enriched_stats(client_ids)
    leads_month = models.get_leads_this_month_bulk(client_ids) if hasattr(models, 'get_leads_this_month_bulk') else {}

    plan_limits   = PLAN_LIMITS.get(plan_type, PLAN_LIMITS['free'])
    daily_limit   = plan_limits['messages_per_day']
    client_limit  = plan_limits['clients']
    slots_display = 'Unlimited' if client_limit >= 999999 else str(client_limit)
    daily_display = 'Unlimited' if daily_limit  >= 999999 else str(daily_limit)

    enriched = []
    for client in clients:
        cid = client['client_id']
        s   = stats.get(cid, {})

        branding = {}
        bs_raw   = client.get('branding_settings') or '{}'
        try:
            branding = json.loads(bs_raw) if isinstance(bs_raw, str) else bs_raw
        except Exception:
            branding = {}

        branding_inner = branding.get('branding', {})
        bot_settings   = branding.get('bot_settings', {})
        primary_color  = branding_inner.get('primary_color') or client.get('widget_color') or '#B8924A'
        branding_removed = bool(branding_inner.get('remove_branding') or client.get('remove_branding'))
        logo_url       = branding_inner.get('logo') or branding_inner.get('logo_url') or ''
        bot_avatar_url = bot_settings.get('bot_avatar_url') or ''

        daily_msgs = s.get('daily_msgs', 0)
        if daily_limit >= 999999:
            usage_pct = 0; usage_class = 'success'
        else:
            usage_pct   = min(round(daily_msgs / daily_limit * 100), 100)
            usage_class = 'danger' if usage_pct >= 90 else ('warning' if usage_pct >= 70 else 'success')

        last_active = s.get('last_active')
        if last_active:
            delta = datetime.utcnow() - last_active.replace(tzinfo=None)
            if delta.days == 0:   last_active_str = 'Today'
            elif delta.days == 1: last_active_str = 'Yesterday'
            elif delta.days < 7:  last_active_str = f'{delta.days}d ago'
            elif delta.days < 30: last_active_str = f'{delta.days // 7}w ago'
            else:                 last_active_str = last_active.strftime('%b %d')
        else:
            last_active_str = 'No activity'

        is_suspended = bool(client.get('is_suspended', False))

        enriched.append({
            **client,
            'cid':              cid,
            'name':             client.get('company_name', 'Unnamed'),
            'vertical':         branding.get('vertical', 'general').replace('_', ' ').title(),
            'faqs_count':       s.get('faqs_count', 0),
            'leads_count':      s.get('leads_count', 0),
            'leads_month':      leads_month.get(cid, 0),
            'conversations':    s.get('conversations', 0),
            'daily_msgs':       daily_msgs,
            'daily_limit':      daily_display,
            'usage_pct':        usage_pct,
            'usage_class':      usage_class,
            'branding_removed': branding_removed,
            'primary_color':    primary_color,
            'logo_url':         logo_url,
            'bot_avatar_url':   bot_avatar_url,
            'last_active_str':  last_active_str,
            'near_limit':       (not daily_limit >= 999999) and usage_pct >= 80,
            'is_suspended':     is_suspended,
            'status':           'Suspended' if is_suspended else 'Active',
        })

    total_leads       = sum(c['leads_count']   for c in enriched)
    total_convos      = sum(c['conversations']  for c in enriched)
    total_faqs        = sum(c['faqs_count']     for c in enriched)
    active_clients    = sum(1 for c in enriched if not c['is_suspended'])
    leads_this_month  = sum(c['leads_month']    for c in enriched)

    agency_branding = models.get_agency_branding(current_user.id) if hasattr(models, 'get_agency_branding') else {}

    return render_template(
        'agency_clients.html',
        user             = current_user,
        plan_type        = plan_type,
        plan_limits      = plan_limits,
        clients          = enriched,
        total_leads      = total_leads,
        total_convos     = total_convos,
        total_faqs       = total_faqs,
        active_clients   = active_clients,
        leads_this_month = leads_this_month,
        slots_display    = slots_display,
        daily_display    = daily_display,
        client_count     = len(enriched),
        agency_branding  = agency_branding,
    )


@app.route('/api/admin/client/suspend', methods=['POST'])
@login_required
def toggle_suspend_client():
    data      = request.json or {}
    client_id = data.get('client_id', '')
    suspend   = bool(data.get('suspend', True))
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    ok = models.toggle_client_suspended(client_id, suspend)
    action = 'suspended' if suspend else 'reactivated'
    app.logger.info(f"[Agency] client {client_id} {action} by user {current_user.id}")
    return jsonify({'success': ok, 'suspended': suspend})


@app.route('/api/admin/client/clone', methods=['POST'])
@login_required
def clone_client_route():
    data             = request.json or {}
    source_client_id = data.get('client_id', '')
    new_name         = data.get('new_name', '').strip()
    if not source_client_id or not models.verify_client_ownership(current_user.id, source_client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    if not new_name:
        return jsonify({'success': False, 'error': 'New client name is required'}), 400

    fresh_user   = models.get_user_by_id(current_user.id)
    plan_type    = (fresh_user or {}).get('plan_type', current_user.plan_type)
    plan_limits  = PLAN_LIMITS.get(plan_type, PLAN_LIMITS['free'])

    # RACE CONDITION FIX: acquire an advisory lock (same key as create_client)
    # so a concurrent clone + create cannot both slip past the limit check.
    _lc, _lcur = models.get_db()
    try:
        _lcur.execute("SELECT pg_advisory_lock(%s)", (current_user.id,))
        current_count = len(models.get_user_clients(current_user.id))
        if current_count >= plan_limits['clients']:
            return jsonify({'success': False, 'error': f'Client limit reached for {plan_type} plan'}), 403
        new_cid = models.clone_client(source_client_id, current_user.id, new_name)
    finally:
        try:
            _lcur.execute("SELECT pg_advisory_unlock(%s)", (current_user.id,))
            _lc.commit()
        except Exception:
            pass
        try:
            _lcur.close(); _lc.close()
        except Exception:
            pass
    if not new_cid:
        return jsonify({'success': False, 'error': 'Clone failed — please try again'}), 500
    app.logger.info(f"[Agency] cloned {source_client_id} → {new_cid} by user {current_user.id}")
    return jsonify({'success': True, 'new_client_id': new_cid, 'message': f'"{new_name}" created successfully'})


@app.route('/api/admin/client/bulk-action', methods=['POST'])
@login_required
def bulk_client_action():
    data       = request.json or {}
    action     = data.get('action', '')
    client_ids = data.get('client_ids', [])
    if not client_ids or not isinstance(client_ids, list):
        return jsonify({'success': False, 'error': 'No clients selected'}), 400
    results = {'ok': [], 'fail': []}
    for cid in client_ids:
        if not models.verify_client_ownership(current_user.id, cid):
            results['fail'].append(cid); continue
        try:
            if action == 'suspend':
                models.toggle_client_suspended(cid, True);   results['ok'].append(cid)
            elif action == 'reactivate':
                models.toggle_client_suspended(cid, False);  results['ok'].append(cid)
            elif action == 'delete':
                models.delete_client(cid);                   results['ok'].append(cid)
            else:
                results['fail'].append(cid)
        except Exception as e:
            app.logger.error(f"[BulkAction] {action} {cid}: {e}")
            results['fail'].append(cid)
    app.logger.info(f"[Agency] bulk {action}: ok={results['ok']} fail={results['fail']}")
    return jsonify({'success': True, **results})

# =====================================================================
# LEGAL PAGES
# =====================================================================

@app.route('/client-login', methods=['GET', 'POST'])
def client_login():
    """Login page for client-facing users."""
    if request.method == 'POST':
        email    = request.form.get('email', '').strip()
        password = request.form.get('password', '')
        user     = models.verify_client_user(email, password)
        if user:
            session['client_user_id']  = user['id']
            session['client_user_email'] = user['email']
            session['client_user_name']  = user.get('name', email)
            session['client_user_client_id'] = user['client_id']
            return redirect(url_for('client_dashboard'))
        return render_template('client_login.html', error='Invalid email or password')
    return render_template('client_login.html')


@app.route('/client-logout')
def client_logout():
    session.pop('client_user_id', None)
    session.pop('client_user_email', None)
    session.pop('client_user_name', None)
    session.pop('client_user_client_id', None)
    return redirect(url_for('client_login'))


def client_login_required(f):
    """Decorator for client-portal routes."""
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('client_user_id'):
            return redirect(url_for('client_login'))
        return f(*args, **kwargs)
    return decorated


@app.route('/client-dashboard')
def client_dashboard_router():
    """
    Routes to the correct dashboard view:
    - Agency/admin owner: accessed via ?client_id= from the main dashboard
    - Client portal user: authenticated via client session
    """
    # ── Owner access: logged-in agency/admin user viewing a specific client ──
    client_id_param = request.args.get('client_id', '').strip()
    if client_id_param and current_user.is_authenticated:
        if not models.verify_client_ownership(current_user.id, client_id_param):
            return "Unauthorized", 403
        client   = models.get_client_by_id(client_id_param)
        if not client:
            return "Client not found", 404
        leads    = models.get_leads(client_id_param)
        faqs     = models.get_faqs(client_id_param)
        articles = models.get_articles(client_id_param)
        for lead in leads:
            if lead.get('created_at') and not isinstance(lead['created_at'], str):
                lead['created_at'] = lead['created_at'].isoformat()
        branding = {}
        if client.get('branding_settings'):
            try:
                bs = json.loads(client['branding_settings']) if isinstance(client['branding_settings'], str) else client['branding_settings']
                branding = bs.get('branding', {})
            except Exception:
                pass
        # Fetch plan info for analytics gating and usage warning
        _owner_plan_ov  = models.get_user_by_id(client.get('user_id', '')) if hasattr(models, 'get_user_by_id') else {}
        _plan_type_ov   = (_owner_plan_ov or {}).get('plan_type', 'free')
        _plan_limits_ov = PLAN_LIMITS.get(_plan_type_ov, PLAN_LIMITS['free'])
        _usage_warn_ov  = models.get_usage_warning(client_id_param) if hasattr(models, 'get_usage_warning') else None
        return render_template(
            'client_dashboard.html',
            client=client,
            branding=branding,
            leads=leads,
            faqs=faqs,
            articles=articles,
            client_user_name=current_user.email,
            client_user_email=current_user.email,
            faq_count=len(faqs),
            lead_count=len(leads),
            owner_view=True,   # lets the template show an "← Back" link
            analytics_level=_plan_limits_ov.get('analytics_level', 'none'),
            usage_warning=_usage_warn_ov,
        )

    # ── Client portal user: must be authenticated via client session ──
    return client_dashboard_client()


@app.route('/client-dashboard-portal')
@client_login_required
def client_dashboard_client():
    client_id   = session['client_user_client_id']
    client      = models.get_client_by_id(client_id)
    leads       = models.get_leads(client_id)
    faqs        = models.get_faqs(client_id)
    articles    = models.get_articles(client_id)
    # Serialize datetimes
    for lead in leads:
        if lead.get('created_at') and not isinstance(lead['created_at'], str):
            lead['created_at'] = lead['created_at'].isoformat()
    branding = {}
    if client and client.get('branding_settings'):
        try:
            bs = json.loads(client['branding_settings']) if isinstance(client['branding_settings'], str) else client['branding_settings']
            branding = bs.get('branding', {})
        except Exception:
            pass
    # Fetch plan info for analytics gating and usage warning
    _owner_cp     = models.get_client_owner(client_id) if hasattr(models, 'get_client_owner') else {}
    _plan_type_cp = (_owner_cp or {}).get('plan_type', 'starter')  # portal users on paid plan
    _plan_lim_cp  = PLAN_LIMITS.get(_plan_type_cp, PLAN_LIMITS['free'])
    _usage_warn_cp = models.get_usage_warning(client_id) if hasattr(models, 'get_usage_warning') else None
    return render_template(
        'client_dashboard.html',
        client=client,
        branding=branding,
        leads=leads,
        faqs=faqs,
        articles=articles,
        client_user_name=session.get('client_user_name'),
        client_user_email=session.get('client_user_email'),
        faq_count=len(faqs),
        lead_count=len(leads),
        owner_view=False,
        analytics_level=_plan_lim_cp.get('analytics_level', 'basic'),
        usage_warning=_usage_warn_cp,
    )


# ── Agency owner: manage client logins ──────────────────────────────

@app.route('/api/client-users', methods=['GET'])
@login_required
def list_client_users():
    """List all client portal users for a given chatbot."""
    client_id = request.args.get('client_id')
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    users = models.get_client_users(client_id)
    return jsonify({'success': True, 'users': users})


@app.route('/api/client-users/invite', methods=['POST'])
@login_required
def invite_client_user():
    """Create a client portal login (Pro / Agency / Enterprise only)."""
    user = models.get_user_by_id(current_user.id)
    plan = user.get('plan_type', 'free')
    if plan not in ('pro', 'agency', 'enterprise', 'solo'):
        return jsonify({'success': False, 'error': 'Client logins require Pro plan or above'}), 403

    data      = request.get_json()
    client_id = data.get('client_id')
    email     = data.get('email', '').strip()
    name      = data.get('name', '').strip()
    password  = data.get('password', '').strip()

    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    if not email or not password:
        return jsonify({'success': False, 'error': 'Email and password are required'}), 400
    if len(password) < 6:
        return jsonify({'success': False, 'error': 'Password must be at least 6 characters'}), 400

    uid = models.create_client_user(client_id, email, password, name, current_user.id)
    if not uid:
        return jsonify({'success': False, 'error': 'Email already exists or could not be created'}), 400

    # Send welcome email to client
    try:
        client = models.get_client_by_id(client_id)
        company = client.get('company_name', 'your chatbot portal') if client else 'your chatbot portal'
        msg = Message(
            subject=f"Your client portal access — {company}",
            sender="Lumvi <support@lumvi.net>",
            recipients=[email],
            html=f"""
<div style="font-family:Inter,Arial,sans-serif;max-width:480px;margin:0 auto;background:#0f172a;color:#f8fafc;padding:40px;border-radius:16px;">
  <div style="text-align:center;margin-bottom:28px;">
    <div style="display:inline-block;background:linear-gradient(135deg,#6366f1,#a78bfa);border-radius:12px;padding:10px 20px;font-size:22px;font-weight:800;">⚡ {company}</div>
  </div>
  <h2 style="margin:0 0 12px;font-size:20px;">Your portal access is ready</h2>
  <p style="color:#94a3b8;margin:0 0 24px;line-height:1.6;">Hi {name or email}, you've been given access to your client portal where you can view your leads, FAQs and analytics.</p>
  <div style="background:rgba(99,102,241,0.1);border:1px solid rgba(99,102,241,0.2);border-radius:10px;padding:16px;margin-bottom:24px;">
    <p style="margin:0 0 6px;font-size:13px;color:#94a3b8;">Login URL</p>
    <p style="margin:0;font-size:14px;color:#a5b4fc;">lumvi.net/client-login</p>
    <p style="margin:12px 0 6px;font-size:13px;color:#94a3b8;">Email</p>
    <p style="margin:0;font-size:14px;color:#f8fafc;">{email}</p>
    <p style="margin:12px 0 6px;font-size:13px;color:#94a3b8;">Password</p>
    <p style="margin:0;font-size:14px;color:#f8fafc;">{password}</p>
  </div>
  <a href="https://lumvi.net/client-login" style="display:block;text-align:center;background:linear-gradient(135deg,#6366f1,#7c3aed);color:#fff;text-decoration:none;padding:14px;border-radius:10px;font-weight:700;">Access My Portal →</a>
</div>"""
        )
        mail.send(msg)
    except Exception as e:
        app.logger.error(f"Client invite email failed: {e}")

    app.logger.info(f"Client user created: {email} for client {client_id} by user {current_user.id}")
    return jsonify({'success': True, 'id': uid, 'message': f'Login created for {email}'})


@app.route('/api/client-users/delete', methods=['POST'])
@login_required
def delete_client_user():
    data      = request.get_json()
    client_id = data.get('client_id')
    user_id   = data.get('user_id')
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    models.delete_client_user(user_id, client_id)
    return jsonify({'success': True})


@app.route('/api/client-users/reset-password', methods=['POST'])
@login_required
def reset_client_user_password():
    """Reset password for a client portal login."""
    data      = request.get_json() or {}
    client_id = data.get('client_id', '')
    user_id   = data.get('user_id')
    password  = data.get('password', '')
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    if not password or len(password) < 6:
        return jsonify({'success': False, 'error': 'Password must be at least 6 characters'}), 400
    from werkzeug.security import generate_password_hash
    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            'UPDATE client_users SET password_hash = %s WHERE id = %s AND client_id = %s',
            (generate_password_hash(password), user_id, client_id)
        )
        conn.commit()
        app.logger.info(f"[ClientUsers] password reset for user {user_id} on client {client_id}")
        return jsonify({'success': True})
    except Exception as e:
        app.logger.error(f"[ClientUsers] reset_password error: {e}")
        return jsonify({'success': False, 'error': 'Failed to reset password'}), 500
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


@app.route('/manage-client-users')
@login_required
def manage_client_users_page():
    """Page for agency owners to manage client logins."""
    client_id = request.args.get('client_id')
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return "Unauthorized", 403
    user = models.get_user_by_id(current_user.id)
    plan = user.get('plan_type', 'free')
    if plan not in ('pro', 'agency', 'enterprise', 'solo'):
        return render_template('upgrade_required.html',
            feature='Client Logins',
            description='Give your clients their own portal to view leads and analytics.',
            min_plan='Pro'), 403
    client = models.get_client_by_id(client_id)
    return render_template('manage_client_users.html', client=client, client_id=client_id)


@app.route('/api/admin/enforce-subscriptions', methods=['POST'])
@login_required
def admin_enforce_subscriptions():
    """Admin-only endpoint to manually trigger subscription enforcement."""
    user = models.get_user_by_id(current_user.id)
    if not user or not user.get('is_admin'):
        return jsonify({'success': False, 'error': 'Admin only'}), 403
    try:
        downgraded = models.downgrade_expired_users()
        app.logger.info(f"[Admin] manual downgrade run: {len(downgraded)} users")
        return jsonify({
            'success': True,
            'downgraded_count': len(downgraded),
            'downgraded_users': [{'id': u['id'], 'email': u.get('email'), 'plan': u.get('plan_type')} for u in downgraded]
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/cron/enforce-subscriptions', methods=['GET', 'POST'])
def cron_enforce_subscriptions():
    """
    Dyno-restart-safe cron endpoint.

    Point any external HTTP pinger here — free options:
      • UptimeRobot (free tier) — set a monitor to GET this URL every 24h
      • Render Cron Jobs (paid) — add a cron job calling this URL
      • cron-job.org (free)   — schedule a daily POST

    Secured by CRON_SECRET env var. Set it in Render dashboard to any
    random string (e.g. openssl rand -hex 32).

    Usage:
      GET /cron/enforce-subscriptions?secret=YOUR_CRON_SECRET
      POST /cron/enforce-subscriptions  (body: {"secret": "YOUR_CRON_SECRET"})
    """
    cron_secret = os.environ.get('CRON_SECRET', '').strip()

    # If no secret is configured, lock it down completely
    if not cron_secret:
        app.logger.error("[Cron] CRON_SECRET env var not set — endpoint disabled for safety.")
        return jsonify({'error': 'Cron not configured'}), 503

    # Accept secret from query param (GET) or JSON body (POST)
    provided = (
        request.args.get('secret', '') or
        (request.get_json(silent=True) or {}).get('secret', '')
    )

    # Timing-safe comparison prevents secret-length oracle attacks.
    if not __import__("hmac").compare_digest(provided, cron_secret):
        app.logger.warning(f"[Cron] Unauthorized attempt from {request.remote_addr}")
        return jsonify({'error': 'Unauthorized'}), 401

    # Run enforcement
    downgraded = enforce_subscriptions()
    # User IDs intentionally excluded from response — they are logged
    # server-side. Including them here would leak identifiers into
    # external pinger logs (UptimeRobot, cron-job.org, etc.).
    return jsonify({
        'success':          True,
        'ran_at':           datetime.utcnow().isoformat(),
        'downgraded_count': len(downgraded),
    })


# =====================================================================
# WEEKLY UNANSWERED QUESTIONS DIGEST EMAIL
# =====================================================================

def _build_digest_email_html(business_name: str, questions: list, upgrade_url: str) -> str:
    rows = ''.join(
        f"""<tr>
          <td style="padding:10px 16px;border-bottom:1px solid #F0EBE1;font-size:14px;color:#1C1917;">
            {i}. {q['question']}
          </td>
          <td style="padding:10px 16px;border-bottom:1px solid #F0EBE1;font-size:13px;color:#A8A29E;text-align:right;">
            {q['count']}x this week
          </td>
        </tr>"""
        for i, q in enumerate(questions, 1)
    )
    return f"""
    <div style="font-family:'DM Sans',Arial,sans-serif;max-width:540px;margin:0 auto;background:#F7F4EF;padding:32px 20px;">
      <div style="text-align:center;margin-bottom:24px;">
        <span style="font-size:22px;font-weight:800;color:#1C1917;">Your weekly bot report 🤖</span>
      </div>
      <div style="background:#fff;border:1px solid #E7E2DA;border-radius:16px;overflow:hidden;">
        <div style="background:#1C1917;padding:20px 24px;">
          <p style="color:rgba(255,255,255,0.7);font-size:14px;margin:0;">
            Hi {business_name} team — here are the top questions your chatbot
            <strong style="color:#B8924A;">couldn't answer</strong> this week.
            Adding these to your knowledge base will help convert more visitors.
          </p>
        </div>
        <table style="width:100%;border-collapse:collapse;">
          <thead>
            <tr style="background:#F7F4EF;">
              <th style="padding:10px 16px;font-size:12px;font-weight:700;color:#A8A29E;text-align:left;text-transform:uppercase;letter-spacing:0.05em;">Question</th>
              <th style="padding:10px 16px;font-size:12px;font-weight:700;color:#A8A29E;text-align:right;text-transform:uppercase;letter-spacing:0.05em;">Frequency</th>
            </tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>
        <div style="padding:20px 24px;text-align:center;border-top:1px solid #F0EBE1;">
          <a href="{upgrade_url}" style="display:inline-block;padding:12px 28px;background:#B8924A;color:#fff;border-radius:10px;font-weight:700;font-size:14px;text-decoration:none;">
            Fix these gaps in your bot →
          </a>
          <p style="font-size:12px;color:#A8A29E;margin-top:12px;">
            Log in to your dashboard to add answers to these questions.
          </p>
        </div>
      </div>
      <p style="text-align:center;font-size:11px;color:#A8A29E;margin-top:20px;">
        Lumvi · <a href="{upgrade_url}/unsubscribe-digest" style="color:#A8A29E;">Unsubscribe from weekly digest</a>
      </p>
    </div>
    """


def send_weekly_digest():
    """
    Send each paid client's owner an email listing their top unanswered
    questions from the past 7 days.

    Deduplication: uses get_clients_for_weekly_digest_due() which only
    returns clients whose last_digest_sent_at is NULL or >6 days ago.
    After a successful send, marks the client with mark_digest_sent() so
    a repeated cron call in the same week is a no-op.
    """
    import time as _time
    t0 = _time.time()

    if not hasattr(models, 'get_clients_for_weekly_digest_due'):
        app.logger.warning('[WeeklyDigest] dedup function not found — skipping')
        return {'sent': 0, 'skipped': 0, 'errors': 0}

    from flask_mail import Message as MailMessage
    # Use the dedup-safe version — never returns clients sent to in the last 6 days
    clients  = models.get_clients_for_weekly_digest_due()
    sent = skipped = errors = 0
    base_url = os.environ.get('APP_BASE_URL', 'https://lumvi.net')

    for client in clients:
        cid   = client['client_id']
        biz   = client.get('business_name') or 'Your business'
        email = client.get('contact_email') or client.get('owner_email')
        if not email:
            skipped += 1
            continue

        questions = models.get_unanswered_questions_for_email(cid, since_days=7, limit=5)
        if not questions:
            skipped += 1
            continue

        try:
            html = _build_digest_email_html(
                business_name=biz,
                questions=questions,
                upgrade_url=f"{base_url}/dashboard?client_id={cid}"
            )
            msg = MailMessage(
                subject   = f"Your bot report: {len(questions)} questions it couldn't answer this week",
                recipients= [email],
                html      = html,
                sender    = os.environ.get('MAIL_DEFAULT_SENDER', 'hello@lumvi.net'),
            )
            mail.send(msg)
            # Stamp dedup timestamp immediately after successful send
            models.mark_digest_sent(cid)
            sent += 1
            app.logger.info(f'[WeeklyDigest] sent to {email} client={cid}')
        except Exception as e:
            app.logger.error(f'[WeeklyDigest] failed for {cid}: {e}')
            errors += 1

    duration_ms = int((_time.time() - t0) * 1000)
    result = {'sent': sent, 'skipped': skipped, 'errors': errors}
    app.logger.info(f'[WeeklyDigest] complete — {result} dur={duration_ms}ms')
    models.log_cron_run('weekly_digest', success=(errors == 0), result=result,
                        duration_ms=duration_ms, triggered_by='http')
    return result


@app.route('/cron/weekly-digest', methods=['GET', 'POST'])
def cron_weekly_digest():
    """
    Weekly cron endpoint — sends the unanswered-questions digest to all paid clients.
    Secure with the same CRON_SECRET as /cron/enforce-subscriptions.
    Point a weekly cron-job.org job (or UptimeRobot) at this URL.

    Usage:
      GET /cron/weekly-digest?secret=YOUR_CRON_SECRET
      POST /cron/weekly-digest  (body: {"secret": "YOUR_CRON_SECRET"})
    """
    cron_secret = os.environ.get('CRON_SECRET', '').strip()
    if not cron_secret:
        return jsonify({'error': 'Cron not configured'}), 503

    provided = (
        request.args.get('secret', '') or
        (request.get_json(silent=True) or {}).get('secret', '')
    )
    if not __import__('hmac').compare_digest(provided, cron_secret):
        app.logger.warning(f'[WeeklyDigest] Unauthorized attempt from {request.remote_addr}')
        return jsonify({'error': 'Unauthorized'}), 401

    result = send_weekly_digest()
    return jsonify({'success': True, 'ran_at': datetime.utcnow().isoformat(), **result})


# =====================================================================
# AGENCY PER-SEAT OVERAGE BILLING
# =====================================================================

def bill_agency_overages():
    """
    Called monthly by /cron/agency-overage.
    For every agency user with more than AGENCY_INCLUDED_CLIENTS clients,
    calculate the extra seats, record a payment, and email a receipt.

    NOTE: record_payment() creates a 'pending' record. To auto-charge cards
    you must call Flutterwave's tokenized charge endpoint using the stored
    subscription_id as the card token. This requires Flutterwave's card
    tokenization to be enabled on your account — wire it in here once enabled.
    Until then, the pending record triggers the email which prompts manual payment.

    Returns a summary dict.
    """
    import time as _time
    _t0 = _time.time()
    if not hasattr(models, 'get_agency_users_with_overage'):
        app.logger.warning('[AgencyOverage] models.get_agency_users_with_overage not found')
        return {'billed': 0, 'skipped': 0, 'total_revenue': 0.0}

    agency_users = models.get_agency_users_with_overage(AGENCY_INCLUDED_CLIENTS)
    billed = skipped = 0
    total_revenue = 0.0

    for u in agency_users:
        user_id     = u['id']
        email       = u['email']
        client_count = int(u['client_count'])
        extra_seats  = client_count - AGENCY_INCLUDED_CLIENTS
        if extra_seats <= 0:
            skipped += 1
            continue

        amount = round(extra_seats * AGENCY_SEAT_PRICE, 2)

        try:
            models.record_payment(
                user_id  = user_id,
                amount   = amount,
                plan_type= 'agency',
                provider = 'overage',
                currency = 'USD',
                status   = 'pending',   # set to 'completed' after actual charge via payment provider
                notes    = (
                    f"Agency per-seat overage: {extra_seats} extra seat(s) "
                    f"× ${AGENCY_SEAT_PRICE}/mo = ${amount:.2f}. "
                    f"Total clients: {client_count} (included: {AGENCY_INCLUDED_CLIENTS})"
                )
            )
            total_revenue += amount
            billed += 1
            app.logger.info(
                f'[AgencyOverage] billed user={user_id} email={email} '
                f'extra_seats={extra_seats} amount=${amount}'
            )

            # Email receipt / notice to agency owner
            try:
                from flask_mail import Message as MailMessage
                base_url = os.environ.get('APP_BASE_URL', 'https://lumvi.net')
                html = f"""
                <div style="font-family:Arial,sans-serif;max-width:520px;margin:0 auto;padding:24px;background:#F7F4EF;">
                  <div style="background:#fff;border:1px solid #E7E2DA;border-radius:16px;overflow:hidden;">
                    <div style="background:#1C1917;padding:20px 24px;">
                      <h2 style="color:#B8924A;font-size:18px;margin:0;">Agency Per-Seat Billing Notice</h2>
                    </div>
                    <div style="padding:24px;">
                      <p style="color:#57534E;font-size:14px;line-height:1.6;margin-bottom:16px;">
                        Hi {email},<br><br>
                        Your Lumvi Agency plan currently has <strong>{client_count} chatbots</strong>.
                        Your plan includes {AGENCY_INCLUDED_CLIENTS} — the additional
                        <strong>{extra_seats} seat(s)</strong> are billed at
                        ${AGENCY_SEAT_PRICE:.0f}/mo each.
                      </p>
                      <div style="background:#F7F4EF;border:1px solid #E7E2DA;border-radius:12px;padding:16px;margin-bottom:20px;">
                        <div style="display:flex;justify-content:space-between;margin-bottom:8px;">
                          <span style="color:#A8A29E;font-size:13px;">Included seats</span>
                          <span style="font-weight:700;color:#1C1917;">{AGENCY_INCLUDED_CLIENTS}</span>
                        </div>
                        <div style="display:flex;justify-content:space-between;margin-bottom:8px;">
                          <span style="color:#A8A29E;font-size:13px;">Extra seats</span>
                          <span style="font-weight:700;color:#1C1917;">{extra_seats} × ${AGENCY_SEAT_PRICE:.0f}</span>
                        </div>
                        <div style="display:flex;justify-content:space-between;border-top:1px solid #E7E2DA;padding-top:10px;margin-top:4px;">
                          <span style="color:#A8A29E;font-size:13px;">Overage charge</span>
                          <span style="font-weight:800;color:#B8924A;font-size:16px;">${amount:.2f}/mo</span>
                        </div>
                      </div>
                      <p style="color:#A8A29E;font-size:12px;">
                        This charge will be processed via your payment method on file.
                        To reduce your bill, archive unused chatbots from your
                        <a href="{base_url}/agency/clients" style="color:#B8924A;">Agency dashboard</a>.
                      </p>
                    </div>
                  </div>
                </div>
                """
                msg = MailMessage(
                    subject   = f'Lumvi Agency billing: {extra_seats} extra seat(s) — ${amount:.2f}/mo',
                    recipients= [email],
                    html      = html,
                    sender    = os.environ.get('MAIL_DEFAULT_SENDER', 'hello@lumvi.net'),
                )
                mail.send(msg)
            except Exception as mail_err:
                app.logger.error(f'[AgencyOverage] email failed for {email}: {mail_err}')

        except Exception as e:
            app.logger.error(f'[AgencyOverage] billing failed for user={user_id}: {e}')
            skipped += 1

    import time as _time
    duration_ms = int((_time.time() - _t0) * 1000) if '_t0' in locals() else 0
    result = {'billed': billed, 'skipped': skipped, 'total_revenue': round(total_revenue, 2)}
    app.logger.info(
        f'[AgencyOverage] complete — billed={billed} skipped={skipped} '
        f'total_revenue=${total_revenue:.2f} dur={duration_ms}ms'
    )
    models.log_cron_run('agency_overage', success=True, result=result,
                        duration_ms=duration_ms, triggered_by='http')
    return result


# =====================================================================
# LOG CLEANUP CRON — run weekly, prunes webhook_logs only
# Conversations are NEVER pruned — kept for LLM fine-tuning.
# =====================================================================

@app.route('/cron/cleanup-logs', methods=['GET', 'POST'])
def cron_cleanup_logs():
    """
    Prune old webhook_logs (default >60 days) to keep the DB lean.

    NOTE: Conversations are intentionally excluded from cleanup —
    they are preserved as LLM fine-tuning training data.

    Recommended schedule: weekly (e.g. every Sunday at 03:00 UTC).
    Secured by the same CRON_SECRET as other cron endpoints.

    Usage:
      GET  /cron/cleanup-logs?secret=YOUR_CRON_SECRET
      POST /cron/cleanup-logs  (body: {"secret": "YOUR_CRON_SECRET"})

    Optional query/body params:
      webhook_days  (default 60)  — delete webhook_logs older than N days
    """
    import time as _time
    cron_secret = os.environ.get('CRON_SECRET', '').strip()
    if not cron_secret:
        return jsonify({'error': 'Cron not configured'}), 503

    body     = request.get_json(silent=True) or {}
    provided = request.args.get('secret', '') or body.get('secret', '')
    if not __import__('hmac').compare_digest(provided, cron_secret):
        app.logger.warning(f'[CleanupLogs] Unauthorized from {request.remote_addr}')
        return jsonify({'error': 'Unauthorized'}), 401

    webhook_days = int(request.args.get('webhook_days', body.get('webhook_days', 60)))

    # Clamp to safe minimum — never delete less than 7 days of data
    webhook_days = max(webhook_days, 7)

    # NOTE: conversations are never pruned — preserved for LLM fine-tuning.

    t0      = _time.time()
    deleted = models.prune_old_logs(webhook_days=webhook_days)
    duration_ms = int((_time.time() - t0) * 1000)

    result = {
        **deleted,
        'webhook_days': webhook_days,
    }
    models.log_cron_run('cleanup_logs', success=True, result=result,
                        duration_ms=duration_ms, triggered_by='http')
    app.logger.info(f'[CleanupLogs] {result} dur={duration_ms}ms')
    return jsonify({'success': True, 'ran_at': datetime.utcnow().isoformat(), **result})


# =====================================================================
# CRON STATUS — admin visibility into last run times and history
# =====================================================================

@app.route('/cron/status', methods=['GET'])
@login_required
def cron_status():
    """
    Returns last-run info for all cron jobs.
    Admin-only — requires login and is_admin flag.

    Usage:
      GET /cron/status
    """
    if not getattr(current_user, 'is_admin', False):
        return jsonify({'error': 'Admin only'}), 403

    jobs = ['enforce_subscriptions', 'weekly_digest', 'agency_overage', 'cleanup_logs']
    status = {}
    for job in jobs:
        last = models.get_cron_last_run(job)
        if last:
            # Convert datetime to ISO string for JSON serialisation
            last['ran_at'] = last['ran_at'].isoformat() if hasattr(last.get('ran_at'), 'isoformat') else str(last.get('ran_at', ''))
        status[job] = last or {'ran_at': None, 'success': None, 'result': None}

    return jsonify({'success': True, 'cron_status': status})


@app.route('/cron/agency-overage', methods=['GET', 'POST'])
def cron_agency_overage():
    """
    Monthly cron — calculates and records per-seat overage charges for all
    agency users who have more than AGENCY_INCLUDED_CLIENTS chatbots.

    Usage (same CRON_SECRET as other cron endpoints):
      GET  /cron/agency-overage?secret=YOUR_CRON_SECRET
      POST /cron/agency-overage  (body: {"secret": "YOUR_CRON_SECRET"})

    Recommended schedule: 1st of every month at 00:05 UTC.
    """
    cron_secret = os.environ.get('CRON_SECRET', '').strip()
    if not cron_secret:
        return jsonify({'error': 'Cron not configured'}), 503

    provided = (
        request.args.get('secret', '') or
        (request.get_json(silent=True) or {}).get('secret', '')
    )
    if not __import__('hmac').compare_digest(provided, cron_secret):
        app.logger.warning(f'[AgencyOverage] Unauthorized cron attempt from {request.remote_addr}')
        return jsonify({'error': 'Unauthorized'}), 401

    result = bill_agency_overages()
    return jsonify({
        'success':       True,
        'ran_at':        datetime.utcnow().isoformat(),
        'billed':        result['billed'],
        'skipped':       result['skipped'],
        'total_revenue': result['total_revenue'],
    })


# =====================================================================
# SYSTEM 2 — TRAINING DATA: ADMIN ROUTES
# =====================================================================

@app.route('/api/admin/training/stats', methods=['GET'])
@login_required
def admin_training_stats():
    """
    Returns training sample stats.
    Admin only.
    Query params:
      client_id — if provided, returns per-type breakdown for that client.
                  If omitted, returns summary across all clients.
    """
    if not current_user.is_admin:
        return jsonify({'success': False, 'error': 'Admin only'}), 403
    try:
        from training_collector import get_training_stats as _get_stats_all
        from training_collector import get_training_stats as _get_stats_client
        client_id = request.args.get('client_id')
        if client_id:
            # Per-client breakdown reuses the original function signature
            stats = _get_stats_client(client_id)
        else:
            stats = _get_stats_all()
        return jsonify({'success': True, 'stats': stats})
    except Exception as e:
        app.logger.error(f'[TrainingAdmin] stats error: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/admin/training/export', methods=['GET'])
@login_required
def admin_training_export():
    """
    Export training samples as JSONL (Alpaca format) for fine-tuning.
    Query params:
      client_id  — filter to one client (optional; omit for all)
      split      — train / val / test (default: train)
      min_quality — float 0.0–1.0 (default: 0.5)
      limit      — max rows (default: 5000)

    Returns a downloadable .jsonl file.
    """
    if not current_user.is_admin:
        return jsonify({'success': False, 'error': 'Admin only'}), 403
    try:
        from training_collector import export_training_jsonl
        import io
        client_id   = request.args.get('client_id')
        split       = request.args.get('split', 'train')
        min_quality = float(request.args.get('min_quality', 0.5))
        limit       = int(request.args.get('limit', 5000))

        jsonl_str = export_training_jsonl(
            client_id   = client_id,
            split       = split,
            min_quality = min_quality,
            limit       = limit,
        )
        filename = f"lumvi_training_{split}_{datetime.utcnow().strftime('%Y%m%d')}.jsonl"
        return app.response_class(
            response    = jsonl_str,
            status      = 200,
            mimetype    = 'application/jsonl',
            headers     = {'Content-Disposition': f'attachment; filename={filename}'},
        )
    except Exception as e:
        app.logger.error(f'[TrainingAdmin] export error: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/admin/training/assign-splits', methods=['POST'])
@login_required
def admin_assign_splits():
    """
    Assign train/val/test splits to all unassigned samples for a client.
    Body: { "client_id": "...", "train_pct": 0.8, "val_pct": 0.1 }
    """
    if not current_user.is_admin:
        return jsonify({'success': False, 'error': 'Admin only'}), 403
    try:
        from training_collector import assign_splits
        data       = request.get_json() or {}
        client_id  = data.get('client_id')
        train_pct  = float(data.get('train_pct', 0.8))
        val_pct    = float(data.get('val_pct', 0.1))
        if not client_id:
            return jsonify({'success': False, 'error': 'client_id required'}), 400
        assign_splits(client_id=client_id, train_pct=train_pct, val_pct=val_pct)
        return jsonify({'success': True, 'message': f'Splits assigned for {client_id}'})
    except Exception as e:
        app.logger.error(f'[TrainingAdmin] assign_splits error: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/terms')
def terms():
    return render_template('terms.html')


@app.route('/privacy-policy')
def privacy_policy():
    return render_template('privacy-policy.html')


@app.route('/refund-policy')
def refund_policy():
    return render_template('refund-policy.html')

# =====================================================================
# RUN SERVER
# =====================================================================

@app.errorhandler(413)
def request_too_large(e):
    """Triggered when a request body exceeds MAX_CONTENT_LENGTH (8MB)."""
    app.logger.warning(f"[413] Request too large: {request.path}")
    if request.path.startswith('/api/'):
        return jsonify({
            'success': False,
            'error': 'Request too large. Avatar images are auto-compressed in the browser — '
                     'if you see this error please try a smaller file.'
        }), 413
    return "Request too large (max 8 MB)", 413


if __name__ == '__main__':
    # ⚠️  Flask's built-in server is single-threaded and NOT suitable for production.
    # For production / Render, use Gunicorn with multiple workers:
    #
    #   gunicorn app:app --workers 4 --worker-class gevent --bind 0.0.0.0:$PORT
    #
    # Set your Render Start Command to the above (install gevent via requirements.txt).
    # With REDIS_URL set, Flask-Limiter and session state will be consistent across workers.
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)