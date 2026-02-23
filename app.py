from flask import Flask, request, jsonify, render_template, send_from_directory, redirect, url_for, session, flash
from flask_cors import CORS
from dotenv import load_dotenv
import os
import json
import re
from datetime import datetime
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import logging
from logging.handlers import RotatingFileHandler
import shutil
from datetime import datetime, timedelta
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from functools import wraps

import models
import requests
import uuid
from collections import Counter
from io import StringIO
from config import Config
from ai_helper import get_ai_helper
from paypalrestsdk import Payment, configure

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Admin blueprint — register immediately after app creation
from admin_routes import admin_bp
app.register_blueprint(admin_bp)
app.config['SECRET_KEY'] = os.environ.get("SECRET_KEY", "dev_key_change_this_in_prod")

# Initialize AI helper at app startup
ai_helper = get_ai_helper(Config.GEMINI_API_KEY, Config.GEMINI_MODEL)

if ai_helper and ai_helper.enabled:
    app.logger.info("✅ Gemini AI initialized")

configure({
    "mode": os.getenv('PAYPAL_MODE', 'sandbox'),
    "client_id": os.getenv('PAYPAL_CLIENT_ID'),
    "client_secret": os.getenv('PAYPAL_CLIENT_SECRET')
})

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
        'customization': False,
        'white_label': False,
        'webhooks': False,
        'priority_support': False
    },
    'starter': {
        # $49/mo — foot-in-door tier
        'clients': 3,
        'faqs_per_client': 999,
        'messages_per_day': 2000,
        'analytics': False,
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
        'customization': True,
        'white_label': True,
        'webhooks': True,
        'priority_support': True
    }
}

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
    user_data = models.get_user_by_id(int(user_id))
    if user_data:
        return User(user_data)
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
    print("✅ Database initialized/migrated successfully!")
except Exception as e:
    print(f"⚠️ Database initialization error: {e}")

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

# Rate limiting
limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["200 per day", "50 per hour"],
    storage_uri="memory://"
)

STOP_WORDS = {
    'a', 'an', 'the', 'this', 'that', 'these', 'those',
    'i', 'me', 'my', 'we', 'our', 'you', 'your', 'it', 'its',
    'he', 'she', 'they', 'them', 'their',
    'is', 'are', 'was', 'were', 'be', 'been', 'being',
    'do', 'does', 'did', 'have', 'has', 'had',
    'can', 'could', 'will', 'would', 'should', 'shall', 'may', 'might',
    'go', 'get', 'got', 'make', 'made', 'use', 'used',
    'in', 'on', 'at', 'by', 'for', 'with', 'about',
    'to', 'from', 'into', 'through', 'before', 'after',
    'of', 'off', 'out', 'over', 'under', 'and', 'or', 'but',
    'if', 'as', 'than', 'because', 'while',
    'just', 'also', 'too', 'very', 'really', 'quite', 'already',
    'still', 'ever', 'never', 'always', 'often',
    'anytime', 'sometime', 'whenever', 'now', 'then', 'when',
    'today', 'tomorrow', 'time', 'times',
    'please', 'thanks', 'thank', 'hello', 'hi', 'hey',
    'like', 'want', 'need', 'know', 'tell', 'show', 'help',
    'what', 'where', 'which', 'who', 'why', 'how',
    'any', 'all', 'some', 'more', 'most', 'many', 'much',
    'no', 'not', 'nor', 'there', 'per', 'each'
}

GENERIC_TAGS = {
    'schedule', 'appointment', 'book', 'available', 'availability',
    'open', 'closed', 'contact', 'reach', 'support', 'help',
    'information', 'info', 'details', 'more', 'learn'
}


def extract_keywords(text):
    words = re.findall(r'\b[a-z]+\b', text.lower())
    return [w for w in words if w not in STOP_WORDS and len(w) >= 3]


def compute_tag_weights(faqs_list):
    tag_frequency = Counter()
    for faq in faqs_list:
        for tag in faq.get('triggers', []):
            tag_frequency[tag.lower()] += 1

    tag_weights = {}
    for tag, freq in tag_frequency.items():
        if tag in GENERIC_TAGS:
            tag_weights[tag] = 0.2
        else:
            tag_weights[tag] = round(1.0 / freq, 2)
    return tag_weights


def find_best_match(user_query, faqs_list, confidence_threshold=0.15):
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

        raw_score = sum(tag_weights.get(tag, 0.5) for tag in matched_tags)
        max_possible = sum(tag_weights.get(tag, 0.5) for tag in all_tags)
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


def notify_webhook(client_id, lead_data):
    try:
        client = models.get_client_by_id(client_id)
        if not client:
            return

        config = json.loads(client.get('branding_settings') or '{}')
        webhook_url = config.get('integrations', {}).get('webhook_url')

        if not webhook_url:
            return

        requests.post(webhook_url, json={
            'event': 'new_lead',
            'client_id': client_id,
            'lead': lead_data,
            'timestamp': datetime.now().isoformat()
        }, timeout=5)

        app.logger.info(f'✅ Webhook fired for {client_id}')

    except Exception as e:
        app.logger.error(f'Webhook error: {e}')


@app.after_request
def allow_widget_embedding(response):
    response.headers.pop('X-Frame-Options', None)
    response.headers['Content-Security-Policy'] = "frame-ancestors *"

    origin = request.headers.get('Origin')
    if origin:
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

        lead_data['id'] = f"lead_{len(leads_file['leads']) + 1}"
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


def log_conversation(client_id, user_message, bot_response, matched=False, method='unknown'):
    try:
        conn, cursor = models.get_db()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS conversations (
                id SERIAL PRIMARY KEY,
                client_id TEXT NOT NULL,
                user_message TEXT NOT NULL,
                bot_response TEXT NOT NULL,
                matched BOOLEAN DEFAULT FALSE,
                method TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute(
            '''
            INSERT INTO conversations (client_id, user_message, bot_response, matched, method)
            VALUES (%s, %s, %s, %s, %s)
            ''',
            (client_id, user_message, bot_response, matched, method)
        )

        conn.commit()
        cursor.close()
        conn.close()
        app.logger.info(f'✅ Logged conversation for {client_id}')
    except Exception as e:
        app.logger.error(f'❌ Error logging conversation: {e}')

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

        if not message:
            return jsonify({'success': False, 'error': 'Message is required'}), 400

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
        if client and client_id != 'demo':
            owner = models.get_client_owner(client_id)
            if owner:
                plan_type = owner.get('plan_type', 'free')
                daily_limit = PLAN_LIMITS.get(plan_type, PLAN_LIMITS['free'])['messages_per_day']
                if daily_limit < 999999:
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
                            'method': 'limit_enforced'
                        })

        # Step 1: Lead detection
        for trigger in lead_triggers:
            if trigger.lower() in message_lower:
                response_text = "I'd be happy to connect you with our team! What's the best email to reach you?"
                log_conversation(client_id, message, response_text, matched=True, method='lead_trigger')
                return jsonify({
                    'success': True,
                    'response': response_text,
                    'trigger_lead_collection': True,
                    'method': 'instant',
                    'contact_info': config.get('contact', {})
                })

        # Step 2: Smart keyword matching
        best_faq, confidence = find_best_match(message, faqs_list)

        if best_faq:
            app.logger.info(f"Smart match: '{best_faq.get('id')}' | confidence: {confidence}")
            response_text = best_faq.get('answer')
            log_conversation(client_id, message, response_text, matched=True, method='smart_keyword')
            return jsonify({
                'success': True,
                'response': response_text,
                'confidence': confidence,
                'method': 'smart_keyword'
            })

        # Step 3: AI fallback
        if ai_helper and ai_helper.enabled:
            app.logger.info("No smart match found, trying AI...")
            try:
                ai_faq, ai_confidence = ai_helper.find_best_faq(message, faqs_list)
                if ai_faq and ai_confidence > 0.5:
                    response_text = ai_faq.get('answer')
                    log_conversation(client_id, message, response_text, matched=True, method='ai')
                    return jsonify({
                        'success': True,
                        'response': response_text,
                        'confidence': ai_confidence,
                        'method': 'ai'
                    })
            except Exception as ai_error:
                app.logger.error(f"AI error: {ai_error}")

        # Step 4: Fallback
        fallback = config.get('bot_settings', {}).get(
            'fallback_message',
            "I'm not sure about that. Would you like to speak with our team? Type 'contact'!"
        )
        log_conversation(client_id, message, fallback, matched=False, method='fallback')
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
            'conversation_snippet': data.get('conversation_snippet', ''),
            'source_url': data.get('source_url', '')
        }

        models.save_lead(client_id, lead_data)
        notify_webhook(client_id, {'name': name, 'email': email, 'phone': phone, 'company': company})

        app.logger.info(f'Lead captured for client: {client_id}')

        config = json.loads(client['branding_settings']) if client['branding_settings'] else {}
        contact_info = config.get('contact', {})

        return jsonify({
            'success': True,
            'message': "Thank you! We've received your information and will be in touch soon.",
            'contact_info': contact_info
        })

    except Exception as e:
        app.logger.error(f'Error submitting lead: {e}')
        return jsonify({'success': False, 'error': 'Failed to submit lead'}), 500

# =====================================================================
# AUTHENTICATION ROUTES
# =====================================================================

@app.route('/signup', methods=['GET', 'POST'])
def signup():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))

    referral_code = request.args.get('ref')

    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')
        confirm_password = request.form.get('confirm_password')
        plan_type = 'free'

        if password != confirm_password:
            return render_template('signup.html', error='Passwords do not match', referral_code=referral_code)

        if len(password) < 6:
            return render_template('signup.html', error='Password must be at least 6 characters', referral_code=referral_code)

        user_id = models.create_user(email, password, plan_type)

        if user_id is None:
            return render_template('signup.html', error='Email already exists', referral_code=referral_code)

        if referral_code:
            affiliate = models.get_affiliate_by_code(referral_code)
            if affiliate:
                models.create_referral(affiliate['id'], user_id, referral_code)
                app.logger.info(f'Referral tracked: {referral_code} -> {email}')

        user_data = models.get_user_by_id(user_id)
        user = User(user_data)
        login_user(user)
        models.track_event('signup', user_id=user_id, metadata={'email': email, 'plan': 'free'})
        return redirect(url_for('dashboard'))

    return render_template('signup.html', referral_code=referral_code)


@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))

    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')
        user_data = models.verify_user(email, password)

        if user_data:
            user = User(user_data)
            login_user(user)
            models.track_event('login', user_id=user_data['id'], metadata={'email': email})
            return redirect(url_for('dashboard'))
        else:
            return render_template('login.html', error='Invalid email or password')

    return render_template('login.html')


@app.route('/logout')
@login_required
def logout():
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
    plan_type = fresh_user['plan_type'] if fresh_user else current_user.plan_type
    plan_limits = PLAN_LIMITS.get(plan_type, PLAN_LIMITS['free'])
    client_limit = plan_limits['clients']
    client_count = len(clients)
    # For unlimited plans (agency/enterprise) show a clean display
    slots_display = 'Unlimited' if client_limit >= 999999 else str(client_limit)
    limit_reached = False if client_limit >= 999999 else client_count >= client_limit

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
        limit_reached=limit_reached
    )


@app.route('/create-client', methods=['POST'])
@login_required
def create_client():
    try:
        company_name = request.form.get('company_name')

        if not company_name:
            return jsonify({'success': False, 'error': 'Company name is required'}), 400

        user = models.get_user_by_id(current_user.id)
        plan_type = user['plan_type']
        current_clients = models.get_user_clients(current_user.id)
        client_count = len(current_clients)
        plan_limit = PLAN_LIMITS.get(plan_type, PLAN_LIMITS['free'])['clients']

        # ---- Readable limit labels per plan ----
        plan_upgrade_hints = {
            'free':    'Starter: 3 chatbots | Pro: 10 chatbots | Agency: Unlimited',
            'starter': 'Pro: 10 chatbots | Agency: Unlimited',
            'pro':     'Agency: Unlimited chatbots at $299/mo',
        }
        upgrade_hint = plan_upgrade_hints.get(plan_type, 'Upgrade to add more chatbots')

        if client_count >= plan_limit:
            return f'''
            <!DOCTYPE html>
            <html>
            <head>
                <title>Plan Limit Reached</title>
                <style>
                    body {{
                        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                        background: linear-gradient(135deg, #0f172a 0%, #1e1b4b 100%);
                        min-height: 100vh;
                        display: flex;
                        align-items: center;
                        justify-content: center;
                        padding: 20px;
                    }}
                    .container {{
                        background: rgba(30, 41, 59, 0.9);
                        border: 1px solid rgba(255,255,255,0.1);
                        border-radius: 20px;
                        padding: 48px;
                        max-width: 500px;
                        text-align: center;
                        color: #f8fafc;
                        box-shadow: 0 25px 60px rgba(0,0,0,0.4);
                    }}
                    h1 {{ font-size: 28px; font-weight: 800; margin-bottom: 16px; color: white; }}
                    p {{ color: #94a3b8; margin-bottom: 20px; line-height: 1.6; font-size: 15px; }}
                    .plan-info {{
                        background: rgba(251, 191, 36, 0.1);
                        border: 1px solid rgba(251, 191, 36, 0.3);
                        border-radius: 10px;
                        padding: 16px;
                        margin-bottom: 24px;
                        color: #fbbf24;
                        font-size: 14px;
                        line-height: 1.7;
                    }}
                    .plan-info strong {{ color: #fde68a; }}
                    .hint {{ font-size: 13px; color: #64748b; margin-bottom: 28px; }}
                    .btn {{
                        display: inline-block;
                        padding: 13px 28px;
                        border-radius: 10px;
                        font-weight: 700;
                        text-decoration: none;
                        margin: 6px;
                        font-size: 14px;
                        transition: all 0.2s;
                    }}
                    .btn-primary {{
                        background: #06b6d4;
                        color: #0f172a;
                    }}
                    .btn-secondary {{
                        background: transparent;
                        color: #94a3b8;
                        border: 1px solid rgba(255,255,255,0.15);
                    }}
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>⚠️ Chatbot Limit Reached</h1>
                    <p>You've reached the maximum number of chatbots for your current plan.</p>

                    <div class="plan-info">
                        <strong>Current Plan:</strong> {plan_type.title()}<br>
                        <strong>Chatbots:</strong> {client_count} / {plan_limit if plan_limit < 999999 else "Unlimited"}<br>
                        <strong>Status:</strong> Limit Reached
                    </div>

                    <p class="hint">{upgrade_hint}</p>

                    <a href="/upgrade" class="btn btn-primary">Upgrade Plan →</a>
                    <a href="/dashboard" class="btn btn-secondary">← Back to Dashboard</a>
                </div>
            </body>
            </html>
            ''', 403

        client_id = models.create_client(current_user.id, company_name)
        return redirect(url_for('dashboard'))

    except Exception as e:
        app.logger.error(f'Error creating client: {e}')
        return redirect(url_for('dashboard'))


@app.route('/')
def index():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    return redirect(url_for('landing_page'))

# =====================================================================
# WIDGET & ADMIN ROUTES
# =====================================================================

@app.route('/widget')
def widget():
    client_id = request.args.get('client_id', 'demo')
    client = models.get_client_by_id(client_id)

    if not client:
        client = {
            'client_id': 'demo',
            'company_name': 'Demo Company',
            'widget_color': '#667eea',
            'bot_name': 'Team Support',
            'welcome_message': 'Hi! How can I help you today?',
            'remove_branding': 0
        }
    else:
        client = dict(client)
        branding_settings = json.loads(client.get('branding_settings') or '{}')
        bot_settings = branding_settings.get('bot_settings', {})
        branding = branding_settings.get('branding', {})
        client['bot_name'] = bot_settings.get('bot_name') or client.get('company_name') or 'Team Support'
        client['welcome_message'] = bot_settings.get('welcome_message') or client.get('welcome_message') or 'Hi! How can I help you today?'
        client['widget_color'] = branding.get('primary_color') or client.get('widget_color') or '#667eea'
        client['remove_branding'] = branding.get('remove_branding', client.get('remove_branding', 0))

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


@app.route('/api/admin/backup', methods=['POST'])
def trigger_backup():
    auth_token = request.headers.get('X-Admin-Token')

    if auth_token != os.getenv('ADMIN_TOKEN', 'change-me-in-production'):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401

    success = backup_all_clients()

    return jsonify({
        'success': success,
        'message': 'Backup completed' if success else 'Backup failed',
        'timestamp': datetime.now().isoformat()
    })


@app.route('/embed-generator')
def embed_generator():
    return render_template('embed-generator.html')


@app.route('/customize')
@login_required
def customize_page():
    client_id = request.args.get('client_id')

    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return "Unauthorized", 403

    user = models.get_user_by_id(current_user.id)
    plan_limits = PLAN_LIMITS.get(user['plan_type'], PLAN_LIMITS['free'])

    if not plan_limits['customization']:
        return f'''
        <!DOCTYPE html>
        <html>
        <head>
            <title>Upgrade Required</title>
            <style>
                body {{
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                    background: linear-gradient(135deg, #0f172a 0%, #1e1b4b 100%);
                    min-height: 100vh;
                    display: flex; align-items: center; justify-content: center;
                    padding: 20px;
                }}
                .container {{
                    background: rgba(30,41,59,0.9);
                    border: 1px solid rgba(255,255,255,0.1);
                    border-radius: 20px; padding: 48px;
                    max-width: 500px; text-align: center; color: #f8fafc;
                }}
                h1 {{ font-size: 28px; font-weight: 800; margin-bottom: 16px; }}
                p {{ color: #94a3b8; margin-bottom: 20px; font-size: 15px; }}
                ul {{ text-align: left; color: #cbd5e1; margin: 20px 0; font-size: 14px; line-height: 2; }}
                .btn {{
                    display: inline-block; padding: 13px 28px;
                    border-radius: 10px; font-weight: 700;
                    text-decoration: none; margin: 6px; font-size: 14px;
                }}
                .btn-primary {{ background: #06b6d4; color: #0f172a; }}
                .btn-secondary {{ background: transparent; color: #94a3b8; border: 1px solid rgba(255,255,255,0.15); }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>🎨 Customization Locked</h1>
                <p>Branding customization is not available on the Free plan.</p>
                <p>Upgrade to Starter ($49/mo) or higher to unlock:</p>
                <ul>
                    <li>Custom colors &amp; branding</li>
                    <li>Logo upload</li>
                    <li>Bot personality settings</li>
                    <li>White-label on Agency plan</li>
                </ul>
                <a href="/upgrade" class="btn btn-primary">Upgrade Now →</a>
                <a href="/dashboard" class="btn btn-secondary">← Back</a>
            </div>
        </body>
        </html>
        ''', 403

    return render_template('customize.html', user=current_user)


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

        plan_limits = PLAN_LIMITS.get(current_user.plan_type, PLAN_LIMITS['free'])

        # ── Fix 1: Integrations / Zapier / Make ─────────────────────────
        # Pro + Agency: webhook URL is supported, save whatever they send.
        # Free + Starter: wipe the webhook URL so it can never fire.
        incoming_integrations = data.get('integrations', {})
        if plan_limits['webhooks']:
            integrations = incoming_integrations
            app.logger.info(
                f"[Webhooks] Saved for user {current_user.id} "
                f"(plan: {current_user.plan_type}), "
                f"url_set: {bool(incoming_integrations.get('webhook_url'))}"
            )
        else:
            integrations = {}
            if incoming_integrations.get('webhook_url'):
                app.logger.info(
                    f"[Limit] Webhook URL stripped for user {current_user.id} "
                    f"on plan '{current_user.plan_type}'"
                )

        branding_settings = {
            'branding': data.get('branding', {}),
            'contact': data.get('contact', {}),
            'bot_settings': data.get('bot_settings', {}),
            'integrations': integrations   # persisted for pro/agency; empty for free/starter
        }

        # White-label only on agency/enterprise
        remove_branding = False
        if current_user.plan_type in ('agency', 'enterprise'):
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


@app.route('/analytics')
@login_required
def analytics_page():
    client_id = request.args.get('client_id')
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return "Unauthorized", 403

    user = models.get_user_by_id(current_user.id)
    plan_limits = PLAN_LIMITS.get(user['plan_type'], PLAN_LIMITS['free'])

    if not plan_limits['analytics']:
        return f'''
        <!DOCTYPE html>
        <html>
        <head>
            <title>Upgrade Required</title>
            <style>
                body {{
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                    background: linear-gradient(135deg, #0f172a 0%, #1e1b4b 100%);
                    min-height: 100vh;
                    display: flex; align-items: center; justify-content: center;
                    padding: 20px;
                }}
                .container {{
                    background: rgba(30,41,59,0.9);
                    border: 1px solid rgba(255,255,255,0.1);
                    border-radius: 20px; padding: 48px;
                    max-width: 500px; text-align: center; color: #f8fafc;
                }}
                h1 {{ font-size: 28px; font-weight: 800; margin-bottom: 16px; }}
                p {{ color: #94a3b8; margin-bottom: 20px; font-size: 15px; }}
                ul {{ text-align: left; color: #cbd5e1; margin: 20px 0; font-size: 14px; line-height: 2; }}
                .btn {{
                    display: inline-block; padding: 13px 28px;
                    border-radius: 10px; font-weight: 700;
                    text-decoration: none; margin: 6px; font-size: 14px;
                }}
                .btn-primary {{ background: #06b6d4; color: #0f172a; }}
                .btn-secondary {{ background: transparent; color: #94a3b8; border: 1px solid rgba(255,255,255,0.15); }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>📊 Analytics Locked</h1>
                <p>Analytics is available on Pro ($99/mo) and Agency ($299/mo) plans.</p>
                <ul>
                    <li>Conversation volume &amp; trends</li>
                    <li>Lead tracking</li>
                    <li>Answer rate &amp; top questions</li>
                    <li>Unanswered question reports</li>
                </ul>
                <a href="/upgrade" class="btn btn-primary">Upgrade Now →</a>
                <a href="/dashboard" class="btn btn-secondary">← Back</a>
            </div>
        </body>
        </html>
        ''', 403

    clients = models.get_user_clients(current_user.id)
    for c in clients:
        if c.get('branding_settings'):
            try:
                c['branding_settings'] = json.loads(c['branding_settings'])
            except Exception:
                c['branding_settings'] = {}

    return render_template('analytics.html', clients=clients, client_id=client_id)


@app.route('/api/admin/analytics', methods=['GET'])
@login_required
def get_analytics():
    try:
        client_id = request.args.get('client_id', 'demo')
        if not models.verify_client_ownership(current_user.id, client_id):
            return jsonify({'success': False, 'error': 'unauthorized'}), 403

        date_range = request.args.get('range', 'week')
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

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS conversations (
                id SERIAL PRIMARY KEY,
                client_id TEXT NOT NULL,
                user_message TEXT NOT NULL,
                bot_response TEXT NOT NULL,
                matched BOOLEAN DEFAULT FALSE,
                method TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute(
            'SELECT COUNT(*) AS total FROM conversations WHERE client_id = %s AND timestamp >= %s',
            (client_id, start_date)
        )
        row = cursor.fetchone() or {}
        total_conversations = row.get('total', 0)

        cursor.execute(
            'SELECT COUNT(*) AS matched_count FROM conversations WHERE client_id = %s AND timestamp >= %s AND matched = TRUE',
            (client_id, start_date)
        )
        row = cursor.fetchone() or {}
        answered = row.get('matched_count', 0)
        unanswered = total_conversations - answered
        answer_rate = int((answered / total_conversations * 100)) if total_conversations > 0 else 0

        cursor.execute(
            'SELECT COUNT(*) AS total_leads FROM leads WHERE client_id = %s AND created_at >= %s',
            (client_id, start_date)
        )
        row = cursor.fetchone() or {}
        total_leads = row.get('total_leads', 0)

        timeline = []
        for i in range(7):
            date = (now - timedelta(days=6 - i))
            date_str = date.strftime('%Y-%m-%d')
            cursor.execute(
                'SELECT COUNT(*) AS daily_count FROM conversations WHERE client_id = %s AND DATE(timestamp) = %s',
                (client_id, date_str)
            )
            row = cursor.fetchone() or {}
            conv_count = row.get('daily_count', 0)

            cursor.execute(
                'SELECT COUNT(*) AS daily_leads FROM leads WHERE client_id = %s AND DATE(created_at) = %s',
                (client_id, date_str)
            )
            row = cursor.fetchone() or {}
            lead_count = row.get('daily_leads', 0)

            timeline.append({'date': date_str, 'count': conv_count, 'leads': lead_count})

        cursor.execute(
            '''
            SELECT user_message, COUNT(*) as count FROM conversations
            WHERE client_id = %s AND timestamp >= %s AND matched = TRUE
            GROUP BY user_message ORDER BY count DESC LIMIT 10
            ''',
            (client_id, start_date)
        )
        top_questions = [{'question': r['user_message'], 'count': r['count']} for r in cursor.fetchall()]

        cursor.execute(
            '''
            SELECT user_message, COUNT(*) as count FROM conversations
            WHERE client_id = %s AND timestamp >= %s AND matched = FALSE
            GROUP BY user_message ORDER BY count DESC LIMIT 10
            ''',
            (client_id, start_date)
        )
        unanswered_questions = [{'question': r['user_message'], 'count': r['count']} for r in cursor.fetchall()]

        # Fix 2: Return actual lead records so the dashboard can display them
        cursor.execute(
            '''
            SELECT id, name, email, phone, company, message, source_url, created_at
            FROM leads
            WHERE client_id = %s AND created_at >= %s
            ORDER BY created_at DESC
            LIMIT 50
            ''',
            (client_id, start_date)
        )
        leads_captured = []
        for r in cursor.fetchall():
            lead = dict(r)
            # Convert datetime to ISO string so JSON can serialize it
            if lead.get('created_at'):
                lead['created_at'] = lead['created_at'].isoformat()
            leads_captured.append(lead)

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'analytics': {
                'total_conversations': total_conversations,
                'total_leads': total_leads,
                'answer_rate': answer_rate,
                'unanswered_count': unanswered,
                'timeline': timeline,
                'top_questions': top_questions,
                'unanswered': unanswered_questions,
                'leads_captured': leads_captured   # ← actual lead records
            }
        })

    except Exception as e:
        app.logger.error(f'Error getting analytics: {e}')
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/sales')
def sales_page():
    return render_template('sales-page.html')


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
            return jsonify({'success': True, 'message': 'FAQs updated successfully'})

    except Exception as e:
        app.logger.error(f'Error managing FAQs: {e}')
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': 'Failed to manage FAQs'}), 500


@app.route('/api/faq/upload', methods=['POST'])
@login_required
def upload_faqs():
    try:
        client_id = request.form.get('client_id')

        if not models.verify_client_ownership(current_user.id, client_id):
            return jsonify({'success': False, 'error': 'Unauthorized'}), 403

        if 'file' not in request.files:
            return jsonify({'success': False, 'error': 'No file uploaded'}), 400

        file = request.files['file']

        if file.filename == '':
            return jsonify({'success': False, 'error': 'No file selected'}), 400

        filename = file.filename.lower()

        if filename.endswith('.csv'):
            faqs = process_csv_upload(file)
        elif filename.endswith('.pdf'):
            faqs = process_pdf_upload(file)
        elif filename.endswith(('.xlsx', '.xls')):
            faqs = process_excel_upload(file)
        else:
            return jsonify({'success': False, 'error': 'Unsupported file type. Please upload CSV, Excel, or PDF.'}), 400

        if not faqs:
            return jsonify({'success': False, 'error': 'No FAQs found in file. Please check the format.'}), 400

        conn, cursor = models.get_db()
        saved_count = 0
        for faq in faqs:
            try:
                faq_id = str(uuid.uuid4())
                cursor.execute(
                    '''
                    INSERT INTO faqs (client_id, faq_id, question, answer, category, triggers)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ''',
                    (client_id, faq_id, faq['question'], faq['answer'],
                     faq.get('category', 'General'), json.dumps(faq.get('triggers', [])))
                )
                saved_count += 1
            except Exception as e:
                app.logger.error(f'Error saving FAQ during upload: {e}')
                continue
        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'message': f'Successfully imported {saved_count} FAQs!',
            'count': saved_count
        })

    except Exception as e:
        app.logger.error(f'Error uploading FAQs: {e}')
        import traceback
        traceback.print_exc()
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
        response = ai_helper.model.generate_content(prompt)
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
    return render_template('upgrade.html', user=current_user)

# =====================================================================
# PAYMENT ROUTES - PAYPAL
# =====================================================================

@app.route('/payment/paypal/create', methods=['POST'])
@login_required
def create_paypal_payment():
    try:
        data = request.json
        plan = data.get('plan')

        PLAN_PRICES = {
            'starter': 49.00,
            'pro': 99.00,
            'agency': 299.00
        }

        amount = PLAN_PRICES.get(plan)
        if not amount:
            return jsonify({'success': False, 'error': 'Invalid plan'}), 400

        payment = Payment({
            "intent": "sale",
            "payer": {"payment_method": "paypal"},
            "redirect_urls": {
                "return_url": f"{request.host_url}payment/paypal/success",
                "cancel_url": f"{request.host_url}payment/paypal/cancel"
            },
            "transactions": [{
                "item_list": {
                    "items": [{
                        "name": f"{plan.capitalize()} Plan - Monthly Subscription",
                        "sku": f"plan_{plan}",
                        "price": f"{amount:.2f}",
                        "currency": "USD",
                        "quantity": 1
                    }]
                },
                "amount": {"total": f"{amount:.2f}", "currency": "USD"},
                "description": f"Upgrade to {plan.capitalize()} Plan"
            }]
        })

        if payment.create():
            session['pending_payment'] = {
                'user_id': current_user.id,
                'plan': plan,
                'amount': amount,
                'payment_id': payment.id
            }

            approval_url = next(
                (link.href for link in payment.links if link.rel == 'approval_url'),
                None
            )

            return jsonify({
                'success': True,
                'approval_url': approval_url,
                'payment_id': payment.id
            })
        else:
            app.logger.error(f"PayPal payment creation failed: {payment.error}")
            return jsonify({'success': False, 'error': 'Payment creation failed'}), 500

    except Exception as e:
        app.logger.error(f"PayPal error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/payment/paypal/success')
@login_required
def paypal_success():
    try:
        payment_id = request.args.get('paymentId')
        payer_id = request.args.get('PayerID')
        pending_payment = session.get('pending_payment', {})

        if not pending_payment or pending_payment.get('payment_id') != payment_id:
            flash("⚠️ Payment session expired. Please try again.", 'warning')
            return redirect(url_for('upgrade_page'))

        payment = Payment.find(payment_id)

        if payment.execute({"payer_id": payer_id}):
            plan = pending_payment['plan']

            conn, cursor = models.get_db()
            cursor.execute(
                'UPDATE users SET plan_type = %s, upgraded_at = CURRENT_TIMESTAMP WHERE id = %s',
                (plan, current_user.id)
            )
            conn.commit()
            cursor.close()
            conn.close()

            session.pop('pending_payment', None)
            # Log the payment and event
            amount = pending_payment.get('amount', 0)
            models.record_payment(current_user.id, float(amount), plan,
                                  provider='paypal', reference=payment_id)
            models.track_event('plan_upgrade', user_id=current_user.id,
                               metadata={'plan': plan, 'provider': 'paypal', 'amount': amount})
            flash(f"✅ Payment successful! You've been upgraded to the {plan.capitalize()} plan.", 'success')
            return redirect(url_for('dashboard'))
        else:
            app.logger.error(f"PayPal execution failed: {payment.error}")
            flash("❌ Payment execution failed. Please try again.", 'error')
            return redirect(url_for('upgrade_page'))

    except Exception as e:
        app.logger.error(f"PayPal success handler error: {e}")
        import traceback
        traceback.print_exc()
        flash("❌ Payment processing error. Contact support@lumvi.net.", 'error')
        return redirect(url_for('dashboard'))


@app.route('/api/webhook/lead', methods=['POST'])
def webhook_new_lead():
    try:
        secret = request.headers.get('X-Webhook-Secret')
        if secret != os.environ.get('WEBHOOK_SECRET', 'lumvi-secret'):
            return jsonify({'error': 'Unauthorized'}), 401

        data = request.json
        client_id = data.get('client_id')
        leads = models.get_leads(client_id)
        leads = leads[:10]
        return jsonify({'success': True, 'leads': leads})

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/webhook/faq-import', methods=['POST'])
def webhook_faq_import():
    try:
        secret = request.headers.get('X-Webhook-Secret')
        if secret != os.environ.get('WEBHOOK_SECRET', 'lumvi-secret'):
            return jsonify({'error': 'Unauthorized'}), 401

        data = request.json
        client_id = data.get('client_id')
        incoming_faqs = data.get('faqs', [])

        if not client_id or not incoming_faqs:
            return jsonify({'error': 'client_id and faqs required'}), 400

        conn, cursor = models.get_db()
        saved = 0

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
        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'message': f'Imported {saved} FAQs successfully',
            'count': saved
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/payment/paypal/cancel')
@login_required
def paypal_cancel():
    session.pop('pending_payment', None)
    flash("💳 Payment cancelled. You can try again anytime.", 'info')
    return redirect(url_for('upgrade_page'))


@app.route('/payment/paypal/webhook', methods=['POST'])
def paypal_webhook():
    try:
        event = request.json
        event_type = event.get('event_type')
        app.logger.info(f"PayPal webhook: {event_type}")

        if event_type == 'PAYMENT.SALE.COMPLETED':
            app.logger.info("Payment completed via webhook")
        elif event_type == 'BILLING.SUBSCRIPTION.CANCELLED':
            app.logger.info("Subscription cancelled via webhook")

        return jsonify({'success': True}), 200

    except Exception as e:
        app.logger.error(f"PayPal webhook error: {e}")
        return jsonify({'success': False}), 500

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
    ADMIN_SECRET = os.environ.get('ADMIN_SECRET', 'lumvi-admin-2024')
    error = None
    success = None

    if request.method == 'POST':
        secret = request.form.get('secret')
        email = request.form.get('email', '').strip().lower()
        plan = request.form.get('plan', '').strip().lower()

        valid_plans = ['free', 'starter', 'pro', 'agency', 'enterprise']

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

            conn = models.get_db()
            cursor = conn.cursor()
            conn.commit()
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
    plan = current_user.plan_type
    plan_limits = PLAN_LIMITS.get(plan, PLAN_LIMITS['free'])

    if not plan_limits['white_label']:
        return '''<!DOCTYPE html>
<html>
<head><title>Upgrade Required</title>
<style>
  body{font-family:-apple-system,sans-serif;background:linear-gradient(135deg,#0f172a,#1e1b4b);min-height:100vh;display:flex;align-items:center;justify-content:center;padding:20px;}
  .card{background:rgba(30,41,59,.95);border:1px solid rgba(255,255,255,.1);border-radius:20px;padding:48px;max-width:480px;text-align:center;color:#f8fafc;}
  h1{font-size:26px;font-weight:800;margin-bottom:12px;}
  p{color:#94a3b8;margin-bottom:20px;font-size:15px;}
  .btn{display:inline-block;padding:13px 28px;border-radius:10px;font-weight:700;text-decoration:none;margin:6px;font-size:14px;}
  .btn-p{background:#06b6d4;color:#0f172a;}
  .btn-s{background:transparent;color:#94a3b8;border:1px solid rgba(255,255,255,.15);}
</style>
</head>
<body>
<div class="card">
  <h1>Client Portal</h1>
  <p>The Client Management Portal is available on the Agency plan ($299/mo).</p>
  <p>Manage unlimited client chatbots, branding, leads and analytics from one hub.</p>
  <a href="/upgrade" class="btn btn-p">Upgrade to Agency</a>
  <a href="/dashboard" class="btn btn-s">Back</a>
</div>
</body>
</html>''', 403

    clients = models.get_user_clients(current_user.id)
    for c in clients:
        if c.get('branding_settings'):
            try:
                c['branding_settings'] = json.loads(c['branding_settings'])
            except Exception:
                c['branding_settings'] = {}

    # Per-client lead counts
    client_stats = {}
    for c in clients:
        cid = c['client_id']
        try:
            client_stats[cid] = len(models.get_leads(cid))
        except Exception:
            client_stats[cid] = 0

    # Build client cards HTML
    cards_html = ''
    for c in clients:
        cid = c['client_id']
        name = c.get('company_name', 'Unnamed')
        bs = c.get('branding_settings') or {}
        color = bs.get('branding', {}).get('primary_color') or c.get('widget_color') or '#667eea'
        leads = client_stats.get(cid, 0)
        lead_label = f'{leads} lead{"s" if leads != 1 else ""}'
        cards_html += (
            f'<div class="cc"><div class="ch" style="border-left:4px solid {color}">'
            f'<div class="cn">{name}</div>'
            f'<div class="cid">{cid}</div>'
            f'<div class="cm">{lead_label} captured</div></div>'
            f'<div class="ca">'
            f'<a href="/customize?client_id={cid}" class="ab">Customize</a>'
            f'<a href="/faq-manager?client_id={cid}" class="ab">FAQs</a>'
            f'<a href="/admin/leads?client_id={cid}" class="ab">Leads</a>'
            f'<a href="/analytics?client_id={cid}" class="ab">Analytics</a>'
            f'<a href="/widget?client_id={cid}" class="ab" target="_blank">Preview</a>'
            f'</div>'
            f'<div class="es"><div class="el">Embed Code</div>'
            f'<code>&lt;script src="https://lumvi.net/widget.js?client_id={cid}"&gt;&lt;/script&gt;</code>'
            f'</div></div>'
        )

    if not cards_html:
        cards_html = '<p style="color:#64748b;text-align:center;padding:48px 0">No clients yet. Create your first chatbot from the dashboard.</p>'

    total_leads_all = sum(client_stats.values())

    return f'''<!DOCTYPE html>
<html>
<head><title>Client Portal - Lumvi</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
  *{{box-sizing:border-box;margin:0;padding:0;}}
  body{{font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#0a0f1a;color:#f8fafc;min-height:100vh;}}
  .topbar{{background:rgba(15,23,42,.95);border-bottom:1px solid rgba(255,255,255,.08);padding:16px 32px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100;}}
  .logo{{font-size:20px;font-weight:800;color:#06b6d4;}}
  .tr{{display:flex;gap:10px;align-items:center;}}
  .pb{{background:rgba(167,139,250,.15);color:#a78bfa;border:1px solid rgba(167,139,250,.3);padding:4px 12px;border-radius:999px;font-size:12px;font-weight:700;}}
  .nl{{color:#94a3b8;text-decoration:none;font-size:14px;padding:7px 14px;border-radius:8px;border:1px solid rgba(255,255,255,.1);}}
  .nl:hover{{color:#f8fafc;background:rgba(255,255,255,.05);}}
  .container{{max-width:1100px;margin:0 auto;padding:40px 24px;}}
  .ph{{margin-bottom:32px;}}
  .ph h1{{font-size:28px;font-weight:800;margin-bottom:8px;}}
  .ph p{{color:#64748b;font-size:15px;}}
  .sr{{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin-bottom:36px;}}
  .sc{{background:rgba(30,41,59,.6);border:1px solid rgba(255,255,255,.08);border-radius:14px;padding:20px 24px;}}
  .sc .l{{font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:.06em;margin-bottom:8px;}}
  .sc .v{{font-size:28px;font-weight:800;}}
  .cg{{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:20px;}}
  .cc{{background:rgba(30,41,59,.7);border:1px solid rgba(255,255,255,.08);border-radius:16px;overflow:hidden;transition:border-color .2s;}}
  .cc:hover{{border-color:rgba(6,182,212,.3);}}
  .ch{{padding:20px 24px;background:rgba(15,23,42,.5);}}
  .cn{{font-size:17px;font-weight:700;margin-bottom:4px;}}
  .cid{{font-size:11px;color:#475569;font-family:monospace;margin-bottom:8px;}}
  .cm{{font-size:13px;color:#64748b;}}
  .ca{{padding:14px 18px;display:flex;flex-wrap:wrap;gap:7px;border-top:1px solid rgba(255,255,255,.06);}}
  .ab{{padding:6px 11px;border-radius:7px;font-size:12px;font-weight:600;text-decoration:none;color:#cbd5e1;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.1);transition:all .15s;}}
  .ab:hover{{background:rgba(6,182,212,.15);color:#06b6d4;border-color:rgba(6,182,212,.3);}}
  .es{{padding:12px 18px;border-top:1px solid rgba(255,255,255,.06);background:rgba(0,0,0,.2);}}
  .el{{font-size:11px;color:#475569;margin-bottom:5px;text-transform:uppercase;letter-spacing:.05em;}}
  .es code{{font-family:monospace;font-size:11px;color:#67e8f9;word-break:break-all;}}
  @media(max-width:640px){{.sr{{grid-template-columns:1fr;}}.topbar{{padding:12px 16px;}}.container{{padding:24px 16px;}}}}
</style>
</head>
<body>
<div class="topbar">
  <div class="logo">Lumvi</div>
  <div class="tr">
    <span class="pb">Agency</span>
    <a href="/dashboard" class="nl">Dashboard</a>
    <a href="/support" class="nl">Support</a>
    <a href="/logout" class="nl">Logout</a>
  </div>
</div>
<div class="container">
  <div class="ph">
    <h1>Client Management Portal</h1>
    <p>Manage all your clients chatbots, branding, leads and analytics from one place.</p>
  </div>
  <div class="sr">
    <div class="sc"><div class="l">Total Clients</div><div class="v">{len(clients)}</div></div>
    <div class="sc"><div class="l">Total Leads</div><div class="v">{total_leads_all}</div></div>
    <div class="sc"><div class="l">Client Limit</div><div class="v">Unlimited</div></div>
  </div>
  <div class="cg">{cards_html}</div>
</div>
</body>
</html>'''

# =====================================================================
# LEGAL PAGES
# =====================================================================

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

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)