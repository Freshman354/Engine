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
import sqlite3
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

# Add this line!
# It attempts to get the key from the environment, 
# or falls back to a dev key if running locally.
app.config['SECRET_KEY'] = os.environ.get("SECRET_KEY", "dev_key_change_this_in_prod")  # ✅

# Initialize AI helper at app startup
ai_helper = get_ai_helper(Config.GEMINI_API_KEY, Config.GEMINI_MODEL)

if ai_helper and ai_helper.enabled:
    app.logger.info("✅ Gemini AI initialized")

    # Initialize PayPal SDK

configure({
    "mode": os.getenv('PAYPAL_MODE', 'sandbox'),  # sandbox or live
    "client_id": os.getenv('PAYPAL_CLIENT_ID'),
    "client_secret": os.getenv('PAYPAL_CLIENT_SECRET')
})

USE_AI = Config.USE_AI

# Plan limits
PLAN_LIMITS = {
    'free': {
        'clients': 1,
        'faqs_per_client': 5,
        'messages_per_day': 50,
        'analytics': False,
        'customization': False,
        'priority_support': False
    },
    'starter': {
        'clients': 5,
        'faqs_per_client': 999,
        'messages_per_day': 999999,
        'analytics': True,
        'customization': True,
        'priority_support': False
    },
    'agency': {
        'clients': 15,
        'faqs_per_client': 999,
        'messages_per_day': 999999,
        'analytics': True,
        'customization': True,
        'priority_support': True
    },
    'enterprise': {
        'clients': 999999,
        'faqs_per_client': 999,
        'messages_per_day': 999999,
        'analytics': True,
        'customization': True,
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

@login_manager.user_loader
def load_user(user_id):
    user_data = models.get_user_by_id(int(user_id))
    if user_data:
        return User(user_data)
    return None

# Initialize database on startup
try:
    models.init_db()
    print("✅ Database initialized successfully!")
except Exception as e:
    print(f"⚠️ Database initialization error: {e}")

# Enhanced CORS configuration for production
CORS(app, resources={
    r"/api/*": {
        "origins": "*",  # Allow all origins for embedded widget
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

# Rate limiting configuration
limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["200 per day", "50 per hour"],
    storage_uri="memory://"
)




STOP_WORDS = {
    # Articles & Pronouns
    'a', 'an', 'the', 'this', 'that', 'these', 'those',
    'i', 'me', 'my', 'we', 'our', 'you', 'your', 'it', 'its',
    'he', 'she', 'they', 'them', 'their',
    # Generic verbs
    'is', 'are', 'was', 'were', 'be', 'been', 'being',
    'do', 'does', 'did', 'have', 'has', 'had',
    'can', 'could', 'will', 'would', 'should', 'shall', 'may', 'might',
    'go', 'get', 'got', 'make', 'made', 'use', 'used',
    # Prepositions & conjunctions
    'in', 'on', 'at', 'by', 'for', 'with', 'about',
    'to', 'from', 'into', 'through', 'before', 'after',
    'of', 'off', 'out', 'over', 'under', 'and', 'or', 'but',
    'if', 'as', 'than', 'because', 'while',
    # Generic adverbs
    'just', 'also', 'too', 'very', 'really', 'quite', 'already',
    'still', 'ever', 'never', 'always', 'often',
    # ⚠️ TIME NOISE - these caused your Business Hours false positives!
    'anytime', 'sometime', 'whenever', 'now', 'then', 'when',
    'today', 'tomorrow', 'time', 'times',
    # Other noise
    'please', 'thanks', 'thank', 'hello', 'hi', 'hey',
    'like', 'want', 'need', 'know', 'tell', 'show', 'help',
    'what', 'where', 'which', 'who', 'why', 'how',
    'any', 'all', 'some', 'more', 'most', 'many', 'much',
    'no', 'not', 'nor', 'there', 'per', 'each'
}

# Tags that appear in many FAQs - matching these scores LOW
GENERIC_TAGS = {
    'schedule', 'appointment', 'book', 'available', 'availability',
    'open', 'closed', 'contact', 'reach', 'support', 'help',
    'information', 'info', 'details', 'more', 'learn'
}


def extract_keywords(text):
    """Extract meaningful keywords - removes stop words and short words"""
    words = re.findall(r'\b[a-z]+\b', text.lower())
    return [w for w in words if w not in STOP_WORDS and len(w) >= 3]


def compute_tag_weights(faqs_list):
    """Rare tags score HIGH, common tags score LOW"""
    tag_frequency = Counter()
    for faq in faqs_list:
        for tag in faq.get('triggers', []):
            tag_frequency[tag.lower()] += 1

    tag_weights = {}
    for tag, freq in tag_frequency.items():
        if tag in GENERIC_TAGS:
            tag_weights[tag] = 0.2  # Generic = low weight
        else:
            tag_weights[tag] = round(1.0 / freq, 2)  # Rare = high weight
    return tag_weights


def find_best_match(user_query, faqs_list, confidence_threshold=0.15):
    """
    Smart FAQ matching with stop word filtering and confidence threshold.
    Returns (faq, score) or (None, 0.0) if no confident match found.
    """
    if not user_query or not faqs_list:
        return None, 0.0

    # Step 1: Filter noise from user query
    # "Can I modify FAQs anytime?" → ['modify', 'faqs']
    query_keywords = extract_keywords(user_query)

    if not query_keywords:
        return None, 0.0  # All words were noise

    query_keyword_set = set(query_keywords)

    # Step 2: Calculate how rare each tag is
    tag_weights = compute_tag_weights(faqs_list)

    # Step 3: Score every FAQ
    best_faq = None
    best_score = 0.0

    for faq in faqs_list:
        # Combine explicit triggers + keywords from the question text
        raw_tags = [t.lower().strip() for t in faq.get('triggers', [])]
        question_keywords = extract_keywords(faq.get('question', ''))
        all_tags = set(raw_tags + question_keywords)

        # Find overlapping keywords
        matched_tags = query_keyword_set.intersection(all_tags)
        if not matched_tags:
            continue

        # Score = sum of weights of matched tags
        raw_score = sum(tag_weights.get(tag, 0.5) for tag in matched_tags)

        # Normalize against max possible score for this FAQ
        max_possible = sum(tag_weights.get(tag, 0.5) for tag in all_tags)
        normalized = raw_score / max_possible if max_possible > 0 else 0.0

        # Bonus for matching more of the user's keywords
        coverage = len(matched_tags) / len(query_keyword_set)
        final_score = (normalized * 0.7) + (coverage * 0.3)

        if final_score > best_score:
            best_score = final_score
            best_faq = faq

    # Step 4: Only return if confident enough
    if best_score < confidence_threshold:
        app.logger.info(f"[Matcher] Low confidence ({best_score:.2f}) for: '{user_query}'")
        return None, 0.0

    app.logger.info(f"[Matcher] Matched '{best_faq.get('question')}' | score: {best_score:.2f}")
    return best_faq, round(best_score, 2)




def notify_webhook(client_id, lead_data):
    """Send lead data to client's configured webhook (Zapier/Make)"""
    try:
        conn = sqlite3.connect('chatbot.db')
        cursor = conn.cursor()

        # Get client's webhook URL (store this in branding_settings)
        cursor.execute("SELECT branding_settings FROM clients WHERE client_id = ?", (client_id,))
        row = cursor.fetchone()
        conn.close()

        if not row:
            return

        config = json.loads(row[0]) if row[0] else {}
        webhook_url = config.get('integrations', {}).get('webhook_url')

        if not webhook_url:
            return  # Client hasn't set up a webhook

        # Send lead to their Zapier/Make webhook
        requests.post(webhook_url, json={
            'event': 'new_lead',
            'client_id': client_id,
            'lead': lead_data,
            'timestamp': datetime.now().isoformat()
        }, timeout=5)

        app.logger.info(f'✅ Webhook fired for {client_id}')

    except Exception as e:
        app.logger.error(f'Webhook error: {e}')
        # Don't crash the main flow if webhook fails!


#from flask import Flask, request, jsonify, render_template
#app = Flask(__name__)

# ADD THIS IMMEDIATELY AFTER app = Flask(__name__)
@app.after_request
def allow_widget_embedding(response):
    """Allow the Lumvi widget to be embedded on any website"""
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
# LOGGING CONFIGURATION
# =====================================================================

# Create logs directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Configure file handler
file_handler = RotatingFileHandler(
    'logs/chatbot.log',
    maxBytes=10240000,  # 10MB
    backupCount=10
)
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
))
file_handler.setLevel(logging.INFO)

# Add handler to app
app.logger.addHandler(file_handler)
app.logger.setLevel(logging.INFO)
app.logger.info('Chatbot startup')

# =====================================================================
# BACKUP SYSTEM
# =====================================================================

def backup_client_data(client_id):
    """Backup client data to backups folder"""
    try:
        client_path = get_client_path(client_id)
        backup_dir = os.path.join('backups', client_id)
        
        # Create backup directory if it doesn't exist
        if not os.path.exists('backups'):
            os.makedirs('backups')
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
        
        # Create timestamped backup
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = os.path.join(backup_dir, f'backup_{timestamp}')
        
        # Copy entire client folder
        shutil.copytree(client_path, backup_path)
        
        # Keep only last 7 backups
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
    """Backup all client data"""
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
    
def log_conversation(client_id, message, response, matched_faq_id=None):
    """Log conversation for analytics"""
    try:
        log_dir = os.path.join('clients', client_id, 'analytics')
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # Use daily log files
        today = datetime.now().strftime('%Y-%m-%d')
        log_file = os.path.join(log_dir, f'conversations_{today}.json')
        
        # Load existing logs
        if os.path.exists(log_file):
            with open(log_file, 'r', encoding='utf-8') as f:
                logs = json.load(f)
        else:
            logs = {'conversations': []}
        
        # Add new conversation
        logs['conversations'].append({
            'timestamp': datetime.now().isoformat(),
            'user_message': message,
            'bot_response': response[:200],  # Truncate long responses
            'matched_faq': matched_faq_id,
            'matched': matched_faq_id is not None
        })
        
        # Save
        with open(log_file, 'w', encoding='utf-8') as f:
            json.dump(logs, f, indent=2)
        
        return True
    except Exception as e:
        app.logger.error(f'Error logging conversation: {e}')
        return False   

# =====================================================================
# UTILITY FUNCTIONS
# =====================================================================

def get_client_path(client_id):
    """Get the path to client data directory"""
    client_path = os.path.join('clients', client_id)
    if not os.path.exists(client_path):
        client_path = os.path.join('clients', 'default')
    return client_path

def load_client_config(client_id):
    """Load client configuration"""
    config_path = os.path.join(get_client_path(client_id), 'config.json')
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading config for {client_id}: {e}")
        return None

def load_client_faqs(client_id):
    """Load client FAQs"""
    faqs_path = os.path.join(get_client_path(client_id), 'faqs.json')
    try:
        with open(faqs_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading FAQs for {client_id}: {e}")
        return {"faqs": []}

def save_lead(client_id, lead_data):
    """Save lead to JSON file"""
    leads_path = os.path.join(get_client_path(client_id), 'leads.json')
    try:
        # Load existing leads
        with open(leads_path, 'r', encoding='utf-8') as f:
            leads_file = json.load(f)
        
        # Add new lead
        lead_data['id'] = f"lead_{len(leads_file['leads']) + 1}"
        lead_data['timestamp'] = datetime.now().isoformat()
        lead_data['client_id'] = client_id
        leads_file['leads'].append(lead_data)
        
        # Save back to file
        with open(leads_path, 'w', encoding='utf-8') as f:
            json.dump(leads_file, f, indent=2)
        
        return True
    except Exception as e:
        print(f"Error saving lead for {client_id}: {e}")
        return False

def is_email(text):
    """Check if text contains an email address"""
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    return re.search(email_pattern, text) is not None

def extract_email(text):
    """Extract email from text"""
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    match = re.search(email_pattern, text)
    return match.group(0) if match else None

def match_faq(message, faqs, lead_triggers):
    """Match user message to FAQ based on triggers with fuzzy matching"""
    message_lower = message.lower()
    
    # Check for lead collection triggers first
    for trigger in lead_triggers:
        if trigger.lower() in message_lower:
            return "TRIGGER_LEAD_COLLECTION", None
    
    # Check if message contains an email (user providing contact info)
    if is_email(message):
        return "TRIGGER_LEAD_COLLECTION", extract_email(message)
    
    # Match against FAQ triggers with scoring
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
            all_matches.append({
                'faq': faq,
                'score': score,
                'matches': matches
            })
        
        if score > max_score:
            max_score = score
            best_match = faq
    
    if best_match and max_score > 0:
        return best_match['answer'], None
    
    # No match found - return similar questions if any partial matches
    if all_matches:
        similar_questions = [m['faq']['question'] for m in all_matches[:3]]
        return "NO_MATCH_WITH_SUGGESTIONS", similar_questions
    
    return None, None


def log_conversation(client_id, user_message, bot_response, matched=False, method='unknown'):
    """Log conversation to database for analytics"""
    try:
        # ✅ Direct connection
        conn = sqlite3.connect('chatbot.db')
        cursor = conn.cursor()
        
        # Create conversations table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS conversations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id TEXT NOT NULL,
                user_message TEXT NOT NULL,
                bot_response TEXT NOT NULL,
                matched BOOLEAN DEFAULT 0,
                method TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Insert conversation
        cursor.execute('''
            INSERT INTO conversations (client_id, user_message, bot_response, matched, method)
            VALUES (?, ?, ?, ?, ?)
        ''', (client_id, user_message, bot_response, matched, method))
        
        conn.commit()
        conn.close()
        
        app.logger.info(f'✅ Logged conversation for {client_id}')
        
    except Exception as e:
        app.logger.error(f'❌ Error logging conversation: {e}')

# =====================================================================
# API ENDPOINTS
# =====================================================================

@app.route('/api/config', methods=['GET'])
def get_config():
    """Get client configuration for widget"""
    try:
        client_id = request.args.get('client_id', 'default')
        
        # Get client from database
        client = models.get_client_by_id(client_id)
        
        if not client:
            return jsonify({
                'success': False,
                'error': 'Client not found'
            }), 404
        
        # Parse branding settings
        branding_settings = json.loads(client['branding_settings']) if client['branding_settings'] else {}
        
        # Build config response
        config = {
            'client_id': client_id,
            'branding': branding_settings.get('branding', {}),
            'contact': branding_settings.get('contact', {}),
            'bot_settings': branding_settings.get('bot_settings', {})
        }
        
        return jsonify({
            'success': True,
            'config': config
        })
        
    except Exception as e:
        app.logger.error(f'Error getting config: {e}')
        return jsonify({
            'success': False,
            'error': 'Failed to load configuration'
        }), 500
    
def sanitize_input(text, max_length=500):
    """Sanitize user input to prevent injection attacks"""
    if not text or not isinstance(text, str):
        return ""
    
    # Remove any HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    
    # Limit length
    text = text[:max_length]
    
    # Remove excessive whitespace
    text = ' '.join(text.split())
    
    return text.strip()

@app.route('/api/user/info')
@login_required
def user_info():
    """Get current user's plan info"""
    return jsonify({
        'success': True,
        'plan_type': current_user.plan_type,
        'email': current_user.email,
        'id': current_user.id
    })


@app.route('/api/chat', methods=['POST'])
@limiter.limit("30 per minute")
def chat():
    """Handle chat messages with FAST hybrid intelligence"""
    try:
        data = request.json
        
        # Sanitize inputs
        message = sanitize_input(data.get('message', ''))
        client_id = sanitize_input(data.get('client_id', 'demo'), max_length=50)
        conversation_history = data.get('history', [])
        
        if not message:
            return jsonify({
                'success': False,
                'error': 'Message is required'
            }), 400
        
        # Get client and FAQs from database
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
                        "answer": "Starter: $49/mo | Agency: $149/mo | Enterprise: Custom pricing! 💰",
                        "triggers": ["price", "pricing", "cost", "fee", "payment", "charge", "afford", "subscription"]
                    },
                    {
                        "id": "demo_3",
                        "question": "Do you offer discounts?",
                        "answer": "Yes! Annual plans get 20% off. Students/nonprofits get 30% off. 🎉",
                        "triggers": ["discount", "sale", "promo", "coupon", "deal", "cheaper", "reduce", "saving"]
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
        
        # Get lead triggers
        lead_triggers = config.get('bot_settings', {}).get('lead_triggers', ['contact', 'sales', 'demo', 'speak', 'talk'])
        
        # ============================================
        # HYBRID INTELLIGENCE (FAST + SMART)
        # ============================================
        
        message_lower = message.lower()
        
        # ── Step 1: INSTANT lead detection (unchanged) ──────────────────
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
        
        # ── Step 2: SMART keyword matching (replaces old simple loop) ───
        # Old code matched ANY trigger word — "anytime" matched Business Hours.
        # New code filters stop words, weights rare tags, and requires a
        # minimum confidence score before returning a match.
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
        
        # ── Step 3: AI-powered matching (only if smart matching failed) ─
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
        
        # ── Step 4: Fallback (no match found anywhere) ──────────────────
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
        return jsonify({
            'success': False,
            'error': 'Internal server error'
        }), 500    
        

@app.route('/api/lead', methods=['POST'])
@limiter.limit("10 per hour")
def submit_lead():
    """Submit lead information"""
    try:
        data = request.json
        
        # Sanitize inputs
        client_id = sanitize_input(data.get('client_id', 'default'), max_length=50)
        name = sanitize_input(data.get('name', ''), max_length=100)
        email = sanitize_input(data.get('email', ''), max_length=200)
        phone = sanitize_input(data.get('phone', ''), max_length=50)
        company = sanitize_input(data.get('company', ''), max_length=100)
        message = sanitize_input(data.get('message', ''), max_length=1000)
        
        # Validate required fields
        if not name or not email:
            return jsonify({
                'success': False,
                'error': 'Name and email are required'
            }), 400
        
        # Validate email format
        if not is_email(email):
            return jsonify({
                'success': False,
                'error': 'Invalid email format'
            }), 400
        
        # Get client config
        client = models.get_client_by_id(client_id)
        if not client:
            return jsonify({
                'success': False,
                'error': 'Client not found'
            }), 404
        
        # Prepare lead data
        lead_data = {
            'name': name,
            'email': email,
            'phone': phone,
            'company': company,
            'message': message,
            'conversation_snippet': data.get('conversation_snippet', ''),
            'source_url': data.get('source_url', '')
        }
        
        # Save lead to database
        models.save_lead(client_id, lead_data)
        
        # ✅ ADD THIS RIGHT HERE (3 lines below save_lead)
        notify_webhook(client_id, {
            'name': name,
            'email': email,
            'phone': phone,
            'company': company
        })
        
        app.logger.info(f'Lead captured for client: {client_id}')
        
        # Get contact info from config
        config = json.loads(client['branding_settings']) if client['branding_settings'] else {}
        contact_info = config.get('contact', {})
        
        return jsonify({
            'success': True,
            'message': "Thank you! We've received your information and will be in touch soon.",
            'contact_info': contact_info
        })
        
    except Exception as e:
        app.logger.error(f'Error submitting lead: {e}')
        return jsonify({
            'success': False,
            'error': 'Failed to submit lead'
        }), 500
            
    except Exception as e:
        print(f"Error in lead endpoint: {e}")
        return jsonify({
            'success': False,
            'error': 'Internal server error'
        }), 500

# =====================================================================
# AUTHENTICATION ROUTES
# =====================================================================

@app.route('/signup', methods=['GET', 'POST'])
def signup():
    """User signup"""
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    
    # Get referral code from URL
    referral_code = request.args.get('ref')
    
    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')
        confirm_password = request.form.get('confirm_password')
        
        # FORCE FREE PLAN
        plan_type = 'free'
        
        # Validation
        if password != confirm_password:
            return render_template('signup.html', error='Passwords do not match', referral_code=referral_code)
        
        if len(password) < 6:
            return render_template('signup.html', error='Password must be at least 6 characters', referral_code=referral_code)
        
        # Create user with FREE plan
        user_id = models.create_user(email, password, plan_type)
        
        if user_id is None:
            return render_template('signup.html', error='Email already exists', referral_code=referral_code)
        
        # Track referral if code provided
        if referral_code:
            affiliate = models.get_affiliate_by_code(referral_code)
            if affiliate:
                models.create_referral(affiliate['id'], user_id, referral_code)
                app.logger.info(f'Referral tracked: {referral_code} -> {email}')
        
        # Auto-login after signup
        user_data = models.get_user_by_id(user_id)
        user = User(user_data)
        login_user(user)
        
        return redirect(url_for('dashboard'))
    
    return render_template('signup.html', referral_code=referral_code)

@app.route('/login', methods=['GET', 'POST'])
def login():
    """User login"""
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    
    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')
        
        user_data = models.verify_user(email, password)
        
        if user_data:
            user = User(user_data)
            login_user(user)
            return redirect(url_for('dashboard'))
        else:
            return render_template('login.html', error='Invalid email or password')
    
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    """User logout"""
    logout_user()
    return redirect(url_for('login'))

@app.route('/dashboard')
@login_required
def dashboard():
    """User dashboard - shows all clients"""
    clients = models.get_user_clients(current_user.id)
    
    # Parse branding settings from JSON
    for client in clients:
        if client['branding_settings']:
            client['branding_settings'] = json.loads(client['branding_settings'])
    
    return render_template('dashboard.html', user=current_user, clients=clients)

@app.route('/create-client', methods=['POST'])
@login_required
def create_client():
    """Create a new client with plan limit enforcement"""
    try:
        company_name = request.form.get('company_name')
        
        if not company_name:
            return jsonify({
                'success': False,
                'error': 'Company name is required'
            }), 400
        
        # Get user's current plan
        user = models.get_user_by_id(current_user.id)
        plan_type = user['plan_type']
        
        # Get current client count
        current_clients = models.get_user_clients(current_user.id)
        client_count = len(current_clients)
        
        # Check plan limit
        plan_limit = PLAN_LIMITS.get(plan_type, PLAN_LIMITS['free'])['clients']
        
        if client_count >= plan_limit:
            return f'''
            <!DOCTYPE html>
            <html>
            <head>
                <title>Plan Limit Reached</title>
                <style>
                    body {{
                        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        min-height: 100vh;
                        display: flex;
                        align-items: center;
                        justify-content: center;
                        padding: 20px;
                    }}
                    .container {{
                        background: white;
                        border-radius: 16px;
                        padding: 48px;
                        max-width: 500px;
                        text-align: center;
                        box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
                    }}
                    h1 {{
                        font-size: 32px;
                        color: #1f2937;
                        margin-bottom: 16px;
                    }}
                    p {{
                        color: #6b7280;
                        margin-bottom: 24px;
                        line-height: 1.6;
                    }}
                    .plan-info {{
                        background: #fef3c7;
                        border: 2px solid #f59e0b;
                        border-radius: 8px;
                        padding: 16px;
                        margin-bottom: 24px;
                    }}
                    .plan-info strong {{
                        color: #92400e;
                    }}
                    .btn {{
                        display: inline-block;
                        padding: 14px 28px;
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        color: white;
                        text-decoration: none;
                        border-radius: 8px;
                        font-weight: 600;
                        margin: 8px;
                    }}
                    .btn-secondary {{
                        background: white;
                        color: #667eea;
                        border: 2px solid #667eea;
                    }}
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>⚠️ Plan Limit Reached</h1>
                    <p>You've reached the maximum number of clients for your current plan.</p>
                    
                    <div class="plan-info">
                        <strong>Current Plan:</strong> {plan_type.title()}<br>
                        <strong>Clients:</strong> {client_count} / {plan_limit}<br>
                        <strong>Status:</strong> Limit Reached
                    </div>
                    
                    <p>
                        <strong>Upgrade to add more clients:</strong><br>
                        <small>Starter: 5 clients | Agency: 15 clients | Enterprise: Unlimited</small>
                    </p>
                    
                    <a href="/upgrade" class="btn">🚀 Upgrade Plan</a>
                    <a href="/dashboard" class="btn btn-secondary">← Back to Dashboard</a>
                </div>
            </body>
            </html>
            ''', 403
        
        # Create client if under limit
        client_id = models.create_client(current_user.id, company_name)
        
        return redirect(url_for('dashboard'))
        
    except Exception as e:
        app.logger.error(f'Error creating client: {e}')
        return redirect(url_for('dashboard'))

@app.route('/')
def index():
    """Homepage - redirect based on auth status"""
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    return redirect(url_for('landing_page'))

# =====================================================================
# WIDGET & ADMIN ROUTES
# =====================================================================

@app.route('/widget')
def widget():
    client_id = request.args.get('client_id', 'demo')
    
    # Get client from database
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
        # ✅ Extract bot_name, colors etc from branding_settings JSON
        client = dict(client)  # make it mutable
        branding_settings = json.loads(client.get('branding_settings') or '{}')
        
        bot_settings = branding_settings.get('bot_settings', {})
        branding = branding_settings.get('branding', {})
        
        # Merge into client dict so template can access them directly
        client['bot_name'] = bot_settings.get('bot_name') or client.get('company_name') or 'Team Support'
        client['welcome_message'] = bot_settings.get('welcome_message') or client.get('welcome_message') or 'Hi! How can I help you today?'
        client['widget_color'] = branding.get('primary_color') or client.get('widget_color') or '#667eea'
        client['remove_branding'] = branding.get('remove_branding', client.get('remove_branding', 0))
    
    return render_template('chat.html', client=client)


@app.route('/admin/leads')
@login_required
def admin_leads():
    """View collected leads for a client"""
    client_id = request.args.get('client_id')
    
    if not client_id:
        return "Client ID required", 400
    
    # Verify ownership
    if not models.verify_client_ownership(current_user.id, client_id):
        return "Unauthorized", 403
    
    # Get leads from database
    leads = models.get_leads(client_id)
    
    # Get client info
    client = models.get_client_by_id(client_id)
    
    return render_template('admin.html', leads=leads, client_id=client_id, client=client)

@app.route('/landing')
def landing_page():
    """Professional landing page - agency focused"""
    return render_template('landing-professional.html')
 

# =====================================================================
# HEALTH CHECK & ADMIN ENDPOINTS
# =====================================================================

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint for monitoring"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'version': '1.0.0'
    })

@app.route('/api/admin/backup', methods=['POST'])
def trigger_backup():
    """Manually trigger backup (add authentication in production!)"""
    # TODO: Add authentication here before production
    auth_token = request.headers.get('X-Admin-Token')
    
    if auth_token != os.getenv('ADMIN_TOKEN', 'change-me-in-production'):
        return jsonify({
            'success': False,
            'error': 'Unauthorized'
        }), 401
    
    success = backup_all_clients()
    
    return jsonify({
        'success': success,
        'message': 'Backup completed' if success else 'Backup failed',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/embed-generator')
def embed_generator():
    """Generate embed code for clients"""
    return render_template('embed-generator.html')

@app.route('/customize')
@login_required
def customize_page():
    """Theme customization admin page"""
    client_id = request.args.get('client_id')
    
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return "Unauthorized", 403
    
    # Check if user plan has customization access
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
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    min-height: 100vh;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    padding: 20px;
                }}
                .container {{
                    background: white;
                    border-radius: 16px;
                    padding: 48px;
                    max-width: 500px;
                    text-align: center;
                }}
                h1 {{ font-size: 32px; color: #1f2937; margin-bottom: 16px; }}
                p {{ color: #6b7280; margin-bottom: 24px; }}
                .btn {{
                    display: inline-block;
                    padding: 14px 28px;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    text-decoration: none;
                    border-radius: 8px;
                    font-weight: 600;
                    margin: 8px;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>🎨 Theme Customization Locked</h1>
                <p>Theme customization is not available on the Free plan.</p>
                <p><strong>Upgrade to Starter or higher to unlock:</strong></p>
                <ul style="text-align: left; color: #374151; margin: 24px 0;">
                    <li>Custom colors & branding</li>
                    <li>Logo upload</li>
                    <li>Bot personality customization</li>
                    <li>White-label capabilities</li>
                </ul>
                <a href="/upgrade" class="btn">🚀 Upgrade Now</a>
                <a href="/dashboard" class="btn" style="background: white; color: #667eea; border: 2px solid #667eea;">← Back</a>
            </div>
        </body>
        </html>
        ''', 403
    
    return render_template('customize.html', user=current_user)

@app.route('/api/admin/customize', methods=['POST'])
@login_required
def save_customization():
    """Save client customization"""
    try:
        data = request.json
        client_id = data.get('client_id')
        
        if not client_id:
            return jsonify({'success': False, 'error': 'Client ID required'}), 400
        
        # Verify ownership
        if not models.verify_client_ownership(current_user.id, client_id):
            return jsonify({'success': False, 'error': 'Unauthorized'}), 403
        
        # Get existing client
        client = models.get_client_by_id(client_id)
        if not client:
            return jsonify({'success': False, 'error': 'Client not found'}), 404
        
        # Update branding settings
        branding_settings = {
            'branding': data.get('branding', {}),
            'contact': data.get('contact', {}),
            'bot_settings': data.get('bot_settings', {})
        }
        
        # Save to database
        conn = models.get_db()
        cursor = conn.cursor()
        
        cursor.execute(
            'UPDATE clients SET branding_settings = ? WHERE client_id = ?',
            (json.dumps(branding_settings), client_id)
        )
        
        conn.commit()
        conn.close()
        
        app.logger.info(f'Customization saved for client: {client_id}')
        
        return jsonify({
            'success': True,
            'message': 'Customization saved successfully'
        })
        
    except Exception as e:
        app.logger.error(f'Error saving customization: {e}')
        return jsonify({
            'success': False,
            'error': 'Failed to save customization'
        }), 500
    
@app.route('/api/admin/customize', methods=['POST'])
@login_required
def admin_customize():
    data = request.json
    client_id = data.get('client_id')
    
    # Get remove_branding value (only for Agency/Enterprise)
    remove_branding = 0
    if current_user.plan_type in ['agency', 'enterprise']:
        remove_branding = 1 if data.get('remove_branding') else 0
    
    # Update database
    conn = models.get_db_connection()
    conn.execute('''
        UPDATE clients 
        SET 
            company_name = ?,
            widget_color = ?,
            welcome_message = ?,
            remove_branding = ?
        WHERE client_id = ? AND user_id = ?
    ''', (
        data['branding']['company_name'],
        data['branding']['primary_color'],
        data['bot_settings']['welcome_message'],
        remove_branding,  # NEW
        client_id,
        current_user.id
    ))
    conn.commit()
    conn.close()
    
    return jsonify({'success': True})

@app.route('/analytics')
@login_required
def analytics_page():
    """Analytics dashboard page"""
    client_id = request.args.get('client_id')
    
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return "Unauthorized", 403
    
    # Check if user plan has analytics access
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
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    min-height: 100vh;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    padding: 20px;
                }}
                .container {{
                    background: white;
                    border-radius: 16px;
                    padding: 48px;
                    max-width: 500px;
                    text-align: center;
                }}
                h1 {{ font-size: 32px; color: #1f2937; margin-bottom: 16px; }}
                p {{ color: #6b7280; margin-bottom: 24px; }}
                .btn {{
                    display: inline-block;
                    padding: 14px 28px;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    text-decoration: none;
                    border-radius: 8px;
                    font-weight: 600;
                    margin: 8px;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>📊 Analytics Unavailable</h1>
                <p>Analytics is not available on the Free plan.</p>
                <p><strong>Upgrade to Starter or higher to unlock:</strong></p>
                <ul style="text-align: left; color: #374151; margin: 24px 0;">
                    <li>Conversation analytics</li>
                    <li>Lead tracking</li>
                    <li>Performance insights</li>
                    <li>Usage reports</li>
                </ul>
                <a href="/upgrade" class="btn">🚀 Upgrade Now</a>
                <a href="/dashboard" class="btn" style="background: white; color: #667eea; border: 2px solid #667eea;">← Back</a>
            </div>
        </body>
        </html>
        ''', 403
    
    return render_template('analytics.html')

@app.route('/api/admin/analytics', methods=['GET'])
def get_analytics():
    """Get analytics data for a client"""
    try:
        client_id = request.args.get('client_id', 'demo')
        date_range = request.args.get('range', 'week')
        
        # Calculate date range
        now = datetime.now()
        if date_range == 'today':
            start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        elif date_range == 'week':
            start_date = now - timedelta(days=7)
        elif date_range == 'month':
            start_date = now - timedelta(days=30)
        else:  # all
            start_date = datetime(2020, 1, 1)
        
        # ✅ FIXED: Direct database connection
        conn = sqlite3.connect('chatbot.db')
        cursor = conn.cursor()
        
        # Count total conversations (if table exists)
        try:
            cursor.execute('''
                SELECT COUNT(*) FROM conversations 
                WHERE client_id = ? AND timestamp >= ?
            ''', (client_id, start_date.isoformat()))
            total_conversations = cursor.fetchone()[0]
        except sqlite3.OperationalError:
            # Table doesn't exist yet
            total_conversations = 0
        
        # Count answered vs unanswered
        try:
            cursor.execute('''
                SELECT COUNT(*) FROM conversations 
                WHERE client_id = ? AND timestamp >= ? AND matched = 1
            ''', (client_id, start_date.isoformat()))
            answered = cursor.fetchone()[0]
        except sqlite3.OperationalError:
            answered = 0
            
        unanswered = total_conversations - answered
        answer_rate = int((answered / total_conversations * 100)) if total_conversations > 0 else 0
        
        # Count total leads
        cursor.execute('''
            SELECT COUNT(*) FROM leads 
            WHERE client_id = ? AND created_at >= ?
        ''', (client_id, start_date.isoformat()))
        total_leads = cursor.fetchone()[0]
        
        # Timeline data (last 7 days)
        timeline = []
        for i in range(7):
            date = (now - timedelta(days=6-i))
            date_str = date.strftime('%Y-%m-%d')
            
            # Count conversations for this day
            try:
                cursor.execute('''
                    SELECT COUNT(*) FROM conversations
                    WHERE client_id = ? AND DATE(timestamp) = DATE(?)
                ''', (client_id, date_str))
                conv_count = cursor.fetchone()[0]
            except sqlite3.OperationalError:
                conv_count = 0
            
            # Count leads for this day
            cursor.execute('''
                SELECT COUNT(*) FROM leads
                WHERE client_id = ? AND DATE(created_at) = DATE(?)
            ''', (client_id, date_str))
            lead_count = cursor.fetchone()[0]
            
            timeline.append({
                'date': date_str,
                'count': conv_count,
                'leads': lead_count
            })
        
        # Top questions
        try:
            cursor.execute('''
                SELECT user_message, COUNT(*) as count
                FROM conversations
                WHERE client_id = ? AND timestamp >= ? AND matched = 1
                GROUP BY user_message
                ORDER BY count DESC
                LIMIT 10
            ''', (client_id, start_date.isoformat()))
            
            top_questions = [
                {'question': row[0], 'count': row[1]}
                for row in cursor.fetchall()
            ]
        except sqlite3.OperationalError:
            top_questions = []
        
        # Unanswered questions
        try:
            cursor.execute('''
                SELECT user_message, COUNT(*) as count
                FROM conversations
                WHERE client_id = ? AND timestamp >= ? AND matched = 0
                GROUP BY user_message
                ORDER BY count DESC
                LIMIT 10
            ''', (client_id, start_date.isoformat()))
            
            unanswered_questions = [
                {'question': row[0], 'count': row[1]}
                for row in cursor.fetchall()
            ]
        except sqlite3.OperationalError:
            unanswered_questions = []
        
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
                'unanswered': unanswered_questions
            }
        })
        
    except Exception as e:
        app.logger.error(f'Error getting analytics: {e}')
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/sales')
def sales_page():
    return render_template('sales-page.html')

@app.route('/thank-you')
def thank_you_page():
    """Thank you page after payment"""
    return render_template('thank-you.html')

@app.route('/faq-manager')
@login_required
def faq_manager_page():
    """FAQ management dashboard"""
    client_id = request.args.get('client_id')
    
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return "Unauthorized", 403
    
    return render_template('faq-manager.html')

@app.route('/api/faqs', methods=['GET', 'POST'])
@login_required
def manage_faqs():
    """Get or update FAQs for a client"""
    try:
        # Get client_id from either query params or JSON body
        if request.method == 'GET':
            client_id = request.args.get('client_id')
        else:
            # For POST, try JSON first, fallback to form data
            if request.is_json:
                client_id = request.json.get('client_id')
            else:
                client_id = request.form.get('client_id')
        
        # Validate client_id
        if not client_id:
            return jsonify({
                'success': False,
                'error': 'Client ID is required'
            }), 400
        
        # Verify ownership
        if not models.verify_client_ownership(current_user.id, client_id):
            return jsonify({
                'success': False,
                'error': 'Unauthorized'
            }), 403
        
        if request.method == 'GET':
            # Get FAQs from database
            try:
                faqs = models.get_faqs(client_id)
                return jsonify({
                    'success': True,
                    'faqs': faqs
                })
            except Exception as e:
                app.logger.error(f'Error loading FAQs: {e}')
                # Return empty list if error
                return jsonify({
                    'success': True,
                    'faqs': []
                })
        
        elif request.method == 'POST':
            # Get FAQs list from request
            if request.is_json:
                faqs_list = request.json.get('faqs', [])
            else:
                return jsonify({
                    'success': False,
                    'error': 'Request must be JSON'
                }), 400
            
            # Get user plan
            user = models.get_user_by_id(current_user.id)
            plan_limits = PLAN_LIMITS.get(user['plan_type'], PLAN_LIMITS['free'])
            max_faqs = plan_limits['faqs_per_client']
            
            # Enforce FAQ limit
            if len(faqs_list) > max_faqs:
                return jsonify({
                    'success': False,
                    'error': f'Plan limit: Maximum {max_faqs} FAQs allowed on {user["plan_type"]} plan',
                    'upgrade_required': True
                }), 403
            
            # Save FAQs to database
            models.save_faqs(client_id, faqs_list)
            
            return jsonify({
                'success': True,
                'message': 'FAQs updated successfully'
            })
            
    except Exception as e:
        app.logger.error(f'Error managing FAQs: {e}')
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': 'Failed to manage FAQs'
        }), 500


@app.route('/api/faq/upload', methods=['POST'])
@login_required
def upload_faqs():
    """Upload FAQs from CSV or PDF"""
    try:
        client_id = request.form.get('client_id')
        
        # Verify ownership
        if not models.verify_client_ownership(current_user.id, client_id):
            return jsonify({'success': False, 'error': 'Unauthorized'}), 403
        
        # Check if file was uploaded
        if 'file' not in request.files:
            return jsonify({'success': False, 'error': 'No file uploaded'}), 400
        
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({'success': False, 'error': 'No file selected'}), 400
        
        # Get file extension
        filename = file.filename.lower()
        
        if filename.endswith('.csv'):
            faqs = process_csv_upload(file)
        elif filename.endswith('.pdf'):
            faqs = process_pdf_upload(file)
        elif filename.endswith(('.xlsx', '.xls')):
            faqs = process_excel_upload(file)
        else:
            return jsonify({
                'success': False,
                'error': 'Unsupported file type. Please upload CSV, Excel, or PDF.'
            }), 400
        
        if not faqs:
            return jsonify({
                'success': False,
                'error': 'No FAQs found in file. Please check the format.'
            }), 400
        
        # Save FAQs to database
        conn = sqlite3.connect('chatbot.db')
        cursor = conn.cursor()
        
        saved_count = 0
        for faq in faqs:
            try:
                cursor.execute('''
                    INSERT INTO faqs (faq_id, client_id, question, answer, category, triggers)
                     VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    str(__import__('uuid').uuid4()),
                    client_id,
                    faq['question'],
                    faq['answer'],
                    faq.get('category', 'General'),
                    json.dumps(faq.get('triggers', []))
                ))
                saved_count += 1
            except Exception as e:
                app.logger.error(f'Error saving FAQ: {e}')
                continue
        
        conn.commit()
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
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


def process_csv_upload(file):
    """Process CSV file and extract FAQs"""
    import pandas as pd
    import io
    
    try:
        # Read CSV
        df = pd.read_csv(io.StringIO(file.stream.read().decode('utf-8')))
        
        # Expected columns: question, answer, category (optional), triggers (optional)
        if 'question' not in df.columns or 'answer' not in df.columns:
            return []
        
        faqs = []
        for _, row in df.iterrows():
            # Extract triggers from text
            triggers = extract_keywords(row['question'])
            
            faq = {
                'question': str(row['question']).strip(),
                'answer': str(row['answer']).strip(),
                'category': str(row.get('category', 'General')).strip(),
                'triggers': triggers
            }
            
            # Skip empty rows
            if faq['question'] and faq['answer']:
                faqs.append(faq)
        
        return faqs
        
    except Exception as e:
        app.logger.error(f'Error processing CSV: {e}')
        return []


def process_excel_upload(file):
    """Process Excel file and extract FAQs"""
    import pandas as pd
    
    try:
        # Read Excel
        df = pd.read_excel(file)
        
        # Expected columns: question, answer, category (optional), triggers (optional)
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
    """Process PDF file and extract FAQs using AI"""
    import PyPDF2
    import io
    
    try:
        # Read PDF
        pdf_reader = PyPDF2.PdfReader(io.BytesIO(file.read()))
        
        # Extract all text
        text = ""
        for page in pdf_reader.pages:
            text += page.extract_text() + "\n"
        
        # Use AI to extract Q&A pairs
        if ai_helper and ai_helper.enabled:
            faqs = extract_faqs_from_text(text)
            return faqs
        else:
            # Fallback: Try to parse structured text
            return parse_structured_faq_text(text)
        
    except Exception as e:
        app.logger.error(f'Error processing PDF: {e}')
        return []


def extract_faqs_from_text(text):
    """Use AI to extract FAQs from text"""
    try:
        prompt = f"""Extract FAQ pairs from this text. Return a JSON array of objects with 'question' and 'answer' fields.

Text:
{text[:3000]}  # Limit to first 3000 chars

Return ONLY valid JSON array like:
[
  {{"question": "What are your hours?", "answer": "We're open 9-5 Monday-Friday"}},
  {{"question": "How much does it cost?", "answer": "$50 per month"}}
]
"""
        
        response = ai_helper.model.generate_content(prompt)
        
        # Parse JSON response
        import re
        json_match = re.search(r'\[.*\]', response.text, re.DOTALL)
        if json_match:
            faqs_data = json.loads(json_match.group())
            
            # Add triggers to each FAQ
            for faq in faqs_data:
                faq['triggers'] = extract_keywords(faq['question'])
                faq['category'] = 'Imported'
            
            return faqs_data
        
        return []
        
    except Exception as e:
        app.logger.error(f'Error extracting FAQs with AI: {e}')
        return []


def parse_structured_faq_text(text):
    """Parse text that has Q: and A: format"""
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
    
    # Add last Q&A
    if current_q and current_a:
        faqs.append({
            'question': current_q,
            'answer': current_a,
            'category': 'Imported',
            'triggers': extract_keywords(current_q)
        })
    
    return faqs


def extract_keywords(text):
    """Extract keywords from text for triggers"""
    import re
    
    # Remove common words
    stop_words = {'the', 'a', 'an', 'is', 'are', 'was', 'were', 'what', 'how', 'when', 'where', 'who', 'why', 'do', 'does', 'did', 'can', 'could', 'would', 'should'}
    
    # Extract words
    words = re.findall(r'\b\w+\b', text.lower())
    keywords = [w for w in words if len(w) > 3 and w not in stop_words]
    
    # Return unique keywords (max 10)
    return list(set(keywords))[:10]

@app.route('/upgrade')
@login_required
def upgrade_page():
    """Plan upgrade page"""
    return render_template('upgrade.html', user=current_user)

# Line 1322 (your existing closing brace)

# ==========================================
# PAYMENT ROUTES - PAYPAL
# ==========================================

@app.route('/payment/paypal/create', methods=['POST'])
@login_required
def create_paypal_payment():
    """Create PayPal payment for plan upgrade"""
    try:
        data = request.json
        plan = data.get('plan')  # starter, agency, enterprise
        
        # Plan pricing
        PLAN_PRICES = {
            'starter': 49.00,
            'agency': 149.00,
            'enterprise': 499.00
        }
        
        amount = PLAN_PRICES.get(plan)
        if not amount:
            return jsonify({'success': False, 'error': 'Invalid plan'}), 400
        
        # Create PayPal payment
        payment = Payment({
            "intent": "sale",
            "payer": {
                "payment_method": "paypal"
            },
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
                "amount": {
                    "total": f"{amount:.2f}",
                    "currency": "USD"
                },
                "description": f"Upgrade to {plan.capitalize()} Plan"
            }]
        })
        
        if payment.create():
            # Save pending payment in session
            session['pending_payment'] = {
                'user_id': current_user.id,
                'plan': plan,
                'amount': amount,
                'payment_id': payment.id
            }
            
            # Get approval URL
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
            return jsonify({
                'success': False,
                'error': 'Payment creation failed'
            }), 500
            
    except Exception as e:
        app.logger.error(f"PayPal error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/payment/paypal/success')
@login_required
def paypal_success():
    """Handle successful PayPal payment"""
    try:
        payment_id = request.args.get('paymentId')
        payer_id = request.args.get('PayerID')
        
        # Get pending payment from session
        pending_payment = session.get('pending_payment', {})
        
        if not pending_payment or pending_payment.get('payment_id') != payment_id:
            flash("⚠️ Payment session expired. Please try again.", 'warning')
            return redirect(url_for('upgrade_page'))
        
        # Execute the payment
        payment = Payment.find(payment_id)
        
        if payment.execute({"payer_id": payer_id}):
            # Payment successful - upgrade user
            plan = pending_payment['plan']
            
            # Update user's plan in database
            conn = models.get_db_connection()
            conn.execute(
                'UPDATE users SET plan = ?, upgraded_at = datetime("now") WHERE id = ?',
                (plan, current_user.id)
            )
            conn.commit()
            conn.close()
            
            # Clear session
            session.pop('pending_payment', None)
            
            flash(f"✅ Payment successful! You've been upgraded to {plan.capitalize()} plan.", 'success')
            return redirect(url_for('dashboard'))
        else:
            app.logger.error(f"PayPal execution failed: {payment.error}")
            flash("❌ Payment execution failed. Please try again.", 'error')
            return redirect(url_for('upgrade_page'))
            
    except Exception as e:
        app.logger.error(f"PayPal success handler error: {e}")
        import traceback
        traceback.print_exc()
        flash("❌ Payment processing error. Contact support.", 'error')
        return redirect(url_for('dashboard'))


@app.route('/api/webhook/lead', methods=['POST'])
def webhook_new_lead():
    """
    Fires when a new lead is captured.
    Make/Zapier polls this OR you push to their webhook URL.
    """
    try:
        # Verify webhook secret (security!)
        secret = request.headers.get('X-Webhook-Secret')
        if secret != os.environ.get('WEBHOOK_SECRET', 'lumvi-secret'):
            return jsonify({'error': 'Unauthorized'}), 401

        data = request.json
        client_id = data.get('client_id')

        # Get latest leads for this client
        conn = sqlite3.connect('chatbot.db')
        cursor = conn.cursor()
        cursor.execute('''
            SELECT name, email, phone, company, created_at
            FROM leads
            WHERE client_id = ?
            ORDER BY created_at DESC
            LIMIT 10
        ''', (client_id,))

        leads = [
            {
                'name': row[0],
                'email': row[1],
                'phone': row[2],
                'company': row[3],
                'created_at': row[4]
            }
            for row in cursor.fetchall()
        ]
        conn.close()

        return jsonify({'success': True, 'leads': leads})

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/webhook/faq-import', methods=['POST'])
def webhook_faq_import():
    """
    Zapier/Make sends FAQs from Google Sheets/Notion.
    Format: { "client_id": "xxx", "faqs": [{"question": "", "answer": ""}] }
    """
    try:
        # Verify secret
        secret = request.headers.get('X-Webhook-Secret')
        if secret != os.environ.get('WEBHOOK_SECRET', 'lumvi-secret'):
            return jsonify({'error': 'Unauthorized'}), 401

        data = request.json
        client_id = data.get('client_id')
        incoming_faqs = data.get('faqs', [])

        if not client_id or not incoming_faqs:
            return jsonify({'error': 'client_id and faqs required'}), 400

        # Save each FAQ
        conn = sqlite3.connect('chatbot.db')
        cursor = conn.cursor()
        saved = 0

        for faq in incoming_faqs:
            question = faq.get('question', '').strip()
            answer = faq.get('answer', '').strip()

            if not question or not answer:
                continue

            # Auto-generate triggers from question keywords
            triggers = extract_keywords(question)

            cursor.execute('''
                INSERT INTO faqs (client_id, question, answer, triggers)
                VALUES (?, ?, ?, ?)
            ''', (client_id, question, answer, json.dumps(triggers)))
            saved += 1

        conn.commit()
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
    """Handle cancelled PayPal payment"""
    # Clear session
    session.pop('pending_payment', None)
    
    flash("💳 Payment cancelled. You can try again anytime.", 'info')
    return redirect(url_for('upgrade_page'))


@app.route('/payment/paypal/webhook', methods=['POST'])
def paypal_webhook():
    """Handle PayPal webhook notifications"""
    try:
        event = request.json
        event_type = event.get('event_type')
        
        app.logger.info(f"PayPal webhook: {event_type}")
        
        # Handle different event types
        if event_type == 'PAYMENT.SALE.COMPLETED':
            # Payment completed
            app.logger.info("Payment completed via webhook")
        
        elif event_type == 'BILLING.SUBSCRIPTION.CANCELLED':
            # Subscription cancelled
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
    """Apply to become an affiliate"""
    # Check if already an affiliate
    existing = models.get_affiliate_by_user_id(current_user.id)
    if existing:
        return redirect(url_for('affiliate_dashboard'))
    
    if request.method == 'POST':
        payment_email = request.form.get('payment_email')
        
        # Create affiliate account
        affiliate = models.create_affiliate(current_user.id, payment_email)
        
        if affiliate:
            return redirect(url_for('affiliate_dashboard'))
        else:
            return "Error creating affiliate account", 500
    
    return render_template('become-affiliate.html')

@app.route('/affiliate-dashboard')
@login_required
def affiliate_dashboard():
    """Affiliate dashboard"""
    affiliate = models.get_affiliate_by_user_id(current_user.id)
    
    if not affiliate:
        return redirect(url_for('become_affiliate'))
    
    stats = models.get_affiliate_stats(affiliate['id'])
    commissions = models.get_affiliate_commissions(affiliate['id'])
    
    return render_template('affiliate-dashboard.html', stats=stats, commissions=commissions)

@app.route('/admin/init-db-production', methods=['GET', 'POST'])
def init_db_production():
    """One-time database initialization for production"""
    if request.method == 'POST':
        secret = request.form.get('secret')
        if secret == 'your-secret-password-here':
            models.init_db()
            # Add affiliate tables
            conn = models.get_db()
            cursor = conn.cursor()
            # ... run affiliate table creation SQL ...
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
    """Interactive demo page"""
    return render_template('demo.html')

# ==========================================
# LEGAL PAGES
# ==========================================

@app.route('/terms')
def terms():
    """Terms and Conditions page"""
    return render_template('terms.html')

@app.route('/privacy-policy')
def privacy_policy():
    """Privacy Policy page"""
    return render_template('privacy-policy.html')

@app.route('/refund-policy')
def refund_policy():
    """Refund Policy page"""
    return render_template('refund-policy.html')

# =====================================================================
# RUN SERVER
# =====================================================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)

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

@login_manager.user_loader
def load_user(user_id):
    user_data = models.get_user_by_id(int(user_id))
    if user_data:
        return User(user_data)
    return None

# Initialize database on startup
with app.app_context():
    models.init_db()    