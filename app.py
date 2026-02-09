from flask import Flask, request, jsonify, render_template, send_from_directory, redirect, url_for
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
from io import StringIO
from config import Config
from ai_helper import get_ai_helper

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Initialize AI helper at app startup
#After creating app
ai_helper = get_ai_helper(Config.GEMINI_API_KEY, Config.GEMINI_MODEL)

if ai_helper and ai_helper.enabled:
    app.logger.info("✅ Gemini AI initialized")

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

# SECRET KEY for sessions (IMPORTANT!)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'your-secret-key-change-in-production')

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
                        "triggers": ["hours", "time", "open", "available", "when", "schedule"]
                    },
                    {
                        "id": "demo_2",
                        "question": "What are your prices?",
                        "answer": "Starter: $49/mo | Agency: $149/mo | Enterprise: Custom pricing! 💰",
                        "triggers": ["price", "pricing", "cost", "how much", "payment", "charge"]
                    },
                    {
                        "id": "demo_3",
                        "question": "Do you offer discounts?",
                        "answer": "Yes! Annual plans get 20% off. Students/nonprofits get 30% off. 🎉",
                        "triggers": ["discount", "sale", "promo", "coupon", "deal", "cheaper"]
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
        
        # Step 1: INSTANT lead detection (no AI needed)
        for trigger in lead_triggers:
            if trigger.lower() in message_lower:
                return jsonify({
                    'success': True,
                    'response': "I'd be happy to connect you with our team! What's the best email to reach you?",
                    'trigger_lead_collection': True,
                    'method': 'instant',
                    'contact_info': config.get('contact', {})
                })
        
        # Step 2: INSTANT keyword matching (no AI - super fast!)
        for faq in faqs_list:
            triggers = faq.get('triggers', [])
            # Check if ANY trigger word is in the message
            for trigger in triggers:
                if trigger.lower() in message_lower:
                    app.logger.info(f"Instant match: {faq.get('id')} via keyword '{trigger}'")
                    return jsonify({
                        'success': True,
                        'response': faq.get('answer'),
                        'confidence': 0.95,
                        'method': 'keyword'  # Debug: shows it was instant
                    })
        
        # Step 3: AI-powered matching (only if keyword matching failed)
        if ai_helper and ai_helper.enabled:
            app.logger.info("No keyword match, using AI...")
            
            try:
                # Single AI call for FAQ matching
                best_faq, confidence = ai_helper.find_best_faq(message, faqs_list)
                
                if best_faq and confidence > 0.5:
                    # Return FAQ answer DIRECTLY (don't generate - faster!)
                    return jsonify({
                        'success': True,
                        'response': best_faq.get('answer'),
                        'confidence': confidence,
                        'method': 'ai'
                    })
            except Exception as ai_error:
                app.logger.error(f"AI error: {ai_error}")
                # Fall through to fallback
        
        # Step 4: Fallback response
        fallback = config.get('bot_settings', {}).get(
            'fallback_message',
            "I'm not sure about that. Would you like to speak with our team? Type 'contact'!"
        )
        
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
    """Serve embeddable widget HTML"""
    return render_template('chat.html')


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
    
    return render_template('customize.html')

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
        
        # Load conversation logs
        analytics_dir = os.path.join('clients', client_id, 'analytics')
        all_conversations = []
        
        if os.path.exists(analytics_dir):
            for filename in os.listdir(analytics_dir):
                if filename.startswith('conversations_'):
                    file_path = os.path.join(analytics_dir, filename)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            all_conversations.extend(data.get('conversations', []))
                    except:
                        continue
        
        # Filter by date range
        filtered_conversations = [
            c for c in all_conversations
            if datetime.fromisoformat(c['timestamp']) >= start_date
        ]
        
        # Load leads
        leads_path = os.path.join(get_client_path(client_id), 'leads.json')
        total_leads = 0
        if os.path.exists(leads_path):
            with open(leads_path, 'r', encoding='utf-8') as f:
                leads_data = json.load(f)
                leads = [
                    l for l in leads_data.get('leads', [])
                    if datetime.fromisoformat(l['timestamp']) >= start_date
                ]
                total_leads = len(leads)
        
        # Calculate stats
        total_conversations = len(filtered_conversations)
        answered = sum(1 for c in filtered_conversations if c.get('matched'))
        unanswered = total_conversations - answered
        answer_rate = int((answered / total_conversations * 100)) if total_conversations > 0 else 0
        
        # Timeline data (last 7 days)
        timeline = []
        for i in range(7):
            date = (now - timedelta(days=6-i)).strftime('%Y-%m-%d')
            count = sum(1 for c in filtered_conversations if c['timestamp'].startswith(date))
            timeline.append({
                'date': date,
                'count': count
            })
        
        # Top questions (group by user message)
        question_counts = {}
        for conv in filtered_conversations:
            if conv.get('matched'):
                msg = conv['user_message']
                question_counts[msg] = question_counts.get(msg, 0) + 1
        
        top_questions = [
            {'question': q, 'count': c}
            for q, c in sorted(question_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        ]
        
        # Unanswered questions
        unanswered_counts = {}
        for conv in filtered_conversations:
            if not conv.get('matched'):
                msg = conv['user_message']
                unanswered_counts[msg] = unanswered_counts.get(msg, 0) + 1
        
        unanswered_questions = [
            {'question': q, 'count': c}
            for q, c in sorted(unanswered_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        ]
        
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
        return jsonify({
            'success': False,
            'error': 'Failed to load analytics'
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
    
@app.route('/upgrade')
@login_required
def upgrade_page():
    """Plan upgrade page"""
    return render_template('upgrade.html', user=current_user)

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