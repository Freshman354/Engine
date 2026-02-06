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


# Load environment variables
load_dotenv()

app = Flask(__name__)

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
    """Handle chat messages"""
    try:
        data = request.json
        
        # Sanitize inputs
        message = sanitize_input(data.get('message', ''))
        client_id = sanitize_input(data.get('client_id', 'default'), max_length=50)
        conversation_context = data.get('context', {})
        
        if not message:
            return jsonify({
                'success': False,
                'error': 'Message is required'
            }), 400
        
        # Get client and config from database
        client = models.get_client_by_id(client_id)
        if not client:
            return jsonify({
                'success': False,
                'error': 'Client not found'
            }), 404
        
        # Parse config
        config = json.loads(client['branding_settings']) if client['branding_settings'] else {}
        
        # Get FAQs from database
        faqs_list = models.get_faqs(client_id)
        faqs = {'faqs': faqs_list}
        
        # Get lead triggers from config
        lead_triggers = config.get('bot_settings', {}).get('lead_triggers', [])
        
        # Match FAQ or detect lead trigger
        response, extracted_email = match_faq(message, faqs, lead_triggers)
        
        # Handle lead collection trigger
        if response == "TRIGGER_LEAD_COLLECTION":
            return jsonify({
                'success': True,
                'response': f"I'd be happy to connect you with our team! To help us serve you better, may I have your name?",
                'trigger_lead_collection': True,
                'extracted_email': extracted_email,
                'contact_info': config.get('contact', {})
            })
        
        # Handle no match with suggestions
        if response == "NO_MATCH_WITH_SUGGESTIONS":
            similar_questions = extracted_email  # Reusing this variable for suggestions list
            
            if similar_questions and len(similar_questions) > 0:
                suggestion_text = "I'm not sure about that exact question, but here are some related topics:\n\n"
                suggestion_text += "\n".join([f"• {q}" for q in similar_questions])
                suggestion_text += "\n\nOr type 'contact' to speak with our team!"
                
                return jsonify({
                    'success': True,
                    'response': suggestion_text,
                    'trigger_lead_collection': False,
                    'suggestions': similar_questions
                })
            else:
                response = None
        
        # Return matched FAQ response
        if response:
            # Log the conversation
            log_conversation(client_id, message, response, matched_faq_id='matched')
            
            return jsonify({
                'success': True,
                'response': response,
                'trigger_lead_collection': False
            })
        
        # Complete fallback
        fallback = config.get('bot_settings', {}).get('fallback_message', 
            "I'm not sure about that. Would you like to speak with a human?")
        
        # Log unanswered question
        log_conversation(client_id, message, fallback, matched_faq_id=None)
        
        return jsonify({
            'success': True,
            'response': fallback,
            'trigger_lead_collection': False,
            'show_contact_button': True
        })
        
    except Exception as e:
        app.logger.error(f'Error in chat endpoint: {e}')
        return jsonify({
            'success': False,
            'error': 'Internal server error'
        }), 500
        
        # Get lead triggers from config
        lead_triggers = config['bot_settings'].get('lead_triggers', [])
        
        # Match FAQ or detect lead trigger
        response, extracted_email = match_faq(message, faqs, lead_triggers)
        
        # Handle lead collection trigger
        if response == "TRIGGER_LEAD_COLLECTION":
            return jsonify({
                'success': True,
                'response': f"I'd be happy to connect you with our team! To help us serve you better, may I have your name?",
                'trigger_lead_collection': True,
                'extracted_email': extracted_email,
                'contact_info': config['contact']
            })
        
# Handle different response types
        if response == "NO_MATCH_WITH_SUGGESTIONS":
            # No exact match but found similar questions
            similar_questions = extracted_email  # Reusing this parameter for suggestions
            suggestion_text = "I'm not sure about that exact question, but here are some related topics:\n\n"
            suggestion_text += "\n".join([f"• {q}" for q in similar_questions])
            suggestion_text += "\n\nOr type 'contact' to speak with our team!"
            
            return jsonify({
                'success': True,
                'response': suggestion_text,
                'trigger_lead_collection': False,
                'suggestions': similar_questions
            })
        elif response:
            # Log the conversation
            log_conversation(client_id, message, response, matched_faq_id='matched')
    
            return jsonify({
                'success': True,
                'response': response,
                'trigger_lead_collection': False
            })
        else:
            # Complete fallback - no matches at all
            fallback = config['bot_settings'].get('fallback_message', 
    "I'm not sure about that. Would you like to speak with a human?")

            # Log unanswered question
            log_conversation(client_id, message, fallback, matched_faq_id=None)

            return jsonify({
                'success': True,
                'response': fallback,
                'trigger_lead_collection': False,
                'show_contact_button': True
            })
        
    except Exception as e:
        print(f"Error in chat endpoint: {e}")
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
    """Public landing page with value proposition"""
    return '''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>White-Label FAQ Chatbot - Deploy & Manage</title>
        <style>
            * {
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }
            
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
                padding: 20px;
            }
            
            /* Navigation */
            .navbar {
                position: fixed;
                top: 0;
                left: 0;
                right: 0;
                background: rgba(255, 255, 255, 0.95);
                backdrop-filter: blur(10px);
                padding: 16px 40px;
                display: flex;
                justify-content: space-between;
                align-items: center;
                box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
                z-index: 1000;
            }
            
            .logo {
                font-size: 24px;
                font-weight: 700;
                color: #667eea;
            }
            
            .hamburger {
                display: none;
                flex-direction: column;
                cursor: pointer;
                gap: 5px;
            }
            
            .hamburger span {
                width: 25px;
                height: 3px;
                background: #667eea;
                border-radius: 3px;
                transition: 0.3s;
            }
            
            .nav-menu {
                display: flex;
                gap: 24px;
                align-items: center;
            }
            
            .nav-menu a {
                color: #374151;
                text-decoration: none;
                font-weight: 600;
                transition: color 0.2s;
            }
            
            .nav-menu a:hover {
                color: #667eea;
            }
            
            .btn-login {
                padding: 8px 20px;
                background: white;
                color: #667eea;
                border: 2px solid #667eea;
                border-radius: 6px;
                font-weight: 600;
                text-decoration: none;
                transition: all 0.2s;
            }
            
            .btn-login:hover {
                background: #667eea;
                color: white;
            }
            
            .btn-signup {
                padding: 8px 20px;
                background: #667eea;
                color: white;
                border: none;
                border-radius: 6px;
                font-weight: 600;
                text-decoration: none;
                transition: all 0.2s;
            }
            
            .btn-signup:hover {
                background: #5568d3;
            }
            
            /* Mobile Menu */
            @media (max-width: 768px) {
                .hamburger {
                    display: flex;
                }
                
                .nav-menu {
                    position: fixed;
                    top: 64px;
                    left: -100%;
                    flex-direction: column;
                    background: white;
                    width: 100%;
                    padding: 20px;
                    gap: 16px;
                    align-items: flex-start;
                    transition: left 0.3s;
                    box-shadow: 0 10px 30px rgba(0, 0, 0, 0.1);
                }
                
                .nav-menu.active {
                    left: 0;
                }
                
                .navbar {
                    padding: 16px 20px;
                }
            }
            
            /* Hero Section */
            .container {
                max-width: 1200px;
                margin: 100px auto 40px;
                background: white;
                border-radius: 24px;
                padding: 60px 40px;
                box-shadow: 0 20px 60px rgba(0, 0, 0, 0.2);
            }
            
            .hero {
                text-align: center;
                margin-bottom: 60px;
            }
            
            .hero-icon {
                font-size: 64px;
                margin-bottom: 20px;
            }
            
            .hero h1 {
                font-size: 48px;
                color: #1f2937;
                margin-bottom: 16px;
                line-height: 1.2;
            }
            
            .hero p {
                font-size: 20px;
                color: #6b7280;
                margin-bottom: 32px;
            }
            
            .cta-buttons {
                display: flex;
                gap: 16px;
                justify-content: center;
                flex-wrap: wrap;
            }
            
            .btn-primary {
                padding: 16px 32px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                border: none;
                border-radius: 8px;
                font-size: 18px;
                font-weight: 600;
                cursor: pointer;
                text-decoration: none;
                display: inline-block;
                transition: transform 0.2s;
            }
            
            .btn-primary:hover {
                transform: translateY(-2px);
            }
            
            .btn-secondary {
                padding: 16px 32px;
                background: white;
                color: #667eea;
                border: 2px solid #667eea;
                border-radius: 8px;
                font-size: 18px;
                font-weight: 600;
                cursor: pointer;
                text-decoration: none;
                display: inline-block;
                transition: all 0.2s;
            }
            
            .btn-secondary:hover {
                background: #f0f9ff;
            }
            
            /* Stats */
            .stats {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 20px;
                margin-bottom: 60px;
                padding: 40px 0;
                border-top: 2px solid #e5e7eb;
                border-bottom: 2px solid #e5e7eb;
            }
            
            .stat {
                text-align: center;
            }
            
            .stat-value {
                font-size: 48px;
                font-weight: 700;
                color: #667eea;
                margin-bottom: 8px;
            }
            
            .stat-label {
                font-size: 14px;
                color: #6b7280;
                text-transform: uppercase;
                letter-spacing: 1px;
            }
            
            /* Features */
            .features {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                gap: 32px;
            }
            
            .feature {
                text-align: center;
            }
            
            .feature-icon {
                font-size: 48px;
                margin-bottom: 16px;
            }
            
            .feature h3 {
                font-size: 20px;
                color: #1f2937;
                margin-bottom: 12px;
            }
            
            .feature p {
                color: #6b7280;
                line-height: 1.6;
            }
            
            @media (max-width: 768px) {
                .container {
                    padding: 40px 20px;
                    margin-top: 80px;
                }
                
                .hero h1 {
                    font-size: 32px;
                }
                
                .hero p {
                    font-size: 16px;
                }
                
                .stat-value {
                    font-size: 32px;
                }
            }
        </style>
    </head>
    <body>
        <!-- Navigation -->
        <nav class="navbar">
            <div class="logo">💬 FAQ Chatbot</div>
            
            <div class="hamburger" onclick="toggleMenu()">
                <span></span>
                <span></span>
                <span></span>
            </div>
            
            <div class="nav-menu" id="navMenu">
                <a href="/sales">Pricing</a>
                <a href="/embed-generator">Embed Generator</a>
                <a href="mailto:support@example.com">Contact Us</a>
                <a href="/login" class="btn-login">Login</a>
                <a href="/signup" class="btn-signup">Sign Up Free</a>
            </div>
        </nav>
        
        <!-- Main Content -->
        <div class="container">
            <div class="hero">
                <div class="hero-icon">💬</div>
                <h1>White-Label FAQ Chatbot</h1>
                <p>Deploy a customizable chatbot — white-label, embeddable, and easy to manage</p>
                
                <div class="cta-buttons">
                    <a href="/widget?client_id=demo" class="btn-primary">🎯 Try Live Demo</a>
                    <a href="/embed-generator" class="btn-secondary">📋 Get Embed Code</a>
                </div>
            </div>
            
            <div class="stats">
                <div class="stat">
                    <div class="stat-value">10 min</div>
                    <div class="stat-label">Setup Time</div>
                </div>
                <div class="stat">
                    <div class="stat-value">∞</div>
                    <div class="stat-label">Clients Supported</div>
                </div>
                <div class="stat">
                    <div class="stat-value">1 line</div>
                    <div class="stat-label">To Embed</div>
                </div>
                <div class="stat">
                    <div class="stat-value">100%</div>
                    <div class="stat-label">White-Label</div>
                </div>
            </div>
            
            <div class="features">
                <div class="feature">
                    <div class="feature-icon">🎨</div>
                    <h3>Fully Customizable</h3>
                    <p>Each client gets their own colors, logo, bot name, and personality. Zero branding from us.</p>
                </div>
                
                <div class="feature">
                    <div class="feature-icon">📊</div>
                    <h3>Lead Collection</h3>
                    <p>Conversational lead capture that feels natural. Collect name, email, phone, and custom fields.</p>
                </div>
                
                <div class="feature">
                    <div class="feature-icon">⚡</div>
                    <h3>Easy Embedding</h3>
                    <p>One line of code on any website. Works with WordPress, Shopify, Webflow, or plain HTML.</p>
                </div>
                
                <div class="feature">
                    <div class="feature-icon">🔒</div>
                    <h3>Secure & Fast</h3>
                    <p>Rate limiting, input validation, CORS protection, and automatic backups included.</p>
                </div>
            </div>
        </div>
        
        <script>
            function toggleMenu() {
                const menu = document.getElementById('navMenu');
                menu.classList.toggle('active');
            }
            
            // Close menu when clicking outside
            document.addEventListener('click', function(event) {
                const menu = document.getElementById('navMenu');
                const hamburger = document.querySelector('.hamburger');
                
                if (!menu.contains(event.target) && !hamburger.contains(event.target)) {
                    menu.classList.remove('active');
                }
            });
        </script>
    </body>
    </html>
    '''
 

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
        client_id = request.args.get('client_id') or request.json.get('client_id')
        
        # Verify ownership
        if not models.verify_client_ownership(current_user.id, client_id):
            return jsonify({'success': False, 'error': 'Unauthorized'}), 403
        
        if request.method == 'GET':
            faqs = models.get_faqs(client_id)
            return jsonify({'success': True, 'faqs': faqs})
        
        elif request.method == 'POST':
            faqs_list = request.json.get('faqs', [])
            
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
            
            models.save_faqs(client_id, faqs_list)
            
            return jsonify({'success': True, 'message': 'FAQs updated successfully'})
            
    except Exception as e:
        app.logger.error(f'Error managing FAQs: {e}')
        return jsonify({'success': False, 'error': 'Failed to manage FAQs'}), 500
    
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