from flask import Flask, request, jsonify, render_template, send_from_directory
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

# Load environment variables
load_dotenv()

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-key-change-me')

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
    client_id = request.args.get('client_id', 'default')
    config = load_client_config(client_id)
    
    if config:
        return jsonify({
            'success': True,
            'config': config
        })
    else:
        return jsonify({
            'success': False,
            'error': 'Client not found'
        }), 404
    
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
@limiter.limit("30 per minute")  # Rate limit: 30 messages per minute
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
        
        # Load client data
        config = load_client_config(client_id)
        faqs = load_client_faqs(client_id)
        
        if not config:
            return jsonify({
                'success': False,
                'error': 'Client configuration not found'
            }), 404
        
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
            # Found a match
            return jsonify({
                'success': True,
                'response': response,
                'trigger_lead_collection': False
            })
        else:
            # Complete fallback - no matches at all
            fallback = config['bot_settings'].get('fallback_message', 
                "I'm not sure about that. Would you like to speak with a human?")
            
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
@limiter.limit("10 per hour")  # Rate limit: 10 lead submissions per hour
def collect_lead():
    """Collect and store lead information"""
    try:
        data = request.json
        client_id = data.get('client_id', 'default')
        
        
        # Sanitize and validate required fields
        name = sanitize_input(data.get('name', ''), max_length=100)
        email = sanitize_input(data.get('email', ''), max_length=200)
        phone = sanitize_input(data.get('phone', ''), max_length=50)
        company = sanitize_input(data.get('company', ''), max_length=200)
        message = sanitize_input(data.get('message', ''), max_length=1000)
        
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
        
        # Prepare lead data
        lead_data = {
        'name': name,
        'email': email,
        'phone': phone,
        'company': company,
        'message': message,
        'conversation_snippet': sanitize_input(data.get('conversation_snippet', ''), max_length=1000),
        'source_url': sanitize_input(data.get('source_url', ''), max_length=500),
        'user_agent': request.headers.get('User-Agent', '')[:200]
        }

        # Backup before saving new lead
        backup_client_data(client_id)
        
        # Save lead
        if save_lead(client_id, lead_data):
            config = load_client_config(client_id)
            return jsonify({
                'success': True,
                'message': f"Thanks {name}! We've received your information and will reach out to you at {email} soon.",
                'contact_info': config['contact'] if config else {}
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Failed to save lead'
            }), 500
            
    except Exception as e:
        print(f"Error in lead endpoint: {e}")
        return jsonify({
            'success': False,
            'error': 'Internal server error'
        }), 500

# =====================================================================
# WIDGET & ADMIN ROUTES
# =====================================================================

@app.route('/widget')
def widget():
    """Serve embeddable widget HTML"""
    return render_template('chat.html')

@app.route('/admin/leads')
def admin_leads():
    """Simple admin page to view leads"""
    return send_from_directory('static', 'admin.html')

@app.route('/api/admin/leads', methods=['GET'])
def get_all_leads():
    """Get all leads for admin (add authentication in production!)"""
    client_id = request.args.get('client_id', 'default')
    leads_path = os.path.join(get_client_path(client_id), 'leads.json')
    
    try:
        with open(leads_path, 'r', encoding='utf-8') as f:
            leads_data = json.load(f)
        return jsonify({
            'success': True,
            'leads': leads_data['leads']
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/')
def index():
    """Homepage with improved value proposition"""
    return '''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>White-Label FAQ Chatbot - Deploy in Minutes</title>
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
            
            .container {
                max-width: 1100px;
                margin: 0 auto;
            }
            
            .hero {
                background: white;
                border-radius: 16px;
                box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
                padding: 60px 40px;
                text-align: center;
                margin-bottom: 24px;
            }
            
            h1 {
                font-size: 48px;
                color: #1f2937;
                margin-bottom: 16px;
                line-height: 1.2;
            }
            
            .tagline {
                font-size: 24px;
                color: #667eea;
                margin-bottom: 16px;
                font-weight: 600;
            }
            
            .description {
                font-size: 18px;
                color: #6b7280;
                max-width: 700px;
                margin: 0 auto 40px;
                line-height: 1.6;
            }
            
            .cta-buttons {
                display: flex;
                gap: 16px;
                justify-content: center;
                flex-wrap: wrap;
                margin-bottom: 40px;
            }
            
            .btn {
                padding: 16px 32px;
                border-radius: 8px;
                font-size: 16px;
                font-weight: 600;
                text-decoration: none;
                transition: transform 0.2s, box-shadow 0.2s;
                display: inline-block;
            }
            
            .btn-primary {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                box-shadow: 0 4px 12px rgba(102, 126, 234, 0.4);
            }
            
            .btn-secondary {
                background: white;
                color: #667eea;
                border: 2px solid #667eea;
            }
            
            .btn:hover {
                transform: translateY(-2px);
                box-shadow: 0 6px 16px rgba(102, 126, 234, 0.5);
            }
            
            .features {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                gap: 24px;
                margin-bottom: 24px;
            }
            
            .feature-card {
                background: white;
                padding: 32px;
                border-radius: 12px;
                box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
            }
            
            .feature-icon {
                font-size: 40px;
                margin-bottom: 16px;
            }
            
            .feature-card h3 {
                font-size: 20px;
                color: #1f2937;
                margin-bottom: 12px;
            }
            
            .feature-card p {
                color: #6b7280;
                line-height: 1.6;
            }
            
            .stats {
                background: white;
                border-radius: 12px;
                padding: 40px;
                display: flex;
                justify-content: space-around;
                flex-wrap: wrap;
                gap: 24px;
                margin-bottom: 24px;
            }
            
            .stat {
                text-align: center;
            }
            
            .stat-number {
                font-size: 48px;
                font-weight: 700;
                color: #667eea;
            }
            
            .stat-label {
                font-size: 14px;
                color: #6b7280;
                text-transform: uppercase;
                letter-spacing: 1px;
            }
            
            .api-section {
                background: white;
                border-radius: 12px;
                padding: 40px;
                margin-bottom: 24px;
            }
            
            .api-section h2 {
                font-size: 28px;
                color: #1f2937;
                margin-bottom: 24px;
                text-align: center;
            }
            
            .endpoint {
                background: #f9fafb;
                padding: 16px;
                border-radius: 8px;
                margin-bottom: 12px;
                font-family: 'Courier New', monospace;
                font-size: 14px;
                border-left: 4px solid #667eea;
            }
            
            .method {
                color: #10b981;
                font-weight: bold;
                margin-right: 8px;
            }
            
            .method.get {
                color: #3b82f6;
            }
            
            @media (max-width: 768px) {
                h1 {
                    font-size: 32px;
                }
                
                .tagline {
                    font-size: 18px;
                }
                
                .hero {
                    padding: 40px 24px;
                }
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="hero">
                <h1>💬 White-Label FAQ Chatbot</h1>
                <p class="tagline">Deploy a customizable chatbot — white-label, embeddable, and easy to manage</p>
                <p class="description">
                    Built for agencies and businesses who need a professional FAQ chatbot with lead collection. 
                    Each client gets their own branding, FAQs, and analytics. One deployment serves unlimited clients.
                </p>
                
                <div class="cta-buttons">
                    <a href="/widget?client_id=demo" class="btn btn-primary">🎨 Try Live Demo</a>
                    <a href="/embed-generator" class="btn btn-secondary">🔌 Get Embed Code</a>
                </div>
            </div>
            
            <div class="stats">
                <div class="stat">
                    <div class="stat-number">10 min</div>
                    <div class="stat-label">Setup Time</div>
                </div>
                <div class="stat">
                    <div class="stat-number">∞</div>
                    <div class="stat-label">Clients Supported</div>
                </div>
                <div class="stat">
                    <div class="stat-number">1 line</div>
                    <div class="stat-label">To Embed</div>
                </div>
                <div class="stat">
                    <div class="stat-number">100%</div>
                    <div class="stat-label">White-Label</div>
                </div>
            </div>
            
            <div class="features">
                <div class="feature-card">
                    <div class="feature-icon">🎨</div>
                    <h3>Fully Customizable</h3>
                    <p>Each client gets their own colors, logo, bot name, and personality. Zero branding from us.</p>
                </div>
                
                <div class="feature-card">
                    <div class="feature-icon">📊</div>
                    <h3>Lead Collection</h3>
                    <p>Conversational lead capture that feels natural. Collect name, email, phone, and custom fields.</p>
                </div>
                
                <div class="feature-card">
                    <div class="feature-icon">⚡</div>
                    <h3>Easy Embedding</h3>
                    <p>One line of code on any website. Works with WordPress, Shopify, Webflow, or plain HTML.</p>
                </div>
                
                <div class="feature-card">
                    <div class="feature-icon">🔒</div>
                    <h3>Secure & Fast</h3>
                    <p>Rate limiting, input validation, CORS protection, and automatic backups included.</p>
                </div>
            </div>
            
            <div class="api-section">
                <h2>🔌 API Endpoints</h2>
                
                <div class="endpoint">
                    <span class="method">POST</span>
                    <span>/api/chat</span>
                    <span style="color: #6b7280; margin-left: 8px;">- Chat with bot</span>
                </div>
                
                <div class="endpoint">
                    <span class="method">POST</span>
                    <span>/api/lead</span>
                    <span style="color: #6b7280; margin-left: 8px;">- Submit lead</span>
                </div>
                
                <div class="endpoint">
                    <span class="method get">GET</span>
                    <span>/api/config?client_id=demo</span>
                    <span style="color: #6b7280; margin-left: 8px;">- Get client config</span>
                </div>
                
                <div class="endpoint">
                    <span class="method get">GET</span>
                    <span>/widget</span>
                    <span style="color: #6b7280; margin-left: 8px;">- Embeddable widget</span>
                </div>
                
                <div class="endpoint">
                    <span class="method get">GET</span>
                    <span>/admin/leads</span>
                    <span style="color: #6b7280; margin-left: 8px;">- View collected leads</span>
                </div>
                
                <div style="text-align: center; margin-top: 32px;">
                    <a href="/admin/leads?client_id=demo" class="btn btn-secondary">📊 View Demo Leads</a>
                </div>
            </div>
        </div>
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

# =====================================================================
# RUN SERVER
# =====================================================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)