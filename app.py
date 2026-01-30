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
    """Match user message to FAQ based on triggers"""
    message_lower = message.lower()
    
    # Check for lead collection triggers first
    for trigger in lead_triggers:
        if trigger.lower() in message_lower:
            return "TRIGGER_LEAD_COLLECTION", None
    
    # Check if message contains an email (user providing contact info)
    if is_email(message):
        return "TRIGGER_LEAD_COLLECTION", extract_email(message)
    
    # Match against FAQ triggers
    best_match = None
    max_matches = 0
    
    for faq in faqs['faqs']:
        matches = sum(1 for trigger in faq['triggers'] if trigger.lower() in message_lower)
        if matches > max_matches:
            max_matches = matches
            best_match = faq
    
    if best_match and max_matches > 0:
        return best_match['answer'], None
    
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
        
        # Return FAQ response or fallback
        if response:
            final_response = response
        else:
            final_response = config['bot_settings'].get('fallback_message', 
                "I'm not sure about that. Would you like to speak with a human?")
        
        return jsonify({
            'success': True,
            'response': final_response,
            'trigger_lead_collection': False
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
    """Homepage with styled interface"""
    return '''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>FAQ Chatbot API</title>
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
                display: flex;
                align-items: center;
                justify-content: center;
                padding: 20px;
            }
            
            .container {
                background: white;
                border-radius: 16px;
                box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
                max-width: 600px;
                width: 100%;
                padding: 40px;
            }
            
            h1 {
                font-size: 32px;
                color: #1f2937;
                margin-bottom: 8px;
                display: flex;
                align-items: center;
                gap: 12px;
            }
            
            .subtitle {
                color: #6b7280;
                margin-bottom: 32px;
                font-size: 16px;
            }
            
            .section {
                margin-bottom: 32px;
            }
            
            .section h2 {
                font-size: 18px;
                color: #374151;
                margin-bottom: 16px;
                display: flex;
                align-items: center;
                gap: 8px;
            }
            
            .endpoint-list {
                background: #f9fafb;
                border-radius: 8px;
                padding: 16px;
            }
            
            .endpoint {
                padding: 12px;
                margin-bottom: 8px;
                background: white;
                border-radius: 6px;
                border-left: 3px solid #667eea;
                font-family: 'Courier New', monospace;
                font-size: 13px;
            }
            
            .endpoint:last-child {
                margin-bottom: 0;
            }
            
            .method {
                color: #059669;
                font-weight: bold;
                margin-right: 8px;
            }
            
            .method.get {
                color: #3b82f6;
            }
            
            .path {
                color: #1f2937;
            }
            
            .links {
                display: grid;
                gap: 12px;
            }
            
            .link-button {
                display: block;
                padding: 16px 24px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                text-decoration: none;
                border-radius: 8px;
                text-align: center;
                font-weight: 600;
                transition: transform 0.2s, box-shadow 0.2s;
                box-shadow: 0 4px 12px rgba(102, 126, 234, 0.4);
            }
            
            .link-button:hover {
                transform: translateY(-2px);
                box-shadow: 0 6px 16px rgba(102, 126, 234, 0.5);
            }
            
            .link-button.secondary {
                background: linear-gradient(135deg, #f59e0b 0%, #ef4444 100%);
                box-shadow: 0 4px 12px rgba(245, 158, 11, 0.4);
            }
            
            .link-button.secondary:hover {
                box-shadow: 0 6px 16px rgba(245, 158, 11, 0.5);
            }
            
            .status {
                display: inline-flex;
                align-items: center;
                gap: 8px;
                padding: 8px 16px;
                background: #d1fae5;
                color: #065f46;
                border-radius: 20px;
                font-size: 14px;
                font-weight: 600;
                margin-bottom: 24px;
            }
            
            .status-dot {
                width: 8px;
                height: 8px;
                background: #10b981;
                border-radius: 50%;
                animation: pulse 2s infinite;
            }
            
            @keyframes pulse {
                0%, 100% { opacity: 1; }
                50% { opacity: 0.5; }
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>💬 FAQ Chatbot API</h1>
            <p class="subtitle">White-label chatbot with lead collection</p>
            
            <div class="status">
                <span class="status-dot"></span>
                Server Running
            </div>
            
            <div class="section">
                <h2>🔌 API Endpoints</h2>
                <div class="endpoint-list">
                    <div class="endpoint">
                        <span class="method">POST</span>
                        <span class="path">/api/chat</span>
                        <span style="color: #6b7280; margin-left: 8px;">- Chat with bot</span>
                    </div>
                    <div class="endpoint">
                        <span class="method">POST</span>
                        <span class="path">/api/lead</span>
                        <span style="color: #6b7280; margin-left: 8px;">- Submit lead</span>
                    </div>
                    <div class="endpoint">
                        <span class="method get">GET</span>
                        <span class="path">/api/config?client_id=demo</span>
                        <span style="color: #6b7280; margin-left: 8px;">- Get client config</span>
                    </div>
                    <div class="endpoint">
                        <span class="method get">GET</span>
                        <span class="path">/widget</span>
                        <span style="color: #6b7280; margin-left: 8px;">- Embeddable widget</span>
                    </div>
                    <div class="endpoint">
                        <span class="method get">GET</span>
                        <span class="path">/admin/leads</span>
                        <span style="color: #6b7280; margin-left: 8px;">- View collected leads</span>
                    </div>
                </div>
            </div>
            
            <div class="section">
                <h2>🚀 Quick Start</h2>
                <div class="links">
                    <a href="/widget?client_id=demo" class="link-button">
                        🎨 Test Demo Widget
                    </a>
                    <a href="/widget?client_id=default" class="link-button">
                        🤖 Test Default Widget
                    </a>
                    <a href="/admin/leads?client_id=demo" class="link-button secondary">
                        📊 View Demo Leads
                    </a>
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

# =====================================================================
# RUN SERVER
# =====================================================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)