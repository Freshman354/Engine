import sqlite3
import bcrypt
import secrets
from datetime import datetime
import json

DATABASE = 'chatbot.db'

def get_db():
    """Get database connection"""
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initialize database with tables"""
    conn = get_db()
    cursor = conn.cursor()
    
    # Users table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            plan_type TEXT DEFAULT 'starter'
        )
    ''')
    
    # Clients table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS clients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            client_id TEXT UNIQUE NOT NULL,
            company_name TEXT NOT NULL,
            branding_settings TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    ''')
    
    # FAQs table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS faqs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id TEXT NOT NULL,
            faq_id TEXT NOT NULL,
            question TEXT NOT NULL,
            answer TEXT NOT NULL,
            triggers TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients (client_id)
        )
    ''')
    
    # Leads table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id TEXT NOT NULL,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            phone TEXT,
            company TEXT,
            message TEXT,
            conversation_snippet TEXT,
            source_url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients (client_id)
        )
    ''')
    
    # ... existing tables (users, clients, faqs, leads) ...
    
    # Affiliates table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS affiliates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            referral_code TEXT UNIQUE NOT NULL,
            commission_rate REAL DEFAULT 0.30,
            total_earnings REAL DEFAULT 0.0,
            total_referrals INTEGER DEFAULT 0,
            payment_email TEXT,
            payment_method TEXT DEFAULT 'bank_transfer',
            bank_details TEXT,
            status TEXT DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    ''')
    
    # Referrals table (tracks who referred whom)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS referrals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            affiliate_id INTEGER NOT NULL,
            referred_user_id INTEGER NOT NULL,
            referral_code TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            converted_at TIMESTAMP,
            FOREIGN KEY (affiliate_id) REFERENCES affiliates (id),
            FOREIGN KEY (referred_user_id) REFERENCES users (id)
        )
    ''')
    
    # Commissions table (tracks each payment)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS commissions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            affiliate_id INTEGER NOT NULL,
            referred_user_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            subscription_amount REAL NOT NULL,
            plan_type TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            payment_date TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (affiliate_id) REFERENCES affiliates (id),
            FOREIGN KEY (referred_user_id) REFERENCES users (id)
        )
    ''')

    conn.commit()
    conn.close()
    print("✅ Database initialized successfully!")

# User functions
def create_user(email, password, plan_type='starter'):
    """Create a new user"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # Hash password
        password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        cursor.execute(
            'INSERT INTO users (email, password_hash, plan_type) VALUES (?, ?, ?)',
            (email, password_hash, plan_type)
        )
        
        user_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return user_id
    except sqlite3.IntegrityError:
        return None  # Email already exists

def verify_user(email, password):
    """Verify user credentials"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM users WHERE email = ?', (email,))
    user = cursor.fetchone()
    conn.close()
    
    if user and bcrypt.checkpw(password.encode('utf-8'), user['password_hash'].encode('utf-8')):
        return dict(user)
    return None

def get_user_by_id(user_id):
    """Get user by ID"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM users WHERE id = ?', (user_id,))
    user = cursor.fetchone()
    conn.close()
    
    return dict(user) if user else None

def get_user_by_email(email):
    """Get user by email"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM users WHERE email = ?', (email,))
    user = cursor.fetchone()
    conn.close()
    
    return dict(user) if user else None

# Client functions
def create_client(user_id, company_name, branding_settings=None):
    """Create a new client for a user"""
    conn = get_db()
    cursor = conn.cursor()
    
    # Generate unique client_id
    client_id = f"{company_name.lower().replace(' ', '-')}-{secrets.token_hex(4)}"
    
    # Default branding
    if branding_settings is None:
        branding_settings = {
            "company_name": company_name,
            "logo_url": "",
            "primary_color": "#667eea",
            "secondary_color": "#764ba2",
            "bot_avatar": "",
            "bot_name": "Support Assistant",
            "welcome_message": "Hi! How can I help you today?"
        }
    
    cursor.execute(
        'INSERT INTO clients (user_id, client_id, company_name, branding_settings) VALUES (?, ?, ?, ?)',
        (user_id, client_id, company_name, json.dumps(branding_settings))
    )
    
    conn.commit()
    conn.close()
    
    return client_id

def get_user_clients(user_id):
    """Get all clients for a user"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM clients WHERE user_id = ?', (user_id,))
    clients = cursor.fetchall()
    conn.close()
    
    return [dict(client) for client in clients]

def get_client_by_id(client_id):
    """Get client by client_id"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM clients WHERE client_id = ?', (client_id,))
    client = cursor.fetchone()
    conn.close()
    
    return dict(client) if client else None

def get_db_connection():
    """Get database connection"""
    conn = sqlite3.connect('chatbot.db')
    conn.row_factory = sqlite3.Row
    return conn

def verify_client_ownership(user_id, client_id):
    """Verify that a user owns a client"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM clients WHERE client_id = ? AND user_id = ?', (client_id, user_id))
    client = cursor.fetchone()
    conn.close()
    
    return client is not None

# FAQ functions
def save_faqs(client_id, faqs):
    """Save FAQs for a client"""
    conn = get_db()
    cursor = conn.cursor()
    
    # Delete existing FAQs
    cursor.execute('DELETE FROM faqs WHERE client_id = ?', (client_id,))
    
    # Insert new FAQs
    for faq in faqs:
        cursor.execute(
            'INSERT INTO faqs (client_id, faq_id, question, answer, triggers) VALUES (?, ?, ?, ?, ?)',
            (client_id, faq['id'], faq['question'], faq['answer'], json.dumps(faq['triggers']))
        )
    
    conn.commit()
    conn.close()

def get_faqs(client_id):
    """Get all FAQs for a client"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM faqs WHERE client_id = ?', (client_id,))
    faqs = cursor.fetchall()
    conn.close()
    
    return [
        {
            'id': faq['faq_id'],
            'question': faq['question'],
            'answer': faq['answer'],
            'triggers': json.loads(faq['triggers'])
        }
        for faq in faqs
    ]

# Lead functions
def save_lead(client_id, lead_data):
    """Save a lead for a client"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute(
        '''INSERT INTO leads (client_id, name, email, phone, company, message, conversation_snippet, source_url)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
        (
            client_id,
            lead_data['name'],
            lead_data['email'],
            lead_data.get('phone', ''),
            lead_data.get('company', ''),
            lead_data.get('message', ''),
            lead_data.get('conversation_snippet', ''),
            lead_data.get('source_url', '')
        )
    )
    
    conn.commit()
    conn.close()

def get_leads(client_id):
    """Get all leads for a client"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM leads WHERE client_id = ? ORDER BY created_at DESC', (client_id,))
    leads = cursor.fetchall()
    conn.close()
    
    return [dict(lead) for lead in leads]


# Affiliate functions
def create_affiliate(user_id, payment_email, commission_rate=0.30):
    """Create affiliate account for a user"""
    import secrets
    
    conn = get_db()
    cursor = conn.cursor()
    
    # Generate unique referral code
    referral_code = f"{secrets.token_hex(4).upper()}"
    
    try:
        cursor.execute(
            '''INSERT INTO affiliates (user_id, referral_code, commission_rate, payment_email)
               VALUES (?, ?, ?, ?)''',
            (user_id, referral_code, commission_rate, payment_email)
        )
        
        affiliate_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return {
            'id': affiliate_id,
            'referral_code': referral_code,
            'commission_rate': commission_rate
        }
    except sqlite3.IntegrityError:
        conn.close()
        return None

def get_affiliate_by_user_id(user_id):
    """Get affiliate account by user ID"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM affiliates WHERE user_id = ?', (user_id,))
    affiliate = cursor.fetchone()
    conn.close()
    
    return dict(affiliate) if affiliate else None

def get_affiliate_by_code(referral_code):
    """Get affiliate by referral code"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM affiliates WHERE referral_code = ?', (referral_code,))
    affiliate = cursor.fetchone()
    conn.close()
    
    return dict(affiliate) if affiliate else None

def create_referral(affiliate_id, referred_user_id, referral_code):
    """Track a new referral"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute(
        '''INSERT INTO referrals (affiliate_id, referred_user_id, referral_code, status)
           VALUES (?, ?, ?, ?)''',
        (affiliate_id, referred_user_id, referral_code, 'pending')
    )
    
    # Update total referrals count
    cursor.execute(
        'UPDATE affiliates SET total_referrals = total_referrals + 1 WHERE id = ?',
        (affiliate_id,)
    )
    
    conn.commit()
    conn.close()

def create_commission(affiliate_id, referred_user_id, subscription_amount, plan_type):
    """Create commission record when referred user pays"""
    conn = get_db()
    cursor = conn.cursor()
    
    # Get affiliate's commission rate
    cursor.execute('SELECT commission_rate FROM affiliates WHERE id = ?', (affiliate_id,))
    result = cursor.fetchone()
    commission_rate = result['commission_rate'] if result else 0.30
    
    # Calculate commission
    commission_amount = subscription_amount * commission_rate
    
    # Create commission record
    cursor.execute(
        '''INSERT INTO commissions (affiliate_id, referred_user_id, amount, subscription_amount, plan_type, status)
           VALUES (?, ?, ?, ?, ?, ?)''',
        (affiliate_id, referred_user_id, commission_amount, subscription_amount, plan_type, 'pending')
    )
    
    # Update affiliate's total earnings
    cursor.execute(
        'UPDATE affiliates SET total_earnings = total_earnings + ? WHERE id = ?',
        (commission_amount, affiliate_id)
    )
    
    # Mark referral as converted
    cursor.execute(
        '''UPDATE referrals SET status = 'converted', converted_at = CURRENT_TIMESTAMP
           WHERE affiliate_id = ? AND referred_user_id = ?''',
        (affiliate_id, referred_user_id)
    )
    
    conn.commit()
    conn.close()

def get_affiliate_stats(affiliate_id):
    """Get affiliate's statistics"""
    conn = get_db()
    cursor = conn.cursor()
    
    # Get basic stats
    cursor.execute('SELECT * FROM affiliates WHERE id = ?', (affiliate_id,))
    affiliate = dict(cursor.fetchone())
    
    # Get referral count by status
    cursor.execute(
        '''SELECT status, COUNT(*) as count FROM referrals 
           WHERE affiliate_id = ? GROUP BY status''',
        (affiliate_id,)
    )
    referral_stats = {row['status']: row['count'] for row in cursor.fetchall()}
    
    # Get pending commissions
    cursor.execute(
        '''SELECT SUM(amount) as pending FROM commissions 
           WHERE affiliate_id = ? AND status = 'pending' ''',
        (affiliate_id,)
    )
    pending_result = cursor.fetchone()
    pending_earnings = pending_result['pending'] if pending_result['pending'] else 0
    
    # Get paid commissions
    cursor.execute(
        '''SELECT SUM(amount) as paid FROM commissions 
           WHERE affiliate_id = ? AND status = 'paid' ''',
        (affiliate_id,)
    )
    paid_result = cursor.fetchone()
    paid_earnings = paid_result['paid'] if paid_result['paid'] else 0
    
    conn.close()
    
    return {
        'affiliate': affiliate,
        'referral_stats': referral_stats,
        'pending_earnings': pending_earnings,
        'paid_earnings': paid_earnings,
        'total_earnings': affiliate['total_earnings']
    }

def get_affiliate_commissions(affiliate_id):
    """Get all commissions for an affiliate"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute(
        '''SELECT c.*, u.email as referred_email 
           FROM commissions c
           JOIN users u ON c.referred_user_id = u.id
           WHERE c.affiliate_id = ?
           ORDER BY c.created_at DESC''',
        (affiliate_id,)
    )
    
    commissions = [dict(row) for row in cursor.fetchall()]
    conn.close()
    
    return commissions    

if __name__ == '__main__':
    init_db()