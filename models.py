import psycopg2
import psycopg2.extras
import bcrypt
import secrets
from datetime import datetime
import json
import os

DATABASE_URL = os.environ.get('DATABASE_URL')

def get_db():
    """Get database connection"""
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return conn, cursor

def get_db_connection():
    """Get database connection (legacy alias)"""
    conn = psycopg2.connect(DATABASE_URL)
    conn.cursor_factory = psycopg2.extras.RealDictCursor
    return conn

def init_db():
    """Initialize database with tables"""
    conn, cursor = get_db()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            plan_type TEXT DEFAULT 'starter'
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS clients (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            client_id TEXT UNIQUE NOT NULL,
            company_name TEXT NOT NULL,
            branding_settings TEXT,
            widget_color TEXT,
            welcome_message TEXT,
            remove_branding BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS faqs (
            id SERIAL PRIMARY KEY,
            client_id TEXT NOT NULL,
            faq_id TEXT NOT NULL,
            question TEXT NOT NULL,
            answer TEXT NOT NULL,
            category TEXT DEFAULT 'General',
            triggers TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients (client_id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS leads (
            id SERIAL PRIMARY KEY,
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

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS conversations (
            id SERIAL PRIMARY KEY,
            client_id TEXT NOT NULL,
            user_message TEXT NOT NULL,
            bot_response TEXT NOT NULL,
            matched BOOLEAN DEFAULT FALSE,
            method TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients (client_id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS affiliates (
            id SERIAL PRIMARY KEY,
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
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS referrals (
            id SERIAL PRIMARY KEY,
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
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS commissions (
            id SERIAL PRIMARY KEY,
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
    cursor.close()
    conn.close()
    print("✅ Database initialized successfully!")


def migrate_clients_table():
    """One-time schema migration for the clients table."""
    conn, cursor = get_db()
    print("🔧 Running clients table migration...")
    cursor.execute("ALTER TABLE clients ADD COLUMN IF NOT EXISTS widget_color TEXT")
    cursor.execute("ALTER TABLE clients ADD COLUMN IF NOT EXISTS welcome_message TEXT")
    cursor.execute("ALTER TABLE clients ADD COLUMN IF NOT EXISTS remove_branding BOOLEAN DEFAULT FALSE")
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Clients table migration complete")


def migrate_faqs_table():
    """One-time schema migration for the faqs table."""
    conn, cursor = get_db()
    print("🔧 Running faqs table migration...")
    cursor.execute("ALTER TABLE faqs ADD COLUMN IF NOT EXISTS category TEXT DEFAULT 'General'")
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ FAQs table migration complete")


# =====================================================================
# PLAN ENFORCEMENT
# =====================================================================

def get_daily_message_count(client_id):
    """
    Return the number of chat messages logged for this client today (UTC).
    Used to enforce messages_per_day plan limits in /api/chat.
    Fails open (returns 0) if the DB is unavailable so chat is never
    blocked by an infrastructure hiccup.
    """
    try:
        conn, cursor = get_db()
        today = datetime.utcnow().strftime('%Y-%m-%d')
        cursor.execute(
            '''
            SELECT COUNT(*) AS cnt
            FROM conversations
            WHERE client_id = %s
              AND DATE(timestamp) = %s
            ''',
            (client_id, today)
        )
        row = cursor.fetchone() or {}
        cursor.close()
        conn.close()
        return int(row.get('cnt', 0))
    except Exception:
        return 0  # fail open — never block chat due to a DB error


def get_client_owner(client_id):
    """
    Return the full user row for whoever owns this client_id.
    Used by plan enforcement helpers in app.py.
    Returns None if client or user not found.
    """
    try:
        conn, cursor = get_db()
        cursor.execute(
            'SELECT user_id FROM clients WHERE client_id = %s',
            (client_id,)
        )
        row = cursor.fetchone()
        if not row:
            cursor.close()
            conn.close()
            return None
        user_id = row['user_id']
        cursor.execute('SELECT * FROM users WHERE id = %s', (user_id,))
        user = cursor.fetchone()
        cursor.close()
        conn.close()
        return dict(user) if user else None
    except Exception:
        return None


# =====================================================================
# USER FUNCTIONS
# =====================================================================

def create_user(email, password, plan_type='starter'):
    """Create a new user"""
    try:
        conn, cursor = get_db()
        password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        cursor.execute(
            'INSERT INTO users (email, password_hash, plan_type) VALUES (%s, %s, %s) RETURNING id',
            (email, password_hash, plan_type)
        )
        user_id = cursor.fetchone()['id']
        conn.commit()
        cursor.close()
        conn.close()
        return user_id
    except psycopg2.IntegrityError:
        return None  # Email already exists

def verify_user(email, password):
    """Verify user credentials"""
    conn, cursor = get_db()
    cursor.execute('SELECT * FROM users WHERE email = %s', (email,))
    user = cursor.fetchone()
    cursor.close()
    conn.close()
    if user and bcrypt.checkpw(password.encode('utf-8'), user['password_hash'].encode('utf-8')):
        return dict(user)
    return None

def get_user_by_id(user_id):
    """Get user by ID"""
    conn, cursor = get_db()
    cursor.execute('SELECT * FROM users WHERE id = %s', (user_id,))
    user = cursor.fetchone()
    cursor.close()
    conn.close()
    return dict(user) if user else None

def get_user_by_email(email):
    """Get user by email"""
    conn, cursor = get_db()
    cursor.execute('SELECT * FROM users WHERE email = %s', (email,))
    user = cursor.fetchone()
    cursor.close()
    conn.close()
    return dict(user) if user else None


# =====================================================================
# CLIENT FUNCTIONS
# =====================================================================

def create_client(user_id, company_name, branding_settings=None):
    """Create a new client for a user"""
    conn, cursor = get_db()
    client_id = f"{company_name.lower().replace(' ', '-')}-{secrets.token_hex(4)}"
    
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
    
    primary_color = branding_settings.get('branding', {}).get('primary_color')
    welcome_msg = branding_settings.get('bot_settings', {}).get('welcome_message')
    remove_flag = bool(branding_settings.get('branding', {}).get('remove_branding', False))

    cursor.execute(
        '''INSERT INTO clients (user_id, client_id, company_name, branding_settings, widget_color, welcome_message, remove_branding)
           VALUES (%s, %s, %s, %s, %s, %s, %s)''',
        (user_id, client_id, company_name, json.dumps(branding_settings), primary_color, welcome_msg, remove_flag)
    )
    conn.commit()
    cursor.close()
    conn.close()
    return client_id

def get_user_clients(user_id):
    """Get all clients for a user"""
    conn, cursor = get_db()
    cursor.execute('SELECT * FROM clients WHERE user_id = %s', (user_id,))
    clients = cursor.fetchall()
    cursor.close()
    conn.close()
    return [dict(client) for client in clients]

def get_client_by_id(client_id):
    """Get client by client_id"""
    conn, cursor = get_db()
    cursor.execute('SELECT * FROM clients WHERE client_id = %s', (client_id,))
    client = cursor.fetchone()
    cursor.close()
    conn.close()
    return dict(client) if client else None

def verify_client_ownership(user_id, client_id):
    """Verify that a user owns a client"""
    conn, cursor = get_db()
    cursor.execute('SELECT * FROM clients WHERE client_id = %s AND user_id = %s', (client_id, user_id))
    client = cursor.fetchone()
    cursor.close()
    conn.close()
    return client is not None


# =====================================================================
# FAQ FUNCTIONS
# =====================================================================

def save_faqs(client_id, faqs):
    """Save FAQs for a client (replaces all existing)"""
    conn, cursor = get_db()
    cursor.execute('DELETE FROM faqs WHERE client_id = %s', (client_id,))
    for faq in faqs:
        cursor.execute(
            'INSERT INTO faqs (client_id, faq_id, question, answer, category, triggers) VALUES (%s, %s, %s, %s, %s, %s)',
            (
                client_id,
                faq['id'],
                faq['question'],
                faq['answer'],
                faq.get('category', 'General'),
                json.dumps(faq['triggers'])
            )
        )
    conn.commit()
    cursor.close()
    conn.close()

def get_faqs(client_id):
    """Get all FAQs for a client"""
    conn, cursor = get_db()
    cursor.execute('SELECT * FROM faqs WHERE client_id = %s', (client_id,))
    faqs = cursor.fetchall()
    cursor.close()
    conn.close()
    return [
        {
            'id': faq['faq_id'],
            'question': faq['question'],
            'answer': faq['answer'],
            'category': faq.get('category', 'General'),
            'triggers': json.loads(faq['triggers'])
        }
        for faq in faqs
    ]


# =====================================================================
# LEAD FUNCTIONS
# =====================================================================

def save_lead(client_id, lead_data):
    """Save a lead for a client"""
    conn, cursor = get_db()
    cursor.execute(
        '''INSERT INTO leads (client_id, name, email, phone, company, message, conversation_snippet, source_url)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)''',
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
    cursor.close()
    conn.close()

def get_leads(client_id):
    """Get all leads for a client"""
    conn, cursor = get_db()
    cursor.execute('SELECT * FROM leads WHERE client_id = %s ORDER BY created_at DESC', (client_id,))
    leads = cursor.fetchall()
    cursor.close()
    conn.close()
    return [dict(lead) for lead in leads]


# =====================================================================
# AFFILIATE FUNCTIONS
# =====================================================================

def create_affiliate(user_id, payment_email, commission_rate=0.30):
    """Create affiliate account for a user"""
    conn, cursor = get_db()
    referral_code = secrets.token_hex(4).upper()
    try:
        cursor.execute(
            '''INSERT INTO affiliates (user_id, referral_code, commission_rate, payment_email)
               VALUES (%s, %s, %s, %s) RETURNING id''',
            (user_id, referral_code, commission_rate, payment_email)
        )
        affiliate_id = cursor.fetchone()['id']
        conn.commit()
        cursor.close()
        conn.close()
        return {'id': affiliate_id, 'referral_code': referral_code, 'commission_rate': commission_rate}
    except psycopg2.IntegrityError:
        cursor.close()
        conn.close()
        return None

def get_affiliate_by_user_id(user_id):
    """Get affiliate account by user ID"""
    conn, cursor = get_db()
    cursor.execute('SELECT * FROM affiliates WHERE user_id = %s', (user_id,))
    affiliate = cursor.fetchone()
    cursor.close()
    conn.close()
    return dict(affiliate) if affiliate else None

def get_affiliate_by_code(referral_code):
    """Get affiliate by referral code"""
    conn, cursor = get_db()
    cursor.execute('SELECT * FROM affiliates WHERE referral_code = %s', (referral_code,))
    affiliate = cursor.fetchone()
    cursor.close()
    conn.close()
    return dict(affiliate) if affiliate else None

def create_referral(affiliate_id, referred_user_id, referral_code):
    """Track a new referral"""
    conn, cursor = get_db()
    cursor.execute(
        '''INSERT INTO referrals (affiliate_id, referred_user_id, referral_code, status)
           VALUES (%s, %s, %s, %s)''',
        (affiliate_id, referred_user_id, referral_code, 'pending')
    )
    cursor.execute(
        'UPDATE affiliates SET total_referrals = total_referrals + 1 WHERE id = %s',
        (affiliate_id,)
    )
    conn.commit()
    cursor.close()
    conn.close()

def create_commission(affiliate_id, referred_user_id, subscription_amount, plan_type):
    """Create commission record when referred user pays"""
    conn, cursor = get_db()
    cursor.execute('SELECT commission_rate FROM affiliates WHERE id = %s', (affiliate_id,))
    result = cursor.fetchone()
    commission_rate = result['commission_rate'] if result else 0.30
    commission_amount = subscription_amount * commission_rate
    cursor.execute(
        '''INSERT INTO commissions (affiliate_id, referred_user_id, amount, subscription_amount, plan_type, status)
           VALUES (%s, %s, %s, %s, %s, %s)''',
        (affiliate_id, referred_user_id, commission_amount, subscription_amount, plan_type, 'pending')
    )
    cursor.execute(
        'UPDATE affiliates SET total_earnings = total_earnings + %s WHERE id = %s',
        (commission_amount, affiliate_id)
    )
    cursor.execute(
        '''UPDATE referrals SET status = 'converted', converted_at = CURRENT_TIMESTAMP
           WHERE affiliate_id = %s AND referred_user_id = %s''',
        (affiliate_id, referred_user_id)
    )
    conn.commit()
    cursor.close()
    conn.close()

def get_affiliate_stats(affiliate_id):
    """Get affiliate's statistics"""
    conn, cursor = get_db()
    cursor.execute('SELECT * FROM affiliates WHERE id = %s', (affiliate_id,))
    affiliate = dict(cursor.fetchone())
    cursor.execute(
        'SELECT status, COUNT(*) as count FROM referrals WHERE affiliate_id = %s GROUP BY status',
        (affiliate_id,)
    )
    referral_stats = {row['status']: row['count'] for row in cursor.fetchall()}
    cursor.execute(
        "SELECT SUM(amount) as pending FROM commissions WHERE affiliate_id = %s AND status = 'pending'",
        (affiliate_id,)
    )
    pending_result = cursor.fetchone()
    pending_earnings = pending_result['pending'] if pending_result['pending'] else 0
    cursor.execute(
        "SELECT SUM(amount) as paid FROM commissions WHERE affiliate_id = %s AND status = 'paid'",
        (affiliate_id,)
    )
    paid_result = cursor.fetchone()
    paid_earnings = paid_result['paid'] if paid_result['paid'] else 0
    cursor.close()
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
    conn, cursor = get_db()
    cursor.execute(
        '''SELECT c.*, u.email as referred_email 
           FROM commissions c
           JOIN users u ON c.referred_user_id = u.id
           WHERE c.affiliate_id = %s
           ORDER BY c.created_at DESC''',
        (affiliate_id,)
    )
    commissions = [dict(row) for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return commissions


if __name__ == '__main__':
    init_db()