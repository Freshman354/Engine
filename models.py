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
            custom_fields TEXT,
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

    # Payments table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS payments (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            currency TEXT DEFAULT 'USD',
            payment_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'completed',
            provider TEXT DEFAULT 'manual',
            plan_type TEXT,
            reference TEXT,
            notes TEXT,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS analytics_events (
            id SERIAL PRIMARY KEY,
            user_id INTEGER,
            event_name TEXT NOT NULL,
            metadata TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
# PASSWORD RESET FUNCTIONS
# =====================================================================

def migrate_password_reset_tokens():
    """Create password_reset_tokens table if it doesn't exist."""
    conn, cursor = get_db()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS password_reset_tokens (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            token TEXT NOT NULL UNIQUE,
            expires_at TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    cursor.close()
    conn.close()


def save_password_reset_token(user_id, token, expires_at):
    """Save a password reset token (one per user — delete old ones first)."""
    conn, cursor = get_db()
    cursor.execute('DELETE FROM password_reset_tokens WHERE user_id = %s', (user_id,))
    cursor.execute(
        'INSERT INTO password_reset_tokens (user_id, token, expires_at) VALUES (%s, %s, %s)',
        (user_id, token, expires_at)
    )
    conn.commit()
    cursor.close()
    conn.close()


def get_password_reset_token(token):
    """Return token row if it exists, else None."""
    conn, cursor = get_db()
    cursor.execute('SELECT * FROM password_reset_tokens WHERE token = %s', (token,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return dict(row) if row else None


def delete_password_reset_token(token):
    """Delete a used or expired token."""
    conn, cursor = get_db()
    cursor.execute('DELETE FROM password_reset_tokens WHERE token = %s', (token,))
    conn.commit()
    cursor.close()
    conn.close()


def update_user_password(user_id, new_password):
    """Hash and save a new password for a user."""
    import bcrypt
    hashed = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    conn, cursor = get_db()
    cursor.execute('UPDATE users SET password_hash = %s WHERE id = %s', (hashed, user_id))
    conn.commit()
    cursor.close()
    conn.close()

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


def delete_client(client_id):
    """
    Cascade-delete a client and all its associated data:
    conversations → leads → FAQs → client row.
    Order matters because of foreign key constraints.
    """
    conn, cursor = get_db()
    try:
        cursor.execute('DELETE FROM conversations WHERE client_id = %s', (client_id,))
        cursor.execute('DELETE FROM leads WHERE client_id = %s', (client_id,))
        cursor.execute('DELETE FROM faqs WHERE client_id = %s', (client_id,))
        cursor.execute('DELETE FROM clients WHERE client_id = %s', (client_id,))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


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
        '''INSERT INTO leads (client_id, name, email, phone, company, message, custom_fields, conversation_snippet, source_url)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)''',
        (
            client_id,
            lead_data['name'],
            lead_data['email'],
            lead_data.get('phone', ''),
            lead_data.get('company', ''),
            lead_data.get('message', ''),
            lead_data.get('custom_fields'),
            lead_data.get('conversation_snippet', ''),
            lead_data.get('source_url', '')
        )
    )
    conn.commit()
    cursor.close()
    conn.close()


def migrate_lead_custom_fields():
    """Add custom_fields column to leads table if not present."""
    conn, cursor = get_db()
    cursor.execute("ALTER TABLE leads ADD COLUMN IF NOT EXISTS custom_fields TEXT")
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



# =====================================================================
# ADMIN MIGRATIONS
# =====================================================================

def migrate_admin_columns():
    """Add is_admin, subscription_status, upgraded_at, cancelled_at to users."""
    conn, cursor = get_db()
    print("Running admin column migration...")
    cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS is_admin BOOLEAN DEFAULT FALSE")
    cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS subscription_status TEXT DEFAULT 'active'")
    cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS upgraded_at TIMESTAMP")
    cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMP")
    conn.commit()
    cursor.close()
    conn.close()
    print("Admin columns migration complete")


def migrate_payments_and_events():
    """Create payments and analytics_events tables."""
    conn, cursor = get_db()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS payments (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            currency TEXT DEFAULT 'USD',
            payment_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'completed',
            provider TEXT DEFAULT 'manual',
            plan_type TEXT,
            reference TEXT,
            notes TEXT,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS analytics_events (
            id SERIAL PRIMARY KEY,
            user_id INTEGER,
            event_name TEXT NOT NULL,
            metadata TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    cursor.close()
    conn.close()
    print("Payments and analytics_events tables ready")


# =====================================================================
# EVENT TRACKING
# =====================================================================

def track_event(event_name, user_id=None, metadata=None):
    """
    Log a named event to analytics_events.
    Fails silently so it never disrupts the main request.
    Usage: track_event('login', user_id=5, metadata={'plan': 'pro'})
    """
    try:
        conn, cursor = get_db()
        cursor.execute(
            'INSERT INTO analytics_events (user_id, event_name, metadata) VALUES (%s, %s, %s)',
            (user_id, event_name, json.dumps(metadata) if metadata else None)
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception:
        pass


# =====================================================================
# PAYMENT FUNCTIONS
# =====================================================================

def record_payment(user_id, amount, plan_type, provider='manual', currency='USD',
                   status='completed', reference=None, notes=None):
    """Insert a payment record and return its id."""
    conn, cursor = get_db()
    cursor.execute(
        '''INSERT INTO payments
           (user_id, amount, currency, status, provider, plan_type, reference, notes)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id''',
        (user_id, amount, currency, status, provider, plan_type, reference, notes)
    )
    payment_id = cursor.fetchone()['id']
    conn.commit()
    cursor.close()
    conn.close()
    return payment_id


def get_all_payments(limit=200):
    """Get recent payments joined with user email."""
    conn, cursor = get_db()
    cursor.execute(
        '''SELECT p.*, u.email
           FROM payments p
           JOIN users u ON p.user_id = u.id
           ORDER BY p.payment_date DESC
           LIMIT %s''',
        (limit,)
    )
    rows = [dict(r) for r in cursor.fetchall()]
    cursor.close()
    conn.close()
    for r in rows:
        if r.get('payment_date'):
            r['payment_date'] = r['payment_date'].isoformat()
    return rows


def get_mrr():
    """Sum completed payments in the current calendar month."""
    conn, cursor = get_db()
    cursor.execute(
        """SELECT COALESCE(SUM(amount), 0) AS mrr
           FROM payments
           WHERE status = 'completed'
             AND DATE_TRUNC('month', payment_date) = DATE_TRUNC('month', CURRENT_DATE)"""
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return float(row['mrr']) if row else 0.0


def get_total_revenue():
    """Sum of all completed payments ever."""
    conn, cursor = get_db()
    cursor.execute("SELECT COALESCE(SUM(amount), 0) AS total FROM payments WHERE status = 'completed'")
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return float(row['total']) if row else 0.0


def get_revenue_by_month(months=6):
    """Monthly revenue totals for the last N months."""
    conn, cursor = get_db()
    cursor.execute(
        """SELECT TO_CHAR(DATE_TRUNC('month', payment_date), 'Mon YYYY') AS month,
                  DATE_TRUNC('month', payment_date) AS month_date,
                  COALESCE(SUM(amount), 0) AS revenue
           FROM payments
           WHERE status = 'completed'
             AND payment_date >= CURRENT_DATE - INTERVAL '%(m)s months'
           GROUP BY DATE_TRUNC('month', payment_date)
           ORDER BY month_date ASC""" % {'m': months}
    )
    rows = [{'month': r['month'], 'revenue': float(r['revenue'])} for r in cursor.fetchall()]
    cursor.close()
    conn.close()
    return rows


# =====================================================================
# ADMIN USER FUNCTIONS
# =====================================================================

def get_all_users(limit=500):
    """All users for admin panel, newest first."""
    conn, cursor = get_db()
    cursor.execute(
        '''SELECT id, email, plan_type, subscription_status, is_admin,
                  created_at, upgraded_at, cancelled_at
           FROM users
           ORDER BY created_at DESC
           LIMIT %s''',
        (limit,)
    )
    rows = [dict(r) for r in cursor.fetchall()]
    cursor.close()
    conn.close()
    for r in rows:
        for col in ['created_at', 'upgraded_at', 'cancelled_at']:
            if r.get(col):
                r[col] = r[col].isoformat()
    return rows


def get_user_count_by_plan():
    """Users grouped by plan_type."""
    conn, cursor = get_db()
    cursor.execute(
        'SELECT plan_type, COUNT(*) AS cnt FROM users GROUP BY plan_type ORDER BY cnt DESC'
    )
    rows = {r['plan_type']: int(r['cnt']) for r in cursor.fetchall()}
    cursor.close()
    conn.close()
    return rows


def get_new_users_this_month():
    """Count signups in the current calendar month."""
    conn, cursor = get_db()
    cursor.execute(
        """SELECT COUNT(*) AS cnt FROM users
           WHERE DATE_TRUNC('month', created_at) = DATE_TRUNC('month', CURRENT_DATE)"""
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return int(row['cnt']) if row else 0


def get_user_growth_by_month(months=6):
    """New signups per month for the last N months."""
    conn, cursor = get_db()
    cursor.execute(
        """SELECT TO_CHAR(DATE_TRUNC('month', created_at), 'Mon YYYY') AS month,
                  DATE_TRUNC('month', created_at) AS month_date,
                  COUNT(*) AS count
           FROM users
           WHERE created_at >= CURRENT_DATE - INTERVAL '%(m)s months'
           GROUP BY DATE_TRUNC('month', created_at)
           ORDER BY month_date ASC""" % {'m': months}
    )
    rows = [{'month': r['month'], 'count': int(r['count'])} for r in cursor.fetchall()]
    cursor.close()
    conn.close()
    return rows


def admin_update_user(user_id, plan_type=None, subscription_status=None, is_admin=None):
    """Update user plan, subscription_status, or admin flag."""
    conn, cursor = get_db()
    updates = []
    params = []
    if plan_type is not None:
        updates.append('plan_type = %s')
        params.append(plan_type)
        updates.append('upgraded_at = CURRENT_TIMESTAMP')
    if subscription_status is not None:
        updates.append('subscription_status = %s')
        params.append(subscription_status)
        if subscription_status == 'cancelled':
            updates.append('cancelled_at = CURRENT_TIMESTAMP')
    if is_admin is not None:
        updates.append('is_admin = %s')
        params.append(bool(is_admin))
    if not updates:
        cursor.close()
        conn.close()
        return False
    params.append(user_id)
    cursor.execute('UPDATE users SET ' + ', '.join(updates) + ' WHERE id = %s', params)
    conn.commit()
    cursor.close()
    conn.close()
    return True


def admin_delete_user(user_id):
    """Hard-delete a user and cascade all their data."""
    conn, cursor = get_db()
    try:
        cursor.execute('SELECT client_id FROM clients WHERE user_id = %s', (user_id,))
        client_ids = [r['client_id'] for r in cursor.fetchall()]
        for cid in client_ids:
            cursor.execute('DELETE FROM conversations WHERE client_id = %s', (cid,))
            cursor.execute('DELETE FROM leads WHERE client_id = %s', (cid,))
            cursor.execute('DELETE FROM faqs WHERE client_id = %s', (cid,))
        cursor.execute('DELETE FROM clients WHERE user_id = %s', (user_id,))
        cursor.execute('DELETE FROM commissions WHERE referred_user_id = %s', (user_id,))
        cursor.execute('DELETE FROM referrals WHERE referred_user_id = %s', (user_id,))
        cursor.execute('DELETE FROM affiliates WHERE user_id = %s', (user_id,))
        cursor.execute('DELETE FROM payments WHERE user_id = %s', (user_id,))
        cursor.execute('DELETE FROM analytics_events WHERE user_id = %s', (user_id,))
        cursor.execute('DELETE FROM users WHERE id = %s', (user_id,))
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def get_all_leads_admin(limit=500, client_id_filter=None, search=None):
    """Leads across all clients for admin view."""
    conn, cursor = get_db()
    query = '''SELECT l.*, c.company_name, u.email as owner_email
               FROM leads l
               LEFT JOIN clients c ON l.client_id = c.client_id
               LEFT JOIN users u ON c.user_id = u.id
               WHERE 1=1'''
    params = []
    if client_id_filter:
        query += ' AND l.client_id = %s'
        params.append(client_id_filter)
    if search:
        query += ' AND (l.name ILIKE %s OR l.email ILIKE %s)'
        params.extend(['%' + search + '%', '%' + search + '%'])
    query += ' ORDER BY l.created_at DESC LIMIT %s'
    params.append(limit)
    cursor.execute(query, params)
    rows = [dict(r) for r in cursor.fetchall()]
    cursor.close()
    conn.close()
    for r in rows:
        if r.get('created_at'):
            r['created_at'] = r['created_at'].isoformat()
    return rows


def admin_delete_lead(lead_id):
    """Delete a single lead by id."""
    conn, cursor = get_db()
    cursor.execute('DELETE FROM leads WHERE id = %s', (lead_id,))
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == '__main__':
    init_db()