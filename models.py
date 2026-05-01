import psycopg2
import psycopg2.extras
import bcrypt
import secrets
from datetime import datetime
import json
import uuid
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

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS articles (
            id SERIAL PRIMARY KEY,
            client_id TEXT NOT NULL,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            category TEXT DEFAULT 'General',
            position INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS client_users (
            id SERIAL PRIMARY KEY,
            client_id TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            name TEXT,
            role TEXT DEFAULT 'client',
            invited_by INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_login TIMESTAMP
        )
    ''')

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Database initialized successfully!")

    # Run FAQ migrations immediately so fresh installs are fully ready
    # without needing a separate startup call.
    migrate_faqs_table()
    migrate_faq_to_knowledge_base()


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


def migrate_white_label():
    """
    Adds white-label columns to clients and users tables.
    Safe to run on every startup — uses IF NOT EXISTS / SAVEPOINT pattern.
    """
    conn, cursor = get_db()
    try:
        # ── clients table ─────────────────────────────────────────────
        for sql in [
            # Custom domain the client owner CNAMEs to lumvi.net
            "ALTER TABLE clients ADD COLUMN IF NOT EXISTS custom_widget_domain TEXT",
            # Custom CSS injected into the widget <style> block
            "ALTER TABLE clients ADD COLUMN IF NOT EXISTS custom_css TEXT",
            # Branded "From" email name  e.g. "Acme Support"
            "ALTER TABLE clients ADD COLUMN IF NOT EXISTS branded_email_from TEXT",
        ]:
            cursor.execute(sql)

        # ── users table ────────────────────────────────────────────────
        for sql in [
            # Agency default branding JSON; auto-applied to new clients
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS agency_branding_settings TEXT",
        ]:
            cursor.execute(sql)

        conn.commit()
        print("✅ migrate_white_label complete")
    except Exception as e:
        conn.rollback()
        print(f"⚠️  migrate_white_label: {e}")
    finally:
        cursor.close()
        conn.close()


def migrate_client_status():
    """
    Add `is_suspended` boolean to clients table.
    Safe — uses IF NOT EXISTS.
    """
    try:
        conn, cursor = get_db()
        cursor.execute(
            "ALTER TABLE clients ADD COLUMN IF NOT EXISTS is_suspended BOOLEAN DEFAULT FALSE"
        )
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ migrate_client_status complete")
    except Exception as e:
        print(f"⚠️  migrate_client_status: {e}")


def migrate_onboarding():
    """Add onboarding_completed column to users table. Safe — uses IF NOT EXISTS."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS onboarding_completed BOOLEAN DEFAULT FALSE"
        )
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ migrate_onboarding complete")
    except Exception as e:
        print(f"⚠️  migrate_onboarding: {e}")


def mark_onboarding_complete(user_id: int) -> None:
    """Mark user's onboarding as done — prevents wizard from re-appearing."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            "UPDATE users SET onboarding_completed = TRUE WHERE id = %s",
            (user_id,)
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[mark_onboarding_complete] {e}")
    """
    Idempotent migration for the faqs table.
    Adds all columns needed for Phase 2 RAG.
    Safe to call on every startup.
    """
    conn, cursor = get_db()
    migrations = [
        "ALTER TABLE faqs ADD COLUMN IF NOT EXISTS category      TEXT    DEFAULT 'General'",
        "ALTER TABLE faqs ADD COLUMN IF NOT EXISTS quality_score  REAL    DEFAULT 0.0",
        "ALTER TABLE faqs ADD COLUMN IF NOT EXISTS embedding      TEXT",
        "ALTER TABLE faqs ADD COLUMN IF NOT EXISTS tags           TEXT    DEFAULT '[]'",
        "ALTER TABLE faqs ADD COLUMN IF NOT EXISTS last_indexed   TIMESTAMP",
        "ALTER TABLE faqs ADD COLUMN IF NOT EXISTS is_active      BOOLEAN DEFAULT TRUE",
    ]
    try:
        for sql in migrations:
            cursor.execute(sql)
        conn.commit()
        print("✅ FAQs table migration complete")
    except Exception as e:
        conn.rollback()
        print(f"⚠️  migrate_faqs_table: {e}")
    finally:
        cursor.close()
        conn.close()


def migrate_faq_to_knowledge_base():
    """
    Idempotent migration that:
      1. Adds RAG columns to the faqs table
      2. Adds a UNIQUE constraint on faq_id (needed for ON CONFLICT upserts)
      3. Creates the knowledge_base table

    The UNIQUE constraint is wrapped in a SAVEPOINT so that if it already
    exists, only that statement is rolled back — the rest of the migration
    continues cleanly. Without a SAVEPOINT, a constraint-already-exists error
    would abort the entire PostgreSQL transaction and block the CREATE TABLE
    that follows.
    """
    conn, cursor = get_db()
    try:
        # ── 1. Add new columns to faqs ────────────────────────────────
        for sql in [
            "ALTER TABLE faqs ADD COLUMN IF NOT EXISTS quality_score REAL    DEFAULT 0.0",
            "ALTER TABLE faqs ADD COLUMN IF NOT EXISTS embedding     TEXT",
            "ALTER TABLE faqs ADD COLUMN IF NOT EXISTS tags          TEXT    DEFAULT '[]'",
            "ALTER TABLE faqs ADD COLUMN IF NOT EXISTS last_indexed  TIMESTAMP",
            "ALTER TABLE faqs ADD COLUMN IF NOT EXISTS is_active     BOOLEAN DEFAULT TRUE",
        ]:
            cursor.execute(sql)

        # ── 2. UNIQUE constraint on faq_id (safe via SAVEPOINT) ───────
        # A plain try/except is NOT enough in PostgreSQL: if the ALTER fails,
        # the transaction enters an aborted state and no further SQL can run
        # until a rollback. SAVEPOINT lets us roll back just this one statement.
        cursor.execute("SAVEPOINT sp_faq_unique")
        try:
            cursor.execute(
                "ALTER TABLE faqs ADD CONSTRAINT faqs_faq_id_unique UNIQUE (faq_id)"
            )
            cursor.execute("RELEASE SAVEPOINT sp_faq_unique")
        except Exception:
            cursor.execute("ROLLBACK TO SAVEPOINT sp_faq_unique")
            # Constraint already exists — that's fine, continue

        # ── 3. Create knowledge_base table ────────────────────────────
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS knowledge_base (
                id            SERIAL PRIMARY KEY,
                client_id     TEXT   NOT NULL,
                kb_id         TEXT   NOT NULL UNIQUE,
                title         TEXT   NOT NULL,
                content       TEXT   NOT NULL,
                type          TEXT   DEFAULT 'faq',
                category      TEXT   DEFAULT 'General',
                tags          TEXT   DEFAULT '[]',
                embedding     TEXT,
                metadata      TEXT   DEFAULT '{}',
                quality_score REAL   DEFAULT 0.8,
                version       INTEGER DEFAULT 1,
                created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_kb_client ON knowledge_base (client_id)"
        )

        conn.commit()
        print("✅ migrate_faq_to_knowledge_base complete")
    except Exception as e:
        conn.rollback()
        print(f"⚠️  migrate_faq_to_knowledge_base: {e}")
    finally:
        cursor.close()
        conn.close()


# ── Keyword stop-words for tag/trigger extraction ──────────────────────
_STOP_WORDS = {
    'a','an','the','is','are','do','does','can','i','you','we','my','your',
    'what','how','when','where','why','to','of','in','on','at','for','with',
    'and','or','but','not','it','this','that','be','have','has','was','were',
    'will','would','could','should','may','might','please','hi','hello','hey',
}


def _extract_keywords(text: str, limit: int = 8) -> list:
    """Simple keyword extractor — used when ai_helper is unavailable."""
    import re
    words = re.findall(r"\b[a-z]{3,}\b", text.lower())
    seen, result = set(), []
    for w in words:
        if w not in _STOP_WORDS and w not in seen:
            seen.add(w)
            result.append(w)
            if len(result) >= limit:
                break
    return result


def _simple_extract_tags(text: str, limit: int = 5) -> list:
    """
    Fallback tag generator — noun-biased, shorter list than _extract_keywords.
    Prefers longer words (more likely to be meaningful nouns/concepts).
    Called by validate_and_enrich_faqs when no tags are provided and
    the AI helper is unavailable.
    """
    import re
    words = re.findall(r"\b[a-z]{4,}\b", text.lower())   # min 4 chars → fewer stop-words
    seen, result = set(), []
    for w in sorted(set(words), key=lambda w: -len(w)):   # longer words first
        if w not in _STOP_WORDS and w not in seen:
            seen.add(w)
            result.append(w)
            if len(result) >= limit:
                break
    return result


def _quality_score(question: str, answer: str) -> float:
    """Heuristic quality score 0.0–1.0."""
    score = 0.4
    if len(question) >= 15:  score += 0.15
    if len(answer)   >= 60:  score += 0.15
    if len(answer)   >= 150: score += 0.10
    if answer.rstrip().endswith(('.', '!', '?')): score += 0.10
    if '?' in question:      score += 0.10
    return round(min(score, 1.0), 2)


def validate_and_enrich_faqs(raw_faqs: list, client_id: str) -> tuple:
    """
    Validate, deduplicate, and enrich a list of raw FAQ dicts.

    Validation rules:
      - question must be >= 10 chars
      - answer must be >= 20 chars
      - duplicate questions (case-insensitive) are skipped after the first

    Enrichment (applied to every passing item):
      - auto-generates triggers if missing
      - generates basic tags if missing
      - calculates quality_score
      - assigns a stable faq_id if not present

    The calls to _extract_keywords / _simple_extract_tags are wrapped in
    try/except so that if either helper is ever unavailable (import error,
    future refactor), enrichment degrades gracefully to a simple word split
    rather than aborting the entire upload.

    Returns: (valid_faqs: list[dict], errors: list[dict])
    Each error dict: {row: int, question: str, reason: str}
    """
    # ── Inline fallbacks in case module-level helpers are not reachable ──
    def _fallback_keywords(text: str, limit: int = 8) -> list:
        import re as _re
        stop = {'the','and','for','are','but','not','you','all','can','has','her',
                'was','one','our','out','day','get','has','him','his','how','its',
                'may','new','now','old','see','two','who','boy','did','its','let',
                'put','say','she','too','use','way','what','when','with','have'}
        words = _re.findall(r'\b[a-z]{3,}\b', text.lower())
        seen, result = set(), []
        for w in words:
            if w not in stop and w not in seen:
                seen.add(w); result.append(w)
                if len(result) >= limit:
                    break
        return result

    def _fallback_tags(text: str, limit: int = 5) -> list:
        import re as _re
        stop = {'the','and','for','are','but','not','you','all','can','has','her',
                'was','one','our','out','day','get','has','him','his','how','its',
                'may','new','now','old','see','two','who','boy','did','its','let',
                'put','say','she','too','use','way','what','when','with','have'}
        words = _re.findall(r'\b[a-z]{4,}\b', text.lower())
        seen, result = set(), []
        for w in sorted(set(words), key=lambda x: -len(x)):
            if w not in stop and w not in seen:
                seen.add(w); result.append(w)
                if len(result) >= limit:
                    break
        return result

    valid  = []
    errors = []
    seen_questions: set = set()

    for row_num, raw in enumerate(raw_faqs, start=1):
        question = str(raw.get('question') or '').strip()
        answer   = str(raw.get('answer')   or '').strip()

        # ── Validation ────────────────────────────────────────────────
        if len(question) < 10:
            errors.append({'row': row_num, 'question': question[:60],
                           'reason': f"Question too short ({len(question)} chars, min 10)"})
            continue

        if len(answer) < 20:
            errors.append({'row': row_num, 'question': question[:60],
                           'reason': f"Answer too short ({len(answer)} chars, min 20)"})
            continue

        q_norm = question.lower().strip()
        if q_norm in seen_questions:
            errors.append({'row': row_num, 'question': question[:60],
                           'reason': "Duplicate question (skipped)"})
            continue
        seen_questions.add(q_norm)

        # ── Enrichment ────────────────────────────────────────────────
        faq_id   = str(raw.get('id') or raw.get('faq_id') or uuid.uuid4())
        category = str(raw.get('category') or 'General').strip() or 'General'

        # Triggers — parse then auto-generate if empty
        triggers = raw.get('triggers', [])
        if isinstance(triggers, str):
            try:
                triggers = json.loads(triggers)
            except Exception:
                triggers = [t.strip() for t in triggers.split(',') if t.strip()]
        if not isinstance(triggers, list):
            triggers = []
        if not triggers:
            try:
                triggers = _extract_keywords(question)
            except Exception:
                triggers = _fallback_keywords(question)

        # Tags — parse then auto-generate if empty (noun-biased, shorter)
        tags = raw.get('tags', [])
        if isinstance(tags, str):
            try:
                tags = json.loads(tags)
            except Exception:
                tags = [t.strip() for t in tags.split(',') if t.strip()]
        if not isinstance(tags, list):
            tags = []
        if not tags:
            try:
                tags = _simple_extract_tags(question)
            except Exception:
                tags = _fallback_tags(question)

        # Quality score
        try:
            quality = _quality_score(question, answer)
        except Exception:
            quality = 0.5

        valid.append({
            'id':            faq_id,
            'faq_id':        faq_id,
            'question':      question,
            'answer':        answer,
            'category':      category,
            'triggers':      triggers,
            'tags':          tags,
            'quality_score': quality,
            'embedding':     raw.get('embedding'),  # pass through if already set
        })

    return valid, errors


def migrate_subscription_expiry():
    """Add subscription_expires_at and grace_period_ends_at to users table."""
    conn, cursor = get_db()
    print("🔧 Running subscription expiry migration...")
    cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS subscription_expires_at TIMESTAMP")
    cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS grace_period_ends_at TIMESTAMP")
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Subscription expiry migration complete")


def migrate_to_recurring_subscriptions():
    """
    One-time migration: add recurring billing columns to users table.
    Safe to call on every startup — uses ADD COLUMN IF NOT EXISTS.
    """
    conn, cursor = get_db()
    try:
        migrations = [
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS subscription_id TEXT",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS billing_provider TEXT DEFAULT 'manual'",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS billing_cycle TEXT DEFAULT 'monthly'",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS is_annual BOOLEAN DEFAULT FALSE",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS cancel_at_period_end BOOLEAN DEFAULT FALSE",
        ]
        for sql in migrations:
            cursor.execute(sql)
        conn.commit()
        print("✅ Recurring subscription columns ready.")
    except Exception as e:
        conn.rollback()
        print(f"⚠️ migrate_to_recurring_subscriptions: {e}")
    finally:
        cursor.close()
        conn.close()


def update_user_subscription(user_id, plan_type, billing_provider='flutterwave',
                              subscription_id=None, is_annual=False):
    """
    Upgrade a user to a paid plan and set recurring subscription fields.
    Called after a successful payment callback.
    """
    cycle = 'annual' if is_annual else 'monthly'
    days  = 365 if is_annual else 30
    conn, cursor = get_db()
    try:
        cursor.execute(
            '''UPDATE users
               SET plan_type            = %s,
                   billing_provider     = %s,
                   subscription_id      = %s,
                   billing_cycle        = %s,
                   is_annual            = %s,
                   cancel_at_period_end = FALSE,
                   upgraded_at          = CURRENT_TIMESTAMP,
                   subscription_expires_at = NOW() + INTERVAL %s,
                   grace_period_ends_at    = NOW() + INTERVAL %s
               WHERE id = %s''',
            (plan_type, billing_provider, subscription_id, cycle,
             is_annual, f'{days} days', f'{days + 3} days', user_id)
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def cancel_user_subscription(user_id):
    """
    Mark a subscription to cancel at period end.
    The user keeps access until subscription_expires_at, then the
    scheduler downgrades them automatically.
    Returns True on success.
    """
    conn, cursor = get_db()
    try:
        cursor.execute(
            '''UPDATE users
               SET cancel_at_period_end = TRUE
               WHERE id = %s AND is_admin IS NOT TRUE''',
            (user_id,)
        )
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        conn.rollback()
        cursor.close()
        conn.close()
        return False


def set_trial_expiry(user_id, days=7):
    """Set a free trial expiry. Called on signup for paid plans. Default: 7 days."""
    conn, cursor = get_db()
    cursor.execute(
        '''UPDATE users
           SET subscription_expires_at = NOW() + INTERVAL %s,
               grace_period_ends_at    = NOW() + INTERVAL %s
           WHERE id = %s''',
        (f'{days} days', f'{days + 3} days', user_id)
    )
    conn.commit()
    cursor.close()
    conn.close()


def set_subscription_expiry(user_id):
    """Set subscription_expires_at to 30 days from now and grace to 33 days."""
    conn, cursor = get_db()
    cursor.execute(
        '''UPDATE users
           SET subscription_expires_at = NOW() + INTERVAL '30 days',
               grace_period_ends_at    = NOW() + INTERVAL '33 days'
           WHERE id = %s''',
        (user_id,)
    )
    conn.commit()
    cursor.close()
    conn.close()


def downgrade_expired_users():
    """
    Downgrade all non-admin paid users whose subscription has expired.

    Two conditions trigger a downgrade:
      A) grace_period_ends_at IS NOT NULL AND grace_period_ends_at < NOW()
         → normal path: grace period has elapsed
      B) subscription_expires_at IS NOT NULL AND subscription_expires_at < NOW()
         AND grace_period_ends_at IS NULL
         → legacy path: users who signed up before the grace column existed,
           or whose grace was never set. They get downgraded immediately when
           subscription_expires_at passes.

    Admin users (is_admin = TRUE) are always skipped.
    Returns list of user dicts that were downgraded.
    """
    conn, cursor = get_db()
    try:
        cursor.execute(
            '''SELECT id, email, plan_type FROM users
               WHERE plan_type NOT IN ('free', 'enterprise')
                 AND (is_admin IS NOT TRUE)
                 AND (
                   -- Normal: grace period has passed
                   (grace_period_ends_at IS NOT NULL AND grace_period_ends_at < NOW())
                   OR
                   -- Legacy: no grace set but subscription has expired
                   (grace_period_ends_at IS NULL
                    AND subscription_expires_at IS NOT NULL
                    AND subscription_expires_at < NOW())
                 )'''
        )
        to_downgrade = cursor.fetchall()

        if to_downgrade:
            cursor.execute(
                '''UPDATE users
                   SET plan_type               = 'free',
                       subscription_expires_at = NULL,
                       grace_period_ends_at    = NULL
                   WHERE plan_type NOT IN ('free', 'enterprise')
                     AND (is_admin IS NOT TRUE)
                     AND (
                       (grace_period_ends_at IS NOT NULL AND grace_period_ends_at < NOW())
                       OR
                       (grace_period_ends_at IS NULL
                        AND subscription_expires_at IS NOT NULL
                        AND subscription_expires_at < NOW())
                     )'''
            )
            conn.commit()

        return [dict(u) for u in to_downgrade]
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def downgrade_single_user(user_id):
    """Immediately downgrade one user to free plan."""
    conn, cursor = get_db()
    cursor.execute(
        '''UPDATE users
           SET plan_type               = 'free',
               subscription_expires_at = NULL,
               grace_period_ends_at    = NULL
           WHERE id = %s AND is_admin IS NOT TRUE''',
        (user_id,)
    )
    conn.commit()
    cursor.close()
    conn.close()


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
    """
    Create a new client for a user.
    If the owner is an agency and has agency_branding_settings, those are
    auto-applied to the new client unless branding_settings is explicitly passed.
    """
    conn, cursor = get_db()
    import re as _re
    slug      = _re.sub(r'[^a-z0-9-]', '', company_name.lower().replace(' ', '-'))
    client_id = f"{slug}-{secrets.token_hex(4)}"

    # Auto-inherit agency defaults when nothing is passed
    if branding_settings is None:
        owner = get_user_by_id(user_id)
        agency_raw = (owner or {}).get('agency_branding_settings')
        if agency_raw:
            try:
                agency_bs = json.loads(agency_raw) if isinstance(agency_raw, str) else agency_raw
                # Deep-copy and personalise for this client
                branding_settings = {
                    'branding': dict(agency_bs.get('branding', {})),
                    'bot_settings': dict(agency_bs.get('bot_settings', {})),
                    'contact': dict(agency_bs.get('contact', {})),
                    'integrations': {},
                }
                # Reset company-specific fields so owner fills them in
                branding_settings['branding']['company_name'] = company_name
            except Exception:
                branding_settings = None

    if branding_settings is None:
        branding_settings = {
            'branding': {
                'company_name': company_name,
                'primary_color': '#B8924A',
                'remove_branding': False,
            },
            'bot_settings': {
                'bot_name': 'Support Assistant',
                'welcome_message': 'Hi! How can I help you today?',
            },
            'contact': {},
            'integrations': {},
        }

    primary_color = branding_settings.get('branding', {}).get('primary_color')
    welcome_msg   = branding_settings.get('bot_settings', {}).get('welcome_message')
    remove_flag   = bool(branding_settings.get('branding', {}).get('remove_branding', False))

    cursor.execute(
        '''INSERT INTO clients
               (user_id, client_id, company_name, branding_settings,
                widget_color, welcome_message, remove_branding)
           VALUES (%s, %s, %s, %s, %s, %s, %s)''',
        (user_id, client_id, company_name,
         json.dumps(branding_settings),
         primary_color, welcome_msg, remove_flag)
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


# =====================================================================
# WHITE-LABEL HELPERS
# =====================================================================

_DOMAIN_RE = __import__('re').compile(
    r'^(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}$'
)

def is_valid_domain(domain: str) -> bool:
    """Return True if `domain` looks like a valid hostname (no scheme, no path)."""
    if not domain or len(domain) > 253:
        return False
    return bool(_DOMAIN_RE.match(domain.strip()))


def get_client_by_custom_domain(domain: str):
    """
    Look up a client whose custom_widget_domain matches `domain`.
    Used by the /widget route to serve white-labelled widgets on custom domains.
    """
    try:
        conn, cursor = get_db()
        cursor.execute(
            'SELECT * FROM clients WHERE custom_widget_domain = %s',
            (domain.lower().strip(),)
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        return dict(row) if row else None
    except Exception:
        return None


def save_white_label_settings(client_id: str, domain: str | None,
                               custom_css: str | None,
                               branded_email_from: str | None) -> None:
    """
    Persist the three white-label columns for a client in one atomic UPDATE.
    Pass None for any field to leave it unchanged.
    """
    try:
        conn, cursor = get_db()
        cursor.execute(
            """UPDATE clients
               SET custom_widget_domain = COALESCE(%s, custom_widget_domain),
                   custom_css           = COALESCE(%s, custom_css),
                   branded_email_from   = COALESCE(%s, branded_email_from)
               WHERE client_id = %s""",
            (domain, custom_css, branded_email_from, client_id)
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[save_white_label_settings] {e}")


def get_email_from_for_client(client_id: str) -> dict:
    """
    Return the branded email sender info for a client.

    Priority:
      1. client.branded_email_from
      2. owner agency_branding_settings.branded_email_from
      3. Lumvi default

    Returns {'name': str, 'address': str}
    """
    DEFAULT = {'name': 'Lumvi', 'address': 'support@lumvi.net'}
    try:
        conn, cursor = get_db()
        cursor.execute(
            '''SELECT c.branded_email_from, c.branding_settings,
                      u.agency_branding_settings, c.company_name
               FROM clients c
               JOIN users u ON u.id = c.user_id
               WHERE c.client_id = %s''',
            (client_id,)
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if not row:
            return DEFAULT

        # 1. Client-level override
        if row.get('branded_email_from'):
            return {'name': row['branded_email_from'], 'address': 'support@lumvi.net'}

        # 2. Agency default
        agency_raw = row.get('agency_branding_settings')
        if agency_raw:
            try:
                ab = json.loads(agency_raw) if isinstance(agency_raw, str) else agency_raw
                agency_from = ab.get('branded_email_from')
                if agency_from:
                    return {'name': agency_from, 'address': 'support@lumvi.net'}
            except Exception:
                pass

        # 3. Use company name as a friendly default
        company = row.get('company_name')
        if company:
            return {'name': company, 'address': 'support@lumvi.net'}

        return DEFAULT
    except Exception:
        return DEFAULT


def save_agency_branding(user_id: int, agency_branding: dict) -> None:
    """Persist the agency-wide branding defaults for a user."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            "UPDATE users SET agency_branding_settings = %s WHERE id = %s",
            (json.dumps(agency_branding), user_id)
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[save_agency_branding] {e}")


def get_agency_branding(user_id: int) -> dict:
    """Return the agency's default branding dict, or {}."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            "SELECT agency_branding_settings FROM users WHERE id = %s", (user_id,)
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        raw = (row or {}).get('agency_branding_settings')
        if raw:
            return json.loads(raw) if isinstance(raw, str) else raw
        return {}
    except Exception:
        return {}
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


def toggle_client_suspended(client_id: str, suspend: bool) -> bool:
    """Set is_suspended for a client. Returns True on success."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            "UPDATE clients SET is_suspended = %s WHERE client_id = %s",
            (suspend, client_id)
        )
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[toggle_client_suspended] {e}")
        return False


def clone_client(source_client_id: str, user_id: int, new_name: str) -> str | None:
    """
    Clone a client: copies branding_settings and all FAQs to a new client.
    Returns the new client_id or None on failure.
    """
    import re as _re
    try:
        conn, cursor = get_db()

        # Fetch source
        cursor.execute('SELECT * FROM clients WHERE client_id = %s', (source_client_id,))
        source = cursor.fetchone()
        if not source:
            cursor.close(); conn.close()
            return None

        # Create new client_id
        slug       = _re.sub(r'[^a-z0-9-]', '', new_name.lower().replace(' ', '-'))
        new_cid    = f"{slug}-{secrets.token_hex(4)}"
        bs         = source.get('branding_settings') or '{}'

        cursor.execute(
            '''INSERT INTO clients
                   (user_id, client_id, company_name, branding_settings,
                    widget_color, welcome_message, remove_branding)
               VALUES (%s, %s, %s, %s, %s, %s, %s)''',
            (user_id, new_cid, new_name, bs,
             source.get('widget_color'), source.get('welcome_message'),
             source.get('remove_branding', False))
        )

        # Clone FAQs
        cursor.execute('SELECT * FROM faqs WHERE client_id = %s', (source_client_id,))
        faqs = cursor.fetchall()
        for faq in faqs:
            new_faq_id = f"faq-{secrets.token_hex(4)}"
            cursor.execute(
                '''INSERT INTO faqs
                       (client_id, faq_id, question, answer, triggers, category,
                        quality_score, tags, is_active)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                (new_cid, new_faq_id,
                 faq.get('question', ''), faq.get('answer', ''),
                 faq.get('triggers', '[]'), faq.get('category', 'General'),
                 faq.get('quality_score', 0.0), faq.get('tags', '[]'), True)
            )

        conn.commit()
        cursor.close()
        conn.close()
        return new_cid
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[clone_client] {e}")
        return None


def get_leads_this_month_bulk(client_ids: list) -> dict:
    """Return leads captured this calendar month, keyed by client_id."""
    if not client_ids:
        return {}
    result = {cid: 0 for cid in client_ids}
    try:
        conn, cursor = get_db()
        from datetime import datetime as _dt
        first_of_month = _dt.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        cursor.execute(
            """SELECT client_id, COUNT(*) AS cnt
               FROM leads
               WHERE client_id = ANY(%s) AND created_at >= %s
               GROUP BY client_id""",
            (client_ids, first_of_month)
        )
        for row in cursor.fetchall():
            result[row['client_id']] = int(row['cnt'])
        cursor.close()
        conn.close()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[get_leads_this_month_bulk] {e}")
    return result


# =====================================================================
# FAQ FUNCTIONS
# =====================================================================

def save_faqs(client_id: str, faqs: list) -> int:
    """
    Upsert FAQs for a client.

    ON CONFLICT behaviour (when faq_id already exists):
      - question / answer / category / triggers / tags / quality_score → always updated
      - embedding   → preserved via COALESCE (don't wipe a stored vector on re-save)
      - last_indexed → preserved via COALESCE (don't reset the indexing timestamp)

    triggers and tags are normalised to JSON strings regardless of whether
    they arrive as Python lists, JSON strings, or comma-separated strings.

    Returns count of rows saved.
    """
    if not faqs:
        return 0

    conn, cursor = get_db()
    saved = 0
    try:
        for faq in faqs:
            faq_id = str(faq.get('id') or faq.get('faq_id') or uuid.uuid4())

            # ── Normalise triggers ────────────────────────────────────
            triggers = faq.get('triggers', [])
            if isinstance(triggers, str):
                try:
                    triggers = json.loads(triggers)
                except Exception:
                    triggers = [t.strip() for t in triggers.split(',') if t.strip()]
            if not isinstance(triggers, list):
                triggers = []

            # ── Normalise tags (same pattern as triggers) ─────────────
            tags = faq.get('tags', [])
            if isinstance(tags, str):
                try:
                    tags = json.loads(tags)
                except Exception:
                    tags = [t.strip() for t in tags.split(',') if t.strip()]
            if not isinstance(tags, list):
                tags = []

            # ── Normalise embedding ───────────────────────────────────
            embedding = faq.get('embedding')
            if isinstance(embedding, list):
                embedding_js = json.dumps(embedding)
            elif isinstance(embedding, str) and embedding.startswith('['):
                embedding_js = embedding      # already a valid JSON string
            else:
                embedding_js = None           # no embedding — DB will keep existing

            quality = float(faq.get('quality_score', 0.0))

            cursor.execute(
                """INSERT INTO faqs
                       (client_id, faq_id, question, answer, category,
                        triggers, tags, quality_score, embedding, is_active)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (faq_id) DO UPDATE SET
                       question      = EXCLUDED.question,
                       answer        = EXCLUDED.answer,
                       category      = EXCLUDED.category,
                       triggers      = EXCLUDED.triggers,
                       tags          = EXCLUDED.tags,
                       quality_score = EXCLUDED.quality_score,
                       embedding     = COALESCE(EXCLUDED.embedding,   faqs.embedding),
                       last_indexed  = COALESCE(faqs.last_indexed, EXCLUDED.last_indexed),
                       is_active     = TRUE""",
                (
                    client_id, faq_id,
                    faq.get('question', '').strip(),
                    faq.get('answer', '').strip(),
                    faq.get('category', 'General'),
                    json.dumps(triggers),
                    json.dumps(tags),
                    quality,
                    embedding_js,
                    True,
                )
            )
            saved += 1

        conn.commit()
        return saved
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def get_faqs(client_id: str, active_only: bool = True) -> list:
    """
    Return all FAQs for a client, including quality_score and tags
    so the AI helper can use them directly without extra queries.
    """
    conn, cursor = get_db()
    try:
        where = "client_id = %s AND is_active = TRUE" if active_only else "client_id = %s"
        cursor.execute(
            f"SELECT * FROM faqs WHERE {where} ORDER BY quality_score DESC, created_at DESC",
            (client_id,)
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

    result = []
    for faq in rows:
        # Parse triggers
        triggers_raw = faq.get('triggers', '[]') or '[]'
        if isinstance(triggers_raw, list):
            triggers = triggers_raw
        else:
            try:
                triggers = json.loads(triggers_raw)
            except Exception:
                triggers = [t.strip() for t in triggers_raw.split(',') if t.strip()]

        # Parse tags
        tags_raw = faq.get('tags', '[]') or '[]'
        try:
            tags = json.loads(tags_raw) if isinstance(tags_raw, str) else tags_raw
        except Exception:
            tags = []

        # Parse embedding (stored as JSON string)
        embedding_raw = faq.get('embedding')
        if embedding_raw and isinstance(embedding_raw, str):
            try:
                embedding_parsed = json.loads(embedding_raw)
            except Exception:
                embedding_parsed = []
        elif isinstance(embedding_raw, list):
            embedding_parsed = embedding_raw
        else:
            embedding_parsed = []

        result.append({
            'id':            faq.get('faq_id') or str(faq.get('id', '')),
            'faq_id':        faq.get('faq_id') or str(faq.get('id', '')),
            'question':      faq.get('question', ''),
            'answer':        faq.get('answer', ''),
            'category':      faq.get('category', 'General'),
            'triggers':      triggers,
            'tags':          tags,
            'quality_score': float(faq.get('quality_score') or 0.0),
            'embedding':     embedding_parsed,   # inline for AI helper — avoids second query
            'last_indexed':  faq.get('last_indexed'),
        })
    return result


def delete_all_faqs(client_id):
    """Delete all FAQs for a client"""
    conn, cursor = get_db()
    cursor.execute('DELETE FROM faqs WHERE client_id = %s', (client_id,))
    conn.commit()
    cursor.close()
    conn.close()


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


# =====================================================================
# HELP CENTER ARTICLES
# =====================================================================

def get_articles(client_id):
    """Get all articles for a client ordered by position."""
    conn, cursor = get_db()
    cursor.execute(
        'SELECT * FROM articles WHERE client_id = %s ORDER BY position ASC, created_at ASC',
        (client_id,)
    )
    rows = [dict(r) for r in cursor.fetchall()]
    cursor.close()
    conn.close()
    for r in rows:
        for col in ('created_at', 'updated_at'):
            if r.get(col):
                r[col] = r[col].isoformat()
    return rows


def get_article_by_id(article_id, client_id):
    """Get a single article."""
    conn, cursor = get_db()
    cursor.execute(
        'SELECT * FROM articles WHERE id = %s AND client_id = %s',
        (article_id, client_id)
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return dict(row) if row else None


def create_article(client_id, title, content, category='General'):
    """Create a new help article."""
    conn, cursor = get_db()
    cursor.execute(
        '''INSERT INTO articles (client_id, title, content, category)
           VALUES (%s, %s, %s, %s) RETURNING id''',
        (client_id, title, content, category)
    )
    row = cursor.fetchone()
    conn.commit()
    cursor.close()
    conn.close()
    return row['id'] if row else None


def update_article(article_id, client_id, title, content, category='General'):
    """Update an existing article."""
    conn, cursor = get_db()
    cursor.execute(
        '''UPDATE articles
           SET title=%s, content=%s, category=%s, updated_at=NOW()
           WHERE id=%s AND client_id=%s''',
        (title, content, category, article_id, client_id)
    )
    conn.commit()
    cursor.close()
    conn.close()


def delete_article(article_id, client_id):
    """Delete an article."""
    conn, cursor = get_db()
    cursor.execute(
        'DELETE FROM articles WHERE id=%s AND client_id=%s',
        (article_id, client_id)
    )
    conn.commit()
    cursor.close()
    conn.close()


# =====================================================================
# CLIENT PORTAL USERS
# =====================================================================

def create_client_user(client_id, email, password, name, invited_by):
    """Create a client-facing login. Returns id or None if email exists."""
    import hashlib, os as _os
    salt = _os.urandom(32)
    pw_hash = hashlib.pbkdf2_hmac('sha256', password.encode(), salt, 100000)
    stored = salt.hex() + ':' + pw_hash.hex()
    try:
        conn, cursor = get_db()
        cursor.execute(
            '''INSERT INTO client_users (client_id, email, password_hash, name, invited_by)
               VALUES (%s, %s, %s, %s, %s) RETURNING id''',
            (client_id, email.lower().strip(), stored, name, invited_by)
        )
        row = cursor.fetchone()
        conn.commit()
        cursor.close()
        conn.close()
        return row['id'] if row else None
    except Exception:
        return None


def verify_client_user(email, password):
    """Verify client user credentials. Returns user dict or None."""
    import hashlib
    conn, cursor = get_db()
    cursor.execute('SELECT * FROM client_users WHERE email = %s', (email.lower().strip(),))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if not row:
        return None
    stored = row['password_hash']
    try:
        salt_hex, hash_hex = stored.split(':')
        salt = bytes.fromhex(salt_hex)
        pw_hash = hashlib.pbkdf2_hmac('sha256', password.encode(), salt, 100000)
        if pw_hash.hex() == hash_hex:
            conn, cursor = get_db()
            cursor.execute('UPDATE client_users SET last_login=NOW() WHERE id=%s', (row['id'],))
            conn.commit()
            cursor.close()
            conn.close()
            return dict(row)
    except Exception:
        pass
    return None


def get_client_users(client_id):
    """Get all users for a client."""
    conn, cursor = get_db()
    cursor.execute(
        '''SELECT id, client_id, email, name, role, created_at, last_login
           FROM client_users WHERE client_id = %s ORDER BY created_at DESC''',
        (client_id,)
    )
    rows = [dict(r) for r in cursor.fetchall()]
    cursor.close()
    conn.close()
    for r in rows:
        for col in ('created_at', 'last_login'):
            if r.get(col):
                r[col] = r[col].isoformat()
    return rows


def get_client_user_by_id(user_id):
    conn, cursor = get_db()
    cursor.execute('SELECT * FROM client_users WHERE id = %s', (user_id,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return dict(row) if row else None


def delete_client_user(client_user_id, client_id):
    conn, cursor = get_db()
    cursor.execute(
        'DELETE FROM client_users WHERE id = %s AND client_id = %s',
        (client_user_id, client_id)
    )
    conn.commit()
    cursor.close()
    conn.close()


def update_client_user_password(client_user_id, new_password):
    import hashlib, os as _os
    salt = _os.urandom(32)
    pw_hash = hashlib.pbkdf2_hmac('sha256', new_password.encode(), salt, 100000)
    stored = salt.hex() + ':' + pw_hash.hex()
    conn, cursor = get_db()
    cursor.execute('UPDATE client_users SET password_hash=%s WHERE id=%s', (stored, client_user_id))
    conn.commit()
    cursor.close()
    conn.close()


def migrate_conversation_features():
    """
    Add conversation_summaries, faq_embeddings, and knowledge_base tables.
    Safe to call every startup (IF NOT EXISTS).
    """
    conn, cursor = get_db()
    try:
        # Conversation summaries
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS conversation_summaries (
                id           SERIAL PRIMARY KEY,
                client_id    TEXT NOT NULL,
                summary      TEXT NOT NULL,
                message_count INTEGER DEFAULT 6,
                created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Legacy FAQ embeddings (kept for backwards compatibility)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS faq_embeddings (
                id         SERIAL PRIMARY KEY,
                client_id  TEXT NOT NULL,
                faq_id     TEXT NOT NULL,
                question   TEXT NOT NULL,
                embedding  TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(client_id, faq_id)
            )
        ''')

        # Knowledge base (replaces flat FAQ store for RAG)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS knowledge_base (
                id          SERIAL PRIMARY KEY,
                client_id   TEXT NOT NULL,
                chunk_id    TEXT NOT NULL UNIQUE,
                title       TEXT,
                content     TEXT NOT NULL,
                type        TEXT DEFAULT 'faq',
                category    TEXT DEFAULT 'General',
                tags        TEXT DEFAULT '[]',
                embedding   TEXT,
                metadata    TEXT DEFAULT '{}',
                quality_score REAL DEFAULT 0.0,
                version     INTEGER DEFAULT 1,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_kb_client ON knowledge_base(client_id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_kb_type ON knowledge_base(client_id, type)"
        )

        conn.commit()
        print("✅ Conversation feature + knowledge_base tables ready.")
    except Exception as e:
        conn.rollback()
        print(f"⚠️ migrate_conversation_features: {e}")
    finally:
        cursor.close()
        conn.close()


def get_conversation_message_count(client_id: str) -> int:
    """Count total conversation turns for a client (used to trigger summarisation)."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            "SELECT COUNT(*) AS cnt FROM conversations WHERE client_id = %s",
            (client_id,)
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        return int(row['cnt']) if row else 0
    except Exception:
        return 0


def get_clients_enriched_stats(client_ids: list) -> dict:
    """
    Fetch stats for multiple clients in bulk — one query per metric
    instead of N×4 individual queries. Used by /agency/clients.

    Returns dict keyed by client_id:
    {
        'faqs_count':    int,
        'leads_count':   int,
        'conversations': int,
        'daily_msgs':    int,
        'last_active':   datetime | None,
    }
    """
    if not client_ids:
        return {}

    # Build a default result so every client_id is always present
    result = {
        cid: {
            'faqs_count':    0,
            'leads_count':   0,
            'conversations': 0,
            'daily_msgs':    0,
            'last_active':   None,
        }
        for cid in client_ids
    }

    try:
        conn, cursor = get_db()
        today = datetime.utcnow().strftime('%Y-%m-%d')

        # ── FAQs per client ───────────────────────────────────────────
        cursor.execute(
            """SELECT client_id, COUNT(*) AS cnt
               FROM faqs
               WHERE client_id = ANY(%s) AND is_active = TRUE
               GROUP BY client_id""",
            (client_ids,)
        )
        for row in cursor.fetchall():
            result[row['client_id']]['faqs_count'] = int(row['cnt'])

        # ── Leads per client ──────────────────────────────────────────
        cursor.execute(
            """SELECT client_id, COUNT(*) AS cnt
               FROM leads
               WHERE client_id = ANY(%s)
               GROUP BY client_id""",
            (client_ids,)
        )
        for row in cursor.fetchall():
            result[row['client_id']]['leads_count'] = int(row['cnt'])

        # ── Total conversations + last active ─────────────────────────
        cursor.execute(
            """SELECT client_id,
                      COUNT(*) AS cnt,
                      MAX(timestamp) AS last_ts
               FROM conversations
               WHERE client_id = ANY(%s)
               GROUP BY client_id""",
            (client_ids,)
        )
        for row in cursor.fetchall():
            result[row['client_id']]['conversations'] = int(row['cnt'])
            result[row['client_id']]['last_active']   = row['last_ts']

        # ── Daily messages (today only) ───────────────────────────────
        cursor.execute(
            """SELECT client_id, COUNT(*) AS cnt
               FROM conversations
               WHERE client_id = ANY(%s)
                 AND DATE(timestamp) = %s
               GROUP BY client_id""",
            (client_ids, today)
        )
        for row in cursor.fetchall():
            result[row['client_id']]['daily_msgs'] = int(row['cnt'])

        cursor.close()
        conn.close()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[get_clients_enriched_stats] {e}")

    return result


def save_conversation_summary(client_id: str, summary: str, message_count: int) -> None:
    """Persist a Gemini-generated conversation summary."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            '''INSERT INTO conversation_summaries (client_id, summary, message_count)
               VALUES (%s, %s, %s)''',
            (client_id, summary, message_count)
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        pass  # Non-critical — never break chat over a summary failure


def get_latest_conversation_summary(client_id: str) -> str:
    """Return the most recent summary string, or empty string if none."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            '''SELECT summary FROM conversation_summaries
               WHERE client_id = %s
               ORDER BY created_at DESC LIMIT 1''',
            (client_id,)
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        return row['summary'] if row else ''
    except Exception:
        return ''


def store_faq_embedding(client_id: str, faq_id: str, question: str, embedding: list) -> None:
    """
    Store an embedding in two places:
      1. faq_embeddings table  — for fast bulk retrieval by the AI helper
      2. faqs.embedding column — so get_faqs() can return embeddings inline
    Both are JSON-encoded float lists.
    """
    if not embedding:
        return
    emb_json = json.dumps(embedding)
    try:
        conn, cursor = get_db()
        # Primary store: faq_embeddings table
        cursor.execute(
            """INSERT INTO faq_embeddings (client_id, faq_id, question, embedding)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (client_id, faq_id)
               DO UPDATE SET question    = EXCLUDED.question,
                             embedding   = EXCLUDED.embedding,
                             created_at  = CURRENT_TIMESTAMP""",
            (client_id, faq_id, question, emb_json)
        )
        # Mirror on faqs table so single-query lookups work
        cursor.execute(
            """UPDATE faqs
               SET embedding    = %s,
                   last_indexed = CURRENT_TIMESTAMP
               WHERE client_id = %s AND faq_id = %s""",
            (emb_json, client_id, faq_id)
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        import logging
        logging.getLogger(__name__).debug(f"[store_faq_embedding] {e}")


def get_faq_embeddings(client_id: str) -> list:
    """Return all stored embeddings for a client as list of dicts."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            "SELECT faq_id, question, embedding FROM faq_embeddings WHERE client_id = %s",
            (client_id,)
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return [
            {
                'faq_id':    r['faq_id'],
                'question':  r['question'],
                'embedding': json.loads(r['embedding'])
            }
            for r in rows
        ]
    except Exception:
        return []


def save_knowledge_chunks(client_id: str, chunks: list) -> int:
    """
    Upsert a list of knowledge chunks for a client.
    Each chunk: {chunk_id, title, content, type, category, tags, embedding, metadata, quality_score}
    Returns count of saved chunks.
    """
    if not chunks:
        return 0
    conn, cursor = get_db()
    saved = 0
    try:
        for chunk in chunks:
            cursor.execute(
                '''INSERT INTO knowledge_base
                   (client_id, chunk_id, title, content, type, category, tags, embedding, metadata, quality_score)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (chunk_id)
                   DO UPDATE SET
                     title         = EXCLUDED.title,
                     content       = EXCLUDED.content,
                     type          = EXCLUDED.type,
                     category      = EXCLUDED.category,
                     tags          = EXCLUDED.tags,
                     embedding     = EXCLUDED.embedding,
                     metadata      = EXCLUDED.metadata,
                     quality_score = EXCLUDED.quality_score,
                     version       = knowledge_base.version + 1,
                     updated_at    = CURRENT_TIMESTAMP''',
                (
                    client_id,
                    chunk['chunk_id'],
                    chunk.get('title', ''),
                    chunk['content'],
                    chunk.get('type', 'faq'),
                    chunk.get('category', 'General'),
                    json.dumps(chunk.get('tags', [])),
                    json.dumps(chunk['embedding']) if chunk.get('embedding') else None,
                    json.dumps(chunk.get('metadata', {})),
                    float(chunk.get('quality_score', 0.0))
                )
            )
            saved += 1
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"save_knowledge_chunks error: {e}")
    finally:
        cursor.close()
        conn.close()
    return saved


def get_knowledge_chunks(client_id: str, chunk_type: str = None, limit: int = 500) -> list:
    """Return knowledge chunks for a client, optionally filtered by type."""
    conn, cursor = get_db()
    try:
        if chunk_type:
            cursor.execute(
                '''SELECT * FROM knowledge_base
                   WHERE client_id = %s AND type = %s
                   ORDER BY quality_score DESC, created_at DESC
                   LIMIT %s''',
                (client_id, chunk_type, limit)
            )
        else:
            cursor.execute(
                '''SELECT * FROM knowledge_base
                   WHERE client_id = %s
                   ORDER BY quality_score DESC, created_at DESC
                   LIMIT %s''',
                (client_id, limit)
            )
        rows = [dict(r) for r in cursor.fetchall()]
        for r in rows:
            for field in ('tags', 'metadata'):
                if r.get(field) and isinstance(r[field], str):
                    try:
                        r[field] = json.loads(r[field])
                    except Exception:
                        r[field] = []
            if r.get('embedding') and isinstance(r['embedding'], str):
                try:
                    r['embedding'] = json.loads(r['embedding'])
                except Exception:
                    r['embedding'] = None
        return rows
    except Exception as e:
        print(f"get_knowledge_chunks error: {e}")
        return []
    finally:
        cursor.close()
        conn.close()


def get_relevant_knowledge(client_id: str, query_embedding: list = None, limit: int = 5) -> list:
    """
    Return top-N knowledge chunks for a client.
    When query_embedding is provided, ranks by cosine similarity.
    Falls back to quality+recency order when no embedding is given.
    Always returns the embedding field so the AI layer can re-rank if needed.
    """
    chunks = get_knowledge_chunks(client_id)
    if not chunks:
        return []

    if query_embedding:
        scored = []
        for chunk in chunks:
            emb = chunk.get('embedding')
            if emb and query_embedding:
                try:
                    dot   = sum(a * b for a, b in zip(query_embedding, emb))
                    mag_q = sum(a * a for a in query_embedding) ** 0.5
                    mag_e = sum(b * b for b in emb) ** 0.5
                    sim   = dot / (mag_q * mag_e) if mag_q and mag_e else 0.0
                except Exception:
                    sim = 0.0
            else:
                sim = 0.0
            scored.append((chunk, sim))
        scored.sort(key=lambda x: x[1], reverse=True)
        return [c for c, _ in scored[:limit]]

    # No embedding provided — return top chunks by quality/recency
    return chunks[:limit]


def store_embedding(client_id: str, chunk_id: str, embedding: list) -> None:
    """Update the embedding for a single knowledge chunk."""
    conn, cursor = get_db()
    try:
        cursor.execute(
            '''UPDATE knowledge_base SET embedding = %s, updated_at = CURRENT_TIMESTAMP
               WHERE client_id = %s AND chunk_id = %s''',
            (json.dumps(embedding), client_id, chunk_id)
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def get_embeddings_for_client(client_id: str) -> list:
    """Return all chunk_id + embedding pairs for a client (for batch re-indexing)."""
    conn, cursor = get_db()
    try:
        cursor.execute(
            "SELECT chunk_id, embedding FROM knowledge_base WHERE client_id = %s AND embedding IS NOT NULL",
            (client_id,)
        )
        rows = cursor.fetchall()
        return [{'chunk_id': r['chunk_id'], 'embedding': json.loads(r['embedding'])} for r in rows]
    except Exception:
        return []
    finally:
        cursor.close()
        conn.close()


def delete_knowledge_chunks(client_id: str) -> None:
    """Delete all knowledge chunks for a client."""
    conn, cursor = get_db()
    try:
        cursor.execute("DELETE FROM knowledge_base WHERE client_id = %s", (client_id,))
        conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def get_recent_conversations(client_id: str, limit: int = 15) -> list:
    """
    Return the last `limit` conversation turns for a client,
    oldest → newest, as a list of {role, content} dicts
    ready to pass directly to generate_human_like_response.
    """
    try:
        conn, cursor = get_db()
        cursor.execute(
            '''SELECT user_message, bot_response
               FROM conversations
               WHERE client_id = %s
               ORDER BY timestamp DESC
               LIMIT %s''',
            (client_id, limit)
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        result = []
        for row in reversed(rows):   # oldest first
            result.append({'role': 'user',      'content': row['user_message']})
            result.append({'role': 'assistant', 'content': row['bot_response']})
        return result
    except Exception:
        return []


# =====================================================================
# KNOWLEDGE BASE — Phase 2 RAG
# =====================================================================

def migrate_knowledge_base():
    """
    Create the knowledge_base table if it doesn't exist.
    Safe to call every startup.
    """
    conn, cursor = get_db()
    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS knowledge_base (
                id          SERIAL PRIMARY KEY,
                client_id   TEXT NOT NULL,
                kb_id       TEXT NOT NULL UNIQUE,
                title       TEXT NOT NULL,
                content     TEXT NOT NULL,
                type        TEXT DEFAULT 'faq',        -- faq | article | policy
                category    TEXT DEFAULT 'General',
                tags        TEXT DEFAULT '[]',          -- JSON array
                embedding   TEXT,                       -- JSON float list
                metadata    TEXT DEFAULT '{}',          -- JSON
                quality     REAL DEFAULT 0.8,
                version     INTEGER DEFAULT 1,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_kb_client ON knowledge_base (client_id)"
        )
        conn.commit()
        print("✅ knowledge_base table ready.")
    except Exception as e:
        conn.rollback()
        print(f"⚠️  migrate_knowledge_base: {e}")
    finally:
        cursor.close()
        conn.close()


def save_knowledge_chunks(client_id: str, chunks: list) -> int:
    """
    Upsert a list of knowledge chunks for a client.
    Each chunk must have: kb_id, title, content, type, category, tags,
                          embedding, metadata, quality.
    Returns count of successfully saved chunks.
    """
    if not chunks:
        return 0

    conn, cursor = get_db()
    saved = 0
    try:
        for chunk in chunks:
            cursor.execute(
                '''INSERT INTO knowledge_base
                   (client_id, kb_id, title, content, type, category, tags,
                    embedding, metadata, quality)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (kb_id)
                   DO UPDATE SET
                     title     = EXCLUDED.title,
                     content   = EXCLUDED.content,
                     type      = EXCLUDED.type,
                     category  = EXCLUDED.category,
                     tags      = EXCLUDED.tags,
                     embedding = EXCLUDED.embedding,
                     metadata  = EXCLUDED.metadata,
                     quality   = EXCLUDED.quality,
                     version   = knowledge_base.version + 1,
                     updated_at = CURRENT_TIMESTAMP''',
                (
                    client_id,
                    chunk['kb_id'],
                    chunk.get('title', ''),
                    chunk.get('content', ''),
                    chunk.get('type', 'faq'),
                    chunk.get('category', 'General'),
                    json.dumps(chunk.get('tags', [])),
                    json.dumps(chunk.get('embedding', [])) if chunk.get('embedding') else None,
                    json.dumps(chunk.get('metadata', {})),
                    float(chunk.get('quality', 0.8)),
                )
            )
            saved += 1
        conn.commit()
    except Exception as e:
        conn.rollback()
        import logging
        logging.getLogger(__name__).error(f"[save_knowledge_chunks] error: {e}")
    finally:
        cursor.close()
        conn.close()
    return saved


def get_knowledge_chunks_raw(client_id: str) -> list:
    """Return all chunks for a client (no embedding filter) — for admin/export."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            '''SELECT kb_id, title, content, type, category, tags, quality, version, created_at
               FROM knowledge_base WHERE client_id = %s ORDER BY created_at DESC''',
            (client_id,)
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        result = []
        for r in rows:
            row = dict(r)
            if row.get('created_at'):
                row['created_at'] = row['created_at'].isoformat()
            result.append(row)
        return result
    except Exception:
        return []


def delete_knowledge_base(client_id: str) -> None:
    """Delete all knowledge base chunks for a client."""
    try:
        conn, cursor = get_db()
        cursor.execute("DELETE FROM knowledge_base WHERE client_id = %s", (client_id,))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception:
        pass


def store_embedding(client_id: str, kb_id: str, embedding: list) -> None:
    """Update the embedding for an existing knowledge chunk."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            "UPDATE knowledge_base SET embedding = %s WHERE client_id = %s AND kb_id = %s",
            (json.dumps(embedding), client_id, kb_id)
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception:
        pass


def get_embeddings_for_client(client_id: str) -> list:
    """Return list of {kb_id, embedding} dicts for cosine ranking."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            "SELECT kb_id, embedding FROM knowledge_base WHERE client_id = %s AND embedding IS NOT NULL",
            (client_id,)
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return [{'kb_id': r['kb_id'], 'embedding': json.loads(r['embedding'])} for r in rows]
    except Exception:
        return []


# =====================================================================
# WEBHOOK MANAGEMENT — Agency-grade multi-webhook system
# =====================================================================

def migrate_webhooks():
    """
    Create webhook_configs and webhook_logs tables.
    Safe to call every startup — uses IF NOT EXISTS.
    """
    conn, cursor = get_db()
    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS webhook_configs (
                id             SERIAL PRIMARY KEY,
                client_id      TEXT NOT NULL,
                webhook_id     TEXT NOT NULL UNIQUE,
                name           TEXT NOT NULL,
                url            TEXT NOT NULL,
                events         TEXT NOT NULL DEFAULT '["lead_captured"]',
                enabled        BOOLEAN DEFAULT TRUE,
                signing_secret TEXT,
                created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_wh_client ON webhook_configs (client_id)"
        )
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS webhook_logs (
                id           SERIAL PRIMARY KEY,
                client_id    TEXT NOT NULL,
                webhook_id   TEXT NOT NULL,
                event_type   TEXT NOT NULL,
                url          TEXT NOT NULL,
                payload      TEXT,
                status_code  INTEGER,
                response     TEXT,
                success      BOOLEAN DEFAULT FALSE,
                duration_ms  INTEGER,
                fired_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_whl_client ON webhook_logs (client_id, fired_at DESC)"
        )
        conn.commit()
        print("✅ Webhook tables ready.")
    except Exception as e:
        conn.rollback()
        print(f"⚠️  migrate_webhooks: {e}")
    finally:
        cursor.close()
        conn.close()


def get_webhooks(client_id: str) -> list:
    """Return all webhook configs for a client."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            """SELECT webhook_id, name, url, events, enabled, signing_secret, created_at
               FROM webhook_configs WHERE client_id = %s ORDER BY created_at""",
            (client_id,)
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        result = []
        for r in rows:
            result.append({
                'webhook_id':     r['webhook_id'],
                'name':           r['name'],
                'url':            r['url'],
                'events':         json.loads(r['events'] or '[]'),
                'enabled':        bool(r['enabled']),
                'signing_secret': r['signing_secret'] or '',
                'created_at':     r['created_at'].isoformat() if r['created_at'] else None,
            })
        return result
    except Exception:
        return []


def save_webhooks(client_id: str, webhooks: list) -> int:
    """
    Replace all webhooks for a client. Preserves signing_secret when
    the caller doesn't send one (secret is managed separately).
    Returns count saved.
    """
    if not isinstance(webhooks, list):
        return 0
    conn, cursor = get_db()
    try:
        # Fetch existing secrets so we don't lose them on update
        cursor.execute(
            "SELECT webhook_id, signing_secret FROM webhook_configs WHERE client_id = %s",
            (client_id,)
        )
        existing_secrets = {r['webhook_id']: r['signing_secret'] for r in cursor.fetchall()}

        # Delete removed webhooks
        incoming_ids = [w.get('webhook_id') for w in webhooks if w.get('webhook_id')]
        cursor.execute(
            "DELETE FROM webhook_configs WHERE client_id = %s AND webhook_id <> ALL(%s)",
            (client_id, incoming_ids or ['__none__'])
        )

        saved = 0
        for wh in webhooks:
            wid    = wh.get('webhook_id') or str(uuid.uuid4())
            secret = existing_secrets.get(wid) or _generate_signing_secret()
            cursor.execute(
                """INSERT INTO webhook_configs
                       (client_id, webhook_id, name, url, events, enabled, signing_secret)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (webhook_id) DO UPDATE SET
                       name       = EXCLUDED.name,
                       url        = EXCLUDED.url,
                       events     = EXCLUDED.events,
                       enabled    = EXCLUDED.enabled,
                       updated_at = CURRENT_TIMESTAMP""",
                (
                    client_id, wid,
                    wh.get('name', 'Webhook')[:120],
                    wh.get('url', ''),
                    json.dumps(wh.get('events', ['lead_captured'])),
                    bool(wh.get('enabled', True)),
                    secret,
                )
            )
            saved += 1
        conn.commit()
        return saved
    except Exception as e:
        conn.rollback()
        import logging
        logging.getLogger(__name__).error(f"[save_webhooks] {e}")
        return 0
    finally:
        cursor.close()
        conn.close()


def get_signing_secret(client_id: str, webhook_id: str) -> str:
    """Return the signing secret for a specific webhook."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            "SELECT signing_secret FROM webhook_configs WHERE client_id = %s AND webhook_id = %s",
            (client_id, webhook_id)
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        return row['signing_secret'] if row else ''
    except Exception:
        return ''


def regenerate_signing_secret(client_id: str, webhook_id: str) -> str:
    """Generate and persist a new signing secret. Returns the new secret."""
    new_secret = _generate_signing_secret()
    try:
        conn, cursor = get_db()
        cursor.execute(
            """UPDATE webhook_configs SET signing_secret = %s, updated_at = CURRENT_TIMESTAMP
               WHERE client_id = %s AND webhook_id = %s""",
            (new_secret, client_id, webhook_id)
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception:
        pass
    return new_secret


def _generate_signing_secret() -> str:
    """32-byte hex signing secret (64 chars)."""
    return secrets.token_hex(32)


def log_webhook_delivery(client_id: str, webhook_id: str, event_type: str,
                         url: str, payload: dict, status_code: int,
                         response_text: str, success: bool, duration_ms: int) -> None:
    """Append one delivery record to webhook_logs."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            """INSERT INTO webhook_logs
                   (client_id, webhook_id, event_type, url, payload,
                    status_code, response, success, duration_ms)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                client_id, webhook_id, event_type, url,
                json.dumps(payload)[:4000],
                status_code,
                (response_text or '')[:1000],
                success,
                duration_ms,
            )
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception:
        pass


def get_webhook_logs(client_id: str, limit: int = 20) -> list:
    """Return latest webhook delivery logs for a client."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            """SELECT l.webhook_id, l.event_type, l.url, l.status_code,
                      l.response, l.success, l.duration_ms, l.fired_at,
                      c.name AS webhook_name
               FROM webhook_logs l
               LEFT JOIN webhook_configs c
                 ON l.webhook_id = c.webhook_id AND l.client_id = c.client_id
               WHERE l.client_id = %s
               ORDER BY l.fired_at DESC
               LIMIT %s""",
            (client_id, limit)
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        result = []
        for r in rows:
            result.append({
                'webhook_id':   r['webhook_id'],
                'webhook_name': r['webhook_name'] or 'Deleted webhook',
                'event_type':   r['event_type'],
                'url':          r['url'],
                'status_code':  r['status_code'],
                'response':     r['response'],
                'success':      bool(r['success']),
                'duration_ms':  r['duration_ms'],
                'fired_at':     r['fired_at'].isoformat() if r['fired_at'] else None,
            })
        return result
    except Exception:
        return []


if __name__ == '__main__':
    init_db()
