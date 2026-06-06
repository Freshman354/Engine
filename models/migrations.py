"""
models/migrations.py
--------------------
All schema migration functions and init_db().
Every migrate_* function is idempotent — safe to call on every startup.
Called from app.py startup block; not imported by other models modules.
"""
import json
import os
import uuid
from .db import get_db

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
            id           SERIAL      PRIMARY KEY,
            client_id    TEXT        NOT NULL,
            user_message TEXT        NOT NULL,
            bot_response TEXT        NOT NULL,
            matched      BOOLEAN     DEFAULT FALSE,
            method       TEXT,
            -- FIX IMPROVE-10: session_id added here (not just in migrate_agent_tables)
            -- so fresh deploys have the column from the very first INSERT, eliminating
            -- the race condition where a chat request fires before migrate_agent_tables
            -- runs. The ALTER TABLE in migrate_agent_tables is still present for
            -- existing deployments and is idempotent (ADD COLUMN IF NOT EXISTS).
            session_id   TEXT,
            timestamp    TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients (client_id)
        )
    ''')
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_conversations_client_session "
        "ON conversations (client_id, session_id) WHERE session_id IS NOT NULL"
    )
    
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

    # kb_gaps — unanswered question tracker (was incorrectly created per-request
    # inside record_kb_gap; moved here so the DDL runs exactly once at startup)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS kb_gaps (
            id          SERIAL PRIMARY KEY,
            client_id   TEXT NOT NULL,
            question    TEXT NOT NULL,
            method      TEXT,
            confidence  REAL DEFAULT 0.0,
            count       INTEGER DEFAULT 1,
            first_seen  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_seen   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    migrate_kb_gaps()       # adds UNIQUE(client_id, question) for upsert counting
    migrate_chat_sessions() # Phase 3 — persistent session memory
    migrate_poor_answers()  # Phase 6 — poor answer feedback loop
    migrate_clients_active() # BUG-02 fix — adds is_active, business_name, contact_email


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


def migrate_clients_active():
    """
    Add is_active, business_name, and contact_email columns to clients table.
    Several queries (weekly digest, agency overage, enriched stats) depend on
    these columns — missing them causes silent [] returns and broken features.
    Safe — uses IF NOT EXISTS.
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        for sql in [
            "ALTER TABLE clients ADD COLUMN IF NOT EXISTS is_active      BOOLEAN   DEFAULT TRUE",
            "ALTER TABLE clients ADD COLUMN IF NOT EXISTS business_name  TEXT",
            "ALTER TABLE clients ADD COLUMN IF NOT EXISTS contact_email  TEXT",
        ]:
            cursor.execute(sql)
        # Back-fill business_name from company_name for existing rows so
        # queries using c.business_name still return results.
        cursor.execute(
            "UPDATE clients SET business_name = company_name WHERE business_name IS NULL"
        )
        conn.commit()
        print("✅ migrate_clients_active complete")
    except Exception as e:
        if conn:
            try: conn.rollback()
            except Exception: pass
        print(f"⚠️  migrate_clients_active: {e}")
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


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
            # Unique partial index — NULLs allowed (multiple unset domains OK)
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_clients_custom_widget_domain ON clients (custom_widget_domain) WHERE custom_widget_domain IS NOT NULL",
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


# =====================================================================
# CRON INFRASTRUCTURE MIGRATIONS
# =====================================================================

def migrate_cron_tables():
    """
    Create cron_runs table for auditing all cron executions.
    Add last_digest_sent_at to clients for deduplication.
    Safe — uses IF NOT EXISTS / IF NOT EXISTS column guard.
    """
    stmts = [
        # Audit log for every cron execution
        """CREATE TABLE IF NOT EXISTS cron_runs (
            id          BIGSERIAL PRIMARY KEY,
            job_name    TEXT        NOT NULL,
            ran_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            success     BOOLEAN     NOT NULL DEFAULT TRUE,
            result      JSONB,
            duration_ms INT,
            triggered_by TEXT        DEFAULT 'http'
        )""",
        "CREATE INDEX IF NOT EXISTS idx_cron_runs_job_ran ON cron_runs (job_name, ran_at DESC)",
        # Deduplication guard for weekly digest
        "ALTER TABLE clients ADD COLUMN IF NOT EXISTS last_digest_sent_at TIMESTAMPTZ",
    ]
    try:
        conn, cursor = get_db()
        for stmt in stmts:
            cursor.execute(stmt)
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ migrate_cron_tables complete")
    except Exception as e:
        print(f"⚠️  migrate_cron_tables: {e}")


def log_cron_run(job_name: str, success: bool, result: dict,
                 duration_ms: int = 0, triggered_by: str = 'http') -> None:
    """Insert one row into cron_runs. Never raises — cron must not fail to log."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            """INSERT INTO cron_runs (job_name, success, result, duration_ms, triggered_by)
               VALUES (%s, %s, %s, %s, %s)""",
            (job_name, success, json.dumps(result), duration_ms, triggered_by)
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[log_cron_run] {e}")


def get_cron_last_run(job_name: str) -> dict | None:
    """Return the most recent cron_runs row for job_name, or None."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            """SELECT * FROM cron_runs
               WHERE job_name = %s
               ORDER BY ran_at DESC LIMIT 1""",
            (job_name,)
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        return dict(row) if row else None
    except Exception:
        return None


def get_cron_history(job_name: str, limit: int = 20) -> list:
    """Return the last N runs for a job — used by admin dashboard."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            """SELECT job_name, ran_at, success, result, duration_ms, triggered_by
               FROM cron_runs
               WHERE job_name = %s
               ORDER BY ran_at DESC LIMIT %s""",
            (job_name, limit)
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def prune_old_logs(webhook_days: int = 60) -> dict:
    """
    Delete old webhook_logs rows to keep the DB lean.
    Conversations are intentionally kept forever — they are used as
    LLM fine-tuning training data and must never be auto-pruned.
    Returns counts of deleted rows.
    Safe — uses explicit WHERE clause with age guard.
    """
    deleted = {'webhook_logs': 0}
    try:
        conn, cursor = get_db()
        cursor.execute(
            "DELETE FROM webhook_logs WHERE created_at < NOW() - (%s * INTERVAL '1 day')",
            (webhook_days,)
        )
        deleted['webhook_logs'] = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[prune_old_logs] {e}")
    return deleted


def get_clients_for_weekly_digest_due() -> list:
    """
    Same as get_clients_for_weekly_digest but only returns clients whose
    last_digest_sent_at is NULL or older than 6 days — prevents double-sending.
    """
    conn, cursor = get_db()
    try:
        cursor.execute("""
            SELECT
                c.client_id,
                c.business_name,
                c.contact_email,
                u.email     AS owner_email,
                u.plan_type,
                c.last_digest_sent_at
            FROM clients c
            JOIN users u ON u.id = c.user_id
            WHERE u.plan_type NOT IN ('free', 'enterprise')
              AND c.is_active = TRUE
              AND (
                c.last_digest_sent_at IS NULL
                OR c.last_digest_sent_at < NOW() - INTERVAL '6 days'
              )
        """)
        return [dict(r) for r in cursor.fetchall()]
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[get_clients_for_weekly_digest_due] {e}")
        return []
    finally:
        cursor.close()
        conn.close()


def mark_digest_sent(client_id: str) -> None:
    """Stamp last_digest_sent_at = NOW() after a successful digest send."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            "UPDATE clients SET last_digest_sent_at = NOW() WHERE client_id = %s",
            (client_id,)
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[mark_digest_sent] {e}")


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


def migrate_faqs_table():
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


def migrate_google_oauth():
    """Add google_id column to users table for Google OAuth sign-in."""
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS google_id TEXT UNIQUE"
        )
        conn.commit()
        print("✅ migrate_google_oauth: google_id column ready")
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f'[migrate_google_oauth] {e}')
        if conn:
            try: conn.rollback()
            except Exception: pass
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


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


def migrate_lead_custom_fields():
    """Add custom_fields column to leads table if not present."""
    conn, cursor = get_db()
    cursor.execute("ALTER TABLE leads ADD COLUMN IF NOT EXISTS custom_fields TEXT")
    conn.commit()
    cursor.close()
    conn.close()


def migrate_lead_pipeline():
    """
    Add pipeline management columns to the leads table.
    Idempotent — safe to call on every startup.
      stage        : pipeline stage (new/contacted/qualified/proposal/closed/lost)
      notes        : internal notes added by the business
      assigned_to  : team member name or email
      priority     : high / med / low
      activity_log : JSON array of {ts, user, action} entries
      updated_at   : last modification timestamp
    """
    conn, cursor = get_db()
    try:
        cols = [
            "ALTER TABLE leads ADD COLUMN IF NOT EXISTS stage        TEXT    DEFAULT 'new'",
            "ALTER TABLE leads ADD COLUMN IF NOT EXISTS notes        TEXT",
            "ALTER TABLE leads ADD COLUMN IF NOT EXISTS assigned_to  TEXT",
            "ALTER TABLE leads ADD COLUMN IF NOT EXISTS priority     TEXT    DEFAULT 'high'",
            "ALTER TABLE leads ADD COLUMN IF NOT EXISTS activity_log TEXT",   # JSON
            "ALTER TABLE leads ADD COLUMN IF NOT EXISTS updated_at   TIMESTAMP",
        ]
        for stmt in cols:
            cursor.execute(stmt)
        conn.commit()
        print("✅ migrate_lead_pipeline complete")
    except Exception as e:
        conn.rollback()
        print(f"⚠️  migrate_lead_pipeline: {e}")
    finally:
        cursor.close()
        conn.close()


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

def migrate_agency_seat_billing():
    """
    Create the agency_overage_seats table if it doesn't exist.
    Records which clients are overage seats and when they were added.
    Idempotent — safe to call on every startup.
    """
    conn, cursor = get_db()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agency_overage_seats (
                id          SERIAL PRIMARY KEY,
                user_id     INT  NOT NULL,
                client_id   TEXT NOT NULL,
                seat_num    INT  NOT NULL,       -- 1-indexed position (21, 22, ...)
                created_at  TIMESTAMP DEFAULT NOW(),
                UNIQUE (client_id)
            )
        """)
        conn.commit()
        print("✅ migrate_agency_seat_billing complete")
    except Exception as e:
        conn.rollback()
        print(f"⚠️  migrate_agency_seat_billing: {e}")
    finally:
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

        # Schema reconciliation: migrate_knowledge_base creates knowledge_base with 'kb_id',
        # but this migration creates it with 'chunk_id'. If this ran first the table has
        # chunk_id — add kb_id as a generated alias column so both code paths work.
        cursor.execute("SAVEPOINT sp_kb_alias")
        try:
            cursor.execute(
                "ALTER TABLE knowledge_base ADD COLUMN IF NOT EXISTS kb_id TEXT"
            )
            # Backfill kb_id from chunk_id for existing rows
            cursor.execute(
                "UPDATE knowledge_base SET kb_id = chunk_id WHERE kb_id IS NULL AND chunk_id IS NOT NULL"
            )
            cursor.execute("RELEASE SAVEPOINT sp_kb_alias")
        except Exception:
            cursor.execute("ROLLBACK TO SAVEPOINT sp_kb_alias")

        conn.commit()
        print("✅ Conversation feature + knowledge_base tables ready.")
    except Exception as e:
        conn.rollback()
        print(f"⚠️ migrate_conversation_features: {e}")
    finally:
        cursor.close()
        conn.close()


def migrate_poor_answers():
    """
    Create the poor_answers table if it doesn't exist.
    Safe to call every startup — fully idempotent.
    Called from init_db() alongside the other migrate_* functions.
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS poor_answers (
                id           SERIAL      PRIMARY KEY,
                client_id    TEXT        NOT NULL,
                question     TEXT        NOT NULL,
                bot_answer   TEXT        NOT NULL,
                confidence   REAL        DEFAULT 0.0,
                method       TEXT,
                session_id   TEXT,
                hit_count    INTEGER     DEFAULT 1,
                first_seen   TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
                last_seen    TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT poor_answers_client_question_uq
                    UNIQUE (client_id, question)
            )
        ''')
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_poor_answers_client "
            "ON poor_answers (client_id, hit_count DESC)"
        )
        conn.commit()
    except Exception as e:
        import logging
        logging.getLogger(__name__).debug(f"[migrate_poor_answers] {e}")
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def migrate_chat_sessions():
    """
    Create chat_sessions table if it doesn't exist. Fully idempotent.
    Keyed by (client_id, session_id). Accumulates name, email,
    purchase_stage, frustration_score, and turn_count across the
    full conversation. session_data is JSONB — new fields never need
    a schema migration, only updates to upsert_session().
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chat_sessions (
                id                SERIAL      PRIMARY KEY,
                client_id         TEXT        NOT NULL,
                session_id        TEXT        NOT NULL,
                name              TEXT,
                email             TEXT,
                phone             TEXT,
                purchase_stage    TEXT,
                frustration_score INTEGER     DEFAULT 0,
                turn_count        INTEGER     DEFAULT 0,
                session_data      JSONB       DEFAULT '{}',
                created_at        TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
                updated_at        TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT chat_sessions_client_session_uq
                    UNIQUE (client_id, session_id)
            )
        ''')
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_chat_sessions_client "
            "ON chat_sessions (client_id, updated_at DESC)"
        )
        conn.commit()
        print('✅ migrate_chat_sessions complete')
    except Exception as e:
        print(f'⚠️  migrate_chat_sessions: {e}')
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def migrate_kb_gaps():
    """
    Add a UNIQUE constraint on (client_id, question) to kb_gaps so that
    record_kb_gap() can use ON CONFLICT upsert to increment hit counts
    rather than silently dropping duplicates.
    Safe to call every startup — uses SAVEPOINT to skip if already exists.
    """
    try:
        conn, cursor = get_db()
        cursor.execute("SAVEPOINT sp_kb_gaps_unique")
        try:
            cursor.execute(
                "ALTER TABLE kb_gaps ADD CONSTRAINT kb_gaps_client_question_unique "
                "UNIQUE (client_id, question)"
            )
            cursor.execute("RELEASE SAVEPOINT sp_kb_gaps_unique")
        except Exception:
            cursor.execute("ROLLBACK TO SAVEPOINT sp_kb_gaps_unique")
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ migrate_kb_gaps complete")
    except Exception as e:
        print(f"⚠️  migrate_kb_gaps: {e}")


def migrate_kb_gap_status():
    """
    Add status and resolved_at columns to kb_gaps, plus a covering index.

    Adds:
      - status TEXT DEFAULT 'open'   — allows filtering resolved vs open gaps
      - resolved_at TIMESTAMP        — set when a gap is approved and published
      - INDEX ON (client_id, status) — fast filtered queries in get_kb_gaps()

    Safe to call every startup — each ALTER uses a SAVEPOINT so it is a
    no-op if the column already exists.
    """
    try:
        conn, cursor = get_db()

        for col_sql in [
            "ALTER TABLE kb_gaps ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'open'",
            "ALTER TABLE kb_gaps ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMP",
        ]:
            cursor.execute("SAVEPOINT sp_kb_gap_status")
            try:
                cursor.execute(col_sql)
                cursor.execute("RELEASE SAVEPOINT sp_kb_gap_status")
            except Exception:
                cursor.execute("ROLLBACK TO SAVEPOINT sp_kb_gap_status")

        # Index for fast status-filtered lookups per client
        cursor.execute("SAVEPOINT sp_kb_gap_status_idx")
        try:
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_kb_gaps_client_status "
                "ON kb_gaps (client_id, status)"
            )
            cursor.execute("RELEASE SAVEPOINT sp_kb_gap_status_idx")
        except Exception:
            cursor.execute("ROLLBACK TO SAVEPOINT sp_kb_gap_status_idx")

        conn.commit()
        cursor.close()
        conn.close()
        print("✅ migrate_kb_gap_status complete")
    except Exception as e:
        print(f"⚠️  migrate_kb_gap_status: {e}")


def mark_kb_gap_resolved(gap_id: int) -> None:
    """
    Mark a kb_gaps row as resolved and record the timestamp.

    Sets status='resolved' and resolved_at=NOW() for the given gap_id.
    Called by ai_helper.approve_and_publish_gap() after a new FAQ is inserted.
    Never raises — errors are logged and swallowed.
    """
    try:
        conn, cursor = get_db()
        cursor.execute(
            """UPDATE kb_gaps
               SET status = 'resolved', resolved_at = NOW()
               WHERE id = %s""",
            (gap_id,)
        )
        conn.commit()
        cursor.close()
        conn.close()
        import logging
        logging.getLogger(__name__).info(
            f"[mark_kb_gap_resolved] gap_id={gap_id} marked resolved"
        )
    except Exception as e:
        import logging
        logging.getLogger(__name__).debug(f"[mark_kb_gap_resolved] non-critical: {e}")


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


def migrate_api_usage_log():
    """Create api_usage_log table. Safe — uses IF NOT EXISTS."""
    try:
        conn, cursor = get_db()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS api_usage_log (
                id            SERIAL PRIMARY KEY,
                user_id       INTEGER REFERENCES users(id) ON DELETE SET NULL,
                client_id     VARCHAR(100),
                model         VARCHAR(80)  DEFAULT 'gemini-1.5-flash',
                input_tokens  INTEGER      NOT NULL DEFAULT 0,
                output_tokens INTEGER      NOT NULL DEFAULT 0,
                endpoint      VARCHAR(100),
                created_at    TIMESTAMP    DEFAULT NOW()
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_api_usage_user    ON api_usage_log(user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_api_usage_created ON api_usage_log(created_at)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_api_usage_client  ON api_usage_log(client_id)")
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ migrate_api_usage_log complete")
    except Exception as e:
        print(f"⚠️  migrate_api_usage_log: {e}")


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


