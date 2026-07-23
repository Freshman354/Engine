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
    migrate_kb_gaps()           # adds UNIQUE(client_id, question) for upsert counting
    migrate_chat_sessions()     # Phase 3 — persistent session memory
    migrate_poor_answers()      # Phase 6 — poor answer feedback loop
    migrate_clients_active()    # BUG-02 fix — adds is_active, business_name, contact_email

    # ── Lead table columns ──────────────────────────────────────────────────
    # These were previously defined but never called from init_db(), causing
    # fresh-deploy crashes: save_lead() inserts `priority` and `intent_summary`
    # which migrate_lead_pipeline() and migrate_lead_intent_summary() create.
    migrate_lead_pipeline()             # stage, priority, notes, assigned_to, activity_log
    migrate_lead_intent_summary()       # intent_summary — set at capture by extract_lead_intent()
    migrate_lead_extra_fields()         # lost_reason, follow_up_at
    migrate_lead_duplicate_tracking()   # submission_count
    migrate_lead_outcome_tracking()     # closed_value, outcome_notes
    migrate_lead_nudge_tracking()       # stale_nudge_sent_at, followup_reminder_sent_at

    # ── Delivery infrastructure ─────────────────────────────────────────────
    # migrate_webhooks() was never called — _fire_lead_webhook() needs webhook_configs.
    # migrate_white_label() was never called — _send_lead_email() needs branded_email_from.
    migrate_webhooks()                  # webhook_configs + webhook_logs tables
    migrate_white_label()               # branded_email_from on clients, agency_branding_settings on users

    # ── Gap 3: lead delivery columns (new) ──────────────────────────────────
    migrate_lead_delivery()             # notification_email, notification_phone, notification_name
    migrate_primary_contact()           # client_users.is_primary_contact
    migrate_agency_email_domains()      # white-label custom email domain per agency
    migrate_seat_subscriptions()        # agency per-seat purchase subscriptions


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


def migrate_account_profile():
    """
    Profile fields (company name, logo, contact phone, notification prefs)
    and self-service account deletion tracking on users. Soft delete sets
    deletion_requested_at + scheduled_hard_delete_at; a daily cron job
    (blueprints/cron.py::cron_hard_delete_accounts) permanently removes
    accounts past their scheduled_hard_delete_at. Safe to call every
    startup (ADD COLUMN IF NOT EXISTS).
    """
    conn, cursor = get_db()
    try:
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS company_name TEXT")
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS logo_url TEXT")
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS contact_phone TEXT")
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS notification_prefs TEXT DEFAULT '{}'")
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS deletion_requested_at TIMESTAMP")
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS scheduled_hard_delete_at TIMESTAMP")
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS deletion_reason TEXT")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_users_hard_delete_due "
            "ON users (scheduled_hard_delete_at) WHERE scheduled_hard_delete_at IS NOT NULL"
        )
        conn.commit()
        print("✅ Account profile + deletion tracking ready (users table).")
    except Exception as e:
        conn.rollback()
        print(f"⚠️  migrate_account_profile: {e}")
    finally:
        cursor.close()
        conn.close()


def migrate_external_integrations():
    """
    Create client_ext_integrations, client_ext_integration_actions, and
    integration_action_log tables — agency-configured external system
    connections (e.g. a client's own Calendly/Shopify/REST API) for
    fully agentic tool calls. Safe to call every startup (IF NOT EXISTS).
    """
    conn, cursor = get_db()
    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS client_ext_integrations (
                id                         SERIAL PRIMARY KEY,
                integration_id             TEXT NOT NULL UNIQUE,
                client_id                  TEXT NOT NULL,
                name                       TEXT NOT NULL,
                base_url                   TEXT NOT NULL,
                auth_type                  TEXT NOT NULL,
                encrypted_credentials      TEXT NOT NULL,
                active                     BOOLEAN DEFAULT TRUE,
                created_by_agency_user_id  INTEGER,
                created_at                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (client_id) REFERENCES clients (client_id)
            )
        ''')
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_ci_client ON client_ext_integrations (client_id)"
        )

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS client_ext_integration_actions (
                id                     SERIAL PRIMARY KEY,
                integration_id         TEXT NOT NULL,
                action_name            TEXT NOT NULL,
                description            TEXT,
                http_method            TEXT NOT NULL,
                endpoint_path          TEXT NOT NULL,
                param_mapping          TEXT NOT NULL DEFAULT '{}',
                response_mapping       TEXT NOT NULL DEFAULT '{}',
                requires_confirmation  BOOLEAN DEFAULT TRUE,
                amount_param           TEXT,
                max_auto_amount        NUMERIC(12,2),
                active                 BOOLEAN DEFAULT TRUE,
                created_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (integration_id) REFERENCES client_ext_integrations (integration_id) ON DELETE CASCADE
            )
        ''')
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_cia_integration ON client_ext_integration_actions (integration_id)"
        )
        # Idempotent for deployments that already ran phase-1's CREATE TABLE
        # before these two columns existed.
        cursor.execute("ALTER TABLE client_ext_integration_actions ADD COLUMN IF NOT EXISTS amount_param TEXT")
        cursor.execute("ALTER TABLE client_ext_integration_actions ADD COLUMN IF NOT EXISTS max_auto_amount NUMERIC(12,2)")

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS integration_action_log (
                id              SERIAL PRIMARY KEY,
                client_id       TEXT NOT NULL,
                session_id      TEXT,
                integration_id  TEXT,
                action_name     TEXT NOT NULL,
                params          TEXT,
                result          TEXT,
                success         BOOLEAN DEFAULT FALSE,
                summary         TEXT,
                fired_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_ial_client ON integration_action_log (client_id, fired_at DESC)"
        )

        conn.commit()
        print("✅ Integration tables ready (client_ext_integrations, client_ext_integration_actions, integration_action_log).")
    except Exception as e:
        conn.rollback()
        print(f"⚠️  migrate_external_integrations: {e}")
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
                model         VARCHAR(80)  DEFAULT 'gemini-2.0-flash',
                input_tokens  INTEGER      NOT NULL DEFAULT 0,
                output_tokens INTEGER      NOT NULL DEFAULT 0,
                cost          NUMERIC(10,6),
                endpoint      VARCHAR(100),
                created_at    TIMESTAMP    DEFAULT NOW()
            )
        """)
        cursor.execute("ALTER TABLE api_usage_log ADD COLUMN IF NOT EXISTS cost NUMERIC(10,6)")
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





def migrate_agency_email_domains():
    """
    Custom email domain table for agency white-label email.
    One row per agency (user_id UNIQUE). Stores the domain, desired
    from-email, Brevo-generated DNS records, and verification status.

    Status values:
      pending  — domain registered with Brevo, DNS records not yet confirmed
      verified — DNS check + Brevo authentication passed
      failed   — last DNS check failed (records not found / wrong values)

    Idempotent — CREATE TABLE IF NOT EXISTS + ADD COLUMN IF NOT EXISTS.
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS agency_email_domains (
                id          SERIAL PRIMARY KEY,
                user_id     INTEGER NOT NULL UNIQUE
                            REFERENCES users(id) ON DELETE CASCADE,
                domain      VARCHAR(255) NOT NULL,
                from_name   VARCHAR(255),
                from_email  VARCHAR(255),
                status      VARCHAR(50)  NOT NULL DEFAULT 'pending',
                spf_host    VARCHAR(255),
                spf_value   TEXT,
                dkim_host   VARCHAR(255),
                dkim_value  TEXT,
                last_check_at  TIMESTAMP,
                verified_at    TIMESTAMP,
                created_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_agency_email_domains_status
            ON agency_email_domains(status)
        ''')

        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_agency_email_domains_domain
            ON agency_email_domains(domain)
        ''')

        conn.commit()
        print("✅ migrate_agency_email_domains complete")
    except Exception as e:
        if conn:
            try: conn.rollback()
            except Exception: pass
        print(f"⚠️  migrate_agency_email_domains: {e}")
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


def migrate_system_settings():
    """
    Generic key-value store for admin-toggleable settings that need to
    take effect live, without a redeploy — e.g. the AI provider switch
    (models/system_settings.py, utils.py::get_ai_provider). Safe to call
    every startup (IF NOT EXISTS).
    """
    conn, cursor = get_db()
    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS system_settings (
                key         TEXT PRIMARY KEY,
                value       TEXT NOT NULL,
                updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_by  INTEGER
            )
        ''')
        conn.commit()
        print("✅ system_settings table ready.")
    except Exception as e:
        conn.rollback()
        print(f"⚠️  migrate_system_settings: {e}")
    finally:
        cursor.close()
        conn.close()


def migrate_lead_delivery():
    """
    Add lead delivery columns to the clients table.
    These are configured by the agency (via manage_client_users.html) and
    control where new leads are sent the moment they are captured.

      notification_email : end client's email address for lead alert emails
      notification_phone : end client's mobile number for SMS alerts (Twilio)
      notification_name  : display name used as the email From: header
                           (white-label — the end client sees the agency's
                           name, never 'Lumvi')

    webhook_url is intentionally NOT added here — webhook delivery is handled
    by the existing webhook_configs / webhook_logs tables created by
    migrate_webhooks(), which supports per-event filtering, signing secrets,
    and delivery logs. The manage_client_users UI writes to webhook_configs.

    Idempotent — ADD COLUMN IF NOT EXISTS is safe on every startup.
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        for sql in [
            "ALTER TABLE clients ADD COLUMN IF NOT EXISTS notification_email TEXT",
            "ALTER TABLE clients ADD COLUMN IF NOT EXISTS notification_phone TEXT",
            "ALTER TABLE clients ADD COLUMN IF NOT EXISTS notification_name  TEXT",
        ]:
            cursor.execute(sql)
        conn.commit()
        print("✅ migrate_lead_delivery complete")
    except Exception as e:
        if conn:
            try: conn.rollback()
            except Exception: pass
        print(f"⚠️  migrate_lead_delivery: {e}")
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


def migrate_primary_contact():
    """
    Add is_primary_contact to client_users.

    Marks which (if any) client_user is the business's designated primary
    contact. Once set, clients.notification_email should be sourced from
    that client_user's own email rather than agency-entered text — see
    client_users.get_primary_contact()/set_primary_contact() and the write
    guard in blueprints/client_settings.py.

    At most one primary contact per client_id — enforced in
    set_primary_contact(), not at the DB level (a partial unique index would
    be the stricter option; not added here since set_primary_contact()
    already unsets any prior primary transactionally before setting a new
    one, and adding a DB constraint on top would need a migration-time check
    for any pre-existing bad data first).

    Idempotent — ADD COLUMN IF NOT EXISTS is safe on every startup.
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute(
            "ALTER TABLE client_users ADD COLUMN IF NOT EXISTS "
            "is_primary_contact BOOLEAN DEFAULT FALSE"
        )
        conn.commit()
        print("✅ migrate_primary_contact complete")
    except Exception as e:
        if conn:
            try: conn.rollback()
            except Exception: pass
        print(f"⚠️  migrate_primary_contact: {e}")
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass

# ═══════════════════════════════════════════════════════════════════════════════
# TIER 1 FEATURE MIGRATIONS
# ═══════════════════════════════════════════════════════════════════════════════

def migrate_page_context():
    """
    Add page-context columns to conversations table.
    Captures page_url, referrer, and UTM params from every widget session.
    Idempotent — safe on every startup.
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        for col_sql in [
            "ALTER TABLE conversations ADD COLUMN IF NOT EXISTS page_url      TEXT",
            "ALTER TABLE conversations ADD COLUMN IF NOT EXISTS referrer      TEXT",
            "ALTER TABLE conversations ADD COLUMN IF NOT EXISTS utm_source    TEXT",
            "ALTER TABLE conversations ADD COLUMN IF NOT EXISTS utm_medium    TEXT",
            "ALTER TABLE conversations ADD COLUMN IF NOT EXISTS utm_campaign  TEXT",
        ]:
            cursor.execute(col_sql)
        conn.commit()
        print("✅ migrate_page_context complete")
    except Exception as e:
        print(f"⚠️  migrate_page_context: {e}")
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def migrate_csat():
    """
    Add CSAT rating columns to chat_sessions.
    csat_rating: 1 = positive, -1 = negative, NULL = not yet rated.
    Idempotent — safe on every startup.
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute(
            "ALTER TABLE chat_sessions ADD COLUMN IF NOT EXISTS "
            "csat_rating SMALLINT CHECK (csat_rating IN (-1, 1))"
        )
        cursor.execute(
            "ALTER TABLE chat_sessions ADD COLUMN IF NOT EXISTS "
            "csat_submitted_at TIMESTAMP"
        )
        conn.commit()
        print("✅ migrate_csat complete")
    except Exception as e:
        print(f"⚠️  migrate_csat: {e}")
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def migrate_conversation_status():
    """
    Add status + per-status timestamp columns to chat_sessions.
    Status values: open | in_progress | pending_customer | resolved
    Also creates index for inbox queries ordered by status + recency.
    Idempotent — safe on every startup.
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        for stmt in [
            "ALTER TABLE chat_sessions ADD COLUMN IF NOT EXISTS "
            "status VARCHAR(20) NOT NULL DEFAULT 'open'",

            "ALTER TABLE chat_sessions ADD COLUMN IF NOT EXISTS "
            "opened_at TIMESTAMP DEFAULT NOW()",

            "ALTER TABLE chat_sessions ADD COLUMN IF NOT EXISTS "
            "in_progress_at TIMESTAMP",

            "ALTER TABLE chat_sessions ADD COLUMN IF NOT EXISTS "
            "pending_customer_at TIMESTAMP",

            "ALTER TABLE chat_sessions ADD COLUMN IF NOT EXISTS "
            "resolved_at TIMESTAMP",
        ]:
            cursor.execute(stmt)

        # Savepoint guard in case index already exists
        cursor.execute("SAVEPOINT sp_status_idx")
        try:
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_chat_sessions_status "
                "ON chat_sessions (client_id, status, updated_at DESC)"
            )
            cursor.execute("RELEASE SAVEPOINT sp_status_idx")
        except Exception:
            cursor.execute("ROLLBACK TO SAVEPOINT sp_status_idx")

        conn.commit()
        print("✅ migrate_conversation_status complete")
    except Exception as e:
        print(f"⚠️  migrate_conversation_status: {e}")
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def migrate_conversation_tags():
    """
    Create tags + session_tags tables for conversation tagging.
    tags:         per-client label library (name + hex colour)
    session_tags: junction — which sessions carry which tags
    Idempotent — safe on every startup.
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tags (
                id         SERIAL      PRIMARY KEY,
                client_id  TEXT        NOT NULL,
                name       VARCHAR(50) NOT NULL,
                color      VARCHAR(7)  NOT NULL DEFAULT '#6366f1',
                created_at TIMESTAMP   DEFAULT NOW(),
                UNIQUE (client_id, name)
            )
        """)
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_tags_client ON tags (client_id)"
        )
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS session_tags (
                session_id TEXT    NOT NULL,
                tag_id     INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
                client_id  TEXT    NOT NULL,
                applied_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (session_id, tag_id)
            )
        """)
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_stags_session "
            "ON session_tags (session_id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_stags_client "
            "ON session_tags (client_id, tag_id)"
        )
        conn.commit()
        print("✅ migrate_conversation_tags complete")
    except Exception as e:
        print(f"⚠️  migrate_conversation_tags: {e}")
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def migrate_proactive_triggers():
    """
    Create proactive_triggers table.
    trigger_type: 'time_on_page' | 'url_match'
    trigger_value: seconds (time) or URL substring (url_match)
    Idempotent — safe on every startup.
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS proactive_triggers (
                id            SERIAL      PRIMARY KEY,
                client_id     TEXT        NOT NULL,
                name          VARCHAR(100) NOT NULL,
                trigger_type  VARCHAR(20)  NOT NULL
                              CHECK (trigger_type IN ('time_on_page', 'url_match')),
                trigger_value TEXT        NOT NULL,
                message       TEXT        NOT NULL,
                is_active     BOOLEAN     DEFAULT TRUE,
                created_at    TIMESTAMP   DEFAULT NOW()
            )
        """)
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_triggers_client "
            "ON proactive_triggers (client_id) WHERE is_active = TRUE"
        )
        conn.commit()
        print("✅ migrate_proactive_triggers complete")
    except Exception as e:
        print(f"⚠️  migrate_proactive_triggers: {e}")
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def migrate_lead_extra_fields():
    """
    Add lost_reason and follow_up_at columns to the leads table.
      lost_reason  : TEXT      — captured when a lead is moved to the 'lost' stage
      follow_up_at : TIMESTAMP — optional date/time the agent wants a reminder
    Idempotent — safe on every startup (ADD COLUMN IF NOT EXISTS).
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute("ALTER TABLE leads ADD COLUMN IF NOT EXISTS lost_reason  TEXT")
        cursor.execute("ALTER TABLE leads ADD COLUMN IF NOT EXISTS follow_up_at TIMESTAMP")
        conn.commit()
        print("✅ migrate_lead_extra_fields complete")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"⚠️  migrate_lead_extra_fields: {e}")
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def migrate_lead_duplicate_tracking():
    """
    Add submission_count column to the leads table.
      submission_count : INTEGER — how many times this email has submitted
                          the lead form for this client. Defaults to 1 so
                          existing rows (each a single historical submission)
                          don't need a backfill.
    Idempotent — safe on every startup (ADD COLUMN IF NOT EXISTS).
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute(
            "ALTER TABLE leads ADD COLUMN IF NOT EXISTS submission_count INTEGER DEFAULT 1"
        )
        conn.commit()
        print("✅ migrate_lead_duplicate_tracking complete")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"⚠️  migrate_lead_duplicate_tracking: {e}")
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def migrate_lead_outcome_tracking():
    """
    Add closed_value and outcome_notes columns to the leads table.
      closed_value  : NUMERIC — deal value captured when a lead is marked
                       'closed'; powers revenue-attribution reporting.
      outcome_notes : TEXT    — free-text context captured alongside it.
    Idempotent — safe on every startup (ADD COLUMN IF NOT EXISTS).
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute("ALTER TABLE leads ADD COLUMN IF NOT EXISTS closed_value  NUMERIC")
        cursor.execute("ALTER TABLE leads ADD COLUMN IF NOT EXISTS outcome_notes TEXT")
        conn.commit()
        print("✅ migrate_lead_outcome_tracking complete")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"⚠️  migrate_lead_outcome_tracking: {e}")
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def migrate_lead_nudge_tracking():
    """
    Add dedup-tracking columns used by the stale-lead-nudge and
    follow-up-reminder cron jobs, so each lead is only ever notified once
    per nudge/reminder.
      stale_nudge_sent_at        : TIMESTAMP
      followup_reminder_sent_at  : TIMESTAMP
    Idempotent — safe on every startup (ADD COLUMN IF NOT EXISTS).
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute("ALTER TABLE leads ADD COLUMN IF NOT EXISTS stale_nudge_sent_at       TIMESTAMP")
        cursor.execute("ALTER TABLE leads ADD COLUMN IF NOT EXISTS followup_reminder_sent_at TIMESTAMP")
        conn.commit()
        print("✅ migrate_lead_nudge_tracking complete")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"⚠️  migrate_lead_nudge_tracking: {e}")
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def migrate_lead_intent_summary():
    """
    Add intent_summary column to the leads table — a 2-3 sentence Gemini
    summary of what the lead wants, generated once at capture time
    (blueprints/leads.py submit_lead -> ai_helper.extract_lead_intent).
    Idempotent — safe on every startup (ADD COLUMN IF NOT EXISTS).
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute("ALTER TABLE leads ADD COLUMN IF NOT EXISTS intent_summary TEXT")
        conn.commit()
        print("✅ migrate_lead_intent_summary complete")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"⚠️  migrate_lead_intent_summary: {e}")
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def migrate_overage_tracking():
    """
    Add five overage-tracking columns to the users table so the billing
    cron and the agency client-creation route can read payment status
    without a join to the payments table.

    Columns:
      overage_amount_due     — amount currently owed (cleared on payment)
      overage_due_date       — invoice deadline (14 days from issue)
      overage_payment_status — 'none' | 'pending' | 'paid' | 'overdue'
      overage_tx_ref         — Flutterwave tx_ref for the outstanding invoice
      overage_payment_link   — hosted checkout URL sent in the invoice email

    Idempotent — ADD COLUMN IF NOT EXISTS is safe to run on every startup.
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS overage_amount_due "
            "NUMERIC(10,2) DEFAULT 0"
        )
        cursor.execute(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS overage_due_date TIMESTAMP"
        )
        cursor.execute(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS overage_payment_status "
            "TEXT DEFAULT 'none'"
        )
        cursor.execute(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS overage_tx_ref TEXT"
        )
        cursor.execute(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS overage_payment_link TEXT"
        )
        conn.commit()
        print("✅ migrate_overage_tracking complete")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"⚠️  migrate_overage_tracking: {e}")
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def migrate_ai_employee_plan_rename():
    """
    One-time DATA migration for the "AI Employee for Shopify & WooCommerce"
    pivot. Supported plans going forward: free / ai_starter $29 / ai_growth
    $79 / ai_scale $199.

    UPDATED (obsolete-plan removal): solo/starter[old $49]/pro/growth[old
    $149]/agency/enterprise are no longer grandfathered — by explicit
    product decision there are no production accounts left on any of them,
    so every one is moved onto the nearest current tier rather than kept
    around as a permanent legacy branch. 'free' is NOT touched by this
    migration — it's a first-class current-generation plan, not obsolete.

    Mapping (nearest-tier by feature/price proximity, store-count NOT
    preserved — the new product is single-store-per-account focused):
        solo, starter[old $49]  -> ai_starter
        pro, growth[old $149]   -> ai_growth
        agency, enterprise      -> ai_scale

    NOTE: this only updates the local plan_type column. It does NOT touch
    Flutterwave — any live FLW subscription still billing an old-tier price
    must be cancelled/recreated at the new price manually in the
    Flutterwave dashboard, or it will keep charging the old amount even
    though the account now shows new-tier limits. Confirmed with product:
    no production accounts are on any obsolete plan_type, so this should
    be a no-op in practice — the WHERE clauses are the safety net if that
    assumption is ever wrong.

    Idempotent — every WHERE clause matches zero rows once no account
    remains on an obsolete plan_type, so safe to leave in the startup
    migration list permanently.
    """
    conn = cursor = None
    plan_map = {
        'solo':       'ai_starter',
        'starter':    'ai_starter',
        'pro':        'ai_growth',
        'growth':     'ai_growth',
        'agency':     'ai_scale',
        'enterprise': 'ai_scale',
    }
    try:
        conn, cursor = get_db()
        for old_plan, new_plan in plan_map.items():
            cursor.execute(
                "UPDATE users SET plan_type = %s WHERE plan_type = %s",
                (new_plan, old_plan)
            )
            moved = cursor.rowcount
            if moved:
                print(f"migrate_ai_employee_plan_rename: moved {moved} '{old_plan}' user(s) to '{new_plan}'")
        conn.commit()
    except Exception as e:
        if conn:
            try: conn.rollback()
            except Exception: pass
        print(f"migrate_ai_employee_plan_rename error: {e}")
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


def migrate_cart_recovery():
    """
    Cart recovery automation (ai_growth/ai_scale 'cart_recovery' feature).

    abandoned_carts       — one row per Shopify checkout that hasn't
                            converted to an order yet. Populated by the
                            Shopify checkouts/create|update webhook handler
                            (see webhooks.py's topic dispatch — NOT this
                            module).
    clients.cart_recovery_enabled — per-client on/off toggle, settable via
                            /api/client/settings only when the account
                            owner's plan includes 'cart_recovery'
                            (blueprints/client_settings.py).

    reply_local_part is the unique part before @ in the per-cart reply
    address (e.g. 'cart-482' in cart-482@reply.lumvi.net) — generated from
    the row's own id once inserted, so it's populated in a second UPDATE
    right after the INSERT rather than at CREATE TABLE time.

    Idempotent — CREATE TABLE/COLUMN IF NOT EXISTS throughout.
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS abandoned_carts (
                id                      SERIAL          PRIMARY KEY,
                client_id               TEXT            NOT NULL,
                platform                VARCHAR(20)     NOT NULL DEFAULT 'shopify',
                checkout_token          TEXT            NOT NULL,
                customer_email          TEXT,
                customer_name           TEXT,
                cart_total              DECIMAL(10,2),
                currency                VARCHAR(10),
                line_items              JSONB,
                checkout_url            TEXT,
                reply_local_part        TEXT            UNIQUE,
                abandoned_at            TIMESTAMP       NOT NULL DEFAULT NOW(),
                recovery_email_sent_at  TIMESTAMP,
                recovered_at            TIMESTAMP,
                status                  VARCHAR(20)     NOT NULL DEFAULT 'pending',
                reply_forwarded_count   INTEGER         NOT NULL DEFAULT 0,
                created_at              TIMESTAMP       NOT NULL DEFAULT NOW(),
                UNIQUE(client_id, checkout_token)
            )
        ''')
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_abandoned_carts_due "
            "ON abandoned_carts (status, abandoned_at) WHERE status = 'pending'"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_abandoned_carts_client "
            "ON abandoned_carts (client_id)"
        )
        cursor.execute(
            "ALTER TABLE clients ADD COLUMN IF NOT EXISTS "
            "cart_recovery_enabled BOOLEAN NOT NULL DEFAULT FALSE"
        )
        conn.commit()
        print("migrate_cart_recovery complete")
    except Exception as e:
        if conn:
            try: conn.rollback()
            except Exception: pass
        print(f"migrate_cart_recovery error: {e}")
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


def migrate_seat_subscriptions():
    """
    Create seat_subscriptions table for agency per-seat purchases.

    Each row represents one paid seat subscription (single or bundle).
    The cron charges monthly_amount on next_billing_date every month.

    package_type : 'single' (1 seat, $15/mo) | 'bundle' (5 seats, $60/mo)
    status       : 'active' | 'past_due' | 'failed' | 'cancelled'
    tx_ref       : last Flutterwave tx_ref (updated on each renewal)

    Idempotent — CREATE TABLE IF NOT EXISTS + indexes use IF NOT EXISTS.
    """
    conn = cursor = None
    try:
        conn, cursor = get_db()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS seat_subscriptions (
                id                SERIAL          PRIMARY KEY,
                agency_id         INTEGER         NOT NULL
                                  REFERENCES users(id) ON DELETE CASCADE,
                purchased_at      TIMESTAMP       NOT NULL DEFAULT NOW(),
                next_billing_date DATE            NOT NULL,
                first_payment     DECIMAL(10,2)   NOT NULL,
                monthly_amount    DECIMAL(10,2)   NOT NULL,
                seat_count        INTEGER         NOT NULL DEFAULT 1,
                package_type      VARCHAR(20)     NOT NULL DEFAULT 'single',
                status            VARCHAR(20)     NOT NULL DEFAULT 'active',
                tx_ref            VARCHAR(100),
                created_at        TIMESTAMP       NOT NULL DEFAULT NOW()
            )
        ''')
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_seat_subs_agency "
            "ON seat_subscriptions (agency_id, status)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_seat_subs_billing "
            "ON seat_subscriptions (next_billing_date) WHERE status = 'active'"
        )
        conn.commit()
        print("✅ migrate_seat_subscriptions complete")
    except Exception as e:
        if conn:
            try: conn.rollback()
            except Exception: pass
        print(f"⚠️  migrate_seat_subscriptions: {e}")
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass
