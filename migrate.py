"""
Migration script — copies data from SQLite (chatbot.db) to PostgreSQL.

Usage:
    python migrate.py                        # full migration
    python migrate.py --dry-run              # simulate; rolls back at the end
    python migrate.py --table leads          # migrate one table only
    python migrate.py --table leads --dry-run

Tables migrated:
    users, clients, faqs, leads, affiliates,
    referrals, commissions, conversations, knowledge_base

Errors are printed to stdout AND written to migration_errors.log.
Row counts are compared between SQLite and Postgres at the end.
"""

import argparse
import json
import logging
import os
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
SQLITE_DB    = os.environ.get("SQLITE_DB", "chatbot.db")
DATABASE_URL = os.environ.get("DATABASE_URL")
CHUNK_SIZE   = int(os.environ.get("MIGRATE_CHUNK_SIZE", 500))
LOG_FILE     = "migration_errors.log"

# Tables with SERIAL PKs that need sequence repair after migration
SEQUENCE_TABLES = frozenset({
    "users", "clients", "faqs", "leads",
    "affiliates", "referrals", "commissions", "conversations",
})

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ── Argument parsing ──────────────────────────────────────────────────────────
def parse_args():
    parser = argparse.ArgumentParser(description="SQLite to PostgreSQL migration")
    parser.add_argument("--dry-run", action="store_true",
        help="Simulate migration — rolls back all changes at the end")
    parser.add_argument("--table", metavar="TABLE",
        help="Migrate a single table only (e.g. --table leads)")
    return parser.parse_args()


# ── Connection helpers ────────────────────────────────────────────────────────
def open_sqlite(path):
    if not os.path.exists(path):
        log.error("SQLite file not found: %s", path)
        sys.exit(1)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def open_postgres(url):
    if not url:
        log.error("DATABASE_URL is not set. Add it to your .env file.")
        sys.exit(1)
    try:
        conn = psycopg2.connect(url)
        conn.autocommit = False
        return conn
    except psycopg2.OperationalError as e:
        log.error("Could not connect to Postgres: %s", e)
        sys.exit(1)


@contextmanager
def savepoint(pg_cursor, name="sp"):
    """
    Wraps a block in a SAVEPOINT so only that block is rolled back on error,
    not the entire transaction. This prevents a single bad row from aborting
    all subsequent inserts (InFailedSqlTransaction).
    """
    pg_cursor.execute("SAVEPOINT %s" % name)
    try:
        yield
        pg_cursor.execute("RELEASE SAVEPOINT %s" % name)
    except Exception:
        pg_cursor.execute("ROLLBACK TO SAVEPOINT %s" % name)
        raise


# ── Chunked SQLite reader ─────────────────────────────────────────────────────
def iter_sqlite(sqlite_cursor, table, chunk=None):
    """
    Yield rows from SQLite in chunks so we never load an entire large table
    into RAM. Each row is a plain dict so .get() works everywhere.
    """
    if chunk is None:
        chunk = CHUNK_SIZE
    sqlite_cursor.execute("SELECT * FROM %s" % table)
    while True:
        rows = sqlite_cursor.fetchmany(chunk)
        if not rows:
            break
        yield [dict(r) for r in rows]


# ── Generic batched upsert ────────────────────────────────────────────────────
def batch_upsert(pg_cursor, sql, rows, label):
    """
    Insert a batch with execute_values (one round-trip per chunk).
    Falls back to row-by-row inside savepoints if the batch fails,
    so one bad row never silently kills the entire chunk.
    Returns (inserted, skipped).
    """
    inserted = skipped = 0
    try:
        with savepoint(pg_cursor, "batch"):
            execute_values(pg_cursor, sql, rows)
            # rowcount reflects actual inserts (skipped rows don't count)
            inserted = pg_cursor.rowcount if pg_cursor.rowcount >= 0 else len(rows)
    except Exception as e:
        log.warning("  Batch insert failed for %s, falling back row-by-row: %s", label, e)
        for row in rows:
            try:
                with savepoint(pg_cursor, "row"):
                    execute_values(pg_cursor, sql, [row])
                    if pg_cursor.rowcount and pg_cursor.rowcount > 0:
                        inserted += 1
                    else:
                        skipped += 1
            except Exception as row_err:
                skipped += 1
                log.warning("    Skipped row in %s: %s | row: %s", label, row_err, row)
    return inserted, skipped


# ── Per-table migration functions ─────────────────────────────────────────────

def migrate_users(sqlite_cursor, pg_cursor):
    log.info("Migrating users...")
    sql = """
        INSERT INTO users (id, email, password_hash, created_at, plan_type)
        VALUES %s
        ON CONFLICT (email) DO NOTHING
    """
    inserted = skipped = 0
    for chunk in iter_sqlite(sqlite_cursor, "users"):
        rows = [
            (r["id"], r["email"], r["password_hash"],
             r["created_at"], r.get("plan_type") or "free")
            for r in chunk
        ]
        i, s = batch_upsert(pg_cursor, sql, rows, "users")
        inserted += i; skipped += s
    log.info("  users done — inserted: %d, skipped/existing: %d", inserted, skipped)
    return {"inserted": inserted, "skipped": skipped}


def migrate_clients(sqlite_cursor, pg_cursor):
    log.info("Migrating clients...")
    try:
        from models import migrate_clients_table
        migrate_clients_table()
    except Exception as exc:
        log.warning("  Could not run clients migration helper: %s", exc)

    sql = """
        INSERT INTO clients
            (id, user_id, client_id, company_name, branding_settings,
             widget_color, welcome_message, remove_branding, created_at)
        VALUES %s
        ON CONFLICT (client_id) DO NOTHING
    """
    inserted = skipped = 0
    for chunk in iter_sqlite(sqlite_cursor, "clients"):
        rows = []
        for r in chunk:
            try:
                branding = json.loads(r.get("branding_settings") or "{}")
            except (json.JSONDecodeError, TypeError):
                branding = {}
            rows.append((
                r["id"], r["user_id"], r["client_id"], r["company_name"],
                r.get("branding_settings"),
                branding.get("branding", {}).get("primary_color"),
                branding.get("bot_settings", {}).get("welcome_message"),
                bool(branding.get("branding", {}).get("remove_branding", False)),
                r["created_at"],
            ))
        i, s = batch_upsert(pg_cursor, sql, rows, "clients")
        inserted += i; skipped += s
    log.info("  clients done — inserted: %d, skipped/existing: %d", inserted, skipped)
    return {"inserted": inserted, "skipped": skipped}


def migrate_faqs(sqlite_cursor, pg_cursor):
    log.info("Migrating FAQs...")
    # ON CONFLICT must name the unique column — without a target, Postgres
    # falls back to the PK and will silently insert duplicate faq_ids.
    sql = """
        INSERT INTO faqs
            (id, client_id, faq_id, question, answer, category, triggers, created_at)
        VALUES %s
        ON CONFLICT (faq_id) DO NOTHING
    """
    inserted = skipped = 0
    for chunk in iter_sqlite(sqlite_cursor, "faqs"):
        rows = [
            (r["id"], r["client_id"], r["faq_id"],
             r["question"], r["answer"],
             r.get("category") or "General",
             r.get("triggers"), r["created_at"])
            for r in chunk
        ]
        i, s = batch_upsert(pg_cursor, sql, rows, "faqs")
        inserted += i; skipped += s
    log.info("  faqs done — inserted: %d, skipped/existing: %d", inserted, skipped)
    return {"inserted": inserted, "skipped": skipped}


def migrate_leads(sqlite_cursor, pg_cursor):
    log.info("Migrating leads...")
    # Conflict on PK id — leads has no natural unique column.
    # Without a target, ON CONFLICT DO NOTHING is a no-op and every re-run
    # duplicates every lead.
    sql = """
        INSERT INTO leads
            (id, client_id, name, email, phone, company,
             message, conversation_snippet, source_url, created_at)
        VALUES %s
        ON CONFLICT (id) DO NOTHING
    """
    inserted = skipped = 0
    for chunk in iter_sqlite(sqlite_cursor, "leads"):
        rows = [
            (r["id"], r["client_id"], r.get("name"), r.get("email"),
             r.get("phone"), r.get("company"), r.get("message"),
             r.get("conversation_snippet"), r.get("source_url"), r["created_at"])
            for r in chunk
        ]
        i, s = batch_upsert(pg_cursor, sql, rows, "leads")
        inserted += i; skipped += s
    log.info("  leads done — inserted: %d, skipped/existing: %d", inserted, skipped)
    return {"inserted": inserted, "skipped": skipped}


def _table_exists_in_sqlite(sqlite_cursor, table):
    sqlite_cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    )
    return sqlite_cursor.fetchone() is not None


def migrate_affiliates(sqlite_cursor, pg_cursor):
    log.info("Migrating affiliates...")
    if not _table_exists_in_sqlite(sqlite_cursor, "affiliates"):
        log.info("  affiliates not in SQLite — skipping")
        return {"inserted": 0, "skipped": 0}

    sql = """
        INSERT INTO affiliates
            (id, user_id, referral_code, commission_rate, total_earnings,
             total_referrals, payment_email, payment_method,
             bank_details, status, created_at)
        VALUES %s
        ON CONFLICT (referral_code) DO NOTHING
    """
    inserted = skipped = 0
    for chunk in iter_sqlite(sqlite_cursor, "affiliates"):
        rows = [
            (r["id"], r["user_id"], r["referral_code"],
             r.get("commission_rate", 0.20), r.get("total_earnings", 0),
             r.get("total_referrals", 0), r.get("payment_email"),
             r.get("payment_method"), r.get("bank_details"),
             r.get("status", "active"), r["created_at"])
            for r in chunk
        ]
        i, s = batch_upsert(pg_cursor, sql, rows, "affiliates")
        inserted += i; skipped += s
    log.info("  affiliates done — inserted: %d, skipped/existing: %d", inserted, skipped)
    return {"inserted": inserted, "skipped": skipped}


def migrate_referrals(sqlite_cursor, pg_cursor):
    log.info("Migrating referrals...")
    if not _table_exists_in_sqlite(sqlite_cursor, "referrals"):
        log.info("  referrals not in SQLite — skipping")
        return {"inserted": 0, "skipped": 0}

    sql = """
        INSERT INTO referrals
            (id, affiliate_id, referred_user_id, referral_code, status, created_at)
        VALUES %s
        ON CONFLICT (id) DO NOTHING
    """
    inserted = skipped = 0
    for chunk in iter_sqlite(sqlite_cursor, "referrals"):
        rows = [
            (r["id"], r["affiliate_id"], r.get("referred_user_id"),
             r["referral_code"], r.get("status", "pending"), r["created_at"])
            for r in chunk
        ]
        i, s = batch_upsert(pg_cursor, sql, rows, "referrals")
        inserted += i; skipped += s
    log.info("  referrals done — inserted: %d, skipped/existing: %d", inserted, skipped)
    return {"inserted": inserted, "skipped": skipped}


def migrate_commissions(sqlite_cursor, pg_cursor):
    log.info("Migrating commissions...")
    if not _table_exists_in_sqlite(sqlite_cursor, "commissions"):
        log.info("  commissions not in SQLite — skipping")
        return {"inserted": 0, "skipped": 0}

    sql = """
        INSERT INTO commissions
            (id, affiliate_id, referred_user_id, amount, status,
             payment_date, created_at)
        VALUES %s
        ON CONFLICT (id) DO NOTHING
    """
    inserted = skipped = 0
    for chunk in iter_sqlite(sqlite_cursor, "commissions"):
        rows = [
            (r["id"], r["affiliate_id"], r.get("referred_user_id"),
             r.get("amount", 0), r.get("status", "pending"),
             r.get("payment_date"), r["created_at"])
            for r in chunk
        ]
        i, s = batch_upsert(pg_cursor, sql, rows, "commissions")
        inserted += i; skipped += s
    log.info("  commissions done — inserted: %d, skipped/existing: %d", inserted, skipped)
    return {"inserted": inserted, "skipped": skipped}


def migrate_conversations(sqlite_cursor, pg_cursor):
    """
    Previously missing — conversations power analytics and daily message count
    enforcement. Omitting this meant historical data was silently lost.
    """
    log.info("Migrating conversations...")
    if not _table_exists_in_sqlite(sqlite_cursor, "conversations"):
        log.info("  conversations not in SQLite — skipping")
        return {"inserted": 0, "skipped": 0}

    sql = """
        INSERT INTO conversations
            (id, client_id, user_message, bot_response, matched, method, timestamp)
        VALUES %s
        ON CONFLICT (id) DO NOTHING
    """
    inserted = skipped = 0
    for chunk in iter_sqlite(sqlite_cursor, "conversations"):
        rows = [
            (r["id"], r["client_id"], r["user_message"], r["bot_response"],
             bool(r.get("matched", False)), r.get("method", "unknown"),
             r.get("timestamp"))
            for r in chunk
        ]
        i, s = batch_upsert(pg_cursor, sql, rows, "conversations")
        inserted += i; skipped += s
    log.info("  conversations done — inserted: %d, skipped/existing: %d", inserted, skipped)
    return {"inserted": inserted, "skipped": skipped}


# ── Sequence repair ───────────────────────────────────────────────────────────
def fix_sequences(pg_cursor):
    """
    Reset each SERIAL sequence to MAX(id) so new inserts don't collide with
    migrated IDs. Table names come from a hardcoded frozenset (not user input)
    so f-string interpolation is safe. The table name is also passed as a bind
    parameter to pg_get_serial_sequence for the portion that accepts one.
    """
    log.info("Fixing ID sequences...")
    for table in SEQUENCE_TABLES:
        try:
            pg_cursor.execute(
                "SELECT setval(pg_get_serial_sequence(%s, 'id'), COALESCE(MAX(id), 1)) FROM " + table,
                (table,)
            )
            log.info("  Sequence fixed: %s", table)
        except Exception as e:
            log.warning("  Skipped sequence for %s: %s", table, e)


# ── Row-count verification ────────────────────────────────────────────────────
def verify_counts(sqlite_cursor, pg_cursor, tables):
    """
    Compare SQLite vs Postgres row counts for each migrated table.
    Flags mismatches so partial migrations are immediately visible
    rather than being discovered later when data is missing.
    """
    log.info("Verifying row counts...")
    all_ok = True
    for table in tables:
        try:
            sqlite_cursor.execute("SELECT COUNT(*) FROM %s" % table)
            sq_count = sqlite_cursor.fetchone()[0]
        except Exception:
            sq_count = None

        try:
            pg_cursor.execute("SELECT COUNT(*) FROM %s" % table)
            row = pg_cursor.fetchone()
            pg_count = row[0] if row else 0
        except Exception:
            pg_count = None

        if sq_count is None:
            log.info("  %-20s  not in SQLite", table)
        elif sq_count == pg_count:
            log.info("  %-20s  %d rows match", table, pg_count)
        else:
            log.warning("  %-20s  SQLite: %d  Postgres: %d  MISMATCH", table, sq_count, pg_count)
            all_ok = False

    if not all_ok:
        log.warning("Some row counts differ — check %s for skipped rows.", LOG_FILE)
    else:
        log.info("All row counts match.")


# ── Dispatch table ────────────────────────────────────────────────────────────
ALL_MIGRATIONS = {
    "users":         migrate_users,
    "clients":       migrate_clients,
    "faqs":          migrate_faqs,
    "leads":         migrate_leads,
    "affiliates":    migrate_affiliates,
    "referrals":     migrate_referrals,
    "commissions":   migrate_commissions,
    "conversations": migrate_conversations,
}


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    args = parse_args()

    if args.table and args.table not in ALL_MIGRATIONS:
        log.error("Unknown table '%s'. Choose from: %s", args.table, ", ".join(ALL_MIGRATIONS))
        sys.exit(1)

    if args.dry_run:
        log.info("DRY RUN — all changes will be rolled back at the end\n")

    log.info("Connecting to databases...")
    sqlite_conn   = open_sqlite(SQLITE_DB)
    sqlite_cursor = sqlite_conn.cursor()
    pg_conn       = open_postgres(DATABASE_URL)
    pg_cursor     = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    log.info("Connected to both databases\n")

    start = datetime.now()

    try:
        to_run = (
            {args.table: ALL_MIGRATIONS[args.table]}
            if args.table
            else ALL_MIGRATIONS
        )

        for name, fn in to_run.items():
            fn(sqlite_cursor, pg_cursor)
            # Commit after each table so progress is preserved if a later table
            # fails. A single end-commit means one failure rolls back everything.
            if not args.dry_run:
                pg_conn.commit()
                log.info("  Committed %s to Postgres\n", name)

        fix_sequences(pg_cursor)
        if not args.dry_run:
            pg_conn.commit()

        verify_counts(sqlite_cursor, pg_cursor, list(to_run.keys()))

        elapsed = (datetime.now() - start).total_seconds()

        if args.dry_run:
            pg_conn.rollback()
            log.info("\nDRY RUN complete in %.1fs — no data was written.", elapsed)
        else:
            log.info("\nMigration complete in %.1fs — data committed to PostgreSQL.", elapsed)
            log.info("Errors (if any) written to: %s", LOG_FILE)

    except Exception as e:
        pg_conn.rollback()
        log.error("Fatal migration error: %s", e)
        raise

    finally:
        for obj, label in [
            (sqlite_cursor, "SQLite cursor"),
            (sqlite_conn,   "SQLite connection"),
            (pg_cursor,     "Postgres cursor"),
            (pg_conn,       "Postgres connection"),
        ]:
            try:
                obj.close()
            except Exception as close_err:
                log.warning("Could not close %s: %s", label, close_err)


if __name__ == "__main__":
    main()
