"""
models/cron.py
--------------
Cron run audit log, digest deduplication, usage warnings, and log pruning.
"""
import json
from .db import get_db

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


def upsert_usage_warning(client_id: str, pct: int, today_count: int, daily_limit: int):
    """
    Store / update a usage-warning record so the dashboard can show a banner.
    Table: usage_warnings(client_id PK, pct INT, today_count INT, daily_limit INT, updated_at TIMESTAMP)
    Created lazily via ADD COLUMN IF NOT EXISTS pattern below.
    """
    conn, cursor = get_db()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS usage_warnings (
                client_id   TEXT PRIMARY KEY,
                pct         INT,
                today_count INT,
                daily_limit INT,
                updated_at  TIMESTAMP DEFAULT NOW()
            )
        """)
        cursor.execute("""
            INSERT INTO usage_warnings (client_id, pct, today_count, daily_limit, updated_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (client_id) DO UPDATE
              SET pct=EXCLUDED.pct, today_count=EXCLUDED.today_count,
                  daily_limit=EXCLUDED.daily_limit, updated_at=NOW()
        """, (client_id, pct, today_count, daily_limit))
        conn.commit()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[upsert_usage_warning] {e}")
        try: conn.rollback()
        except: pass
    finally:
        cursor.close()
        conn.close()


def get_usage_warning(client_id: str):
    """Return the latest usage warning for a client, or None."""
    conn, cursor = get_db()
    try:
        cursor.execute(
            "SELECT * FROM usage_warnings WHERE client_id = %s", (client_id,)
        )
        row = cursor.fetchone()
        return dict(row) if row else None
    except Exception:
        return None
    finally:
        cursor.close()
        conn.close()


def get_stale_new_leads(min_hours: int = 24, max_hours: int = 48) -> list:
    """
    Return leads still in 'new' stage that have never been touched since
    capture (updated_at IS NULL) and are between min_hours and max_hours
    old. Excludes leads already nudged (stale_nudge_sent_at IS NULL) so
    each lead is nudged at most once per stale window.
    """
    conn, cursor = get_db()
    try:
        cursor.execute("""
            SELECT id, client_id, name, email, stage, created_at
            FROM leads
            WHERE stage = 'new'
              AND updated_at IS NULL
              AND stale_nudge_sent_at IS NULL
              AND created_at <= NOW() - (%s * INTERVAL '1 hour')
              AND created_at >  NOW() - (%s * INTERVAL '1 hour')
        """, (min_hours, max_hours))
        return [dict(r) for r in cursor.fetchall()]
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[get_stale_new_leads] {e}")
        return []
    finally:
        cursor.close()
        conn.close()


def mark_stale_nudge_sent(lead_id: int) -> None:
    """Stamp stale_nudge_sent_at = NOW() after a successful stale-lead nudge."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            "UPDATE leads SET stale_nudge_sent_at = NOW() WHERE id = %s",
            (lead_id,)
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[mark_stale_nudge_sent] {e}")


def get_due_follow_ups() -> list:
    """
    Return leads whose scheduled follow_up_at has arrived and that haven't
    been reminded yet (followup_reminder_sent_at IS NULL). Excludes leads
    already 'closed' or 'lost' — nothing to follow up on there.
    """
    conn, cursor = get_db()
    try:
        cursor.execute("""
            SELECT id, client_id, name, email, stage, follow_up_at
            FROM leads
            WHERE follow_up_at IS NOT NULL
              AND follow_up_at <= NOW()
              AND followup_reminder_sent_at IS NULL
              AND stage NOT IN ('closed', 'lost')
        """)
        return [dict(r) for r in cursor.fetchall()]
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[get_due_follow_ups] {e}")
        return []
    finally:
        cursor.close()
        conn.close()


def mark_followup_reminder_sent(lead_id: int) -> None:
    """Stamp followup_reminder_sent_at = NOW() after a successful reminder."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            "UPDATE leads SET followup_reminder_sent_at = NOW() WHERE id = %s",
            (lead_id,)
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[mark_followup_reminder_sent] {e}")


def get_unanswered_questions_for_email(client_id: str, since_days: int = 7, limit: int = 5):
    """
    Return the top unanswered questions for a client from the past N days.
    Used for the weekly digest email.
    """
    conn, cursor = get_db()
    try:
        cursor.execute("""
            SELECT user_message AS question, COUNT(*) AS cnt
            FROM conversations
            WHERE client_id = %s
              AND matched = FALSE
              AND timestamp >= NOW() - (%s * INTERVAL '1 day')
            GROUP BY user_message
            ORDER BY cnt DESC
            LIMIT %s
        """, (client_id, since_days, limit))
        rows = cursor.fetchall()
        return [{'question': r['question'], 'count': int(r['cnt'])} for r in rows]
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[get_unanswered_for_email] {e}")
        return []
    finally:
        cursor.close()
        conn.close()


def get_clients_for_weekly_digest():
    """
    Return all active paid clients with their owner email and contact_email,
    for the weekly unanswered-questions digest.
    Only returns clients where the owner is on a paid plan (not free).
    """
    conn, cursor = get_db()
    try:
        cursor.execute("""
            SELECT
                c.client_id,
                c.business_name,
                c.contact_email,
                u.email   AS owner_email,
                u.plan_type
            FROM clients c
            JOIN users u ON u.id = c.user_id
            WHERE u.plan_type NOT IN ('free', 'enterprise')
              AND c.is_active = TRUE
        """)
        return [dict(r) for r in cursor.fetchall()]
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"[get_clients_for_weekly_digest] {e}")
        return []
    finally:
        cursor.close()
        conn.close()


# =====================================================================
# HELP CENTER ARTICLES
# =====================================================================

