"""
models/knowledge_imports.py
-----------------------------
Business Knowledge Phase 1 — bulk-import job tracking, claim/reclaim/
retry. Deliberately a close structural mirror of models/faq_imports.py
rather than a generalized/shared job mechanism — per Phase 1's
constraints, this stays isolated from the FAQ pipeline rather than
risking it. Worth reconsidering a shared job table once both systems
have real usage behind them, not now.

Only ever touches business_knowledge and business_knowledge_import_jobs.
Never touches faqs, faq_import_jobs, knowledge_base, or articles.

Job lifecycle: queued -> processing -> completed | completed_with_errors | failed
Per-row (business_knowledge.embedding_status): pending -> embedding -> embedded | failed
  ('embedding' is a short-lived claim/lease state set by
  claim_knowledge_for_embedding() via FOR UPDATE SKIP LOCKED, so two
  concurrent workers for the same job can never claim the same row — see
  models/faq_imports.py::claim_faqs_for_embedding()'s docstring for the
  full reasoning; identical mechanism here.)
"""
from .db import get_db
from psycopg2.extras import execute_values


def create_import_job(job_id: str, client_id: str, total: int) -> None:
    conn, cursor = get_db()
    try:
        cursor.execute(
            """INSERT INTO business_knowledge_import_jobs
                   (job_id, client_id, status, total)
               VALUES (%s, %s, 'queued', %s)""",
            (job_id, client_id, total)
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def mark_import_job_started(job_id: str) -> None:
    conn, cursor = get_db()
    try:
        cursor.execute(
            """UPDATE business_knowledge_import_jobs
               SET status = 'processing',
                   started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
                   updated_at = CURRENT_TIMESTAMP
               WHERE job_id = %s AND status IN ('queued', 'processing')""",
            (job_id,)
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def increment_import_job_progress(job_id: str, processed_delta: int = 0,
                                   failed_delta: int = 0) -> None:
    conn, cursor = get_db()
    try:
        cursor.execute(
            """UPDATE business_knowledge_import_jobs
               SET processed  = processed + %s,
                   failed     = failed + %s,
                   updated_at = CURRENT_TIMESTAMP
               WHERE job_id = %s""",
            (processed_delta, failed_delta, job_id)
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def finalize_import_job(job_id: str) -> None:
    conn, cursor = get_db()
    try:
        cursor.execute(
            "SELECT total, processed, failed FROM business_knowledge_import_jobs "
            "WHERE job_id = %s",
            (job_id,)
        )
        row = cursor.fetchone()
        if not row:
            return
        total, processed, failed = row['total'], row['processed'], row['failed']
        if processed + failed < total:
            return  # not actually finished — leave status as 'processing'

        if failed == 0:
            status = 'completed'
        elif processed > 0:
            status = 'completed_with_errors'
        else:
            status = 'failed'

        cursor.execute(
            """UPDATE business_knowledge_import_jobs
               SET status = %s, completed_at = CURRENT_TIMESTAMP,
                   updated_at = CURRENT_TIMESTAMP
               WHERE job_id = %s""",
            (status, job_id)
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def get_import_job(job_id: str, client_id: str = None) -> dict:
    conn, cursor = get_db()
    try:
        if client_id:
            cursor.execute(
                "SELECT * FROM business_knowledge_import_jobs "
                "WHERE job_id = %s AND client_id = %s",
                (job_id, client_id)
            )
        else:
            cursor.execute(
                "SELECT * FROM business_knowledge_import_jobs WHERE job_id = %s",
                (job_id,)
            )
        row = cursor.fetchone()
        return dict(row) if row else None
    finally:
        cursor.close()
        conn.close()


def reclaim_stale_import_job(job_id: str, stale_minutes: int = 10) -> bool:
    """
    Atomic UPDATE...RETURNING — only one concurrent caller can ever win a
    given staleness window. On a genuine win, also resets any of this
    job's rows still 'embedding' (claimed by a dead worker, never
    finished) back to 'pending'. Identical mechanism and reasoning to
    models/faq_imports.py::reclaim_stale_import_job().
    """
    conn, cursor = get_db()
    try:
        cursor.execute(
            """UPDATE business_knowledge_import_jobs
               SET updated_at = CURRENT_TIMESTAMP
               WHERE job_id = %s
                 AND status = 'processing'
                 AND (processed + failed) < total
                 AND updated_at < CURRENT_TIMESTAMP - (%s || ' minutes')::interval
               RETURNING job_id, client_id""",
            (job_id, stale_minutes)
        )
        row = cursor.fetchone()
        if not row:
            conn.commit()
            return False

        cursor.execute(
            """UPDATE business_knowledge SET embedding_status = 'pending'
               WHERE client_id = %s AND import_batch_id = %s
                 AND embedding_status = 'embedding'""",
            (row['client_id'], job_id)
        )
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def retry_failed_embeddings(job_id: str, client_id: str) -> int:
    """
    Resets this job's 'failed' rows back to 'pending' and un-finalizes the
    job so it goes through the normal lifecycle again. Identical mechanism
    to models/faq_imports.py::retry_failed_embeddings().
    """
    conn, cursor = get_db()
    try:
        cursor.execute(
            """UPDATE business_knowledge SET embedding_status = 'pending'
               WHERE client_id = %s AND import_batch_id = %s
                 AND embedding_status = 'failed'""",
            (client_id, job_id)
        )
        n = cursor.rowcount
        if n:
            cursor.execute(
                """UPDATE business_knowledge_import_jobs
                   SET failed = failed - %s, status = 'processing',
                       completed_at = NULL, updated_at = CURRENT_TIMESTAMP
                   WHERE job_id = %s""",
                (n, job_id)
            )
        conn.commit()
        return n
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def delete_import_jobs_for_client(client_id: str) -> None:
    conn, cursor = get_db()
    try:
        cursor.execute(
            "DELETE FROM business_knowledge_import_jobs WHERE client_id = %s",
            (client_id,)
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def claim_knowledge_for_embedding(client_id: str, import_batch_id: str,
                                   limit: int = 200) -> list:
    """
    Atomic claim via FOR UPDATE SKIP LOCKED — identical mechanism to
    models/faq_imports.py::claim_faqs_for_embedding(). Two concurrent
    workers for the same job (or a FAQ job and a Business Knowledge job
    running at the same time, which share nothing at the DB level but do
    share the Voyage rate limiter downstream) claim disjoint rows, never
    the same one twice.
    """
    conn, cursor = get_db()
    try:
        cursor.execute(
            """UPDATE business_knowledge SET embedding_status = 'embedding'
               WHERE id IN (
                   SELECT id FROM business_knowledge
                   WHERE client_id = %s AND import_batch_id = %s
                     AND embedding_status = 'pending'
                   ORDER BY id
                   LIMIT %s
                   FOR UPDATE SKIP LOCKED
               )
               RETURNING id, title, content""",
            (client_id, import_batch_id, limit)
        )
        rows = [dict(r) for r in cursor.fetchall()]
        conn.commit()
        return rows
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def bulk_update_knowledge_embeddings(client_id: str, updates: list) -> int:
    """
    updates: list of (row_id, embedding_json_str_or_None, status) tuples.
    Identical batched-UPDATE pattern to
    models/faq_imports.py::bulk_update_faq_embeddings() (client_id folded
    into each VALUES row for the same reason — execute_values only binds
    the single %s immediately after VALUES).
    """
    if not updates:
        return 0
    rows = [(client_id, row_id, embedding, status) for (row_id, embedding, status) in updates]
    conn, cursor = get_db()
    try:
        execute_values(
            cursor,
            """UPDATE business_knowledge AS bk SET
                   embedding        = v.embedding,
                   embedding_status = v.status,
                   last_indexed     = CURRENT_TIMESTAMP
               FROM (VALUES %s) AS v(client_id, id, embedding, status)
               WHERE bk.client_id = v.client_id AND bk.id = v.id""",
            rows,
            template="(%s, %s, %s, %s)",
        )
        conn.commit()
        return len(rows)
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
