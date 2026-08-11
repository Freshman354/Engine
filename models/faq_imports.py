"""
models/faq_imports.py
----------------------
Bulk FAQ upload (Stage A) job tracking.

This is deliberately a separate, small module rather than new columns on
knowledge_base: knowledge_base is not part of the live retrieval path today
(see models/faqs.py / blueprints/faqs.py upload flow, which writes
embeddings straight to faqs.embedding), so it gets no new schema and no job
coupling. This module only ever touches `faqs` and the new
`faq_import_jobs` table (see migrations.migrate_faqs_import_tracking()).

Job lifecycle: queued -> processing -> completed | completed_with_errors | failed
Per-row (faqs.embedding_status): pending -> embedding -> embedded | failed
  ('embedding' is a short-lived claim/lease state set by
  claim_faqs_for_embedding() via FOR UPDATE SKIP LOCKED, so two concurrent
  workers for the same job can never claim the same row.)
"""
from .db import get_db
from psycopg2.extras import execute_values


def create_import_job(job_id: str, client_id: str, total: int) -> None:
    """Create a job row in 'queued' state. Called synchronously in the
    upload request, before the background embedding phase is submitted."""
    conn, cursor = get_db()
    try:
        cursor.execute(
            """INSERT INTO faq_import_jobs (job_id, client_id, status, total)
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
    """Flip queued -> processing. Idempotent — safe to call again if a
    resubmitted (retried) run finds the job already 'processing'."""
    conn, cursor = get_db()
    try:
        cursor.execute(
            """UPDATE faq_import_jobs
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
    """Bump counters after each embedding sub-batch completes. Also touches
    updated_at, which doubles as the staleness/lease timestamp for crash
    detection — see reclaim_stale_import_job()."""
    conn, cursor = get_db()
    try:
        cursor.execute(
            """UPDATE faq_import_jobs
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
    """Set a terminal status once processed+failed reaches total. Idempotent
    — if there's still pending work, this is a no-op, so calling it
    speculatively is always safe."""
    conn, cursor = get_db()
    try:
        cursor.execute(
            "SELECT total, processed, failed FROM faq_import_jobs WHERE job_id = %s",
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
            """UPDATE faq_import_jobs
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
                "SELECT * FROM faq_import_jobs WHERE job_id = %s AND client_id = %s",
                (job_id, client_id)
            )
        else:
            cursor.execute(
                "SELECT * FROM faq_import_jobs WHERE job_id = %s", (job_id,)
            )
        row = cursor.fetchone()
        return dict(row) if row else None
    finally:
        cursor.close()
        conn.close()


def reclaim_stale_import_job(job_id: str, stale_minutes: int = 10) -> bool:
    """
    Lease-style crash recovery — made atomic (single UPDATE ... RETURNING,
    not a separate SELECT-then-UPDATE) so two concurrent callers (e.g. two
    browser tabs polling the same job, or a race between a status poll and
    a retry) can't both decide the job is stale and both resubmit a worker.

    Postgres row-locking makes this safe: if two callers run this at the
    same instant, the second one's UPDATE blocks until the first commits,
    then re-evaluates its WHERE clause against the now-just-updated row —
    which no longer matches (updated_at is fresh), so it affects 0 rows and
    returns False. Only one caller ever gets True back for a given staleness
    window.

    On a genuine win, also resets any of this job's rows still marked
    embedding_status='embedding' (claimed by the dead worker but never
    finished) back to 'pending' — otherwise they'd be stuck forever,
    invisible to both the 'pending' claim query and the terminal
    'embedded'/'failed' states. This is what actually prevents a crashed
    worker from leaving orphaned in-progress rows, on top of
    claim_faqs_for_embedding()'s SKIP LOCKED preventing two *live* workers
    from double-processing the same rows.

    Returns True exactly when this call is the one that should resubmit
    the embedding phase.
    """
    conn, cursor = get_db()
    try:
        cursor.execute(
            """UPDATE faq_import_jobs
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
            """UPDATE faqs SET embedding_status = 'pending'
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
    Resets this job's 'failed' rows back to 'pending' so the next run of
    the embedding phase picks them up via claim_faqs_for_embedding() —
    'embedded' rows are untouched, so a retry only reprocesses what's
    actually still outstanding, never re-embeds work that already
    succeeded.

    Un-finalizes the job (status -> 'processing', completed_at cleared) so
    it goes through the normal lifecycle again and finalize_import_job()
    can re-decide the terminal status once the retry finishes.

    Idempotent: once failed rows are reset, a second call finds none left
    and returns 0. Does not resubmit the background task itself — the
    caller (the retry route) does that after seeing a nonzero return.
    """
    conn, cursor = get_db()
    try:
        cursor.execute(
            """UPDATE faqs SET embedding_status = 'pending'
               WHERE client_id = %s AND import_batch_id = %s
                 AND embedding_status = 'failed'""",
            (client_id, job_id)
        )
        n = cursor.rowcount
        if n:
            cursor.execute(
                """UPDATE faq_import_jobs
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
    """Called from the delete-all-FAQs route so job rows don't accumulate
    forever after a client wipes their FAQ set."""
    conn, cursor = get_db()
    try:
        cursor.execute("DELETE FROM faq_import_jobs WHERE client_id = %s", (client_id,))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def claim_faqs_for_embedding(client_id: str, import_batch_id: str, limit: int = 200) -> list:
    """
    Atomically claims up to `limit` not-yet-embedded rows for this job in
    one statement: SELECT ... FOR UPDATE SKIP LOCKED to pick the rows,
    UPDATE to mark them embedding_status='embedding' (an in-progress lease
    state distinct from 'pending'), RETURNING what got claimed.

    This is the fix for concurrent duplicate processing: if two workers
    for the same job_id run this at once (e.g. a stale-job reclaim
    resubmits while the original worker turns out to still be alive), the
    second caller's SKIP LOCKED simply skips whatever the first already
    has locked and claims a disjoint set of rows instead. Neither worker
    can ever get the same row, so results can't be double-embedded or
    double-counted into job progress — regardless of *why* two workers
    ended up running.

    Rows claimed but never finished because the worker died mid-batch are
    recovered by reclaim_stale_import_job(), which resets any row still
    'embedding' back to 'pending' once the job itself is confirmed stale.
    """
    conn, cursor = get_db()
    try:
        cursor.execute(
            """UPDATE faqs SET embedding_status = 'embedding'
               WHERE faq_id IN (
                   SELECT faq_id FROM faqs
                   WHERE client_id = %s AND import_batch_id = %s
                     AND embedding_status = 'pending'
                   ORDER BY id
                   LIMIT %s
                   FOR UPDATE SKIP LOCKED
               )
               RETURNING faq_id, question, answer""",
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


def bulk_update_faq_embeddings(client_id: str, updates: list) -> int:
    """
    Batched write of embedding results back onto `faqs` — one round trip
    for the whole sub-batch instead of one UPDATE per row. Transitions
    rows out of the 'embedding' claim state into a terminal state
    ('embedded' or 'failed') regardless of what they were before, so this
    is safe to call unconditionally once embed_batch() returns.

    updates: list of (faq_id, embedding_json_str_or_None, status) tuples,
             status is 'embedded' or 'failed'.

    client_id is folded into each VALUES row (rather than passed as a
    separate bound parameter) because psycopg2's execute_values only
    substitutes the single %s immediately following VALUES — a second bare
    %s elsewhere in the query string would NOT be parameter-bound.
    """
    if not updates:
        return 0
    rows = [(client_id, faq_id, embedding, status) for (faq_id, embedding, status) in updates]
    conn, cursor = get_db()
    try:
        execute_values(
            cursor,
            """UPDATE faqs AS f SET
                   embedding        = v.embedding,
                   embedding_status = v.status,
                   last_indexed     = CURRENT_TIMESTAMP
               FROM (VALUES %s) AS v(client_id, faq_id, embedding, status)
               WHERE f.client_id = v.client_id AND f.faq_id = v.faq_id""",
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
