"""
models/business_knowledge.py
-----------------------------
Business Knowledge Phase 1 — storage layer for business_knowledge, the
generalized knowledge table (Architecture 2: one table, source_id +
chunk_index instead of a separate documents table).

Entirely additive and isolated: does not read from or write to faqs,
knowledge_base, or articles. Not wired into live retrieval yet — that's
Phase 2 (chat.py merging this with models.get_faqs()). Nothing here is
called by any existing request path.

Mirrors models/faqs.py's conventions closely (batched execute_values,
same null-byte cleaning, same COALESCE-preserving upsert style is not
needed here since there's no natural external unique key for arbitrary
content the way faq_id/kb_id work for FAQs — dedup instead runs through
content_hash + normalized_url, see get_by_normalized_url()).
"""
import json
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from psycopg2.extras import execute_values
from .db import get_db


# ── URL normalization (source identity) ─────────────────────────────────────
#
# Two URL strings that normalize to the same value are treated as the same
# source. This is deliberately the identity key a future crawler will also
# need — "have I seen this page before" — so it's built once, correctly,
# here rather than ad hoc later.

_URL_NOISE_PARAMS = {
    'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
    'fbclid', 'gclid', 'ref',
}


def normalize_url(url: str) -> str:
    """
    Lowercases scheme+host, strips default ports, strips a trailing slash
    (except a bare root path), strips the fragment, and strips known
    tracking-noise query params while conservatively keeping any others
    (can't safely assume every other query param is noise for an
    arbitrary client site).
    """
    url = (url or '').strip()
    if not url:
        return ''
    if not url.lower().startswith(('http://', 'https://')):
        url = 'https://' + url

    parsed = urlparse(url)
    scheme = (parsed.scheme or 'https').lower()
    netloc = parsed.netloc.lower()

    if scheme == 'http' and netloc.endswith(':80'):
        netloc = netloc[:-3]
    elif scheme == 'https' and netloc.endswith(':443'):
        netloc = netloc[:-4]

    path = parsed.path or '/'
    if len(path) > 1 and path.endswith('/'):
        path = path.rstrip('/')

    query_pairs = sorted(
        (k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True)
        if k.lower() not in _URL_NOISE_PARAMS
    )
    query = urlencode(query_pairs)

    return urlunparse((scheme, netloc, path, '', query, ''))


# ── Writes ────────────────────────────────────────────────────────────────

def save_business_knowledge_items(client_id: str, sources: list,
                                   import_batch_id: str = None) -> list:
    """
    Bulk-inserts one or more logical sources, each a list of 1+ chunk
    dicts already in chunk_index order (chunk_index set on each dict).
    A standalone source is just a 1-item list with chunk_index=0.

    Assigns a real, non-NULL source_id to EVERY row — self-referential
    (its own id) for a standalone source, shared across every chunk of a
    multi-chunk source — via a two-statement insert-then-backfill:

      1. One batched INSERT...RETURNING id for every chunk of every
         source in this call, in the exact flattened order passed in.
      2. Using the returned ids (same order): each source's
         chunk_index=0 row's id becomes that source's source_id. One
         batched UPDATE applies it to every row, including chunk 0
         itself.

    Two round trips for the WHOLE call, regardless of how many sources
    or chunks — not two per source. A single manual create (one source,
    one chunk) is just the batch-of-one case of the same mechanism.

    Each chunk dict expects: knowledge_type, source_type, title, content,
    chunk_index, and optionally category, tags, source_url,
    normalized_url, source_filename, content_hash, quality_score.

    Returns the new row ids, in the same flattened order as the input.
    """
    if not sources:
        return []

    def _clean(val) -> str:
        return str(val).replace('\x00', '').strip()

    flat = []  # (source_group_idx, chunk_index, row_tuple)
    for group_idx, chunks in enumerate(sources):
        for chunk in chunks:
            tags = chunk.get('tags', [])
            if not isinstance(tags, list):
                tags = []
            flat.append((
                group_idx,
                chunk.get('chunk_index', 0),
                (
                    client_id,
                    chunk['knowledge_type'],
                    chunk['source_type'],
                    _clean(chunk.get('title', '')),
                    _clean(chunk.get('content', '')),
                    chunk.get('category', 'General'),
                    json.dumps(tags),
                    'pending',
                    import_batch_id,
                    chunk.get('chunk_index', 0),
                    chunk.get('source_url'),
                    chunk.get('normalized_url'),
                    chunk.get('source_filename'),
                    chunk.get('content_hash'),
                    float(chunk.get('quality_score', 0.8)),
                ),
            ))

    conn, cursor = get_db()
    try:
        rows_for_insert = [r for (_, _, r) in flat]
        returned = execute_values(
            cursor,
            """INSERT INTO business_knowledge
                   (client_id, knowledge_type, source_type, title, content,
                    category, tags, embedding_status, import_batch_id,
                    chunk_index, source_url, normalized_url, source_filename,
                    content_hash, quality_score)
               VALUES %s
               RETURNING id""",
            rows_for_insert,
            template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            page_size=500,
            fetch=True,
        )
        inserted_ids = [r[0] for r in returned]

        source_id_by_group = {
            group_idx: row_id
            for (group_idx, chunk_index, _), row_id in zip(flat, inserted_ids)
            if chunk_index == 0
        }

        update_pairs = [
            (row_id, source_id_by_group[group_idx])
            for (group_idx, _, _), row_id in zip(flat, inserted_ids)
        ]
        execute_values(
            cursor,
            """UPDATE business_knowledge AS bk SET source_id = v.source_id
               FROM (VALUES %s) AS v(id, source_id)
               WHERE bk.id = v.id""",
            update_pairs,
            template="(%s, %s)",
            page_size=500,
        )

        conn.commit()
        return inserted_ids
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def delete_source_chunks(client_id: str, source_id: int) -> int:
    """
    Deletes every chunk of a source (source_id = X naturally includes the
    chunk_index=0 row itself, since it's self-referential — no special
    casing needed). Used when a URL's content_hash changes: replace
    wholesale rather than diff individual chunks.
    """
    conn, cursor = get_db()
    try:
        cursor.execute(
            "DELETE FROM business_knowledge WHERE client_id = %s AND source_id = %s",
            (client_id, source_id)
        )
        n = cursor.rowcount
        conn.commit()
        return n
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


# ── Reads ─────────────────────────────────────────────────────────────────

def get_by_normalized_url(client_id: str, normalized_url: str) -> dict:
    """
    Looks up the existing source (its chunk_index=0 row) for a given
    normalized URL, for content-hash dedup at ingest time. Returns None
    if this URL hasn't been imported before.
    """
    if not normalized_url:
        return None
    conn, cursor = get_db()
    try:
        cursor.execute(
            """SELECT id, source_id, content_hash FROM business_knowledge
               WHERE client_id = %s AND normalized_url = %s AND chunk_index = 0
               LIMIT 1""",
            (client_id, normalized_url)
        )
        row = cursor.fetchone()
        return dict(row) if row else None
    finally:
        cursor.close()
        conn.close()


def touch_last_fetched(client_id: str, source_id: int) -> None:
    """Called on a dedup hit (unchanged content) — records that a refetch
    happened without touching content/embedding at all."""
    conn, cursor = get_db()
    try:
        cursor.execute(
            """UPDATE business_knowledge SET last_fetched_at = CURRENT_TIMESTAMP
               WHERE client_id = %s AND source_id = %s""",
            (client_id, source_id)
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def get_business_knowledge(client_id: str, knowledge_type: str = None) -> list:
    """
    General read, for the manager UI and Phase 1 testing. Not called by
    any retrieval path yet — that wiring is Phase 2.
    """
    conn, cursor = get_db()
    try:
        if knowledge_type:
            cursor.execute(
                """SELECT * FROM business_knowledge
                   WHERE client_id = %s AND knowledge_type = %s AND is_active = TRUE
                   ORDER BY source_id, chunk_index""",
                (client_id, knowledge_type)
            )
        else:
            cursor.execute(
                """SELECT * FROM business_knowledge
                   WHERE client_id = %s AND is_active = TRUE
                   ORDER BY source_id, chunk_index""",
                (client_id,)
            )
        return [dict(r) for r in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()
