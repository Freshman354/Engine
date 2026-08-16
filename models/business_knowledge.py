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


def get_business_knowledge_for_retrieval(client_id: str) -> list:
    """
    Business Knowledge Phase 2 — the sole integration point with the
    existing RAG pipeline (see blueprints/chat.py, where this is merged
    with models.get_faqs()). Adapts business_knowledge rows into exactly
    the dict shape models.get_faqs() already produces, so every existing
    retrieval function (embedding_search, bm25_only_search,
    find_best_match, hybrid_rerank, cross_encoder_rerank, generation)
    consumes it with zero changes to any of them:

        {id, faq_id, question, answer, category, triggers, tags,
         quality_score, embedding, last_indexed}

    plus extra, retrieval-inert metadata (knowledge_type, source_type,
    source_url, source_id, chunk_index) that no existing function reads
    but is preserved for future use (e.g. citations) without needing
    another retrieval or schema change.

    Filters to embedding_status = 'embedded' only — a pending/embedding/
    failed row is not returned here at all, regardless of whether its
    text content could still be keyword-matched. This is a deliberate
    product choice, not a technical necessity (embedding_search() would
    have gracefully skipped a missing embedding either way): a failed
    row can stay in that state indefinitely until someone retries it,
    and Business Knowledge content (shipping/return/privacy/terms) is
    judged higher-stakes to get right than a typical short FAQ. Note
    this is stricter than models.get_faqs(), which has no such filter —
    a deliberate, known divergence between the two systems, not an
    oversight.

    Returns [] if the client has no embedded Business Knowledge yet —
    the caller's list-concatenation is then a no-op, so FAQ-only
    customers see byte-identical behavior to today.
    """
    conn, cursor = get_db()
    try:
        cursor.execute(
            """SELECT * FROM business_knowledge
               WHERE client_id = %s AND is_active = TRUE
                 AND embedding_status = 'embedded'
               ORDER BY source_id, chunk_index""",
            (client_id,)
        )
        rows = [dict(r) for r in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()

    result = []
    for row in rows:
        tags_raw = row.get('tags') or '[]'
        try:
            tags = json.loads(tags_raw) if isinstance(tags_raw, str) else (tags_raw or [])
        except Exception:
            tags = []

        embedding_raw = row.get('embedding')
        if isinstance(embedding_raw, str) and embedding_raw:
            try:
                embedding = json.loads(embedding_raw)
            except Exception:
                # Defensive only — every row here has embedding_status=
                # 'embedded', so this should never actually be hit.
                embedding = []
        elif isinstance(embedding_raw, list):
            embedding = embedding_raw
        else:
            embedding = []

        row_id = str(row['id'])
        result.append({
            'id':             row_id,
            'faq_id':         row_id,
            'question':       row.get('title', ''),
            'answer':         row.get('content', ''),
            'category':       row.get('category', 'General'),
            'triggers':       [],
            'tags':           tags,
            'quality_score':  float(row.get('quality_score') or 0.0),
            'embedding':      embedding,
            'last_indexed':   row.get('last_indexed'),
            # Extra, retrieval-inert metadata — no existing retrieval
            # function reads these; preserved for future use.
            'knowledge_type': row.get('knowledge_type'),
            'source_type':    row.get('source_type'),
            'source_url':     row.get('source_url'),
            'source_id':      row.get('source_id'),
            'chunk_index':    row.get('chunk_index'),
        })
    return result


def get_business_knowledge_sources_summary(client_id: str) -> list:
    """
    One row per logical source (grouped by source_id), for a lightweight
    "what does Lumvi know about your business" display — deliberately
    NOT per-chunk, since the frontend for this phase only needs enough
    to show discoverability/acceptance, not a full chunk-level editor
    (see the Phase 3 UX plan — CRUD on individual chunks is explicitly
    out of scope for now).

    Aggregates embedding_status across a source's chunks into one
    overall status:
      'processing' — any chunk still pending/embedding
      'failed'     — no chunk embedded yet, at least one failed
      'partial'    — some embedded, some failed
      'ready'      — every chunk embedded
    """
    conn, cursor = get_db()
    try:
        cursor.execute(
            """SELECT
                   source_id,
                   MIN(knowledge_type)   AS knowledge_type,
                   MIN(title)            AS title,
                   MIN(source_type)      AS source_type,
                   MIN(source_url)       AS source_url,
                   MIN(source_filename)  AS source_filename,
                   COUNT(*)              AS chunk_count,
                   MIN(created_at)       AS created_at,
                   COUNT(*) FILTER (WHERE embedding_status = 'embedded') AS embedded_count,
                   COUNT(*) FILTER (WHERE embedding_status = 'failed')   AS failed_count,
                   COUNT(*) FILTER (WHERE embedding_status IN ('pending','embedding')) AS pending_count
               FROM business_knowledge
               WHERE client_id = %s AND is_active = TRUE
               GROUP BY source_id
               ORDER BY MIN(created_at) DESC""",
            (client_id,)
        )
        rows = [dict(r) for r in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()

    result = []
    for row in rows:
        if row['pending_count'] > 0:
            status = 'processing'
        elif row['embedded_count'] > 0 and row['failed_count'] > 0:
            status = 'partial'
        elif row['failed_count'] > 0:
            status = 'failed'
        else:
            status = 'ready'
        created_at = row.get('created_at')
        result.append({
            'source_id':       row['source_id'],
            'knowledge_type':  row['knowledge_type'],
            'title':           row['title'],
            'source_type':     row['source_type'],
            'source_url':      row['source_url'],
            'source_filename': row['source_filename'],
            'chunk_count':     row['chunk_count'],
            'created_at':      created_at.isoformat() if created_at else None,
            'status':          status,
        })
    return result


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
