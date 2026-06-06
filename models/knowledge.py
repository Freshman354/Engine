"""
models/knowledge.py
-------------------
Knowledge base chunk storage, embedding storage and retrieval,
and semantic search helpers.
"""
import json
import uuid
from .db import get_db

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
            # ai_helper reads chunk.get('kb_id') — alias chunk_id so it resolves correctly
            if 'kb_id' not in r and r.get('chunk_id'):
                r['kb_id'] = r['chunk_id']
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


def store_embedding(client_id: str, chunk_id: str = None, embedding: list = None,
                    kb_id: str = None) -> None:
    """Update the embedding for a single knowledge chunk.
    Accepts chunk_id (chunk-based schema) or kb_id (kb-based schema) — tries both.
    """
    key_val = chunk_id or kb_id
    if not key_val or embedding is None:
        return
    conn, cursor = get_db()
    try:
        # Try chunk_id column first; fall back to kb_id
        updated = 0
        if chunk_id:
            cursor.execute(
                '''UPDATE knowledge_base SET embedding = %s, updated_at = CURRENT_TIMESTAMP
                   WHERE client_id = %s AND chunk_id = %s''',
                (json.dumps(embedding), client_id, chunk_id)
            )
            updated = cursor.rowcount
        if not updated and kb_id:
            cursor.execute(
                '''UPDATE knowledge_base SET embedding = %s, updated_at = CURRENT_TIMESTAMP
                   WHERE client_id = %s AND kb_id = %s''',
                (json.dumps(embedding), client_id, kb_id)
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def get_embeddings_for_client(client_id: str) -> list:
    """Return all chunk_id/kb_id + embedding pairs for a client (for batch re-indexing).
    Returns dicts with both 'chunk_id' and 'kb_id' keys so callers using either schema work.
    """
    conn, cursor = get_db()
    try:
        cursor.execute(
            """SELECT COALESCE(chunk_id, kb_id) AS cid,
                      COALESCE(kb_id, chunk_id) AS kid,
                      embedding
               FROM knowledge_base
               WHERE client_id = %s AND embedding IS NOT NULL""",
            (client_id,)
        )
        rows = cursor.fetchall()
        return [
            {
                'chunk_id':  r['cid'],
                'kb_id':     r['kid'],
                'embedding': json.loads(r['embedding']),
            }
            for r in rows
        ]
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


# store_embedding and get_embeddings_for_client are defined above (unified version
# that handles both chunk_id and kb_id column schemas).


# =====================================================================
# WEBHOOK MANAGEMENT — Agency-grade multi-webhook system
# =====================================================================

