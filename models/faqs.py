"""
models/faqs.py
--------------
FAQ storage and retrieval, validation and enrichment pipeline,
keyword extraction helpers, and weekly digest client queries.
"""
import json
import re
import uuid
from datetime import datetime
from .db import get_db

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

            # Strip null bytes (0x00) that arrive from PDF/binary uploads
            # and cause "ValueError: A string literal cannot contain NUL characters"
            def _clean(val: str) -> str:
                return str(val).replace('\x00', '').strip()

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
                    _clean(faq.get('question', '')),
                    _clean(faq.get('answer', '')),
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

