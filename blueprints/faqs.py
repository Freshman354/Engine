"""
blueprints/faqs.py
------------------
FAQ management, knowledge-base upload, URL import, article management,
and the system-to-system webhook FAQ import endpoint.

Extracted from app.py. All behaviour is identical to the original;
nothing has been changed except:
  - Route registration: Blueprint vs app
  - app.logger  → current_app.logger
  - Inline stdlib imports promoted to module level
  - app.app_context() in background thread uses injected _app reference
  - Dependencies injected at registration time via init_faqs()

Routes
------
  GET         /api/articles                       get_articles
  GET/POST/
  PUT/DELETE  /api/articles/manage                manage_articles
  GET         /faq-manager                        faq_manager_page
  GET         /article-manager                    article_manager_page
  GET/POST    /api/faqs                            manage_faqs
  POST        /api/faqs/delete-all                delete_all_faqs
  POST        /api/faq/upload                     upload_faqs
  POST        /api/faq/import-url                 import_faqs_from_url
  POST        /api/webhook/faq-import             webhook_faq_import

Registration in app.py:
  from blueprints.faqs import faqs_bp, init_faqs
  init_faqs(
      app=app,
      plan_limits=PLAN_LIMITS,
      ai_helper=ai_helper,
      extract_keywords=extract_keywords,
  )
  app.register_blueprint(faqs_bp)
"""

import hmac
import html as _html
import io
import json
import os
import re
import threading
import traceback
import urllib.error
import urllib.request
import uuid

from flask import Blueprint, jsonify, redirect, render_template, request, current_app, url_for
from flask_login import current_user, login_required

import cache_utils
import models

# ── Blueprint ────────────────────────────────────────────────────────────────

faqs_bp = Blueprint('faqs', __name__)

# Injected dependencies — populated by init_faqs() before first request.
_app              = None   # needed only to push context in background thread
_plan_limits      = None
_ai_helper        = None
_extract_keywords = None


def init_faqs(app, plan_limits, ai_helper, extract_keywords):
    """
    Called once in app.py after all shared objects are ready.
    Must be called before the first request reaches this blueprint.
    """
    global _app, _plan_limits, _ai_helper, _extract_keywords
    _app              = app
    _plan_limits      = plan_limits
    _ai_helper        = ai_helper
    _extract_keywords = extract_keywords


# ── Background helpers ───────────────────────────────────────────────────────

def _save_legacy_faqs(client_id: str, chunks: list):
    """Insert enriched chunks into the legacy faqs table (backward compat)."""
    conn, cursor = models.get_db()
    saved = 0
    try:
        for chunk in chunks:
            faq_id = str(uuid.uuid4())
            cursor.execute(
                '''INSERT INTO faqs (client_id, faq_id, question, answer, category, triggers)
                   VALUES (%s, %s, %s, %s, %s, %s)''',
                (
                    client_id, faq_id,
                    chunk['title'],
                    chunk['content'],
                    chunk.get('category', 'General'),
                    json.dumps(chunk.get('tags', []))
                )
            )
            saved += 1
        conn.commit()
    except Exception as _e:
        current_app.logger.warning(
            f"[Upload/BG] Legacy FAQ save error (non-critical): {_e}"
        )
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
    return saved


def _bg_enrich_and_save(client_id: str, valid_faqs: list):
    """
    Background worker: enrich → chunk → embed → save.
    Runs in a daemon thread so the HTTP response is returned immediately.
    The entire pipeline (Gemini enrichment + per-item embedding) can take
    30-120 seconds for a large file — it must never block the request cycle.
    """
    with _app.app_context():
        try:
            current_app.logger.info(
                f"[Upload/BG] starting enrich for client={client_id} items={len(valid_faqs)}"
            )
            if _ai_helper and _ai_helper.enabled:
                chunks = _ai_helper.enrich_and_chunk(valid_faqs, client_id)
            else:
                chunks = [
                    {
                        'kb_id':     str(uuid.uuid4()),
                        'title':     item['question'],
                        'content':   item['answer'],
                        'type':      'faq',
                        'category':  item.get('category', 'General'),
                        'tags':      item.get('tags', []),
                        'embedding': [],
                        'metadata':  {'source': 'upload'},
                        'quality':   item.get('quality_score', 0.75),
                    }
                    for item in valid_faqs
                ]

            if not chunks:
                current_app.logger.warning(
                    f"[Upload/BG] enrich returned 0 chunks for client={client_id}"
                )
                return

            kb_saved  = models.save_knowledge_chunks(client_id, chunks)
            faq_saved = _save_legacy_faqs(client_id, chunks)
            cache_utils.bump_kb_version(client_id)
            current_app.logger.info(
                f"[Upload/BG] done client={client_id} kb_saved={kb_saved} faq_saved={faq_saved}"
            )
        except Exception as e:
            current_app.logger.error(
                f"[Upload/BG] error for client={client_id}: {e}", exc_info=True
            )


# ── File parsing helpers ─────────────────────────────────────────────────────

# Character budget for AI-based extraction (PDF text, URL page text, or a
# rendered spreadsheet table). Chunked rather than a single hard truncation —
# see extract_faqs_from_text() below for why.
_EXTRACTION_CHUNK_SIZE   = 12000
_MAX_EXTRACTION_CHUNKS   = 8   # bounds worst case at ~8 sequential Gemini
                               # calls (~96,000 chars, roughly a 40-60 page
                               # document) so this stays a synchronous, fast
                               # request path rather than reintroducing the
                               # multi-minute hang enrich_and_chunk() was
                               # already backgrounded to avoid (see
                               # upload_faqs()'s docstring). A document
                               # longer than that still gets truncated, but
                               # honestly — see the `truncated` flag threaded
                               # through every caller below.

_QUESTION_COL_ALIASES = {'question', 'q', 'faq_question', 'title', 'topic'}
_ANSWER_COL_ALIASES   = {'answer', 'a', 'response', 'description', 'details', 'content', 'body'}


def _find_column(columns, aliases):
    """Case-insensitive match of a dataframe's columns against a set of
    acceptable aliases. Returns the real column name (original case
    preserved, needed to index the dataframe) or None."""
    lower_map = {str(c).lower().strip(): c for c in columns}
    for alias in aliases:
        if alias in lower_map:
            return lower_map[alias]
    return None


def _dataframe_to_faqs(df):
    """
    Try to build FAQs directly from recognisable question/answer-shaped
    columns (case-insensitive, common aliases — not just an exact
    'question'/'answer' match). Returns (faqs, matched) — matched=False
    means no recognisable columns were found and the caller should fall
    back to AI extraction on the rendered table instead.
    """
    q_col = _find_column(df.columns, _QUESTION_COL_ALIASES)
    a_col = _find_column(df.columns, _ANSWER_COL_ALIASES)
    if not q_col or not a_col:
        return [], False

    faqs = []
    for _, row in df.iterrows():
        question = str(row[q_col]).strip()
        answer   = str(row[a_col]).strip()
        if question and answer and question.lower() != 'nan' and answer.lower() != 'nan':
            faqs.append({
                'question': question,
                'answer':   answer,
                'category': str(row.get('category', 'General')).strip(),
                'triggers': _extract_keywords(question),
            })
    return faqs, True


def _dataframe_to_text(df, max_rows=200):
    """
    Render a dataframe as plain text for AI-based FAQ extraction when it
    doesn't have recognisable question/answer columns — e.g. a pricing
    sheet (Service, Price) or product catalog (SKU, Name, Description).
    Capped at max_rows to keep the rendered text (and therefore the
    chunking/extraction cost below) bounded for very large spreadsheets.
    """
    rows = df.head(max_rows)
    lines = []
    for _, row in rows.iterrows():
        parts = [
            f"{col}: {row[col]}" for col in df.columns
            if str(row[col]).strip().lower() != 'nan'
        ]
        if parts:
            lines.append(', '.join(parts))
    return '\n'.join(lines)


def _process_dataframe(df):
    """
    Shared CSV/Excel processing. Returns (faqs, truncated).

    FIX: previously required columns named EXACTLY 'question' and 'answer'
    (case-sensitive) — any other shape (a pricing sheet with 'Service'/
    'Price', a catalog with 'SKU'/'Description') silently produced zero
    rows, surfacing only a generic "check the format" error with no
    indication of what was actually expected. Now: (1) tries flexible,
    case-insensitive column matching against common aliases first — fast,
    no AI call needed; (2) if that finds nothing and AI is available,
    renders the table as text and runs it through the same AI extraction
    used for PDFs, so a spreadsheet that isn't already shaped as Q&A can
    still produce sensible FAQs instead of an empty result.
    """
    faqs, matched = _dataframe_to_faqs(df)
    if matched:
        return faqs, False

    if _ai_helper and _ai_helper.enabled and _ai_helper.model:
        table_text = _dataframe_to_text(df)
        if table_text.strip():
            return extract_faqs_from_text(table_text)

    return [], False


def process_csv_upload(file):
    import pandas as pd
    try:
        df = pd.read_csv(io.StringIO(file.stream.read().decode('utf-8')))
        return _process_dataframe(df)
    except Exception as e:
        current_app.logger.error(f'Error processing CSV: {e}')
        return [], False


def process_excel_upload(file):
    import pandas as pd
    try:
        df = pd.read_excel(file)
        return _process_dataframe(df)
    except Exception as e:
        current_app.logger.error(f'Error processing Excel: {e}')
        return [], False


def process_pdf_upload(file):
    """Returns (faqs, truncated) — see extract_faqs_from_text()."""
    import PyPDF2
    try:
        pdf_reader = PyPDF2.PdfReader(io.BytesIO(file.read()))
        text = ""
        for page in pdf_reader.pages:
            text += page.extract_text() + "\n"
        if _ai_helper and _ai_helper.enabled:
            return extract_faqs_from_text(text)
        else:
            return parse_structured_faq_text(text), False
    except Exception as e:
        current_app.logger.error(f'Error processing PDF: {e}')
        return [], False


def _chunk_text(text, chunk_size=_EXTRACTION_CHUNK_SIZE, max_chunks=_MAX_EXTRACTION_CHUNKS):
    """
    Split text into up to max_chunks pieces of at most chunk_size
    characters, breaking on a paragraph or sentence boundary near each cut
    point where possible rather than mid-sentence — a question/answer pair
    split across two chunks is much more likely to extract correctly if
    the cut lands between sentences.
    """
    if len(text) <= chunk_size:
        return [text]
    chunks = []
    start = 0
    while start < len(text) and len(chunks) < max_chunks:
        end = start + chunk_size
        if end >= len(text):
            chunks.append(text[start:])
            break
        break_point = text.rfind('\n\n', start, end)
        if break_point <= start:
            break_point = text.rfind('. ', start, end)
        if break_point <= start:
            break_point = end
        else:
            break_point += 1  # keep the newline/period with the preceding chunk
        chunks.append(text[start:break_point])
        start = break_point
    return chunks


def extract_faqs_from_text(text):
    """
    Extract FAQ pairs from arbitrary text using AI. Returns (faqs, truncated).

    FIX: this used to make a single call on text[:3000] — roughly one
    page. Anything longer (a multi-page policy document, a full product
    catalog, a real FAQ page with 20+ questions) had the majority of its
    content silently dropped, with zero indication to the user that only
    a fraction was ever processed. Now chunks the text and extracts from
    each chunk, merging and deduplicating results — covers up to
    _MAX_EXTRACTION_CHUNKS * _EXTRACTION_CHUNK_SIZE characters (~96,000,
    roughly a 40-60 page document) instead of ~3,000. Still bounded, not
    unlimited/backgrounded — see the constants above for why — so a
    document long enough to exceed even that gets `truncated=True` back,
    which every caller now threads through to the user instead of staying
    silent about it.
    """
    if not _ai_helper or not _ai_helper.enabled or not _ai_helper.model:
        return parse_structured_faq_text(text), False

    chunks    = _chunk_text(text)
    truncated = len(text) > _EXTRACTION_CHUNK_SIZE * _MAX_EXTRACTION_CHUNKS

    from utils import generate as _generate
    all_faqs = []
    seen_questions = set()

    for chunk in chunks:
        try:
            prompt = f"""Extract FAQ pairs from this text. Return a JSON array of objects with 'question' and 'answer' fields.
Only extract genuine question/answer content — skip navigation text, headers, or unrelated boilerplate.

Text:
{chunk}

Return ONLY valid JSON array like:
[
  {{"question": "What are your hours?", "answer": "We're open 9-5 Monday-Friday"}},
  {{"question": "How much does it cost?", "answer": "$49 per month"}}
]
"""
            response = _generate(_ai_helper.model, prompt, _ai_helper.model_name)
            json_match = re.search(r'\[.*\]', response.text, re.DOTALL)
            if not json_match:
                continue
            faqs_data = json.loads(json_match.group())
            for faq in faqs_data:
                question = str(faq.get('question', '')).strip()
                answer   = str(faq.get('answer', '')).strip()
                if not question or not answer:
                    continue
                dedup_key = question.lower()
                if dedup_key in seen_questions:
                    continue  # a question can legitimately repeat right at a chunk boundary
                seen_questions.add(dedup_key)
                all_faqs.append({
                    'question': question,
                    'answer':   answer,
                    'category': 'Imported',
                    'triggers': _extract_keywords(question),
                })
        except Exception as e:
            current_app.logger.error(f'Error extracting FAQs with AI (chunk): {e}')
            continue  # one bad chunk shouldn't take down the whole extraction

    return all_faqs, truncated


def parse_structured_faq_text(text):
    faqs = []
    lines = text.split('\n')
    current_q = None
    current_a = None

    for line in lines:
        line = line.strip()
        if line.startswith(('Q:', 'Question:', 'q:', 'question:')):
            if current_q and current_a:
                faqs.append({
                    'question': current_q,
                    'answer':   current_a,
                    'category': 'Imported',
                    'triggers': _extract_keywords(current_q),
                })
            current_q = line.split(':', 1)[1].strip()
            current_a = None
        elif line.startswith(('A:', 'Answer:', 'a:', 'answer:')):
            current_a = line.split(':', 1)[1].strip()

    if current_q and current_a:
        faqs.append({
            'question': current_q,
            'answer':   current_a,
            'category': 'Imported',
            'triggers': _extract_keywords(current_q),
        })

    return faqs


# ── Routes ───────────────────────────────────────────────────────────────────

@faqs_bp.route('/api/articles', methods=['GET'])
def get_articles():
    """Public endpoint — used by chat widget to load articles."""
    client_id = request.args.get('client_id')
    if not client_id:
        return jsonify({'success': False, 'error': 'client_id required'}), 400
    articles = models.get_articles(client_id)
    return jsonify({'success': True, 'articles': articles})


@faqs_bp.route('/api/articles/manage', methods=['GET', 'POST', 'PUT', 'DELETE'])
@login_required
def manage_articles():
    try:
        if request.method == 'GET':
            client_id = request.args.get('client_id')
            if not client_id or not models.verify_client_ownership(current_user.id, client_id):
                return jsonify({'success': False, 'error': 'Unauthorized'}), 403
            articles = models.get_articles(client_id)
            return jsonify({'success': True, 'articles': articles})

        data      = request.get_json()
        client_id = data.get('client_id')
        if not client_id or not models.verify_client_ownership(current_user.id, client_id):
            return jsonify({'success': False, 'error': 'Unauthorized'}), 403

        if request.method == 'POST':
            title    = data.get('title', '').strip()
            content  = data.get('content', '').strip()
            category = data.get('category', 'General').strip()
            if not title or not content:
                return jsonify({'success': False, 'error': 'Title and content are required'}), 400
            article_id = models.create_article(client_id, title, content, category)
            return jsonify({'success': True, 'id': article_id})

        if request.method == 'PUT':
            article_id = data.get('id')
            title    = data.get('title', '').strip()
            content  = data.get('content', '').strip()
            category = data.get('category', 'General').strip()
            if not article_id or not title or not content:
                return jsonify({'success': False, 'error': 'id, title and content are required'}), 400
            models.update_article(article_id, client_id, title, content, category)
            return jsonify({'success': True})

        if request.method == 'DELETE':
            article_id = data.get('id')
            if not article_id:
                return jsonify({'success': False, 'error': 'id required'}), 400
            models.delete_article(article_id, client_id)
            return jsonify({'success': True})

    except Exception as e:
        current_app.logger.error(f'Articles error: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500


@faqs_bp.route('/faq-manager')
@login_required
def faq_manager_page():
    client_id = request.args.get('client_id')
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return "Unauthorized", 403
    return render_template('faq-manager.html')


@faqs_bp.route('/article-manager')
@login_required
def article_manager_page():
    """Help Center article manager — create, edit and delete articles per client."""
    client_id = request.args.get('client_id')
    if not client_id or not models.verify_client_ownership(current_user.id, client_id):
        return "Unauthorized", 403

    client     = models.get_client_by_id(client_id)
    fresh_user = models.get_user_by_id(current_user.id)
    plan_type  = (fresh_user or {}).get('plan_type', current_user.plan_type)

    return render_template(
        'article-manager.html',
        client_id  = client_id,
        client     = client,
        plan_type  = plan_type,
        user       = current_user,
    )


@faqs_bp.route('/api/faqs', methods=['GET', 'POST'])
@login_required
def manage_faqs():
    try:
        if request.method == 'GET':
            client_id = request.args.get('client_id')
        else:
            if request.is_json:
                client_id = request.json.get('client_id')
            else:
                client_id = request.form.get('client_id')

        if not client_id:
            return jsonify({'success': False, 'error': 'Client ID is required'}), 400

        if not models.verify_client_ownership(current_user.id, client_id):
            return jsonify({'success': False, 'error': 'Unauthorized'}), 403

        if request.method == 'GET':
            try:
                faqs = models.get_faqs(client_id)
                return jsonify({'success': True, 'faqs': faqs})
            except Exception as e:
                current_app.logger.error(f'Error loading FAQs: {e}')
                return jsonify({'success': True, 'faqs': []})

        elif request.method == 'POST':
            if not request.is_json:
                return jsonify({'success': False, 'error': 'Request must be JSON'}), 400

            faqs_list   = request.json.get('faqs', [])
            user        = models.get_user_by_id(current_user.id)
            plan_limits = _plan_limits.get(user['plan_type'], _plan_limits['free'])
            max_faqs    = plan_limits['faqs_per_client']

            if len(faqs_list) > max_faqs:
                return jsonify({
                    'success': False,
                    'error': (
                        f'Plan limit: Maximum {max_faqs} FAQs allowed '
                        f'on {user["plan_type"]} plan'
                    ),
                    'upgrade_required': True,
                }), 403

            models.save_faqs(client_id, faqs_list)
            cache_utils.bump_kb_version(client_id)
            current_app.logger.info(
                f"[Cache] KB invalidated after FAQ save: client={client_id}"
            )

            # Record each saved FAQ as a correction training sample —
            # human-curated knowledge edits are the highest-quality signal.
            if client_id != 'demo':
                try:
                    from training_collector import collect_correction
                    vertical = json.loads(
                        models.get_client_by_id(client_id).get('branding_settings') or '{}'
                    ).get('vertical', 'general')
                    for faq in faqs_list[:50]:  # cap at 50 per save to avoid burst writes
                        q = (faq.get('question') or '').strip()
                        a = (faq.get('answer')   or '').strip()
                        if q and a:
                            collect_correction(
                                client_id        = client_id,
                                session_id       = '',
                                original_message = q,
                                bad_response     = '',
                                correct_response = a,
                                corrected_by     = f'user:{current_user.id}',
                                vertical         = vertical,
                            )
                except Exception as _tc_err:
                    current_app.logger.debug(
                        f'[TrainingCollector] FAQ correction error: {_tc_err}'
                    )

            # Re-index embeddings for semantic search (non-blocking)
            if _ai_helper and _ai_helper.enabled:
                try:
                    _ai_helper.index_faqs(faqs_list, client_id)
                except Exception as _idx_err:
                    current_app.logger.warning(
                        f"[index_faqs] non-critical error: {_idx_err}"
                    )

            return jsonify({'success': True, 'message': 'FAQs updated successfully'})

    except Exception as e:
        current_app.logger.error(f'Error managing FAQs: {e}')
        traceback.print_exc()
        return jsonify({'success': False, 'error': 'Failed to manage FAQs'}), 500


@faqs_bp.route('/api/faqs/delete-all', methods=['POST'])
@login_required
def delete_all_faqs():
    """Delete all FAQs for a client — called by the FAQ Manager Delete All button."""
    try:
        data      = request.get_json()
        client_id = data.get('client_id') if data else None

        if not client_id:
            return jsonify({'success': False, 'error': 'Client ID required'}), 400

        if not models.verify_client_ownership(current_user.id, client_id):
            return jsonify({'success': False, 'error': 'Unauthorized'}), 403

        if hasattr(models, 'delete_all_faqs'):
            models.delete_all_faqs(client_id)
        else:
            current_app.logger.warning(
                "[delete_all_faqs] models.delete_all_faqs not found — using direct SQL fallback"
            )

        # Always delete from both tables directly to guarantee clean state
        try:
            conn, cursor = models.get_db()
            cursor.execute('DELETE FROM faqs WHERE client_id = %s', (client_id,))
            cursor.execute('DELETE FROM knowledge_base WHERE client_id = %s', (client_id,))
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as _del_err:
            current_app.logger.error(
                f"[delete_all_faqs] Direct SQL delete failed: {_del_err}"
            )

        cache_utils.bump_kb_version(client_id)
        current_app.logger.info(
            f'[Cache] KB invalidated after delete-all: client={client_id}'
        )
        current_app.logger.info(
            f'All FAQs deleted for client {client_id} by user {current_user.id}'
        )
        return jsonify({'success': True, 'message': 'All FAQs deleted successfully'})

    except Exception as e:
        current_app.logger.error(f'Error deleting all FAQs: {e}')
        return jsonify({'success': False, 'error': 'Failed to delete FAQs'}), 500


@faqs_bp.route('/api/faq/upload', methods=['POST'])
@login_required
def upload_faqs():
    """
    Smart upload pipeline:
      1. Parse file (CSV / Excel / PDF)      — synchronous, fast
      2. Validate + basic enrichment         — synchronous, fast
      3. AI enrichment + embed + save        — BACKGROUND THREAD
         (enrich_and_chunk makes 100s of Gemini calls for large files;
          running it synchronously caused the 3-5 minute hang)
    """
    try:
        client_id = request.form.get('client_id')
        if not models.verify_client_ownership(current_user.id, client_id):
            return jsonify({'success': False, 'error': 'Unauthorized'}), 403

        if 'file' not in request.files:
            return jsonify({'success': False, 'error': 'No file uploaded'}), 400

        file = request.files['file']
        if not file.filename:
            return jsonify({'success': False, 'error': 'No file selected'}), 400

        filename = file.filename.lower()

        if filename.endswith('.csv'):
            raw_items, truncated = process_csv_upload(file)
        elif filename.endswith(('.xlsx', '.xls')):
            raw_items, truncated = process_excel_upload(file)
        elif filename.endswith('.pdf'):
            raw_items, truncated = process_pdf_upload(file)
        else:
            return jsonify({
                'success': False,
                'error': 'Unsupported file type. Upload CSV, Excel, or PDF.',
            }), 400

        if not raw_items:
            return jsonify({
                'success': False,
                'error': 'No content found in file. Check the format.',
            }), 400

        current_app.logger.info(
            f"[Upload] client={client_id} raw_items={len(raw_items)} file={filename}"
            + (" truncated=True" if truncated else "")
        )

        valid_faqs, errors = models.validate_and_enrich_faqs(raw_items, client_id)

        if errors:
            current_app.logger.info(
                f"[Upload] client={client_id} skipped={len(errors)} errors: "
                + "; ".join(f"row {e['row']}: {e['reason']}" for e in errors[:5])
            )

        if not valid_faqs:
            return jsonify({
                'success': False,
                'error': 'No valid content to import after validation.',
                'validation_errors': errors[:10],
            }), 400

        t = threading.Thread(
            target=_bg_enrich_and_save,
            args=(client_id, valid_faqs),
            daemon=True,
        )
        t.start()

        response = {
            'success':    True,
            'message':    (
                f'Processing {len(valid_faqs)} items — your knowledge base will be '
                'ready in about 30–60 seconds. Refresh the FAQ Manager to see them.'
            ),
            'count':      len(valid_faqs),
            'processing': True,
        }
        # FIX: extraction used to hard-truncate to ~3,000 characters with
        # zero indication to the user — "count" just quietly reflected
        # whatever fraction of a large file got processed. Now honest
        # about it when a file was big enough to actually hit the (much
        # higher) chunking cap.
        if truncated:
            response['truncated'] = True
            response['warning'] = (
                'This file is large — only the first part was processed '
                f'({len(valid_faqs)} items found so far). For complete '
                'coverage, consider splitting it into smaller files.'
            )
        if errors:
            response['skipped']           = len(errors)
            response['validation_errors'] = errors[:10]
        return jsonify(response)

    except Exception as e:
        current_app.logger.error(f"[Upload] Error: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@faqs_bp.route('/api/faq/import-url', methods=['POST'])
@login_required
def import_faqs_from_url():
    """
    Fetch a webpage by URL, extract visible text, then use AI to parse
    Q&A pairs — same enrichment pipeline as PDF/CSV uploads.
    """
    try:
        data      = request.get_json(silent=True) or {}
        client_id = data.get('client_id')
        url       = (data.get('url') or '').strip()

        if not models.verify_client_ownership(current_user.id, client_id):
            return jsonify({'success': False, 'error': 'Unauthorized'}), 403

        if not url:
            return jsonify({'success': False, 'error': 'No URL provided'}), 400

        if not re.match(r'^https?://', url):
            url = 'https://' + url

        # ── Fetch the page ────────────────────────────────────────────
        try:
            req = urllib.request.Request(
                url,
                headers={'User-Agent': 'Mozilla/5.0 (compatible; LumviBot/1.0)'},
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                raw_bytes = resp.read(500_000)   # cap at 500 KB
        except urllib.error.HTTPError as e:
            return jsonify({
                'success': False,
                'error': f'Could not fetch URL: HTTP {e.code}',
            }), 400
        except Exception as e:
            return jsonify({
                'success': False,
                'error': f'Could not fetch URL: {e}',
            }), 400

        # ── Strip HTML tags → plain text ──────────────────────────────
        try:
            html_text = raw_bytes.decode('utf-8', errors='replace')
        except Exception:
            html_text = raw_bytes.decode('latin-1', errors='replace')

        html_text = re.sub(
            r'(?is)<(script|style|nav|footer|header)[^>]*>.*?</\1>', ' ', html_text
        )
        html_text = re.sub(r'<[^>]+>', ' ', html_text)
        html_text = _html.unescape(html_text)
        html_text = re.sub(r'[ \t]{2,}', ' ', html_text)
        html_text = re.sub(r'\n{3,}', '\n\n', html_text).strip()

        if len(html_text) < 50:
            return jsonify({
                'success': False,
                'error': 'Page had no readable text content.',
            }), 400

        # FIX: was extract_faqs_from_text(html_text[:6000]) — redundant AND
        # counterproductive now that extract_faqs_from_text() does its own
        # chunking internally (it used to re-truncate to 3,000 chars
        # regardless of what was passed in, making this pre-truncation
        # pointless; now it would just needlessly cut off content before
        # chunking ever got a chance to cover more of it). Pass the full
        # page text through.
        raw_items, truncated = extract_faqs_from_text(html_text)

        if not raw_items:
            return jsonify({
                'success': False,
                'error': 'No FAQ pairs found on that page. Try a dedicated FAQ/Help page URL.',
            }), 400

        current_app.logger.info(
            f"[ImportURL] client={client_id} url={url} raw={len(raw_items)}"
            + (" truncated=True" if truncated else "")
        )

        valid_faqs, errors = models.validate_and_enrich_faqs(raw_items, client_id)

        if not valid_faqs:
            return jsonify({
                'success': False,
                'error': 'All extracted items failed validation (duplicates or missing fields).',
                'validation_errors': errors[:10],
            }), 400

        t = threading.Thread(
            target=_bg_enrich_and_save,
            args=(client_id, valid_faqs),
            daemon=True,
        )
        t.start()

        response = {
            'success':    True,
            'message':    (
                f'Found {len(valid_faqs)} FAQ{"s" if len(valid_faqs) != 1 else ""} on that page — '
                'your knowledge base will be ready in about 30–60 seconds.'
            ),
            'count':      len(valid_faqs),
            'processing': True,
        }
        # FIX: same honesty fix as upload_faqs() — a large FAQ page used to
        # silently lose most of its content to the old 3,000-char cap.
        if truncated:
            response['truncated'] = True
            response['warning'] = (
                'This page has a lot of content — only the first part was '
                f'processed ({len(valid_faqs)} items found so far).'
            )
        if errors:
            response['skipped']           = len(errors)
            response['validation_errors'] = errors[:10]
        return jsonify(response)

    except Exception as e:
        current_app.logger.error(f'[ImportURL] Error: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@faqs_bp.route('/api/webhook/faq-import', methods=['POST'])
def webhook_faq_import():
    try:
        # Fail closed if WEBHOOK_SECRET not configured (APP-BUG-04 fix)
        _wh_secret = os.environ.get('WEBHOOK_SECRET', '').strip()
        if not _wh_secret:
            return jsonify({'error': 'Webhook not configured'}), 503
        _provided = request.headers.get('X-Webhook-Secret', '')
        if not hmac.compare_digest(_provided, _wh_secret):
            return jsonify({'error': 'Unauthorized'}), 401

        data          = request.json or {}
        client_id     = data.get('client_id')
        incoming_faqs = data.get('faqs', [])

        if not client_id or not incoming_faqs:
            return jsonify({'error': 'client_id and faqs required'}), 400

        conn = cursor = None
        saved = 0
        try:
            conn, cursor = models.get_db()
            for faq in incoming_faqs:
                question = faq.get('question', '').strip()
                answer   = faq.get('answer', '').strip()
                if not question or not answer:
                    continue
                triggers = _extract_keywords(question)
                cursor.execute(
                    '''
                    INSERT INTO faqs (client_id, faq_id, question, answer, category, triggers)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ''',
                    (
                        client_id,
                        str(uuid.uuid4()),
                        question,
                        answer,
                        faq.get('category', 'General') if isinstance(faq, dict) else 'General',
                        json.dumps(triggers)
                    )
                )
                saved += 1
            conn.commit()
        except Exception as _db_err:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise _db_err
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

        cache_utils.bump_kb_version(client_id)
        current_app.logger.info(
            f"[Cache] KB invalidated after webhook FAQ import: client={client_id}"
        )
        return jsonify({
            'success': True,
            'message': f'Imported {saved} FAQs successfully',
            'count':   saved,
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500


