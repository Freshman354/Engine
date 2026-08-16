"""
blueprints/business_knowledge.py
----------------------------------
Business Knowledge Phase 1 — ingestion for shipping_policy, return_policy,
privacy_policy, terms, about, contact, website_page. Manual create, CSV/
PDF upload, and single-page URL import (no crawling yet — Phase 3+).

Entirely additive and isolated: NOT wired into live retrieval (that's
Phase 2 — merging into chat.py's faqs_list), does not read or write
faqs, knowledge_base, or articles. Reuses Stage A's proven claim/
reclaim/retry pattern (via models/knowledge_imports.py, repointed at
business_knowledge) and the shared, process-wide Voyage rate limiter
(via the completely unmodified services.embedding.embed_batch()).

Routes
------
  GET         /api/business-knowledge                        list_business_knowledge
  POST        /api/business-knowledge                        create_business_knowledge_item
  POST        /api/business-knowledge/upload                  upload_business_knowledge
  POST        /api/business-knowledge/import-url               import_business_knowledge_url
  GET         /api/business-knowledge/upload/status/<job_id>   business_knowledge_upload_status
  POST        /api/business-knowledge/upload/retry/<job_id>    business_knowledge_upload_retry
"""
import io
import json
import uuid
import hashlib

from flask import Blueprint, request, jsonify, current_app
from flask_login import login_required, current_user

import models
from ai_helper import _bg_submit
from services.embedding import embed_batch
from blueprints.faqs import extract_pdf_text, fetch_and_extract_page_text, _find_column

business_knowledge_bp = Blueprint('business_knowledge', __name__)

_app = None
_ai_helper = None

_VALID_KNOWLEDGE_TYPES = {
    'shipping_policy', 'return_policy', 'privacy_policy', 'terms',
    'about', 'contact', 'website_page',
}
_VALID_SOURCE_TYPES = {'manual', 'url_import', 'pdf_upload', 'csv_upload'}

_TITLE_COL_ALIASES   = {'title', 'name', 'page_title', 'heading'}
_CONTENT_COL_ALIASES = {'content', 'body', 'text', 'description', 'details'}
_TYPE_COL_ALIASES    = {'knowledge_type', 'type', 'category_type'}

_CHUNK_MAX_CHARS = 1200  # matches ai_helper._split_content()'s own default


def init_business_knowledge(app, ai_helper):
    global _app, _ai_helper
    _app = app
    _ai_helper = ai_helper


def _hash_text(text: str) -> str:
    """Whitespace-collapsed before hashing so trivial formatting noise
    (extra blank lines, re-fetched page whitespace differences) doesn't
    register as changed content."""
    normalized = ' '.join((text or '').split())
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()


def _chunk_into_source(knowledge_type, source_type, title, content,
                        category='General', source_url=None,
                        normalized_url=None, source_filename=None,
                        content_hash=None):
    """
    Builds one logical source's list of chunk-dicts (chunk_index 0..N-1),
    ready for models.save_business_knowledge_items(). Chunking reuses
    ai_helper's _split_content() unchanged — paragraph-aware, byte-
    identical to the pre-existing behavior for short/single-paragraph
    text, genuinely paragraph-boundary-respecting for longer content.
    A standalone item (fits in one chunk) is just the length-1 case.
    """
    if _ai_helper:
        pieces = _ai_helper._split_content(content, max_chars=_CHUNK_MAX_CHARS)
    else:
        pieces = [content]
    if not pieces:
        pieces = [content]

    return [
        {
            'knowledge_type':  knowledge_type,
            'source_type':     source_type,
            'title':           title,
            'content':         piece,
            'category':        category,
            'chunk_index':     i,
            'source_url':      source_url,
            'normalized_url':  normalized_url,
            'source_filename': source_filename,
            'content_hash':    content_hash,
        }
        for i, piece in enumerate(pieces)
    ]


# ── Stage A pattern, reused (not reinvented) — see blueprints/faqs.py's
#    _start_faq_import_job() / _run_stage_a_embedding_job() for the
#    original this mirrors, function for function. ──────────────────────

def _start_knowledge_import_job(client_id: str, sources: list) -> dict:
    """
    sources: list of logical sources (each a list of chunk-dicts from
    _chunk_into_source()). Bulk-inserts everything now, synchronously —
    durable and fast, no AI, no embedding yet — then hands the embedding
    phase to the shared _BG_EXECUTOR. Same idempotency anchor as Stage A:
    a crash after this returns can only delay embeddings, never lose or
    duplicate records.
    """
    job_id = str(uuid.uuid4())
    total_chunks = sum(len(s) for s in sources)
    models.create_knowledge_import_job(job_id, client_id, total=total_chunks)
    models.save_business_knowledge_items(client_id, sources, import_batch_id=job_id)
    _bg_submit(_run_knowledge_embedding_job, job_id, client_id)
    return {'job_id': job_id, 'total': total_chunks}


def _run_knowledge_embedding_job(job_id: str, client_id: str) -> None:
    """
    Background embedding phase — runs on the shared _BG_EXECUTOR, not a
    raw thread. Identical mechanics to Stage A's
    _run_stage_a_embedding_job(): claim via FOR UPDATE SKIP LOCKED, batch-
    embed with embed_batch() (same shared rate limiter as FAQ uploads —
    a concurrent FAQ job and a Business Knowledge job correctly draw from
    one pacing budget with zero new code), batched write-back, resumable
    via the same stale-job reclaim mechanism.

    Deliberately does NOT call cache_utils.bump_kb_version() — nothing
    reads business_knowledge yet (Phase 2), so there's no retrieval cache
    to invalidate for it.
    """
    with _app.app_context():
        models.mark_knowledge_import_job_started(job_id)
        _PAGE = 200

        try:
            while True:
                claimed = models.claim_knowledge_for_embedding(client_id, job_id, limit=_PAGE)
                if not claimed:
                    break

                texts = [f"{c['title']} {c['content']}".strip() for c in claimed]
                vectors = embed_batch(texts, task='retrieval_document')

                updates = []
                processed_delta = 0
                failed_delta = 0
                for c, vec in zip(claimed, vectors):
                    if vec:
                        updates.append((c['id'], json.dumps(vec), 'embedded'))
                        processed_delta += 1
                    else:
                        updates.append((c['id'], None, 'failed'))
                        failed_delta += 1

                models.bulk_update_knowledge_embeddings(client_id, updates)
                models.increment_knowledge_import_progress(job_id, processed_delta, failed_delta)

                current_app.logger.info(
                    f"[BusinessKnowledge/StageA] job={job_id} client={client_id} "
                    f"batch_embedded={processed_delta} batch_failed={failed_delta}"
                )

            models.finalize_knowledge_import_job(job_id)
            current_app.logger.info(f"[BusinessKnowledge/StageA] job={job_id} finished")

        except Exception as e:
            current_app.logger.error(
                f"[BusinessKnowledge/StageA] job={job_id} client={client_id} error: {e}",
                exc_info=True,
            )


# ── Routes ────────────────────────────────────────────────────────────────

@business_knowledge_bp.route('/api/business-knowledge', methods=['GET'])
@login_required
def list_business_knowledge():
    """
    Smallest possible read endpoint for the Knowledge Base page's "what
    does Lumvi know about your business" display — one row per source
    (grouped, not per-chunk), with an aggregated status. No edit/delete;
    full chunk-level CRUD is explicitly deferred to a later phase.
    """
    client_id = request.args.get('client_id')
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    items = models.get_business_knowledge_sources_summary(client_id)
    return jsonify({'success': True, 'items': items, 'count': len(items)})


@business_knowledge_bp.route('/api/business-knowledge', methods=['POST'])
@login_required
def create_business_knowledge_item():
    """Manual create — one policy/about/contact item, typed directly."""
    try:
        data = request.get_json(silent=True) or {}
        client_id = data.get('client_id')
        if not models.verify_client_ownership(current_user.id, client_id):
            return jsonify({'success': False, 'error': 'Unauthorized'}), 403

        knowledge_type = (data.get('knowledge_type') or '').strip()
        title = (data.get('title') or '').strip()
        content = (data.get('content') or '').strip()
        category = data.get('category', 'General')

        if knowledge_type not in _VALID_KNOWLEDGE_TYPES:
            return jsonify({
                'success': False,
                'error': f'knowledge_type must be one of: {", ".join(sorted(_VALID_KNOWLEDGE_TYPES))}',
            }), 400
        if not title or not content:
            return jsonify({'success': False, 'error': 'title and content are required'}), 400

        source = _chunk_into_source(knowledge_type, 'manual', title, content, category=category)
        job = _start_knowledge_import_job(client_id, [source])

        return jsonify({
            'success':    True,
            'job_id':     job['job_id'],
            'total':      job['total'],
            'status':     'queued',
            'processing': True,
            'message':    f"Saved — embedding {job['total']} chunk(s) in the background.",
        }), 202

    except Exception as e:
        current_app.logger.error(f"[BusinessKnowledge/Create] Error: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@business_knowledge_bp.route('/api/business-knowledge/upload', methods=['POST'])
@login_required
def upload_business_knowledge():
    """CSV/Excel/PDF upload. Reuses faqs.py's extract_pdf_text() and
    _find_column() rather than duplicating parsing logic."""
    try:
        client_id = request.form.get('client_id')
        if not models.verify_client_ownership(current_user.id, client_id):
            return jsonify({'success': False, 'error': 'Unauthorized'}), 403

        knowledge_type = (request.form.get('knowledge_type') or '').strip()
        default_category = request.form.get('category', 'General')

        file = request.files.get('file')
        if not file or not file.filename:
            return jsonify({'success': False, 'error': 'No file provided'}), 400

        filename = file.filename.lower()
        sources = []

        if filename.endswith('.pdf'):
            if knowledge_type not in _VALID_KNOWLEDGE_TYPES:
                return jsonify({
                    'success': False,
                    'error': f'knowledge_type is required for PDF upload, one of: '
                             f'{", ".join(sorted(_VALID_KNOWLEDGE_TYPES))}',
                }), 400
            try:
                text = extract_pdf_text(file)
            except Exception as e:
                current_app.logger.error(f"[BusinessKnowledge/PDF] extraction error: {e}")
                return jsonify({'success': False, 'error': 'Could not read that PDF.'}), 400
            if not text.strip():
                return jsonify({'success': False, 'error': 'PDF had no readable text.'}), 400

            title = file.filename.rsplit('.', 1)[0]
            sources = [_chunk_into_source(
                knowledge_type, 'pdf_upload', title, text,
                category=default_category, source_filename=file.filename,
                content_hash=_hash_text(text),
            )]

        elif filename.endswith(('.csv', '.xlsx', '.xls')):
            import pandas as pd
            try:
                if filename.endswith('.csv'):
                    df = pd.read_csv(io.StringIO(file.stream.read().decode('utf-8')))
                else:
                    df = pd.read_excel(file)
            except Exception as e:
                current_app.logger.error(f"[BusinessKnowledge/CSV] read error: {e}")
                return jsonify({'success': False, 'error': 'Could not read that file.'}), 400

            title_col   = _find_column(df.columns, _TITLE_COL_ALIASES)
            content_col = _find_column(df.columns, _CONTENT_COL_ALIASES)
            type_col    = _find_column(df.columns, _TYPE_COL_ALIASES)

            if not title_col or not content_col:
                return jsonify({
                    'success': False,
                    'error': 'CSV/Excel needs recognizable title and content columns '
                             '(e.g. "title"/"name" and "content"/"body"/"description").',
                }), 400

            for _, row in df.iterrows():
                row_title   = str(row[title_col]).strip()
                row_content = str(row[content_col]).strip()
                if (not row_title or not row_content
                        or row_title.lower() == 'nan' or row_content.lower() == 'nan'):
                    continue
                row_type = str(row[type_col]).strip().lower() if type_col else knowledge_type
                if row_type not in _VALID_KNOWLEDGE_TYPES:
                    row_type = knowledge_type if knowledge_type in _VALID_KNOWLEDGE_TYPES else 'website_page'
                sources.append(_chunk_into_source(
                    row_type, 'csv_upload', row_title, row_content,
                    category=default_category, source_filename=file.filename,
                    content_hash=_hash_text(row_content),
                ))

            if not sources:
                return jsonify({'success': False, 'error': 'No valid rows found in that file.'}), 400
        else:
            return jsonify({
                'success': False,
                'error': 'Unsupported file type — use PDF, CSV, or Excel.',
            }), 400

        job = _start_knowledge_import_job(client_id, sources)
        return jsonify({
            'success':    True,
            'job_id':     job['job_id'],
            'total':      job['total'],
            'count':      len(sources),
            'status':     'queued',
            'processing': True,
            'message':    f"Saved {len(sources)} item(s) — embedding {job['total']} "
                           f"chunk(s) in the background.",
        }), 202

    except Exception as e:
        current_app.logger.error(f"[BusinessKnowledge/Upload] Error: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@business_knowledge_bp.route('/api/business-knowledge/import-url', methods=['POST'])
@login_required
def import_business_knowledge_url():
    """
    Single-page URL import (no crawling — one page per call). Reuses
    faqs.py's fetch_and_extract_page_text() rather than duplicating
    fetch/strip logic. Content-hash dedup: unchanged content is skipped
    entirely (only last_fetched_at is touched); changed content replaces
    the source's chunks wholesale and re-embeds.
    """
    try:
        data = request.get_json(silent=True) or {}
        client_id = data.get('client_id')
        if not models.verify_client_ownership(current_user.id, client_id):
            return jsonify({'success': False, 'error': 'Unauthorized'}), 403

        url = (data.get('url') or '').strip()
        knowledge_type = (data.get('knowledge_type') or '').strip()
        title = (data.get('title') or '').strip()
        category = data.get('category', 'General')

        if not url:
            return jsonify({'success': False, 'error': 'No URL provided'}), 400
        if knowledge_type not in _VALID_KNOWLEDGE_TYPES:
            return jsonify({
                'success': False,
                'error': f'knowledge_type must be one of: {", ".join(sorted(_VALID_KNOWLEDGE_TYPES))}',
            }), 400

        try:
            text = fetch_and_extract_page_text(url)
        except ValueError as e:
            return jsonify({'success': False, 'error': str(e)}), 400

        normalized = models.normalize_url(url)
        content_hash = _hash_text(text)

        existing = models.get_by_normalized_url(client_id, normalized)
        if existing and existing.get('content_hash') == content_hash:
            models.touch_last_fetched(client_id, existing['source_id'])
            return jsonify({
                'success':   True,
                'unchanged': True,
                'message':   'This page is unchanged since it was last imported — nothing re-embedded.',
            }), 200

        if existing:
            models.delete_source_chunks(client_id, existing['source_id'])

        if not title:
            title = url

        source = _chunk_into_source(
            knowledge_type, 'url_import', title, text, category=category,
            source_url=url, normalized_url=normalized, content_hash=content_hash,
        )
        job = _start_knowledge_import_job(client_id, [source])

        return jsonify({
            'success':    True,
            'job_id':     job['job_id'],
            'total':      job['total'],
            'status':     'queued',
            'processing': True,
            'replaced':   bool(existing),
            'message':    f"Saved — embedding {job['total']} chunk(s) in the background.",
        }), 202

    except Exception as e:
        current_app.logger.error(f"[BusinessKnowledge/URL] Error: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@business_knowledge_bp.route('/api/business-knowledge/upload/status/<job_id>', methods=['GET'])
@login_required
def business_knowledge_upload_status(job_id):
    """Mirrors faqs.py::faq_upload_status() exactly, repointed."""
    client_id = request.args.get('client_id')
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    job = models.get_knowledge_import_job(job_id, client_id)
    if not job:
        return jsonify({'success': False, 'error': 'Job not found'}), 404

    if job['status'] == 'processing':
        if models.reclaim_stale_knowledge_import_job(job_id, stale_minutes=10):
            _bg_submit(_run_knowledge_embedding_job, job_id, client_id)
            current_app.logger.warning(
                f"[BusinessKnowledge/StageA] job={job_id} appeared stale on poll — resubmitted"
            )

    return jsonify({
        'success':      True,
        'job_id':       job['job_id'],
        'status':       job['status'],
        'total':        job['total'],
        'processed':    job['processed'],
        'failed':       job['failed'],
        'created_at':   job['created_at'].isoformat()   if job['created_at']   else None,
        'updated_at':   job['updated_at'].isoformat()   if job['updated_at']   else None,
        'completed_at': job['completed_at'].isoformat() if job['completed_at'] else None,
    })


@business_knowledge_bp.route('/api/business-knowledge/upload/retry/<job_id>', methods=['POST'])
@login_required
def business_knowledge_upload_retry(job_id):
    """Mirrors faqs.py::faq_upload_retry() exactly, repointed."""
    client_id = request.args.get('client_id')
    if not models.verify_client_ownership(current_user.id, client_id):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    job = models.get_knowledge_import_job(job_id, client_id)
    if not job:
        return jsonify({'success': False, 'error': 'Job not found'}), 404

    reset_count = models.retry_failed_knowledge_embeddings(job_id, client_id)
    if reset_count:
        _bg_submit(_run_knowledge_embedding_job, job_id, client_id)
        current_app.logger.info(
            f"[BusinessKnowledge/StageA] job={job_id} retry: "
            f"{reset_count} failed rows reset, resubmitted"
        )

    return jsonify({
        'success': True,
        'job_id':  job_id,
        'retried': reset_count,
        'message': (
            f"Retrying {reset_count} failed item(s)." if reset_count
            else "Nothing to retry — no failed items for this job."
        ),
    })
