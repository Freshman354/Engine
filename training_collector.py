"""
training_collector.py — System 2: Training Data Collection
===========================================================

Records every conversation turn, tool call, agent correction, escalation,
and user rating to the `training_samples` table in PostgreSQL.

Goal: build a fine-tuning dataset for a future local LLM (Llama / Mistral).
Export format: Alpaca JSONL (see training_exporter.py).

Design rules (match models.py exactly):
  - get_db() → (_PooledConn, RealDictCursor)
  - cursor.close() then conn.close() inside finally — always
  - conn.rollback() inside except before finally — on writes
  - Migration: try/except/finally, IF NOT EXISTS, SAVEPOINT for constraints
  - print() for migration output — same as all other migrate_* functions
  - FK: training_samples references clients(client_id)
  - Never raises from public API — all exceptions caught and logged
  - Fire-and-forget writers use threading.Thread(daemon=True) — same as
    app.py's notify_webhook — they never block the HTTP response

Sample types recorded:
  conversation  — every normal chat turn (user_message → bot_response)
  tool_call     — tool name + args + result dict from tools.py
  correction    — human agent edits the bot's answer (label for fine-tuning)
  escalation    — conversation handed off to human inbox
  rating        — user thumbs-up / thumbs-down on a bot response

Wire-in points in app.py (System 2 changes, done after this file is confirmed):
  /api/chat          → collect_conversation_turn() in background thread
  /api/chat/rate     → collect_user_rating()       in background thread
  /api/admin/inbox/* → collect_correction()         in background thread
"""

import json
import logging
import re
import threading
import uuid
from datetime import datetime

import models  # identical import to app.py and tools.py

logger = logging.getLogger(__name__)


# =====================================================================
# INPUT SANITISATION
# Mirrors sanitize_input() in app.py and _sanitize() in tools.py.
# Kept local — no circular import on app.py.
# =====================================================================

def _sanitize(text, max_length=2000):
    """Strip HTML tags, collapse whitespace, truncate. Returns '' for non-strings."""
    if not text or not isinstance(text, str):
        return ''
    text = re.sub(r'<[^>]+>', '', text)
    text = text[:max_length]
    text = ' '.join(text.split())
    return text.strip()


def _safe_json(obj, max_length=8000):
    """
    Serialise obj to a JSON string, truncated to max_length.
    Returns '{}' on any failure — never raises.
    """
    try:
        s = json.dumps(obj, default=str)
        return s[:max_length]
    except Exception:
        return '{}'


# =====================================================================
# DB MIGRATION
# Follows the exact pattern of migrate_kb_gaps() and
# migrate_faq_to_knowledge_base() in models.py:
#   - try/except/finally
#   - IF NOT EXISTS on CREATE TABLE
#   - SAVEPOINT for the UNIQUE constraint
#   - print() for success/warning output
#
# Wire into app.py startup block after migrate_agent_tables():
#
#     try:
#         from training_collector import migrate_training_tables
#         migrate_training_tables()
#     except Exception as _e:
#         print(f'⚠️  migrate_training_tables failed: {_e}')
# =====================================================================

def _quality_to_tier(quality: float) -> str:
    """
    Map a float quality score to an explicit tier label.
    Used at write time so every row has a human-readable tier.

    Tiers and their training use:
      gold   (1.0)      — human-corrected or human-written FAQ.
                          Always included; highest training weight.
      silver (0.8–0.99) — high-confidence RAG match or thumbs-up.
                          Core training set.
      bronze (0.6–0.79) — good RAG match, unrated.
                          Supplementary training.
      weak   (0.3–0.59) — low-confidence, fallback, or unrated low match.
                          Val/test only — do not train on these.
      noise  (< 0.3)    — near-zero confidence, pure guesses.
                          Excluded from all exports.
    """
    q = float(quality or 0.0)
    if q >= 1.0:
        return 'gold'
    if q >= 0.8:
        return 'silver'
    if q >= 0.6:
        return 'bronze'
    if q >= 0.3:
        return 'weak'
    return 'noise'


def migrate_training_tables():
    """
    Create the training_samples table.
    Safe to call on every startup — fully idempotent.

    Schema highlights:
      quality_tier  — explicit tier (gold/silver/bronze/weak/noise).
                      Assigned at write time from the quality float.
                      Drives all export filters — trainers use this column,
                      not the raw quality float.
      split_assigned — prevents assign_splits() from reshuffling already-
                      assigned val/test rows on a second call.
      reviewed      — admin approval flag for a future review queue.
    """
    conn = cursor = None
    try:
        conn, cursor = models.get_db()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS training_samples (
                id             SERIAL       PRIMARY KEY,
                sample_id      TEXT         NOT NULL UNIQUE,
                client_id      TEXT         NOT NULL
                               REFERENCES clients(client_id),
                session_id     TEXT,
                sample_type    TEXT         NOT NULL DEFAULT 'conversation',
                quality_tier   TEXT         NOT NULL DEFAULT 'bronze',
                instruction    TEXT         NOT NULL,
                input          TEXT         NOT NULL DEFAULT '',
                output         TEXT         NOT NULL,
                metadata_json  TEXT         NOT NULL DEFAULT '{}',
                quality        REAL         NOT NULL DEFAULT 0.5,
                split          TEXT         NOT NULL DEFAULT 'train',
                split_assigned BOOLEAN      NOT NULL DEFAULT FALSE,
                reviewed       BOOLEAN      NOT NULL DEFAULT FALSE,
                created_at     TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Primary export index — tier + split is the most common query shape
        cursor.execute(
            'CREATE INDEX IF NOT EXISTS idx_ts_tier_split '
            'ON training_samples (client_id, quality_tier, split, created_at DESC)'
        )
        cursor.execute(
            'CREATE INDEX IF NOT EXISTS idx_ts_session '
            'ON training_samples (client_id, session_id, created_at DESC)'
        )
        cursor.execute(
            'CREATE INDEX IF NOT EXISTS idx_ts_client_type '
            'ON training_samples (client_id, sample_type, created_at DESC)'
        )

        # Idempotent column additions for tables created before this schema revision
        for col, defn in [
            ('quality_tier',   "TEXT    NOT NULL DEFAULT 'bronze'"),
            ('split_assigned', 'BOOLEAN NOT NULL DEFAULT FALSE'),
            ('reviewed',       'BOOLEAN NOT NULL DEFAULT FALSE'),
        ]:
            cursor.execute(
                f"ALTER TABLE training_samples "
                f"ADD COLUMN IF NOT EXISTS {col} {defn}"
            )

        # Back-fill quality_tier on any existing rows
        cursor.execute(
            """
            UPDATE training_samples
            SET quality_tier = CASE
                WHEN quality >= 1.0 THEN 'gold'
                WHEN quality >= 0.8 THEN 'silver'
                WHEN quality >= 0.6 THEN 'bronze'
                WHEN quality >= 0.3 THEN 'weak'
                ELSE                     'noise'
            END
            WHERE quality_tier = 'bronze'
              AND quality <> 0.5
            """
        )

        conn.commit()
        print('✅ migrate_training_tables complete (training_samples + quality_tier)')

    except Exception as e:
        print(f'⚠️  migrate_training_tables error: {e}')
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# =====================================================================
# CORE WRITE — save_training_sample()
#
# All public collect_* functions funnel through here.
# Mirrors log_api_usage() in models.py: fire-and-forget, never raises,
# uses the same get_db() / cursor.close() / conn.close() pattern.
# =====================================================================

def save_training_sample(
    client_id:   str,
    sample_type: str,
    instruction: str,
    output:      str,
    input_ctx:   str  = '',
    session_id:  str  = '',
    metadata:    dict = None,
    quality:     float = 0.5,
) -> str:
    """
    Insert one training sample row. Returns the sample_id on success, '' on failure.

    Args:
        client_id:    Lumvi client identifier — verified server-side before calling
        sample_type:  conversation | tool_call | correction | escalation | rating
        instruction:  Alpaca "instruction" — the task description for the model
        output:       Alpaca "output"      — the desired response
        input_ctx:    Alpaca "input"       — additional context (may be empty)
        session_id:   chat_sessions session UUID (stored as TEXT, no FK)
        metadata:     arbitrary dict stored as JSON (method, confidence, tool_name…)
        quality:      float 0.0–1.0 — higher = better training signal

    Returns:
        sample_id string on success, '' on failure (never raises)
    """
    # Sanitise all text fields
    client_id   = _sanitize(client_id, 50)
    session_id  = _sanitize(session_id, 100)
    instruction = _sanitize(instruction, 4000)
    input_ctx   = _sanitize(input_ctx, 4000)
    output      = _sanitize(output, 4000)
    sample_type = _sanitize(sample_type, 30)
    quality     = max(0.0, min(float(quality or 0.5), 1.0))

    if not client_id or not instruction or not output:
        logger.debug('[TrainingCollector] save skipped — missing required fields')
        return ''

    valid_types = {'conversation', 'tool_call', 'correction', 'escalation', 'rating'}
    if sample_type not in valid_types:
        sample_type = 'conversation'

    sample_id     = f'ts_{uuid.uuid4().hex[:16]}'
    metadata_json = _safe_json(metadata or {})
    quality_tier  = _quality_to_tier(quality)

    conn = cursor = None
    try:
        conn, cursor = models.get_db()

        cursor.execute(
            '''
            INSERT INTO training_samples
                (sample_id, client_id, session_id, sample_type,
                 quality_tier, instruction, input, output, metadata_json, quality)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''',
            (sample_id, client_id, session_id or None, sample_type,
             quality_tier, instruction, input_ctx, output, metadata_json, quality)
        )
        conn.commit()

        logger.debug(
            f'[TrainingCollector] saved sample_id={sample_id} '
            f'type={sample_type} tier={quality_tier} client={client_id}'
        )
        return sample_id

    except Exception as e:
        logger.debug(f'[TrainingCollector] save_training_sample error: {e}')
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return ''
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# =====================================================================
# BACKGROUND WRAPPER
# All public collect_* functions use this to fire off DB writes without
# blocking the HTTP response — identical to notify_webhook threading
# pattern in app.py.
# =====================================================================

def _bg(fn, *args, **kwargs):
    """
    Run fn(*args, **kwargs) in a daemon thread.
    Matches app.py pattern: threading.Thread(daemon=True).start()
    Never raises — thread failures are logged inside fn.
    """
    t = threading.Thread(target=fn, args=args, kwargs=kwargs, daemon=True)
    t.start()


# =====================================================================
# PUBLIC COLLECTORS
# Each is a thin builder that constructs the Alpaca fields and calls
# save_training_sample(). Background versions (_bg_*) fire-and-forget.
# =====================================================================

# ── 1. Conversation turns ─────────────────────────────────────────────

def collect_conversation_turn(
    client_id:     str,
    session_id:    str,
    user_message:  str,
    bot_response:  str,
    method:        str  = '',
    confidence:    float = 0.0,
    vertical:      str  = 'general',
    is_lead:       bool = False,
) -> None:
    """
    Record a normal chat turn in the background.
    Called from /api/chat after generate_response() returns.
    Never blocks the HTTP response.

    Alpaca shape:
      instruction: system prompt describing the bot's role + vertical
      input:       user_message
      output:      bot_response
    """
    _bg(
        _collect_conversation_turn_sync,
        client_id, session_id, user_message, bot_response,
        method, confidence, vertical, is_lead,
    )


def _collect_conversation_turn_sync(
    client_id, session_id, user_message, bot_response,
    method, confidence, vertical, is_lead,
):
    quality = min(float(confidence or 0.0), 1.0)

    # ── Tier-aware quality mapping ────────────────────────────────────
    # Cache hits are high-quality by definition (they were cached because
    # they were previously a good answer).
    if method == 'cache':
        quality = max(quality, 0.80)

    # Keyword fallback is low-confidence by nature
    elif method == 'keyword_fallback':
        quality = min(quality, 0.55)

    # Lead pipeline responses are human-defined triggers — good signal
    elif method in ('lead_trigger', 'lead_pipeline'):
        quality = max(quality, 0.75)

    # Skip noise — pure guesses pollute the dataset
    if quality < 0.05:
        logger.debug(
            f'[TrainingCollector] skipping noise: client={client_id} '
            f'confidence={confidence:.3f} method={method}'
        )
        return

    # Map to tier (already done inside save_training_sample via _quality_to_tier,
    # but compute here for the metadata so trainers can filter on it later)
    tier = _quality_to_tier(quality)

    # weak and noise tiers only go into val/test — never train
    # We record them but tag them in metadata so the exporter can respect this
    train_eligible = tier in ('gold', 'silver', 'bronze')

    instruction = (
        f'You are a helpful customer support assistant for a {vertical} business. '
        f'Answer the customer\'s question accurately and concisely based on the '
        f'knowledge base you have been given.'
    )
    save_training_sample(
        client_id   = client_id,
        sample_type = 'conversation',
        instruction = instruction,
        input_ctx   = _sanitize(user_message, 4000),
        output      = _sanitize(bot_response, 4000),
        session_id  = session_id,
        quality     = quality,
        metadata    = {
            'method':          method,
            'confidence':      round(float(confidence or 0.0), 4),
            'vertical':        vertical,
            'is_lead':         is_lead,
            'tier':            tier,
            'train_eligible':  train_eligible,
        },
    )


# ── 2. Tool calls ─────────────────────────────────────────────────────

def collect_tool_call(
    client_id:   str,
    session_id:  str,
    tool_name:   str,
    tool_args:   dict,
    tool_result: dict,
    user_message: str = '',
) -> None:
    """
    Record a tool call + result in the background.
    Called from /api/chat when action_intent['is_tool'] is True.
    Never blocks the HTTP response.

    Alpaca shape:
      instruction: task description for tool-calling fine-tuning
      input:       tool_name + sanitised args (no client_id)
      output:      tool result summary
    """
    _bg(
        _collect_tool_call_sync,
        client_id, session_id, tool_name, tool_args, tool_result, user_message,
    )


def _collect_tool_call_sync(
    client_id, session_id, tool_name, tool_args, tool_result, user_message,
):
    # Strip client_id from args — never store it in training data
    safe_args = {k: v for k, v in (tool_args or {}).items() if k != 'client_id'}

    instruction = (
        f'You are a customer support assistant with access to tools. '
        f'The customer sent the following message. '
        f'Decide which tool to call and with what arguments.'
    )
    input_ctx = json.dumps({
        'user_message': _sanitize(user_message, 500),
        'tool_name':    tool_name,
        'args':         safe_args,
    }, default=str)[:4000]

    # Build a readable output from the result
    success = tool_result.get('success', False)
    if success:
        # Summarise the happy-path result without internal IDs
        summary_keys = ['message', 'confirmation_message', 'order', 'slots',
                        'count', 'results', 'ticket_id', 'booking_id']
        result_summary = {k: tool_result[k] for k in summary_keys if k in tool_result}
        output = json.dumps({'tool': tool_name, 'result': result_summary}, default=str)[:4000]
    else:
        output = json.dumps({
            'tool':  tool_name,
            'error': tool_result.get('error', 'Tool call failed')
        })

    # Tool calls make excellent fine-tuning data — high quality
    quality = 0.85 if success else 0.4

    save_training_sample(
        client_id   = client_id,
        sample_type = 'tool_call',
        instruction = instruction,
        input_ctx   = input_ctx,
        output      = output,
        session_id  = session_id,
        quality     = quality,
        metadata    = {
            'tool_name': tool_name,
            'success':   success,
            'args_keys': list(safe_args.keys()),
        },
    )


# ── 3. Human corrections ──────────────────────────────────────────────

def collect_correction(
    client_id:       str,
    session_id:      str,
    user_message:    str,
    original_response: str,
    corrected_response: str,
    agent_id:        str = '',
    vertical:        str = 'general',
) -> None:
    """
    Record a human agent's correction in the background.
    Called from /api/admin/inbox/* when an agent edits a bot response.
    Corrections are the highest-quality training signal — quality=1.0.
    Never blocks.

    Alpaca shape:
      instruction: same as conversation but with correction context
      input:       user_message
      output:      the corrected (human-approved) response
    """
    _bg(
        _collect_correction_sync,
        client_id, session_id, user_message, original_response,
        corrected_response, agent_id, vertical,
    )


def _collect_correction_sync(
    client_id, session_id, user_message, original_response,
    corrected_response, agent_id, vertical,
):
    instruction = (
        f'You are a helpful customer support assistant for a {vertical} business. '
        f'A human agent reviewed the following conversation and provided the correct response. '
        f'Learn from this correction.'
    )
    save_training_sample(
        client_id   = client_id,
        sample_type = 'correction',
        instruction = instruction,
        input_ctx   = _sanitize(user_message, 4000),
        output      = _sanitize(corrected_response, 4000),
        session_id  = session_id,
        quality     = 1.0,  # Human corrections are the best training signal
        metadata    = {
            'original_response': _sanitize(original_response, 500),
            'agent_id':          _sanitize(agent_id, 50),
            'vertical':          vertical,
            'correction':        True,
        },
    )


# ── 4. Escalations ────────────────────────────────────────────────────

def collect_escalation(
    client_id:    str,
    session_id:   str,
    user_message: str,
    reason:       str,
    ticket_id:    str  = '',
    urgency:      str  = 'normal',
    vertical:     str  = 'general',
) -> None:
    """
    Record a conversation escalation in the background.
    Called from /api/chat when escalate_to_human tool is used, or when
    the pipeline fires an escalation return (escalation / idk_fallback).
    Never blocks.

    Alpaca shape:
      instruction: task — when to escalate
      input:       user_message + reason
      output:      the escalation acknowledgement message
    """
    _bg(
        _collect_escalation_sync,
        client_id, session_id, user_message, reason,
        ticket_id, urgency, vertical,
    )


def _collect_escalation_sync(
    client_id, session_id, user_message, reason,
    ticket_id, urgency, vertical,
):
    instruction = (
        f'You are a customer support assistant for a {vertical} business. '
        f'This conversation requires escalation to a human agent. '
        f'Acknowledge the customer\'s issue empathetically and confirm that '
        f'a team member will follow up.'
    )
    input_ctx = json.dumps({
        'user_message': _sanitize(user_message, 500),
        'reason':       _sanitize(reason, 500),
        'urgency':      urgency,
    })
    output = (
        "I've flagged this conversation for our support team. "
        "A team member will follow up with you as soon as possible."
    )
    save_training_sample(
        client_id   = client_id,
        sample_type = 'escalation',
        instruction = instruction,
        input_ctx   = input_ctx,
        output      = output,
        session_id  = session_id,
        quality     = 0.75,
        metadata    = {
            'reason':    _sanitize(reason, 200),
            'ticket_id': _sanitize(ticket_id, 30),
            'urgency':   urgency,
            'vertical':  vertical,
        },
    )


# ── 5. User ratings ───────────────────────────────────────────────────

def collect_user_rating(
    client_id:   str,
    session_id:  str,
    sample_id:   str,
    rating:      int,
    user_message: str = '',
    bot_response: str = '',
) -> None:
    """
    Record a user thumbs-up (1) / thumbs-down (-1) / neutral (0) rating.
    Also updates the quality field on the original sample_id row so the
    exporter can filter by human-validated quality.
    Never blocks.

    Called from /api/chat/rate in app.py (new route added in System 2
    app.py changes).

    Args:
        rating: 1 = positive, -1 = negative, 0 = neutral
    """
    _bg(
        _collect_user_rating_sync,
        client_id, session_id, sample_id, rating, user_message, bot_response,
    )


def _collect_user_rating_sync(
    client_id, session_id, sample_id, rating, user_message, bot_response,
):
    # Clamp rating to valid values
    try:
        rating = int(rating)
    except (TypeError, ValueError):
        rating = 0
    rating = max(-1, min(rating, 1))

    # Convert to quality update: positive → 0.9, neutral → 0.5, negative → 0.1
    quality_map = {1: 0.9, 0: 0.5, -1: 0.1}
    new_quality = quality_map.get(rating, 0.5)

    # 1. Update quality on the original sample if sample_id is provided
    if sample_id:
        _update_sample_quality(_sanitize(sample_id, 30), client_id, new_quality)

    # 2. Save this rating itself as a new sample — useful for RLHF fine-tuning
    if user_message and bot_response:
        instruction = (
            'You are a customer support assistant. '
            'A user rated the following response. '
            'Use this signal to improve future responses.'
        )
        input_ctx = json.dumps({
            'user_message': _sanitize(user_message, 500),
            'bot_response': _sanitize(bot_response, 500),
            'rating':       rating,
        })
        output = _sanitize(bot_response, 4000)  # the rated response is the target
        save_training_sample(
            client_id   = client_id,
            sample_type = 'rating',
            instruction = instruction,
            input_ctx   = input_ctx,
            output      = output,
            session_id  = session_id,
            quality     = new_quality,
            metadata    = {
                'rating':          rating,
                'original_sample': _sanitize(sample_id, 30),
            },
        )


def _update_sample_quality(sample_id: str, client_id: str, quality: float) -> None:
    """
    Update quality AND quality_tier on an existing training_samples row.
    A thumbs-up promotes a bronze row to silver; thumbs-down demotes it to weak.
    Uses client_id in the WHERE clause — prevents cross-client writes.
    """
    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            '''
            UPDATE training_samples
            SET quality      = %s,
                quality_tier = %s
            WHERE sample_id = %s AND client_id = %s
            ''',
            (quality, _quality_to_tier(quality), sample_id, client_id)
        )
        conn.commit()
    except Exception as e:
        logger.debug(f'[TrainingCollector] _update_sample_quality error: {e}')
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# =====================================================================
# READ API — used by training_exporter.py and admin routes
# Mirrors get_kb_gaps() in models.py exactly:
#   - try/except, returns [] on failure, never raises
#   - isoformat() on timestamps before returning
# =====================================================================

def get_training_samples(
    client_id:   str,
    sample_type: str  = None,
    split:       str  = None,
    min_quality: float = 0.0,
    limit:       int  = 1000,
    offset:      int  = 0,
) -> list:
    """
    Return training samples for a client, filtered and paginated.

    Args:
        client_id:   Lumvi client identifier
        sample_type: Optional filter (conversation | tool_call | correction | …)
        split:       Optional filter (train | val | test)
        min_quality: Minimum quality score (default 0.0 = all)
        limit:       Max rows to return (default 1000, max 5000)
        offset:      Pagination offset

    Returns:
        List of dicts with all training_samples columns. [] on failure.
    """
    limit  = min(int(limit or 1000), 5000)
    offset = max(int(offset or 0), 0)

    conn = cursor = None
    try:
        conn, cursor = models.get_db()

        conditions = ['client_id = %s', 'quality >= %s']
        params     = [client_id, float(min_quality)]

        if sample_type:
            conditions.append('sample_type = %s')
            params.append(_sanitize(sample_type, 30))
        if split:
            conditions.append('split = %s')
            params.append(_sanitize(split, 10))

        where = ' AND '.join(conditions)
        params += [limit, offset]

        cursor.execute(
            f'''
            SELECT sample_id, client_id, session_id, sample_type,
                   instruction, input, output, metadata_json,
                   quality, split, created_at
            FROM training_samples
            WHERE {where}
            ORDER BY quality DESC, created_at DESC
            LIMIT %s OFFSET %s
            ''',
            params
        )
        rows = cursor.fetchall()

        result = []
        for r in rows:
            row = dict(r)
            # Deserialise metadata_json
            raw_meta = row.get('metadata_json') or '{}'
            try:
                row['metadata'] = json.loads(raw_meta) if isinstance(raw_meta, str) else raw_meta
            except Exception:
                row['metadata'] = {}
            # Serialise timestamp — mirrors get_kb_gaps() pattern
            if row.get('created_at'):
                row['created_at'] = row['created_at'].isoformat()
            result.append(row)

        return result

    except Exception as e:
        logger.debug(f'[TrainingCollector] get_training_samples error: {e}')
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def get_training_stats(client_id: str) -> dict:
    """
    Return tier breakdown + sample_type counts for a client.
    This is what you look at before a fine-tuning run to understand
    how much usable data you have per tier.

    Returns a dict with:
      by_tier   — {gold, silver, bronze, weak, noise} counts
      by_type   — {conversation, tool_call, correction, escalation, rating}
      train_ready — gold + silver + bronze (usable for training)
      total     — all rows
      avg_quality — mean quality score
    """
    _zero = {
        'total':       0,
        'train_ready': 0,
        'avg_quality': 0.0,
        'by_tier': {'gold': 0, 'silver': 0, 'bronze': 0, 'weak': 0, 'noise': 0},
        'by_type': {
            'conversation': 0, 'tool_call': 0,
            'correction': 0, 'escalation': 0, 'rating': 0,
        },
    }
    conn = cursor = None
    try:
        conn, cursor = models.get_db()
        cursor.execute(
            '''
            SELECT
                COUNT(*)                                              AS total,
                COUNT(*) FILTER (WHERE quality_tier = 'gold')        AS gold,
                COUNT(*) FILTER (WHERE quality_tier = 'silver')      AS silver,
                COUNT(*) FILTER (WHERE quality_tier = 'bronze')      AS bronze,
                COUNT(*) FILTER (WHERE quality_tier = 'weak')        AS weak,
                COUNT(*) FILTER (WHERE quality_tier = 'noise')       AS noise,
                COUNT(*) FILTER (WHERE sample_type = 'conversation') AS conversation,
                COUNT(*) FILTER (WHERE sample_type = 'tool_call')    AS tool_call,
                COUNT(*) FILTER (WHERE sample_type = 'correction')   AS correction,
                COUNT(*) FILTER (WHERE sample_type = 'escalation')   AS escalation,
                COUNT(*) FILTER (WHERE sample_type = 'rating')       AS rating,
                ROUND(AVG(quality)::NUMERIC, 3)                      AS avg_quality
            FROM training_samples
            WHERE client_id = %s
            ''',
            (client_id,)
        )
        row = cursor.fetchone()
        if not row:
            return _zero

        gold   = int(row.get('gold')   or 0)
        silver = int(row.get('silver') or 0)
        bronze = int(row.get('bronze') or 0)

        return {
            'total':       int(row.get('total') or 0),
            'train_ready': gold + silver + bronze,
            'avg_quality': float(row.get('avg_quality') or 0.0),
            'by_tier': {
                'gold':   gold,
                'silver': silver,
                'bronze': bronze,
                'weak':   int(row.get('weak')  or 0),
                'noise':  int(row.get('noise') or 0),
            },
            'by_type': {
                'conversation': int(row.get('conversation') or 0),
                'tool_call':    int(row.get('tool_call')    or 0),
                'correction':   int(row.get('correction')   or 0),
                'escalation':   int(row.get('escalation')   or 0),
                'rating':       int(row.get('rating')       or 0),
            },
        }
    except Exception as e:
        logger.debug(f'[TrainingCollector] get_training_stats error: {e}')
        return _zero
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def assign_splits(client_id: str, train_pct: float = 0.8,
                  val_pct: float = 0.1) -> dict:
    """
    Assign train / val / test splits to all unassigned samples for a client.
    Unassigned means split = 'train' (the default) AND no explicit assignment yet.

    This is called by training_exporter.py before export.
    Samples are randomly assigned in-DB using PostgreSQL's random() function.

    Args:
        train_pct: Fraction of samples for training (default 0.80)
        val_pct:   Fraction for validation (default 0.10)
                   Remainder goes to test.

    Returns:
        {'train': int, 'val': int, 'test': int} counts assigned.
    """
    if train_pct + val_pct >= 1.0:
        val_pct = 0.10
    test_pct = round(1.0 - train_pct - val_pct, 4)

    conn = cursor = None
    try:
        conn, cursor = models.get_db()

        # ── Step 1: weak + noise → val/test only (50/50 split) ────────────
        # These tiers have quality < 0.6 and must never appear in train.
        # They are still useful for measuring how the model handles
        # uncertain inputs in a held-out evaluation.
        # FIX: Added split_assigned = FALSE — idempotent on repeat calls.
        cursor.execute(
            '''
            WITH ranked_low AS (
                SELECT id,
                       ROW_NUMBER() OVER (ORDER BY random()) AS rn,
                       COUNT(*) OVER ()                      AS total
                FROM training_samples
                WHERE client_id = %s
                  AND quality_tier IN ('weak', 'noise')
                  AND split_assigned = FALSE
            )
            UPDATE training_samples ts
            SET split          = CASE
                                   WHEN r.rn <= FLOOR(r.total * 0.5) THEN 'val'
                                   ELSE 'test'
                                 END,
                split_assigned = TRUE
            FROM ranked_low r
            WHERE ts.id = r.id
            ''',
            (client_id,)
        )

        # ── Step 2: gold/silver/bronze → train/val/test at requested ratio ──
        # FIX: split_assigned = FALSE prevents reshuffling already-assigned rows.
        cursor.execute(
            '''
            WITH ranked AS (
                SELECT id,
                       ROW_NUMBER() OVER (ORDER BY random()) AS rn,
                       COUNT(*) OVER ()                      AS total
                FROM training_samples
                WHERE client_id = %s
                  AND quality_tier IN ('gold', 'silver', 'bronze')
                  AND split_assigned = FALSE
            )
            UPDATE training_samples ts
            SET split          = CASE
                                   WHEN r.rn <= FLOOR(r.total * %s) THEN 'train'
                                   WHEN r.rn <= FLOOR(r.total * %s) THEN 'val'
                                   ELSE 'test'
                                 END,
                split_assigned = TRUE
            FROM ranked r
            WHERE ts.id = r.id
            ''',
            (client_id, train_pct, train_pct + val_pct)
        )
        conn.commit()

        # Count how many landed in each split
        cursor.execute(
            '''
            SELECT split, COUNT(*) AS cnt
            FROM training_samples
            WHERE client_id = %s
            GROUP BY split
            ''',
            (client_id,)
        )
        counts = {r['split']: int(r['cnt']) for r in cursor.fetchall()}
        return {
            'train': counts.get('train', 0),
            'val':   counts.get('val', 0),
            'test':  counts.get('test', 0),
        }

    except Exception as e:
        logger.error(f'[TrainingCollector] assign_splits error: {e}')
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return {'train': 0, 'val': 0, 'test': 0}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
