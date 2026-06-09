"""
ai_helper.py  (refactored)
==========================
Thin AIHelper class. Responsibilities:
  1. Gemini model initialisation
  2. generate_response() — pipeline orchestrator (delegates to stages)
  3. KB management methods (enrich_and_chunk, index_faqs, etc.)
  4. Module-level re-exports for backward compatibility with app.py

What moved out:
  Pure math          → pipeline/stages/math_helpers.py
  Retrieval          → pipeline/stages/retrieval.py
  Generation         → pipeline/stages/generation.py
  Intent / tools     → pipeline/stages/intent.py
  Escalation         → pipeline/stages/escalation.py
  Session extraction → services/session_store.py
  KB gap tracking    → services/kb_gap.py
  Embedding / cache  → services/embedding.py
  Shared constants   → constants.py

app.py requires zero changes — all public names remain importable from here.
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import json
import logging
import os
import re
import threading
import time
from typing import Any, Dict, List, Optional

import google.genai as genai

# ── New module imports ────────────────────────────────────────────────────────
from constants import (
    BARE_PRONOUNS,
    GLOBAL_PRICING_KW,
    IDK_METHODS_ALL,
    INFERENCE_EXPANSION_ENABLED,
    PERSONALITIES,
    STAGE_SIGNALS,
    STAGE_ORDER,
)
from pipeline.context import PipelineRequest, PipelineResult
from pipeline.stages.escalation  import check_escalation
from pipeline.stages.generation  import (
    build_context,
    dynamic_fallback,
    guardrails,
    make_fallback,
    maybe_summarise,
    parse_clarify_response,
    rag_generate_and_polish,
)
from pipeline.stages.intent import (
    detect_intent,
    detect_action_intent,
    detect_simple_intent,
    dispatch_tool,
    handle_detected_action,
)
from pipeline.stages.retrieval   import (
    cross_encoder_rerank,
    decompose_intents,
    embedding_search,
    hybrid_rerank,
    is_followup,
    last_response_category,
    resolve_query,
    rewrite_query,
)
from services.embedding    import embed as _embed, normalize as _normalize
from services.kb_gap       import (
    get_poor_answers,
    get_top_kb_gaps,
    record_kb_gap,
    record_poor_answer,
    send_kb_gap_digest,
)
from services.session_store import (
    clear_chat_session,
    extract_session_memory,
    load_chat_session,
    persist_session,
)
from utils import get_logger, log_crash, generate as _gemini_call

logger = get_logger('lumvi.ai_helper')

# ── Background task executor ──────────────────────────────────────────────────
# Single process-level executor. Under Railway --workers 1 --threads N this
# is shared across all request threads safely.
_BG_EXECUTOR: concurrent.futures.ThreadPoolExecutor = (
    concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix='lumvi_bg')
)

# ── Response-level Redis cache ────────────────────────────────────────────────
_REDIS_RESP_PREFIX  = 'lumvi:resp:v1:'
_REDIS_RESP_TTL_SEC = 3600  # 1 hour

_redis_resp_client: Optional[Any] = None
try:
    import redis as _redis_lib
    _redis_url = os.environ.get('REDIS_URL')
    if _redis_url:
        _redis_resp_client = _redis_lib.from_url(
            _redis_url,
            decode_responses=True,
            socket_connect_timeout=1,
            socket_timeout=1,
        )
        logger.info("[AIHelper] Redis response cache enabled")
except Exception as _rc_err:
    logger.info(f"[AIHelper] Redis response cache unavailable: {_rc_err}")

# ── Singleton ─────────────────────────────────────────────────────────────────
_ai_helper_instance: Optional['AIHelper'] = None
_ai_helper_lock     = threading.Lock()

# ── Lead extraction — regex (no Gemini call) ──────────────────────────────────
_EMAIL_RE   = re.compile(r'\b[\w.+-]+@[\w-]+\.[a-zA-Z]{2,}\b')
_PHONE_RE   = re.compile(r'\b(?:\+?\d[\d\s\-().]{7,14}\d)\b')
_NAME_RE    = re.compile(
    r"(?:i'?m|my name is|this is|call me)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)",
    re.IGNORECASE,
)
_URGENT_KW  = frozenset({'urgent', 'asap', 'immediately', 'today', 'right now', 'right away'})

# ── IDK response cache TTL (shorter than normal — refresh as KB grows) ────────
_REDIS_IDK_TTL_SEC = 900   # 15 minutes

# ── Lead nudge — vertical-specific capture prompts ────────────────────────────
_VERTICAL_NUDGES: Dict[str, str] = {
    'real_estate': (
        "Would you like one of our agents to reach out? "
        "Drop your email and we'll be in touch."
    ),
    'law_firm': (
        "If you'd like to arrange a consultation, share your email "
        "and a member of the team will reach out to discuss your situation."
    ),
    'healthcare': (
        "If you'd like to discuss this further, share your email "
        "and someone from the team will be in touch."
    ),
    'dental': (
        "Happy to arrange a consultation — what email should we use "
        "to get in touch with you?"
    ),
    'saas': (
        "Would you like someone from the team to walk you through it? "
        "Share your email and we'll set something up."
    ),
    'gym': (
        "Want us to reach out about membership options? "
        "Drop your email and we'll be in touch."
    ),
}

# ── Purchase stage detection (module-level — called per turn in pipeline) ────
def _detect_purchase_stage(msg: str) -> Optional[str]:
    """
    Scan a single message for purchase stage signals.
    Returns the detected stage name or None.
    """
    msg_l = msg.lower()
    for stage, signals in STAGE_SIGNALS.items():
        if any(s in msg_l for s in signals):
            return stage
    return None


def _advance_stage(current: Optional[str], detected: Optional[str]) -> Optional[str]:
    """
    One-way ratchet: only advance purchase_stage, never regress.
    Returns the winning stage, or the existing one if detected is lower/None.
    """
    if not detected:
        return current
    if not current:
        return detected
    try:
        if STAGE_ORDER.index(detected) >= STAGE_ORDER.index(current):
            return detected
    except ValueError:
        pass
    return current


def get_ai_helper(
    api_key:    Optional[str] = None,
    model_name: Optional[str] = None,
) -> 'AIHelper':
    """
    Return the process-level AIHelper singleton.
    Thread-safe. Safe to call on every request.
    """
    global _ai_helper_instance
    if _ai_helper_instance is None:
        with _ai_helper_lock:
            if _ai_helper_instance is None:
                _ai_helper_instance = AIHelper(
                    api_key    = api_key    or os.environ.get('GEMINI_API_KEY', ''),
                    model_name = model_name or os.environ.get('GEMINI_MODEL', 'gemini-2.0-flash'),
                )
    return _ai_helper_instance


# ═════════════════════════════════════════════════════════════════════════════
# AIHelper
# ═════════════════════════════════════════════════════════════════════════════

class AIHelper:

    def __init__(
        self,
        api_key:    str = '',
        model_name: str = 'gemini-2.0-flash',
    ) -> None:
        self.api_key    = api_key
        self.model_name = model_name
        self.enabled    = bool(api_key)
        self.model: Optional[Any] = None

        if self.enabled:
            try:
                self._genai_client = genai.Client(api_key=api_key)
                self.model = self._genai_client.models
                self._model_name = model_name
                logger.info(f"[AIHelper] google.genai ready model={model_name}")
            except Exception as e:
                log_crash(logger, 'AIHelper/init', e, model=model_name)
                self.enabled = False
                self.model = None
        else:
            logger.warning(
                "[AIHelper] GEMINI_API_KEY not set — running in static fallback mode. "
                "Retrieval (embedding search) still works; LLM generation disabled."
            )

    # ── Private helpers ───────────────────────────────────────────────────────

    def _cache_key(self, text: str, *parts: str) -> str:
        raw = ':'.join([text, *parts])
        return hashlib.sha256(raw.encode()).hexdigest()

    def _parse_json(self, text: str) -> Optional[Dict]:
        try:
            clean = text.strip().strip('`')
            if clean.lower().startswith('json'):
                clean = clean[4:].strip()
            return json.loads(clean)
        except Exception:
            return None

    def _read_resp_cache(self, cache_key: str) -> Optional[Dict]:
        if not _redis_resp_client:
            return None
        try:
            raw = _redis_resp_client.get(_REDIS_RESP_PREFIX + cache_key)
            return json.loads(raw) if raw else None
        except Exception:
            return None

    def _write_resp_cache(self, cache_key: str, result: Dict) -> None:
        if not _redis_resp_client:
            return
        try:
            _redis_resp_client.setex(
                _REDIS_RESP_PREFIX + cache_key,
                _REDIS_RESP_TTL_SEC,
                json.dumps(result),
            )
        except Exception:
            pass

    def _write_resp_cache_ttl(self, cache_key: str, result: Dict, ttl: int) -> None:
        """Write to response cache with a custom TTL (e.g. shorter for IDK responses)."""
        if not _redis_resp_client:
            return
        try:
            _redis_resp_client.setex(
                _REDIS_RESP_PREFIX + cache_key,
                ttl,
                json.dumps(result),
            )
        except Exception:
            pass

    def _build_handoff_payload(
        self,
        history: List[Dict],
        session_mem: Dict,
        last_question: str,
        session_id: Optional[str],
        trigger_method: str = 'bot',
    ) -> Dict:
        return {
            'trigger':         trigger_method,
            'session_id':      session_id,
            'last_question':   last_question,
            'name':            session_mem.get('name'),
            'email':           session_mem.get('email'),
            'phone':           session_mem.get('phone'),
            'purchase_stage':  session_mem.get('purchase_stage'),
            'turn_count':      session_mem.get('turn_count', 0),
            'is_frustrated':   session_mem.get('is_frustrated', False),
        }

    def _extract_lead_info(
        self,
        message: str,
        session_mem: Dict,
        vertical: str,
        lead_triggers: List[str],
    ) -> Dict:
        """
        Extract structured lead info using regex — no Gemini call.

        Handles email, phone, name, urgency, and purchase_stage.
        Falls back to session_mem for fields already captured in earlier turns.
        """
        result: Dict = {}

        # Email — structured pattern, regex is exact
        m = _EMAIL_RE.search(message)
        result['email'] = m.group(0) if m else session_mem.get('email')

        # Phone — liberal pattern covers international formats
        m = _PHONE_RE.search(message)
        result['phone'] = m.group(0).strip() if m else session_mem.get('phone')

        # Name — common self-introduction patterns
        m = _NAME_RE.search(message)
        result['name'] = m.group(1).strip() if m else session_mem.get('name')

        # Urgency — keyword heuristic
        msg_lower = message.lower()
        result['urgency'] = 'high' if any(kw in msg_lower for kw in _URGENT_KW) else 'medium'

        # Purchase stage — carry through to CRM/webhook record
        result['purchase_stage'] = session_mem.get('purchase_stage')

        # Strip None values so callers get a clean dict
        return {k: v for k, v in result.items() if v}

    def _build_lead_nudge(
        self,
        lead_info: Dict,
        vertical: str,
        purchase_stage: Optional[str] = None,
        is_sales: bool = False,
        lead_q3: str = '',
    ) -> str:
        """
        Build a contextual lead capture prompt.

        Priority:
          1. Already have email — soft confirmation + optional lead_q3 follow-up
          2. Buying/evaluating stage or sales query — direct ask
          3. Vertical-specific soft ask
          4. Generic fallback
        """
        # Already have email
        if lead_info.get('email'):
            if lead_q3:
                return lead_q3
            return (
                "I've noted your details — someone from the team will be in "
                "touch shortly. Is there anything else I can help with in the "
                "meantime?"
            )

        # High-intent stages — direct email ask
        if purchase_stage in ('buying', 'evaluating') or is_sales:
            return (
                "To make sure the right person follows up with you — "
                "what's the best email address for you?"
            )

        # Vertical-specific phrasing
        if vertical in _VERTICAL_NUDGES:
            return _VERTICAL_NUDGES[vertical]

        # Generic fallback
        return (
            "If you'd like someone to follow up, I can pass your details to "
            "the team — what's the best email to reach you on?"
        )

    # ── generate_response ─────────────────────────────────────────────────────

    def generate_response(
        self,
        user_message:          str,
        faqs:                  List[Dict],
        vertical:              str = 'general',
        conversation_history:  Optional[List[Dict]] = None,
        client_id:             Optional[str] = None,
        lead_triggers:         Optional[List[str]] = None,
        kb_version:            Optional[int] = None,
        session_id:            Optional[str] = None,
        lead_q3:               str = '',
    ) -> Dict:

        conversation_history = conversation_history or []
        lead_triggers        = lead_triggers or []

        # ── Empty message ─────────────────────────────────────────────
        if not user_message or not user_message.strip():
            return PipelineResult.empty_message().to_dict()

        # ── Build pipeline context ────────────────────────────────────
        ctx = PipelineRequest(
            user_message=user_message,
            faqs=faqs,
            vertical=vertical,
            conversation_history=conversation_history,
            client_id=client_id,
            lead_triggers=lead_triggers,
            kb_version=kb_version,
            session_id=session_id,
        )

        # ── Response cache check ──────────────────────────────────────
        ctx.resp_cache_key = self._cache_key(
            user_message.lower().strip(),
            client_id or '',
            vertical,
            str(kb_version or ''),
        )
        cached = self._read_resp_cache(ctx.resp_cache_key)
        if cached:
            logger.debug(f"[Pipeline] cache hit trace={ctx.trace_id}")
            return cached

        try:
            # ── Preprocess ────────────────────────────────────────────
            ctx.clean_message = re.sub(r'\s+', ' ', user_message.strip())

            # ── Session loading ───────────────────────────────────────
            db_session: Dict = {}
            if client_id and session_id:
                db_session = load_chat_session(client_id, session_id)

            regex_session = extract_session_memory(conversation_history, ctx.clean_message)
            ctx.session_mem = {**db_session, **regex_session}

            # ── Purchase stage detection (one-way ratchet) ────────────
            _detected_stage = _detect_purchase_stage(ctx.clean_message)
            _new_stage = _advance_stage(
                ctx.session_mem.get('purchase_stage'), _detected_stage
            )
            if _new_stage:
                ctx.session_mem['purchase_stage'] = _new_stage

            # ── Handoff state machine ─────────────────────────────────
            # If the user is responding to a handoff offer
            if ctx.session_mem.get('handoff_offered'):
                msg_lower = ctx.clean_message.lower()
                if any(k in msg_lower for k in ['yes', 'yeah', 'sure', 'please', 'ok', 'go ahead']):
                    handoff_payload = self._build_handoff_payload(
                        conversation_history, ctx.session_mem,
                        ctx.clean_message, session_id, 'accepted'
                    )
                    result = PipelineResult(
                        response=(
                            "Perfect! I'll connect you with the team now. "
                            "They'll be in touch shortly."
                        ),
                        method='handoff_accepted',
                        confidence=1.0,
                        handoff=handoff_payload,
                    )
                    _BG_EXECUTOR.submit(
                        persist_session, client_id, session_id,
                        {**ctx.session_mem, 'handoff_offered': False}
                    )
                    return result.to_dict()

                if any(k in msg_lower for k in [
                    'no', 'nope', 'not now', 'no thanks', "don't", "i'm fine", 'im fine'
                ]):
                    ctx.session_mem['handoff_offered'] = False
                    _BG_EXECUTOR.submit(
                        persist_session, client_id, session_id, ctx.session_mem
                    )
                    return PipelineResult.declined_handoff().to_dict()

            # ── Escalation check ──────────────────────────────────────
            escalation_text = check_escalation(
                ctx.clean_message, ctx.session_mem, vertical
            )
            if escalation_text:
                handoff_payload = self._build_handoff_payload(
                    conversation_history, ctx.session_mem,
                    ctx.clean_message, session_id, 'escalation'
                )
                ctx.session_mem['handoff_offered'] = True
                _BG_EXECUTOR.submit(persist_session, client_id, session_id, ctx.session_mem)
                return PipelineResult(
                    response=escalation_text,
                    method='escalation',
                    confidence=1.0,
                    handoff=handoff_payload,
                ).to_dict()

            # ── Load poor KB IDs ──────────────────────────────────────
            try:
                poor = get_poor_answers(client_id or '', limit=100)
                ctx.poor_kb_ids = {
                    str(p.get('kb_id', '')) for p in poor if p.get('kb_id')
                }
            except Exception:
                ctx.poor_kb_ids = set()

            # ── Intent detection ──────────────────────────────────────
            # skip_gemini=True on first pass; Gemini intent only if needed
            intent = detect_intent(
                ctx.clean_message, vertical, lead_triggers,
                model=None, skip_gemini=True,
            )

            # ── Simple intent exit (no retrieval needed) ──────────────
            if intent['intent'] == 'goodbye':
                return PipelineResult.goodbye().to_dict()
            if intent['intent'] == 'gratitude':
                return PipelineResult.gratitude().to_dict()

            # ── Action exit (demo / meeting / contact CTA) ────────────
            if intent['intent'] == 'action' and intent.get('action'):
                action_data = handle_detected_action(intent['action'], vertical)
                lead_info = self._extract_lead_info(
                    ctx.clean_message, ctx.session_mem, vertical, lead_triggers
                )
                ctx.session_mem['handoff_offered'] = True
                _BG_EXECUTOR.submit(persist_session, client_id, session_id, ctx.session_mem)
                return PipelineResult(
                    response=action_data['response'],
                    method='action',
                    confidence=1.0,
                    is_lead=True,
                    lead_metadata=lead_info,
                    action=action_data,
                ).to_dict()

            # ── Tool dispatch exit ────────────────────────────────────
            if intent['intent'] == 'tool' and intent.get('tool'):
                tool_result = dispatch_tool(
                    intent['tool'], ctx.clean_message, client_id, ctx.session_mem
                )
                return PipelineResult(
                    response=tool_result.get('response', make_fallback(vertical)),
                    method='tool',
                    confidence=0.9 if tool_result.get('success') else 0.0,
                    action={'tool': intent['tool'], **tool_result},
                ).to_dict()

            # ── Confidence thresholds (sales queries get lower floor) ──
            ctx.is_sales_query = intent.get('is_sales', False)
            if ctx.is_sales_query:
                ctx.vector_threshold -= 0.05
                ctx.confidence_high  -= 0.05
                ctx.confidence_medium -= 0.05

            # ── Lead detection ────────────────────────────────────────
            is_lead      = intent.get('is_lead', False)
            lead_metadata: Optional[Dict] = None
            if is_lead:
                lead_info = self._extract_lead_info(
                    ctx.clean_message, ctx.session_mem, vertical, lead_triggers
                )
                if lead_info:
                    ctx.session_mem.update({
                        k: v for k, v in lead_info.items()
                        if v and k in ('name', 'email', 'phone')
                    })
                    lead_metadata = lead_info

            # ── Pronoun short-circuit → clarification ─────────────────
            words = ctx.clean_message.lower().split()
            if len(words) <= 3 and all(w in BARE_PRONOUNS for w in words if len(w) > 1):
                category = last_response_category(conversation_history)
                clarification = (
                    f"Could you tell me a bit more about what you'd like to know "
                    f"about {category}?" if category else
                    "Could you tell me a little more about what you're looking for?"
                )
                return PipelineResult(
                    response=clarification,
                    method='clarification',
                    confidence=1.0,
                ).to_dict()

            # ── Query resolution ──────────────────────────────────────
            ctx.search_query = resolve_query(ctx.clean_message, conversation_history)

            # ── Multi-intent decomposition ────────────────────────────
            sub_queries = decompose_intents(ctx.search_query)

            # ── Helper: run embedding search across sub-queries ───────
            def _do_embedding_search(query: str, sq_list: List[str]) -> tuple:
                if len(sq_list) > 1:
                    merged_c: List[Dict] = []
                    merged_s: List[float] = []
                    seen: set = set()
                    for sq in sq_list[:3]:
                        cands, scores = embedding_search(sq, faqs, client_id, ctx.poor_kb_ids)
                        for c, s in zip(cands, scores):
                            cid = str(c.get('kb_id', c.get('id', id(c))))
                            if cid not in seen:
                                merged_c.append(c)
                                merged_s.append(s)
                                seen.add(cid)
                    return merged_c, merged_s
                return embedding_search(query, faqs, client_id, ctx.poor_kb_ids)

            # ── Preflight embedding search (free — local model) ───────
            # Search on the raw resolved query first. If top cosine is
            # already ≥ confidence_high, the phrasing is unambiguous —
            # skip rewrite_query entirely (saves Call 1 for ~60% of msgs).
            ctx.candidates, ctx.vector_scores = _do_embedding_search(
                ctx.search_query, sub_queries
            )
            ctx.top_cosine = ctx.vector_scores[0] if ctx.vector_scores else 0.0

            # ── Query rewriting — only when retrieval is ambiguous (Call 1)
            if (self.enabled and self.model
                    and len(ctx.search_query.split()) >= 4
                    and ctx.top_cosine < ctx.confidence_high):
                rewritten = rewrite_query(
                    ctx.search_query, vertical, conversation_history, self.model
                )
                if rewritten != ctx.search_query:
                    ctx.search_query = rewritten
                    ctx.call1_used   = True
                    sub_queries      = decompose_intents(rewritten)
                    ctx.candidates, ctx.vector_scores = _do_embedding_search(
                        ctx.search_query, sub_queries
                    )
                    ctx.top_cosine = ctx.vector_scores[0] if ctx.vector_scores else 0.0

            # ── Hybrid rerank (RRF) ───────────────────────────────────
            ctx.hybrid_ranked, ctx.hybrid_scores = hybrid_rerank(
                ctx.search_query, ctx.candidates, ctx.vector_scores, faqs
            )
            ctx.top_hybrid = ctx.hybrid_scores[0] if ctx.hybrid_scores else 0.0

            # ── Cross-encoder rerank ──────────────────────────────────
            # Only fires for genuinely uncertain short-message retrievals.
            # Gate tightened from confidence_high (0.65) → 0.35: between
            # 0.35–0.65 the hybrid RRF is already reliable enough.
            if (self.enabled and self.model
                    and not ctx.call1_used
                    and ctx.top_cosine < 0.35
                    and len(ctx.hybrid_ranked) >= 2):
                ctx.hybrid_ranked = cross_encoder_rerank(
                    ctx.search_query, ctx.hybrid_ranked, self.model
                )
                ctx.call1_used = True

            # ── Context building + summarisation ──────────────────────
            _BG_EXECUTOR.submit(
                maybe_summarise, client_id, conversation_history, self.model
            )
            ctx.context_str = build_context(conversation_history, client_id, ctx.clean_message)

            # ── RAG qualification gate ─────────────────────────────────
            ctx.rag_qualified = ctx.top_cosine >= ctx.vector_threshold

            # ── Topic mismatch gate ───────────────────────────────────
            if ctx.rag_qualified and ctx.hybrid_ranked:
                top_faq = ctx.hybrid_ranked[0]
                overlap = topic_overlap = __import__(
                    'pipeline.stages.math_helpers', fromlist=['topic_overlap']
                ).topic_overlap
                if overlap(ctx.clean_message, top_faq.get('question', '')) < 0.05:
                    ctx.rag_qualified = False
                    logger.debug(
                        f"[Pipeline] topic mismatch gate fired trace={ctx.trace_id}"
                    )

            # ── FAQ-keyed semantic cache (Fix 7) ──────────────────────
            # If this client has already answered a question that retrieved
            # the same top FAQ, return the cached response. This catches
            # rephrasings ("what's the price?" vs "how much does it cost?")
            # without any vector scanning — just a second Redis lookup.
            _faq_cache_key: Optional[str] = None
            if ctx.rag_qualified and ctx.hybrid_ranked:
                _top_faq_id = str(
                    ctx.hybrid_ranked[0].get('kb_id',
                    ctx.hybrid_ranked[0].get('id', ''))
                )
                if _top_faq_id:
                    _faq_cache_key = self._cache_key(
                        _top_faq_id, client_id or '', vertical
                    )
                    _faq_cached = self._read_resp_cache(_faq_cache_key)
                    if _faq_cached:
                        logger.debug(
                            f"[Pipeline] FAQ-keyed cache hit trace={ctx.trace_id}"
                        )
                        return _faq_cached

            # ── Generation ────────────────────────────────────────────
            if ctx.rag_qualified and self.enabled and self.model:
                final, confidence, method = rag_generate_and_polish(
                    ctx.search_query,
                    ctx.context_str,
                    ctx.hybrid_ranked,
                    vertical,
                    ctx.session_mem,
                    ctx.is_sales_query,
                    self.model,
                )
            elif self.enabled and self.model:
                # Go straight to dynamic_fallback — it references the actual
                # query topic and is strictly better than vertical_fallback for
                # IDK paths. Eliminates the 2-call vertical→dynamic chain.
                final = dynamic_fallback(
                    ctx.search_query, vertical, ctx.session_mem,
                    self.model, self._model_name,
                )
                confidence = 0.0
                method     = 'dynamic_fallback_idk'
            else:
                final      = dynamic_fallback(
                    ctx.search_query, vertical, ctx.session_mem, None
                )
                confidence = 0.0
                method     = 'static_fallback'

            # ── CLARIFY handling ──────────────────────────────────────
            clarify_q = parse_clarify_response(final)
            if clarify_q:
                return PipelineResult(
                    response=clarify_q,
                    method='clarification',
                    confidence=1.0,
                ).to_dict()

            # ── Guardrails ────────────────────────────────────────────
            final = guardrails(final, ctx.hybrid_ranked)

            # ── Cache write ────────────────────────────────────────────
            _result_to_cache = PipelineResult(
                response=final,
                method=method,
                confidence=confidence,
                is_lead=is_lead,
                lead_metadata=lead_metadata,
            ).to_dict()

            if confidence >= ctx.confidence_high and ctx.resp_cache_key:
                # High-confidence RAG hit — write to both exact and FAQ-keyed cache
                _BG_EXECUTOR.submit(self._write_resp_cache, ctx.resp_cache_key, _result_to_cache)
                if _faq_cache_key:   # same answer for all rephrasings of this FAQ
                    _BG_EXECUTOR.submit(self._write_resp_cache, _faq_cache_key, _result_to_cache)
            elif method == 'dynamic_fallback_idk' and ctx.resp_cache_key:
                # IDK responses cached briefly — avoids re-running Gemini for the same
                # unanswered question from different users. Short TTL so new KB entries
                # surface quickly.
                _BG_EXECUTOR.submit(
                    self._write_resp_cache_ttl,
                    ctx.resp_cache_key,
                    _result_to_cache,
                    _REDIS_IDK_TTL_SEC,
                )

            # ── KB gap recording ──────────────────────────────────────
            if method in IDK_METHODS_ALL:
                _BG_EXECUTOR.submit(
                    record_kb_gap, client_id or '', ctx.clean_message,
                    method, confidence, session_id
                )
                ctx.session_mem['handoff_offered'] = True

            # ── Lead nudge (append to response if lead detected) ──────
            # Fires whenever is_lead=True and we don't yet have an email —
            # regardless of confidence. A high-confidence answer is the best
            # moment to ask; removing the confidence gate captures far more leads.
            trigger_lead_collection = False
            _have_email = bool(
                (lead_metadata or {}).get('email') or ctx.session_mem.get('email')
            )
            if is_lead and not _have_email:
                nudge = self._build_lead_nudge(
                    lead_info=ctx.session_mem,
                    vertical=vertical,
                    purchase_stage=ctx.session_mem.get('purchase_stage'),
                    is_sales=ctx.is_sales_query,
                    lead_q3=lead_q3,
                )
                final += '\n\n' + nudge
                trigger_lead_collection = True

            # ── Session persistence ───────────────────────────────────
            if client_id and session_id:
                _BG_EXECUTOR.submit(persist_session, client_id, session_id, ctx.session_mem)

            # ── Handoff payload for IDK paths ─────────────────────────
            handoff = None
            if method in IDK_METHODS_ALL or ctx.session_mem.get('handoff_offered'):
                handoff = self._build_handoff_payload(
                    conversation_history, ctx.session_mem,
                    ctx.clean_message, session_id, method
                )

            logger.info(
                f"[Pipeline] done trace={ctx.trace_id} method={method} "
                f"conf={confidence:.2f} elapsed={ctx.elapsed_ms:.0f}ms "
                f"client={client_id}"
            )

            return PipelineResult(
                response=final,
                method=method,
                confidence=round(confidence, 4),
                is_lead=is_lead,
                lead_metadata=lead_metadata,
                handoff=handoff,
                trigger_lead_collection=trigger_lead_collection,
            ).to_dict()

        except Exception as e:
            log_crash(logger, 'Pipeline/Fatal', e,
                      client_id=client_id,
                      trace_id=ctx.trace_id,
                      msg_preview=user_message[:80])
            return PipelineResult.fatal_error().to_dict()

    # ── KB management methods ─────────────────────────────────────────────────
    # These stay on AIHelper because they use self.model and are not part of
    # the real-time request pipeline.

    def _split_content(self, text: str, max_chars: int = 1200) -> List[str]:
        """Split long text into overlapping chunks at sentence boundaries."""
        sentences = re.split(r'(?<=[.!?])\s+', text.strip())
        chunks, current = [], ''
        for sent in sentences:
            if len(current) + len(sent) + 1 <= max_chars:
                current = (current + ' ' + sent).strip()
            else:
                if current:
                    chunks.append(current)
                current = sent
        if current:
            chunks.append(current)
        return chunks or [text[:max_chars]]

    def _extract_tags(self, text: str) -> List[str]:
        """Extract keyword tags from FAQ text (no model call)."""
        words = re.findall(r'\b[a-zA-Z]{4,}\b', text.lower())
        from collections import Counter
        freq = Counter(words)
        return [w for w, _ in freq.most_common(8) if w not in {
            'that', 'this', 'with', 'from', 'have', 'will', 'your',
            'what', 'when', 'where', 'also', 'more', 'some', 'been',
        }]

    def _ai_enrich(self, question: str, answer: str, vertical: str) -> Dict:
        """Use Gemini to enrich an FAQ entry with tags, paraphrases, and metadata."""
        if not self.enabled or not self.model:
            return {'tags': self._extract_tags(f"{question} {answer}"), 'paraphrases': []}
        prompt = (
            f"Enrich this FAQ entry for a {vertical} chatbot.\n\n"
            f"Q: {question}\nA: {answer}\n\n"
            "Respond ONLY with JSON:\n"
            '{"tags": ["tag1", "tag2"], "paraphrases": ["alt question 1", "alt question 2"], '
            '"summary": "one sentence summary", "topic": "topic category"}'
        )
        try:
            resp   = _gemini_call(self.model, prompt, self._model_name)
            parsed = self._parse_json(resp.text or '')
            return parsed or {'tags': [], 'paraphrases': []}
        except Exception as e:
            log_crash(logger, 'AIEnrich', e)
            return {'tags': [], 'paraphrases': []}

    def _ai_enrich_batch(
        self,
        faq_pairs: List[tuple],
        vertical: str,
    ) -> List[Dict]:
        """
        Enrich up to 5 FAQ pairs in a single Gemini call.
        Returns a list of enrichment dicts in the same order as faq_pairs.
        Falls back to tag-only enrichment if the model is unavailable or parsing fails.
        """
        if not self.enabled or not self.model:
            return [
                {'tags': self._extract_tags(f"{q} {a}"), 'paraphrases': []}
                for q, a in faq_pairs
            ]
        entries = '\n\n'.join(
            f"FAQ {i + 1}:\nQ: {q}\nA: {a[:400]}"
            for i, (q, a) in enumerate(faq_pairs)
        )
        prompt = (
            f"Enrich these {len(faq_pairs)} FAQ entries for a {vertical} chatbot.\n\n"
            f"{entries}\n\n"
            "Respond ONLY with a JSON array — one object per FAQ, same order:\n"
            '[{"tags": ["t1","t2"], "paraphrases": ["alt q1","alt q2"], '
            '"summary": "one sentence", "topic": "category"}, ...]'
        )
        try:
            resp   = _gemini_call(self.model, prompt, self._model_name)
            parsed = self._parse_json(resp.text or '')
            if isinstance(parsed, list) and len(parsed) == len(faq_pairs):
                return parsed
            logger.warning(f"[AIEnrichBatch] unexpected response length, falling back")
        except Exception as e:
            log_crash(logger, 'AIEnrichBatch', e)
        return [{'tags': [], 'paraphrases': []} for _ in faq_pairs]

    def _quality_score(self, question: str, answer: str) -> float:
        """Heuristic quality score for an FAQ entry (0.0–1.0)."""
        q_len = len(question.split())
        a_len = len(answer.split())
        score = 0.0
        if 3  <= q_len <= 20:  score += 0.3
        if 10 <= a_len <= 200: score += 0.4
        if '?' in question:    score += 0.1
        if a_len > 20:         score += 0.2
        return round(min(score, 1.0), 2)

    def enrich_and_chunk(
        self,
        question: str,
        answer: str,
        vertical: str = 'general',
        client_id: Optional[str] = None,
    ) -> List[Dict]:
        """
        Enrich a Q&A pair with AI-generated metadata and split long answers
        into overlapping chunks. Returns a list of chunk dicts ready for indexing.
        """
        enriched = self._ai_enrich(question, answer, vertical)
        chunks   = self._split_content(answer)
        results  = []

        for i, chunk in enumerate(chunks):
            # Include paraphrases in the embed text so alternative phrasings
            # of the question are captured in the vector space. This improves
            # retrieval on natural language variations and reduces how often
            # rewrite_query needs to fire.
            paraphrase_str = ' '.join(enriched.get('paraphrases', [])[:3])
            embed_text = (
                f"{question} "
                f"{paraphrase_str} "
                f"{enriched.get('summary', '')} "
                f"{chunk}"
            ).strip()
            embedding  = _embed(embed_text, task='retrieval_document')
            results.append({
                'question':    question,
                'answer':      chunk,
                'chunk_index': i,
                'tags':        ' '.join(enriched.get('tags', [])),
                'paraphrases': enriched.get('paraphrases', []),
                'topic':       enriched.get('topic', ''),
                'quality':     self._quality_score(question, chunk),
                'embedding':   embedding,
                'vertical':    vertical,
                'client_id':   client_id,
            })
        return results

    def index_faqs(
        self,
        faqs: List[Dict],
        client_id: Optional[str] = None,
        vertical: str = 'general',
    ) -> int:
        """
        Embed and upsert a list of FAQ dicts into the database.
        Enrichment is batched (5 FAQs per Gemini call) to minimise API cost.
        Returns the count of successfully indexed entries.
        """
        import models as _m
        from cache_utils import bump_kb_version
        from itertools import islice

        def _batched(iterable: list, n: int):
            it = iter(iterable)
            while batch := list(islice(it, n)):
                yield batch

        # Filter valid FAQs upfront
        valid_faqs = [
            f for f in faqs
            if str(f.get('question', '')).strip() and str(f.get('answer', '')).strip()
        ]

        indexed = 0
        for batch in _batched(valid_faqs, 5):
            # Enrich entire batch in one Gemini call
            pairs       = [(str(f['question']).strip(), str(f['answer']).strip()) for f in batch]
            enrichments = self._ai_enrich_batch(pairs, vertical)

            for faq, enriched in zip(batch, enrichments):
                try:
                    question = str(faq.get('question', '')).strip()
                    answer   = str(faq.get('answer', '')).strip()
                    kb_id    = faq.get('kb_id') or faq.get('id')
                    if not kb_id:
                        continue

                    chunks = self._split_content(answer)
                    for i, chunk in enumerate(chunks):
                        paraphrase_str = ' '.join(enriched.get('paraphrases', [])[:3])
                        embed_text = (
                            f"{question} {paraphrase_str} "
                            f"{enriched.get('summary', '')} {chunk}"
                        ).strip()
                        embedding = _embed(embed_text, task='retrieval_document')
                        chunk_data = {
                            'question':    question,
                            'answer':      chunk,
                            'chunk_index': i,
                            'tags':        ' '.join(enriched.get('tags', [])),
                            'paraphrases': enriched.get('paraphrases', []),
                            'topic':       enriched.get('topic', ''),
                            'quality':     self._quality_score(question, chunk),
                            'embedding':   embedding,
                            'vertical':    vertical,
                            'client_id':   client_id,
                        }
                        existing = _m.KbEntry.query.filter_by(
                            client_id=client_id, kb_id=kb_id,
                            chunk_index=i
                        ).first()
                        if existing:
                            for k, v in chunk_data.items():
                                setattr(existing, k, v)
                        else:
                            _m.db.session.add(_m.KbEntry(kb_id=kb_id, **chunk_data))
                    indexed += 1
                except Exception as e:
                    log_crash(logger, 'IndexFAQ', e, faq_id=faq.get('kb_id'))

        if indexed:
            try:
                _m.db.session.commit()
                if client_id:
                    bump_kb_version(client_id)
                logger.info(f"[IndexFAQs] indexed={indexed} client={client_id}")
            except Exception as e:
                log_crash(logger, 'IndexFAQ/commit', e, client_id=client_id)
                _m.db.session.rollback()

        return indexed

    def approve_and_publish_gap(
        self,
        gap_id: int,
        answer: str,
        client_id: str,
        vertical: str = 'general',
    ) -> bool:
        """Approve a KB gap draft, index it, and mark the gap as resolved."""
        try:
            import models as _m
            from cache_utils import bump_kb_version
            gap = _m.KbGap.query.filter_by(id=gap_id, client_id=client_id).first()
            if not gap:
                return False
            chunks = self.enrich_and_chunk(gap.question, answer, vertical, client_id)
            for chunk in chunks:
                entry = _m.KbEntry(kb_id=f"gap_{gap_id}", **chunk)
                _m.db.session.add(entry)
            gap.status     = 'resolved'
            gap.resolution = answer
            _m.db.session.commit()
            bump_kb_version(client_id)
            logger.info(f"[ApproveGap] gap_id={gap_id} client={client_id}")
            return True
        except Exception as e:
            log_crash(logger, 'ApproveGap', e, gap_id=gap_id, client_id=client_id)
            return False

    def draft_gap_answer(
        self,
        question: str,
        client_id: str,
        api_key: Optional[str] = None,
        model_name: Optional[str] = None,
    ) -> Optional[str]:
        """Use Gemini to draft an answer for an unanswered question."""
        if not self.enabled or not self.model:
            return None
        prompt = (
            f"Draft a helpful, accurate answer to this customer question:\n\n"
            f"Q: {question}\n\n"
            "Write in a friendly, professional tone. 2–4 sentences. "
            "Acknowledge any uncertainty. Do not make up specific numbers or policies."
        )
        try:
            resp = _gemini_call(self.model, prompt, self._model_name)
            return (resp.text or '').strip() or None
        except Exception as e:
            log_crash(logger, 'DraftGap', e, client_id=client_id)
            return None

    def reindex_all_clients(self, app_context: Any = None) -> Dict[str, int]:
        """
        Re-embed all KB entries for all clients.
        Used after a model version change (e.g., voyage-3-lite upgrade).
        Heavy operation — run from a one-off Railway job, not a web request.
        """
        try:
            import models as _m
            context = app_context or __import__('flask').current_app.app_context()
            results: Dict[str, int] = {}
            with context:
                clients = _m.db.session.query(_m.KbEntry.client_id).distinct().all()
                for (cid,) in clients:
                    entries = _m.KbEntry.query.filter_by(client_id=cid).all()
                    count = 0
                    for entry in entries:
                        text = f"{entry.question} {entry.answer}"
                        vec  = _embed(text, task='retrieval_document')
                        if vec:
                            entry.embedding = vec
                            count += 1
                    _m.db.session.commit()
                    results[cid] = count
                    logger.info(f"[Reindex] client={cid} entries={count}")
            return results
        except Exception as e:
            log_crash(logger, 'Reindex', e)
            return {}

    def find_best_faq(
        self,
        query: str,
        faqs: List[Dict],
        threshold: float = 0.40,
    ) -> Optional[Dict]:
        """Backward-compat helper: return single best FAQ or None."""
        candidates, scores = embedding_search(query, faqs, client_id=None)
        if candidates and scores and scores[0] >= threshold:
            return candidates[0]
        return None
