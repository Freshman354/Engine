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
import struct
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import google.generativeai as genai

# ── New module imports ────────────────────────────────────────────────────────
from constants import (
    BARE_PRONOUNS,
    GLOBAL_PRICING_KW,
    IDK_METHODS_ALL,
    INFERENCE_EXPANSION_ENABLED,
    PERSONALITIES,
)
from pipeline.context import PipelineRequest, PipelineResult
from pipeline.stages.escalation  import check_escalation
from pipeline.stages.generation  import (
    build_context,
    guardrails,
    make_fallback,
    maybe_summarise,
    parse_clarify_response,
    rag_generate_and_polish,
    vertical_fallback,
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
from utils import get_logger, log_crash

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
                    model_name = model_name or os.environ.get('GEMINI_MODEL', 'gemini-1.5-flash'),
                )
    return _ai_helper_instance


# ═════════════════════════════════════════════════════════════════════════════
# AIHelper
# ═════════════════════════════════════════════════════════════════════════════

class AIHelper:

    def __init__(
        self,
        api_key:    str = '',
        model_name: str = 'gemini-1.5-flash',
    ) -> None:
        self.api_key    = api_key
        self.model_name = model_name
        self.enabled    = bool(api_key)
        self.model: Optional[Any] = None

        if self.enabled:
            try:
                genai.configure(api_key=api_key)
                self.model = genai.GenerativeModel(
                    model_name,
                    generation_config=genai.GenerationConfig(
                        temperature=0.3,
                        max_output_tokens=512,
                    ),
                )
                logger.info(f"[AIHelper] Gemini ready model={model_name}")
            except Exception as e:
                log_crash(logger, 'AIHelper/init', e, model=model_name)
                self.enabled = False
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
        """Extract structured lead info using Gemini."""
        if not self.enabled or not self.model:
            return {}
        personality = PERSONALITIES.get(vertical, PERSONALITIES['general'])
        lead_kws    = personality.get('lead_keywords', []) + (lead_triggers or [])
        prompt = (
            f"Extract lead information from this customer message.\n"
            f"Message: \"{message}\"\n"
            f"Existing session data: {json.dumps({k: v for k, v in session_mem.items() if k in ('name','email','phone')})}\n\n"
            "Respond ONLY with JSON: "
            '{"name": null|"string", "email": null|"string", "phone": null|"string", '
            '"interest": null|"string", "urgency": "low|medium|high"}'
        )
        try:
            resp   = self.model.generate_content(prompt)
            parsed = self._parse_json(resp.text or '')
            return parsed or {}
        except Exception as e:
            log_crash(logger, 'LeadExtract', e)
            return {}

    def _build_lead_nudge(self, lead_info: Dict, vertical: str) -> str:
        """Build a lead capture follow-up message."""
        personality = PERSONALITIES.get(vertical, PERSONALITIES['general'])
        if not lead_info.get('email'):
            return (
                "It sounds like you'd like to take this further! "
                "Could I grab your email so the team can follow up with you?"
            )
        return (
            f"Great, I've noted your details. "
            f"Someone from our {vertical} team will be in touch shortly!"
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

            # ── Query rewriting (Call 1) ──────────────────────────────
            ctx.search_query = resolve_query(ctx.clean_message, conversation_history)

            if self.enabled and self.model and len(ctx.search_query.split()) >= 4:
                rewritten, is_sales = rewrite_query(
                    ctx.search_query, vertical, conversation_history, self.model
                )
                ctx.search_query   = rewritten
                ctx.is_sales_query = ctx.is_sales_query or is_sales
                ctx.call1_used     = True

            # ── Multi-intent decomposition ────────────────────────────
            sub_queries = decompose_intents(ctx.search_query)

            # ── Embedding search + hybrid rerank ──────────────────────
            if len(sub_queries) > 1:
                # Run retrieval for each sub-query, merge results
                merged_candidates: List[Dict] = []
                merged_scores: List[float]    = []
                seen_ids: set = set()
                for sq in sub_queries[:3]:  # max 3 sub-queries
                    cands, scores = embedding_search(
                        sq, faqs, client_id, ctx.poor_kb_ids
                    )
                    for c, s in zip(cands, scores):
                        cid = str(c.get('kb_id', c.get('id', id(c))))
                        if cid not in seen_ids:
                            merged_candidates.append(c)
                            merged_scores.append(s)
                            seen_ids.add(cid)
                ctx.candidates    = merged_candidates
                ctx.vector_scores = merged_scores
            else:
                ctx.candidates, ctx.vector_scores = embedding_search(
                    ctx.search_query, faqs, client_id, ctx.poor_kb_ids
                )

            ctx.top_cosine = ctx.vector_scores[0] if ctx.vector_scores else 0.0

            # ── Hybrid rerank (RRF) ───────────────────────────────────
            ctx.hybrid_ranked, ctx.hybrid_scores = hybrid_rerank(
                ctx.search_query, ctx.candidates, ctx.vector_scores, faqs
            )
            ctx.top_hybrid = ctx.hybrid_scores[0] if ctx.hybrid_scores else 0.0

            # ── Cross-encoder rerank ──────────────────────────────────
            # Only when: budget free, ambiguous top score, 2+ candidates
            if (self.enabled and self.model
                    and not ctx.call1_used
                    and ctx.top_cosine < ctx.confidence_high
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
                final, confidence, method = vertical_fallback(
                    ctx.search_query, vertical, ctx.session_mem, self.model
                )
            else:
                final      = make_fallback(vertical, ctx.session_mem.get('is_frustrated', False))
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

            # ── Cache write (high-confidence only) ────────────────────
            if confidence >= ctx.confidence_high and ctx.resp_cache_key:
                result_dict = PipelineResult(
                    response=final,
                    method=method,
                    confidence=confidence,
                    is_lead=is_lead,
                    lead_metadata=lead_metadata,
                ).to_dict()
                _BG_EXECUTOR.submit(self._write_resp_cache, ctx.resp_cache_key, result_dict)

            # ── KB gap recording ──────────────────────────────────────
            if method in IDK_METHODS_ALL:
                _BG_EXECUTOR.submit(
                    record_kb_gap, client_id or '', ctx.clean_message,
                    method, confidence, session_id
                )
                ctx.session_mem['handoff_offered'] = True

            # ── Lead nudge (append to response if lead detected) ──────
            trigger_lead_collection = False
            if is_lead and not lead_metadata and confidence < ctx.confidence_medium:
                final += '\n\n' + self._build_lead_nudge({}, vertical)
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
            resp   = self.model.generate_content(prompt)
            parsed = self._parse_json(resp.text or '')
            return parsed or {'tags': [], 'paraphrases': []}
        except Exception as e:
            log_crash(logger, 'AIEnrich', e)
            return {'tags': [], 'paraphrases': []}

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
            embed_text = f"{question} {enriched.get('summary', '')} {chunk}"
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
        Returns the count of successfully indexed entries.
        """
        import models as _m
        from cache_utils import bump_kb_version
        indexed = 0
        for faq in faqs:
            try:
                question = str(faq.get('question', '')).strip()
                answer   = str(faq.get('answer', '')).strip()
                if not question or not answer:
                    continue
                chunks = self.enrich_and_chunk(question, answer, vertical, client_id)
                for chunk in chunks:
                    kb_id = faq.get('kb_id') or faq.get('id')
                    if not kb_id:
                        continue
                    existing = _m.KbEntry.query.filter_by(
                        client_id=client_id, kb_id=kb_id,
                        chunk_index=chunk['chunk_index']
                    ).first()
                    if existing:
                        for k, v in chunk.items():
                            setattr(existing, k, v)
                    else:
                        entry = _m.KbEntry(kb_id=kb_id, **chunk)
                        _m.db.session.add(entry)
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
            resp = self.model.generate_content(prompt)
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
