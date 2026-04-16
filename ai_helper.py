"""
AI Helper — Phase 2: Full RAG Pipeline
Pipeline: Preprocessing → Intent → Embedding Search → Reranking
          → RAG Injection → Generation → Guardrails → Caching

Also preserved from Phase 1:
  - 15-message conversation memory
  - Conversation summarisation every 6 messages
  - Detailed structured logging
  - Response cache (in-process dict)
  - find_best_faq / generate_human_like_response kept as fallbacks
"""

import google.generativeai as genai
import json
import logging
import re
import hashlib
import math
import uuid
from typing import List, Dict, Tuple, Optional

logger = logging.getLogger(__name__)

# ── Zero-cost intent keywords ─────────────────────────────────────────
_SIMPLE_INTENTS = {
    'greeting':  ['hi', 'hello', 'hey', 'good morning', 'good afternoon', 'good evening'],
    'gratitude': ['thanks', 'thank you', 'cheers', 'appreciate', 'thx'],
    'goodbye':   ['bye', 'goodbye', 'see you', 'take care', 'cya'],
}

# ── Shared pricing keywords ────────────────────────────────────────────
_GLOBAL_PRICING_KW = [
    'price', 'pricing', 'cost', 'how much', 'enterprise', 'plan',
    'subscription', 'buy', 'quote', 'invoice', 'billing',
]

# ── Embedding via Gemini text-embedding-004 ───────────────────────────
# Zero memory overhead — uses the same google-generativeai package already installed.
# Free tier: 1,500 requests/min, 1M tokens/min — well within Lumvi's needs.
# No sentence-transformers, no PyTorch, no RAM spike on startup.

def _cosine(a: list, b: list) -> float:
    """Pure-Python cosine similarity — no numpy required."""
    dot   = sum(x * y for x, y in zip(a, b))
    mag_a = math.sqrt(sum(x * x for x in a))
    mag_b = math.sqrt(sum(x * x for x in b))
    return dot / (mag_a * mag_b) if mag_a and mag_b else 0.0


def _embed(text: str, task: str = 'retrieval_document') -> list:
    """
    Embed text using Gemini text-embedding-004.
    task: 'retrieval_document' for FAQs/chunks, 'retrieval_query' for user messages.
    Returns [] on any failure — callers degrade gracefully.
    """
    if not text or not text.strip():
        return []
    try:
        result = genai.embed_content(
            model='models/text-embedding-004',
            content=text.strip()[:2048],   # API hard limit
            task_type=task,
        )
        return result['embedding']
    except Exception as _e:
        logger.debug(f"[_embed] API error: {_e}")
        return []


class AIHelper:
    """Lumvi AI Helper — Phase 2 Full RAG Pipeline."""

    def __init__(self, api_key: str, model_name: str = 'gemini-2.0-flash'):
        self.api_key    = api_key
        self.model_name = model_name
        self.enabled    = bool(api_key and api_key.strip())

        # Per-vertical config: tone + domain-specific lead/pricing keywords
        self.personalities = {
            'general': {
                'tone':           "warm, friendly, and helpful — like a knowledgeable colleague",
                'lead_keywords':  ['demo', 'speak to someone', 'contact me', 'call me', 'book a call', 'human', 'agent', 'talk to sales'],
                'pricing_keywords': _GLOBAL_PRICING_KW,
            },
            'real_estate': {
                'tone':           "enthusiastic, reassuring, and professional — make buying/renting feel exciting",
                'lead_keywords':  ['viewing', 'appointment', 'book a tour', 'schedule', 'visit the property', 'speak to agent'],
                'pricing_keywords': _GLOBAL_PRICING_KW + ['rent', 'mortgage', 'deposit'],
            },
            'saas': {
                'tone':           "patient, clear, and solution-oriented — great at explaining features",
                'lead_keywords':  ['demo', 'trial', 'onboarding', 'integration', 'speak to sales', 'account manager'],
                'pricing_keywords': _GLOBAL_PRICING_KW + ['monthly fee', 'annual plan', 'seats'],
            },
            'ecommerce': {
                'tone':           "fast, friendly, and shopper-focused — keep it quick and helpful",
                'lead_keywords':  ['order status', 'return', 'refund', 'speak to support'],
                'pricing_keywords': _GLOBAL_PRICING_KW + ['shipping cost', 'discount', 'promo'],
            },
            'healthcare': {
                'tone':           "calm, empathetic, and professional — never give medical advice",
                'lead_keywords':  ['appointment', 'booking', 'schedule', 'consultation', 'see a doctor'],
                'pricing_keywords': _GLOBAL_PRICING_KW + ['consultation fee', 'insurance'],
            },
            'law_firm': {
                'tone':           "formal, precise, trustworthy, and cautious — excellent at intake",
                'lead_keywords':  ['consultation', 'case review', 'speak to lawyer', 'legal advice'],
                'pricing_keywords': _GLOBAL_PRICING_KW + ['retainer', 'hourly rate', 'flat fee'],
            },
        }

        # In-process response cache {md5_key: response_text}
        self._response_cache: Dict[str, str] = {}

        if self.enabled:
            try:
                genai.configure(api_key=api_key)
                self.model = genai.GenerativeModel(model_name)
                logger.info(
                    f"✅ AI Helper Phase 2 ready | model={model_name} | "
                    f"embed=gemini-text-embedding-004 | cache=ON | rag=ON"
                )
            except Exception as e:
                logger.error(f"[AIHelper.__init__] Gemini init failed: {e}")
                self.enabled = False
        else:
            logger.warning("[AIHelper] Disabled — GEMINI_API_KEY not set")

    # ═══════════════════════════════════════════════════════════════════
    # PUBLIC ENTRY POINT — called from /api/chat
    # ═══════════════════════════════════════════════════════════════════

    def generate_response(self, user_message: str, faqs: List[Dict],
                          vertical: str = 'general',
                          conversation_history: List[Dict] = None,
                          client_id: str = None,
                          lead_triggers: List[str] = None) -> Dict:
        """
        Full RAG pipeline. Returns:
          {'response': str, 'method': str, 'confidence': float, 'is_lead': bool}
        Steps:
          1. Input preprocessing
          2. Intent detection (vertical-aware)
          3. Embedding search → top-5 candidates
          4. Reranking
          5. RAG context injection + LLM generation
          6. Guardrails
          7. Cache write
        """
        if not user_message or not user_message.strip():
            return {'response': "How can I help you today?", 'method': 'empty',
                    'confidence': 1.0, 'is_lead': False}

        # ── 1. Preprocessing ───────────────────────────────────────────
        clean = self._preprocess(user_message)

        # ── 2. Intent detection ────────────────────────────────────────
        intent = self.detect_intent(clean, lead_triggers or [], vertical)

        if intent.get('is_lead') and intent.get('confidence', 0) >= 0.65:
            logger.info(
                f"[Lead] client={client_id} vertical={vertical} "
                f"score={intent.get('score', 0):.1f} conf={intent['confidence']:.2f}"
            )
            return {
                'response':   "I'd be happy to connect you with our team! What's the best way to reach you?",
                'method':     'lead_detection',
                'confidence': intent['confidence'],
                'is_lead':    True,
            }

        # ── 3. Conversation context (built BEFORE search so it can enrich query) ──
        context_str = self._build_context(conversation_history, client_id, clean)

        # ── 4. Embedding search — use enriched query for follow-ups ────
        # For referential messages ("how about pro?") the enriched query
        # borrows topic keywords from history so the embedding score stays high.
        search_query = self._resolve_query(clean, conversation_history or [])
        candidates, scores = self._embedding_search(search_query, faqs, client_id)

        # ── 5. Reranking ────────────────────────────────────────────────
        reranked = self._rerank(search_query, candidates, scores)

        # ── Cache check (keyed on original clean message, not enriched query) ──
        top_id    = str(reranked[0]['kb_id']) if reranked else ''
        cache_key = self._cache_key(clean, top_id, vertical)
        if cache_key in self._response_cache:
            logger.debug(f"[Cache HIT] key={cache_key[:10]}…")
            return {
                'response':   self._response_cache[cache_key],
                'method':     'cache',
                'confidence': scores[0] if scores else 0.8,
                'is_lead':    False,
            }

        # ── 6. LLM generation ─────────────────────────────────────────
        if reranked and self.enabled:
            response_text, confidence, method = self._rag_generate(
                clean, reranked, scores, vertical, context_str
            )
        elif self.enabled:
            # No embedding hit — let Gemini answer with whatever FAQs exist
            # (even zero FAQs — context_str + personality is enough)
            response_text = self._vertical_fallback(clean, faqs[:12], vertical, context_str)
            confidence, method = 0.35, 'vertical_fallback'
        else:
            response_text = self._make_fallback(faqs[0].get('answer', '') if faqs else '')
            confidence, method = 0.0, 'static_fallback'

        # ── 7. Guardrails ──────────────────────────────────────────────
        response_text = self._guardrails(response_text, reranked)

        # ── Write cache ────────────────────────────────────────────────
        if confidence > 0.4:
            self._response_cache[cache_key] = response_text

        logger.info(
            f"[Chat] method={method} confidence={confidence:.2f} "
            f"top_chunk={top_id[:12] if top_id else 'none'} vertical={vertical}"
        )
        return {
            'response':   response_text,
            'method':     method,
            'confidence': confidence,
            'is_lead':    False,
        }

    # ═══════════════════════════════════════════════════════════════════
    # 1. PREPROCESSING
    # ═══════════════════════════════════════════════════════════════════

    def _preprocess(self, text: str) -> str:
        text = text.strip()
        text = re.sub(r'\s+', ' ', text)
        return text

    # ═══════════════════════════════════════════════════════════════════
    # 2. INTENT DETECTION
    # ═══════════════════════════════════════════════════════════════════

    def detect_intent(self, user_message: str, lead_triggers: List[str],
                      vertical: str = 'general') -> Dict:
        """
        Three-tier intent detection:
          Tier 1 — Simple intents (greeting/gratitude/bye): free keyword match
          Tier 2 — Keyword lead scoring (strong + pricing + custom)
          Tier 3 — Gemini confirmation for borderline scores
        """
        msg      = user_message.lower().strip()
        vert_cfg = self.personalities.get(vertical, self.personalities['general'])

        # Tier 1 — Zero-cost simple intents
        for intent, keywords in _SIMPLE_INTENTS.items():
            if any(msg == k or msg.startswith(k) for k in keywords):
                return {'intent': intent, 'is_lead': False, 'confidence': 0.97, 'score': 0}

        # Tier 2 — Lead keyword scoring
        score   = 0.0
        reasons = []

        for kw in vert_cfg.get('lead_keywords', []):
            if kw in msg:
                score += 4.0
                reasons.append(f"lead:{kw}")

        for kw in vert_cfg.get('pricing_keywords', _GLOBAL_PRICING_KW):
            if kw in msg:
                score += 2.5
                reasons.append(f"price:{kw}")

        for trigger in lead_triggers:
            if trigger.lower() in msg:
                score += 3.0
                reasons.append(f"custom:{trigger}")

        if score >= 5.0:
            confidence = min(0.97, score / 12.0)
            logger.debug(f"[Intent] lead_hit score={score:.1f} reasons={reasons[:3]}")
            return {'intent': 'lead_request', 'is_lead': True,
                    'score': score, 'confidence': confidence, 'reasons': reasons[:3]}

        # Tier 3 — Borderline AI confirmation
        if self.enabled and 2.5 <= score < 5.0:
            try:
                prompt = f"""Is this a lead request (user wants human contact, a demo, or to buy)?
Message: "{user_message}"
Triggers: {', '.join(lead_triggers)}
Return ONLY JSON: {{"is_lead": true, "confidence": 0.75}}"""
                resp   = self.model.generate_content(prompt)
                result = self._parse_json(resp.text)
                if result:
                    is_lead = result.get('is_lead', False)
                    conf    = float(result.get('confidence', 0.6))
                    if is_lead:
                        logger.info(f"[Intent] AI-confirmed lead score={score:.1f} conf={conf:.2f}")
                    return {'intent': 'lead_request' if is_lead else 'question',
                            'is_lead': is_lead, 'score': score, 'confidence': conf}
            except Exception as _e:
                logger.debug(f"[Intent] AI tier failed: {_e}")

        return {'intent': 'question', 'is_lead': False, 'score': score, 'confidence': 0.6}

    # ═══════════════════════════════════════════════════════════════════
    # 3. EMBEDDING SEARCH
    # ═══════════════════════════════════════════════════════════════════

    def _embedding_search(self, user_message: str, faqs: List[Dict],
                          client_id: str = None) -> Tuple[List[Dict], List[float]]:
        """
        Retrieve top-5 candidates using Gemini cosine similarity.
        Search order: knowledge_base chunks → legacy FAQ embeddings → keyword overlap.
        """
        if not self.enabled:
            return [], []

        # Embed the user query (retrieval_query task type for better accuracy)
        query_vec = _embed(user_message, task='retrieval_query')

        # ── Try knowledge_base first (Phase 2 preferred source) ────────
        if client_id and query_vec:
            try:
                import models as _m
                # Pass query_vec so the model function can rank by cosine similarity
                kb_chunks = _m.get_relevant_knowledge(client_id, query_vec, limit=5)
                if kb_chunks:
                    scored = []
                    for chunk in kb_chunks:
                        emb = chunk.get('embedding')
                        if emb:
                            score = _cosine(query_vec, emb)
                        else:
                            # Chunk returned without embedding — use positional rank score
                            score = max(0.0, 0.5 - (len(scored) * 0.05))
                        if score > 0.30:   # lowered from 0.38 — catches more valid hits
                            scored.append(({
                                'id':       chunk.get('kb_id', chunk.get('id', '')),
                                'kb_id':    chunk.get('kb_id', chunk.get('id', '')),
                                'question': chunk.get('title', ''),
                                'answer':   chunk.get('content', ''),
                                'category': chunk.get('category', 'General'),
                                'type':     chunk.get('type', 'faq'),
                            }, score))
                    if scored:
                        scored.sort(key=lambda x: x[1], reverse=True)
                        logger.debug(f"[KB Search] top_score={scored[0][1]:.3f} hits={len(scored)}")
                        return [s[0] for s in scored[:5]], [s[1] for s in scored[:5]]
            except Exception as _e:
                logger.warning(f"[_embedding_search] KB error: {_e}")

        # ── Fall back to legacy FAQ embeddings ──────────────────────────
        if not faqs:
            return [], []

        if client_id and query_vec:
            try:
                import models as _m
                # Lazy-index any FAQs not yet embedded
                stored = {e['faq_id']: e['embedding'] for e in _m.get_faq_embeddings(client_id)}
                for faq in faqs:
                    fid = str(faq.get('id', ''))
                    if fid and fid not in stored and faq.get('question'):
                        vec = _embed(faq['question'], task='retrieval_document')
                        if vec:
                            _m.store_faq_embedding(client_id, fid, faq['question'], vec)
                            stored[fid] = vec

                if stored:
                    faq_idx = {str(f.get('id', '')): f for f in faqs}
                    scored  = []
                    for fid, emb in stored.items():
                        score = _cosine(query_vec, emb)
                        if score > 0.30 and fid in faq_idx:   # lowered from 0.38
                            scored.append((faq_idx[fid], score))
                    scored.sort(key=lambda x: x[1], reverse=True)
                    if scored:
                        logger.debug(f"[FAQ Embed] top={scored[0][1]:.3f} hits={len(scored)}")
                        return [s[0] for s in scored[:5]], [s[1] for s in scored[:5]]
            except Exception as _e:
                logger.warning(f"[_embedding_search] FAQ embed error: {_e}")

        # ── Keyword overlap — also used when query_vec is empty ─────────
        q_words = set(user_message.lower().split())
        scored  = []
        for faq in faqs:
            combined = (faq.get('question', '') + ' ' + faq.get('answer', '')).lower()
            fq_words  = set(combined.split())
            overlap   = len(q_words & fq_words)
            if overlap > 0:
                scored.append((faq, overlap / max(len(q_words), 1)))
        scored.sort(key=lambda x: x[1], reverse=True)
        if scored:
            logger.debug(f"[Keyword Search] hits={len(scored)} top_score={scored[0][1]:.3f}")
            return [s[0] for s in scored[:5]], [s[1] for s in scored[:5]]

        return [], []

    # ═══════════════════════════════════════════════════════════════════
    # 4. RERANKING
    # ═══════════════════════════════════════════════════════════════════

    def _rerank(self, user_message: str, candidates: List[Dict],
                scores: List[float]) -> List[Dict]:
        """
        Lightweight reranking: combines embedding score + keyword overlap + answer length.
        Returns reranked list (same objects, new order).
        """
        if not candidates:
            return []

        q_words = set(user_message.lower().split())
        reranked = []

        for i, faq in enumerate(candidates):
            embed_score = scores[i] if i < len(scores) else 0.0
            fq_words    = set(faq.get('question', '').lower().split())
            overlap     = len(q_words & fq_words) / max(len(q_words), 1)
            length_norm = min(len(faq.get('answer', '')) / 300, 1.0) * 0.05
            final_score = (embed_score * 0.75) + (overlap * 0.20) + length_norm
            reranked.append((faq, final_score))

        reranked.sort(key=lambda x: x[1], reverse=True)
        logger.debug(f"[Rerank] top_score={reranked[0][1]:.3f} candidates={len(reranked)}")
        return [r[0] for r in reranked]

    # ═══════════════════════════════════════════════════════════════════
    # 5. RAG GENERATION
    # ═══════════════════════════════════════════════════════════════════

    def _rag_generate(self, user_message: str, reranked: List[Dict],
                      scores: List[float], vertical: str,
                      context_str: str) -> Tuple[str, float, str]:
        """
        Inject top chunks as RAG context and generate a grounded response.
        The prompt is structured in three layers:
          1. Universal follow-up inference rules (always present, covers every vertical)
          2. Conversation context built by _build_context
          3. Retrieved knowledge chunks + tightly scoped instructions

        Returns (response_text, confidence, method_tag).
        """
        vert_cfg    = self.personalities.get(vertical, self.personalities['general'])
        personality = vert_cfg['tone']
        top_score   = scores[0] if scores else 0.5

        # Build multi-chunk context (top 3 for richer grounding)
        chunks_context = ""
        for i, chunk in enumerate(reranked[:3], 1):
            chunks_context += (
                f"\n[Source {i}]\n"
                f"Title: {chunk.get('question', chunk.get('title', ''))}\n"
                f"Content: {chunk.get('answer', chunk.get('content', ''))}\n"
            )

        is_followup = '[Follow-up context]' in context_str

        # ── Universal follow-up inference rules ───────────────────────
        # Always injected — cheap insurance even when is_followup is False,
        # because the model should always be ready to handle referential phrasing.
        followup_rules = """
╔══════════════════════════════════════════════════════════════════════╗
║  FOLLOW-UP & REFERENTIAL QUESTION RULES  (read before answering)   ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  This assistant is embedded on websites across many industries:      ║
║  SaaS platforms, law firms, dental clinics, e-commerce stores,       ║
║  restaurants, real estate agencies, gyms, plumbers, and more.        ║
║  Users ALWAYS speak in conversational shorthand. Short or vague      ║
║  messages are almost NEVER standalone questions — they are           ║
║  continuations of the previous topic.                                ║
║                                                                      ║
║  FOLLOW-UP PATTERNS you will encounter in every vertical:           ║
║                                                                      ║
║  Pricing / plans:                                                    ║
║    "how about the pro plan?"  "and agency?"  "same for enterprise?"  ║
║    "what does that one cost?" "how much is it?"  "the yearly price?" ║
║                                                                      ║
║  Features / inclusions:                                              ║
║    "what's included?"  "what are the features?"  "does it have X?"   ║
║    "is that covered?"  "what does that come with?"                   ║
║                                                                      ║
║  Services / appointments / hours:                                    ║
║    "and their Saturday hours?"  "do they also do X?"                 ║
║    "how long does that take?"  "is it available on weekends?"        ║
║                                                                      ║
║  Comparisons:                                                        ║
║    "which is better?"  "what's the difference?"  "vs the other one?" ║
║    "is the pro worth it over starter?"                               ║
║                                                                      ║
║  Contact / location:                                                 ║
║    "and their number?"  "where are they located?"  "email address?"  ║
║                                                                      ║
║  Generic continuations:                                              ║
║    "what about X?"  "same question for Y"  "the Z one?"              ║
║    "tell me more"  "go on"  "and that one?"  "is that right?"        ║
║                                                                      ║
║  HOW TO RESOLVE INTENT:                                              ║
║  1. Read [Conversation so far] to identify the active topic.        ║
║  2. Apply the current short message to that topic.                   ║
║  3. Answer directly from the retrieved knowledge below.             ║
║                                                                      ║
║  ABSOLUTE RULES:                                                     ║
║  ✗ NEVER say "I'm not sure what you mean" when history is present.  ║
║  ✗ NEVER say "Could you clarify?" when a reasonable read exists.    ║
║  ✗ NEVER say "I don't know" just because the message is short.      ║
║  ✓ If the entity or topic can be named from history, answer it.     ║
║  ✓ Only say you can't help when the knowledge truly has no answer.  ║
╚══════════════════════════════════════════════════════════════════════╝"""

        # Extra emphasis block only shown when follow-up is positively detected
        followup_emphasis = ""
        if is_followup:
            followup_emphasis = """
[CONFIRMED FOLLOW-UP] The message below is flagged as a direct continuation.
The [Follow-up context] section above names the preceding question explicitly.
Do NOT treat the current message in isolation — combine it with that context
to reconstruct the full intent, then answer it from the knowledge provided."""

        prompt = f"""You are a {personality} customer support assistant having a real, flowing conversation.
{followup_rules}
{followup_emphasis}
{context_str}

Current message: "{user_message}"

Retrieved knowledge (answer ONLY from what is here):
{chunks_context.strip()}

Response rules:
- Infer the full question from context when the current message is short or referential.
- Ground every fact in the retrieved knowledge above — never invent details.
- Natural and warm tone: 1–3 sentences, contractions welcome (I'm, it's, you'd, they're).
- A short bullet list is fine only when listing 3+ distinct items — otherwise prose.
- Do NOT hedge, ask for clarification, or offer to escalate unless the knowledge
  genuinely cannot answer the inferred question.
- No markdown headers, no preamble, no sign-off.

Return ONLY the response text."""

        try:
            response      = self.model.generate_content(prompt)
            response_text = response.text.strip()

            if not response_text or len(response_text) < 10:
                return reranked[0].get('answer', reranked[0].get('content', '')), top_score, 'rag_fallback'

            return response_text, top_score, 'rag_pipeline'

        except Exception as e:
            logger.error(f"[_rag_generate] Gemini error: {e}")
            answer = reranked[0].get('answer', reranked[0].get('content', ''))
            return self._make_fallback(answer), top_score * 0.7, 'rag_static'

    def _vertical_fallback(self, user_message: str, faqs: List[Dict],
                           vertical: str, context_str: str) -> str:
        """Fallback when no strong embedding hit — uses knowledge context loosely."""
        vert_cfg    = self.personalities.get(vertical, self.personalities['general'])
        personality = vert_cfg['tone']

        # Build a light context from top FAQs
        faq_context = "\n".join([
            f"- {f.get('question', '')}: {f.get('answer', '')[:120]}"
            for f in faqs[:8]
        ])

        prompt = f"""You are a {personality} assistant.

{context_str}

User asked: "{user_message}"

Available knowledge (use only if relevant):
{faq_context}

Give a helpful, honest, 1–2 sentence response. If you can't answer well, politely offer to connect them with the team.
Sound friendly and human. Return ONLY the response text."""

        try:
            response = self.model.generate_content(prompt)
            text     = response.text.strip()
            return text if len(text) > 10 else "I'm happy to help! Could you tell me a bit more about what you're looking for?"
        except Exception as e:
            logger.error(f"[_vertical_fallback] error: {e}")
            return "I'm not sure I have the exact answer. Would you like me to connect you with the team?"

    # ═══════════════════════════════════════════════════════════════════
    # 6. GUARDRAILS
    # ═══════════════════════════════════════════════════════════════════

    def _guardrails(self, response_text: str, candidates: List[Dict]) -> str:
        """
        Post-generation quality checks:
        - Too short / empty → safe fallback
        - Contains "I don't know" without a candidate → safe fallback
        - Excessive length (>500 chars) → keep first 2 sentences
        """
        if not response_text or len(response_text) < 10:
            return "I'm not sure about that. Would you like me to connect you with the team?"

        if "i don't know" in response_text.lower() and not candidates:
            return "I'm not sure about that. Would you like me to connect you with the team?"

        if len(response_text) > 500:
            # Keep first 2 sentences
            sentences = re.split(r'(?<=[.!?])\s+', response_text)
            response_text = ' '.join(sentences[:2])

        return response_text

    # ═══════════════════════════════════════════════════════════════════
    # FOLLOW-UP DETECTION & QUERY ENRICHMENT
    # ═══════════════════════════════════════════════════════════════════

    # Phrases that almost always mean the message is continuing a prior topic
    _FOLLOWUP_STARTERS = (
        'how about', 'what about', 'and the ', 'and a ', 'and an ',
        'what are the ', 'what are its ', "what's the ", "what's its ",
        'tell me about the ', 'how much is the ', 'how much does the ',
        'what does the ', 'what is the ', 'is the ', 'does the ',
        'how about the ', 'same for ', 'same question for ', 'and for ',
        'the ', 'that one', 'this one', 'the same ',
    )

    def _is_followup(self, message: str, history: List[Dict]) -> bool:
        """
        Returns True when the current message is likely a referential follow-up
        that needs conversation history to resolve intent.

        Triggers on:
          - Messages starting with known continuation phrases
          - Very short messages (≤ 6 words) when a substantive prior answer exists
        """
        if not history:
            return False

        msg = message.strip().lower()

        # Explicit continuation openers
        for starter in self._FOLLOWUP_STARTERS:
            if msg.startswith(starter):
                return True

        # Short message + prior assistant answer exists → likely a follow-up
        words = msg.split()
        if len(words) <= 6:
            # Ignore pure greetings / acknowledgements
            GREETINGS = {'hi', 'hello', 'hey', 'thanks', 'thank', 'ok', 'okay',
                         'great', 'cool', 'bye', 'goodbye', 'yes', 'no', 'sure'}
            if not GREETINGS.issuperset(set(words)):
                last_bot = next(
                    (m.get('content', '') for m in reversed(history)
                     if m.get('role') != 'user'),
                    None
                )
                if last_bot and len(last_bot) > 40:
                    return True

        return False

    # Stop words excluded when extracting topic keywords from history
    _TOPIC_STOPS = {
        'what', 'how', 'is', 'are', 'the', 'a', 'an', 'do', 'does', 'can',
        'could', 'would', 'should', 'tell', 'me', 'about', 'much', 'many',
        'long', 'i', 'you', 'we', 'my', 'your', 'it', 'its', 'please',
        'hi', 'hey', 'and', 'or', 'for', 'in', 'on', 'at', 'with', 'to',
        'of', 'that', 'this', 'there', 'their', 'these', 'those', 'was',
        'were', 'been', 'have', 'has', 'had', 'get', 'got', 'also', 'just',
        'more', 'than', 'some', 'any', 'all', 'one', 'two', 'three',
    }

    def _resolve_query(self, message: str, history: List[Dict]) -> str:
        """
        For referential follow-up messages, builds an enriched search query
        by borrowing topic keywords from the previous conversation turns.

        Examples:
          history: "how much is the starter plan?" + answer
          message: "how about the pro plan?"
          → "how about the pro plan? pro plan pricing starter"

          history: "what features does the pro plan have?" + answer
          message: "and the agency one?"
          → "and the agency one? agency plan features pro"

        The enriched query is used ONLY for embedding search — the LLM always
        receives the original clean message.
        """
        if not self._is_followup(message, history):
            return message   # not a follow-up — use message as-is

        # Collect keywords from the last 2 user messages in history
        recent_user = [
            m.get('content', '').strip()
            for m in (history or [])[-6:]
            if m.get('role') == 'user' and m.get('content')
        ]

        topic_words: List[str] = []
        for past_msg in recent_user[-2:]:
            words = re.findall(r'\b[a-z]{3,}\b', past_msg.lower())
            topic_words.extend(
                w for w in words if w not in self._TOPIC_STOPS
            )

        # Also get keywords from the current message
        current_kws = [
            w for w in re.findall(r'\b[a-z]{3,}\b', message.lower())
            if w not in self._TOPIC_STOPS
        ]

        # Append only the *new* topic words (not already in current_kws)
        current_set  = set(current_kws)
        extra_topics = []
        seen: set    = set()
        for w in topic_words:
            if w not in current_set and w not in seen:
                seen.add(w)
                extra_topics.append(w)
            if len(extra_topics) >= 6:
                break

        if not extra_topics:
            return message

        enriched = message + ' ' + ' '.join(extra_topics)
        logger.debug(f"[ResolveQuery] '{message}' → '{enriched}'")
        return enriched

    # ═══════════════════════════════════════════════════════════════════
    # CONVERSATION CONTEXT
    # ═══════════════════════════════════════════════════════════════════

    def _build_context(self, conversation_history: List[Dict],
                       client_id: str = None,
                       current_message: str = None) -> str:
        """
        Build a rich prompt context from:
          1. Stored conversation summary (earlier context from DB)
          2. Last 8 turns as an explicit structured dialogue
          3. A follow-up annotation block when the current message is referential

        The explicit structure is critical for follow-up handling: the LLM
        sees not just raw turns but a labelled conversation thread and an
        explicit flag when the current message depends on prior context.
        """
        parts = []

        # ── 1. Earlier summary from DB ────────────────────────────────
        if client_id:
            try:
                import models as _m
                summary = _m.get_latest_conversation_summary(client_id)
                if summary:
                    parts.append(f"[Earlier in this conversation]\n{summary}")
            except Exception:
                pass

        # ── 2. Recent turns as structured dialogue ────────────────────
        if conversation_history:
            recent = conversation_history[-8:]   # last 8 turns — tighter than 15

            turns_lines = []
            for m in recent:
                role    = 'User' if m.get('role') == 'user' else 'Assistant'
                content = m.get('content', '').strip()
                if not content:
                    continue
                # Truncate long assistant answers to keep prompt lean
                if role == 'Assistant' and len(content) > 220:
                    content = content[:220] + '…'
                turns_lines.append(f"  {role}: {content}")

            if turns_lines:
                parts.append("[Conversation so far]\n" + "\n".join(turns_lines))

            # ── 3. Follow-up annotation ───────────────────────────────
            if current_message:
                recent_user_msgs = [
                    m.get('content', '').strip()
                    for m in recent
                    if m.get('role') == 'user' and m.get('content')
                ]

                if self._is_followup(current_message, conversation_history) and recent_user_msgs:
                    # Surface the most recent prior question explicitly so the
                    # LLM has no ambiguity about what topic is being continued.
                    prev_question = recent_user_msgs[-1]
                    parts.append(
                        "[Follow-up context]\n"
                        f"The user's current message (\"{current_message}\") is a follow-up "
                        f"or continuation.\n"
                        f"Their immediately preceding question was: \"{prev_question}\"\n"
                        f"Infer what they are now asking from the conversation thread above "
                        f"and answer it directly."
                    )

        return "\n\n".join(parts) if parts else ""

    def maybe_summarise(self, client_id: str,
                        conversation_history: List[Dict]) -> None:
        """Summarise every 6 messages and store in DB. Non-blocking."""
        if not self.enabled or not conversation_history or not client_id:
            return
        if len(conversation_history) % 6 != 0:
            return
        try:
            window = conversation_history[-6:]
            turns  = "\n".join([
                f"{'User' if m.get('role') == 'user' else 'Assistant'}: {m.get('content', '')}"
                for m in window if m.get('content')
            ])
            prompt = f"""Summarise this support conversation in 1–2 concise sentences.
Focus on what the user needed and what was resolved.

{turns}

Return ONLY the summary."""
            response = self.model.generate_content(prompt)
            summary  = response.text.strip()
            if summary and len(summary) > 10:
                import models as _m
                _m.save_conversation_summary(client_id, summary, len(conversation_history))
                logger.info(f"[Summarise] client={client_id} msgs={len(conversation_history)}")
        except Exception as e:
            logger.debug(f"[maybe_summarise] non-critical: {e}")

    # ═══════════════════════════════════════════════════════════════════
    # SMART UPLOAD PIPELINE
    # ═══════════════════════════════════════════════════════════════════

    def enrich_and_chunk(self, raw_items: List[Dict],
                         client_id: str) -> List[Dict]:
        """
        AI enrichment pipeline for uploaded content.
        Input:  list of {question, answer, category} (from CSV/PDF/Excel parser)
        Output: list of knowledge_base chunks ready for save_knowledge_chunks()

        Steps per item:
          1. Quality check (skip very short/empty)
          2. AI enrichment: generate tags + improved title
          3. Deduplication check (cosine vs existing embeddings)
          4. Embed + build chunk dict
        """
        if not raw_items:
            return []

        # Load existing embeddings for dedup
        existing_embeddings: List[Dict] = []
        if client_id and self.enabled:
            try:
                import models as _m
                existing_embeddings = _m.get_embeddings_for_client(client_id)
            except Exception:
                pass

        chunks = []
        seen_embeddings: List[list] = [e['embedding'] for e in existing_embeddings]

        for item in raw_items:
            question = (item.get('question') or '').strip()
            answer   = (item.get('answer')   or '').strip()

            # Quality gate
            if not question or not answer or len(answer) < 10:
                continue

            # Chunk long answers (>800 chars → split into ≤400-char chunks)
            content_chunks = self._split_content(answer)

            for idx, chunk_text in enumerate(content_chunks):
                chunk_id = str(uuid.uuid4())

                # AI enrichment (title + tags) — only first chunk gets full AI pass
                if self.enabled and idx == 0:
                    tags, ai_category = self._ai_enrich(question, chunk_text)
                else:
                    tags        = self._extract_tags(question)
                    ai_category = item.get('category', 'General')

                # Embed (document task type for stored content)
                embed_text = f"{question} {chunk_text}"
                embedding  = _embed(embed_text, task='retrieval_document')

                # Deduplication — skip if cosine > 0.92 with existing
                if embedding and seen_embeddings:
                    max_sim = max((_cosine(embedding, ex) for ex in seen_embeddings), default=0.0)
                    if max_sim > 0.92:
                        logger.debug(f"[Dedup] skipped chunk (sim={max_sim:.3f}): {question[:50]}")
                        continue

                # Quality score heuristic
                quality = self._quality_score(question, chunk_text)

                chunk = {
                    'kb_id':    chunk_id,
                    'title':    question if idx == 0 else f"{question} (part {idx + 1})",
                    'content':  chunk_text,
                    'type':     item.get('type', 'faq'),
                    'category': ai_category,
                    'tags':     tags,
                    'embedding': embedding,
                    'metadata': {
                        'source':        item.get('source', 'upload'),
                        'original_q':    question,
                        'chunk_index':   idx,
                        'total_chunks':  len(content_chunks),
                    },
                    'quality':  quality,
                }

                chunks.append(chunk)
                if embedding:
                    seen_embeddings.append(embedding)

        logger.info(f"[Enrich] client={client_id} input={len(raw_items)} output={len(chunks)}")
        return chunks

    def _split_content(self, text: str, max_len: int = 400) -> List[str]:
        """Split long text into sentence-aware chunks."""
        if len(text) <= max_len:
            return [text]
        sentences = re.split(r'(?<=[.!?])\s+', text)
        chunks    = []
        current   = ""
        for sent in sentences:
            if len(current) + len(sent) + 1 <= max_len:
                current = (current + " " + sent).strip()
            else:
                if current:
                    chunks.append(current)
                current = sent
        if current:
            chunks.append(current)
        return chunks if chunks else [text[:max_len]]

    def _ai_enrich(self, question: str, answer: str) -> Tuple[List[str], str]:
        """Use Gemini to generate tags and category. Returns (tags, category)."""
        if not self.enabled:
            return self._extract_tags(question), 'General'
        try:
            prompt = f"""Given this FAQ:
Q: {question}
A: {answer[:200]}

Return ONLY valid JSON:
{{"tags": ["tag1", "tag2", "tag3"], "category": "Billing"}}

tags: 2–5 short keyword tags
category: one of General | Billing | Support | Product | Policy | Sales | Technical"""
            response = self.model.generate_content(prompt)
            result   = self._parse_json(response.text)
            if result:
                return result.get('tags', [])[:5], result.get('category', 'General')
        except Exception:
            pass
        return self._extract_tags(question), 'General'

    def _extract_tags(self, text: str) -> List[str]:
        """Simple keyword extraction for tags when AI is unavailable."""
        stop = {'a', 'an', 'the', 'is', 'are', 'do', 'does', 'can', 'i', 'you',
                'we', 'my', 'your', 'what', 'how', 'when', 'where', 'why', 'to', 'of'}
        words = re.findall(r'\b[a-z]{3,}\b', text.lower())
        return list(dict.fromkeys(w for w in words if w not in stop))[:5]

    def _quality_score(self, question: str, answer: str) -> float:
        """Heuristic quality score 0.0–1.0 based on length and completeness."""
        score = 0.5
        if len(question) > 15:
            score += 0.15
        if len(answer) > 50:
            score += 0.15
        if answer.endswith(('.', '!', '?')):
            score += 0.1
        if '?' in question:
            score += 0.1
        return min(score, 1.0)

    # ═══════════════════════════════════════════════════════════════════
    # BACKWARD-COMPAT: Phase 1 methods (still used by analytics/admin)
    # ═══════════════════════════════════════════════════════════════════

    def find_best_faq(self, user_message: str, faqs: List[Dict],
                      client_id: str = None) -> Tuple[Optional[Dict], float]:
        """Phase 1 compat — delegates to embedding search + rerank."""
        candidates, scores = self._embedding_search(user_message, faqs, client_id)
        reranked            = self._rerank(user_message, candidates, scores)
        if reranked and scores:
            return reranked[0], scores[0]
        return None, 0.0

    def index_faqs(self, faqs: List[Dict], client_id: str) -> int:
        """Pre-index FAQ embeddings via Gemini (called after bulk upload)."""
        if not self.enabled or not client_id:
            return 0
        count = 0
        try:
            import models as _m
            for faq in faqs:
                fid = str(faq.get('id', ''))
                if fid and faq.get('question'):
                    vec = _embed(faq['question'], task='retrieval_document')
                    if vec:
                        _m.store_faq_embedding(client_id, fid, faq['question'], vec)
                        count += 1
            logger.info(f"[index_faqs] client={client_id} indexed={count}")
        except Exception as e:
            logger.error(f"[index_faqs] error: {e}")
        return count

    # ═══════════════════════════════════════════════════════════════════
    # PRIVATE HELPERS
    # ═══════════════════════════════════════════════════════════════════

    def _cache_key(self, msg: str, faq_id: str, vertical: str) -> str:
        raw = f"{msg.lower().strip()}|{faq_id}|{vertical}"
        return hashlib.md5(raw.encode()).hexdigest()

    def _make_fallback(self, answer: str = '') -> str:
        if not answer:
            return "I'm not sure about that. Would you like me to connect you with the team?"
        return (answer
                .replace(" I am ",   " I'm ")
                .replace(" You are ", " You're ")
                .replace(" it is ",  " it's ")
                .replace(" do not ", " don't ")
                .replace(" cannot ", " can't "))

    def _parse_json(self, text: str) -> Optional[Dict]:
        text = text.strip()
        if text.startswith('```'):
            text = re.sub(r'^```(?:json)?\s*|\s*```$', '', text, flags=re.DOTALL).strip()
        try:
            return json.loads(text)
        except Exception:
            m = re.search(r'\{.*\}', text, re.DOTALL)
            if m:
                try:
                    return json.loads(m.group(0))
                except Exception:
                    pass
        return None


# ── Singleton ─────────────────────────────────────────────────────────

_ai_helper: Optional[AIHelper] = None


def get_ai_helper(api_key: str, model_name: str = 'gemini-2.0-flash') -> AIHelper:
    """Get or create the AI helper singleton."""
    global _ai_helper
    if _ai_helper is None:
        _ai_helper = AIHelper(api_key, model_name)
    return _ai_helper
