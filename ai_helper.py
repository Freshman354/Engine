"""
AI Helper — Phase 4: Cost-Efficient Consolidated RAG Pipeline
=============================================================
Cost model: MAX 2 Gemini calls per user message (down from 4).

  Call 1  _combined_rewrite_intent  (ONLY for short/ambiguous follow-ups)
            → rewrites the query AND checks intent in one shot.
            → skipped entirely for standalone messages (saves a full call).

  Call 2  _rag_generate_and_polish  (ONE call: RAG answer + tone polish)
            → merged from the old separate _rag_generate + _polish_response.
            → uses IDK_FALLBACK grounding gate (replaces the old YES/NO self-check).
            → if model returns "IDK_FALLBACK", trigger fallback immediately.

Performance:
  - _cosine uses list comprehensions + zip (measurably faster on CPython).
  - _bm25_score uses a dict-comprehension tf_map (single pass).
  - Hard 50-candidate cap before any math — prevents CPU redlining on large KBs.

Memory:
  - Keep last 8 messages always (cheap, no API call).
  - Summarise only when estimated token count > 2 000 (avoids expensive LLM call on short chats).

Lead capture:
  - Scans history for '@' and name patterns BEFORE calling Gemini extraction.
  - Never asks for info the user already provided.

Preserved:
  - Vertical personalities, dynamic thresholds, category stickiness, BM25 hybrid rank.
  - Smart lead extraction (_extract_lead_info) and nudge builder (_build_lead_nudge).
  - enrich_and_chunk / find_best_faq / index_faqs backward compat.
  - Pure-Python math — no numpy / scipy.
"""

import google.generativeai as genai
import json
import logging
import re
import math
import hashlib
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

# ── Action / Tool Engine keyword maps ────────────────────────────────
# Pure keyword matching — zero LLM calls. Ordered from most specific
# to least specific so "pricing pdf" wins over bare "pricing".
_ACTION_KEYWORDS: Dict[str, List[str]] = {
    'pricing_request': [
        'send pricing', 'pricing pdf', 'email me pricing',
        'email the pricing', 'send me pricing', 'pricing details',
        'get pricing', 'share pricing',
    ],
    'demo_request': [
        'book a demo', 'schedule a demo', 'request a demo',
        'arrange a demo', 'try it', 'show me', 'see a demo',
        'want a demo', 'demo please', 'demo',
    ],
    'meeting_request': [
        'schedule a call', 'book a call', 'set up a call',
        'arrange a call', 'have a meeting', 'book a meeting',
        'schedule a meeting', 'call me', 'schedule', 'meeting',
    ],
    'contact_request': [
        'contact sales', 'talk to sales', 'speak to sales',
        'reach sales', 'contact someone', 'speak to a person',
        'talk to a human', 'human agent', 'real person',
    ],
}

# Human-friendly labels for each action (used in response messages)
_ACTION_LABELS: Dict[str, str] = {
    'demo_request':    'demo',
    'meeting_request': 'call',
    'pricing_request': 'pricing details',
    'contact_request': 'conversation with our team',
}


# Hard limit: only score the top N candidates retrieved from DB.
# Prevents CPU redlining when the knowledge base has thousands of entries.
_MAX_CANDIDATES = 50


# ─────────────────────────────────────────────────────────────────────
# Pure-Python math helpers (list-comprehension optimised)
# ─────────────────────────────────────────────────────────────────────

def _cosine(a: list, b: list) -> float:
    """
    Pure-Python cosine similarity using list comprehensions.
    Measurably faster than generator expressions on CPython due to
    reduced frame overhead — no numpy required.
    """
    pairs = list(zip(a, b))
    dot   = sum([x * y for x, y in pairs])
    mag_a = math.sqrt(sum([x * x for x in a]))
    mag_b = math.sqrt(sum([x * x for x in b]))
    return dot / (mag_a * mag_b) if mag_a and mag_b else 0.0


def _bm25_score(query_tokens: List[str], doc_tokens: List[str],
                avg_doc_len: float = 40.0,
                k1: float = 1.5, b: float = 0.75,
                corpus_size: int = 100) -> float:
    """
    BM25-lite: pure-Python, single-document BM25 approximation.
    tf_map built with a dict comprehension (single pass, faster than loop).
    Returns a normalised score in [0, 1].
    """
    if not query_tokens or not doc_tokens:
        return 0.0

    doc_len = len(doc_tokens)
    # Single-pass dict comprehension — avoids repeated dict.get() updates
    tf_map  = {tok: doc_tokens.count(tok) for tok in set(doc_tokens)}

    df  = max(1, corpus_size // 10)
    idf = math.log((corpus_size - df + 0.5) / (df + 0.5) + 1)

    score = sum(
        idf * (tf_map[term] * (k1 + 1)) /
              (tf_map[term] + k1 * (1 - b + b * doc_len / avg_doc_len))
        for term in query_tokens
        if term in tf_map
    )
    return min(score / 10.0, 1.0)


def _tokenize(text: str) -> List[str]:
    """Lowercase word tokeniser — strips punctuation, returns word list."""
    return re.findall(r'\b[a-z]{2,}\b', text.lower())


def _embed(text: str, task: str = 'retrieval_document') -> list:
    """
    Embed text using Gemini text-embedding-004.
    task: 'retrieval_document' for FAQs/chunks, 'retrieval_query' for user messages.
    Returns [] on failure — callers degrade gracefully.
    """
    if not text or not text.strip():
        return []
    try:
        result = genai.embed_content(
            model='models/text-embedding-004',
            content=text.strip()[:2048],
            task_type=task,
        )
        return result['embedding']
    except Exception as _e:
        logger.debug(f"[_embed] error: {_e}")
        return []


# ─────────────────────────────────────────────────────────────────────
# AIHelper — Phase 3
# ─────────────────────────────────────────────────────────────────────

class AIHelper:
    """Lumvi AI Helper — Phase 3: 6-Stage Advanced RAG Pipeline."""

    def __init__(self, api_key: str, model_name: str = 'gemini-2.0-flash'):
        self.api_key    = api_key
        self.model_name = model_name
        self.enabled    = bool(api_key and api_key.strip())

        self.personalities = {
            'general': {
                'tone':             "warm, friendly, and helpful — like a knowledgeable colleague",
                'polish_hint':      "Keep it conversational and approachable. Use plain English.",
                'lead_keywords':    ['demo', 'speak to someone', 'contact me', 'call me',
                                     'book a call', 'human', 'agent', 'talk to sales'],
                'pricing_keywords': _GLOBAL_PRICING_KW,
            },
            'real_estate': {
                'tone':             "enthusiastic, reassuring, and professional — make buying/renting feel exciting",
                'polish_hint':      "Use upbeat, encouraging language. Mention next steps naturally.",
                'lead_keywords':    ['viewing', 'appointment', 'book a tour', 'schedule',
                                     'visit the property', 'speak to agent'],
                'pricing_keywords': _GLOBAL_PRICING_KW + ['rent', 'mortgage', 'deposit'],
            },
            'saas': {
                'tone':             "patient, clear, and solution-oriented — great at explaining features",
                'polish_hint':      "Be technically precise. Use bullet points when listing 3+ features.",
                'lead_keywords':    ['demo', 'trial', 'onboarding', 'integration',
                                     'speak to sales', 'account manager'],
                'pricing_keywords': _GLOBAL_PRICING_KW + ['monthly fee', 'annual plan', 'seats'],
            },
            'ecommerce': {
                'tone':             "fast, friendly, and shopper-focused — keep it quick and helpful",
                'polish_hint':      "Short sentences. Get to the point fast. Always end with a CTA if relevant.",
                'lead_keywords':    ['order status', 'return', 'refund', 'speak to support'],
                'pricing_keywords': _GLOBAL_PRICING_KW + ['shipping cost', 'discount', 'promo'],
            },
            'healthcare': {
                'tone':             "calm, empathetic, and professional — never give medical advice",
                'polish_hint':      "Warm but careful tone. Never diagnose. Direct to professionals when needed.",
                'lead_keywords':    ['appointment', 'booking', 'schedule', 'consultation', 'see a doctor'],
                'pricing_keywords': _GLOBAL_PRICING_KW + ['consultation fee', 'insurance'],
            },
            'law_firm': {
                'tone':             "formal, precise, trustworthy, and cautious — excellent at intake",
                'polish_hint':      "Formal register. Never give legal advice. Use passive voice sparingly.",
                'lead_keywords':    ['consultation', 'case review', 'speak to lawyer', 'legal advice'],
                'pricing_keywords': _GLOBAL_PRICING_KW + ['retainer', 'hourly rate', 'flat fee'],
            },
            'dental': {
                'tone':             "friendly, reassuring, and professional — make patients feel at ease",
                'polish_hint':      "Reassure first, inform second. Avoid clinical jargon unless asked.",
                'lead_keywords':    ['appointment', 'booking', 'consultation', 'see a dentist'],
                'pricing_keywords': _GLOBAL_PRICING_KW + ['treatment cost', 'insurance', 'payment plan'],
            },
            'gym': {
                'tone':             "energetic, motivating, and supportive — like a great personal trainer",
                'polish_hint':      "High energy, positive framing. Use active verbs.",
                'lead_keywords':    ['membership', 'sign up', 'trial', 'class', 'book a session'],
                'pricing_keywords': _GLOBAL_PRICING_KW + ['membership fee', 'monthly', 'annual'],
            },
        }

        self._response_cache: Dict[str, str] = {}

        if self.enabled:
            try:
                genai.configure(api_key=api_key)
                self.model = genai.GenerativeModel(model_name)
                logger.info(
                    f"✅ AI Helper Phase 3 ready | model={model_name} | "
                    f"embed=text-embedding-004 | pipeline=6-stage | hybrid_rank=ON"
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
        Cost-efficient 2-call pipeline:
          Call 1 (conditional): _combined_rewrite_intent — only for short/ambiguous
                                follow-ups. Skipped entirely for standalone messages.
          Call 2: _rag_generate_and_polish — RAG answer + tone + IDK_FALLBACK gate.

        Unchanged: intent detection (keyword tier), embedding search, hybrid rerank,
                   lead extraction, dynamic thresholds, category stickiness, cache.
        """
        if not user_message or not user_message.strip():
            return {'response': "How can I help you today?", 'method': 'empty',
                    'confidence': 1.0, 'is_lead': False, 'lead_metadata': None, 'action': None}

        history = conversation_history or []

        # ── Preprocess ────────────────────────────────────────────────
        clean = self._preprocess(user_message)
        logger.debug(f"[Pipeline] start | msg='{clean[:60]}' | vertical={vertical}")

        # ── ACTION ENGINE (Step 3) — runs before RAG, zero LLM cost ───
        # If the user's message maps to a concrete action (demo, meeting,
        # pricing, contact), handle it immediately and skip the RAG pipeline.
        action_intent = self.detect_action_intent(clean)
        if action_intent.get('action'):
            # Scan history for already-known email / name (Step 4 — anti-repetition)
            quick_meta   = self._extract_lead_info(clean, history)
            user_context = {
                'email':    quick_meta.get('email'),
                'name':     quick_meta.get('name'),
                'vertical': vertical,
            }
            action_result = self.handle_detected_action(action_intent['action'], user_context)
            logger.info(f"[Action] short-circuiting RAG | action={action_intent['action']}")
            return {
                'response':      action_result['message'],
                'method':        f"action:{action_intent['action']}",
                'confidence':    1.0,
                'is_lead':       action_intent['action'] in ('demo_request', 'meeting_request', 'contact_request'),
                'lead_metadata': quick_meta,
                'action':        action_result,   # full structured payload for caller
            }

        # ── Keyword intent detection (zero cost) ──────────────────────
        intent = self.detect_intent(clean, lead_triggers or [], vertical)
        if intent.get('is_lead') and intent.get('confidence', 0) >= 0.65:
            logger.info(
                f"[Lead] client={client_id} vertical={vertical} "
                f"score={intent.get('score', 0):.1f} conf={intent['confidence']:.2f}"
            )
            lead_meta = self._extract_lead_info(clean, history)
            logger.info(
                f"[LeadMeta] name={lead_meta.get('name')} "
                f"email={lead_meta.get('email')} topic={lead_meta.get('interest_topic')}"
            )
            nudge = self._build_lead_nudge(lead_meta, vertical, clean)
            return {
                'response':      nudge,
                'method':        'lead_detection',
                'confidence':    intent['confidence'],
                'is_lead':       True,
                'lead_metadata': lead_meta,
            }

        # ── Dynamic threshold (pricing gets lower bar) ─────────────────
        msg_lower        = clean.lower()
        is_sales_query   = (intent.get('intent') == 'lead_request' or
                            any(kw in msg_lower for kw in _GLOBAL_PRICING_KW))
        vector_threshold = 0.22 if is_sales_query else 0.28
        logger.debug(f"[Threshold] sales={is_sales_query} threshold={vector_threshold}")

        # ── CALL 1 (conditional): Combined rewrite + intent ────────────
        # Only fires for short or ambiguous follow-ups.
        # Standalone messages (≥10 words, not a follow-up) skip this entirely.
        word_count  = len(clean.split())
        is_followup = self._is_followup(clean, history)
        if (word_count < 10 or is_followup) and history and self.enabled:
            search_query = self._combined_rewrite_intent(clean, history)
        else:
            search_query = self._resolve_query(clean, history)
        logger.debug(f"[Rewrite] '{clean[:40]}' → '{search_query[:60]}'")

        # ── Embedding search (hard 50-candidate cap enforced inside) ───
        candidates, vector_scores = self._embedding_search(
            search_query, faqs, client_id, threshold=vector_threshold
        )
        logger.debug(f"[Search] hits={len(candidates)} top={vector_scores[0]:.3f if vector_scores else 0}")

        # ── Hybrid rerank with category stickiness ────────────────────
        last_category           = self._last_response_category(history)
        hybrid_ranked, hybrid_scores = self._hybrid_rerank(
            search_query, candidates, vector_scores, last_category=last_category
        )
        logger.debug(f"[Hybrid] top={hybrid_scores[0]:.3f if hybrid_scores else 0}")

        # ── Cache check ────────────────────────────────────────────────
        top_id    = str(hybrid_ranked[0].get('kb_id', hybrid_ranked[0].get('id', ''))) if hybrid_ranked else ''
        cache_key = self._cache_key(clean, top_id, vertical)
        if cache_key in self._response_cache:
            logger.debug(f"[Cache HIT] key={cache_key[:10]}…")
            return {
                'response':      self._response_cache[cache_key],
                'method':        'cache',
                'confidence':    hybrid_scores[0] if hybrid_scores else 0.8,
                'is_lead':       False,
                'lead_metadata': None,
                'action':        None,
            }

        # ── Build conversation context string ─────────────────────────
        context_str = self._build_context(history, client_id, clean)

        # ── CALL 2: Merged RAG + Polish (with IDK_FALLBACK gate) ───────
        if hybrid_ranked and self.enabled:
            final, confidence, method = self._rag_generate_and_polish(
                clean, hybrid_ranked, hybrid_scores, vertical, context_str
            )
            # IDK_FALLBACK means model confirmed it can't answer from context
            if final == 'IDK_FALLBACK':
                logger.info(f"[IDK] model returned IDK_FALLBACK — routing to fallback")
                final      = self._vertical_fallback(clean, faqs[:8], vertical, context_str)
                confidence = 0.3
                method     = 'idk_fallback'
        elif self.enabled:
            final      = self._vertical_fallback(clean, faqs[:8], vertical, context_str)
            confidence = 0.35
            method     = 'vertical_fallback'
        else:
            final      = self._make_fallback(faqs[0].get('answer', '') if faqs else '')
            confidence = 0.0
            method     = 'static_fallback'

        # ── Guardrails + cache ─────────────────────────────────────────
        final = self._guardrails(final, hybrid_ranked)
        if confidence > 0.4:
            self._response_cache[cache_key] = final

        logger.info(
            f"[Pipeline] done | method={method} conf={confidence:.2f} "
            f"chunk={top_id[:12] if top_id else 'none'} calls≤2"
        )
        return {
            'response':      final,
            'method':        method,
            'confidence':    confidence,
            'is_lead':       False,
            'lead_metadata': None,
            'action':        None,
        }

    # ═══════════════════════════════════════════════════════════════════
    # STAGE 1 — PREPROCESSING
    # ═══════════════════════════════════════════════════════════════════

    def _preprocess(self, text: str) -> str:
        text = text.strip()
        text = re.sub(r'\s+', ' ', text)
        return text

    # ═══════════════════════════════════════════════════════════════════
    # STAGE 1b — INTENT DETECTION
    # ═══════════════════════════════════════════════════════════════════

    # ═══════════════════════════════════════════════════════════════════
    # ACTION / TOOL ENGINE  (Steps 1 & 2)
    # ═══════════════════════════════════════════════════════════════════

    @staticmethod
    def detect_action_intent(message: str) -> Dict:
        """
        Step 1 — Keyword-based action detection. Zero LLM calls.

        Scans the lowercased message against _ACTION_KEYWORDS in specificity
        order (most specific phrases first so "pricing pdf" wins over "demo").

        Returns:
            {'action': 'demo_request'}   — when an action is matched
            {'action': None}             — when nothing matches
        """
        msg = message.lower().strip()
        for action, keywords in _ACTION_KEYWORDS.items():
            for kw in keywords:                 # keywords already ordered specific→broad
                if kw in msg:
                    logger.info(f"[Action] detected='{action}' via keyword='{kw}'")
                    return {'action': action}
        return {'action': None}

    @staticmethod
    def handle_detected_action(action: str, user_context: Dict) -> Dict:
        """
        Step 2 — Return a structured action response.

        user_context keys (all optional):
            email    — str | None   already-known email from conversation history
            name     — str | None   already-known name
            vertical — str          current bot vertical (for tone)

        Steps 4 integration — Email gate:
            For action types that need follow-up (demo, meeting, contact),
            if email is not yet known the response asks for it instead of
            confirming the action. This prevents asking twice if the user
            already shared their email.

        Returns a structured dict; the caller decides how to surface it.
        """
        email    = (user_context.get('email') or '').strip()
        name     = (user_context.get('name')  or '').strip()
        vertical = user_context.get('vertical', 'general')
        label    = _ACTION_LABELS.get(action, 'request')

        # Personalise greeting if we have the user's name
        greeting = f"Thanks, {name.split()[0]}! " if name else "Great! "

        # Actions that need a human follow-up — gate on email
        NEEDS_EMAIL = {'demo_request', 'meeting_request', 'contact_request'}

        if action == 'pricing_request':
            # Pricing can be fulfilled without collecting a lead first
            response_msg = (
                f"{greeting}I'd be happy to send over the pricing details. "
                f"What's the best email address to send them to?"
                if not email else
                f"{greeting}I'll send the pricing details to {email} right away!"
            )

        elif action in NEEDS_EMAIL and not email:
            # Email gate — politely ask before confirming the action
            response_msg = (
                f"{greeting}I can arrange a {label} for you. "
                f"Could you share your email address so our team can reach you?"
            )

        elif action == 'demo_request':
            response_msg = (
                f"{greeting}I've noted your demo request. "
                f"Our team will reach out to {email} within one business day to confirm the time."
            )

        elif action == 'meeting_request':
            response_msg = (
                f"{greeting}I've logged your meeting request. "
                f"Someone from our team will contact you at {email} to schedule a convenient time."
            )

        elif action == 'contact_request':
            response_msg = (
                f"{greeting}I'll connect you with our team. "
                f"They'll reach out to {email} shortly."
            )

        else:
            response_msg = (
                f"{greeting}I've received your {label} request and our team will be in touch soon!"
            )

        logger.info(
            f"[ActionHandler] action={action} has_email={bool(email)} "
            f"vertical={vertical} → '{response_msg[:60]}…'"
        )

        return {
            'type':    'action',
            'action':  action,
            'message': response_msg,
        }

    # ═══════════════════════════════════════════════════════════════════
    # STAGE 1b — INTENT DETECTION (lead capture)
    # ═══════════════════════════════════════════════════════════════════

    def detect_intent(self, user_message: str, lead_triggers: List[str],
                      vertical: str = 'general') -> Dict:
        """
        Three-tier intent detection:
          Tier 1 — Simple intents (greeting/gratitude/bye): free keyword match
          Tier 2 — Keyword lead scoring (strong + pricing + custom triggers)
          Tier 3 — Gemini confirmation for borderline scores (2.5 ≤ score < 5)
        """
        msg      = user_message.lower().strip()
        vert_cfg = self.personalities.get(vertical, self.personalities['general'])

        # Tier 1 — Zero-cost simple intents
        for intent_name, keywords in _SIMPLE_INTENTS.items():
            if any(msg == k or msg.startswith(k) for k in keywords):
                return {'intent': intent_name, 'is_lead': False, 'confidence': 0.97, 'score': 0}

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
                prompt = (
                    f'Is this a lead request (user wants human contact, a demo, or to buy)?\n'
                    f'Message: "{user_message}"\n'
                    f'Triggers: {", ".join(lead_triggers)}\n'
                    f'Return ONLY JSON: {{"is_lead": true, "confidence": 0.75}}'
                )
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
    # SMART LEAD INFO EXTRACTION
    # ═══════════════════════════════════════════════════════════════════

    def _extract_lead_info(self, user_message: str,
                           conversation_history: List[Dict]) -> Dict:
        """
        Scan the current message AND conversation history for any lead data
        the user has already shared. Returns a dict with four fields:

            {
              "name":           "Sarah Johnson" | null,
              "email":          "sarah@company.com" | null,
              "phone":          "+1 555 000 0000" | null,
              "interest_topic": "Agency plan pricing" | null
            }

        Strategy:
          Pass 1 — Pure-Python regex over the message + last 8 history turns.
                   Fast, free, works for clearly formatted data (emails, phones).
          Pass 2 — Gemini extraction for everything Pass 1 missed
                   (names buried in prose, implied interests, informal phone formats).

        Never raises — always returns a dict (fields may be None).
        Falls back to whatever Pass 1 found if Gemini is unavailable or fails.
        """
        # Collect text to scan: current message + last 8 turns
        history     = conversation_history or []
        recent_turns = [
            m.get('content', '').strip()
            for m in history[-8:]
            if m.get('content')
        ]
        all_text = ' '.join(recent_turns + [user_message])

        # ── Pass 1: Regex extraction ───────────────────────────────────
        extracted: Dict = {'name': None, 'email': None, 'phone': None, 'interest_topic': None}

        # Email — unambiguous
        email_match = re.search(
            r'\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b', all_text
        )
        if email_match:
            extracted['email'] = email_match.group(0)

        # Phone — international and local formats
        phone_match = re.search(
            r'(\+?[\d][\d\s\-().]{7,14}\d)', all_text
        )
        if phone_match:
            candidate = re.sub(r'[\s\-().]', '', phone_match.group(1))
            if 7 <= len(candidate) <= 15:
                extracted['phone'] = phone_match.group(1).strip()

        # Name — "my name is X" / "I'm X" / "this is X" patterns
        name_match = re.search(
            r"(?:my name is|i(?:'?m| am)|this is|call me)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)",
            all_text, re.IGNORECASE
        )
        if name_match:
            extracted['name'] = name_match.group(1).strip().title()

        logger.debug(f"[LeadExtract] Pass1 → {extracted}")

        # ── Pass 2: Gemini extraction for the rest ─────────────────────
        if not self.enabled:
            return extracted

        # Only call Gemini if something is still missing
        still_missing = [k for k, v in extracted.items() if v is None]
        if not still_missing:
            return extracted

        # Build a compact conversation snippet for context
        convo_snippet = '\n'.join([
            f"  {'User' if m.get('role') == 'user' else 'Bot'}: {m.get('content', '')[:150]}"
            for m in history[-6:]
            if m.get('content')
        ] + [f"  User: {user_message}"])

        prompt = f"""You are a lead data extractor. Analyze the conversation below and extract contact information the user has shared.

Conversation:
{convo_snippet}

Extract the following fields if present in the conversation. Use null (not "null") if absent.

Return ONLY valid JSON — no explanation, no markdown:
{{
  "name":           <string or null>,
  "email":          <string or null>,
  "phone":          <string or null>,
  "interest_topic": <short phrase describing what the user wants, or null>
}}

Rules:
- name: The user's real name (not a username). Infer from greetings like "I'm Sarah" or "My name is John".
- email: A valid email address. Must contain @.
- phone: A phone number in any format. Include country code if given.
- interest_topic: 3–8 words summarising what the user wants (e.g. "Agency plan pricing", "Book a dental appointment").
- Do NOT invent information. Only extract what the user actually said."""

        try:
            resp   = self.model.generate_content(prompt)
            result = self._parse_json(resp.text)
            if result and isinstance(result, dict):
                # Merge: Gemini fills in what Pass 1 missed; Pass 1 values win on conflict
                for field in ('name', 'email', 'phone', 'interest_topic'):
                    ai_val = result.get(field)
                    if ai_val and ai_val not in ('null', 'None', '') and extracted[field] is None:
                        extracted[field] = str(ai_val).strip()
                logger.info(
                    f"[LeadExtract] Pass2 merge → "
                    f"name={extracted['name']} email={extracted['email']} "
                    f"phone={extracted['phone']} topic={extracted['interest_topic']}"
                )
        except Exception as e:
            logger.debug(f"[LeadExtract] Gemini pass failed (non-critical): {e}")

        return extracted

    def _build_lead_nudge(self, lead_meta: Dict, vertical: str,
                          user_message: str) -> str:
        """
        Build a personalised nudge that:
          1. Acknowledges what's already known (name / topic)
          2. Asks for the single most important MISSING piece of info
             Priority: email > name > phone

        Anti-repetition: the lead_meta passed in is already populated by
        _extract_lead_info which scanned conversation history — so if the
        user already gave their email earlier, lead_meta.email will be set
        and we won't ask for it again.

        Pure-Python — no Gemini call.
        """
        name  = lead_meta.get('name')
        email = lead_meta.get('email')
        phone = lead_meta.get('phone')
        topic = lead_meta.get('interest_topic')

        # ── Greeting ──────────────────────────────────────────────────
        parts = []
        if name:
            parts.append(f"Thanks, {name.split()[0]}!")
        if topic:
            parts.append(f"Happy to help with your {topic.lower()} inquiry.")
        elif not name:
            parts.append("I'd love to help with that!")
        greeting = ' '.join(parts)

        # ── Vertical action phrases ────────────────────────────────────
        action_phrases = {
            'real_estate': "have one of our agents reach out",
            'saas':        "have our team send you the details",
            'ecommerce':   "get you sorted quickly",
            'healthcare':  "have someone from our team follow up",
            'law_firm':    "arrange a confidential consultation",
            'dental':      "book you in with our team",
            'gym':         "get you set up with a trial",
            'general':     "have our team reach out",
        }
        action = action_phrases.get(vertical, action_phrases['general'])

        # ── Priority nudge: email > name > phone > all present ─────────
        if not email:
            nudge = (
                f"{greeting} To {action}, "
                f"what's the best email address to reach you at?"
            )
        elif not name:
            nudge = (
                f"{greeting} Just so we can personalise things — "
                f"what's your name?"
            )
        elif not phone:
            nudge = (
                f"{greeting} Our team will be in touch at {email}. "
                f"Would you also like to share a phone number for a faster response?"
            )
        else:
            nudge = (
                f"{greeting} Perfect — our team will be in touch at {email} very soon!"
            )

        logger.debug(
            f"[Nudge] vertical={vertical} name={bool(name)} "
            f"email={bool(email)} phone={bool(phone)} → '{nudge[:60]}…'"
        )
        return nudge

    # ═══════════════════════════════════════════════════════════════════
    # CALL 1 — COMBINED REWRITE + INTENT (consolidated from _rewrite_query)
    # ═══════════════════════════════════════════════════════════════════

    def _combined_rewrite_intent(self, user_message: str,
                                  conversation_history: List[Dict]) -> str:
        """
        Single lightweight Gemini call — replaces the old _rewrite_query.
        Only fires for short (<10 words) or follow-up messages.
        Standalone messages skip this entirely → zero cost for that turn.
        Falls back to _resolve_query (pure-Python) on any error.
        """
        if not conversation_history:
            return user_message

        recent = conversation_history[-6:]
        turns  = [
            f"  {'User' if m.get('role') == 'user' else 'Bot'}: {m.get('content', '').strip()[:150]}"
            for m in recent if m.get('content', '').strip()
        ]
        if not turns:
            return user_message

        convo_snippet = "\n".join(turns)
        try:
            prompt = (
                f"Given this conversation:\n{convo_snippet}\n\n"
                f"User's latest message: \"{user_message}\"\n\n"
                f"Rewrite into a standalone search query (5-15 words). "
                f"No filler phrases. Return ONLY the rewritten query."
            )
            response  = self.model.generate_content(prompt)
            rewritten = response.text.strip().strip('"\'')
            if not rewritten or len(rewritten) < 5 or len(rewritten) > 200:
                raise ValueError(f"bad length: {len(rewritten)}")
            if rewritten.lower().startswith(('i ', 'can you', 'please', 'could you')):
                raise ValueError("starts with filler")
            logger.debug(f"[Call1] rewrite OK: '{rewritten[:60]}'")
            return rewritten
        except Exception as e:
            logger.debug(f"[Call1] failed ({e}), falling back to keyword enrichment")
            return self._resolve_query(user_message, conversation_history)

    def _rewrite_query(self, user_message: str,
                       conversation_history: List[Dict]) -> str:
        """Backward-compat alias → _combined_rewrite_intent."""
        return self._combined_rewrite_intent(user_message, conversation_history)

    # ═══════════════════════════════════════════════════════════════════
    # STAGE 4 — EMBEDDING SEARCH
    # ═══════════════════════════════════════════════════════════════════

    def _embedding_search(self, search_query: str, faqs: List[Dict],
                          client_id: str = None,
                          threshold: float = 0.28) -> Tuple[List[Dict], List[float]]:
        """
        Retrieve top-8 candidates using Gemini cosine similarity.
        threshold: dynamic — 0.22 for sales/pricing, 0.28 for general queries.
        Search order:
          1. knowledge_base chunks (preferred)
          2. Legacy FAQ embeddings (lazy-indexed on first query)
          3. Keyword overlap fallback
        """
        if not self.enabled:
            return [], []

        query_vec = _embed(search_query, task='retrieval_query')

        # ── 1. knowledge_base chunks (hard cap: top 50 before math) ──────
        if client_id and query_vec:
            try:
                import models as _m
                kb_chunks = _m.get_relevant_knowledge(client_id, query_vec, limit=_MAX_CANDIDATES)
                if kb_chunks:
                    # Cap before expensive cosine loops
                    kb_chunks = kb_chunks[:_MAX_CANDIDATES]
                    scored = [
                        ({
                            'id':       chunk.get('kb_id', chunk.get('id', '')),
                            'kb_id':    chunk.get('kb_id', chunk.get('id', '')),
                            'question': chunk.get('title', ''),
                            'answer':   chunk.get('content', ''),
                            'category': chunk.get('category', 'General'),
                            'type':     chunk.get('type', 'faq'),
                        }, _cosine(query_vec, chunk['embedding']) if chunk.get('embedding')
                           else max(0.0, 0.5 - (i * 0.05)))
                        for i, chunk in enumerate(kb_chunks)
                    ]
                    scored = [(c, s) for c, s in scored if s > threshold]
                    if scored:
                        scored.sort(key=lambda x: x[1], reverse=True)
                        logger.debug(f"[KB] cap={_MAX_CANDIDATES} threshold={threshold} top={scored[0][1]:.3f} hits={len(scored)}")
                        return [s[0] for s in scored[:8]], [s[1] for s in scored[:8]]
            except Exception as _e:
                logger.warning(f"[Search] KB error: {_e}")

        # ── 2. Legacy FAQ embeddings ───────────────────────────────────
        if not faqs:
            return [], []

        if client_id and query_vec:
            try:
                import models as _m
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
                    # Cap before math loop
                    capped  = list(stored.items())[:_MAX_CANDIDATES]
                    scored  = [
                        (faq_idx[fid], _cosine(query_vec, emb))
                        for fid, emb in capped
                        if fid in faq_idx and _cosine(query_vec, emb) > threshold
                    ]
                    scored.sort(key=lambda x: x[1], reverse=True)
                    if scored:
                        logger.debug(f"[FAQ Embed] cap={_MAX_CANDIDATES} threshold={threshold} top={scored[0][1]:.3f} hits={len(scored)}")
                        return [s[0] for s in scored[:8]], [s[1] for s in scored[:8]]
            except Exception as _e:
                logger.warning(f"[Search] FAQ embed error: {_e}")

        # ── 3. Keyword overlap fallback ────────────────────────────────
        q_words = set(search_query.lower().split())
        scored  = []
        for faq in faqs:
            combined = (faq.get('question', '') + ' ' + faq.get('answer', '')).lower()
            overlap  = len(q_words & set(combined.split())) / max(len(q_words), 1)
            if overlap > 0:
                scored.append((faq, overlap))
        scored.sort(key=lambda x: x[1], reverse=True)
        if scored:
            logger.debug(f"[Keyword] hits={len(scored)} top={scored[0][1]:.3f}")
            return [s[0] for s in scored[:8]], [s[1] for s in scored[:8]]

        return [], []

    # ═══════════════════════════════════════════════════════════════════
    # STAGE 5 — HYBRID RERANK (NEW — replaces _rerank)
    # ═══════════════════════════════════════════════════════════════════

    def _hybrid_rerank(self, search_query: str,
                       candidates: List[Dict],
                       vector_scores: List[float],
                       last_category: Optional[str] = None
                       ) -> Tuple[List[Dict], List[float]]:
        """
        Combines Gemini vector score + BM25-lite keyword score + category stickiness boost.

        Category stickiness (Enhancement 2):
          If a chunk's category matches the last successfully answered topic,
          apply a 15% boost to its hybrid score. This keeps the bot focused on
          the current thread (e.g. staying in 'Billing' for invoice follow-ups).

        Score formula:
          hybrid = 0.65 × vector_score + 0.30 × bm25_score + 0.05 × length_norm
          hybrid *= 1.15  if chunk.category == last_category  (stickiness boost)

        Returns (ranked_candidates, ranked_scores) — both sorted descending.
        """
        if not candidates:
            return [], []

        query_tokens = _tokenize(search_query)
        all_doc_lengths = []
        for cand in candidates:
            doc_text = (cand.get('question', '') + ' ' + cand.get('answer', cand.get('content', '')))
            all_doc_lengths.append(len(_tokenize(doc_text)))
        avg_doc_len = sum(all_doc_lengths) / max(len(all_doc_lengths), 1)

        # Normalise last_category for comparison
        active_category = (last_category or '').strip().lower()

        scored = []
        for i, cand in enumerate(candidates):
            vec_score = vector_scores[i] if i < len(vector_scores) else 0.0

            doc_text   = (cand.get('question', '') + ' ' + cand.get('answer', cand.get('content', '')))
            doc_tokens = _tokenize(doc_text)
            kw_score   = _bm25_score(
                query_tokens, doc_tokens,
                avg_doc_len=avg_doc_len,
                corpus_size=max(len(candidates), 10)
            )

            doc_len_chars = len(cand.get('answer', cand.get('content', '')))
            length_norm   = min(doc_len_chars / 400.0, 1.0) * 0.05

            hybrid = (vec_score * 0.65) + (kw_score * 0.30) + length_norm

            # ── Category stickiness boost ──────────────────────────────
            cand_category = (cand.get('category') or '').strip().lower()
            sticky        = False
            if active_category and cand_category and cand_category == active_category:
                hybrid *= 1.15
                sticky  = True

            scored.append((cand, hybrid, vec_score, kw_score, sticky))
            logger.debug(
                f"[Hybrid] '{cand.get('question', '')[:40]}' "
                f"vec={vec_score:.3f} bm25={kw_score:.3f} hybrid={hybrid:.3f}"
                f"{' [sticky+15%]' if sticky else ''}"
            )

        scored.sort(key=lambda x: x[1], reverse=True)
        logger.info(
            f"[Stage5/Hybrid] top_hybrid={scored[0][1]:.3f} "
            f"vec={scored[0][2]:.3f} bm25={scored[0][3]:.3f} "
            f"sticky={scored[0][4]} active_cat='{active_category}' n={len(scored)}"
        )
        return [s[0] for s in scored], [s[1] for s in scored]

    def _last_response_category(self, history: List[Dict]) -> Optional[str]:
        """
        Scan conversation history (most recent first) to find the category of
        the last knowledge chunk that was successfully used in a response.

        We store category in assistant messages via a metadata convention:
        if the message contains '[cat:<Category>]' anywhere, we extract it.
        Otherwise we fall back to scanning assistant message content for
        category-like keywords (Billing, Support, Product, etc.).

        Returns None when no active category can be determined.
        """
        if not history:
            return None

        # Known categories from enrich pipeline
        KNOWN_CATS = {
            'billing', 'support', 'product', 'policy',
            'sales', 'technical', 'general',
        }

        for msg in reversed(history):
            if msg.get('role') in ('assistant', 'model'):
                content = msg.get('content', '') or ''

                # Explicit tag written by _rag_generate (see below)
                tag_match = re.search(r'\[cat:([^\]]+)\]', content)
                if tag_match:
                    return tag_match.group(1).strip()

                # Keyword heuristic fallback
                content_lower = content.lower()
                for cat in KNOWN_CATS:
                    if cat in content_lower:
                        return cat.title()

        return None
    # ═══════════════════════════════════════════════════════════════════
    # CALL 2 — MERGED RAG + POLISH with IDK_FALLBACK grounding gate
    # ═══════════════════════════════════════════════════════════════════

    def _rag_generate_and_polish(self, user_message: str, hybrid_ranked: List[Dict],
                                  hybrid_scores: List[float], vertical: str,
                                  context_str: str) -> Tuple[str, float, str]:
        """
        Single Gemini call replacing the old _rag_generate + _polish_response.

        IDK_FALLBACK grounding gate (replaces the old YES/NO self-check call):
          If the retrieved context does not contain the answer, the model returns
          the literal string 'IDK_FALLBACK'. The caller catches this and routes
          to fallback — no separate validation call, no polished hallucination.

        Returns (response_text, confidence, method_tag).
        """
        vert_cfg    = self.personalities.get(vertical, self.personalities['general'])
        personality = vert_cfg['tone']
        polish_hint = vert_cfg.get('polish_hint', 'Keep it conversational and approachable.')
        math_score  = hybrid_scores[0] if hybrid_scores else 0.5
        confidence  = max(math_score, 0.75)   # trust hybrid rank; no extra YES/NO call

        chunks_context = "\n".join([
            f"[Source {i}]\nQ: {chunk.get('question', chunk.get('title', ''))}\n"
            f"A: {chunk.get('answer', chunk.get('content', ''))}"
            for i, chunk in enumerate(hybrid_ranked[:3], 1)
        ])

        is_followup       = '[Follow-up context]' in context_str
        followup_emphasis = (
            "\n[FOLLOW-UP] Resolve intent from conversation history, "
            "then answer strictly from context.\n"
            if is_followup else ""
        )

        prompt = (
            f"You are a {personality} customer support assistant. {polish_hint}\n\n"
            f"{followup_emphasis}{context_str}\n\n"
            f'Customer message: "{user_message}"\n\n'
            f"Knowledge base context (ground your answer ONLY in these sources):\n"
            f"{chunks_context}\n\n"
            f"CRITICAL RULES:\n"
            f"1. If the context does NOT contain enough information to answer accurately,\n"
            f"   respond with ONLY: IDK_FALLBACK\n"
            f"   (No explanation, no guessing — just the string IDK_FALLBACK)\n"
            f"2. If you CAN answer:\n"
            f"   - Direct, conversational, 1–3 sentences.\n"
            f"   - Bullets only for 3+ distinct items; otherwise prose.\n"
            f"   - Natural contractions (I'm, it's, we're).\n"
            f"   - No markdown headers, preamble, or sign-off.\n"
            f"   - Do NOT invent facts not in the context.\n"
            f"3. Short/vague messages are usually follow-ups — infer full intent from history.\n\n"
            f"Return ONLY the final response (or IDK_FALLBACK)."
        )

        try:
            response      = self.model.generate_content(prompt)
            response_text = response.text.strip()
            if not response_text:
                return 'IDK_FALLBACK', math_score, 'rag_empty'
            logger.info(
                f"[Call2] conf={confidence:.2f} "
                f"idk={response_text == 'IDK_FALLBACK'} len={len(response_text)}"
            )
            return response_text, confidence, 'rag_pipeline'
        except Exception as e:
            logger.error(f"[Call2] Gemini error: {e}")
            answer = hybrid_ranked[0].get('answer', hybrid_ranked[0].get('content', ''))
            return self._make_fallback(answer), math_score * 0.7, 'rag_static'

    # Backward-compat aliases
    def _rag_generate(self, user_message: str, hybrid_ranked: List[Dict],
                      hybrid_scores: List[float], vertical: str,
                      context_str: str) -> Tuple[str, float, str]:
        """Alias → _rag_generate_and_polish."""
        return self._rag_generate_and_polish(
            user_message, hybrid_ranked, hybrid_scores, vertical, context_str)

    def _polish_response(self, raw_text: str, vertical: str, user_message: str) -> str:
        """No-op stub — polish is now merged into _rag_generate_and_polish."""
        return raw_text

    # ═══════════════════════════════════════════════════════════════════
    # GUARDRAILS
    # ═══════════════════════════════════════════════════════════════════

    def _guardrails(self, response_text: str, candidates: List[Dict]) -> str:
        """
        Post-generation quality checks:
        - Too short / empty → safe fallback
        - Contains "I don't know" with no candidates → safe fallback
        - Excessive length (> 600 chars) → trim to first 2 sentences
        """
        if not response_text or len(response_text) < 8:
            return "I'm not sure about that. Would you like me to connect you with the team?"

        if "i don't know" in response_text.lower() and not candidates:
            return "I'm not sure about that. Would you like me to connect you with the team?"

        if len(response_text) > 600:
            sentences     = re.split(r'(?<=[.!?])\s+', response_text)
            response_text = ' '.join(sentences[:2])

        return response_text

    # ═══════════════════════════════════════════════════════════════════
    # VERTICAL FALLBACK
    # ═══════════════════════════════════════════════════════════════════

    def _vertical_fallback(self, user_message: str, faqs: List[Dict],
                           vertical: str, context_str: str) -> str:
        """Fallback when no strong embedding hit — loosely uses available FAQs."""
        vert_cfg    = self.personalities.get(vertical, self.personalities['general'])
        personality = vert_cfg['tone']

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
            logger.error(f"[VerticalFallback] error: {e}")
            return "I'm not sure I have the exact answer. Would you like me to connect you with the team?"

    # ═══════════════════════════════════════════════════════════════════
    # FOLLOW-UP DETECTION & KEYWORD ENRICHMENT (Phase 2 preserved)
    # ═══════════════════════════════════════════════════════════════════

    _FOLLOWUP_STARTERS = (
        'how about', 'what about', 'and the ', 'and a ', 'and an ',
        'what are the ', "what's the ", "what's its ", 'tell me about the ',
        'how much is the ', 'how much does the ', 'what does the ',
        'what is the ', 'is the ', 'does the ', 'how about the ',
        'same for ', 'same question for ', 'and for ',
        'the ', 'that one', 'this one', 'the same ',
    )

    _TOPIC_STOPS = {
        'what', 'how', 'is', 'are', 'the', 'a', 'an', 'do', 'does', 'can',
        'could', 'would', 'should', 'tell', 'me', 'about', 'much', 'many',
        'long', 'i', 'you', 'we', 'my', 'your', 'it', 'its', 'please',
        'hi', 'hey', 'and', 'or', 'for', 'in', 'on', 'at', 'with', 'to',
        'of', 'that', 'this', 'there', 'their', 'these', 'those', 'was',
        'were', 'been', 'have', 'has', 'had', 'get', 'got', 'also', 'just',
        'more', 'than', 'some', 'any', 'all', 'one', 'two', 'three',
    }

    def _is_followup(self, message: str, history: List[Dict]) -> bool:
        if not history:
            return False
        msg = message.strip().lower()
        for starter in self._FOLLOWUP_STARTERS:
            if msg.startswith(starter):
                return True
        words = msg.split()
        if len(words) <= 6:
            GREETINGS = {'hi', 'hello', 'hey', 'thanks', 'thank', 'ok', 'okay',
                         'great', 'cool', 'bye', 'goodbye', 'yes', 'no', 'sure'}
            if not GREETINGS.issuperset(set(words)):
                last_bot = next(
                    (m.get('content', '') for m in reversed(history)
                     if m.get('role') != 'user'), None
                )
                if last_bot and len(last_bot) > 40:
                    return True
        return False

    def _resolve_query(self, message: str, history: List[Dict]) -> str:
        """
        Keyword enrichment fallback for when _rewrite_query is unavailable.
        Borrows topic terms from the last 2 user messages.
        """
        if not self._is_followup(message, history):
            return message

        recent_user = [
            m.get('content', '').strip()
            for m in (history or [])[-6:]
            if m.get('role') == 'user' and m.get('content')
        ]

        topic_words: List[str] = []
        for past_msg in recent_user[-2:]:
            words = re.findall(r'\b[a-z]{3,}\b', past_msg.lower())
            topic_words.extend(w for w in words if w not in self._TOPIC_STOPS)

        current_kws = [
            w for w in re.findall(r'\b[a-z]{3,}\b', message.lower())
            if w not in self._TOPIC_STOPS
        ]
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
    # CONVERSATION CONTEXT (Phase 2 preserved)
    # ═══════════════════════════════════════════════════════════════════

    def _build_context(self, conversation_history: List[Dict],
                       client_id: str = None,
                       current_message: str = None) -> str:
        """
        Builds the conversation context block passed into prompts.
        Includes: earlier summary (DB) + last 8 turns + follow-up annotation.
        """
        parts = []

        if client_id:
            try:
                import models as _m
                summary = _m.get_latest_conversation_summary(client_id)
                if summary:
                    parts.append(f"[Earlier in this conversation]\n{summary}")
            except Exception:
                pass

        if conversation_history:
            recent     = conversation_history[-8:]
            turns_lines = []
            for m in recent:
                role    = 'User' if m.get('role') == 'user' else 'Assistant'
                content = m.get('content', '').strip()
                if not content:
                    continue
                if role == 'Assistant' and len(content) > 220:
                    content = content[:220] + '…'
                turns_lines.append(f"  {role}: {content}")
            if turns_lines:
                parts.append("[Conversation so far]\n" + "\n".join(turns_lines))

            if current_message:
                recent_user_msgs = [
                    m.get('content', '').strip()
                    for m in recent
                    if m.get('role') == 'user' and m.get('content')
                ]
                if self._is_followup(current_message, conversation_history) and recent_user_msgs:
                    prev_question = recent_user_msgs[-1]
                    parts.append(
                        "[Follow-up context]\n"
                        f"The user's current message (\"{current_message}\") is a follow-up.\n"
                        f"Their preceding question was: \"{prev_question}\"\n"
                        f"Infer what they are now asking and answer it directly."
                    )

        return "\n\n".join(parts) if parts else ""

    def maybe_summarise(self, client_id: str,
                        conversation_history: List[Dict]) -> None:
        """
        Token-aware summarisation — only fires when the conversation history
        exceeds ~2 000 tokens (estimated as total_chars / 4).

        Avoids expensive LLM calls on short conversations where the full
        history fits comfortably in the context window already.
        Non-blocking — any failure is logged and silently ignored.
        """
        if not self.enabled or not conversation_history or not client_id:
            return

        # Estimate token count: chars / 4 is a reasonable approximation for
        # English text with Gemini's tokeniser (slightly generous — intentional).
        total_chars   = sum(len(m.get('content', '')) for m in conversation_history)
        estimated_tks = total_chars // 4
        if estimated_tks < 2000:
            logger.debug(f"[Summarise] skipped — est. tokens={estimated_tks} < 2000")
            return

        try:
            # Summarise the oldest messages not yet captured in a summary.
            # Use the first half of history to preserve the "recent turns"
            # that go into every prompt unchanged.
            half    = max(6, len(conversation_history) // 2)
            window  = conversation_history[:half]
            turns   = "\n".join([
                f"{'User' if m.get('role') == 'user' else 'Assistant'}: {m.get('content', '')}"
                for m in window if m.get('content')
            ])
            prompt = (
                "Summarise this support conversation in 1–2 concise sentences. "
                "Focus on what the user needed and what was resolved.\n\n"
                f"{turns}\n\nReturn ONLY the summary."
            )
            response = self.model.generate_content(prompt)
            summary  = response.text.strip()
            if summary and len(summary) > 10:
                import models as _m
                _m.save_conversation_summary(client_id, summary, len(conversation_history))
                logger.info(
                    f"[Summarise] client={client_id} msgs={len(conversation_history)} "
                    f"est_tokens={estimated_tks}"
                )
        except Exception as e:
            logger.debug(f"[maybe_summarise] non-critical: {e}")

    # ═══════════════════════════════════════════════════════════════════
    # SMART UPLOAD PIPELINE (Phase 2 preserved)
    # ═══════════════════════════════════════════════════════════════════

    def enrich_and_chunk(self, raw_items: List[Dict],
                         client_id: str) -> List[Dict]:
        """AI enrichment pipeline for uploaded content — unchanged from Phase 2."""
        if not raw_items:
            return []

        existing_embeddings: List[Dict] = []
        if client_id and self.enabled:
            try:
                import models as _m
                existing_embeddings = _m.get_embeddings_for_client(client_id)
            except Exception:
                pass

        chunks          = []
        seen_embeddings = [e['embedding'] for e in existing_embeddings]

        for item in raw_items:
            question = (item.get('question') or '').strip()
            answer   = (item.get('answer')   or '').strip()
            if not question or not answer or len(answer) < 10:
                continue

            content_chunks = self._split_content(answer)

            for idx, chunk_text in enumerate(content_chunks):
                chunk_id = str(uuid.uuid4())

                if self.enabled and idx == 0:
                    tags, ai_category = self._ai_enrich(question, chunk_text)
                else:
                    tags        = self._extract_tags(question)
                    ai_category = item.get('category', 'General')

                embed_text = f"{question} {chunk_text}"
                embedding  = _embed(embed_text, task='retrieval_document')

                if embedding and seen_embeddings:
                    max_sim = max((_cosine(embedding, ex) for ex in seen_embeddings), default=0.0)
                    if max_sim > 0.92:
                        logger.debug(f"[Dedup] skipped (sim={max_sim:.3f}): {question[:50]}")
                        continue

                quality = self._quality_score(question, chunk_text)

                chunk = {
                    'kb_id':     chunk_id,
                    'title':     question if idx == 0 else f"{question} (part {idx + 1})",
                    'content':   chunk_text,
                    'type':      item.get('type', 'faq'),
                    'category':  ai_category,
                    'tags':      tags,
                    'embedding': embedding,
                    'metadata':  {
                        'source':       item.get('source', 'upload'),
                        'original_q':   question,
                        'chunk_index':  idx,
                        'total_chunks': len(content_chunks),
                    },
                    'quality':   quality,
                }
                chunks.append(chunk)
                if embedding:
                    seen_embeddings.append(embedding)

        logger.info(f"[Enrich] client={client_id} input={len(raw_items)} output={len(chunks)}")
        return chunks

    def _split_content(self, text: str, max_len: int = 400) -> List[str]:
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
        if not self.enabled:
            return self._extract_tags(question), 'General'
        try:
            prompt = (
                f"Given this FAQ:\nQ: {question}\nA: {answer[:200]}\n\n"
                f'Return ONLY valid JSON:\n{{"tags": ["tag1", "tag2"], "category": "Billing"}}\n\n'
                f"tags: 2–5 short keyword tags\n"
                f"category: one of General | Billing | Support | Product | Policy | Sales | Technical"
            )
            response = self.model.generate_content(prompt)
            result   = self._parse_json(response.text)
            if result:
                return result.get('tags', [])[:5], result.get('category', 'General')
        except Exception:
            pass
        return self._extract_tags(question), 'General'

    def _extract_tags(self, text: str) -> List[str]:
        stop  = {'a', 'an', 'the', 'is', 'are', 'do', 'does', 'can', 'i', 'you',
                 'we', 'my', 'your', 'what', 'how', 'when', 'where', 'why', 'to', 'of'}
        words = re.findall(r'\b[a-z]{3,}\b', text.lower())
        return list(dict.fromkeys(w for w in words if w not in stop))[:5]

    def _quality_score(self, question: str, answer: str) -> float:
        score = 0.5
        if len(question) > 15: score += 0.15
        if len(answer)   > 50: score += 0.15
        if answer.endswith(('.', '!', '?')): score += 0.1
        if '?' in question:                  score += 0.1
        return min(score, 1.0)

    # ═══════════════════════════════════════════════════════════════════
    # BACKWARD-COMPAT: Phase 1 / Phase 2 methods (admin / analytics)
    # ═══════════════════════════════════════════════════════════════════

    def find_best_faq(self, user_message: str, faqs: List[Dict],
                      client_id: str = None) -> Tuple[Optional[Dict], float]:
        """Phase 1 compat — delegates to embedding search + hybrid rerank."""
        candidates, vector_scores = self._embedding_search(user_message, faqs, client_id)
        ranked, scores            = self._hybrid_rerank(user_message, candidates, vector_scores)
        if ranked and scores:
            return ranked[0], scores[0]
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
