"""
AI Helper — Phase 3: Advanced 6-Stage RAG Pipeline
====================================================
Stage 1  _rewrite_query      → Gemini rewrites the user message into a
                                standalone search query using conversation history
Stage 2  _embedding_search   → Retrieves top candidates using Gemini embeddings
Stage 3  _hybrid_rerank      → Combines vector score + BM25-lite keyword score
Stage 4  _rag_generate       → Injects top hybrid-ranked chunks and generates answer
Stage 5  _polish_response    → Final LLM pass for tone match + clean formatting
Stage 6  generate_response   → Orchestrates all stages; preserves lead detection,
                                conversation memory, and summarisation

Preserved from Phase 2:
  - 15-message conversation memory
  - Summarisation every 6 messages
  - Personality / vertical system
  - Lead detection (3-tier keyword → AI)
  - Pure-Python cosine + BM25-lite (no numpy / scipy)
  - Response cache
  - enrich_and_chunk / find_best_faq / index_faqs compat
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


# ─────────────────────────────────────────────────────────────────────
# Pure-Python math helpers
# ─────────────────────────────────────────────────────────────────────

def _cosine(a: list, b: list) -> float:
    """Pure-Python cosine similarity — no numpy required."""
    dot   = sum(x * y for x, y in zip(a, b))
    mag_a = math.sqrt(sum(x * x for x in a))
    mag_b = math.sqrt(sum(x * x for x in b))
    return dot / (mag_a * mag_b) if mag_a and mag_b else 0.0


def _bm25_score(query_tokens: List[str], doc_tokens: List[str],
                avg_doc_len: float = 40.0,
                k1: float = 1.5, b: float = 0.75,
                corpus_size: int = 100) -> float:
    """
    BM25-lite: a pure-Python, single-document BM25 approximation.
    No corpus pre-indexing needed — IDF is estimated using corpus_size.
    Returns a normalised score in [0, 1].
    """
    if not query_tokens or not doc_tokens:
        return 0.0

    doc_len  = len(doc_tokens)
    tf_map   = {}
    for tok in doc_tokens:
        tf_map[tok] = tf_map.get(tok, 0) + 1

    score = 0.0
    for term in query_tokens:
        tf = tf_map.get(term, 0)
        if tf == 0:
            continue
        # IDF approximation: assume term appears in ~10% of docs
        df  = max(1, corpus_size // 10)
        idf = math.log((corpus_size - df + 0.5) / (df + 0.5) + 1)
        numerator   = tf * (k1 + 1)
        denominator = tf + k1 * (1 - b + b * doc_len / avg_doc_len)
        score      += idf * (numerator / denominator)

    # Normalise to [0, 1] using a soft cap of 10 as a practical maximum BM25 score
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
        6-Stage RAG pipeline:
          1. Preprocessing + intent detection (lead short-circuit)
          2. Build conversation context
          3. Query rewrite  → standalone search query
          4. Embedding search → raw candidates + vector scores
          5. Hybrid rerank → BM25-lite + vector combined score
          6. RAG generation → grounded LLM answer
          7. Response polish → tone + formatting pass
          8. Guardrails + cache
        """
        if not user_message or not user_message.strip():
            return {'response': "How can I help you today?", 'method': 'empty',
                    'confidence': 1.0, 'is_lead': False, 'lead_metadata': None}

        history = conversation_history or []

        # ── Stage 1a: Preprocess ──────────────────────────────────────
        clean  = self._preprocess(user_message)
        logger.debug(f"[Pipeline] start | msg='{clean[:60]}' | vertical={vertical}")

        # ── Stage 1b: Intent detection (lead short-circuit) ───────────
        intent = self.detect_intent(clean, lead_triggers or [], vertical)
        if intent.get('is_lead') and intent.get('confidence', 0) >= 0.65:
            logger.info(
                f"[Lead] client={client_id} vertical={vertical} "
                f"score={intent.get('score', 0):.1f} conf={intent['confidence']:.2f}"
            )

            # ── Smart lead extraction + contextual nudge ───────────────
            lead_meta = self._extract_lead_info(clean, history)
            logger.info(
                f"[LeadMeta] name={lead_meta.get('name')} "
                f"email={lead_meta.get('email')} "
                f"phone={lead_meta.get('phone')} "
                f"topic={lead_meta.get('interest_topic')}"
            )

            nudge = self._build_lead_nudge(lead_meta, vertical, clean)

            return {
                'response':      nudge,
                'method':        'lead_detection',
                'confidence':    intent['confidence'],
                'is_lead':       True,
                'lead_metadata': lead_meta,   # ← saved directly to PostgreSQL by caller
            }

        # ── Stage 2: Build conversation context ───────────────────────
        context_str = self._build_context(history, client_id, clean)

        # ── Dynamic confidence threshold ──────────────────────────────
        # Lower the bar for sales/pricing queries so the bot answers rather
        # than falling back when a user is close to buying.
        msg_lower      = clean.lower()
        is_pricing_msg = any(kw in msg_lower for kw in _GLOBAL_PRICING_KW)
        is_sales_query = intent.get('intent') == 'lead_request' or is_pricing_msg
        vector_threshold = 0.22 if is_sales_query else 0.28
        logger.debug(
            f"[Threshold] sales={is_sales_query} "
            f"threshold={vector_threshold} intent={intent.get('intent')}"
        )

        # ── Stage 3: Query rewrite ────────────────────────────────────
        search_query = self._rewrite_query(clean, history)
        logger.debug(f"[Stage3/Rewrite] '{clean[:40]}' → '{search_query[:60]}'")

        # ── Stage 4: Embedding search (dynamic threshold) ─────────────
        candidates, vector_scores = self._embedding_search(
            search_query, faqs, client_id, threshold=vector_threshold
        )
        logger.debug(f"[Stage4/Search] hits={len(candidates)} top_vec={vector_scores[0]:.3f if vector_scores else 0}")

        # ── Resolve last active category from history for Stage 5 ─────
        last_category = self._last_response_category(history)

        # ── Stage 5: Hybrid rerank (with category stickiness) ─────────
        hybrid_ranked, hybrid_scores = self._hybrid_rerank(
            search_query, candidates, vector_scores, last_category=last_category
        )
        logger.debug(f"[Stage5/Hybrid] top_score={hybrid_scores[0]:.3f if hybrid_scores else 0}")

        # ── Cache check (keyed on original clean message) ─────────────
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
            }

        # ── Stage 6: RAG generation ───────────────────────────────────
        if hybrid_ranked and self.enabled:
            raw_text, confidence, method = self._rag_generate(
                clean, hybrid_ranked, hybrid_scores, vertical, context_str
            )
        elif self.enabled:
            raw_text   = self._vertical_fallback(clean, faqs[:12], vertical, context_str)
            confidence = 0.35
            method     = 'vertical_fallback'
        else:
            raw_text   = self._make_fallback(faqs[0].get('answer', '') if faqs else '')
            confidence = 0.0
            method     = 'static_fallback'

        # ── Stage 7: Response polish ──────────────────────────────────
        if self.enabled and confidence > 0.3 and method not in ('cache', 'static_fallback'):
            polished = self._polish_response(raw_text, vertical, clean)
        else:
            polished = raw_text

        # ── Stage 8: Guardrails + cache write ─────────────────────────
        final = self._guardrails(polished, hybrid_ranked)

        if confidence > 0.4:
            self._response_cache[cache_key] = final

        logger.info(
            f"[Pipeline] done | method={method} conf={confidence:.2f} "
            f"top_chunk={top_id[:12] if top_id else 'none'} vertical={vertical}"
        )
        return {
            'response':      final,
            'method':        method,
            'confidence':    confidence,
            'is_lead':       False,
            'lead_metadata': None,
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
        Build a personalised, contextual nudge that:
          1. Acknowledges what we already know (name / topic)
          2. Asks for the single most important missing piece of info
             Priority: email > name > phone

        Pure-Python — no Gemini call. Tone shaped by vertical personality.
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

    def _rewrite_query(self, user_message: str,
                       conversation_history: List[Dict]) -> str:
        """
        Turn the user's message into a standalone, self-contained search query
        by incorporating conversation context via a fast Gemini call.

        Examples:
          history:  "what's the pro plan cost?" → "$99/month"
          message:  "how about the agency one?"
          rewrite:  "What is the price of the agency plan?"

          history:  "do you do same-day appointments?"
          message:  "and weekends?"
          rewrite:  "Are same-day appointments available on weekends?"

        Falls back to _resolve_query (keyword enrichment) on any error,
        so this stage never blocks the pipeline.
        """
        # Skip rewrite for long standalone messages — already fully specified
        word_count = len(user_message.split())
        if word_count >= 10 and not self._is_followup(user_message, conversation_history):
            logger.debug(f"[Rewrite] skipped (standalone, {word_count} words)")
            return user_message

        # No history = nothing to rewrite
        if not conversation_history:
            return user_message

        # Build a compact conversation snippet (last 6 turns max)
        recent = conversation_history[-6:]
        turns  = []
        for m in recent:
            role    = 'User'      if m.get('role') == 'user' else 'Assistant'
            content = m.get('content', '').strip()
            if content:
                turns.append(f"  {role}: {content[:150]}")
        if not turns:
            return user_message

        convo_snippet = "\n".join(turns)

        if not self.enabled:
            return self._resolve_query(user_message, conversation_history)

        try:
            prompt = f"""You are a search query optimizer.

Given this conversation:
{convo_snippet}

The user's latest message is: "{user_message}"

Rewrite this message into a single, complete, standalone search query that:
- Contains all necessary context from the conversation
- Is specific enough to retrieve the right information
- Is phrased as a clear question or search phrase
- Is 5–15 words long
- Contains NO filler like "I want to know" or "Can you tell me"

Return ONLY the rewritten query. No explanation, no quotes."""

            response   = self.model.generate_content(prompt)
            rewritten  = response.text.strip().strip('"\'')

            # Sanity checks — fall back if the model returned something odd
            if not rewritten or len(rewritten) < 5 or len(rewritten) > 200:
                raise ValueError(f"bad rewrite length: {len(rewritten)}")
            if rewritten.lower().startswith(('i ', 'can you', 'please', 'could you')):
                raise ValueError("rewrite starts with filler")

            return rewritten

        except Exception as e:
            logger.debug(f"[Rewrite] Gemini failed ({e}), using keyword enrichment")
            return self._resolve_query(user_message, conversation_history)

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

        # ── 1. knowledge_base chunks ───────────────────────────────────
        if client_id and query_vec:
            try:
                import models as _m
                kb_chunks = _m.get_relevant_knowledge(client_id, query_vec, limit=8)
                if kb_chunks:
                    scored = []
                    for chunk in kb_chunks:
                        emb = chunk.get('embedding')
                        if emb:
                            score = _cosine(query_vec, emb)
                        else:
                            score = max(0.0, 0.5 - (len(scored) * 0.05))
                        if score > threshold:   # dynamic threshold
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
                        logger.debug(f"[KB] threshold={threshold} top={scored[0][1]:.3f} hits={len(scored)}")
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
                    scored  = []
                    for fid, emb in stored.items():
                        score = _cosine(query_vec, emb)
                        if score > threshold and fid in faq_idx:   # dynamic threshold
                            scored.append((faq_idx[fid], score))
                    scored.sort(key=lambda x: x[1], reverse=True)
                    if scored:
                        logger.debug(f"[FAQ Embed] threshold={threshold} top={scored[0][1]:.3f} hits={len(scored)}")
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
    # STAGE 6 — RAG GENERATION
    # ═══════════════════════════════════════════════════════════════════

    def _rag_generate(self, user_message: str, hybrid_ranked: List[Dict],
                      hybrid_scores: List[float], vertical: str,
                      context_str: str) -> Tuple[str, float, str]:
        """
        Stage 6: Inject top hybrid-ranked chunks + generate a grounded response.

        Enhancement 3 — Semantic validation (self-check gate):
          Before generating the full answer, a single YES/NO prompt asks:
          "Does the retrieved knowledge contain enough info to answer this?"
          - YES: confidence = max(math_score, 0.85) — prevents low cosine scores
                 from incorrectly routing to fallback when content IS relevant.
          - NO + math_score < 0.35: skip generation, return fallback immediately.
          - NO + math_score >= 0.35: generate anyway (math says content is close).
          - API error: continue with math_score unchanged.

        Returns (response_text, confidence, method_tag).
        """
        vert_cfg    = self.personalities.get(vertical, self.personalities['general'])
        personality = vert_cfg['tone']
        math_score  = hybrid_scores[0] if hybrid_scores else 0.5
        confidence  = math_score

        # Build multi-chunk context — top 3 for richness
        chunks_context = ""
        for i, chunk in enumerate(hybrid_ranked[:3], 1):
            q = chunk.get('question', chunk.get('title', ''))
            a = chunk.get('answer',   chunk.get('content', ''))
            chunks_context += f"\n[Source {i}]\nQ: {q}\nA: {a}\n"

        # ── Enhancement 3: Semantic self-check gate ────────────────────
        try:
            selfcheck_prompt = (
                f"You are a retrieval quality judge.\n\n"
                f"User query: \"{user_message}\"\n\n"
                f"Retrieved knowledge:\n{chunks_context.strip()}\n\n"
                f"Does the retrieved knowledge contain enough specific information "
                f"to answer the user query accurately?\n\n"
                f"Reply with a single word: YES or NO"
            )
            check_resp  = self.model.generate_content(selfcheck_prompt)
            verdict     = check_resp.text.strip().upper().split()[0]
            semantic_ok = (verdict == 'YES')

            if semantic_ok:
                confidence = max(math_score, 0.85)
                logger.info(
                    f"[SelfCheck] YES — confidence boosted "
                    f"math={math_score:.3f} → final={confidence:.3f}"
                )
            else:
                if math_score < 0.35:
                    logger.info(
                        f"[SelfCheck] NO + low math ({math_score:.3f}) → fallback"
                    )
                    answer = hybrid_ranked[0].get('answer', hybrid_ranked[0].get('content', ''))
                    return self._make_fallback(answer), math_score * 0.5, 'selfcheck_fallback'
                else:
                    logger.info(
                        f"[SelfCheck] NO but math OK ({math_score:.3f}) → generating"
                    )
        except Exception as _sce:
            logger.debug(f"[SelfCheck] non-critical error ({_sce}), using math_score")

        # ── Build full generation prompt ──────────────────────────────
        is_followup       = '[Follow-up context]' in context_str
        followup_emphasis = ""
        if is_followup:
            followup_emphasis = (
                "\n[CONFIRMED FOLLOW-UP] The current message is a continuation. "
                "Resolve the full intent from the conversation thread above, "
                "then answer it strictly from the retrieved knowledge.\n"
            )

        followup_rules = """
╔══════════════════════════════════════════════════════════════════╗
║  FOLLOW-UP & REFERENTIAL RULES (read before generating)        ║
╠══════════════════════════════════════════════════════════════════╣
║  Short/vague messages are almost always continuations.          ║
║  Read [Conversation so far] to identify the active topic,      ║
║  map the current message onto that topic, then answer from     ║
║  the retrieved sources below. Never ask for clarification       ║
║  when a reasonable interpretation exists in history.           ║
║  NEVER say "I don't know" when history provides context.       ║
╚══════════════════════════════════════════════════════════════════╝"""

        prompt = f"""You are a {personality} customer support assistant.
{followup_rules}
{followup_emphasis}
{context_str}

Current message: "{user_message}"

Retrieved knowledge (ONLY use facts from here — never invent):
{chunks_context.strip()}

Rules:
- Ground every fact in the retrieved knowledge above.
- Infer full intent from context for short or referential messages.
- Natural tone: 1–3 sentences. Contractions welcome.
- Use a bullet list only when listing 3+ distinct items; otherwise prose.
- Do NOT hedge, ask to clarify, or offer to escalate unless knowledge genuinely lacks the answer.
- No markdown headers, no preamble, no sign-off.

Return ONLY the response text."""

        try:
            response      = self.model.generate_content(prompt)
            response_text = response.text.strip()
            if not response_text or len(response_text) < 10:
                fallback = hybrid_ranked[0].get('answer', hybrid_ranked[0].get('content', ''))
                return fallback, confidence, 'rag_fallback'
            return response_text, confidence, 'rag_pipeline'
        except Exception as e:
            logger.error(f"[RAG] Gemini generation error: {e}")
            answer = hybrid_ranked[0].get('answer', hybrid_ranked[0].get('content', ''))
            return self._make_fallback(answer), confidence * 0.7, 'rag_static'

    # ═══════════════════════════════════════════════════════════════════
    # STAGE 7 — RESPONSE POLISH (NEW)
    # ═══════════════════════════════════════════════════════════════════

    def _polish_response(self, raw_text: str, vertical: str,
                         user_message: str) -> str:
        """
        A final lightweight LLM pass that:
          1. Adjusts tone to match the vertical personality
          2. Cleans up formatting (removes stray markdown, fixes bullet style)
          3. Ensures the response feels natural and appropriately sized

        This is deliberately cheap — the model only sees the raw answer
        and a short instruction set. It never has access to the knowledge
        chunks so it cannot hallucinate new facts.

        Falls back to raw_text on any error.
        """
        if not raw_text or not raw_text.strip():
            return raw_text

        vert_cfg     = self.personalities.get(vertical, self.personalities['general'])
        polish_hint  = vert_cfg.get('polish_hint', "Keep it conversational and approachable.")
        personality  = vert_cfg['tone']

        # Skip polish for very short responses — already clean enough
        if len(raw_text.split()) <= 12:
            return raw_text

        try:
            prompt = f"""You are a tone and formatting editor for a {personality} customer support chatbot.

Polish the following draft response WITHOUT changing any facts.

Editing rules:
1. Tone: {polish_hint}
2. Length: Keep it 1–3 sentences unless the original uses bullet points for 3+ items.
3. Bullets: Use them only if the draft already has 3+ list items; otherwise convert to prose.
4. Format: Remove any markdown headers (#, ##), bold (**text**), or excessive punctuation.
5. Contractions: Use natural contractions (I'm, it's, we're, you'd).
6. Do NOT add new facts, examples, or caveats not in the original.
7. Return ONLY the polished response — no explanation, no quotes around it.

Draft response:
{raw_text}

User's original message (for tone context only): "{user_message[:100]}"

Polished response:"""

            response = self.model.generate_content(prompt)
            polished = response.text.strip()

            # Sanity checks — reject if the polish makes it too different
            if not polished or len(polished) < 8:
                return raw_text
            # Don't let polish dramatically expand the response
            if len(polished) > len(raw_text) * 2.5:
                logger.debug("[Polish] rejected: response grew too much")
                return raw_text

            logger.debug(
                f"[Stage7/Polish] raw={len(raw_text)}ch → polished={len(polished)}ch"
            )
            return polished

        except Exception as e:
            logger.debug(f"[Polish] Gemini error (non-critical): {e}")
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
        """Summarise every 6 messages and store to DB. Non-blocking."""
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
                logger.info(f"[Summarise] client={client_id} msgs={len(conversation_history)}")
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
