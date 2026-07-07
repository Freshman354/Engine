"""
pipeline/stages/generation.py
==============================
RAG generation, guardrails, vertical fallback, context building,
and conversation summarisation.

Previously: _rag_generate_and_polish, _guardrails, _vertical_fallback,
_build_context, _get_dynamic_personality, _make_fallback, maybe_summarise
on AIHelper.

All functions receive `model` as an explicit parameter.
No reference to AIHelper — importable and testable independently.
"""

import re
from typing import Any, Dict, List, Optional, Tuple

from constants import PERSONALITIES
from utils import get_logger, log_crash, generate as _gemini_generate

logger = get_logger('lumvi.generation')


# ── Fallback pool ─────────────────────────────────────────────────────────────
# 20 varied options across different registers and styles.
# Used when Gemini is unavailable or as a fast baseline.
# Rotated randomly so repeat IDK responses don't feel identical.

_FALLBACK_POOL = [
    # Honest + offer
    "That one's a bit outside what I have right now — want me to get someone who can help properly?",
    "Hmm, I want to make sure you get an accurate answer rather than me guessing. "
    "Shall I connect you with the team?",
    "Not something I can answer confidently from what I have — the team would be much better placed. "
    "Can I connect you?",
    "I've looked through what I know and I'm not confident I have the right answer for that. "
    "Rather than guess, want me to put you in touch with someone?",
    "That's pushed to the edge of what I know! Let me get someone with the full picture to help. "
    "Sound good?",
    # Short + direct
    "I'm not sure I have the full picture on that one. "
    "Our team would know better — shall I connect you?",
    "Good question — I just want to make sure you get the right information rather than something approximate. "
    "Want me to reach out to the team for you?",
    "That's not something I can answer well from my knowledge base right now. "
    "Want me to get the right person on it?",
    # Warm + empathetic
    "I want to be upfront — I don't have a confident answer for that. "
    "Let me make sure you get the right help instead.",
    "Rather than give you a half-answer, let me find someone who can give you the full picture. "
    "Does that work?",
    "I'd rather be honest and get you proper help than guess on this one. "
    "Shall I connect you?",
    # Context-acknowledging
    "That's a bit beyond what I can answer accurately here. "
    "Our team would have the right information — want me to arrange a chat?",
    "I don't have enough detail on that to give you a confident answer. "
    "Would it help if I connected you with someone who does?",
    "I can see why you're asking — I just don't have enough in my knowledge base to answer it well. "
    "Let me get someone better placed to help.",
    # Minimal / punchy
    "That one needs a human — want me to make the connection?",
    "I'm missing some details to answer that well. Team chat?",
    "Not quite something I can cover — can I put you in touch with the right person?",
    "I'd rather get you the right answer than a rough one. Can I connect you?",
    "That's outside what I can answer accurately. Want me to get someone involved?",
    "Good question for the team — shall I arrange that for you?",
]

_FRUSTRATED_FALLBACKS = [
    "I can hear this hasn't been easy — let me get a person involved who can help directly. "
    "Can I connect you now?",
    "You deserve a proper answer on this. Let me get someone from the team to help you directly — OK?",
    "I don't want to keep going in circles. Let me connect you with someone who can sort this out properly.",
    "I'm sorry this hasn't been resolved — the right move is getting a real person on it. "
    "Shall I arrange that?",
    "Let me stop guessing and get you someone who can actually fix this. One moment?",
]


def make_fallback(
    vertical: str = 'general',
    is_frustrated: bool = False,
    query: str = '',
) -> str:
    """
    Return a varied fallback string from the pool.
    Uses the frustrated pool when session frustration is detected.
    If query is provided, appends a brief topic reference so it doesn't
    sound completely generic.
    """
    import random
    pool = _FRUSTRATED_FALLBACKS if is_frustrated else _FALLBACK_POOL
    text = random.choice(pool)

    # If we have the query topic, prepend a brief acknowledgement so it
    # feels connected to what was asked rather than a canned non-answer.
    if query and not is_frustrated and len(query.split()) >= 3:
        topic = ' '.join(query.split()[:6]).rstrip('?.!')
        # Lowercase the first char then re-capitalise leading 'I' pronoun
        rest  = text[0].lower() + text[1:]
        rest  = re.sub(r'^i(?=[ \'])', 'I', rest)
        text  = f'On "{topic}" — {rest}'

    return text


def dynamic_fallback(
    query: str,
    vertical: str,
    session_mem: Dict,
    model: Any,
    model_name: str = '',
) -> str:
    """
    Generate a natural, contextual IDK response using Gemini.
    References the user's actual query so it never sounds like a canned message.

    Falls back to make_fallback() if the model is unavailable or errors.
    This is a lightweight call — short prompt, short output, fast.
    """
    is_frustrated = session_mem.get('is_frustrated', False)

    if model is None:
        return make_fallback(vertical, is_frustrated, query)

    personality = get_dynamic_personality(vertical, session_mem)

    prompt = (
        f"You are a {vertical} support assistant. Tone: {personality['tone']}.\n\n"
        f"A customer asked: \"{query.strip()[:200]}\"\n\n"
        "You don't have an accurate answer in your knowledge base. "
        "Write a single natural response (1–2 sentences) that:\n"
        "- Honestly acknowledges you can't fully answer this\n"
        "- Warmly offers to connect them with the team\n"
        "- References the topic they asked about (don't be generic)\n"
        "- Feels conversational, not robotic or formulaic\n"
        "- Does NOT start with 'I'\n"
        "- Does NOT use 'Great question', 'Certainly', 'Of course'\n"
        "- Does NOT use markdown or formatting\n\n"
        "Write the response only. No quotes, no explanation."
    )
    try:
        resp = _gemini_generate(model, prompt, model_name)
        text = (resp.text or '').strip()
        # Sanity check: must be a real sentence, not an empty or very short response
        if text and len(text) > 25 and len(text) < 400:
            return text
    except Exception as e:
        log_crash(logger, 'Generation/DynamicFallback', e, query_preview=query[:60])

    return make_fallback(vertical, is_frustrated, query)


# ── Personality / tone helpers ────────────────────────────────────────────────

def get_dynamic_personality(
    vertical: str,
    session_mem: Dict,
    is_sales_query: bool = False,
) -> Dict:
    """
    Return the personality dict for this vertical, optionally adjusted for
    frustration or sales context.
    """
    base = dict(PERSONALITIES.get(vertical, PERSONALITIES['general']))

    if session_mem.get('is_frustrated'):
        base['tone'] = (
            "empathetic and solution-focused — the user is frustrated, "
            "prioritise acknowledgement before information"
        )
        base['polish_hint'] = (
            "Lead with empathy. Keep it short. Offer human escalation if you can't fully resolve."
        )

    if is_sales_query:
        base['polish_hint'] = (
            base.get('polish_hint', '') +
            " Naturally surface the value proposition. End with a soft CTA if appropriate."
        )

    return base


# ── Context building ──────────────────────────────────────────────────────────

def build_context(
    conversation_history: List[Dict],
    client_id: Optional[str],
    current_message: str,
    session_id: Optional[str] = None,
) -> str:
    """
    Build the conversation context string passed to the RAG prompt.
    Combines:
      - The last 6 turns of conversation history
      - An optional DB-stored conversation summary (for sessions > 10 turns)
    Returns a formatted string.
    """
    # Try to load a stored summary for long conversations
    #
    # FIX: previously queried ConversationSummary filtered on client_id
    # ALONE, taking the most-recently-created row for that client. Any
    # business with two concurrent conversations would have one customer's
    # summary — potentially including their name, symptoms, case details —
    # silently injected into a completely different customer's prompt the
    # moment either conversation crossed the summarisation trigger length.
    # Requires a `session_id` column on ConversationSummary; if that column
    # doesn't exist yet, add it the same way tools.py's migrate_agent_tables()
    # adds conversations.session_id (ALTER TABLE ... ADD COLUMN IF NOT EXISTS).
    # Without a session_id we skip the lookup entirely rather than fall back
    # to the unscoped (leaky) query.
    db_summary = ''
    if client_id and session_id and len(conversation_history) > 10:
        try:
            import models as _m
            summary_rec = _m.ConversationSummary.query.filter_by(
                client_id=client_id, session_id=session_id
            ).order_by(_m.ConversationSummary.created_at.desc()).first()
            if summary_rec:
                db_summary = f"[Earlier conversation summary: {summary_rec.summary}]\n\n"
        except Exception:
            pass

    # Use last 6 turns only (older turns are in the summary)
    recent = conversation_history[-6:] if conversation_history else []
    formatted_turns = []
    for turn in recent:
        role    = 'User' if turn.get('role') == 'user' else 'Assistant'
        content = str(turn.get('content', '')).strip()[:300]
        formatted_turns.append(f"{role}: {content}")

    history_str = '\n'.join(formatted_turns)
    return f"{db_summary}{history_str}".strip()


# ── RAG generation (Call 2) ───────────────────────────────────────────────────

def rag_generate_and_polish(
    query: str,
    context_str: str,
    candidates: List[Dict],
    vertical: str,
    session_mem: Dict,
    is_sales_query: bool,
    model: Any,
    retrieval_score: float = 0.0,
) -> Tuple[str, float, str]:
    """
    Generate a polished answer from the top-ranked FAQ candidates.
    This is the primary Gemini call in the pipeline ("Call 2").

    Returns (response_text, confidence, method).

    confidence is a float 0.0–1.0 reflecting how well the retrieved
    context maps to the question. Derived from the generation response
    blended with the actual retrieval score (see retrieval_score below),
    not a static heuristic alone.

    retrieval_score: the cosine/qualifying score that got this candidate
    set past the RAG qualification gate (0.0–1.0). Callers should pass
    the same score used to set ctx.rag_qualified.

    FIX: confidence used to be a flat 0.75 (no IDK phrase) or 0.45 (IDK
    phrase) with zero regard for how good the retrieval match actually
    was. A borderline match that just barely cleared vector_threshold
    could still get phrased confidently by the model and would score
    identically to a rock-solid match — and since callers cache whenever
    confidence >= confidence_high, a weak/marginal match got cached and
    replayed as if it were high-confidence. Confidence is now blended
    with the real retrieval score so a weak match can no longer
    masquerade as a strong one.
    """
    if not candidates or model is None:
        return make_fallback(vertical, session_mem.get('is_frustrated', False)), 0.0, 'no_candidates'

    personality  = get_dynamic_personality(vertical, session_mem, is_sales_query)
    top_faqs_txt = '\n\n'.join(
        f"Q: {c.get('question','')}\nA: {c.get('answer','')}"
        for c in candidates[:5]
    )

    prompt = (
        f"You are a {vertical} customer support assistant.\n"
        f"Tone: {personality['tone']}\n"
        f"Polish guidelines: {personality.get('polish_hint', '')}\n\n"
        f"Conversation so far:\n{context_str}\n\n"
        f"User question: {query}\n\n"
        f"Relevant knowledge base entries:\n{top_faqs_txt}\n\n"
        "Instructions:\n"
        "- Answer the user's question using ONLY the knowledge base entries above.\n"
        "- Be concise (2-4 sentences unless detail is genuinely needed).\n"
        "- Match the tone described.\n"
        "- If the KB entries don't fully answer the question, say so honestly and offer to connect the user with the team.\n"
        "- Do NOT make up information not in the KB entries.\n\n"
        "Respond with ONLY the answer text. No preamble, no metadata."
    )

    try:
        resp  = _gemini_generate(model, prompt)
        text  = (resp.text or '').strip()
        if not text:
            return make_fallback(vertical, session_mem.get('is_frustrated', False)), 0.0, 'empty_generation'

        # Confidence heuristic: longer, more specific answers score higher,
        # blended with how good the underlying retrieval match actually was.
        has_idk_signal = any(p in text.lower() for p in [
            "i don't have", "i'm not sure", "not in my knowledge",
            "i can't find", "don't know", "connect you",
        ])
        _score = max(0.0, min(1.0, retrieval_score))
        if has_idk_signal:
            confidence = round(min(0.45, 0.2 + 0.3 * _score), 4)
            method     = 'idk_fallback'
        else:
            confidence = round(min(0.95, 0.35 + 0.6 * _score), 4)
            method     = 'rag'

        logger.debug(
            f"[Generation/RAG] method={method} conf={confidence:.2f} "
            f"len={len(text)} query='{query[:40]}'"
        )
        return text, confidence, method

    except Exception as e:
        log_crash(logger, 'Generation/RAG', e, query_preview=query[:60])
        return make_fallback(vertical, session_mem.get('is_frustrated', False)), 0.0, 'rag_error'


# ── Vertical fallback generation ──────────────────────────────────────────────

def vertical_fallback(
    query: str,
    vertical: str,
    session_mem: Dict,
    model: Any,
) -> Tuple[str, float, str]:
    """
    When RAG confidence is too low, attempt a vertical-aware Gemini fallback
    that acknowledges the gap and offers next steps.

    Returns (response_text, confidence, method).
    """
    if model is None:
        return make_fallback(vertical, session_mem.get('is_frustrated', False)), 0.0, 'vertical_fallback'

    personality = get_dynamic_personality(vertical, session_mem)
    prompt = (
        f"You are a {vertical} customer support assistant.\n"
        f"Tone: {personality['tone']}\n\n"
        f"The user asked: \"{query}\"\n\n"
        "You don't have a specific KB article for this. "
        "Acknowledge that honestly, stay helpful, and offer to connect them with the team. "
        "Keep it to 2 sentences max. No speculation. No invented information.\n\n"
        "Respond with ONLY the answer text."
    )
    try:
        resp = _gemini_generate(model, prompt)
        text = (resp.text or '').strip()
        if not text:
            return make_fallback(vertical, session_mem.get('is_frustrated', False)), 0.0, 'vertical_fallback_empty'

        idk = any(p in text.lower() for p in [
            "i don't have", "i'm not sure", "connect you", "team", "specialist"
        ])
        method     = 'vertical_fallback_idk' if idk else 'vertical_fallback'
        confidence = 0.3
        return text, confidence, method
    except Exception as e:
        log_crash(logger, 'Generation/VerticalFallback', e, query_preview=query[:60])
        return make_fallback(vertical, session_mem.get('is_frustrated', False)), 0.0, 'vertical_fallback_error'


# ── Guardrails ────────────────────────────────────────────────────────────────

_GUARDRAIL_BLOCKED_PATTERNS = re.compile(
    r'\b(lawyer|legal advice|doctor|medical advice|diagnos[ie]|'
    r'guaranteed returns?|investment advice|prescri(?:be|ption)|'
    r'suicide|self[- ]harm)\b',
    re.IGNORECASE,
)

_GUARDRAIL_REDIRECT = (
    "I want to make sure you get the right guidance on that. "
    "This is something best discussed with a qualified professional. "
    "Is there anything else I can help you with today?"
)


def guardrails(response_text: str, candidates: List[Dict]) -> str:
    """
    Post-generation safety check.
    - Blocks legal/medical/financial advice patterns.
    - Trims trailing hallucination markers (common LLM artifacts).
    Returns the cleaned response text.
    """
    if _GUARDRAIL_BLOCKED_PATTERNS.search(response_text):
        logger.info(f"[Guardrails] Blocked sensitive pattern in response")
        return _GUARDRAIL_REDIRECT

    # Trim common LLM meta-text artifacts
    cleaned = re.sub(
        r'\n?\n?(Note:|Disclaimer:|Remember:|Please note:).*$',
        '',
        response_text,
        flags=re.DOTALL | re.IGNORECASE,
    ).strip()

    # Remove markdown headers (should be plain text)
    cleaned = re.sub(r'^#{1,3}\s+', '', cleaned, flags=re.MULTILINE).strip()

    return cleaned or response_text


# ── Conversation summarisation ────────────────────────────────────────────────

def maybe_summarise(
    client_id: str,
    conversation_history: List[Dict],
    model: Any,
    session_id: Optional[str] = None,
    trigger_length: int = 12,
) -> None:
    """
    If the conversation has grown long, summarise it and store in the DB.
    Called as a background task — never blocks the pipeline.
    Summarisation fires once per 12-turn boundary to avoid redundant calls.

    FIX: now requires session_id and scopes the stored row by
    (client_id, session_id) instead of client_id alone — see build_context()
    for why an unscoped row leaks one customer's conversation into another's.
    """
    if not client_id or not session_id or not conversation_history:
        return
    if len(conversation_history) < trigger_length:
        return
    if len(conversation_history) % trigger_length != 0:
        return
    if model is None:
        return

    try:
        import models as _m
        history_txt = '\n'.join(
            f"{t.get('role','?').upper()}: {str(t.get('content',''))[:200]}"
            for t in conversation_history[-trigger_length:]
        )
        prompt = (
            "Summarise this customer service conversation in 2–3 sentences. "
            "Include: main topic(s) discussed, any unresolved issues, and "
            "any contact details or commitments made.\n\n"
            f"Conversation:\n{history_txt}\n\n"
            "Summary:"
        )
        resp    = _gemini_generate(model, prompt)
        summary = (resp.text or '').strip()
        if not summary:
            return

        existing = _m.ConversationSummary.query.filter_by(
            client_id=client_id, session_id=session_id
        ).first()
        if existing:
            existing.summary    = summary
            existing.created_at = __import__('datetime').datetime.utcnow()
        else:
            rec = _m.ConversationSummary(
                client_id=client_id,
                session_id=session_id,
                summary=summary,
                created_at=__import__('datetime', fromlist=['datetime']).datetime.utcnow(),
            )
            _m.db.session.add(rec)
        _m.db.session.commit()
        logger.debug(f"[Generation/Summary] stored summary for client={client_id} session={session_id}")

    except Exception as e:
        log_crash(logger, 'Generation/Summary', e, client_id=client_id)


# ── CLARIFY response parser ───────────────────────────────────────────────────

def parse_clarify_response(text: str) -> Optional[str]:
    """
    If the model returned a CLARIFY marker, extract the clarification question.
    Format: CLARIFY: <question>
    Returns the question string or None.
    """
    match = re.match(r'CLARIFY:\s*(.+)', text.strip(), re.IGNORECASE)
    return match.group(1).strip() if match else None
