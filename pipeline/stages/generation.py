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
from utils import get_logger, log_crash

logger = get_logger('lumvi.generation')


# ── Static fallbacks ──────────────────────────────────────────────────────────

_STATIC_FALLBACKS = [
    "I don't have the exact answer for that right now, but our team would be happy to help. "
    "Would you like me to connect you with someone?",
    "That's a great question — let me get the right person to answer it properly for you. "
    "Shall I arrange that?",
    "I want to make sure you get an accurate answer on that. "
    "Can I connect you with a specialist?",
]


def make_fallback(vertical: str = 'general', is_frustrated: bool = False) -> str:
    """Return a static fallback string when the model is disabled or erroring."""
    import random
    if is_frustrated:
        return (
            "I can hear this hasn't been easy — let me get a person involved who "
            "can help you directly. Can I connect you now?"
        )
    return random.choice(_STATIC_FALLBACKS)


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
) -> str:
    """
    Build the conversation context string passed to the RAG prompt.
    Combines:
      - The last 6 turns of conversation history
      - An optional DB-stored conversation summary (for sessions > 10 turns)
    Returns a formatted string.
    """
    # Try to load a stored summary for long conversations
    db_summary = ''
    if client_id and len(conversation_history) > 10:
        try:
            import models as _m
            summary_rec = _m.ConversationSummary.query.filter_by(
                client_id=client_id
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
) -> Tuple[str, float, str]:
    """
    Generate a polished answer from the top-ranked FAQ candidates.
    This is the primary Gemini call in the pipeline ("Call 2").

    Returns (response_text, confidence, method).

    confidence is a float 0.0–1.0 reflecting how well the retrieved
    context maps to the question. Derived from the generation response
    or falls back to a static heuristic.
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
        resp  = model.generate_content(prompt)
        text  = (resp.text or '').strip()
        if not text:
            return make_fallback(vertical, session_mem.get('is_frustrated', False)), 0.0, 'empty_generation'

        # Confidence heuristic: longer, more specific answers score higher
        has_idk_signal = any(p in text.lower() for p in [
            "i don't have", "i'm not sure", "not in my knowledge",
            "i can't find", "don't know", "connect you",
        ])
        confidence = 0.45 if has_idk_signal else 0.75
        method     = 'idk_fallback' if has_idk_signal else 'rag'

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
        resp = model.generate_content(prompt)
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
    trigger_length: int = 12,
) -> None:
    """
    If the conversation has grown long, summarise it and store in the DB.
    Called as a background task — never blocks the pipeline.
    Summarisation fires once per 12-turn boundary to avoid redundant calls.
    """
    if not client_id or not conversation_history:
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
        resp    = model.generate_content(prompt)
        summary = (resp.text or '').strip()
        if not summary:
            return

        existing = _m.ConversationSummary.query.filter_by(client_id=client_id).first()
        if existing:
            existing.summary    = summary
            existing.created_at = __import__('datetime').datetime.utcnow()
        else:
            rec = _m.ConversationSummary(
                client_id=client_id,
                summary=summary,
                created_at=__import__('datetime', fromlist=['datetime']).datetime.utcnow(),
            )
            _m.db.session.add(rec)
        _m.db.session.commit()
        logger.debug(f"[Generation/Summary] stored summary for client={client_id}")

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
