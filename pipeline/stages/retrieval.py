"""
pipeline/stages/retrieval.py
=============================
Embedding search, BM25 scoring, hybrid rerank, cross-encoder rerank,
query rewriting, multi-intent decomposition, and followup resolution.

Previously: _embedding_search, _hybrid_rerank, _cross_encoder_rerank,
_combined_rewrite_intent, _decompose_intents, _is_followup, _resolve_query,
_last_response_category on AIHelper.

Functions that need the Gemini model receive it as an explicit parameter.
Functions that need embed() import it from services.embedding.
"""

import re
from typing import Any, Dict, List, Optional, Tuple

from constants import (
    INTENT_SPLITTERS,
    MAX_CANDIDATES,
    QUESTION_SIGNALS,
    BARE_PRONOUNS,
    INFERENCE_EXPANSION_ENABLED,
)
from pipeline.stages.math_helpers import (
    bm25_score,
    build_bm25_corpus,
    cosine,
    reciprocal_rank_fusion,
    tokenize,
    topic_overlap,
)
from services.embedding import embed
from utils import get_logger, log_crash

logger = get_logger('lumvi.retrieval')


# ── Followup / pronoun resolution ─────────────────────────────────────────────

_FOLLOWUP_STARTERS = (
    'what about', 'how about', 'and what', 'tell me more', 'more about',
    'elaborate on', 'expand on', "what's that", 'explain that',
    'what does that mean', 'so how', 'so what', 'ok and', 'ok but',
)

_TOPIC_STOPS = frozenset([
    'a', 'an', 'the', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
    'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could',
    'should', 'may', 'might', 'shall', 'can', 'to', 'of', 'in', 'on',
    'at', 'by', 'for', 'with', 'from', 'up', 'about', 'into', 'then',
    'and', 'but', 'or', 'if', 'as', 'it', 'its', 'that', 'this',
])


def is_followup(clean_message: str) -> bool:
    """
    True when the message looks like a continuation of the previous turn
    rather than a standalone question.
    """
    msg = clean_message.lower().strip()
    if any(msg.startswith(s) for s in _FOLLOWUP_STARTERS):
        return True
    words = msg.split()
    if words and words[0] in BARE_PRONOUNS:
        return True
    if len(words) <= 4 and not QUESTION_SIGNALS.search(msg):
        return True
    return False


def resolve_query(
    clean_message: str,
    conversation_history: List[Dict],
) -> str:
    """
    For short followup messages, prepend the topic from the previous
    assistant turn to give the embedding search meaningful context.

    Example:
      History: "Our Pro plan costs £49/month."
      Message: "What about the Enterprise plan?"
      Resolved: "Enterprise plan pricing"
    """
    if not is_followup(clean_message):
        return clean_message
    if not conversation_history:
        return clean_message

    # Extract topic words from last assistant turn
    last_bot = ''
    for turn in reversed(conversation_history):
        if turn.get('role') == 'assistant':
            last_bot = str(turn.get('content', ''))
            break

    if not last_bot:
        return clean_message

    topic_words = [
        w for w in last_bot.split()
        if len(w) > 4 and w.lower() not in _TOPIC_STOPS
    ][:6]

    if not topic_words:
        return clean_message

    topic_str = ' '.join(topic_words)
    resolved  = f"{topic_str} — {clean_message}"
    logger.debug(f"[Retrieval] followup resolved: '{clean_message}' → '{resolved}'")
    return resolved


# ── Multi-intent decomposition ────────────────────────────────────────────────

def decompose_intents(query: str) -> List[str]:
    """
    Split compound queries into sub-queries for parallel retrieval.
    E.g. "What's the price and how do I integrate?" → ["What's the price",
    "how do I integrate?"]

    Returns [query] (single-element list) when no split point is detected.
    """
    parts = INTENT_SPLITTERS.split(query)
    sub_queries = []
    for part in parts:
        part = part.strip()
        if part and QUESTION_SIGNALS.search(part):
            sub_queries.append(part)
    return sub_queries if len(sub_queries) >= 2 else [query]


# ── Query rewriting (Call 1) ──────────────────────────────────────────────────

def rewrite_query(
    clean_message: str,
    vertical: str,
    conversation_history: List[Dict],
    model: Any,
) -> Tuple[str, bool]:
    """
    Use Gemini to rewrite a user query into a clean retrieval query.
    Also returns is_sales_query classification as a side-effect.

    Returns (rewritten_query, is_sales_query).
    Falls back to (clean_message, False) on any error.

    This is "Call 1" in the 2-call budget.
    Only invoke when:
      - The message is longer than 3 words, AND
      - The previous call1_used flag is False
    """
    history_snippet = ''
    if conversation_history:
        last_turns = conversation_history[-4:]
        history_snippet = '\n'.join(
            f"{t.get('role','?').upper()}: {str(t.get('content',''))[:120]}"
            for t in last_turns
        )

    prompt = (
        f"You are a retrieval query optimizer for a {vertical} business chatbot.\n\n"
        f"Recent conversation:\n{history_snippet}\n\n"
        f"User message: \"{clean_message}\"\n\n"
        "Task: Rewrite the user message as a clean, self-contained search query "
        "that will retrieve the best FAQ match. "
        "Also classify whether this is a pricing/sales query.\n\n"
        "Respond ONLY with valid JSON:\n"
        '{"query": "<rewritten query>", "is_sales": true|false}'
    )
    try:
        resp   = model.generate_content(prompt)
        text   = (resp.text or '').strip().strip('`')
        if text.startswith('json'):
            text = text[4:].strip()
        parsed = __import__('json').loads(text)
        query  = str(parsed.get('query', clean_message)).strip() or clean_message
        is_sales = bool(parsed.get('is_sales', False))
        logger.debug(f"[Retrieval/Rewrite] '{clean_message[:50]}' → '{query[:50]}' sales={is_sales}")
        return query, is_sales
    except Exception as e:
        log_crash(logger, 'Retrieval/Rewrite', e, msg_preview=clean_message[:60])
        return clean_message, False


# ── Embedding search ──────────────────────────────────────────────────────────

def embedding_search(
    query: str,
    faqs: List[Dict],
    client_id: Optional[str],
    poor_kb_ids: Optional[set] = None,
    top_k: int = MAX_CANDIDATES,
) -> Tuple[List[Dict], List[float]]:
    """
    Embed the query, compute cosine similarity against all FAQ embeddings,
    return top_k results sorted descending by score.

    FAQs without embeddings are skipped (not failed — avoids blocking
    on partially-indexed KB updates).

    poor_kb_ids: set of KB IDs with poor-answer feedback. These are pushed
    to the bottom of the ranking (score multiplied by 0.5) but not removed
    entirely, in case no better option exists.
    """
    if not faqs:
        return [], []

    poor_kb_ids = poor_kb_ids or set()
    query_vec   = embed(query.strip()[:1024], task='retrieval_query')
    if not query_vec:
        logger.warning(f"[Retrieval/Search] embed returned [] for query='{query[:60]}'")
        return [], []

    scored: List[Tuple[float, Dict]] = []
    for faq in faqs:
        faq_vec = faq.get('embedding') or []
        if not faq_vec or len(faq_vec) != len(query_vec):
            continue
        sim = cosine(query_vec, faq_vec)
        kb_id = str(faq.get('kb_id', faq.get('id', '')))
        if kb_id in poor_kb_ids:
            sim *= 0.5
        scored.append((sim, faq))

    scored.sort(key=lambda x: x[0], reverse=True)
    top = scored[:top_k]
    candidates = [f for _, f in top]
    scores     = [s for s, _ in top]

    logger.debug(
        f"[Retrieval/Search] query='{query[:40]}' "
        f"candidates={len(candidates)} "
        f"top_score={scores[0]:.3f if scores else 0:.3f}"
    )
    return candidates, scores


# ── Hybrid rerank (RRF) ───────────────────────────────────────────────────────

def hybrid_rerank(
    query: str,
    candidates: List[Dict],
    vector_scores: List[float],
    faqs: List[Dict],
    top_n: int = 10,
) -> Tuple[List[Dict], List[float]]:
    """
    Re-rank candidates using Reciprocal Rank Fusion of vector + BM25 scores.

    vector_scores: cosine scores from embedding_search (same order as candidates).
    faqs: full FAQ list (needed to build BM25 corpus statistics).
    top_n: how many results to keep after reranking.

    Returns (reranked_candidates, rrf_scores).
    """
    if not candidates:
        return [], []
    if len(candidates) == 1:
        return candidates, vector_scores[:1]

    # Build BM25 corpus from the full FAQ set (not just top candidates)
    corpus_size, avg_doc_len, doc_freq, tokenized_docs = build_bm25_corpus(faqs)
    query_tokens = tokenize(query)

    # BM25 score each candidate
    faq_id_to_idx = {
        str(f.get('kb_id', f.get('id', i))): i
        for i, f in enumerate(faqs)
    }

    bm25_scored: List[Tuple[float, Dict]] = []
    for cand in candidates:
        cand_id = str(cand.get('kb_id', cand.get('id', '')))
        idx = faq_id_to_idx.get(cand_id)
        if idx is not None and idx < len(tokenized_docs):
            doc_tokens = tokenized_docs[idx]
        else:
            text = f"{cand.get('question','')} {cand.get('answer','')} {cand.get('tags','')}"
            doc_tokens = tokenize(text)
        bm25 = bm25_score(query_tokens, doc_tokens, corpus_size, avg_doc_len, doc_freq)
        bm25_scored.append((bm25, cand))

    bm25_scored.sort(key=lambda x: x[0], reverse=True)
    bm25_ranked = [c for _, c in bm25_scored]

    # RRF fusion
    fused = reciprocal_rank_fusion(
        vector_ranked=candidates,
        bm25_ranked=bm25_ranked,
        id_key='kb_id',
    )

    reranked = [doc for doc, _ in fused[:top_n]]
    rrf_scores = [score for _, score in fused[:top_n]]

    if reranked:
        logger.debug(
            f"[Retrieval/Hybrid] top_rrf={rrf_scores[0]:.4f} "
            f"top_q='{reranked[0].get('question','')[:50]}'"
        )
    return reranked, rrf_scores


# ── Cross-encoder rerank (Gemini) ─────────────────────────────────────────────

def cross_encoder_rerank(
    query: str,
    candidates: List[Dict],
    model: Any,
    top_n: int = 5,
) -> List[Dict]:
    """
    Use Gemini as a cross-encoder to reorder the top-N candidates by
    relevance to the query.

    Only called when:
      - call1_used is False (budget available), AND
      - top cosine score is below confidence_high (ambiguous result), AND
      - len(candidates) >= 2

    Returns reordered candidates. Falls back to original order on error.
    """
    if not candidates or model is None:
        return candidates

    pool = candidates[:top_n]
    if len(pool) < 2:
        return candidates

    summaries = '\n'.join(
        f"{i+1}. Q: {c.get('question','')[:100]} | A: {c.get('answer','')[:120]}"
        for i, c in enumerate(pool)
    )

    prompt = (
        f"Rank these FAQ entries by relevance to the user query.\n\n"
        f"User query: \"{query}\"\n\n"
        f"FAQ candidates:\n{summaries}\n\n"
        f"Respond ONLY with a JSON array of the original 1-based positions in "
        f"relevance order, e.g. [2,1,3]. No explanation."
    )
    try:
        resp  = model.generate_content(prompt)
        text  = (resp.text or '').strip()
        match = re.search(r'\[[\d,\s]+\]', text)
        if not match:
            return candidates
        order = __import__('json').loads(match.group(0))
        reordered = []
        for rank in order:
            idx = int(rank) - 1
            if 0 <= idx < len(pool):
                reordered.append(pool[idx])
        # Append any remaining candidates not in the reordered list
        reordered_ids = {id(c) for c in reordered}
        for c in candidates[top_n:]:
            reordered.append(c)
        for c in pool:
            if id(c) not in reordered_ids:
                reordered.append(c)
        logger.debug(f"[Retrieval/CrossEnc] reordered top-{len(reordered)} candidates")
        return reordered
    except Exception as e:
        log_crash(logger, 'Retrieval/CrossEnc', e, query_preview=query[:60])
        return candidates


# ── Last response category (pronoun resolution helper) ───────────────────────

def last_response_category(conversation_history: List[Dict]) -> Optional[str]:
    """
    Infer the topic category of the last bot response from its text.
    Used to resolve "it" / "that" references to a specific feature.
    Returns a category string or None.
    """
    if not conversation_history:
        return None
    for turn in reversed(conversation_history):
        if turn.get('role') == 'assistant':
            txt = str(turn.get('content', '')).lower()
            if any(w in txt for w in ['price', 'cost', 'plan', 'subscription']):
                return 'pricing'
            if any(w in txt for w in ['feature', 'integration', 'connect', 'embed']):
                return 'feature'
            if any(w in txt for w in ['how to', 'steps', 'guide', 'tutorial']):
                return 'how_to'
            if any(w in txt for w in ['support', 'contact', 'help', 'team']):
                return 'support'
            break
    return None
