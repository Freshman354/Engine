"""
pipeline/stages/math_helpers.py
================================
Pure math and text utilities for retrieval scoring.
No I/O, no model calls, no DB. All functions are stateless and testable.
"""

import math
import re
from collections import Counter
from typing import Dict, List, Tuple

from constants import OVERLAP_STOPWORDS
from utils import get_logger

logger = get_logger('lumvi.math')


# ── Vector math ───────────────────────────────────────────────────────────────

def cosine(a: List[float], b: List[float]) -> float:
    """Cosine similarity between two vectors. Returns 0.0 on zero-magnitude input."""
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    mag_a = math.sqrt(sum(x * x for x in a))
    mag_b = math.sqrt(sum(x * x for x in b))
    if mag_a == 0.0 or mag_b == 0.0:
        return 0.0
    return dot / (mag_a * mag_b)


# ── Text helpers ──────────────────────────────────────────────────────────────

def stem(word: str) -> str:
    """
    Minimal suffix-stripping stemmer (no NLTK dependency).
    Good enough for FAQ overlap scoring; not used for embedding.
    """
    w = word.lower()
    for suffix in ('ing', 'tion', 'tions', 'ness', 'ment', 'able', 'ible',
                   'ful', 'less', 'ous', 'ive', 'est', 'er', 'ed', 'ly',
                   'ies', 'es', 's'):
        if len(w) > len(suffix) + 3 and w.endswith(suffix):
            return w[: -len(suffix)]
    return w


def tokenize(text: str) -> List[str]:
    """Lowercase, split on non-alpha, stem, remove stopwords."""
    raw = re.sub(r'[^a-zA-Z0-9\s]', ' ', text.lower()).split()
    return [stem(t) for t in raw if len(t) > 2 and t not in OVERLAP_STOPWORDS]


def topic_overlap(query: str, faq_question: str, min_len: int = 4) -> float:
    """
    Jaccard-style overlap between query and FAQ question tokens.
    Used as a fast topic-relevance gate before embedding comparison.
    Returns 0.0–1.0.
    """
    q_tok = {t for t in tokenize(query)    if len(t) >= min_len}
    f_tok = {t for t in tokenize(faq_question) if len(t) >= min_len}
    if not q_tok or not f_tok:
        return 0.0
    return len(q_tok & f_tok) / len(q_tok | f_tok)


# ── BM25 ──────────────────────────────────────────────────────────────────────

def bm25_score(
    query_tokens: List[str],
    doc_tokens: List[str],
    corpus_size: int,
    avg_doc_len: float,
    doc_freq: Dict[str, int],
    k1: float = 1.5,
    b: float = 0.75,
) -> float:
    """
    Okapi BM25 score for a single document.
    corpus_size, avg_doc_len, and doc_freq must be computed once per
    FAQ set (not per query) — see score_bm25_corpus().
    """
    score = 0.0
    doc_len = len(doc_tokens)
    doc_term_freq = Counter(doc_tokens)

    for term in set(query_tokens):
        tf  = doc_term_freq.get(term, 0)
        df  = doc_freq.get(term, 0)
        if df == 0:
            continue
        idf = math.log((corpus_size - df + 0.5) / (df + 0.5) + 1)
        tf_norm = (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * doc_len / max(avg_doc_len, 1)))
        score += idf * tf_norm

    return score


def build_bm25_corpus(faqs: List[Dict]) -> Tuple[int, float, Dict[str, int], List[List[str]]]:
    """
    Pre-compute BM25 corpus statistics for a FAQ list.
    Returns (corpus_size, avg_doc_len, doc_freq, tokenized_docs).
    Call once per request (cheap) and pass results to bm25_score().
    """
    tokenized = []
    for faq in faqs:
        text = f"{faq.get('question','')} {faq.get('answer','')} {faq.get('tags','')}"
        tokenized.append(tokenize(text))

    corpus_size = len(tokenized)
    avg_doc_len = sum(len(d) for d in tokenized) / max(corpus_size, 1)

    doc_freq: Dict[str, int] = {}
    for doc in tokenized:
        for term in set(doc):
            doc_freq[term] = doc_freq.get(term, 0) + 1

    return corpus_size, avg_doc_len, doc_freq, tokenized


# ── Reciprocal Rank Fusion ────────────────────────────────────────────────────

def reciprocal_rank_fusion(
    vector_ranked: List[Dict],
    bm25_ranked: List[Dict],
    id_key: str = 'kb_id',
    k: int = 60,
) -> List[Tuple[Dict, float]]:
    """
    Combine vector and BM25 rankings using Reciprocal Rank Fusion.
    Returns list of (faq_dict, rrf_score) sorted descending by score.

    k=60 is the standard RRF constant (Cormack et al., 2009).
    Higher k gives more weight to items consistently ranked well;
    lower k amplifies single-ranking leaders.

    NOTE on chunked documents: multiple physical rows can share the same
    id_key value (e.g. a long FAQ answer split into several KbEntry
    chunks, which all carry the same kb_id). When that happens, this
    function merges their scores under one id — but must not just keep
    whichever chunk it happened to see last. The representative doc kept
    for each id is the one with the single best (lowest) rank seen across
    either ranking, so a well-matched chunk can never be silently
    replaced by a worse-ranked duplicate of the same document.

    FIX: previously `doc_map[doc_id] = doc` was unconditional on every
    iteration, so for a doc_id that appeared more than once (chunked
    FAQs), the surviving entry was whichever occurrence was processed
    *last* — the worst-ranked chunk in the BM25 pass, since that loop ran
    second and walked best-to-worst. That could hand generation a
    fragment of an FAQ's answer instead of the best-matching chunk, even
    though the best chunk ranked #1.
    """
    scores: Dict[str, float] = {}
    doc_map: Dict[str, Dict] = {}
    best_rank: Dict[str, int] = {}

    def _accumulate(ranked_list: List[Dict]) -> None:
        for rank, doc in enumerate(ranked_list):
            doc_id = str(doc.get(id_key, id(doc)))
            scores[doc_id] = scores.get(doc_id, 0.0) + 1.0 / (k + rank + 1)
            if doc_id not in best_rank or rank < best_rank[doc_id]:
                best_rank[doc_id] = rank
                doc_map[doc_id] = doc

    _accumulate(vector_ranked)
    _accumulate(bm25_ranked)

    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    return [(doc_map[doc_id], score) for doc_id, score in ranked]
