"""
cache_utils.py — KB Version-Based Cache Invalidation for Lumvi
==============================================================
Drop next to app.py and ai_helper.py.  No other files need to change
except the targeted patches listed in the guide.

BACKEND PRIORITY
  1. Redis  (REDIS_URL env var set)        — shared across all Gunicorn workers ✓
  2. In-process dict fallback              — safe for single-worker / dev / free tier

CACHE KEY FORMAT
  client:{client_id}:kb:{kb_version}:q:{sha256[:16]}

WHAT IS NEVER CACHED
  - Responses produced with conversation history (personalised)
  - Lead / action responses  (method starts with 'lead' or 'action:')
  - Fallback / IDK responses (method in fallback set)
  - Demo client              (client_id == 'demo')
  - Anything confidence < 0.4 (ai_helper already gates this internally,
    we double-check at the app.py layer for safety)

STALE-CACHE GUARANTEE
  Every KB mutation calls bump_kb_version(client_id).
  The cache key includes the version, so old keys are structurally
  unreachable after a bump.  They expire via TTL (default 24 h) so
  Redis memory never grows unboundedly.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
import threading
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ── TTL ───────────────────────────────────────────────────────────────────────
CACHE_TTL_SECONDS: int = int(os.environ.get("LUMVI_CACHE_TTL", 86400))  # 24 h

# ── Methods that must NEVER be cached ────────────────────────────────────────
_NEVER_CACHE_METHODS = frozenset({
    "fallback",
    "idk_fallback",
    "vertical_fallback",
    "static_fallback",
    "rag_empty",
    "rag_static",
    "lead_detection",
    "lead_ai",
    "lead_trigger",
    "lead_pipeline",
    "limit_enforced",
    "empty",
})

# ── Redis (optional, lazy-initialised once) ───────────────────────────────────
_redis_client = None
_redis_lock   = threading.Lock()


def _get_redis():
    """
    Return the Redis client, or None if unavailable / not configured.
    Thread-safe lazy initialisation — only one connection attempt per process.
    """
    global _redis_client
    if _redis_client is not None:
        return _redis_client

    redis_url = os.environ.get("REDIS_URL", "").strip()
    if not redis_url:
        return None

    with _redis_lock:
        if _redis_client is not None:   # double-checked locking
            return _redis_client
        try:
            import redis  # type: ignore
            client = redis.from_url(
                redis_url,
                decode_responses=True,
                socket_connect_timeout=2,
                socket_timeout=2,
                retry_on_timeout=False,
            )
            client.ping()
            _redis_client = client
            logger.info("[Cache] Redis connected (%s)", redis_url.split("@")[-1])
        except Exception as exc:
            logger.warning("[Cache] Redis unavailable (%s) — using in-process cache", exc)
    return _redis_client


# ── In-process fallback ───────────────────────────────────────────────────────
_local_store: dict[str, tuple[Any, float]] = {}
_local_lock   = threading.Lock()


# =============================================================================
# NORMALISATION & KEY BUILDING
# =============================================================================

def normalize_question(text: str) -> str:
    """
    Normalise a user question before hashing so semantically equivalent
    phrasing shares the same cache slot.

      1. Lowercase
      2. Strip surrounding whitespace
      3. Collapse internal whitespace runs → single space
      4. Remove leading punctuation artefacts (?, !, . at start)
    """
    text = text.lower().strip()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"^[?!.\s]+", "", text)
    return text


def make_cache_key(client_id: str, kb_version: int, question: str) -> str:
    """
    Build a deterministic, collision-resistant, tenant-isolated cache key.

    Format:  client:{client_id}:kb:{kb_version}:q:{sha256[:16]}

    The client_id prefix guarantees multi-tenant isolation — one client's
    cached answers can never leak into another's key space.
    """
    normalized    = normalize_question(question)
    question_hash = hashlib.sha256(normalized.encode()).hexdigest()[:16]
    return f"client:{client_id}:kb:{kb_version}:q:{question_hash}"


def _version_key(client_id: str) -> str:
    return f"client:{client_id}:kb_version"


# =============================================================================
# VERSION MANAGEMENT
# =============================================================================

def get_kb_version(client_id: str) -> int:
    """
    Return the current kb_version integer for a client.

    Redis path : GET client:{id}:kb_version  (O(1))
    Local path : dict lookup  (O(1))
    Default    : 1  (safe — cache is still isolated per key)

    Never raises; returns 1 on any error so chat is never blocked.
    """
    vkey = _version_key(client_id)

    r = _get_redis()
    if r:
        try:
            val = r.get(vkey)
            if val is not None:
                return int(val)
            r.set(vkey, 1)   # seed — no TTL, versions are permanent counters
            return 1
        except Exception as exc:
            logger.warning("[Cache] get_kb_version Redis error: %s", exc)

    # Local fallback
    with _local_lock:
        entry = _local_store.get(vkey)
        if entry:
            return int(entry[0])
        _local_store[vkey] = (1, float("inf"))
        return 1


def bump_kb_version(client_id: str) -> int:
    """
    Atomically increment kb_version and return the new value.

    Called after EVERY KB mutation:
      • FAQ save (bulk)
      • FAQ delete-all
      • File upload (CSV / Excel / PDF)
      • Webhook FAQ import
      • Any future single-FAQ add / edit / delete

    Redis INCR is atomic even under concurrent workers.
    Local fallback uses a threading.Lock.

    Never raises; returns current+1 estimate on error.
    """
    vkey = _version_key(client_id)

    r = _get_redis()
    if r:
        try:
            new_v = r.incr(vkey)
            logger.info("[Cache] kb_version bumped: client=%s → v%d", client_id, new_v)
            return new_v
        except Exception as exc:
            logger.warning("[Cache] bump_kb_version Redis error: %s", exc)

    # Local fallback (single-process safe)
    with _local_lock:
        entry   = _local_store.get(vkey)
        current = int(entry[0]) if entry else 0
        new_v   = current + 1
        _local_store[vkey] = (new_v, float("inf"))

    logger.info("[Cache] kb_version bumped (local): client=%s → v%d", client_id, new_v)
    return new_v


# =============================================================================
# RESPONSE CACHE  GET / SET
# =============================================================================

def cache_get(client_id: str, kb_version: int, question: str) -> Optional[dict]:
    """
    Look up a cached AI response dict.

    Returns the stored dict on a hit, or None on a miss / error.
    Never raises — Redis failures degrade gracefully to a cache miss.
    """
    key = make_cache_key(client_id, kb_version, question)

    r = _get_redis()
    if r:
        try:
            raw = r.get(key)
            if raw:
                logger.info("[Cache] HIT  client=%s v%d key=…%s", client_id, kb_version, key[-8:])
                return json.loads(raw)
            logger.debug("[Cache] MISS client=%s v%d key=…%s", client_id, kb_version, key[-8:])
        except Exception as exc:
            logger.warning("[Cache] cache_get Redis error: %s", exc)
        return None

    # Local fallback
    with _local_lock:
        entry = _local_store.get(key)
        if entry:
            value, expires_at = entry
            if time.monotonic() < expires_at:
                logger.info("[Cache] HIT  (local) client=%s v%d", client_id, kb_version)
                return value
            del _local_store[key]

    return None


def cache_set(
    client_id:     str,
    kb_version:    int,
    question:      str,
    response_data: dict,
) -> bool:
    """
    Store an AI response in cache.

    Guards:
      • method in _NEVER_CACHE_METHODS → skip (fallback / lead / error)
      • method starts with 'action:' → skip (personalised action responses)
      • confidence < 0.4 → skip (low-quality answer)
      • client_id == 'demo' → skip (shared demo, no isolation guarantee)

    Returns True on success, False if skipped or on error (non-fatal either way).
    """
    if client_id == "demo":
        return False

    method     = response_data.get("method", "")
    confidence = float(response_data.get("confidence", 0.0))

    if method in _NEVER_CACHE_METHODS:
        logger.debug("[Cache] skip (method=%s)", method)
        return False
    if method.startswith("action:"):
        logger.debug("[Cache] skip (action response)")
        return False
    if confidence < 0.4:
        logger.debug("[Cache] skip (confidence=%.2f < 0.4)", confidence)
        return False

    key = make_cache_key(client_id, kb_version, question)

    r = _get_redis()
    if r:
        try:
            r.setex(key, CACHE_TTL_SECONDS, json.dumps(response_data))
            logger.info(
                "[Cache] SET  client=%s v%d method=%s conf=%.2f ttl=%ds key=…%s",
                client_id, kb_version, method, confidence, CACHE_TTL_SECONDS, key[-8:],
            )
            return True
        except Exception as exc:
            logger.warning("[Cache] cache_set Redis error: %s", exc)
            return False

    # Local fallback
    expires_at = time.monotonic() + CACHE_TTL_SECONDS
    with _local_lock:
        _local_store[key] = (response_data, expires_at)
    logger.info("[Cache] SET  (local) client=%s v%d method=%s", client_id, kb_version, method)
    return True


# =============================================================================
# CONVENIENCE ALIAS
# =============================================================================

def invalidate(client_id: str) -> int:
    """
    Canonical invalidation call — bump kb_version so all old cache keys
    become structurally unreachable.  Returns the new version.
    """
    return bump_kb_version(client_id)


# =============================================================================
# DIAGNOSTICS
# =============================================================================

def cache_stats(client_id: str) -> dict:
    """Return diagnostic info for monitoring / admin endpoint."""
    version = get_kb_version(client_id)
    backend = "redis" if _get_redis() else "in-process"
    return {
        "client_id":   client_id,
        "kb_version":  version,
        "backend":     backend,
        "ttl_seconds": CACHE_TTL_SECONDS,
    }
