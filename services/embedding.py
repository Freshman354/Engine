"""
services/embedding.py
=====================
Voyage AI embedding service with two-level caching.

WHY THIS IS A SEPARATE MODULE
------------------------------
The embedding layer (_embed, LRUCache, Redis L2) was previously buried in
ai_helper.py as module-level state mixed with class code. Extracting it means:

  - The embedding provider (Voyage AI today) can be swapped without touching
    the pipeline. Change this file, re-run index_faqs(), done.
  - The cache layer can be tested and monitored independently.
  - The Railway multi-worker problem (each Gunicorn process had its own LRU
    that never shared state) is now contained in one place — it's obvious
    that you need Redis for cross-worker sharing and exactly where to fix it.
  - ai_helper.py imports embed() from here; nothing else in the codebase
    needs to change during migration.

PUBLIC API
----------
  embed(text, task='retrieval_document') -> List[float]
      Returns a normalised 512-dim unit vector, or [] on failure.
      task: 'retrieval_document' for KB entries (index time)
            'retrieval_query'    for user messages (query time)

  normalize(vec) -> List[float]
      Normalise a vector to unit length. Exported so other modules
      (enrich_and_chunk, index_faqs) can normalise blended vectors
      using the same function.

MIGRATION STEPS FOR ai_helper.py
----------------------------------
1. Remove _LRUCache, _EMBED_CACHE, _redis_embed_client setup,
   _REDIS_EMBED_PREFIX, _REDIS_EMBED_TTL_SEC, _normalize, _embed
   from ai_helper.py.

2. Add at the top of ai_helper.py:
       from services.embedding import embed as _embed, normalize as _normalize

3. All existing call sites (_embed(...), _normalize(...)) work unchanged.

RAILWAY NOTES
--------------
Set REDIS_URL in your Railway environment to activate L2 caching across
Gunicorn workers. Without it the module falls back to the in-process LRU —
correct but each worker warms its own cache independently.

  Railway dashboard → your service → Variables → Add Variable
  Key:   REDIS_URL
  Value: (copy from your Railway Redis plugin)

The Redis connection uses socket_connect_timeout=1 and socket_timeout=1 so
a Redis hiccup never adds more than ~1s latency — the L1 LRU is the fallback.
"""

from __future__ import annotations

import collections
import hashlib
import json
import logging
import math
import os
import struct
import threading
import time
from typing import List, Optional

# ── Logger ────────────────────────────────────────────────────────────────────

_LOG_LEVEL = os.environ.get('LUMVI_LOG_LEVEL', 'INFO').upper()

try:
    _numeric_level = getattr(logging, _LOG_LEVEL, logging.INFO)
    logger = logging.getLogger('lumvi.embedding')
    logger.setLevel(_numeric_level)
except Exception:
    logger = logging.getLogger('lumvi.embedding')


def _log_crash(tag: str, err: Exception, **context) -> None:
    """Log an exception with structured context. Mirrors the helper in ai_helper.py."""
    ctx_str = ' '.join(f'{k}={v}' for k, v in context.items())
    logger.error(
        f"[{tag}] {type(err).__name__}: {err} | {ctx_str}",
        exc_info=True,
    )


# ── Voyage AI configuration ───────────────────────────────────────────────────
#
# Model: voyage-3-lite
#   - 512-dim output
#   - Asymmetric retrieval via input_type param (query vs document)
#   - Free tier: 50M tokens/month
#   - No extra packages — pure stdlib urllib
#
# IMPORTANT: All stored embeddings must be unit-normalised (Fix 1).
# After deploying this module, run index_faqs() for every client_id so
# stored vectors match the new dimensionality and normalisation.

VOYAGE_MODEL     = 'voyage-3-lite'
VOYAGE_DIM       = 512          # output dimensionality
VOYAGE_EMBED_URL = 'https://api.voyageai.com/v1/embeddings'
VOYAGE_API_KEY   = os.environ.get('VOYAGE_API_KEY', '')

if not VOYAGE_API_KEY:
    logger.warning(
        "[Embed] VOYAGE_API_KEY not set — embed() will return [] for all calls. "
        "Set this env var to enable semantic search."
    )
else:
    logger.info(f"[Embed] Voyage AI configured model={VOYAGE_MODEL} dim={VOYAGE_DIM}")


# ── Thread-safe LRU cache ─────────────────────────────────────────────────────

class _LRUCache:
    """
    Thread-safe, size-bounded LRU cache backed by an OrderedDict.

    Used as the in-process L1 cache in front of Redis. Default size is 2048
    entries — enough for ~8MB of 512-dim float32 vectors at max capacity.
    """

    def __init__(self, maxsize: int = 2048):
        self._maxsize = maxsize
        self._cache: collections.OrderedDict = collections.OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: str, default=None):
        with self._lock:
            if key not in self._cache:
                return default
            self._cache.move_to_end(key)
            return self._cache[key]

    def __contains__(self, key: str) -> bool:
        with self._lock:
            return key in self._cache

    def __getitem__(self, key: str):
        with self._lock:
            if key not in self._cache:
                raise KeyError(key)
            self._cache.move_to_end(key)
            return self._cache[key]

    def __setitem__(self, key: str, value):
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
            self._cache[key] = value
            if len(self._cache) > self._maxsize:
                self._cache.popitem(last=False)  # evict oldest

    def __len__(self) -> int:
        with self._lock:
            return len(self._cache)


# Module-level L1 cache — one instance per process.
# Under Railway with --workers 1 --threads N this is shared across all threads.
# Under multiple Gunicorn workers each process has its own cache — use Redis L2.
_EMBED_CACHE = _LRUCache(maxsize=2048)


# ── Redis L2 cache ────────────────────────────────────────────────────────────
#
# Shared across all Gunicorn workers. Falls back silently to L1-only when
# REDIS_URL is absent (development / single-worker deployments).
#
# Cache key scheme: lumvi:embed:v2:<sha256(model:task:text)>
#   v2 = voyage-3-lite 512-dim (bumped from v1 = bge 384/768-dim)
# TTL: 7 days — embeddings are deterministic for a given model + text.
#
# Encoding: float32 binary (struct.pack) — ~2KB per 512-dim vector,
# much smaller than JSON encoding.

_REDIS_EMBED_PREFIX  = 'lumvi:embed:v2:'
_REDIS_EMBED_TTL_SEC = 86400 * 7  # 7 days

_redis_client: Optional[object] = None  # type: ignore[type-arg]

try:
    import redis as _redis_lib
    _redis_url = os.environ.get('REDIS_URL')
    if _redis_url:
        _redis_client = _redis_lib.from_url(
            _redis_url,
            decode_responses=False,     # embeddings stored as raw bytes
            socket_connect_timeout=1,
            socket_timeout=1,
        )
        logger.info("[EmbedCache] Redis L2 cache enabled")
except Exception as _redis_init_err:
    logger.info(f"[EmbedCache] Redis unavailable — L1 only ({_redis_init_err})")
    _redis_client = None


# ── Math helpers ──────────────────────────────────────────────────────────────

def normalize(vec: List[float]) -> List[float]:
    """
    Normalise a vector to unit length (L2 norm = 1).

    Unit vectors turn cosine similarity into a cheap dot product, eliminating
    redundant magnitude computation on every query comparison and removing
    floating-point drift on averaged blended embeddings.

    Returns the original list unchanged when magnitude is zero (zero vector).
    Exported so enrich_and_chunk() can normalise blended q+a vectors using
    the same function.
    """
    mag = math.sqrt(sum(x * x for x in vec))
    if mag == 0.0:
        return vec
    return [x / mag for x in vec]


# ── Public embedding function ─────────────────────────────────────────────────

def embed(text: str, task: str = 'retrieval_document') -> List[float]:
    """
    Embed text via Voyage AI's HTTP API (stdlib urllib — no extra packages).

    Voyage AI asymmetric retrieval:
      task='retrieval_query'    → input_type='query'    (user messages)
      task='retrieval_document' → input_type='document' (KB entries / FAQs)

    Caching (two levels):
      L1: in-process LRU (_EMBED_CACHE, 2048 entries, zero latency)
      L2: Redis shared across workers (7-day TTL, float32 binary)

    Retry policy:
      Up to 3 attempts with exponential backoff (1s, 2s, 4s).
      Rate-limit (429) retries with backoff.
      Auth failure (401) fails immediately — no point retrying.

    Returns:
      A normalised 512-dim unit vector as List[float].
      Returns [] on any failure so callers degrade gracefully to keyword search.
    """
    import urllib.error as _urllib_err
    import urllib.request as _urllib_req

    if not text or not text.strip():
        return []
    if not VOYAGE_API_KEY:
        logger.warning("[Embed] VOYAGE_API_KEY missing — returning empty vector")
        return []

    # ── Cache key: sha256(model:task:text) ───────────────────────────
    cache_key = hashlib.sha256(
        f"voyage:{VOYAGE_MODEL}:{task}:{text.strip()[:2048]}".encode()
    ).hexdigest()

    # ── L1: in-process LRU ───────────────────────────────────────────
    cached = _EMBED_CACHE.get(cache_key)
    if cached is not None:
        return cached

    # ── L2: Redis ────────────────────────────────────────────────────
    if _redis_client is not None:
        try:
            raw = _redis_client.get(_REDIS_EMBED_PREFIX + cache_key)
            if raw is not None:
                n_floats = len(raw) // 4
                vec = list(struct.unpack(f'{n_floats}f', raw))
                _EMBED_CACHE[cache_key] = vec
                logger.debug(f"[Embed] L2 cache hit key={cache_key[:10]}…")
                return vec
        except Exception as _redis_get_err:
            logger.warning(
                f"[EmbedCache] Redis GET failed (non-critical): {_redis_get_err}"
            )

    # ── Voyage AI HTTP call ───────────────────────────────────────────
    _input_type = 'query' if task == 'retrieval_query' else 'document'
    _t0 = time.monotonic()

    _payload = json.dumps({
        'input':      [text.strip()[:2048]],
        'model':      VOYAGE_MODEL,
        'input_type': _input_type,
    }).encode('utf-8')

    _MAX_RETRIES = 3
    _RETRY_BASE  = 1.0  # seconds; doubles each attempt

    for _attempt in range(1, _MAX_RETRIES + 1):
        try:
            _req = _urllib_req.Request(
                VOYAGE_EMBED_URL,
                data    = _payload,
                headers = {
                    'Authorization': f'Bearer {VOYAGE_API_KEY}',
                    'Content-Type':  'application/json',
                },
                method  = 'POST',
            )

            with _urllib_req.urlopen(_req, timeout=10) as _resp:
                _body = json.loads(_resp.read().decode('utf-8'))

            _raw_vec = _body['data'][0]['embedding']
            vec      = normalize(_raw_vec)

            _elapsed_ms = (time.monotonic() - _t0) * 1000
            logger.debug(
                f"[Embed/Voyage] task={task} input_type={_input_type} "
                f"dim={len(vec)} elapsed={_elapsed_ms:.0f}ms "
                f"text='{text[:40]}'"
            )

            # Write to L1
            _EMBED_CACHE[cache_key] = vec

            # Write to L2 (Redis)
            if _redis_client is not None:
                try:
                    _redis_client.setex(
                        _REDIS_EMBED_PREFIX + cache_key,
                        _REDIS_EMBED_TTL_SEC,
                        struct.pack(f'{len(vec)}f', *vec),
                    )
                except Exception as _redis_set_err:
                    logger.warning(
                        f"[EmbedCache] Redis SET failed (non-critical): {_redis_set_err}"
                    )

            return vec

        except _urllib_err.HTTPError as _http_err:
            _err_body = ''
            try:
                _err_body = _http_err.read().decode('utf-8')[:300]
            except Exception:
                pass

            if _http_err.code == 429:
                _wait = _RETRY_BASE * (2 ** (_attempt - 1))
                if _attempt < _MAX_RETRIES:
                    logger.warning(
                        f"[Embed/Voyage] Rate limit (429) on attempt {_attempt}/{_MAX_RETRIES} "
                        f"— retrying in {_wait:.1f}s."
                    )
                    time.sleep(_wait)
                    continue
                else:
                    logger.error(
                        f"[Embed/Voyage] Rate limit (429) — all {_MAX_RETRIES} attempts "
                        f"exhausted. Falling back to keyword search."
                    )
            elif _http_err.code == 401:
                logger.error(
                    "[Embed/Voyage] Authentication failed (401). "
                    "Check VOYAGE_API_KEY env var."
                )

            _log_crash(
                'Embed/Voyage/HTTP', _http_err,
                status=_http_err.code,
                task=task,
                attempt=_attempt,
                text_preview=text[:60],
                body=_err_body,
            )
            return []

        except Exception as _e:
            _log_crash(
                'Embed/Voyage', _e,
                task=task,
                attempt=_attempt,
                text_len=len(text),
                text_preview=text[:60],
                elapsed_ms=f"{(time.monotonic() - _t0) * 1000:.0f}",
            )
            return []

    return []  # all retries exhausted


# ── Startup health check ──────────────────────────────────────────────────────

def startup_health_check() -> bool:
    """
    Log the embedding service configuration at startup.

    Called once at import time. Returns True when fully configured.
    Logs WARNING (not ERROR) for missing config so the app can still
    start and fall back to keyword search.
    """
    ok = bool(VOYAGE_API_KEY and VOYAGE_MODEL and VOYAGE_EMBED_URL)

    if ok:
        logger.info(
            f"[Embed/Startup] OK — model={VOYAGE_MODEL} dim={VOYAGE_DIM} "
            f"redis={'enabled' if _redis_client else 'disabled (L1 only)'}"
        )
    else:
        logger.warning(
            "[Embed/Startup] MISSING VOYAGE_API_KEY — "
            "semantic search disabled, falling back to keyword search. "
            "Set VOYAGE_API_KEY in Railway environment variables."
        )

    return ok


_embedding_service_ready = startup_health_check()
