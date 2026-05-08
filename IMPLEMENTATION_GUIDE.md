# Lumvi — Production Cache Implementation
## Final Architecture & Step-by-Step Guide

---

## 1. Architecture Trace (Your Actual Code)

### 1.1 Full pipeline flow in ai_helper.py

```
generate_response(user_message, faqs, vertical, conversation_history,
                  client_id, lead_triggers)
  │
  ├─ _preprocess(message)                  # strip / collapse whitespace
  │
  ├─ detect_action_intent(clean)           # keyword match — ZERO LLM cost
  │     └─ if action hit → handle_detected_action() → return early
  │
  ├─ detect_intent(clean, lead_triggers)   # 3-tier: keyword → keyword score → Gemini confirm
  │     └─ if is_lead → _build_lead_nudge() → return early
  │
  ├─ CALL 1 (conditional):
  │     _combined_rewrite_intent()         # only for short (<10w) or follow-ups
  │     or _resolve_query()                # pure-Python keyword enrichment
  │
  ├─ _embedding_search()                   # cosine on knowledge_base chunks
  │     ├─ KB chunks (primary)
  │     ├─ legacy FAQ embeddings (fallback)
  │     └─ keyword overlap (last resort)
  │
  ├─ _hybrid_rerank()                      # 0.65×vector + 0.30×BM25 + 0.05×length
  │                                        # + 1.15× category stickiness boost
  │
  ├─ self._response_cache check            # in-process dict keyed on (msg, top_id, vertical)
  │                                        # catches SAME-SESSION repeated questions
  │
  ├─ _build_context()                      # last 8 turns + earlier summary
  │
  ├─ CALL 2: _rag_generate_and_polish()    # single Gemini call → answer + polish
  │     └─ IDK_FALLBACK gate → _vertical_fallback() if model can't answer
  │
  ├─ _guardrails()                         # length trim, empty check
  │
  ├─ self._response_cache[key] = final     # store for same-session dedup
  │
  └─ return {response, method, confidence, is_lead, lead_metadata, action}
```

### 1.2 The existing self._response_cache (line 235 in __init__)

This is an **in-process dict** on the AIHelper singleton. It caches by
`(message.lower(), top_kb_id, vertical)`. Its purpose is to avoid
re-calling Gemini for the exact same question twice **within a single
server process session**.

**Problem:** It is never invalidated after KB updates. It also doesn't
survive worker restarts, and on Gunicorn with multiple workers each
worker has its own copy.

**Decision:** We KEEP it exactly as-is for its same-session dedup value.
We ADD a Redis layer above it for persistent, cross-worker, invalidatable
caching. The two layers are complementary:

```
Request hits Redis HIT  → return (free, persistent, kb-version-safe)
Request hits internal HIT → return (free, same-session)
Request is a MISS on both → call Gemini → write to BOTH caches
```

### 1.3 Where caching MUST live

```
app.py     — fetch kb_version once per /api/chat request, pass to helper
ai_helper  — internal cache check (in-process dedup, unchanged)
           — NEW: Redis cache_get before Call 2, Redis cache_set after
cache_utils — all Redis / local-store logic (new module)
```

The kb_version is fetched in **app.py** (one Redis GET per request)
and passed into `generate_response()` as a new optional kwarg. This
avoids a second Redis round-trip inside the helper.

---

## 2. Why the Old Internal Cache Goes Stale

`self._response_cache` keys on `(clean_msg, top_kb_id, vertical)`.

When a FAQ is edited, the **top_kb_id can remain the same** (same row
in knowledge_base) while only its content changes. The cache key is
unchanged, so `self._response_cache` returns the old Gemini-generated
answer for the old FAQ content.

Additionally, on every dyno restart (Render free tier restarts
frequently) the dict is wiped, but if you have multiple workers running
simultaneously they each have their own stale copy.

The fix is the same kb_version approach: when the KB changes, bump
a counter so all old Redis keys are structurally unreachable. The
in-process dict is just an ephemeral same-session optimization on top.

---

## 3. Files Delivered

```
cache_utils.py            → new module, drop next to app.py
app_patch.py              → 7 labelled patches for app.py
ai_helper_patch.py        → 1 method replacement + 1 import addition for ai_helper.py
IMPLEMENTATION_GUIDE.md   → this file
```

---

## 4. Step-by-Step Implementation

### Step 1 — Install Redis client

```bash
pip install redis
```

Add to `requirements.txt`:
```
redis>=4.6.0
```

### Step 2 — Drop cache_utils.py

Place `cache_utils.py` in the same directory as `app.py` and `ai_helper.py`.
No other configuration needed at this point.

### Step 3 — Patch ai_helper.py (2 changes)

#### 3a — Add import block

Find this line near the top of `ai_helper.py`:
```python
from typing import List, Dict, Tuple, Optional
```

Add immediately after it:
```python
try:
    import cache_utils as _cache
    _CACHE_AVAILABLE = True
except ImportError:
    _cache = None           # type: ignore
    _CACHE_AVAILABLE = False
```

#### 3b — Replace generate_response()

Find the existing `generate_response()` method (starts around line 255,
`def generate_response(self, user_message: str, faqs: List[Dict],`).

Replace the **entire method** — from `def generate_response(` through the
final `}` of its `return` dict (around line 407) — with the replacement
in `ai_helper_patch.py` → `GENERATE_RESPONSE_REPLACEMENT`.

Key differences from original:
- Accepts new optional `kb_version: int = None` kwarg
- Redis cache lookup added AFTER internal cache check, BEFORE Call 2
- Redis cache write added AFTER guardrails, alongside internal cache write
- `from_cache: True` added to returned dict on Redis hit (for logging only)
- All existing logic (action engine, lead detection, reranking, guardrails,
  internal cache) is preserved verbatim

### Step 4 — Patch app.py (7 patches)

All patches are in `app_patch.py`. Apply in any order:

| Patch | What | Where |
|---|---|---|
| P1 | `import cache_utils` | After `import models` (~line 19) |
| P2 | Wrap Step 2 with kb_version fetch + pass to generate_response | chat() ~line 1009 |
| P3 | bump_kb_version after FAQ save | manage_faqs() ~line 2773 |
| P4 | bump_kb_version after delete-all | delete_all_faqs() ~line 2805 |
| P5 | bump_kb_version after file upload | upload_faqs() ~line 2921 |
| P6 | bump_kb_version after webhook import | webhook_faq_import() ~line 3473 |
| P7 | Add 2 new admin routes | After /health ~line 1707 |

### Step 5 — Environment variables

| Variable | Required | Default | Notes |
|---|---|---|---|
| `REDIS_URL` | No | _(none)_ | Set for production. Without it, uses in-process dict. |
| `LUMVI_CACHE_TTL` | No | `86400` | TTL in seconds (24h). Orphaned keys auto-expire. |

### Step 6 — Redis setup options

#### Option A — Render Redis (recommended)
1. Render dashboard → **New → Redis** (free tier: 25MB, enough for thousands of cached responses)
2. Copy **Internal Redis URL**
3. Add env var in your Lumvi web service: `REDIS_URL = redis://red-xxxx:6379`
4. Redeploy

#### Option B — Upstash (free, works anywhere)
1. https://upstash.com → create free database
2. Copy `rediss://` TLS URL (works on Render)
3. `REDIS_URL = rediss://...`

#### Option C — No Redis (in-process only)
Do nothing. `cache_utils.py` detects no `REDIS_URL` and falls back silently.
**Tradeoff:** Each Gunicorn worker has its own cache copy. Works fine for
single-worker setups and local dev.

---

## 5. End-to-End Flow After Implementation

```
User sends: "What are your prices?"
  │
  ├── app.py/chat()
  │     ├── kb_version = cache_utils.get_kb_version(client_id)  [Redis GET O(1)]
  │     └── ai_helper.generate_response(..., kb_version=kb_version)
  │
  ├── ai_helper/generate_response()
  │     ├── action engine → no match
  │     ├── lead detection → no match
  │     ├── rewrite/resolve query
  │     ├── embedding search + hybrid rerank
  │     ├── internal self._response_cache → MISS (first time)
  │     ├── cache_utils.cache_get(client_id, kb_version, message) → MISS (first time)
  │     ├── _build_context()
  │     ├── _rag_generate_and_polish()  ← Gemini API call (costs money)
  │     ├── _guardrails()
  │     ├── self._response_cache[key] = final
  │     ├── cache_utils.cache_set(...)  [Redis SETEX 86400s]
  │     └── return {response, method='rag_pipeline', confidence=0.87}
  │
  └── Response returned to user

User sends: "What are your prices?" (again, same session)
  └── self._response_cache HIT → return (FREE, no Redis, no Gemini)

Different user sends: "what are your prices?" (new session, same client)
  └── cache_utils.cache_get() → Redis HIT → return (FREE, no Gemini)

Admin edits FAQ: "Prices" answer changed
  └── manage_faqs() → models.save_faqs() → cache_utils.bump_kb_version()
      kb_version: 3 → 4
      Old Redis key: client:X:kb:3:q:abc123  (never looked up again, expires in 24h)
      Next request builds: client:X:kb:4:q:abc123  → MISS → fresh Gemini call
      → User gets the updated answer ✓
```

---

## 6. What Is Never Cached

| Scenario | Cached? | Reason |
|---|---|---|
| Requests with conversation history | ❌ | Personalised — context changes per session |
| Lead/action responses | ❌ | Dynamic, personalised |
| IDK / fallback responses | ❌ | No value; next KB update might answer it |
| confidence < 0.4 | ❌ | Low-quality answer, better to re-run |
| demo client | ❌ | Shared context, no KB version isolation |
| Keyword fallback (Step 3 in app.py) | ❌ | Already free, no Gemini call |

---

## 7. Redis Failure Safety

All cache operations are wrapped in try/except. On any Redis error:
- `cache_get()` → returns None → falls through to Gemini (correct behaviour)
- `cache_set()` → returns False → response is returned normally
- `get_kb_version()` → returns 1 (safe default) → cache key v1 is just treated as a miss
- `bump_kb_version()` → logs warning, increments local counter

**The chatbot never crashes due to a Redis outage. It degrades to no-cache mode.**

---

## 8. Monitoring

After deploying, test with:

```bash
# Check cache state for a client
curl -b "session=..." \
  "https://lumvi.net/api/admin/cache-stats?client_id=YOUR_CLIENT_ID"
# → {"success": true, "client_id": "...", "kb_version": 3,
#    "backend": "redis", "ttl_seconds": 86400}

# Manually invalidate (emergency)
curl -b "session=..." -X POST \
  -H "Content-Type: application/json" \
  -d '{"client_id": "YOUR_CLIENT_ID"}' \
  https://lumvi.net/api/admin/cache-invalidate
# → {"success": true, "new_kb_version": 4}
```

Watch your app logs for:
```
[Cache] Redis connected (red-xxxx:6379)
[Cache] kb_version bumped: client=abc123 → v4
[RedisCache HIT] client=abc123 v4 method=rag_pipeline conf=0.87
[Cache] SET  client=abc123 v4 method=rag_pipeline conf=0.87 ttl=86400s
```

---

## 9. requirements.txt additions

```
redis>=4.6.0
```

That's the only new dependency. `cache_utils.py` imports it inside a
try/except so the app still starts if `redis` is not installed
(falls back to in-process mode with a warning log).
