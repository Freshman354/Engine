"""
utils.py
========
Shared utilities: logging setup and the log_crash helper.
Import logger and log_crash from here across all modules.

Provider switch
----------------
AI_PROVIDER env var controls which LLM is primary for generate():
  'gemini'      (default) — Gemini primary, Groq fallback on quota/auth errors.
                 Unchanged from prior behavior if AI_PROVIDER is unset.
  'openrouter'  — OpenRouter (Llama 4 Maverick by default, OPENROUTER_MODEL
                 env var to change it) primary, Groq fallback on error.
                 Gemini is not called at all in this mode.

Changes from original:
  - generate() now catches Gemini quota/auth errors and transparently
    retries on Groq (llama-3.1-8b-instant) before re-raising.
  - _OpenAICompatResponse wraps Groq/OpenRouter completions so resp.text
    works identically to google.genai's GenerateContentResponse — zero
    changes needed in any pipeline stage.
  - GROQ_API_KEY / OPENROUTER_API_KEY env vars control whether each
    provider is available. If neither AI_PROVIDER nor these are set,
    behaviour is identical to the original utils.py.
  - Quota events are logged at ERROR level so you can see them in Render logs.
"""

import logging
import os
from typing import Any, Optional

_LOG_LEVEL = os.environ.get('LUMVI_LOG_LEVEL', 'INFO').upper()


def get_logger(name: str) -> logging.Logger:
    lg = logging.getLogger(name)
    lg.setLevel(getattr(logging, _LOG_LEVEL, logging.INFO))
    return lg


def log_crash(logger: logging.Logger, tag: str, err: Exception, **ctx: Any) -> None:
    """Structured exception logger. All modules call this instead of bare logger.error."""
    ctx_str = ' '.join(f'{k}={v}' for k, v in ctx.items())
    logger.error(f"[{tag}] {type(err).__name__}: {err} | {ctx_str}", exc_info=True)


# ── Gemini generation config ──────────────────────────────────────────────────

_DEFAULT_GENAI_CONFIG = {
    'temperature':       0.3,
    'max_output_tokens': 512,
}

_logger = get_logger('lumvi.utils')

# ── Provider switch ─────────────────────────────────────────────────────────
# AI_PROVIDER_ENV is the deployment-time default (Render env var). The admin
# dashboard's System page can override it live via a system_settings DB row
# (models/system_settings.py) — get_ai_provider() checks that first, with a
# short in-process cache, and falls back to the env var if no override is
# set. This is what makes the switch actually live: nothing that reads the
# provider should ever cache it for the lifetime of the process (that's
# what the old plain `AI_PROVIDER = os.environ.get(...)` module constant did
# — it was frozen at import time and a toggle would've needed a restart).

AI_PROVIDER_ENV = os.environ.get('AI_PROVIDER', 'gemini').strip().lower()
if AI_PROVIDER_ENV not in ('gemini', 'openrouter'):
    _logger.warning(f"[Utils] Unknown AI_PROVIDER={AI_PROVIDER_ENV!r}, defaulting to 'gemini'")
    AI_PROVIDER_ENV = 'gemini'

OPENROUTER_MODEL = os.environ.get('OPENROUTER_MODEL', 'meta-llama/llama-4-maverick')

_provider_cache: dict = {'value': None, 'expires_at': 0.0}
_PROVIDER_CACHE_TTL_SECONDS = 15


def get_ai_provider() -> str:
    """
    The live, current provider ('gemini' | 'openrouter'). Checks the
    system_settings DB override first (admin dashboard toggle), cached
    in-process for _PROVIDER_CACHE_TTL_SECONDS so a toggle takes effect
    within ~15s across every Gunicorn worker without a restart, and falls
    back to AI_PROVIDER_ENV if no override row exists yet.
    """
    import time
    now = time.time()
    if _provider_cache['value'] is not None and _provider_cache['expires_at'] > now:
        return _provider_cache['value']

    value = AI_PROVIDER_ENV
    try:
        import models
        stored = models.get_setting('ai_provider')
        if stored in ('gemini', 'openrouter'):
            value = stored
    except Exception as e:
        _logger.warning(f'[Utils] get_ai_provider DB lookup failed, using env default: {e}')

    _provider_cache['value'] = value
    _provider_cache['expires_at'] = now + _PROVIDER_CACHE_TTL_SECONDS
    return value


# ── Groq fallback — lazy client ───────────────────────────────────────────────

_groq_client: Optional[Any] = None
_groq_model                 = 'llama-3.1-8b-instant'
_groq_lock                  = __import__('threading').Lock()


def _get_groq_client() -> Optional[Any]:
    """
    Lazy-init the Groq client using the OpenAI SDK (Groq is API-compatible).
    Returns None if GROQ_API_KEY is not set — fallback stays disabled silently.
    """
    global _groq_client
    if _groq_client is not None:
        return _groq_client

    api_key = os.environ.get('GROQ_API_KEY', '').strip()
    if not api_key:
        return None

    with _groq_lock:
        if _groq_client is None:
            try:
                from openai import OpenAI
                _groq_client = OpenAI(
                    base_url='https://api.groq.com/openai/v1',
                    api_key=api_key,
                )
                _logger.info(
                    f'[Utils] Groq fallback client initialised model={_groq_model}'
                )
            except Exception as e:
                _logger.warning(
                    f'[Utils] Groq client init failed — fallback disabled: {e}'
                )
                return None

    return _groq_client


# ── OpenRouter — lazy client ──────────────────────────────────────────────────

_openrouter_client: Optional[Any] = None
_openrouter_lock                  = __import__('threading').Lock()


def _get_openrouter_client() -> Optional[Any]:
    """
    Lazy-init the OpenRouter client using the OpenAI SDK (OpenRouter is
    API-compatible). Returns None if OPENROUTER_API_KEY is not set.
    """
    global _openrouter_client
    if _openrouter_client is not None:
        return _openrouter_client

    api_key = os.environ.get('OPENROUTER_API_KEY', '').strip()
    if not api_key:
        return None

    with _openrouter_lock:
        if _openrouter_client is None:
            try:
                from openai import OpenAI
                _openrouter_client = OpenAI(
                    base_url='https://openrouter.ai/api/v1',
                    api_key=api_key,
                )
                _logger.info(
                    f'[Utils] OpenRouter client initialised model={OPENROUTER_MODEL}'
                )
            except Exception as e:
                _logger.warning(
                    f'[Utils] OpenRouter client init failed: {e}'
                )
                return None

    return _openrouter_client


# ── OpenAI-compatible response wrapper (Groq + OpenRouter share this shape) ──

class _OpenAICompatResponse:
    """
    Makes an OpenAI-compatible chat completion (Groq, OpenRouter) look
    identical to google.genai's GenerateContentResponse. Every pipeline
    stage does resp.text — this ensures they work without any changes,
    regardless of which provider actually answered.
    """
    def __init__(self, completion: Any) -> None:
        try:
            self.text: Optional[str] = (
                completion.choices[0].message.content or ''
            )
        except Exception:
            self.text = ''
        try:
            self.input_tokens  = completion.usage.prompt_tokens
            self.output_tokens = completion.usage.completion_tokens
        except Exception:
            self.input_tokens  = None
            self.output_tokens = None

    def __repr__(self) -> str:
        preview = (self.text or '')[:60].replace('\n', ' ')
        return f'<_OpenAICompatResponse text={preview!r}>'


def _extract_usage(resp: Any) -> tuple:
    """
    Returns (input_tokens, output_tokens) for either response shape —
    google.genai's native response (.usage_metadata) or
    _OpenAICompatResponse (.input_tokens/.output_tokens). Returns
    (None, None) if usage data isn't available for any reason; callers
    should skip logging rather than log zeros, which would be wrong data,
    not missing data.
    """
    try:
        if hasattr(resp, 'usage_metadata') and resp.usage_metadata is not None:
            return (
                getattr(resp.usage_metadata, 'prompt_token_count', None),
                getattr(resp.usage_metadata, 'candidates_token_count', None),
            )
    except Exception:
        pass
    try:
        if hasattr(resp, 'input_tokens'):
            return (resp.input_tokens, resp.output_tokens)
    except Exception:
        pass
    return (None, None)


# Backward-compatible alias — _GroqResponse was the name before OpenRouter
# was added; nothing outside this file should reference it, but keeping
# the alias avoids surprises if something does.
_GroqResponse = _OpenAICompatResponse


# ── Quota error detection ─────────────────────────────────────────────────────

def _is_quota_error(exc: Exception) -> bool:
    """
    Return True if this is a Gemini quota exhaustion or auth error that
    warrants a Groq fallback attempt.

    Covers ResourceExhausted (429), PermissionDenied (bad/revoked key),
    and ServiceUnavailable (503). Uses both isinstance checks and string
    matching so it works regardless of whether google-api-core is installed.
    """
    try:
        from google.api_core.exceptions import (
            ResourceExhausted,
            PermissionDenied,
            ServiceUnavailable,
        )
        if isinstance(exc, (ResourceExhausted, PermissionDenied, ServiceUnavailable)):
            return True
    except ImportError:
        pass

    exc_type = type(exc).__name__
    exc_msg  = str(exc).lower()

    quota_types = {'ResourceExhausted', 'PermissionDenied', 'ServiceUnavailable'}
    quota_msgs  = {
        'quota', '429', 'rate limit', 'resource exhausted',
        'permission denied', 'api key', 'service unavailable',
    }

    return (
        exc_type in quota_types
        or any(kw in exc_msg for kw in quota_msgs)
    )


# ── Core generate() ───────────────────────────────────────────────────────────

def generate(model_client: Any, prompt: str, model_name: str = '',
             client_id: Optional[str] = None, endpoint: Optional[str] = None) -> Any:
    """
    Text generation entry point used by every pipeline stage. Routes to
    Gemini or OpenRouter based on the live AI_PROVIDER switch (see module
    docstring).

    model_client : google.genai Client().models object from AIHelper.
                   Ignored when the live provider is 'openrouter'.
    prompt       : plain text prompt string
    model_name   : e.g. 'gemini-2.0-flash'. Ignored when the live provider
                   is 'openrouter' (OPENROUTER_MODEL env var is used instead).
    client_id    : optional — when provided, this call's token usage is
                   logged to api_usage_log (models.log_api_usage) for the
                   Costs admin dashboard, regardless of which provider
                   actually answered. Omit to skip cost logging (e.g. for
                   calls that aren't part of a specific client's usage).
    endpoint     : optional label for which pipeline stage made this call
                   (e.g. 'rag_generate', 'dynamic_fallback') — shows up in
                   the Costs dashboard's per-endpoint breakdown.
    """
    provider = get_ai_provider()
    if provider == 'openrouter':
        resp = _generate_openrouter_primary(prompt)
        used_model = OPENROUTER_MODEL
    else:
        resp = _generate_gemini_primary(model_client, prompt, model_name)
        used_model = model_name or os.environ.get('GEMINI_MODEL', 'gemini-2.0-flash')

    if client_id:
        _log_usage_fire_and_forget(resp, client_id, used_model, endpoint)

    return resp


def _log_usage_fire_and_forget(resp: Any, client_id: str, model: str, endpoint: Optional[str]) -> None:
    """Never allowed to raise or block — a cost-logging failure must never affect the chat response."""
    try:
        input_tokens, output_tokens = _extract_usage(resp)
        if input_tokens is None and output_tokens is None:
            return   # no usage data available (e.g. call errored before completion) — skip rather than log zeros
        import models
        user_id = models.get_client_owner_id(client_id)
        if not user_id:
            return
        models.log_api_usage(
            user_id=user_id, client_id=client_id,
            input_tokens=input_tokens or 0, output_tokens=output_tokens or 0,
            model=model, endpoint=endpoint,
        )
    except Exception as e:
        _logger.warning(f'[Utils] Cost logging failed (non-fatal): {e}')


def _generate_gemini_primary(model_client: Any, prompt: str, model_name: str = '') -> Any:
    """Original behavior: Gemini primary, Groq fallback on quota/auth errors."""
    gemini_exc: Optional[Exception] = None
    try:
        from google.genai import types as _types
        name = model_name or os.environ.get('GEMINI_MODEL', 'gemini-2.0-flash')
        return model_client.generate_content(
            model=name,
            contents=prompt,
            config=_types.GenerateContentConfig(
                temperature=_DEFAULT_GENAI_CONFIG['temperature'],
                max_output_tokens=_DEFAULT_GENAI_CONFIG['max_output_tokens'],
            ),
        )
    except Exception as exc:
        if not _is_quota_error(exc):
            raise   # Non-quota error — don't mask it, raise immediately

        gemini_exc = exc
        _logger.error(
            f'[Utils] Gemini quota/auth error — attempting Groq fallback. '
            f'error={type(exc).__name__}: {str(exc)[:120]}'
        )

    groq = _get_groq_client()
    if groq is None:
        _logger.error(
            '[Utils] Groq fallback not available (GROQ_API_KEY not set). '
            'Add GROQ_API_KEY to your Render env vars to enable it.'
        )
        raise gemini_exc

    try:
        _logger.info(f'[Utils] Routing to Groq model={_groq_model}')
        completion = groq.chat.completions.create(
            model=_groq_model,
            messages=[{'role': 'user', 'content': prompt}],
            temperature=_DEFAULT_GENAI_CONFIG['temperature'],
            max_tokens=_DEFAULT_GENAI_CONFIG['max_output_tokens'],
        )
        resp = _OpenAICompatResponse(completion)
        _logger.info(
            f'[Utils] Groq fallback succeeded '
            f'len={len(resp.text or "")} model={_groq_model}'
        )
        return resp

    except Exception as groq_exc:
        _logger.error(
            f'[Utils] Groq fallback also failed. '
            f'groq_error={type(groq_exc).__name__}: {str(groq_exc)[:120]}'
        )
        raise gemini_exc   # Re-raise original Gemini error


def _generate_openrouter_primary(prompt: str) -> Any:
    """AI_PROVIDER='openrouter': OpenRouter (Llama 4 Maverick) primary, Groq fallback."""
    openrouter_exc: Optional[Exception] = None
    client = _get_openrouter_client()
    if client is None:
        openrouter_exc = RuntimeError(
            'OPENROUTER_API_KEY not set — cannot use AI_PROVIDER=openrouter'
        )
        _logger.error(f'[Utils] {openrouter_exc}')
    else:
        try:
            completion = client.chat.completions.create(
                model=OPENROUTER_MODEL,
                messages=[{'role': 'user', 'content': prompt}],
                temperature=_DEFAULT_GENAI_CONFIG['temperature'],
                max_tokens=_DEFAULT_GENAI_CONFIG['max_output_tokens'],
            )
            return _OpenAICompatResponse(completion)
        except Exception as exc:
            if not _is_quota_error(exc):
                raise   # Non-quota error — don't mask it, raise immediately
            openrouter_exc = exc
            _logger.error(
                f'[Utils] OpenRouter quota/auth error — attempting Groq fallback. '
                f'error={type(exc).__name__}: {str(exc)[:120]}'
            )

    groq = _get_groq_client()
    if groq is None:
        _logger.error(
            '[Utils] Groq fallback not available (GROQ_API_KEY not set). '
            'Add GROQ_API_KEY to your Render env vars to enable it.'
        )
        raise openrouter_exc

    try:
        _logger.info(f'[Utils] Routing to Groq model={_groq_model}')
        completion = groq.chat.completions.create(
            model=_groq_model,
            messages=[{'role': 'user', 'content': prompt}],
            temperature=_DEFAULT_GENAI_CONFIG['temperature'],
            max_tokens=_DEFAULT_GENAI_CONFIG['max_output_tokens'],
        )
        resp = _OpenAICompatResponse(completion)
        _logger.info(
            f'[Utils] Groq fallback succeeded '
            f'len={len(resp.text or "")} model={_groq_model}'
        )
        return resp

    except Exception as groq_exc:
        _logger.error(
            f'[Utils] Groq fallback also failed. '
            f'groq_error={type(groq_exc).__name__}: {str(groq_exc)[:120]}'
        )
        raise openrouter_exc