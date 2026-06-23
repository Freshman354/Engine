"""
utils.py
========
Shared utilities: logging setup and the log_crash helper.
Import logger and log_crash from here across all modules.

Changes from original:
  - generate() now catches Gemini quota/auth errors and transparently
    retries on Groq (llama-3.1-8b-instant) before re-raising.
  - GroqResponse wraps Groq's completion so resp.text works identically
    to google.genai's GenerateContentResponse — zero changes needed in
    any pipeline stage.
  - GROQ_API_KEY env var controls whether fallback is active.
    If unset, behaviour is identical to the original utils.py.
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


# ── Groq response wrapper ─────────────────────────────────────────────────────

class _GroqResponse:
    """
    Makes Groq's chat completion look identical to google.genai's
    GenerateContentResponse. Every pipeline stage does resp.text — this
    ensures they work without any changes.
    """
    def __init__(self, completion: Any) -> None:
        try:
            self.text: Optional[str] = (
                completion.choices[0].message.content or ''
            )
        except Exception:
            self.text = ''

    def __repr__(self) -> str:
        preview = (self.text or '')[:60].replace('\n', ' ')
        return f'<_GroqResponse text={preview!r}>'


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

def generate(model_client: Any, prompt: str, model_name: str = '') -> Any:
    """
    Thin wrapper around google.genai client.models.generate_content().
    Transparently falls back to Groq on quota/auth errors.

    Signature is identical to the original — no changes needed in any
    pipeline stage that calls this.

    model_client : google.genai Client().models object from AIHelper
    prompt       : plain text prompt string
    model_name   : e.g. 'gemini-2.0-flash' — falls back to GEMINI_MODEL env var
    """
    # ── Attempt 1: Gemini ─────────────────────────────────────────────────────
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

    # ── Attempt 2: Groq fallback ──────────────────────────────────────────────
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
        resp = _GroqResponse(completion)
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