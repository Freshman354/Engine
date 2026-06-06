"""
utils.py
========
Shared utilities: logging setup and the _log_crash helper.
Import logger and _log_crash from here across all modules.
"""

import logging
import os
from typing import Any

_LOG_LEVEL = os.environ.get('LUMVI_LOG_LEVEL', 'INFO').upper()


def get_logger(name: str) -> logging.Logger:
    lg = logging.getLogger(name)
    lg.setLevel(getattr(logging, _LOG_LEVEL, logging.INFO))
    return lg


def log_crash(logger: logging.Logger, tag: str, err: Exception, **ctx: Any) -> None:
    """Structured exception logger. All modules call this instead of bare logger.error."""
    ctx_str = ' '.join(f'{k}={v}' for k, v in ctx.items())
    logger.error(f"[{tag}] {type(err).__name__}: {err} | {ctx_str}", exc_info=True)


# ── Gemini generation helper ──────────────────────────────────────────────────
# Wraps google.genai client so pipeline stages call generate(model, prompt)
# with the same interface regardless of SDK version.

_DEFAULT_GENAI_CONFIG = {
    'temperature':     0.3,
    'max_output_tokens': 512,
}


def generate(model_client: 'Any', prompt: str, model_name: str = '') -> 'Any':
    """
    Thin wrapper around google.genai client.models.generate_content().
    Pipeline stages call this instead of model.generate_content(prompt)
    so the SDK migration is contained in one place.

    model_client: the google.genai Client().models object from AIHelper
    model_name:   e.g. 'gemini-2.0-flash' — falls back to GEMINI_MODEL env var
    """
    try:
        from google.genai import types as _types
        name = model_name or os.environ.get('GEMINI_MODEL', 'gemini-2.0-flash')
        response = model_client.generate_content(
            model=name,
            contents=prompt,
            config=_types.GenerateContentConfig(
                temperature=_DEFAULT_GENAI_CONFIG['temperature'],
                max_output_tokens=_DEFAULT_GENAI_CONFIG['max_output_tokens'],
            ),
        )
        return response
    except Exception as _e:
        raise _e
