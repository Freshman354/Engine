"""
app_utils.py
------------
Shared utility functions used by app.py and every blueprint.

Keep this module free of Flask app-context dependencies (no `app`, no `mail`,
no `models`) so it can be imported at module load time without side-effects.
"""

import re


def sanitize_input(text, max_length: int = 500) -> str:
    """
    Strip HTML tags, collapse whitespace, and truncate to max_length.
    Returns an empty string for any non-string or falsy input.
    Applied to all user-supplied strings before storage or processing.
    """
    if not text or not isinstance(text, str):
        return ""
    text = re.sub(r'<[^>]+>', '', text)
    text = text[:max_length]
    text = ' '.join(text.split())
    return text.strip()
