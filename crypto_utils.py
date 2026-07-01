"""
crypto_utils.py
----------------
Encrypt/decrypt third-party API credentials stored in client_integrations.
Used when an agency wires a client's external system (Calendly, Shopify,
a custom REST API, etc.) into Lumvi during onboarding — see models/integrations.py.

Requires INTEGRATION_ENCRYPTION_KEY in the environment (Render/local .env).
Generate one once with:

    python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

This key is NOT the same as SECRET_KEY (Flask sessions) or FLW_SECRET_KEY
(Lumvi's own Flutterwave key) — it exists solely to protect *client-supplied*
third-party credentials at rest. Rotating it requires re-encrypting every row
in client_integrations, so treat it as critical and back it up securely.
"""
import json
import os
from functools import lru_cache

from cryptography.fernet import Fernet, InvalidToken


@lru_cache(maxsize=1)
def _get_fernet() -> Fernet:
    key = os.environ.get('INTEGRATION_ENCRYPTION_KEY', '').strip()
    if not key:
        raise RuntimeError(
            "INTEGRATION_ENCRYPTION_KEY environment variable is not set. "
            "Add it to your Render/local .env before storing integration credentials."
        )
    return Fernet(key.encode())


def encrypt_credentials(creds: dict) -> str:
    """Serialise a credentials dict to encrypted text for storage."""
    payload = json.dumps(creds or {}).encode()
    return _get_fernet().encrypt(payload).decode()


def decrypt_credentials(token: str) -> dict:
    """Decrypt and parse stored credentials. Returns {} if token is empty/invalid."""
    if not token:
        return {}
    try:
        raw = _get_fernet().decrypt(token.encode())
        return json.loads(raw.decode())
    except (InvalidToken, ValueError, json.JSONDecodeError):
        return {}
