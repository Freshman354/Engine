"""
Real, runnable tests against the ACTUAL crypto_utils.py and webhooks.py
files from this migration — not reimplementations of their logic. A stub
models.py sits next to this script purely so `import webhooks` succeeds
without a live Postgres connection (unavailable in this sandbox — no
network access, psycopg2 isn't installable here). Every function called
below is the real, unmodified production code; nothing about the
encryption, HMAC, or field-redaction logic itself is faked.

What this does NOT cover — genuinely can't, from this sandbox:
  - Anything touching a real DB (get_integration, upsert_integration,
    get_client_id_by_shopify_shop, list_integrations, delete_integration)
  - Anything touching real Shopify (OAuth token exchange, webhook
    registration, shop.json fetch)
  - app.py's routes end-to-end (needs Flask, flask_login, a live app
    context, a live DB)
That's what the manual test plan alongside this script is for.

Run with: python3 test_pure_logic.py
"""
import os
import sys
import hashlib
import hmac as hmac_module
import base64
import json

# Real files under test — /home/claude/work has the actual, unmodified-
# except-by-this-migration webhooks.py and crypto_utils.py.
# Real files under test — the project root (one level up) has the actual
# webhooks.py / crypto_utils.py. That same root also has the REAL models/
# package now (this test file ships inside the full project structure),
# so the local stub models.py (right here in testing/) must go LAST in
# insertion order to end up FIRST in sys.path — otherwise `import models`
# would resolve to the real package and fail on the missing psycopg2
# dependency, which is exactly what it's being stubbed out to avoid.
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))  # project root: webhooks.py, crypto_utils.py
sys.path.insert(0, os.path.dirname(__file__))                   # testing/: stub models.py — must win

# Needed before import — crypto_utils.py raises at call time (not import
# time) if this is missing; webhooks.py's _decrypt_secret call path needs
# it live. Fresh throwaway key, this process only.
os.environ.setdefault('INTEGRATION_ENCRYPTION_KEY',
                       __import__('cryptography.fernet', fromlist=['Fernet']).Fernet.generate_key().decode())
os.environ.setdefault('SHOPIFY_APP_CLIENT_SECRET', 'test-app-client-secret-do-not-use-in-prod')

import crypto_utils
import webhooks

passed = 0
failed = 0

def check(label, condition):
    global passed, failed
    if condition:
        passed += 1
        print(f'  PASS  {label}')
    else:
        failed += 1
        print(f'  FAIL  {label}')


print('crypto_utils.py — encrypt_credentials / decrypt_credentials')
blob = crypto_utils.encrypt_credentials({'access_token': 'shpat_realtoken123'})
check('encrypted blob is not the plaintext token', 'shpat_realtoken123' not in blob)
decrypted = crypto_utils.decrypt_credentials(blob)
check('round-trips to the original value', decrypted.get('access_token') == 'shpat_realtoken123')
check('decrypt_credentials returns {} for garbage input (not an exception)',
      crypto_utils.decrypt_credentials('not-a-real-token') == {})

print()
print('webhooks.py — _encrypt_secret / _decrypt_secret (Phase 0)')
enc = webhooks._encrypt_secret('shpat_anothertoken')
check('encrypted value differs from plaintext', enc != 'shpat_anothertoken')
check('round-trips correctly', webhooks._decrypt_secret(enc) == 'shpat_anothertoken')
check('empty string passes through unchanged (no-op on falsy input)',
      webhooks._encrypt_secret('') == '' and webhooks._decrypt_secret('') == '')
check('a legacy PLAINTEXT value (pre-encryption row) passes through unchanged '
      'on decrypt — this is what makes the migration backward compatible '
      'with rows written before Phase 0, with no backfill needed',
      webhooks._decrypt_secret('shpat_never_encrypted_legacy_value') == 'shpat_never_encrypted_legacy_value')

print()
print('webhooks.py — _encrypt_platform_config / _decrypt_platform_config')
cfg = {'shop_domain': 'test-store.myshopify.com', 'access_token': 'shpat_xyz',
       'order_lookup_enabled': True}
enc_cfg = webhooks._encrypt_platform_config(cfg)
check('shop_domain (not sensitive) left untouched', enc_cfg['shop_domain'] == 'test-store.myshopify.com')
check('order_lookup_enabled (not sensitive) left untouched', enc_cfg['order_lookup_enabled'] is True)
check('access_token (sensitive) is encrypted', enc_cfg['access_token'] != 'shpat_xyz')
dec_cfg = webhooks._decrypt_platform_config(enc_cfg)
check('full round-trip restores the original config exactly', dec_cfg == cfg)

print()
print('webhooks.py — _verify_shopify_signature (per-integration HMAC)')
secret = 'a-per-client-webhook-secret'
body = b'{"id": 12345, "email": "buyer@example.com"}'
correct_sig = base64.b64encode(hmac_module.new(secret.encode(), body, hashlib.sha256).digest()).decode()
check('correct signature verifies', webhooks._verify_shopify_signature(body, correct_sig, secret))
check('tampered body fails verification',
      not webhooks._verify_shopify_signature(body + b'tampered', correct_sig, secret))
check('wrong secret fails verification',
      not webhooks._verify_shopify_signature(body, correct_sig, 'wrong-secret'))
check('missing signature header fails verification',
      not webhooks._verify_shopify_signature(body, '', secret))

print()
print('webhooks.py — _verify_shopify_app_signature (Phase 2, compliance webhooks)')
app_secret = os.environ['SHOPIFY_APP_CLIENT_SECRET']
app_sig = base64.b64encode(hmac_module.new(app_secret.encode(), body, hashlib.sha256).digest()).decode()
check('correct app-secret signature verifies', webhooks._verify_shopify_app_signature(body, app_sig))
check('tampered body fails', not webhooks._verify_shopify_app_signature(body + b'x', app_sig))
del os.environ['SHOPIFY_APP_CLIENT_SECRET']
check('missing SHOPIFY_APP_CLIENT_SECRET env var fails closed, not open',
      not webhooks._verify_shopify_app_signature(body, app_sig))
os.environ['SHOPIFY_APP_CLIENT_SECRET'] = app_secret  # restore for anything after

print()
print(f'{passed} passed, {failed} failed')
sys.exit(1 if failed else 0)
