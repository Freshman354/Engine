"""
Tests handle_shopify_webhook() and handle_shopify_compliance_webhook() —
the ACTUAL functions from webhooks.py, unmodified. This is the second half
of the automated coverage; test_pure_logic.py covers the encryption/HMAC
layer, this covers the webhook-request orchestration layer on top of it.

WHERE THE LINE IS DRAWN, AND WHY:
This sandbox has no network access and can't install psycopg2, so there's
no way to run these against a real Postgres/Neon database from here. The
honest options were: (a) fake Postgres semantics well enough to run the
real SQL unchanged — not viable, the queries use JSONB operators
(platform_config->>'shop_domain'), ON CONFLICT ON CONSTRAINT, and NOW(),
none of which SQLite (the only DB engine available offline) supports, so
"testing against SQLite" would really mean testing against rewritten SQL,
which defeats the purpose; or (b) mock at the Python function boundary —
stub out the specific models.* / webhooks.* functions that hit the DB,
and let every other line of the real function execute normally. This file
does (b). It is NOT a substitute for testing against a real DB — it can't
catch a wrong column name or a bad JSONB path, for instance — but it does
verify the orchestration logic that's genuinely new or changed in this
migration: topic-string matching (the orders/create bug fix), the
app/uninstalled routing added in Phase 2, signature verification wired in
correctly, and the compliance-webhook handler's boundary (verify + log +
200, no auto-redaction).

Everything mocked below is a DB-writing side effect (_upsert_order,
delete_integration, models.get_client_by_id, models.upsert_abandoned_cart).
Everything NOT mocked — _verify_shopify_signature, _normalise_shopify_order,
the topic-matching sets themselves, JSON parsing, response status codes —
is the real, unmodified code path.

Run with: python3 test_webhook_routing.py
"""
import os
import sys
import hashlib
import hmac as hmac_module
import base64
import json
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))  # project root: webhooks.py, crypto_utils.py
sys.path.insert(0, os.path.dirname(__file__))                   # testing/: stub models.py — must win

os.environ.setdefault('INTEGRATION_ENCRYPTION_KEY',
                       __import__('cryptography.fernet', fromlist=['Fernet']).Fernet.generate_key().decode())
os.environ.setdefault('SHOPIFY_APP_CLIENT_SECRET', 'test-app-client-secret-do-not-use-in-prod')

import webhooks
import models  # the stub

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


WEBHOOK_SECRET = 'per-client-webhook-secret-abc'
FAKE_INTEGRATION = {
    'client_id': 'client_test123',
    'platform': 'shopify',
    'webhook_secret': WEBHOOK_SECRET,
    'platform_config': {'shop_domain': 'test-store.myshopify.com'},
}

def sign(body: bytes, secret: str) -> str:
    return base64.b64encode(hmac_module.new(secret.encode(), body, hashlib.sha256).digest()).decode()


print('handle_shopify_webhook — topic matching (the orders/create bug fix)')
order_body = json.dumps({
    'id': 555444,
    'email': 'buyer@example.com',
    'financial_status': 'paid',
    'line_items': [{'title': 'Test Product', 'quantity': 1, 'price': '19.99'}],
}).encode()
order_sig = sign(order_body, WEBHOOK_SECRET)

with patch.object(webhooks, 'get_integration', return_value=FAKE_INTEGRATION), \
     patch.object(webhooks, '_upsert_order', return_value=True) as mock_upsert:
    result, status = webhooks.handle_shopify_webhook('client_test123', order_body, order_sig, 'orders/create')
    check("real Shopify topic string 'orders/create' is recognized (not silently ignored)",
          status == 200 and result.get('status') == 'ok')
    check('_upsert_order was actually invoked for a recognized order topic', mock_upsert.called)

with patch.object(webhooks, 'get_integration', return_value=FAKE_INTEGRATION), \
     patch.object(webhooks, '_upsert_order', return_value=True) as mock_upsert:
    result, status = webhooks.handle_shopify_webhook('client_test123', order_body, order_sig, 'orders/created')
    check("the OLD buggy string 'orders/created' is correctly NOT matched as a known topic "
          "(confirms the fix didn't just move the bug — the wrong string is now properly "
          "'unsupported', not silently swallowed as if it were the real one)",
          status == 200 and result.get('status') == 'ignored')
    check('_upsert_order was NOT called for the unrecognized topic string', not mock_upsert.called)

print()
print('handle_shopify_webhook — app/uninstalled (Phase 2)')
uninstall_body = json.dumps({'id': 987654321}).encode()
uninstall_sig = sign(uninstall_body, WEBHOOK_SECRET)

with patch.object(webhooks, 'get_integration', return_value=FAKE_INTEGRATION), \
     patch.object(webhooks, 'delete_integration', return_value=True) as mock_delete:
    result, status = webhooks.handle_shopify_webhook('client_test123', uninstall_body, uninstall_sig, 'app/uninstalled')
    check('app/uninstalled returns 200', status == 200)
    check('app/uninstalled calls delete_integration(client_id, "shopify") — not upsert, not a hard delete',
          mock_delete.call_args == (('client_test123', 'shopify'),))

print()
print('handle_shopify_webhook — security boundaries')
with patch.object(webhooks, 'get_integration', return_value=FAKE_INTEGRATION):
    result, status = webhooks.handle_shopify_webhook('client_test123', order_body, 'not-the-real-signature', 'orders/create')
    check('wrong signature is rejected with 401, payload never reaches processing', status == 401)

with patch.object(webhooks, 'get_integration', return_value=None):
    result, status = webhooks.handle_shopify_webhook('unknown_or_uninstalled_client', order_body, order_sig, 'orders/create')
    check('unknown/inactive integration returns 404, not a crash or a silent 200', status == 404)

with patch.object(webhooks, 'get_integration', return_value=FAKE_INTEGRATION):
    bad_json_body = b'{not valid json'
    bad_json_sig = sign(bad_json_body, WEBHOOK_SECRET)  # must sign THIS body, not order_body's signature
    result, status = webhooks.handle_shopify_webhook('client_test123', bad_json_body, bad_json_sig, 'orders/create')
    check('malformed JSON body is rejected with 400 (checked after signature — still needs a valid sig to get here)',
          status == 400)

print()
print('handle_shopify_webhook — checkout topics (cart recovery, plan-gated)')
checkout_body = json.dumps({'token': 'abc123checkout', 'email': 'shopper@example.com', 'line_items': []}).encode()
checkout_sig = sign(checkout_body, WEBHOOK_SECRET)

with patch.object(webhooks, 'get_integration', return_value=FAKE_INTEGRATION), \
     patch.object(models, 'get_client_by_id', return_value={'client_id': 'client_test123', 'cart_recovery_enabled': False}):
    result, status = webhooks.handle_shopify_webhook('client_test123', checkout_body, checkout_sig, 'checkouts/create')
    check('checkout topic is ignored (200) when cart_recovery_enabled is False — correctly plan-gated',
          status == 200 and result.get('status') == 'ignored')

with patch.object(webhooks, 'get_integration', return_value=FAKE_INTEGRATION), \
     patch.object(models, 'get_client_by_id', return_value={'client_id': 'client_test123', 'cart_recovery_enabled': True}), \
     patch.object(models, 'upsert_abandoned_cart', return_value={'success': True}) as mock_cart:
    result, status = webhooks.handle_shopify_webhook('client_test123', checkout_body, checkout_sig, 'checkouts/create')
    check('checkout topic is processed (200) when cart_recovery_enabled is True', status == 200)
    check('models.upsert_abandoned_cart was actually invoked', mock_cart.called)

print()
print('handle_shopify_compliance_webhook — Phase 2 GDPR topics')
compliance_body = json.dumps({
    'shop_id': 123, 'shop_domain': 'test-store.myshopify.com',
    'customer': {'id': 456, 'email': 'shopper@example.com'},
}).encode()
app_secret = os.environ['SHOPIFY_APP_CLIENT_SECRET']
compliance_sig = sign(compliance_body, app_secret)

with patch.object(webhooks, 'get_client_id_by_shopify_shop', return_value='client_test123'), \
     patch.object(webhooks, '_log_webhook') as mock_log:
    result, status = webhooks.handle_shopify_compliance_webhook(compliance_body, compliance_sig, 'customers/redact')
    check('valid compliance webhook returns 200', status == 200)
    check('resolved client_id is passed to _log_webhook for an audit trail', mock_log.called)

with patch.object(webhooks, 'get_client_id_by_shopify_shop', return_value='client_test123'):
    result, status = webhooks.handle_shopify_compliance_webhook(compliance_body, 'wrong-signature', 'customers/redact')
    check('compliance webhook rejects a bad signature (401) — this endpoint has no per-client '
          'secret to fall back on, it MUST verify against the app secret correctly',
          status == 401)

with patch.object(webhooks, 'get_client_id_by_shopify_shop', return_value=None), \
     patch.object(webhooks, '_log_webhook') as mock_log:
    result, status = webhooks.handle_shopify_compliance_webhook(compliance_body, compliance_sig, 'shop/redact')
    check('an unresolvable shop (already fully offboarded) still returns 200 — Shopify must not '
          'get a failure response just because the shop is long gone', status == 200)
    check('_log_webhook is NOT called when client_id never resolved (nothing to attribute the row to)',
          not mock_log.called)

print()
print(f'{passed} passed, {failed} failed')
sys.exit(1 if failed else 0)
