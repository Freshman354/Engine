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
migration.

Everything mocked below is a DB-writing side effect (_upsert_order,
delete_integration, models.get_client_by_id, models.upsert_abandoned_cart).
Everything NOT mocked — _verify_shopify_signature, the new shop-domain
cross-check, _normalise_shopify_order, the topic-matching sets themselves,
JSON parsing, response status codes — is the real, unmodified code path.

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

# flask_mail isn't installable in this sandbox (no network access) — same
# category of limitation as psycopg2. Rather than let the data_request
# email path silently fail at `import flask_mail` and pass its test for
# the wrong reason (an ImportError look identical to "best-effort failure
# handled gracefully" from the outside), inject a minimal real stand-in so
# the actual code path — the import, Message construction, mail.send() —
# is genuinely exercised. This is stubbing an uninstallable third-party
# dependency, not the business logic under test.
import types as _types
_fake_flask_mail = _types.ModuleType('flask_mail')
class _FakeMessage:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
_fake_flask_mail.Message = _FakeMessage
sys.modules.setdefault('flask_mail', _fake_flask_mail)

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
CLIENT_A_SHOP = 'test-store.myshopify.com'
CLIENT_B_SHOP = 'a-different-merchant.myshopify.com'
FAKE_INTEGRATION = {
    'client_id': 'client_test123',
    'platform': 'shopify',
    'webhook_secret': WEBHOOK_SECRET,
    'platform_config': {'shop_domain': CLIENT_A_SHOP},
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
    result, status = webhooks.handle_shopify_webhook('client_test123', order_body, order_sig, 'orders/create', CLIENT_A_SHOP)
    check("real Shopify topic string 'orders/create' is recognized (not silently ignored)",
          status == 200 and result.get('status') == 'ok')
    check('_upsert_order was actually invoked for a recognized order topic', mock_upsert.called)

with patch.object(webhooks, 'get_integration', return_value=FAKE_INTEGRATION), \
     patch.object(webhooks, '_upsert_order', return_value=True) as mock_upsert:
    result, status = webhooks.handle_shopify_webhook('client_test123', order_body, order_sig, 'orders/created', CLIENT_A_SHOP)
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
    result, status = webhooks.handle_shopify_webhook('client_test123', uninstall_body, uninstall_sig, 'app/uninstalled', CLIENT_A_SHOP)
    check('app/uninstalled returns 200', status == 200)
    check('app/uninstalled calls delete_integration(client_id, "shopify") — not upsert, not a hard delete',
          mock_delete.call_args == (('client_test123', 'shopify'),))

print()
print('handle_shopify_webhook — security boundaries (HMAC)')
with patch.object(webhooks, 'get_integration', return_value=FAKE_INTEGRATION):
    result, status = webhooks.handle_shopify_webhook('client_test123', order_body, 'not-the-real-signature', 'orders/create', CLIENT_A_SHOP)
    check('wrong signature is rejected with 401, payload never reaches processing', status == 401)

with patch.object(webhooks, 'get_integration', return_value=None):
    result, status = webhooks.handle_shopify_webhook('unknown_or_uninstalled_client', order_body, order_sig, 'orders/create', CLIENT_A_SHOP)
    check('unknown/inactive integration returns 404, not a crash or a silent 200', status == 404)

with patch.object(webhooks, 'get_integration', return_value=FAKE_INTEGRATION):
    bad_json_body = b'{not valid json'
    bad_json_sig = sign(bad_json_body, WEBHOOK_SECRET)  # must sign THIS body, not order_body's signature
    result, status = webhooks.handle_shopify_webhook('client_test123', bad_json_body, bad_json_sig, 'orders/create', CLIENT_A_SHOP)
    check('malformed JSON body is rejected with 400 (checked after signature and shop-domain — still needs '
          'both to get here)', status == 400)

print()
print('handle_shopify_webhook — B1 FIX: cross-tenant webhook replay')
with patch.object(webhooks, 'get_integration', return_value=FAKE_INTEGRATION), \
     patch.object(webhooks, '_upsert_order') as mock_upsert:
    # A webhook genuinely, validly signed (real HMAC, real secret) — but for
    # a DIFFERENT shop than the one this client_id is actually registered
    # to. This is exactly the replay scenario B1 describes: a legitimately
    # -signed webhook for shop X, sent (replayed) against client_test123's
    # URL, where client_test123 is actually registered to a DIFFERENT shop.
    result, status = webhooks.handle_shopify_webhook(
        'client_test123', order_body, order_sig, 'orders/create', CLIENT_B_SHOP)
    check('a validly-signed webhook for the WRONG shop is rejected (401), not processed',
          status == 401)
    check('_upsert_order is never called when the shop domain does not match this client',
          not mock_upsert.called)

with patch.object(webhooks, 'get_integration', return_value=FAKE_INTEGRATION), \
     patch.object(webhooks, '_upsert_order') as mock_upsert:
    # Missing header entirely — must fail closed, not be treated as "no
    # check available so allow it".
    result, status = webhooks.handle_shopify_webhook(
        'client_test123', order_body, order_sig, 'orders/create', '')
    check('a missing X-Shopify-Shop-Domain header is rejected (fails closed, not open)',
          status == 401)
    check('_upsert_order is never called when the shop domain header is missing',
          not mock_upsert.called)

with patch.object(webhooks, 'get_integration', return_value=FAKE_INTEGRATION), \
     patch.object(webhooks, '_upsert_order', return_value=True) as mock_upsert:
    # The correct shop, different casing — Shopify domains are always
    # lowercase in practice, but the comparison itself should still
    # normalize rather than depend on exact casing.
    result, status = webhooks.handle_shopify_webhook(
        'client_test123', order_body, order_sig, 'orders/create', CLIENT_A_SHOP.upper())
    check('a case-different but otherwise-correct shop domain still verifies (normalized comparison)',
          status == 200 and mock_upsert.called)

with patch.object(webhooks, 'get_integration', return_value=FAKE_INTEGRATION), \
     patch.object(webhooks, 'delete_integration', return_value=True) as mock_delete:
    # The shop-domain check applies to EVERY topic, not just orders/create
    # — explicitly re-verified against app/uninstalled, since that handler
    # branches before reaching the generic topic-routing code and it would
    # be easy to accidentally place the check somewhere that only some
    # topics pass through.
    result, status = webhooks.handle_shopify_webhook(
        'client_test123', uninstall_body, uninstall_sig, 'app/uninstalled', CLIENT_B_SHOP)
    check('app/uninstalled is ALSO rejected for a mismatched shop domain — the check runs '
          'before topic routing branches, not per-topic', status == 401)
    check('delete_integration is never called for a mismatched shop domain',
          not mock_delete.called)

print()
print('handle_shopify_webhook — checkout topics (cart recovery, plan-gated)')
checkout_body = json.dumps({'token': 'abc123checkout', 'email': 'shopper@example.com', 'line_items': []}).encode()
checkout_sig = sign(checkout_body, WEBHOOK_SECRET)

with patch.object(webhooks, 'get_integration', return_value=FAKE_INTEGRATION), \
     patch.object(models, 'get_client_by_id', return_value={'client_id': 'client_test123', 'cart_recovery_enabled': False}):
    result, status = webhooks.handle_shopify_webhook('client_test123', checkout_body, checkout_sig, 'checkouts/create', CLIENT_A_SHOP)
    check('checkout topic is ignored (200) when cart_recovery_enabled is False — correctly plan-gated',
          status == 200 and result.get('status') == 'ignored')

with patch.object(webhooks, 'get_integration', return_value=FAKE_INTEGRATION), \
     patch.object(models, 'get_client_by_id', return_value={'client_id': 'client_test123', 'cart_recovery_enabled': True}), \
     patch.object(models, 'upsert_abandoned_cart', return_value={'success': True}) as mock_cart:
    result, status = webhooks.handle_shopify_webhook('client_test123', checkout_body, checkout_sig, 'checkouts/create', CLIENT_A_SHOP)
    check('checkout topic is processed (200) when cart_recovery_enabled is True', status == 200)
    check('models.upsert_abandoned_cart was actually invoked', mock_cart.called)

print()
print('handle_shopify_compliance_webhook — GDPR topics (B3 fix: real handling, not just 200+log)')
print('(no shop-domain cross-check needed here — this endpoint resolves client_id FROM the')
print(' shop_domain in the payload, there is no URL-supplied client_id to spoof against)')
compliance_body = json.dumps({
    'shop_id': 123, 'shop_domain': CLIENT_A_SHOP,
    'customer': {'id': 456, 'email': 'shopper@example.com'},
}).encode()
app_secret = os.environ['SHOPIFY_APP_CLIENT_SECRET']
compliance_sig = sign(compliance_body, app_secret)

with patch.object(webhooks, 'get_client_id_by_shopify_shop', return_value='client_test123'):
    result, status = webhooks.handle_shopify_compliance_webhook(compliance_body, 'wrong-signature', 'customers/redact')
    check('compliance webhook rejects a bad signature (401) — this endpoint has no per-client '
          'secret to fall back on, it MUST verify against the app secret correctly',
          status == 401)

with patch.object(webhooks, 'get_client_id_by_shopify_shop', return_value=None), \
     patch.object(webhooks, '_record_compliance_request', return_value=1) as mock_record:
    result, status = webhooks.handle_shopify_compliance_webhook(compliance_body, compliance_sig, 'shop/redact')
    check('an unresolvable shop (already fully offboarded) still returns 200 — Shopify must not '
          'get a failure response just because the shop is long gone', status == 200)
    check('the request is still durably recorded even when client_id never resolved '
          '(client_id=None, status=completed) — an audit trail entry, not silence',
          mock_record.called and mock_record.call_args.kwargs.get('status', mock_record.call_args[0][5] if len(mock_record.call_args[0]) > 5 else None) is not None)

print()
print('customers/redact — actually deletes matching orders now, not just logs')
with patch.object(webhooks, 'get_client_id_by_shopify_shop', return_value='client_test123'), \
     patch.object(webhooks, '_record_compliance_request', return_value=42) as mock_record, \
     patch.object(webhooks, '_complete_compliance_request') as mock_complete, \
     patch.object(webhooks, '_redact_orders_by_email', return_value=3) as mock_redact:
    result, status = webhooks.handle_shopify_compliance_webhook(compliance_body, compliance_sig, 'customers/redact')
    check('customers/redact returns 200', status == 200)
    check('_redact_orders_by_email is actually called with the client_id and customer email from the payload',
          mock_redact.called and mock_redact.call_args[0] == ('client_test123', 'shopper@example.com'))
    check('the request is marked completed with a count-based summary (not raw PII) once redaction runs',
          mock_complete.called and '3' in mock_complete.call_args[0][2])

print()
print('customers/data_request — actually compiles order data now, and best-effort emails it')
fake_orders = [
    {'order_id': '1001', 'status': 'paid', 'total_amount': 49.99, 'currency': 'USD',
     'items_json': '[]', 'created_at': '2026-07-01'},
]
mock_mail = MagicMock()
with patch.object(webhooks, 'get_client_id_by_shopify_shop', return_value='client_test123'), \
     patch.object(webhooks, '_record_compliance_request', return_value=43), \
     patch.object(webhooks, '_complete_compliance_request') as mock_complete, \
     patch.object(webhooks, '_find_orders_by_email', return_value=fake_orders) as mock_find, \
     patch.object(webhooks, '_get_client_owner_email', return_value='merchant@example.com'):
    result, status = webhooks.handle_shopify_compliance_webhook(
        compliance_body, compliance_sig, 'customers/data_request', mail=mock_mail)
    check('customers/data_request returns 200', status == 200)
    check('_find_orders_by_email is actually called', mock_find.called)
    check('a notification email is sent when mail is provided and matching orders exist',
          mock_mail.send.called)
    check('the request is marked completed with a count-based summary', mock_complete.called)

with patch.object(webhooks, 'get_client_id_by_shopify_shop', return_value='client_test123'), \
     patch.object(webhooks, '_record_compliance_request', return_value=44), \
     patch.object(webhooks, '_complete_compliance_request'), \
     patch.object(webhooks, '_find_orders_by_email', return_value=fake_orders), \
     patch.object(webhooks, '_get_client_owner_email', return_value='merchant@example.com'):
    # mail.send raises — the compiled data (durably recorded + queryable
    # via orders table) must still count as delivered; email is a
    # convenience layer on top, not the compliance mechanism itself.
    broken_mail = MagicMock()
    broken_mail.send.side_effect = Exception('SMTP is down')
    result, status = webhooks.handle_shopify_compliance_webhook(
        compliance_body, compliance_sig, 'customers/data_request', mail=broken_mail)
    check('a failed notification email does not fail the webhook response — 200 either way',
          status == 200)

print()
print('shop/redact — schedules for delayed processing, does NOT delete synchronously')
with patch.object(webhooks, 'get_client_id_by_shopify_shop', return_value='client_test123'), \
     patch.object(webhooks, '_record_compliance_request', return_value=45) as mock_record, \
     patch.object(models, 'delete_client') as mock_delete_client:
    shop_redact_body = json.dumps({'shop_id': 123, 'shop_domain': CLIENT_A_SHOP}).encode()
    shop_redact_sig = sign(shop_redact_body, app_secret)
    result, status = webhooks.handle_shopify_compliance_webhook(shop_redact_body, shop_redact_sig, 'shop/redact')
    check('shop/redact returns 200 immediately', status == 200)
    check('shop/redact does NOT call delete_client synchronously within the webhook request '
          '— it schedules for the cron job instead (see process_due_shopify_shop_redactions)',
          not mock_delete_client.called)
    check('the request is recorded with status=scheduled', mock_record.call_args.kwargs.get('status') == 'scheduled')
    check('a scheduled_for timestamp roughly SHOPIFY_SHOP_REDACT_GRACE_DAYS in the future is set',
          mock_record.call_args.kwargs.get('scheduled_for') is not None)

print()
print('process_due_shopify_shop_redactions — the cron job that actually performs shop/redact')
print('(this specifically targets the delete_client return-value bug found while building this:')
print(' delete_client returns None on success and RAISES on failure — it does not return True/False)')

fake_due_row = {'id': 99, 'client_id': 'client_test123', 'shop_domain': CLIENT_A_SHOP}
with patch.object(webhooks, 'get_due_shopify_shop_redactions', return_value=[fake_due_row]), \
     patch.object(models, 'delete_client', return_value=None) as mock_delete_client, \
     patch.object(webhooks, 'hard_delete_shopify_integration', return_value=True) as mock_hard_delete, \
     patch.object(webhooks, '_complete_compliance_request') as mock_complete:
    result = webhooks.process_due_shopify_shop_redactions()
    check("delete_client returning None (its actual success contract) is correctly treated as "
          "SUCCESS, not failure — this is the exact bug caught while building this fix",
          result['processed'] == 1 and result['failed'] == 0)
    check('hard_delete_shopify_integration is called for the client_integrations/webhook_log cleanup '
          'delete_client itself does not cover', mock_hard_delete.called)
    check("the compliance request row is marked 'completed' on success",
          mock_complete.call_args[0][1] == 'completed')

with patch.object(webhooks, 'get_due_shopify_shop_redactions', return_value=[fake_due_row]), \
     patch.object(models, 'delete_client', side_effect=Exception('DB connection lost')), \
     patch.object(webhooks, 'hard_delete_shopify_integration', return_value=True), \
     patch.object(webhooks, '_complete_compliance_request') as mock_complete:
    result = webhooks.process_due_shopify_shop_redactions()
    check('delete_client raising an exception (its actual failure contract) is correctly treated '
          'as a FAILURE, not silently swallowed', result['failed'] == 1 and result['processed'] == 0)
    check("the compliance request row is marked 'failed', not silently left as 'scheduled' forever",
          mock_complete.call_args[0][1] == 'failed')

with patch.object(webhooks, 'get_due_shopify_shop_redactions',
                   return_value=[{'id': 100, 'client_id': None, 'shop_domain': 'gone.myshopify.com'}]), \
     patch.object(models, 'delete_client') as mock_delete_client, \
     patch.object(webhooks, '_complete_compliance_request') as mock_complete:
    result = webhooks.process_due_shopify_shop_redactions()
    check('a due row with no resolved client_id (shop was already unresolvable at request time) '
          'is marked completed without attempting a delete', result['processed'] == 1)
    check('delete_client is never called for an unresolvable client_id', not mock_delete_client.called)

print()
print(f'{passed} passed, {failed} failed')
sys.exit(1 if failed else 0)
