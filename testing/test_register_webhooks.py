"""
Tests ShopifyAdapter.register_webhooks() — specifically the retry/backoff
logic added for the W1 fix (a 429 on the first of 8 sequential topic
registrations was cascading through the rest with zero spacing).

Real code under test, not a reimplementation: only requests.post and
time.sleep are mocked — the retry loop, the Retry-After header handling,
the exponential-backoff fallback, and the max-retries cutoff are all the
actual commerce_adapters.py code running against controlled responses.

Run with: python3 test_register_webhooks.py
"""
import os
import sys
import json
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.dirname(__file__))

os.environ.setdefault('INTEGRATION_ENCRYPTION_KEY',
                       __import__('cryptography.fernet', fromlist=['Fernet']).Fernet.generate_key().decode())

import commerce_adapters

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


def make_response(status_code, json_body=None, headers=None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_body or {}
    resp.headers = headers or {}
    return resp


adapter = commerce_adapters.ShopifyAdapter({
    'shop_domain': 'test-store.myshopify.com',
    'access_token': 'shpat_faketoken',
})

success_response = make_response(200, {
    'data': {'webhookSubscriptionCreate': {'webhookSubscription': {'id': 'gid://1'}, 'userErrors': []}}
})
already_exists_response = make_response(200, {
    'data': {'webhookSubscriptionCreate': {'webhookSubscription': None,
             'userErrors': [{'field': ['topic'], 'message': 'Address for this topic has already been taken'}]}}
})
real_error_response = make_response(200, {
    'data': {'webhookSubscriptionCreate': {'webhookSubscription': None,
             'userErrors': [{'field': ['address'], 'message': 'is not a valid URL'}]}}
})
rate_limited_response = make_response(429, headers={'Retry-After': '0.01'})


print('register_webhooks — happy path')
with patch('requests.post', return_value=success_response) as mock_post:
    results = adapter.register_webhooks('https://app.lumvi.net/webhooks/shopify/client_abc')
    check('all 8 topics register successfully on a clean 200 response',
          len(results) == 8 and all(results.values()))
    check('one HTTP call per topic on the happy path (no unnecessary retries)',
          mock_post.call_count == 8)

print()
print('register_webhooks — W1 FIX: 429 retry/backoff')
with patch('requests.post',
           side_effect=[rate_limited_response, success_response] + [success_response] * 20) as mock_post, \
     patch('commerce_adapters.time.sleep') as mock_sleep:
    results = adapter.register_webhooks('https://app.lumvi.net/webhooks/shopify/client_abc')
    check('a 429 on the first attempt is retried and eventually succeeds, not treated as a hard failure',
          all(results.values()))
    check('the retry actually waits (time.sleep called) rather than hammering Shopify immediately',
          mock_sleep.called)
    check("the wait duration honors Shopify's own Retry-After header (0.01s) rather than guessing",
          mock_sleep.call_args_list[0][0][0] == 0.01)

print()
print('register_webhooks — 429 exhausting all retries fails that topic without crashing')
with patch('requests.post', return_value=rate_limited_response), \
     patch('commerce_adapters.time.sleep'):
    results = adapter.register_webhooks('https://app.lumvi.net/webhooks/shopify/client_abc')
    check('a topic that stays rate-limited through every retry is marked False, not left unresolved',
          all(v is False for v in results.values()))
    check('the whole call still returns a complete dict for all 8 topics, even when every one fails',
          len(results) == 8)

print()
print('register_webhooks — one topic rate-limited does not block or fail OTHER topics')
call_sequence = []
def side_effect(*args, **kwargs):
    topic = kwargs['json']['variables']['topic']
    call_sequence.append(topic)
    if topic == 'ORDERS_CREATE' and call_sequence.count('ORDERS_CREATE') == 1:
        return rate_limited_response
    return success_response

with patch('requests.post', side_effect=side_effect), \
     patch('commerce_adapters.time.sleep'):
    results = adapter.register_webhooks('https://app.lumvi.net/webhooks/shopify/client_abc')
    check('a rate limit on ONE topic does not prevent the other 7 from registering successfully '
          '— this is the actual W1 scenario: a cascade failure across all 8 from one 429',
          results.get('ORDERS_CREATE') is True and all(results.values()))

print()
print('register_webhooks — "already subscribed" is treated as success, not a failure to retry')
with patch('requests.post', return_value=already_exists_response) as mock_post:
    results = adapter.register_webhooks('https://app.lumvi.net/webhooks/shopify/client_abc')
    check('an already-exists userError counts as success (reinstall / already covered by '
          'shopify.app.toml managed webhooks)', all(results.values()))
    check('no retries attempted for an already-exists response — this is not treated like a 429',
          mock_post.call_count == 8)

print()
print('register_webhooks — a genuine userError (not "already exists") fails cleanly, no retry loop')
with patch('requests.post', return_value=real_error_response) as mock_post:
    results = adapter.register_webhooks('https://app.lumvi.net/webhooks/shopify/client_abc')
    check('a real validation error is marked False', all(v is False for v in results.values()))
    check('a non-rate-limit error is not retried (retrying a malformed request wastes calls '
          'without fixing anything)', mock_post.call_count == 8)

print()
print(f'{passed} passed, {failed} failed')
sys.exit(1 if failed else 0)
