# Minimal stub — only exists so `import webhooks` / `import app` don't fail
# at import time for reasons unrelated to the pure functions under test
# (psycopg2 isn't installable in this sandbox — no network access). Nothing
# in this stub is exercised directly; test_webhook_routing.py patches these
# per-test with unittest.mock so the REAL handle_shopify_webhook logic runs
# against controlled inputs instead of a real DB.
def get_db():
    raise NotImplementedError("stub — DB not available in this test sandbox")

def get_client_by_id(client_id):
    raise NotImplementedError("stub — patch this per-test")

def delete_client(client_id):
    raise NotImplementedError("stub — patch this per-test")

def upsert_abandoned_cart(*args, **kwargs):
    raise NotImplementedError("stub — patch this per-test")

def mark_cart_recovered(*args, **kwargs):
    raise NotImplementedError("stub — patch this per-test")

