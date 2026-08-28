"""
tests/test_google_signup_welcome_email.py
==========================================
Regression tests for: "new Google user -> welcome email triggered exactly
once" and "existing Google user -> welcome email not triggered again."

SCOPE — what these tests cover and why:
  blueprints/auth.py's google_callback() gates the welcome-email send on
  a single boolean: `is_new`, returned by
  models.create_or_link_google_user(). That function's three branches
  (existing by google_id / existing by email / brand-new) are exactly
  where "is this a new account?" gets decided — get that boolean wrong
  and the email either never fires for a genuine new user, or fires
  again for a returning one. These tests exercise that decision logic
  directly with a mocked DB (no real Postgres in this sandbox).

  They do NOT exercise google_callback() itself as a live Flask route
  (that needs a real app context, Authlib OAuth client, and session
  handling this sandbox can't stand up), so they can't prove the
  route wiring stays correct — only that the function it depends on for
  this decision returns the right answer for each of the three cases.
  Treat a pass here as "the is_new logic is correct", not "a live Google
  signup was tested".

Run with: python3 -m unittest tests.test_google_signup_welcome_email -v
"""
import os
import sys
import types
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'models'))

# Same stubbing rationale as test_cart_recovery_attribution.py — importing
# models.users unavoidably runs models/__init__.py first, pulling in every
# module in the package. None of these are exercised by the tests below
# (get_db is fully mocked), so they're stubbed only far enough for the
# import chain to succeed.
def _stub_module(name, **attrs):
    if name in sys.modules:
        return
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod

_psycopg2_extras = types.ModuleType('psycopg2.extras')
_psycopg2_extras.RealDictCursor = object
_psycopg2_extras.execute_values = lambda *a, **k: None
_psycopg2_pool = types.ModuleType('psycopg2.pool')
_psycopg2_pool.ThreadedConnectionPool = object
_stub_module('psycopg2', extras=_psycopg2_extras, pool=_psycopg2_pool, IntegrityError=Exception)
sys.modules['psycopg2'].extras = _psycopg2_extras
sys.modules['psycopg2'].pool = _psycopg2_pool
_stub_module('psycopg2.extras', RealDictCursor=object, execute_values=lambda *a, **k: None)
_stub_module('psycopg2.pool', ThreadedConnectionPool=object)

_stub_module(
    'bcrypt',
    hashpw=lambda pw, salt: b'stub-hash',
    gensalt=lambda: b'stub-salt',
    checkpw=lambda pw, h: False,
)
_stub_module(
    'crypto_utils',
    encrypt_credentials=lambda *a, **k: '',
    decrypt_credentials=lambda *a, **k: {},
)
_stub_module('utils', get_logger=lambda name: types.SimpleNamespace(
    info=lambda *a, **k: None, warning=lambda *a, **k: None,
    error=lambda *a, **k: None, debug=lambda *a, **k: None,
))

os.environ.setdefault('DATABASE_URL', 'postgresql://stub:stub@localhost:5432/stub')

from models import users  # noqa: E402


class FakeCursor:
    def __init__(self, fetchone_results=None):
        self.fetchone_results = list(fetchone_results or [])
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self.fetchone_results.pop(0) if self.fetchone_results else None

    def close(self):
        pass


class FakeConn:
    def __init__(self):
        self.committed = False

    def commit(self):
        self.committed = True

    def rollback(self):
        pass

    def close(self):
        pass


def _patched_get_db(cursor):
    conn = FakeConn()
    return patch.object(users, 'get_db', return_value=(conn, cursor)), conn


class TestGoogleSignupIsNew(unittest.TestCase):
    """
    is_new is the single gate google_callback() checks before calling
    _send_welcome_email(). Get this right and the welcome-email trigger
    condition is correct by construction; these tests exist to pin that
    down and catch a future regression in the branch logic itself.
    """

    def test_brand_new_google_user_is_new_true(self):
        """Neither a google_id match nor an email match exists -> a row
        is actually INSERTed and is_new=True. This is the case that
        must trigger the welcome email exactly once."""
        cursor = FakeCursor(fetchone_results=[
            None,                                          # no match by google_id
            None,                                          # no match by email
            {'id': 1, 'email': 'new@example.com', 'google_id': 'g123'},  # INSERT ... RETURNING *
        ])
        patcher, conn = _patched_get_db(cursor)
        with patcher:
            user_data, is_new = users.create_or_link_google_user('g123', 'new@example.com', return_is_new=True)
        self.assertTrue(is_new)
        self.assertEqual(user_data['id'], 1)
        # Exactly one INSERT statement (the 3rd execute call) — not two,
        # which would risk a duplicate-row/duplicate-email situation.
        insert_calls = [sql for sql, _ in cursor.executed if 'INSERT INTO users' in sql]
        self.assertEqual(len(insert_calls), 1)

    def test_returning_google_user_matched_by_google_id_is_new_false(self):
        """The normal "log in with Google again" case — found directly
        by google_id on the first query. Must NOT re-trigger the
        welcome email."""
        cursor = FakeCursor(fetchone_results=[
            {'id': 1, 'email': 'existing@example.com', 'google_id': 'g123'},
        ])
        patcher, conn = _patched_get_db(cursor)
        with patcher:
            user_data, is_new = users.create_or_link_google_user('g123', 'existing@example.com', return_is_new=True)
        self.assertFalse(is_new)
        self.assertEqual(user_data['id'], 1)
        # No INSERT should have happened at all for a returning user.
        insert_calls = [sql for sql, _ in cursor.executed if 'INSERT INTO users' in sql]
        self.assertEqual(len(insert_calls), 0)

    def test_existing_email_password_account_linking_google_is_new_false(self):
        """A user who originally signed up via email/password, now using
        "Sign in with Google" for the first time with the same email —
        no google_id match, but an email match exists. The account gets
        linked (google_id set), but this is NOT a new account and must
        NOT trigger a second welcome email (they already got one at
        their original signup)."""
        cursor = FakeCursor(fetchone_results=[
            None,                                                              # no match by google_id
            {'id': 7, 'email': 'olduser@example.com', 'google_id': None},      # match by email
        ])
        patcher, conn = _patched_get_db(cursor)
        with patcher:
            user_data, is_new = users.create_or_link_google_user('g999', 'olduser@example.com', return_is_new=True)
        self.assertFalse(is_new)
        self.assertEqual(user_data['id'], 7)
        # Should have linked the google_id via UPDATE, not created a new row.
        update_calls = [sql for sql, _ in cursor.executed if 'UPDATE users SET google_id' in sql]
        insert_calls = [sql for sql, _ in cursor.executed if 'INSERT INTO users' in sql]
        self.assertEqual(len(update_calls), 1)
        self.assertEqual(len(insert_calls), 0)

    def test_db_error_on_insert_is_never_reported_as_new(self):
        """If the brand-new-user INSERT itself fails (e.g. a race with a
        concurrent signup), is_new must come back False, not True — a
        caller must never fire a welcome email for an account that
        doesn't actually exist."""
        cursor = FakeCursor(fetchone_results=[None, None])
        cursor.execute_calls = 0

        def failing_execute(sql, params=None):
            cursor.executed.append((sql, params))
            if 'INSERT INTO users' in sql:
                raise Exception('duplicate key value violates unique constraint')
        cursor.execute = failing_execute

        patcher, conn = _patched_get_db(cursor)
        with patcher:
            user_data, is_new = users.create_or_link_google_user('g555', 'race@example.com', return_is_new=True)
        self.assertIsNone(user_data)
        self.assertFalse(is_new)


if __name__ == '__main__':
    unittest.main()
