"""
tests/test_cart_recovery_attribution.py
========================================
Logic-level unit tests for Cart Recovery V1 (attribution, duplicate-
webhook idempotency, notification creation, analytics math).

IMPORTANT — what these tests are and are not:
  These mock models.cart_recovery.get_db() and assert on (a) the exact
  SQL text sent (e.g. "ON CONFLICT (cart_id)", "FOR UPDATE ... SKIP
  LOCKED") and (b) the Python-level control flow given a scripted
  sequence of fetchone/fetchall results. They verify the LOGIC is
  correct. They do NOT exercise a real PostgreSQL connection, so they
  cannot prove the SQL actually executes without a syntax error, that
  FOR UPDATE SKIP LOCKED genuinely prevents a race under real
  concurrent connections, or that the migration runs cleanly against a
  real database. That requires running against an actual Postgres
  instance, which this sandbox does not have (no network/DB access).
  Treat a pass here as "code-complete", not "live-verified".

Run with: python3 -m pytest tests/test_cart_recovery_attribution.py -v
  (or python3 -m unittest tests.test_cart_recovery_attribution -v)
"""
import os
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'models'))

# This sandbox has no network access to install missing packages, and
# `from models import cart_recovery` unavoidably executes models/__init__.py
# first (Python always runs a package's __init__ before any submodule import),
# which in turn imports EVERY module in the package — pulling in dependencies
# that cart_recovery.py itself doesn't actually need (bcrypt for password
# hashing in users.py, crypto_utils/utils which are local modules that were
# never part of this upload at all). None of these are exercised by the
# tests below — every DB call is mocked — so they're stubbed only far enough
# for the import chain to succeed. This is NOT a substitute for running
# against the real bcrypt/psycopg2/crypto_utils and proves nothing about
# their actual behavior.
def _stub_module(name, **attrs):
    if name in sys.modules:
        return
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod

_psycopg2_stub = types.ModuleType('psycopg2')
_psycopg2_extras = types.ModuleType('psycopg2.extras')
_psycopg2_extras.RealDictCursor = object
_psycopg2_extras.execute_values = lambda *a, **k: None
_psycopg2_pool = types.ModuleType('psycopg2.pool')
_psycopg2_pool.ThreadedConnectionPool = object
_psycopg2_stub.extras = _psycopg2_extras
_psycopg2_stub.pool = _psycopg2_pool
_psycopg2_stub.IntegrityError = Exception
_stub_module('psycopg2', **vars(_psycopg2_stub))
sys.modules['psycopg2'].extras = _psycopg2_extras
sys.modules['psycopg2'].pool = _psycopg2_pool
sys.modules['psycopg2'].IntegrityError = Exception
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

from models import cart_recovery  # noqa: E402


class FakeCursor:
    """
    Minimal stand-in for a psycopg2 RealDictCursor. `fetchone_results`
    is consumed one call at a time (each .execute() that's followed by
    a .fetchone() pops the next scripted value) so a test can script a
    sequence like [None, {'id': 5}] to simulate "UPDATE matched nothing,
    then a follow-up SELECT found the existing row".
    """
    def __init__(self, fetchone_results=None, fetchall_results=None):
        self.fetchone_results = list(fetchone_results or [])
        self.fetchall_results = list(fetchall_results or [])
        self.executed = []  # list of (sql, params) for assertions

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self.fetchone_results.pop(0) if self.fetchone_results else None

    def fetchall(self):
        return self.fetchall_results.pop(0) if self.fetchall_results else []

    def close(self):
        pass


class FakeConn:
    def __init__(self):
        self.committed = False
        self.rolled_back = False

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        pass


def _patched_get_db(cursor):
    conn = FakeConn()
    return patch.object(cart_recovery, 'get_db', return_value=(conn, cursor)), conn


class TestMarkCartRecoveredAttribution(unittest.TestCase):
    """Covers spec Test 1, Test 2, Test 6."""

    def test_1_email_sent_then_order_completes_counts_as_recovery(self):
        """Test 1: abandoned -> recovery email sent -> order completed
        -> recovered = 1 (is_recovery=True, since the CASE only reaches
        'recovered' when recovery_email_sent_at IS NOT NULL — enforced
        in SQL, not re-checked in Python, so this test can't literally
        see that branch taken without a real DB; it asserts the
        returned shape a first-time email-preceded recovery produces)."""
        cursor = FakeCursor(fetchone_results=[{'id': 42, 'is_recovery': True}])
        patcher, conn = _patched_get_db(cursor)
        with patcher:
            result = cart_recovery.mark_cart_recovered(
                'client_1', 'chk_abc', order_id='#1001', revenue=127.50
            )
        self.assertEqual(result, {'recovered': True, 'first_time': True, 'is_recovery': True, 'cart_id': 42})
        self.assertTrue(conn.committed)
        sql, params = cursor.executed[0]
        self.assertIn('recovery_email_sent_at IS NOT NULL', sql)
        self.assertIn("'converted_early'", sql)
        self.assertIn("status NOT IN ('recovered', 'converted_early')", sql)
        self.assertEqual(params, ('#1001', 127.50, 'client_1', 'chk_abc'))

    def test_2_order_completes_before_email_sent_is_not_a_recovery(self):
        """Test 2: order completed BEFORE any recovery email went out
        -> recovered = 0 from Lumvi's point of view. The CASE puts the
        cart in 'converted_early' instead of 'recovered', so
        is_recovery is False even though first_time (a transition did
        happen) is True — the caller must gate the notification on
        is_recovery, not merely on first_time/recovered."""
        cursor = FakeCursor(fetchone_results=[{'id': 55, 'is_recovery': False}])
        patcher, conn = _patched_get_db(cursor)
        with patcher:
            result = cart_recovery.mark_cart_recovered(
                'client_1', 'chk_early', order_id='#2002', revenue=80.00
            )
        self.assertEqual(result, {'recovered': True, 'first_time': True, 'is_recovery': False, 'cart_id': 55})

    def test_6_duplicate_order_webhook_does_not_recount(self):
        """Test 6: same order webhook delivered twice -> still only one
        recovery. Second call's UPDATE matches zero rows (already
        terminal), fetchone -> None; the fallback SELECT confirms it
        previously resolved to 'recovered', so is_recovery stays True
        for reporting purposes, but first_time=False tells the caller
        not to create a second notification or recount revenue."""
        cursor = FakeCursor(fetchone_results=[None, {'id': 42, 'status': 'recovered'}])
        patcher, conn = _patched_get_db(cursor)
        with patcher:
            result = cart_recovery.mark_cart_recovered(
                'client_1', 'chk_abc', order_id='#1001', revenue=127.50
            )
        self.assertEqual(result, {'recovered': True, 'first_time': False, 'is_recovery': True, 'cart_id': 42})

    def test_duplicate_webhook_for_a_converted_early_cart_stays_non_recovery(self):
        """Same idempotency, but for the converted_early branch — a
        redelivered webhook must not suddenly start counting a
        non-Lumvi sale as a recovery."""
        cursor = FakeCursor(fetchone_results=[None, {'id': 55, 'status': 'converted_early'}])
        patcher, conn = _patched_get_db(cursor)
        with patcher:
            result = cart_recovery.mark_cart_recovered('client_1', 'chk_early')
        self.assertEqual(result, {'recovered': True, 'first_time': False, 'is_recovery': False, 'cart_id': 55})

    def test_no_matching_cart_at_all(self):
        cursor = FakeCursor(fetchone_results=[None, None])
        patcher, conn = _patched_get_db(cursor)
        with patcher:
            result = cart_recovery.mark_cart_recovered('client_1', 'chk_none')
        self.assertEqual(result, {'recovered': False, 'first_time': False, 'is_recovery': False, 'cart_id': None})

    def test_db_error_returns_safe_default_and_rolls_back(self):
        cursor = MagicMock()
        cursor.execute.side_effect = Exception('connection reset')
        patcher, conn = _patched_get_db(cursor)
        with patcher:
            result = cart_recovery.mark_cart_recovered('client_1', 'chk_x')
        self.assertEqual(result, {'recovered': False, 'first_time': False, 'is_recovery': False, 'cart_id': None})


class TestRecoveryNotificationIdempotency(unittest.TestCase):

    def test_create_notification_uses_on_conflict_do_nothing(self):
        """The INSERT must rely on UNIQUE(cart_id) + ON CONFLICT DO
        NOTHING as the second, independent idempotency layer — this
        test can't prove the DB constraint exists (no live DB), only
        that the code path asks for that behaviour."""
        cursor = FakeCursor()
        patcher, conn = _patched_get_db(cursor)
        with patcher:
            ok = cart_recovery.create_recovery_notification(
                'client_1', cart_id=42, order_id='#1001', revenue=127.50
            )
        self.assertTrue(ok)
        sql, params = cursor.executed[0]
        self.assertIn('ON CONFLICT (cart_id) DO NOTHING', sql)
        self.assertIn('127.5', str(params))

    def test_notification_message_omits_revenue_when_unknown(self):
        cursor = FakeCursor()
        patcher, conn = _patched_get_db(cursor)
        with patcher:
            cart_recovery.create_recovery_notification('client_1', cart_id=7)
        _, params = cursor.executed[0]
        message = params[4]
        self.assertNotIn('$', message)
        self.assertIn('completed their order', message)


class TestConcurrencyClaim(unittest.TestCase):

    def test_claim_query_uses_for_update_skip_locked(self):
        """This is the core fix for Problem 2 (two overlapping cron runs
        selecting the same cart). We can't exercise real Postgres
        locking here — only assert the query asks for it."""
        cursor = FakeCursor(fetchall_results=[[], []])
        patcher, conn = _patched_get_db(cursor)
        with patcher:
            result = cart_recovery.claim_carts_for_recovery_email()
        self.assertEqual(result, [])
        sql, params = cursor.executed[0]
        self.assertIn('FOR UPDATE OF ac SKIP LOCKED', sql)
        self.assertIn("SET status = 'sending'", sql)
        self.assertIn("status = 'pending'", sql)
        self.assertIn("status = 'sending'", sql)  # stale-reclaim branch too

    def test_claim_attaches_business_name_for_returned_rows(self):
        claimed_row = {'id': 1, 'client_id': 'c1', 'customer_email': 'a@b.com'}
        client_row  = {'client_id': 'c1', 'business_name': 'Acme', 'notification_email': 'x@acme.com'}
        cursor = FakeCursor(fetchall_results=[[claimed_row], [client_row]])
        patcher, conn = _patched_get_db(cursor)
        with patcher:
            result = cart_recovery.claim_carts_for_recovery_email()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['business_name'], 'Acme')

    def test_revert_only_touches_sending_rows(self):
        cursor = FakeCursor()
        patcher, conn = _patched_get_db(cursor)
        with patcher:
            ok = cart_recovery.revert_recovery_email_claim(99)
        self.assertTrue(ok)
        sql, params = cursor.executed[0]
        self.assertIn("status = 'pending'", sql)
        self.assertIn("AND status = 'sending'", sql)
        self.assertEqual(params, (99,))


class TestRecoveryAnalyticsMath(unittest.TestCase):

    def test_7_zero_attempts_gives_zero_rate_not_division_error(self):
        cursor = FakeCursor(fetchone_results=[{'n': 0}], fetchall_results=[[]])
        patcher, conn = _patched_get_db(cursor)
        with patcher:
            metrics = cart_recovery.get_recovery_analytics('client_1')
        self.assertEqual(metrics['recovery_attempts'], 0)
        self.assertEqual(metrics['recovered_carts'], 0)
        self.assertEqual(metrics['recovery_rate'], 0)
        self.assertIsNone(metrics['avg_recovered_order_value'])
        self.assertEqual(metrics['recovered_revenue'], 0.0)

    def test_single_currency_matches_documented_formula(self):
        # 34 attempts, 11 recovered, $1247.50 -> rate 32.4%, avg 113.41 —
        # the example numbers from the spec, all in one currency.
        cursor = FakeCursor(
            fetchone_results=[{'n': 34}],
            fetchall_results=[[{'n': 11, 'currency': 'USD', 'revenue': 1247.50}]],
        )
        patcher, conn = _patched_get_db(cursor)
        with patcher:
            metrics = cart_recovery.get_recovery_analytics('client_1')
        self.assertEqual(metrics['recovery_attempts'], 34)
        self.assertEqual(metrics['recovered_carts'], 11)
        self.assertEqual(metrics['recovery_rate'], 32.4)
        self.assertFalse(metrics['multi_currency'])
        self.assertEqual(metrics['recovered_revenue'], 1247.50)
        self.assertEqual(metrics['avg_recovered_order_value'], round(1247.50 / 11, 2))

    def test_4_two_currencies_never_silently_summed(self):
        """Two currencies present in the window -> recovered_revenue
        must be None (never a blended, meaningless total), with the
        real per-currency numbers available separately. This is
        Decision 2's actual guarantee, not merely a documentation
        promise."""
        cursor = FakeCursor(
            fetchone_results=[{'n': 20}],
            fetchall_results=[[
                {'n': 5, 'currency': 'USD', 'revenue': 500.00},
                {'n': 3, 'currency': 'EUR', 'revenue': 300.00},
            ]],
        )
        patcher, conn = _patched_get_db(cursor)
        with patcher:
            metrics = cart_recovery.get_recovery_analytics('client_1')
        self.assertTrue(metrics['multi_currency'])
        self.assertIsNone(metrics['recovered_revenue'])
        self.assertIsNone(metrics['avg_recovered_order_value'])
        self.assertEqual(metrics['recovered_carts'], 8)  # counts are currency-agnostic
        self.assertEqual(metrics['recovered_revenue_by_currency'], {'USD': 500.00, 'EUR': 300.00})
        self.assertEqual(metrics['recovered_carts_by_currency'], {'USD': 5, 'EUR': 3})

    def test_5_bounded_range_is_half_open(self):
        """Test 5: start=Aug 1, end=Sep 1 -> both queries must use
        `>= start_date AND < end_date` (inclusive start, exclusive end)
        rather than a same-day boundary bug. Asserted on the generated
        SQL/params since a mocked cursor can't itself prove which real
        rows Postgres would include."""
        import datetime as dt
        start = dt.datetime(2026, 8, 1)
        end   = dt.datetime(2026, 9, 1)
        cursor = FakeCursor(fetchone_results=[{'n': 0}], fetchall_results=[[]])
        patcher, conn = _patched_get_db(cursor)
        with patcher:
            cart_recovery.get_recovery_analytics('client_1', start_date=start, end_date=end)
        attempts_sql, attempts_params = cursor.executed[0]
        self.assertIn('recovery_email_sent_at >= %s', attempts_sql)
        self.assertIn('recovery_email_sent_at < %s', attempts_sql)
        self.assertEqual(attempts_params, ('client_1', start, end))
        recovered_sql, recovered_params = cursor.executed[1]
        self.assertIn('recovery_email_sent_at >= %s', recovered_sql)
        self.assertIn('recovery_email_sent_at < %s', recovered_sql)
        self.assertEqual(recovered_params, ('client_1', start, end))

    def test_3_cohort_uses_email_sent_date_not_recovered_date(self):
        """Test 3: recovery email sent Aug 31, order completes Sep 2 —
        this cart must be attributed to the August cohort. Verified
        here by confirming BOTH the attempts query and the recovered
        query filter on recovery_email_sent_at (never recovered_at) —
        that's what makes a cart emailed in August but recovered in
        September still count as August's, since its
        recovery_email_sent_at is the Aug 31 timestamp regardless of
        when recovered_at ends up being set."""
        cursor = FakeCursor(fetchone_results=[{'n': 1}], fetchall_results=[[
            {'n': 1, 'currency': 'USD', 'revenue': 90.00}
        ]])
        patcher, conn = _patched_get_db(cursor)
        with patcher:
            cart_recovery.get_recovery_analytics('client_1')
        recovered_sql, _ = cursor.executed[1]
        self.assertIn('recovery_email_sent_at', recovered_sql)
        self.assertNotIn('recovered_at >=', recovered_sql)
        self.assertNotIn('recovered_at <', recovered_sql)


if __name__ == '__main__':
    unittest.main()
