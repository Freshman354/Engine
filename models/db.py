"""
models/db.py
------------
Connection pool, _PooledConn wrapper, get_db(), get_db_connection().
Every other models sub-module imports get_db from here — nothing else.
"""
import psycopg2
import psycopg2.extras
import bcrypt
import secrets
from datetime import datetime
import json
import uuid
import os

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL environment variable is not set. "
        "Add it to your Render/local .env before starting the server."
    )

# ── Connection pool ───────────────────────────────────────────────────
# Opens at most (maxconn) connections to Postgres. Every call to get_db()
# checks out one connection from the pool and every caller must return it
# via cursor.close() + conn.close() (which puts it back, not disconnects).
# psycopg2 v2.9+ made connection a C extension — its attributes are
# read-only and cannot be monkey-patched. We use a thin Python wrapper
# class (_PooledConn) so conn.close() can return the connection to the
# pool without touching the underlying C object's attributes.
import psycopg2.pool as _pool
import threading as _threading

_pool_lock = _threading.Lock()
_db_pool = None


def _get_pool():
    """Initialise the connection pool lazily (thread-safe)."""
    global _db_pool
    if _db_pool is None:
        with _pool_lock:
            if _db_pool is None:
                _db_pool = _pool.ThreadedConnectionPool(
                    minconn=0,  # 0 so the pool holds no connections during idle periods (e.g. overnight)
                    maxconn=int(os.environ.get('DB_POOL_MAX', 10)),
                    dsn=DATABASE_URL,
                )
    return _db_pool


class _PooledConn:
    """
    Wraps a psycopg2 connection checked out from the pool.
    Proxies every attribute to the real connection EXCEPT close(),
    which returns the connection to the pool instead of destroying it.
    This avoids monkey-patching conn.close, which psycopg2 v2.9+ forbids.
    All calling code is unchanged — conn.close() still works as expected.
    """
    __slots__ = ('_conn',)

    def __init__(self, conn):
        object.__setattr__(self, '_conn', conn)

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, '_conn'), name)

    def __setattr__(self, name, value):
        if name == '_conn':
            object.__setattr__(self, name, value)
        else:
            setattr(object.__getattribute__(self, '_conn'), name, value)

    def close(self):
        """Return this connection to the pool."""
        try:
            _get_pool().putconn(object.__getattribute__(self, '_conn'))
        except Exception:
            pass

    def cursor(self, *args, **kwargs):
        return object.__getattribute__(self, '_conn').cursor(*args, **kwargs)

    def commit(self):
        return object.__getattribute__(self, '_conn').commit()

    def rollback(self):
        return object.__getattribute__(self, '_conn').rollback()


def get_db():
    """Check out a *live* connection from the pool. Returns (_PooledConn, RealDictCursor).

    Render / Heroku Postgres closes idle connections after ~5 minutes.
    psycopg2's ThreadedConnectionPool doesn't detect this — it happily
    hands out a dead socket, which raises OperationalError on the first
    query, causing a 500 on the user's first request after an idle period.

    Fix: up to 3 attempts.  Each time, inspect the raw connection's state
    before handing it out.  If it looks stale, discard it (close=True
    destroys the socket and lets the pool open a fresh one) and retry.
    """
    pool = _get_pool()
    last_err = None
    for _attempt in range(3):
        raw = pool.getconn()
        # conn.closed is non-zero when psycopg2 has already marked the
        # socket as dead (e.g. after a previous unrecovered OperationalError).
        if raw.closed:
            try:
                pool.putconn(raw, close=True)
            except Exception:
                pass
            continue
        # STATUS_READY  = idle, ready for a new command (the happy path).
        # STATUS_BEGIN  = inside an open transaction — also usable.
        # Anything else (STATUS_IN_TRANSACTION_INERROR, STATUS_INTRANS_INERROR,
        # or the legacy integer 4) means the connection is in an aborted
        # transaction and must be rolled back before reuse.
        if raw.status not in (
            psycopg2.extensions.STATUS_READY,
            psycopg2.extensions.STATUS_BEGIN,
        ):
            try:
                raw.rollback()
            except Exception as e:
                last_err = e
                try:
                    pool.putconn(raw, close=True)
                except Exception:
                    pass
                continue
        raw.cursor_factory = psycopg2.extras.RealDictCursor
        conn = _PooledConn(raw)
        cursor = conn.cursor()
        return conn, cursor

    # All retry slots consumed — let the pool raise naturally so the
    # caller's except-block (or a 500 handler) sees the real error.
    if last_err:
        raise last_err
    raw = pool.getconn()          # may raise PoolError if exhausted
    raw.cursor_factory = psycopg2.extras.RealDictCursor
    conn = _PooledConn(raw)
    return conn, conn.cursor()


def get_db_connection():
    """Legacy alias — returns a _PooledConn with RealDictCursor factory set.
    Routes through get_db() so it inherits the same retry/SSL-recovery logic."""
    conn, cursor = get_db()
    cursor.close()
    return conn

