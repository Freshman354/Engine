"""
models/system_settings.py
---------------------------
Generic key-value store for admin-toggleable settings that need to take
effect live, without a redeploy — e.g. AI_PROVIDER (utils.py). Env vars
remain the deployment-time default; a row here overrides them at runtime.
"""
import logging

from .db import get_db

logger = logging.getLogger(__name__)


def get_setting(key: str, default=None):
    """Returns the stored value for key, or default if not set."""
    try:
        conn, cursor = get_db()
        cursor.execute('SELECT value FROM system_settings WHERE key = %s', (key,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        return row['value'] if row else default
    except Exception as e:
        logger.error(f'[SystemSettings] get_setting error key={key}: {e}')
        return default


def set_setting(key: str, value: str, updated_by: int = None) -> bool:
    """Upserts a setting."""
    try:
        conn, cursor = get_db()
        cursor.execute(
            """INSERT INTO system_settings (key, value, updated_at, updated_by)
               VALUES (%s, %s, CURRENT_TIMESTAMP, %s)
               ON CONFLICT (key) DO UPDATE
                   SET value = EXCLUDED.value,
                       updated_at = CURRENT_TIMESTAMP,
                       updated_by = EXCLUDED.updated_by""",
            (key, value, updated_by)
        )
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f'[SystemSettings] set {key}={value} by user={updated_by}')
        return True
    except Exception as e:
        logger.error(f'[SystemSettings] set_setting error key={key}: {e}')
        return False


def get_all_settings() -> list:
    """For the admin dashboard's System page — every setting and when it last changed."""
    try:
        conn, cursor = get_db()
        cursor.execute('SELECT key, value, updated_at, updated_by FROM system_settings ORDER BY key')
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return [{
            'key': r['key'], 'value': r['value'],
            'updated_at': r['updated_at'].isoformat() if r['updated_at'] else None,
            'updated_by': r['updated_by'],
        } for r in rows]
    except Exception as e:
        logger.error(f'[SystemSettings] get_all_settings error: {e}')
        return []
