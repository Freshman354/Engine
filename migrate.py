"""
Migration script - copies data from SQLite (chatbot.db) to PostgreSQL
Run this ONCE locally with: python migrate.py
"""

import sqlite3
import psycopg2
import psycopg2.extras
import json
import os
from dotenv import load_dotenv

load_dotenv()

SQLITE_DB = 'chatbot.db'
DATABASE_URL = os.environ.get('DATABASE_URL')

if not DATABASE_URL:
    print("❌ DATABASE_URL not found in .env file")
    exit(1)

print("🔌 Connecting to databases...")
sqlite_conn = sqlite3.connect(SQLITE_DB)
sqlite_conn.row_factory = sqlite3.Row
sqlite_cursor = sqlite_conn.cursor()

pg_conn = psycopg2.connect(DATABASE_URL)
pg_cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

print("✅ Connected to both databases\n")


def migrate_users():
    print("👤 Migrating users...")
    sqlite_cursor.execute('SELECT * FROM users')
    users = sqlite_cursor.fetchall()

    count = 0
    for user in users:
        try:
            pg_cursor.execute(
                '''INSERT INTO users (id, email, password_hash, created_at, plan_type)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (email) DO NOTHING''',
                (user['id'], user['email'], user['password_hash'],
                 user['created_at'], user['plan_type'])
            )
            count += 1
        except Exception as e:
            print(f"  ⚠️ Skipped user {user['email']}: {e}")

    print(f"  ✅ Migrated {count} users")


def migrate_clients():
    print("🏢 Migrating clients...")
    # before migrating rows make sure PostgreSQL has all columns
    try:
        from models import migrate_clients_table
        migrate_clients_table()
    except Exception as exc:
        print(f"  ⚠️ Could not run clients migration helper: {exc}")

    sqlite_cursor.execute('SELECT * FROM clients')
    clients = sqlite_cursor.fetchall()

    count = 0
    for client in clients:
        try:
            # derive new columns from the JSON if possible
            branding = {}
            try:
                branding = json.loads(client['branding_settings'] or '{}')
            except Exception:
                branding = {}
            primary = branding.get('branding', {}).get('primary_color')
            welcome = branding.get('bot_settings', {}).get('welcome_message')
            remove = bool(branding.get('branding', {}).get('remove_branding', False))

            pg_cursor.execute(
                '''INSERT INTO clients (id, user_id, client_id, company_name, branding_settings, widget_color, welcome_message, remove_branding, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (client_id) DO NOTHING''',
                (client['id'], client['user_id'], client['client_id'],
                 client['company_name'], client['branding_settings'],
                 primary, welcome, remove, client['created_at'])
            )
            count += 1
        except Exception as e:
            print(f"  ⚠️ Skipped client {client['client_id']}: {e}")

    print(f"  ✅ Migrated {count} clients")


def migrate_faqs():
    print("❓ Migrating FAQs...")
    sqlite_cursor.execute('SELECT * FROM faqs')
    faqs = sqlite_cursor.fetchall()

    count = 0
    for faq in faqs:
        try:
            pg_cursor.execute(
                '''INSERT INTO faqs (id, client_id, faq_id, question, answer, triggers, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT DO NOTHING''',
                (faq['id'], faq['client_id'], faq['faq_id'],
                 faq['question'], faq['answer'], faq['triggers'], faq['created_at'])
            )
            count += 1
        except Exception as e:
            print(f"  ⚠️ Skipped FAQ {faq['faq_id']}: {e}")

    print(f"  ✅ Migrated {count} FAQs")


def migrate_leads():
    print("📋 Migrating leads...")
    sqlite_cursor.execute('SELECT * FROM leads')
    leads = sqlite_cursor.fetchall()

    count = 0
    for lead in leads:
        try:
            pg_cursor.execute(
                '''INSERT INTO leads (id, client_id, name, email, phone, company, message, conversation_snippet, source_url, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT DO NOTHING''',
                (lead['id'], lead['client_id'], lead['name'], lead['email'],
                 lead['phone'], lead['company'], lead['message'],
                 lead['conversation_snippet'], lead['source_url'], lead['created_at'])
            )
            count += 1
        except Exception as e:
            print(f"  ⚠️ Skipped lead {lead['id']}: {e}")

    print(f"  ✅ Migrated {count} leads")


def migrate_affiliates():
    print("🤝 Migrating affiliates...")
    try:
        sqlite_cursor.execute('SELECT * FROM affiliates')
        affiliates = sqlite_cursor.fetchall()

        count = 0
        for affiliate in affiliates:
            try:
                pg_cursor.execute(
                    '''INSERT INTO affiliates (id, user_id, referral_code, commission_rate, total_earnings, total_referrals, payment_email, payment_method, bank_details, status, created_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (referral_code) DO NOTHING''',
                    (affiliate['id'], affiliate['user_id'], affiliate['referral_code'],
                     affiliate['commission_rate'], affiliate['total_earnings'],
                     affiliate['total_referrals'], affiliate['payment_email'],
                     affiliate['payment_method'], affiliate['bank_details'],
                     affiliate['status'], affiliate['created_at'])
                )
                count += 1
            except Exception as e:
                print(f"  ⚠️ Skipped affiliate {affiliate['id']}: {e}")

        print(f"  ✅ Migrated {count} affiliates")
    except Exception as e:
        print(f"  ⚠️ Affiliates table skipped: {e}")


# Fix sequences so new inserts don't conflict with migrated IDs
def fix_sequences():
    print("\n🔧 Fixing ID sequences...")
    tables = ['users', 'clients', 'faqs', 'leads', 'affiliates', 'referrals', 'commissions']
    for table in tables:
        try:
            pg_cursor.execute(f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), COALESCE(MAX(id), 1)) FROM {table}")
            print(f"  ✅ Fixed sequence for {table}")
        except Exception as e:
            print(f"  ⚠️ Skipped sequence for {table}: {e}")


# Run migration
try:
    migrate_users()
    migrate_clients()
    migrate_faqs()
    migrate_leads()
    migrate_affiliates()
    fix_sequences()

    pg_conn.commit()
    print("\n✅ Migration complete! All data copied to PostgreSQL.")

except Exception as e:
    pg_conn.rollback()
    print(f"\n❌ Migration failed: {e}")

finally:
    sqlite_cursor.close()
    sqlite_conn.close()
    pg_cursor.close()
    pg_conn.close()