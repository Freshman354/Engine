"""
Setup demo client - Works even with database locks
Run: python setup_demo.py
"""

import sqlite3
import json
import time
import sys

def get_db_connection():
    """Get database connection with proper settings"""
    conn = sqlite3.connect('chatbot.db', timeout=10.0)
    conn.row_factory = sqlite3.Row
    # Enable WAL mode to allow concurrent access
    conn.execute('PRAGMA journal_mode=WAL')
    return conn

def setup_demo():
    """Setup demo client with better error handling"""
    
    print("\n" + "="*70)
    print("DEMO CLIENT SETUP")
    print("="*70 + "\n")
    
    try:
        # Step 1: Find or create demo user
        print("Step 1: Setting up demo user...")
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if demo user exists
        cursor.execute('SELECT id FROM users WHERE email = ?', ('demo@example.com',))
        user = cursor.fetchone()
        
        if user:
            user_id = user['id']
            print(f"✅ Demo user found (ID: {user_id})")
        else:
            # Create demo user
            import bcrypt
            password_hash = bcrypt.hashpw('demo123'.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
            
            cursor.execute(
                'INSERT INTO users (email, password_hash, plan_type, created_at) VALUES (?, ?, ?, datetime("now"))',
                ('demo@example.com', password_hash, 'agency')
            )
            user_id = cursor.lastrowid
            conn.commit()
            print(f"✅ Demo user created (ID: {user_id})")
        
        conn.close()
        time.sleep(0.3)
        
        # Step 2: Create or update demo client
        print("\nStep 2: Setting up demo client...")
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if demo client exists
        cursor.execute('SELECT client_id FROM clients WHERE client_id = ?', ('demo',))
        existing_client = cursor.fetchone()
        
        # Prepare branding settings
        branding = {
            "branding": {
                "company_name": "Demo Company",
                "logo_url": "https://via.placeholder.com/150x50?text=Demo+Co",
                "primary_color": "#667eea",
                "secondary_color": "#764ba2",
                "bot_avatar": "https://via.placeholder.com/40?text=🤖",
                "bot_name": "Demo Assistant"
            },
            "contact": {
                "email": "hello@democompany.com",
                "phone": "+1-555-DEMO",
                "whatsapp": "+15550200",
                "business_hours": "Mon-Fri, 9 AM - 6 PM EST"
            },
            "bot_settings": {
                "bot_name": "Demo Assistant",
                "welcome_message": "👋 Hi! I'm Demo Assistant. Ask me anything!",
                "tone": "friendly",
                "fallback_message": "I'm not sure about that. Type 'contact' to speak with our team!",
                "lead_triggers": ["contact", "sales", "pricing", "demo", "agent", "human"]
            }
        }
        
        if existing_client:
            # Update existing
            cursor.execute(
                'UPDATE clients SET user_id = ?, company_name = ?, branding_settings = ? WHERE client_id = ?',
                (user_id, 'Demo Company', json.dumps(branding), 'demo')
            )
            print("✅ Demo client updated (client_id: demo)")
        else:
            # Create new
            cursor.execute(
                'INSERT INTO clients (client_id, user_id, company_name, branding_settings, created_at) VALUES (?, ?, ?, ?, datetime("now"))',
                ('demo', user_id, 'Demo Company', json.dumps(branding))
            )
            print("✅ Demo client created (client_id: demo)")
        
        conn.commit()
        conn.close()
        time.sleep(0.3)
        
        # Step 3: Add basic FAQs
        print("\nStep 3: Adding demo FAQs...")
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Delete old FAQs first
        cursor.execute('DELETE FROM faqs WHERE client_id = ?', ('demo',))
        
        # Add new FAQs
        demo_faqs = [
            {
                "question": "What are your hours?",
                "answer": "We're open Monday-Friday, 9 AM - 6 PM EST! Weekend hours: Saturday 10 AM - 4 PM. 🕒",
                "triggers": ["hours", "time", "open", "available", "when", "schedule"]
            },
            {
                "question": "How much does it cost?",
                "answer": "Starter: $49/mo (5 clients) | Agency: $149/mo (15 clients) | Enterprise: Custom pricing (unlimited). All plans include unlimited conversations! 💰",
                "triggers": ["price", "pricing", "cost", "how much", "payment", "fee"]
            },
            {
                "question": "Is there a free trial?",
                "answer": "Yes! Start with our Free plan (1 client, 5 FAQs, 50 messages/day). No credit card required. Upgrade anytime! 🆓",
                "triggers": ["trial", "free", "test", "demo", "try"]
            },
            {
                "question": "Can I customize the chatbot?",
                "answer": "100% customizable! Change colors, logo, bot name, personality, and welcome message. Perfect for white-label agencies. 🎨",
                "triggers": ["customize", "customization", "branding", "white label", "appearance", "colors"]
            },
            {
                "question": "How do I install it?",
                "answer": "Super easy! Just add one line of code to your website. Works with WordPress, Shopify, Webflow, or any HTML site. Takes 2 minutes! 💻",
                "triggers": ["install", "setup", "embed", "add", "integrate", "how to use"]
            },
            {
                "question": "Does it work on mobile?",
                "answer": "Yes! The chatbot is fully responsive and works perfectly on smartphones, tablets, and desktops. 📱",
                "triggers": ["mobile", "phone", "smartphone", "tablet", "responsive"]
            },
            {
                "question": "Can it collect leads?",
                "answer": "Absolutely! Conversational lead capture collects name, email, phone, company, and custom fields. Leads saved to your dashboard. Export anytime! 📊",
                "triggers": ["lead", "leads", "collect", "capture", "contact info"]
            },
            {
                "question": "Do you have analytics?",
                "answer": "Yes! Track conversations, popular questions, conversion rates, lead sources, and user engagement in real-time. 📈",
                "triggers": ["analytics", "statistics", "stats", "reports", "tracking", "data"]
            }
        ]
        
        for i, faq in enumerate(demo_faqs):
            cursor.execute(
                'INSERT INTO faqs (client_id, faq_id, question, answer, triggers, created_at) VALUES (?, ?, ?, ?, ?, datetime("now"))',
                ('demo', f'demo_faq_{i+1}', faq['question'], faq['answer'], json.dumps(faq['triggers']))
            )
        
        conn.commit()
        conn.close()
        print(f"✅ Added {len(demo_faqs)} demo FAQs")
        
        # Success!
        print("\n" + "="*70)
        print("✅ DEMO SETUP COMPLETE!")
        print("="*70)
        
        print("\n📊 What was created:")
        print("  • Demo User: demo@example.com (password: demo123)")
        print("  • Demo Client: client_id='demo'")
        print(f"  • Demo FAQs: {len(demo_faqs)} FAQs")
        
        print("\n🚀 Next Steps:")
        print("  1. Run: python app.py")
        print("  2. Visit: http://127.0.0.1:5000/widget?client_id=demo")
        print("  3. Test questions:")
        print("     - 'What are your hours?'")
        print("     - 'How much does it cost?'")
        print("     - 'Is there a free trial?'")
        print("     - 'Can I customize it?'")
        
        print("\n💡 Want more FAQs? Run: python add_demo_faqs.py")
        
        return True
        
    except sqlite3.OperationalError as e:
        if "locked" in str(e):
            print("\n❌ ERROR: Database is locked!")
            print("\n🔧 Solutions:")
            print("  1. Stop the Flask server (Ctrl+C)")
            print("  2. Close any database viewers in VS Code")
            print("  3. Try again")
            print("\nOR")
            print("  1. Close VS Code completely")
            print("  2. Reopen VS Code")
            print("  3. Run this script again")
        else:
            print(f"\n❌ Database error: {e}")
        return False
        
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    success = setup_demo()
    sys.exit(0 if success else 1)