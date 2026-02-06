"""
Add comprehensive demo FAQs (30+ questions)
Run AFTER setup_demo.py: python add_more_faqs.py
"""

import sqlite3
import json
import time

def get_db_connection():
    """Get database connection with proper settings"""
    conn = sqlite3.connect('chatbot.db', timeout=10.0)
    conn.row_factory = sqlite3.Row
    # Enable WAL mode for concurrent access
    conn.execute('PRAGMA journal_mode=WAL')
    return conn

def add_comprehensive_faqs():
    """Add 30+ comprehensive demo FAQs"""
    
    print("\n" + "="*70)
    print("ADDING COMPREHENSIVE DEMO FAQs")
    print("="*70 + "\n")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if demo client exists
        cursor.execute('SELECT client_id FROM clients WHERE client_id = ?', ('demo',))
        if not cursor.fetchone():
            print("❌ Demo client not found!")
            print("Run 'python setup_demo.py' first to create the demo client.")
            conn.close()
            return False
        
        print("✅ Demo client found")
        print("\n📝 Adding comprehensive FAQs...\n")
        
        # Delete old FAQs first
        cursor.execute('DELETE FROM faqs WHERE client_id = ?', ('demo',))
        print("  → Cleared old FAQs")
        
        # Comprehensive FAQ list
        comprehensive_faqs = [
            # HOURS & AVAILABILITY (3)
            {
                "question": "What are your business hours?",
                "answer": "We're open Monday-Friday, 9 AM - 6 PM EST. Weekend hours: Saturday 10 AM - 4 PM. Closed Sundays. 🕐",
                "triggers": ["hours", "time", "open", "available", "when open", "schedule", "timing"]
            },
            {
                "question": "Are you open on holidays?",
                "answer": "We're closed on major holidays: New Year's, Easter, Independence Day, Thanksgiving, and Christmas. We're open other holidays with modified hours. 🎄",
                "triggers": ["holiday", "holidays", "christmas", "thanksgiving", "closed", "special days"]
            },
            {
                "question": "What's your response time?",
                "answer": "Email: 2-4 hours during business hours. Phone: answered immediately. Chat: instant during business hours, within 24 hours after hours. ⚡",
                "triggers": ["response time", "how fast", "how quick", "reply", "answer time"]
            },
            
            # PRICING & PLANS (5)
            {
                "question": "How much does it cost?",
                "answer": "Starter: $49/month (5 clients) | Agency: $149/month (15 clients) | Enterprise: Custom pricing (unlimited). All plans include unlimited conversations! 💰",
                "triggers": ["price", "pricing", "cost", "how much", "payment", "fee", "charge"]
            },
            {
                "question": "Do you offer discounts?",
                "answer": "Yes! Annual plans get 20% off. Students/nonprofits get 30% off. Agencies managing 50+ clients get custom enterprise pricing. 🎉",
                "triggers": ["discount", "sale", "promo", "coupon", "deal", "cheaper", "save money"]
            },
            {
                "question": "Is there a free trial?",
                "answer": "Absolutely! Start with our Free plan (1 client, 5 FAQs, 50 messages/day). No credit card required. Upgrade anytime! 🆓",
                "triggers": ["trial", "free", "test", "try", "demo", "sample"]
            },
            {
                "question": "What payment methods do you accept?",
                "answer": "We accept all major credit cards (Visa, Mastercard, Amex), debit cards, and bank transfers. Payments processed securely through Flutterwave. 💳",
                "triggers": ["payment method", "how to pay", "credit card", "debit card", "bank transfer"]
            },
            {
                "question": "Can I cancel anytime?",
                "answer": "Yes! No contracts or commitments. Cancel anytime from your dashboard. You'll have access until the end of your billing period. No cancellation fees. ✅",
                "triggers": ["cancel", "cancellation", "refund", "unsubscribe", "quit", "stop"]
            },
            
            # FEATURES & CAPABILITIES (6)
            {
                "question": "What features are included?",
                "answer": "All plans include: custom branding, lead collection, analytics, email notifications, unlimited FAQs (except free), embeddable widget, mobile responsive, and priority support! 🚀",
                "triggers": ["features", "what included", "capabilities", "what can it do", "functions"]
            },
            {
                "question": "Can I customize the chatbot appearance?",
                "answer": "100% customizable! Change colors, logo, bot name, personality, welcome message, and button styles. Perfect for white-label agencies. 🎨",
                "triggers": ["customize", "customization", "branding", "white label", "appearance", "design", "colors", "logo"]
            },
            {
                "question": "Does it work on mobile?",
                "answer": "Yes! The chatbot is fully responsive and works perfectly on smartphones, tablets, and desktops. Your customers can chat from any device. 📱",
                "triggers": ["mobile", "phone", "smartphone", "tablet", "responsive", "ios", "android"]
            },
            {
                "question": "Can it collect leads?",
                "answer": "Absolutely! Conversational lead capture collects name, email, phone, company, and custom fields. Leads automatically saved to your dashboard. Export anytime! 📊",
                "triggers": ["lead", "leads", "collect", "capture", "contact info", "email capture"]
            },
            {
                "question": "Do you have analytics?",
                "answer": "Yes! Track conversations, popular questions, conversion rates, lead sources, and user engagement. See what customers are asking in real-time. 📈",
                "triggers": ["analytics", "statistics", "stats", "reports", "tracking", "data", "metrics"]
            },
            {
                "question": "Can I integrate with other tools?",
                "answer": "Yes! We integrate with Zapier, webhooks, email marketing tools, CRMs, and more. API access available on Agency and Enterprise plans. 🔌",
                "triggers": ["integrate", "integration", "api", "webhook", "zapier", "connect", "third party"]
            },
            
            # SETUP & TECHNICAL (5)
            {
                "question": "How do I install it on my website?",
                "answer": "Super easy! Just add one line of code to your website. Works with WordPress, Shopify, Webflow, Wix, or any HTML site. Takes 2 minutes. Copy code from your dashboard! 💻",
                "triggers": ["install", "setup", "embed", "add", "integrate", "how to use", "implement"]
            },
            {
                "question": "Do I need coding skills?",
                "answer": "Nope! Zero coding required. Our dashboard is point-and-click. Just add your FAQs, customize colors, and copy the embed code. That's it! 🎯",
                "triggers": ["coding", "code", "technical", "developer", "programming", "skills required"]
            },
            {
                "question": "How fast does it load?",
                "answer": "Lightning fast! The widget loads in under 1 second and doesn't slow down your website. We use CDN for global speed. ⚡",
                "triggers": ["speed", "fast", "load time", "performance", "slow"]
            },
            {
                "question": "Is my data secure?",
                "answer": "100% secure! SSL encryption, secure database, automatic backups, GDPR compliant, and SOC 2 certified. Your data is never shared. 🔒",
                "triggers": ["secure", "security", "safe", "privacy", "data protection", "gdpr", "encryption"]
            },
            {
                "question": "What if my website crashes?",
                "answer": "The chatbot is hosted separately, so even if your website has issues, the chatbot keeps working! 99.9% uptime guaranteed. 🛡️",
                "triggers": ["crash", "uptime", "downtime", "reliability", "availability"]
            },
            
            # SUPPORT & HELP (3)
            {
                "question": "How do I get support?",
                "answer": "Email: support@example.com (2-4 hour response). Live chat: During business hours. Knowledge base: 24/7 self-service. Phone support on Enterprise plans. 📞",
                "triggers": ["support", "help", "contact", "assistance", "customer service"]
            },
            {
                "question": "Do you offer training?",
                "answer": "Yes! Free video tutorials for all users. Live onboarding calls for Agency plans. Dedicated success manager for Enterprise. Plus extensive documentation. 📚",
                "triggers": ["training", "tutorial", "learn", "onboarding", "guide", "documentation"]
            },
            {
                "question": "Can you migrate my existing FAQs?",
                "answer": "Absolutely! Send us your existing FAQ list and we'll import it for free. Works from any format: Excel, Word, PDF, or website. 🔄",
                "triggers": ["migrate", "import", "transfer", "move", "existing", "old faqs"]
            },
            
            # USE CASES (4)
            {
                "question": "Who uses this?",
                "answer": "Marketing agencies, SaaS companies, e-commerce stores, real estate agents, consultants, healthcare providers, and service businesses. Perfect for anyone with customer questions! 🏢",
                "triggers": ["who uses", "use case", "customers", "industries", "examples"]
            },
            {
                "question": "Can I use it for multiple clients?",
                "answer": "Yes! That's our specialty. Perfect for agencies. Each client gets their own branded chatbot with separate analytics. Manage everything from one dashboard. 🎯",
                "triggers": ["multiple clients", "agency", "white label", "resell", "clients"]
            },
            {
                "question": "Does it work for e-commerce?",
                "answer": "Perfectly! Answer product questions, shipping info, return policies, sizing guides, and collect buyer information. Boost conversions by 40%! 🛍️",
                "triggers": ["ecommerce", "e-commerce", "online store", "shop", "products", "selling"]
            },
            {
                "question": "Can it schedule appointments?",
                "answer": "Yes! The chatbot can collect appointment requests and preferred times. Integrates with Calendly, Google Calendar, and other scheduling tools. 📅",
                "triggers": ["appointment", "schedule", "booking", "calendar", "meeting"]
            },
            
            # COMPARISONS (2)
            {
                "question": "How is this different from Intercom?",
                "answer": "We're 10x cheaper ($49 vs $500+/month), focus on FAQs, easier setup (2 min vs 2 hours), and perfect for agencies. No bloated features you don't need! ⚡",
                "triggers": ["intercom", "competitor", "different", "compare", "vs", "versus", "alternative"]
            },
            {
                "question": "Why not just use a contact form?",
                "answer": "Chatbots get 3x more responses! They're conversational, answer questions instantly, work 24/7, and feel more engaging. Forms are boring. 😴",
                "triggers": ["contact form", "form", "why chatbot", "benefits"]
            },
            
            # ADVANCED (3)
            {
                "question": "Can it understand natural language?",
                "answer": "Yes! Uses smart keyword matching and synonyms. If someone asks 'when are you available?' it understands they mean business hours. AI-powered! 🤖",
                "triggers": ["ai", "natural language", "understand", "smart", "intelligent"]
            },
            {
                "question": "What languages do you support?",
                "answer": "The chatbot can display FAQs in any language! English, Spanish, French, German, Chinese, etc. Just type your FAQs in the language you want. 🌍",
                "triggers": ["language", "languages", "spanish", "french", "international", "multilingual"]
            },
            {
                "question": "Can I export my data?",
                "answer": "Yes! Export leads as CSV, JSON, or Excel. Download conversation logs, analytics reports, and all your FAQs anytime. Your data, your control. 💾",
                "triggers": ["export", "download", "backup", "data export", "csv"]
            },
            
            # SPECIFIC SCENARIOS (3)
            {
                "question": "What if someone asks something not in the FAQs?",
                "answer": "The bot shows: 'I'm not sure about that. Would you like to speak with a human?' Then offers to collect their contact info. You get notified! 📧",
                "triggers": ["unknown question", "not found", "doesn't know", "no answer"]
            },
            {
                "question": "Can I update FAQs anytime?",
                "answer": "Absolutely! Add, edit, or delete FAQs instantly from your dashboard. Changes appear on your website in real-time. No code updates needed. ✏️",
                "triggers": ["update", "edit", "change", "modify", "add faq"]
            },
            {
                "question": "Does it work offline?",
                "answer": "The chatbot needs internet to work (it's cloud-based). But responses load instantly, and we have 99.9% uptime. Visitors will rarely notice any issues. ☁️",
                "triggers": ["offline", "internet", "connection", "network"]
            }
        ]
        
        # Insert all FAQs
        print(f"  → Adding {len(comprehensive_faqs)} FAQs...")
        
        for i, faq in enumerate(comprehensive_faqs, 1):
            cursor.execute(
                'INSERT INTO faqs (client_id, faq_id, question, answer, triggers, created_at) VALUES (?, ?, ?, ?, ?, datetime("now"))',
                ('demo', f'faq_{i}', faq['question'], faq['answer'], json.dumps(faq['triggers']))
            )
            
            # Progress indicator
            if i % 10 == 0:
                print(f"    ✓ Added {i} FAQs...")
        
        conn.commit()
        conn.close()
        
        print(f"\n✅ Successfully added {len(comprehensive_faqs)} FAQs!")
        
        # Show categories
        print("\n📊 FAQ Breakdown:")
        categories = {
            "Hours & Availability": 3,
            "Pricing & Plans": 5,
            "Features & Capabilities": 6,
            "Setup & Technical": 5,
            "Support & Help": 3,
            "Use Cases": 4,
            "Comparisons": 2,
            "Advanced Features": 3,
            "Specific Scenarios": 3
        }
        
        for category, count in categories.items():
            print(f"  • {category}: {count} FAQs")
        
        print("\n" + "="*70)
        print("✅ COMPREHENSIVE FAQs ADDED!")
        print("="*70)
        
        print("\n🎯 Next Steps:")
        print("  1. Run: python app.py")
        print("  2. Visit: http://127.0.0.1:5000/widget?client_id=demo")
        print("  3. Test questions:")
        print("     - 'What are your hours?'")
        print("     - 'How much does it cost?'")
        print("     - 'Do you offer discounts?'")
        print("     - 'Can I customize it?'")
        print("     - 'How is this different from Intercom?'")
        
        print("\n🚀 Your demo now has 34 comprehensive FAQs!")
        
        return True
        
    except sqlite3.OperationalError as e:
        if "locked" in str(e):
            print("\n❌ ERROR: Database is locked!")
            print("\n🔧 Solution: Stop the Flask server first (Ctrl+C), then run this script again.")
        else:
            print(f"\n❌ Database error: {e}")
        return False
        
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    import sys
    success = add_comprehensive_faqs()
    sys.exit(0 if success else 1)