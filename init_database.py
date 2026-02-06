"""
Initialize database with demo data
Run this once: python init_database.py
"""

import models
import json

def init_demo_data():
    """Create demo user and client"""
    
    print("Initializing database...")
    models.init_db()
    
    # Create demo user
    print("Creating demo user...")
    user_id = models.create_user('demo@example.com', 'password123', 'agency')
    
    if user_id:
        print(f"✅ Demo user created (ID: {user_id})")
        print("   Email: demo@example.com")
        print("   Password: password123")
        
        # Create demo client
        print("\nCreating demo client...")
        
        branding_settings = {
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
                "welcome_message": "👋 Hi! I'm Demo Assistant. Try asking:\n\n• What are your hours?\n• How much does it cost?\n• Can I get a demo?",
                "tone": "friendly",
                "fallback_message": "I'm not sure about that. Type 'contact' to speak with our team!",
                "lead_triggers": ["contact", "sales", "pricing", "demo", "agent", "human"]
            }
        }
        
        client_id = models.create_client(user_id, "Demo Company", branding_settings)
        print(f"✅ Demo client created (ID: {client_id})")
        
        # Add demo FAQs
        print("\nAdding demo FAQs...")
        
        demo_faqs = [
            {
                "id": "demo_1",
                "question": "What are your hours?",
                "answer": "We're here Monday-Friday, 9 AM - 6 PM EST! 🕒",
                "triggers": ["hours", "time", "open", "available", "when"]
            },
            {
                "id": "demo_2",
                "question": "What are your prices?",
                "answer": "Starter: $49/mo | Agency: $149/mo | Enterprise: Custom pricing. Want details? Type 'contact'!",
                "triggers": ["price", "pricing", "cost", "how much", "payment"]
            },
            {
                "id": "demo_3",
                "question": "Can I try a demo?",
                "answer": "You're in one! 🎉 Want to see it for YOUR business? Type 'contact' and we'll set you up!",
                "triggers": ["demo", "trial", "test", "try"]
            }
        ]
        
        models.save_faqs(client_id, demo_faqs)
        print(f"✅ {len(demo_faqs)} FAQs added")
        
        print("\n" + "="*50)
        print("✅ DATABASE INITIALIZED SUCCESSFULLY!")
        print("="*50)
        print("\nYou can now:")
        print("1. Run: python app.py")
        print("2. Visit: http://127.0.0.1:5000/login")
        print("3. Login with:")
        print("   Email: demo@example.com")
        print("   Password: password123")
        print("\n" + "="*50)
        
    else:
        print("❌ User already exists or creation failed")
        print("Database may already be initialized.")

if __name__ == '__main__':
    init_demo_data()