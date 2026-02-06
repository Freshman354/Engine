import models

email = input("Enter user email: ").strip()
user = models.get_user_by_email(email)

if not user:
    print(f"❌ User not found: {email}")
else:
    clients = models.get_user_clients(user['id'])
    
    print(f"\n📧 User: {email}")
    print(f"📊 Plan: {user['plan_type']}")
    print(f"👥 Clients: {len(clients)}\n")
    
    if clients:
        print("-"*60)
        for client in clients:
            print(f"  • {client['company_name']} (ID: {client['client_id']})")
        print("-"*60)
    else:
        print("  No clients yet")
    print()
