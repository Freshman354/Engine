"""
Admin script to manually upgrade users to different plans
Usage: python upgrade_user.py
"""

import models

def upgrade_user():
    """Upgrade a user's plan"""
    print("\n" + "="*50)
    print("USER PLAN UPGRADE TOOL")
    print("="*50 + "\n")
    
    email = input("Enter user email: ").strip()
    
    user = models.get_user_by_email(email)
    
    if not user:
        print(f"❌ User not found: {email}")
        return
    
    print(f"\n📧 User found: {email}")
    print(f"📊 Current plan: {user['plan_type']}")
    
    print("\n Available plans:")
    print("  1. free")
    print("  2. starter")
    print("  3. agency")
    print("  4. enterprise")
    
    new_plan = input("\nEnter new plan (or press Enter to cancel): ").strip().lower()
    
    if new_plan not in ['free', 'starter', 'agency', 'enterprise']:
        print("❌ Invalid plan. Cancelled.")
        return
    
    # Confirm
    confirm = input(f"\n⚠️  Change {email} from '{user['plan_type']}' to '{new_plan}'? (yes/no): ").strip().lower()
    
    if confirm != 'yes':
        print("❌ Cancelled.")
        return
    
    # Update database
    conn = models.get_db()
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET plan_type = ? WHERE email = ?", (new_plan, email))
    conn.commit()
    conn.close()
    
    print(f"\n✅ Success! {email} upgraded to '{new_plan}'")
    print("="*50 + "\n")

if __name__ == '__main__':
    upgrade_user()