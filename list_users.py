import models

conn = models.get_db()
cursor = conn.cursor()
cursor.execute("SELECT email, plan_type, created_at FROM users ORDER BY created_at DESC")
users = cursor.fetchall()
conn.close()

print("\n" + "="*70)
print("ALL USERS")
print("="*70)
print(f"{'Email':<35} {'Plan':<12} {'Created':<20}")
print("-"*70)

for user in users:
    print(f"{user['email']:<35} {user['plan_type']:<12} {user['created_at']:<20}")

print("-"*70)
print(f"Total users: {len(users)}\n")