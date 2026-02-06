import models
import sys

client_id = sys.argv[1] if len(sys.argv) > 1 else 'popo-af2dcd10'

client = models.get_client_by_id(client_id)

if client:
    print(f"Client: {client['client_id']}")
    print(f"Company: {client['company_name']}")
    print(f"Owner User ID: {client['user_id']}")
    
    # Get owner details
    user = models.get_user_by_id(client['user_id'])
    if user:
        print(f"Owner Email: {user['email']}")
        print(f"Owner Plan: {user['plan_type']}")
else:
    print(f"Client '{client_id}' not found!")