import requests
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('MAILERLITE_API_KEY')

GROUPS = {
    "townhouse": "143626801579558524",
    "stowaway": "143626801579558524",
    "citizen": "143626801579558524",
    "the virgil": "143626801579558524",

    "church": "143625708407621344",
    "palace": "143625708407621344",

    "blind barber fulton market": "148048384759956607",
    "blind barber": "148048384759956607",

    "revision lounge": "143625553542383583",
    "velvet room": "143625553542383583",

    "rabbitbox": "170455675935131130",

    "uncategorized": "143572290783675542"
}

def is_valid_email(email):
    import re
    email_regex = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
    return re.match(email_regex, email) is not None

def batch_add_contacts_to_mailerlite(emailsToAdd):

    print("Debug: Emails to Add:")
    from pprint import pprint
    pprint(emailsToAdd)

    batch_url = "https://connect.mailerlite.com/api/batch"

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {API_KEY}"
    }

    requests_list = []

    for show, contacts in emailsToAdd.items():
        
        for contact in contacts:
            email = contact[2]
            group_id = GROUPS.get(contact[0].lower(), GROUPS["uncategorized"])

            if not is_valid_email(email):
                print(f"Invalid email skipped: {email}")
                continue

            first_name = contact[6]
            last_name = contact[7]
            name = f"{first_name} {last_name}".strip()

            body = {
                "email": email,
                "fields": {"name": name},
                "groups": [group_id]
            }

            requests_list.append({
                "method": "POST",
                "path": "/api/subscribers",
                "body": body
            })

    for i in range(0, len(requests_list), 50):
        batch_payload = {"requests": requests_list[i:i+50]}

        response = requests.post(batch_url, json=batch_payload, headers=headers)

        if response.status_code == 200:
            result = response.json()
            print(f"Batch Process Completed: {result['successful']} successful, {result['failed']} failed.")
            for res in result['responses']:
                print(res)
        else:
            print(f"Failed to process batch: {response.status_code}", response.json())

