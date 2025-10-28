import requests
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MailerLite API Key from environment
API_KEY = os.getenv('MAILERLITE_API_KEY')

# Segment Mapping
GROUPS = {
    "townhouse": "143572270449690387",
    "stowaway": "143572260771333843",
    "citizen": "143572251965392675",
    "church": "143572232163034114",
    "palace": "143571926962407099",
    "blind barber fulton market": "148048384759956607",
    "uncategorized": "143572290783675542"
}

# Function to validate email
def is_valid_email(email):
    """
    Validates if the provided string is a valid email address.

    :param email: Email address to validate
    :return: True if valid, False otherwise
    """
    import re
    email_regex = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
    return re.match(email_regex, email) is not None

# Function to Batch Add Contacts to MailerLite
def batch_add_contacts_to_mailerlite(emailsToAdd):

    # Debugging output
    print("Debug: Emails to Add:")
    from pprint import pprint
    pprint(emailsToAdd)

    """
    Batch adds contacts to MailerLite segments.

    :param batch_data: Dictionary with show names as keys and contact lists as values
    """
    # API Endpoint for batch requests
    batch_url = "https://connect.mailerlite.com/api/batch"

    # Request Headers
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {API_KEY}"
    }

    # Collect requests for batch processing
    requests_list = []

    for show, contacts in emailsToAdd.items():
        
        for contact in contacts:
            email = contact[2]
            group_id = GROUPS.get(contact[0].lower(), GROUPS["uncategorized"])

            # Skip if email is not valid
            if not is_valid_email(email):
                print(f"Invalid email skipped: {email}")
                continue

            first_name = contact[6]
            last_name = contact[7]
            name = f"{first_name} {last_name}".strip()

            # Prepare individual request body
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

    # Split requests into batches of 50
    for i in range(0, len(requests_list), 50):
        batch_payload = {"requests": requests_list[i:i+50]}

        # Make the batch request
        response = requests.post(batch_url, json=batch_payload, headers=headers)

        # Handle response
        if response.status_code == 200:
            result = response.json()
            print(f"Batch Process Completed: {result['successful']} successful, {result['failed']} failed.")
            for res in result['responses']:
                print(res)
        else:
            print(f"Failed to process batch: {response.status_code}", response.json())

# Example Data Structure
#test_data = {
#    "Townhouse": [
#        ["Townhouse", "2025-01-15", "janedoe@example.com", "Guest List", "7:00 PM", "GA", "Jane", "Doe", 2],
#        ["Townhouse", "2025-01-15", "johndoe@example.com", "Eventbrite", "8:00 PM", "GA", "John", "Doe", 3],
#        ["Townhouse", "2025-01-15", "", "Eventbrite", "8:00 PM", "GA", "John", "Doe", 3],
#        ["Townhouse", "2025-01-15", "none", "Eventbrite", "8:00 PM", "GA", "John", "Doe", 3]
#    ],
#    "Speakeasy": [
#        ["Speakeasy", "2025-01-16", "alice@example.com", "Squarespace", "7:30 PM", "GA", "Alice", "Smith", 1],
#        ["Speakeasy", "2025-01-16", "invalid-email", "Squarespace", "9:00 PM", "VIP", "Bob", "Johnson", 4]
#    ],
#    "Hotel": [
#        ["Hotel", "2025-01-16", "steven@example.com", "Squarespace", "7:30 PM", "GA", "Steven", "Smith", 1],
#        ["Hotel", "2025-01-16", "ghrt3", "Squarespace", "9:00 PM", "VIP", "Bob", "Johnson", 4]
#    ]
#}

# Call the function with the example data
#batch_add_contacts_to_mailerlite(test_data)
