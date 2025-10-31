#!/usr/bin/env python3
"""
Daily MailerLite Sync Script

This script runs as a daily cron job to:
1. Query MongoDB for contacts from completed shows (show_date < today)
2. Find contacts that haven't been added to MailerLite yet
3. Add them to MailerLite using the existing batch logic
4. Mark them as processed in MongoDB

Run this script once daily via cron job.
"""

import json
import logging
import os
import argparse
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
from datetime import datetime, date
from pymongo import MongoClient
import requests
import re

# Configure logging - will be set up after config is loaded
logger = logging.getLogger(__name__)

# MailerLite API Key - will be loaded from config
API_KEY = None

def setup_logging():
    """Setup logging configuration using config file"""
    config = load_config()
    if not config:
        # Fallback to default log file if config fails
        log_file = "/home/ec2-user/MailerLiteSync/logs/mailerlite_sync.log"
    else:
        log_file = config["LOG_FILE"]
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

# Venue to MailerLite Group Mapping (from addEmailToMailerLite.py)
GROUPS = {
    "townhouse": "143572270449690387",
    "stowaway": "143572260771333843",
    "citizen": "143572251965392675",
    "church": "143572232163034114",
    "palace": "143571926962407099",
    "blind barber fulton market": "148048384759956607",
    "uncategorized": "143572290783675542"
}

def load_config():
    """Load configuration from config file"""
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    CONFIG_FILE = os.path.join(script_dir, "config.json")
    try:
        with open(CONFIG_FILE, 'r') as f:
            config_data = json.load(f)
        return {
            "MONGO_URI": config_data["MONGO_URI"],
            "MONGO_DB": config_data["MONGO_DB"],
            "MONGO_COLLECTIONS": config_data["MONGO_COLLECTIONS"],
            "LOG_FILE": config_data["LOG_FILE"],
            "MAILER_LITE_TOKEN": config_data["MAILER_LITE_TOKEN"]
        }
    except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
        print(f"Error loading config from {CONFIG_FILE}: {str(e)}")  # Use print since logger might not be configured yet
        return None

def is_valid_email(email):
    """Validate email address format"""
    if not email or email.lower() in ['', 'none', 'null']:
        return False
    email_regex = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
    return re.match(email_regex, email) is not None

def parse_show_date(show_date_str):
    """
    Parse show date string to date object.
    Handles various date formats that might be in the database.
    If year is missing, assumes current year.
    """
    try:
        # Try common date formats with year
        for fmt in ["%Y-%m-%d %I:%M %p", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%m/%d/%Y", "%B %d, %Y %I:%M %p"]:
            try:
                parsed = datetime.strptime(show_date_str, fmt)
                return parsed.date()
            except ValueError:
                continue
        
        # Try formats without year (assume current year)
        current_year = datetime.now().year
        
        # Remove ordinal suffixes (1st, 2nd, 3rd, 4th, etc.) only after digits
        import re
        date_clean = re.sub(r'(\d)(st|nd|rd|th)\b', r'\1', show_date_str)
        
        # Try different time formats
        time_formats = ["%I%p", "%I:%M%p"]
        
        for time_fmt in time_formats:
            try:
                parsed = datetime.strptime(f"{date_clean} {current_year}", f"%A %B %d {time_fmt} %Y")
                logger.info(f"Parsed date '{show_date_str}' as {parsed.date()} (assumed year {current_year})")
                return parsed.date()
            except ValueError:
                continue
        
        # If all formats fail, log and return None
        logger.warning(f"Could not parse date: {show_date_str}")
        return None
    except Exception as e:
        logger.warning(f"Error parsing date '{show_date_str}': {str(e)}")
        return None

def get_contacts_to_process(limit=None):
    """
    Query MongoDB for contacts from completed shows that haven't been added to MailerLite
    
    :param limit: Maximum number of contacts to retrieve (optional, for debugging)
    """
    mongo_config = load_config()
    if not mongo_config:
        return []
    
    MONGO_URI = mongo_config["MONGO_URI"]
    MONGO_DB = mongo_config["MONGO_DB"]
    MONGO_COLLECTIONS = mongo_config["MONGO_COLLECTIONS"]
    
    if limit:
        logger.info(f"Running in debug mode: processing maximum {limit} contacts")
    
    all_valid_contacts = []
    
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        
        # Query each collection
        for collection_name in MONGO_COLLECTIONS:
            # Stop if we've reached the limit
            if limit and len(all_valid_contacts) >= limit:
                logger.info(f"Reached limit of {limit} contacts, skipping remaining collections")
                break
                
            logger.info(f"Processing collection: {collection_name}")
            collection = db[collection_name]
            
            # Find contacts that haven't been added to MailerLite (false or missing field)
            query = {
                "$or": [
                    {"added_to_mailerlite": False},
                    {"added_to_mailerlite": {"$exists": False}}
                ],
                "email": {"$exists": True, "$ne": "", "$nin": [None, "none", "null"]}
            }
            
            contacts = list(collection.find(query))
            
            # Count breakdown for logging
            false_count = len(list(collection.find({
                "added_to_mailerlite": False,
                "email": {"$exists": True, "$ne": "", "$nin": [None, "none", "null"]}
            })))
            missing_count = len(list(collection.find({
                "added_to_mailerlite": {"$exists": False},
                "email": {"$exists": True, "$ne": "", "$nin": [None, "none", "null"]}
            })))
            
            logger.info(f"Found {len(contacts)} unprocessed contacts in {collection_name} ({false_count} with false, {missing_count} missing field)")
            
            # Filter by show date (only completed shows)
            today = date.today()
            
            for contact in contacts:
                # Stop if we've reached the limit
                if limit and len(all_valid_contacts) >= limit:
                    break
                    
                show_date = parse_show_date(contact.get('show_date', ''))
                if show_date and show_date < today:
                    # Add collection source for tracking
                    contact['_collection_source'] = collection_name
                    all_valid_contacts.append(contact)
                elif show_date is None:
                    logger.warning(f"Skipping contact with unparseable date: {contact.get('show_date')} for {contact.get('email')} in {collection_name}")
        
        logger.info(f"Found {len(all_valid_contacts)} total contacts from completed shows to process across all collections")
        return all_valid_contacts
        
    except Exception as e:
        logger.error(f"Error querying MongoDB: {str(e)}")
        return []
    finally:
        if 'client' in locals():
            client.close()

def convert_to_mailerlite_format(contacts):
    """
    Convert MongoDB contacts to the format expected by MailerLite batch function
    Returns tuple: (mailerlite_data, invalid_emails)
    - mailerlite_data: dictionary in format {venue: [[contact_array], ...]}
    - invalid_emails: list of email addresses that failed validation
    """
    mailerlite_data = {}
    invalid_emails = []
    
    for contact in contacts:
        # Skip invalid emails
        if not is_valid_email(contact.get('email')):
            email = contact.get('email')
            logger.warning(f"Skipping invalid email: {email}")
            invalid_emails.append(email)
            continue
            
        venue = contact.get('venue', 'uncategorized')
        
        # Create contact array in expected format
        # [venue, date, email, source, time, type, firstname, lastname, tickets, phone(optional)]
        contact_array = [
            contact.get('venue', ''),
            contact.get('show_date', ''),
            contact.get('email', ''),
            contact.get('source', ''),
            contact.get('show_time', ''),
            contact.get('ticket_type', ''),
            contact.get('first_name', ''),
            contact.get('last_name', ''),
            contact.get('tickets', 1),
            contact.get('phone', '')
        ]
        
        if venue not in mailerlite_data:
            mailerlite_data[venue] = []
        
        mailerlite_data[venue].append(contact_array)
    
    return mailerlite_data, invalid_emails

def batch_add_contacts_to_mailerlite(emailsToAdd, api_key):
    """
    Add contacts to MailerLite using batch API (adapted from addEmailToMailerLite.py)
    Returns tuple: (successful_emails, failed_emails, success_by_group)
    """
    logger.info("Starting MailerLite batch upload")
    
    # API Endpoint for batch requests
    batch_url = "https://connect.mailerlite.com/api/batch"
    
    # Request Headers
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    
    # Collect requests for batch processing
    requests_list = []
    processed_emails = []
    group_stats = {}  # Track which groups contacts are added to
    
    for show, contacts in emailsToAdd.items():
        for contact in contacts:
            email = contact[2]
            venue = (contact[0] or "uncategorized").lower()
            group_id = GROUPS.get(venue, GROUPS["uncategorized"])
            group_name = venue if venue in GROUPS else "uncategorized"
            
            # Warn if venue is uncategorized
            if group_name == "uncategorized":
                logger.warning(f"Contact {email} from venue '{contact[0]}' mapped to 'uncategorized' - venue not found in GROUPS mapping")
            
            # Skip if email is not valid
            if not is_valid_email(email):
                logger.warning(f"Invalid email skipped: {email}")
                continue
            
            # Track group statistics
            if group_name not in group_stats:
                group_stats[group_name] = 0
            group_stats[group_name] += 1
            
            first_name = contact[6]
            last_name = contact[7]
            name = f"{first_name} {last_name}".strip()
            phone = contact[9] if len(contact) > 9 else None
            
            # Prepare individual request body
            fields = {"name": name}
            
            # Add phone if it exists and is not empty
            if phone and str(phone).strip() and str(phone).lower() not in ['none', 'null', '']:
                fields["phone"] = str(phone).strip()
            
            body = {
                "email": email,
                "fields": fields,
                "groups": [group_id]
            }
            
            requests_list.append({
                "method": "POST",
                "path": "/api/subscribers",
                "body": body
            })
            
            processed_emails.append(email)
    
    # Log group distribution
    for group_name, count in group_stats.items():
        logger.info(f"Preparing to add {count} contacts to mailing list: {group_name}")
    
    if not requests_list:
        logger.info("No valid contacts to process")
        return []
    
    # Split requests into batches of 50
    successful_emails = []
    failed_emails = []
    successful_by_group = {}  # Track successful additions per group
    
    for i in range(0, len(requests_list), 50):
        batch_payload = {"requests": requests_list[i:i+50]}
        
        # Make the batch request
        try:
            response = requests.post(batch_url, json=batch_payload, headers=headers)
            
            # Handle response
            if response.status_code == 200:
                result = response.json()
                logger.info(f"Batch Process Completed: {result['successful']} successful, {result['failed']} failed.")
                
                # Track successful emails for this batch
                batch_start = i
                for idx, res in enumerate(result['responses']):
                    email_idx = batch_start + idx
                    if email_idx < len(processed_emails):
                        if res.get('code') in [200, 201]:  # Success codes
                            email = processed_emails[email_idx]
                            successful_emails.append(email)
                            
                            # Track which group this success belongs to
                            request_body = requests_list[email_idx]["body"]
                            group_id = request_body["groups"][0]
                            group_name = next((k for k, v in GROUPS.items() if v == group_id), "unknown")
                            
                            if group_name not in successful_by_group:
                                successful_by_group[group_name] = []
                            successful_by_group[group_name].append(email)
                        else:
                            failed_emails.append(processed_emails[email_idx])
                            logger.warning(f"Failed to add {processed_emails[email_idx]}: {res}")
            else:
                logger.error(f"Failed to process batch: {response.status_code} - {response.text}")
                # Mark all emails in this batch as failed
                batch_emails = processed_emails[i:i+50]
                failed_emails.extend(batch_emails)
                
        except Exception as e:
            logger.error(f"Error processing batch: {str(e)}")
            batch_emails = processed_emails[i:i+50]
            failed_emails.extend(batch_emails)
    
    # Log successful additions per mailing list
    for group_name, emails in successful_by_group.items():
        emails_with_details = []
        for email in emails:
            # Find the original contact to check for phone
            idx = processed_emails.index(email)
            request_body = requests_list[idx]["body"]
            fields = request_body.get("fields", {})
            
            if "phone" in fields:
                emails_with_details.append(f"{email} (phone: {fields['phone']})")
            else:
                emails_with_details.append(email)
        
        logger.info(f"Successfully added {len(emails)} contacts to mailing list '{group_name}': {', '.join(emails_with_details)}")
    
    logger.info(f"MailerLite upload complete: {len(successful_emails)} successful, {len(failed_emails)} failed")
    return successful_emails, failed_emails, successful_by_group

def mark_contacts_as_processed(successful_emails, processed_contacts):
    """
    Mark successfully processed contacts as added to MailerLite in MongoDB
    """
    if not successful_emails:
        return
        
    mongo_config = load_config()
    if not mongo_config:
        return
    
    MONGO_URI = mongo_config["MONGO_URI"]
    MONGO_DB = mongo_config["MONGO_DB"]
    MONGO_COLLECTIONS = mongo_config["MONGO_COLLECTIONS"]
    
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        
        # Group successful emails by their source collection
        email_to_collection = {}
        for contact in processed_contacts:
            if contact.get('email') in successful_emails:
                email_to_collection[contact.get('email')] = contact.get('_collection_source')
        
        total_updated = 0
        
        # Update each collection separately
        for collection_name in MONGO_COLLECTIONS:
            collection = db[collection_name]
            
            # Get emails that belong to this collection
            emails_for_this_collection = [
                email for email, source in email_to_collection.items() 
                if source == collection_name
            ]
            
            if emails_for_this_collection:
                logger.info(f"Updating {len(emails_for_this_collection)} contacts in {collection_name} with added_to_mailerlite: true")
                
                # Update contacts in this collection
                result = collection.update_many(
                    {"email": {"$in": emails_for_this_collection}},
                    {
                        "$set": {
                            "added_to_mailerlite": True,
                            "mailerlite_added_date": datetime.utcnow(),
                            "updated_at": datetime.utcnow()
                        }
                    }
                )
                
                logger.info(f"Successfully updated {result.modified_count} contacts as processed in {collection_name}")
                total_updated += result.modified_count
            else:
                logger.info(f"No contacts to update in {collection_name}")
        
        logger.info(f"Database update complete: {total_updated} total contacts marked as added_to_mailerlite: true")
        
    except Exception as e:
        logger.error(f"Error updating MongoDB: {str(e)}")
    finally:
        if 'client' in locals():
            client.close()

def handle_failed_contacts(failed_emails, processed_contacts):
    """
    Handle contacts that failed to be added to MailerLite:
    1. Add them to the 'failed' collection
    2. Delete them from their original collection
    
    :param failed_emails: List of email addresses that failed
    :param processed_contacts: Original list of contact documents with _collection_source
    """
    if not failed_emails:
        logger.info("No failed contacts to handle")
        return
    
    mongo_config = load_config()
    if not mongo_config:
        return
    
    MONGO_URI = mongo_config["MONGO_URI"]
    MONGO_DB = mongo_config["MONGO_DB"]
    
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        failed_collection = db["failed"]
        
        # Group failed emails by their source collection
        email_to_contact = {}
        for contact in processed_contacts:
            if contact.get('email') in failed_emails:
                email_to_contact[contact.get('email')] = contact
        
        total_moved = 0
        total_deleted = 0
        
        for email, contact in email_to_contact.items():
            source_collection_name = contact.get('_collection_source')
            
            if not source_collection_name:
                logger.warning(f"Cannot move failed contact {email}: no source collection")
                continue
            
            # Add failure metadata
            contact['failed_at'] = datetime.utcnow()
            contact['failure_reason'] = 'mailerlite_import_failed'
            
            # Insert into failed collection
            try:
                failed_collection.insert_one(contact)
                logger.info(f"Added failed contact {email} to 'failed' collection")
                total_moved += 1
                
                # Delete from original collection
                source_collection = db[source_collection_name]
                result = source_collection.delete_one({"email": email})
                
                if result.deleted_count > 0:
                    logger.info(f"Deleted {email} from original collection '{source_collection_name}'")
                    total_deleted += 1
                else:
                    logger.warning(f"Failed to delete {email} from '{source_collection_name}'")
                    
            except Exception as e:
                logger.error(f"Error handling failed contact {email}: {str(e)}")
        
        logger.info(f"Failed contacts handling complete: {total_moved} moved to 'failed' collection, {total_deleted} deleted from original collections")
        
    except Exception as e:
        logger.error(f"Error in handle_failed_contacts: {str(e)}")
    finally:
        if 'client' in locals():
            client.close()

def main():
    """Main function to run the daily sync process"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Sync contacts to MailerLite')
    parser.add_argument('--limit', type=int, default=None, 
                       help='Maximum number of contacts to process (for debugging)')
    args = parser.parse_args()
    
    # Setup logging first
    setup_logging()
    logger.info("Starting daily MailerLite sync process")
    
    # Load config to get API key
    config = load_config()
    if not config:
        logger.error("Failed to load config, cannot proceed")
        return
    
    api_key = config.get("MAILER_LITE_TOKEN")
    if not api_key:
        logger.error("MAILER_LITE_TOKEN not found in config")
        return
    
    try:
        # Step 1: Get contacts to process
        contacts = get_contacts_to_process(limit=args.limit)
        
        if not contacts:
            logger.info("No contacts to process today")
            return
        
        # Step 2: Convert to MailerLite format
        mailerlite_data, invalid_emails = convert_to_mailerlite_format(contacts)
        
        if not mailerlite_data:
            logger.info("No valid contacts after filtering")
            # Still need to handle invalid emails even if no valid ones
            if invalid_emails:
                handle_failed_contacts(invalid_emails, contacts)
            return
        
        # Step 3: Add to MailerLite
        successful_emails, failed_emails, success_by_group = batch_add_contacts_to_mailerlite(mailerlite_data, api_key)
        
        # Step 4: Mark successful contacts as processed in MongoDB
        mark_contacts_as_processed(successful_emails, contacts)
        
        # Step 5: Handle failed contacts (both invalid and MailerLite rejected)
        all_failed_emails = invalid_emails + failed_emails
        handle_failed_contacts(all_failed_emails, contacts)
        
        # Step 6: Log summary
        logger.info("=" * 60)
        logger.info("SYNC SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total contacts processed: {len(successful_emails) + len(all_failed_emails)}")
        logger.info(f"Successfully added to MailerLite: {len(successful_emails)}")
        logger.info(f"Failed: {len(all_failed_emails)}")
        logger.info("")
        logger.info("Breakdown by mailing list:")
        for group_name in sorted(success_by_group.keys()):
            logger.info(f"  - {group_name}: {len(success_by_group[group_name])} contacts")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")

if __name__ == "__main__":
    main()
