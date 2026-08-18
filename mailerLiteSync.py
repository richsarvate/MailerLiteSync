#!/usr/bin/env python3

import json
import logging
import os
import argparse
from dotenv import load_dotenv

load_dotenv()
from datetime import datetime, date
from pymongo import MongoClient
import requests
import re

logger = logging.getLogger(__name__)

API_KEY = None

def setup_logging():
    config = load_config()
    if not config:
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

from addEmailToMailerLite import GROUPS

def load_config():
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
        print(f"Error loading config from {CONFIG_FILE}: {str(e)}")
        return None

def is_valid_email(email):
    if not email or email.lower() in ['', 'none', 'null']:
        return False
    email_regex = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
    return re.match(email_regex, email) is not None

def parse_show_date(contact):
    if contact.get('show_datetime'):
        show_datetime = contact.get('show_datetime')
        try:
            if isinstance(show_datetime, datetime):
                return show_datetime.date()
            elif isinstance(show_datetime, date):
                return show_datetime
            elif isinstance(show_datetime, str):
                return datetime.strptime(show_datetime, "%Y-%m-%d %H:%M:%S").date()
        except Exception as e:
            logger.warning(f"Failed to parse show_datetime '{show_datetime}', falling back to show_date: {str(e)}")
    
    show_date_str = contact.get('show_date', '')
    
    try:
        import re
        date_clean = re.sub(r'(\d)(st|nd|rd|th)\b', r'\1', show_date_str)
        
        for fmt in ["%Y-%m-%d %I:%M %p", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%m/%d/%Y", "%B %d, %Y %I:%M %p", "%A %B %d %I%p %Y", "%A %B %d %I:%M%p %Y"]:
            try:
                parsed = datetime.strptime(date_clean, fmt)
                return parsed.date()
            except ValueError:
                continue
        
        current_year = datetime.now().year
        
        time_formats = ["%I%p", "%I:%M%p"]
        
        for time_fmt in time_formats:
            try:
                parsed = datetime.strptime(f"{date_clean} {current_year}", f"%A %B %d {time_fmt} %Y")
                logger.info(f"Parsed date '{show_date_str}' as {parsed.date()} (assumed year {current_year})")
                return parsed.date()
            except ValueError:
                continue
        
        logger.warning(f"Could not parse date: {show_date_str}")
        return None
    except Exception as e:
        logger.warning(f"Error parsing date '{show_date_str}': {str(e)}")
        return None

def get_contacts_to_process(limit=None):
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
        
        for collection_name in MONGO_COLLECTIONS:
            if limit and len(all_valid_contacts) >= limit:
                logger.info(f"Reached limit of {limit} contacts, skipping remaining collections")
                break
                
            logger.info(f"Processing collection: {collection_name}")
            collection = db[collection_name]
            
            query = {
                "$or": [
                    {"added_to_mailerlite": False},
                    {"added_to_mailerlite": {"$exists": False}}
                ],
                "email": {"$exists": True, "$ne": "", "$nin": [None, "none", "null"]}
            }
            
            contacts = list(collection.find(query))
            
            false_count = len(list(collection.find({
                "added_to_mailerlite": False,
                "email": {"$exists": True, "$ne": "", "$nin": [None, "none", "null"]}
            })))
            missing_count = len(list(collection.find({
                "added_to_mailerlite": {"$exists": False},
                "email": {"$exists": True, "$ne": "", "$nin": [None, "none", "null"]}
            })))
            
            logger.info(f"Found {len(contacts)} unprocessed contacts in {collection_name} ({false_count} with false, {missing_count} missing field)")
            
            today = date.today()
            
            for contact in contacts:
                if limit and len(all_valid_contacts) >= limit:
                    break
                    
                show_date = parse_show_date(contact)
                if show_date and show_date < today:
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
    mailerlite_data = {}
    invalid_emails = []
    
    for contact in contacts:
        if not is_valid_email(contact.get('email')):
            email = contact.get('email')
            logger.warning(f"Skipping invalid email: {email}")
            invalid_emails.append(email)
            continue
            
        venue = contact.get('venue', 'uncategorized')
        
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
    logger.info("Starting MailerLite batch upload")
    
    batch_url = "https://connect.mailerlite.com/api/batch"
    
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    
    requests_list = []
    processed_emails = []
    group_stats = {}
    
    for show, contacts in emailsToAdd.items():
        for contact in contacts:
            email = contact[2]
            venue = (contact[0] or "uncategorized").lower()
            group_id = GROUPS.get(venue, GROUPS["uncategorized"])
            group_name = venue if venue in GROUPS else "uncategorized"
            
            if group_name == "uncategorized":
                logger.warning(f"Contact {email} from venue '{contact[0]}' mapped to 'uncategorized' - venue not found in GROUPS mapping")
            
            if not is_valid_email(email):
                logger.warning(f"Invalid email skipped: {email}")
                continue
            
            if group_name not in group_stats:
                group_stats[group_name] = 0
            group_stats[group_name] += 1
            
            first_name = contact[6]
            last_name = contact[7]
            name = f"{first_name} {last_name}".strip()
            phone = contact[9] if len(contact) > 9 else None
            
            fields = {"name": name}
            
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
    
    for group_name, count in group_stats.items():
        logger.info(f"Preparing to add {count} contacts to mailing list: {group_name}")
    
    if not requests_list:
        logger.info("No valid contacts to process")
        return []
    
    successful_emails = []
    failed_emails = []
    successful_by_group = {}
    
    for i in range(0, len(requests_list), 50):
        batch_payload = {"requests": requests_list[i:i+50]}
        
        try:
            response = requests.post(batch_url, json=batch_payload, headers=headers)
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"Batch Process Completed: {result['successful']} successful, {result['failed']} failed.")
                
                batch_start = i
                for idx, res in enumerate(result['responses']):
                    email_idx = batch_start + idx
                    if email_idx < len(processed_emails):
                        if res.get('code') in [200, 201]:
                            email = processed_emails[email_idx]
                            successful_emails.append(email)
                            
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
                batch_emails = processed_emails[i:i+50]
                failed_emails.extend(batch_emails)
                
        except Exception as e:
            logger.error(f"Error processing batch: {str(e)}")
            batch_emails = processed_emails[i:i+50]
            failed_emails.extend(batch_emails)
    
    for group_name, emails in successful_by_group.items():
        emails_with_details = []
        for email in emails:
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
        
        email_to_collection = {}
        for contact in processed_contacts:
            if contact.get('email') in successful_emails:
                email_to_collection[contact.get('email')] = contact.get('_collection_source')
        
        total_updated = 0
        
        for collection_name in MONGO_COLLECTIONS:
            collection = db[collection_name]
            
            emails_for_this_collection = [
                email for email, source in email_to_collection.items() 
                if source == collection_name
            ]
            
            if emails_for_this_collection:
                logger.info(f"Updating {len(emails_for_this_collection)} contacts in {collection_name} with added_to_mailerlite: true")
                
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
            
            contact['failed_at'] = datetime.utcnow()
            contact['failure_reason'] = 'mailerlite_import_failed'
            
            try:
                failed_collection.insert_one(contact)
                logger.info(f"Added failed contact {email} to 'failed' collection")
                total_moved += 1
                
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
    parser = argparse.ArgumentParser(description='Sync contacts to MailerLite')
    parser.add_argument('--limit', type=int, default=None, 
                       help='Maximum number of contacts to process (for debugging)')
    args = parser.parse_args()
    
    setup_logging()
    logger.info("Starting daily MailerLite sync process")
    
    config = load_config()
    if not config:
        logger.error("Failed to load config, cannot proceed")
        return
    
    api_key = config.get("MAILER_LITE_TOKEN")
    if not api_key:
        logger.error("MAILER_LITE_TOKEN not found in config")
        return
    
    try:
        contacts = get_contacts_to_process(limit=args.limit)
        
        if not contacts:
            logger.info("No contacts to process today")
            return
        
        mailerlite_data, invalid_emails = convert_to_mailerlite_format(contacts)
        
        if not mailerlite_data:
            logger.info("No valid contacts after filtering")
            if invalid_emails:
                handle_failed_contacts(invalid_emails, contacts)
            return
        
        successful_emails, failed_emails, success_by_group = batch_add_contacts_to_mailerlite(mailerlite_data, api_key)
        
        mark_contacts_as_processed(successful_emails, contacts)
        
        all_failed_emails = invalid_emails + failed_emails
        handle_failed_contacts(all_failed_emails, contacts)
        
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
