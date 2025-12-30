import os
import requests
import re
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from extract import *
from urllib.parse import quote_plus
from sqlalchemy import create_engine,text,types
from sqlalchemy.types import Integer
from dotenv import load_dotenv
import json

# Load environment variables for database credentials
load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")

MAP_PATH = os.path.join(os.path.dirname(__file__), 'map.json')

def get_industry_for_client(client_name):
    with open(MAP_PATH, 'r') as f:
        client_map = json.load(f)
    client_data = client_map.get(client_name)
    return client_data.get('industry') if client_data else "Unknown"

def normalize_account_name(name):
    """
    Normalize account names for comparison.
    Args:
        name (str): The account name to normalize.
    Returns:
        str: The normalized account name.
    """
    try:
        if not name or not isinstance(name, str):
            return ''
        name = name.lower()
        name = re.sub(r'\s*-\s*praytell\s*$', '', name)
        name = re.sub(r'\s*praytell\s*$', '', name)
        name = re.sub(r'\s*campus\s*$', '', name)
        name = re.sub(r'\s*\(.*?\)', '', name)
        name = re.sub(r'[^\w\s-]', '', name)
        name = re.sub(r'\s+', ' ', name)
        return name.strip()
    except Exception as e:
        print(f"Error normalizing name '{name}': {str(e)}")
        return ''

def get_db_name(name):
    """
    Automatically extracts and formats the database name from the input string.
    For hyphenated names (e.g., 'client-name - praytell'), uses the first part.
    For non-hyphenated names, uses the full name with appropriate formatting.
    Args:
        name (str): Input filename or client name.
    Returns:
        str: Formatted database name.
    """
    try:
        if not name or not isinstance(name, str):
            return 'files'
        normalized_name = normalize_account_name(name)
        if not normalized_name:
            return 'files'
        if 'ao' in normalized_name:
            return 'angry_orchard'
        if 'g-p' in normalized_name:
            return 'g_p'
        parts = re.split(r'\s*[-–—]\s*praytell\s*', normalized_name, flags=re.IGNORECASE)
        client_name = parts[0].strip()
        if not client_name:
            return 'files'
        db_name = re.sub(r'[^a-z0-9]+', '_', client_name)
        db_name = db_name.strip('_')
        if not db_name:
            return 'files'
        print(f"Filename: {name} -> Database: {db_name}")
        return db_name
    except Exception as e:
        print(f"Error processing name '{name}': {str(e)}")
        return 'files'

# This function retrieves the list of databases from the MySQL server.
def get_available_db():
    """Return a DataFrame listing databases available on the MySQL server."""
    db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}"
    engine = create_engine(db_url)
    query = "show databases;"  # Replace with your actual table name
    # Execute the query
    with engine.connect() as connection:
        result = connection.execute(text(query))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df

#cleaning the client name
def get_client_name(name):
# Remove trailing patterns like "- export" or "export str"
  name = re.sub(r'\s*-\s*(export.*|str.*)\s*$', '', name)
  # Remove any extra spaces
  name = re.sub(r'\s+', ' ', name).strip()
  return get_db_name(name)

# get accounts
def get_facebook_accounts(fb_access_token):
    """Get Facebook ad accounts."""
    try:
        url = 'https://graph.facebook.com/v21.0/me'
        params = {
            'fields': 'id,name,adaccounts.limit(1000){name,id}',
            'access_token': fb_access_token
        }

        response = requests.get(url, params=params)
        data = response.json()

        if 'adaccounts' in data and 'data' in data['adaccounts']:
            accounts = {str(item['id']): item['name'] for item in data['adaccounts']['data']}
            print(f"Retrieved {len(accounts)} Facebook ad accounts")
            return accounts
        else:
            print("No Facebook ad accounts found in the response")
            return {}

    except requests.exceptions.RequestException as e:
        print(f"Error fetching Facebook ad accounts: {str(e)}")
        return {}

def get_tiktok_accounts(tiktok_access_token, tiktok_app_id, tiktok_secret):
    """Get TikTok advertisers."""
    url = "https://business-api.tiktok.com/open_api/v1.3/oauth2/advertiser/get/"
    headers = {"Access-Token": tiktok_access_token}
    params = {"app_id": tiktok_app_id, "secret": tiktok_secret}

    try:
        response = requests.get(url, headers=headers, params=params)
        data = response.json()
        advertisers = {
            str(item['advertiser_id']): item['advertiser_name']
            for item in data.get('data', {}).get('list', [])
        }
        print(f"Retrieved {len(advertisers)} TikTok advertisers")
        return advertisers
    except Exception as e:
        print(f"Error getting TikTok advertisers: {e}")
        return {}

def get_linkedin_accounts(linkedin_access_token):
    """Get LinkedIn ad accounts."""
    url = "https://api.linkedin.com/rest/adAccounts?q=search&search=(type:(values:List(BUSINESS,ENTERPRISE)),status:(values:List(ACTIVE)))"
    headers = {
        "Authorization": f"Bearer {linkedin_access_token}",
        "Linkedin-Version": "202410",
        "X-Restli-Protocol-Version": "2.0.0",
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        accounts = {
            str(element['id']): element['name']
            for element in data.get('elements', [])
        }
        print(f"Retrieved {len(accounts)} LinkedIn ad accounts")
        return accounts
    except Exception as e:
        print(f"Error getting LinkedIn ad accounts: {e}")
        return {}

def get_youtube_accounts(ga_developer_token, ga_client_id, ga_client_secret, ga_refresh_token):
    """Fetch Google Ads VIDEO campaign accounts as a proxy for YouTube advertisers."""
    load_dotenv()

    config = {
        "developer_token": ga_developer_token,
        "client_id": ga_client_id,
        "client_secret": ga_client_secret,
        "refresh_token": ga_refresh_token,
        "use_proto_plus": True
    }

    try:
        client = GoogleAdsClient.load_from_dict(config)
        customer_service = client.get_service("CustomerService")

        # Get all accessible accounts
        accessible_customers = customer_service.list_accessible_customers()
        customer_ids = [res.replace("customers/", "") for res in accessible_customers.resource_names]

        accounts = {}
        for customer_id in customer_ids:
            try:
                ga_service = client.get_service("GoogleAdsService")
                query = """SELECT customer.descriptive_name, campaign.advertising_channel_type FROM campaign LIMIT 1"""
                response = ga_service.search(customer_id=customer_id, query=query)

                for row in response:
                    accounts[customer_id] = row.customer.descriptive_name
                    break

            except GoogleAdsException as e:
                print(f"❌ Skipped {customer_id}: {e}")
            except Exception as e:
                print(f"❗ Unexpected error for {customer_id}: {e}")
        print(f"Retrieved {len(accounts)} YouTube ad accounts")
        return accounts

    except Exception as e:
        print(f"Error retrieving YouTube accounts: {e}")
        return {}
# This function generates a mapping of Facebook, TikTok, and LinkedIn accounts.
# It normalizes the account names for comparison and returns a dictionary with the mapping.
# The mapping includes the display name and the corresponding account IDs for each platform.
# The function also handles cases where account names may have different formats or contain special characters.
# It ensures that the mapping is consistent and can be used for further processing or analysis.

def generate_mapping(fb_access_token, tiktok_access_token, tiktok_app_id, tiktok_secret, linkedin_access_token,ga_developer_token, ga_client_id, ga_client_secret, ga_refresh_token):
    fb_accounts = get_facebook_accounts(fb_access_token)
    tiktok_accounts = get_tiktok_accounts(tiktok_access_token, tiktok_app_id, tiktok_secret)
    linkedin_accounts = get_linkedin_accounts(linkedin_access_token)
    youtube_accounts = get_youtube_accounts(ga_developer_token, ga_client_id, ga_client_secret, ga_refresh_token)

    normalized_mapping = {}

    # Facebook
    for account_id, name in fb_accounts.items():
        normalized_name = normalize_account_name(name)
        normalized_mapping.setdefault(normalized_name, {
            'display_name': name,
            'facebook': [],
            'tiktok': [],
            'linkedin': [],
            'youtube': []
        })['facebook'].append((str(account_id), name))

    # TikTok
    for account_id, name in tiktok_accounts.items():
        normalized_name = normalize_account_name(name)
        normalized_mapping.setdefault(normalized_name, {
            'display_name': name,
            'facebook': [],
            'tiktok': [],
            'linkedin': [],
            'youtube': []
        })['tiktok'].append((str(account_id), name))

    # LinkedIn
    for account_id, name in linkedin_accounts.items():
        normalized_name = normalize_account_name(name)
        normalized_mapping.setdefault(normalized_name, {
            'display_name': name,
            'facebook': [],
            'tiktok': [],
            'linkedin': [],
            'youtube': []
        })['linkedin'].append((str(account_id), name))

    # YouTube
    for account_id, name in youtube_accounts.items():
        normalized_name = normalize_account_name(name)

        normalized_mapping.setdefault(normalized_name, {
            'display_name': name,
            'facebook': [],
            'tiktok': [],
            'linkedin': [],
            'youtube': []
        })['youtube'].append((str(account_id), name))

    # Final tidy mapping
    final_mapping = {}
    for data in normalized_mapping.values():
        display_name = data['display_name']
        final_mapping[display_name] = {
            'facebook': data['facebook'],
            'tiktok': data['tiktok'],
            'linkedin': data['linkedin'],
            'youtube': data['youtube']
        }

    return final_mapping





