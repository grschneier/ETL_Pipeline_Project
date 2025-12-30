import requests
import json
import pandas as pd
from facebook_business.api import FacebookAdsApi
import urllib.parse
from dotenv import load_dotenv
import os
import time
from datetime import datetime, timedelta
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException


# Map keys to clients to create dictionary json-style
def convert_lists_to_tuples(obj):
    if isinstance(obj, list):
        # Convert lists of length 2 (assuming that's your tuple structure) to tuples
        if all(isinstance(i, list) and len(i) == 2 for i in obj):
            return [tuple(i) for i in obj]
        else:
            return [convert_lists_to_tuples(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_lists_to_tuples(v) for k, v in obj.items()}
    else:
        return obj

# Load JSON from a file or string
with open('map.json', 'r') as f:
    map = json.load(f)

# Convert inner lists to tuples
map = convert_lists_to_tuples(map)

def fetch_facebook_report(platforms):
    load_dotenv()
    access_token = os.getenv("FB_ACCESS_TOKEN")
    app_id = os.getenv("FB_APP_ID")
    app_secret = os.getenv("FB_APP_SECRET")
    if not access_token or not app_id or not app_secret:
        raise ValueError("Facebook credentials are missing")
    try:
        FacebookAdsApi.init(app_id, app_secret, access_token)
    except Exception as e:
        print(f"Error initializing Facebook API: {str(e)}")
        return pd.DataFrame()

    def get_adset_details(ad_account_id):
        try:
            params = {
                'fields': 'id,name,start_time,end_time,lifetime_budget,budget_remaining',
                'limit': 5000,
                'date_preset': 'maximum',
                'access_token': access_token
            }
            adset_url = f"https://graph.facebook.com/v21.0/{ad_account_id}/adsets"
            adset_details = requests.get(adset_url, params=params).json()
            return adset_details['data']
        except requests.exceptions.RequestException as e:
            print(f"Network error fetching adset details for account {ad_account_id}: {str(e)}")
            return []
        except json.JSONDecodeError as e:
            print(f"JSON decoding error for account {ad_account_id}: {str(e)}")
            return []

    def fetch_adsets(ad_account_id):
    #Fetch ad insights from yesterday at the ad level.
        try:
            # Get yesterday's date
            yesterday = datetime.now() - timedelta(days=1)
            date_str = yesterday.strftime('%Y-%m-%d')

            url = f"https://graph.facebook.com/v21.0/{ad_account_id}/insights?"
            params = {
                'fields': 'campaign_id,objective,adset_id,ad_name,adset_name,campaign_name,impressions,spend,reach,ad_id,actions,date_start,date_stop',
                'time_range': json.dumps({'since': date_str, 'until': date_str}),
                'time_increment': 1,
                'limit': 4000,
                'filtering': '[{"field":"action_type","operator":"IN","value":["post_reaction","post","comment","link_click","video_view","onsite_conversion.post_save","post_engagement"]}]',
                'level': 'ad',
                'breakdowns': 'publisher_platform',
                'access_token': access_token
            }
            response = requests.get(url, params=params)
            data = response.json()
            #print(data)
            if 'data' in data:
                return data['data']
            else:
                print(f"No 'data' key in Facebook API response: {data}")
                return []
        except requests.exceptions.RequestException as e:
            print(f"Network error fetching adsets for account {ad_account_id}: {str(e)}")
            return []
        except json.JSONDecodeError as e:
            print(f"JSON decoding error for account {ad_account_id}: {str(e)}")
            return []
        except Exception as e:
            print(f"Error {e}")
            return []

    def process_actions(actions):
        action_dict = {}
        for action in actions:
            action_type = action.get('action_type')
            value = action.get('value', '0')
            if action_type:
                action_dict[action_type] = value
        return action_dict

    all_data = []

    for ad_account_id, account_name in platforms.get('facebook', []):
        adsets_data = fetch_adsets(ad_account_id)
        adsets_details_data = get_adset_details(ad_account_id)
        adsets_details_dict = {item['id']: {k: v for k, v in item.items() if k != 'id'} for item in adsets_details_data}

        for adset in adsets_data:
            try:
                adset_id = adset['adset_id']
                adset_name = adset['adset_name']
                ad_name = adset['ad_name']
                campaign_name = adset['campaign_name']
                spend = adset['spend']
                impressions = adset['impressions']
                reach = adset['reach']
                platform = adset['publisher_platform']
                campaign_id = adset['campaign_id']
                objective = adset['objective']
                actions = process_actions(adset.get('actions', []))
                adset_details = adsets_details_dict.get(adset_id, {})
                budget = adset_details.get('lifetime_budget', 0)
                budget_remaining = adset_details.get('budget_remaining', 0)
                start_time = adset_details.get('start_time')
                end_time = adset_details.get('end_time')
                status = adset_details.get('status')

                all_data.append({
                    'Ad Account Name': account_name,
                    'Campaign Name': campaign_name,
                    'Campaign ID': campaign_id,
                    'Ad Set Name': adset_name,
                    'Ad Set ID': adset_id,
                    'Ad Name': ad_name,
                    'Date Start': adset.get('date_start'),
                    'Date Stop': adset.get('date_stop'),
                    'Date': adset.get('date_start'), 
                    'Start Date': start_time,
                    'End Date': end_time,
                    'Budget': budget,
                    'Budget Remaining': budget_remaining,
                    'Amount Spent': spend,
                    'Impressions': impressions,
                    'Reach': reach,
                    'Link Clicks': actions.get('link_click', '0'),
                    'Post Engagements': actions.get('post_engagement', '0'),
                    'Post Shares': actions.get('post', '0'),
                    'Post Reactions': actions.get('post_reaction', '0'),
                    'Post Comments': actions.get('comment', '0'),
                    'Post Saves': actions.get('onsite_conversion.post_save', '0'),
                    '3-second Video Plays': actions.get('video_view', '0'),
                    'Status': status,
                    'Platform': platform,
                    'Objective': objective
                })
            except Exception as e:
                print(f"Error processing adset {adset.get('adset_id')} for account {ad_account_id} named {account_name}. Error: {str(e)}")

    if all_data:
        return pd.DataFrame(all_data)
    else:
        return pd.DataFrame()

# Fetch Tiktok Data
def fetch_tiktok_report(platforms, start_date=None, end_date=None, chunk_days=1):
    load_dotenv()
    access_token = os.getenv("TIKTOK_ACCESS_TOKEN")
    app_id = os.getenv("TIKTOK_APP_ID")
    app_secret = os.getenv("TIKTOK_SECRET")

    if not access_token or not app_id or not app_secret:
        raise ValueError("TikTok credentials are missing")

    base_url = 'https://business-api.tiktok.com/open_api/v1.2/'

    if end_date is None:
        end_date = datetime.now() - timedelta(days=1)
    if start_date is None:
        start_date = end_date

    def request_with_retry(url, headers, params=None, max_retries=3):
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=headers, params=params, timeout=30)
                if response.status_code >= 500:
                    time.sleep(2 ** attempt)
                    continue
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)

    
    def get_campaigns(base_url, headers, advertiser_id):
        """Get all campaigns for an advertiser with pagination."""
        page = 1
        campaigns = []
        while True:
            url = f"{base_url}campaign/get/?advertiser_id={advertiser_id}&page_size=1000&page={page}"
            try:
                resp = request_with_retry(url, headers)
                data = resp.json()
                campaigns.extend(data.get('data', {}).get('list', []))
                if not data.get('data', {}).get('page_info', {}).get('has_more'):
                    break
                page += 1
            except Exception as e:
                print(f"Network error getting campaigns for advertiser {advertiser_id}: {str(e)}")
                break
        return {'data': {'list': campaigns}}

    def get_adgroups(base_url, headers, advertiser_id, campaign_id):
        """Get ad groups for a campaign with pagination."""
        page = 1
        adgroups = []
        while True:
            url = f"{base_url}adgroup/get/?advertiser_id={advertiser_id}&filtering={{\"campaign_ids\":[\"{campaign_id}\"]}}&page_size=1000&page={page}"
            try:
                resp = request_with_retry(url, headers)
                data = resp.json()
                adgroups.extend(data.get('data', {}).get('list', []))
                if not data.get('data', {}).get('page_info', {}).get('has_more'):
                    break
                page += 1
            except Exception as e:
                print(f"Network error getting ad groups for campaign {campaign_id} in advertiser {advertiser_id}: {str(e)}")
                break
        return {'data': {'list': adgroups}}

    def get_ad_metrics(base_url, headers, advertiser_id, campaign_id, start_date, end_date):
        """Get detailed metrics for ads for a date range with pagination."""
        metrics_list = ["spend", "ad_name", "adgroup_name", "impressions", "reach", "clicks", "ctr",
                        "video_watched_2s", "campaign_budget", "shares", "likes", "comments",
                        "follows", "profile_visits"]
        filters = json.dumps([{"field_name": "campaign_ids", "filter_type": "IN", "filter_value": f'["{campaign_id}"]'}])
        page = 1
        metrics = []
        while True:
            url = (
                f"{base_url}reports/integrated/get/?advertiser_id={advertiser_id}&service_type=AUCTION&report_type=BASIC"
                f"&data_level=AUCTION_AD&dimensions={json.dumps(['ad_id'])}&metrics={json.dumps(metrics_list)}"
                f"&start_date={start_date}&end_date={end_date}"
                f"&order_field=impressions&page={page}&page_size=1000&filters={filters}"
            )
            try:
                resp = request_with_retry(url, headers)
                data = resp.json()
                metrics.extend(data.get('data', {}).get('list', []))
                if not data.get('data', {}).get('page_info', {}).get('has_more'):
                    break
                page += 1
            except Exception as e:
                print(f"Network error getting ad metrics for campaign {campaign_id} in advertiser {advertiser_id}: {str(e)}")
                break
        return {'data': {'list': metrics}}

    headers = {"Access-Token": access_token}
    all_data = []
    # Process each TikTok advertiser ID
    for advertiser_id, account_name in platforms.get('tiktok', []):
        campaigns = get_campaigns(base_url, headers, advertiser_id)

        for campaign in campaigns.get('data', {}).get('list', []):
            campaign_id = campaign['campaign_id']
            campaign_name = campaign['campaign_name']
            current_start = start_date
            while current_start <= end_date:
                chunk_end = min(current_start + timedelta(days=chunk_days-1), end_date)
                date_str_start = current_start.strftime('%Y-%m-%d')
                date_str_end = chunk_end.strftime('%Y-%m-%d')
                metrics = get_ad_metrics(base_url, headers, advertiser_id, campaign_id, date_str_start, date_str_end)
                adgroups_response = get_adgroups(base_url, headers, advertiser_id, campaign_id)

                adgroups_dict = {
                    adgroup['adgroup_name']: {
                        'budget': adgroup['budget'],
                        'create_time': adgroup['create_time'],
                        'schedule_start_time': adgroup['schedule_start_time'],
                        'schedule_end_time': adgroup['schedule_end_time']
                    }
                    for adgroup in adgroups_response['data']['list']
                }

                if 'data' in metrics and 'list' in metrics['data']:
                    for metric in metrics['data']['list']:
                        adgroup_name = metric['metrics']['adgroup_name']
                        adgroup_info = adgroups_dict.get(adgroup_name, {})

                        all_data.append({
                            'Ad Account Name' : account_name,
                            'Campaign Name': campaign_name,
                            'Ad Group Name': adgroup_name,
                            'Ad Group Budget': adgroup_info.get('budget', ''),
                            'Create Time': adgroup_info.get('create_time', ''),
                            'Schedule Start Time': adgroup_info.get('schedule_start_time', ''),
                            'Schedule End Time': adgroup_info.get('schedule_end_time', ''),
                            'Date': date_str_start,
                            'Ad Name': metric['metrics']['ad_name'],
                            'Impressions': metric['metrics']['impressions'],
                            'Reach': metric['metrics']['reach'],
                            'Clicks': metric['metrics']['clicks'],
                            'CTR': metric['metrics']['ctr'],
                            'Video Views (2s)': metric['metrics']['video_watched_2s'],
                            'Campaign Budget': metric['metrics']['campaign_budget'],
                            'Shares': metric['metrics']['shares'],
                            'Likes': metric['metrics']['likes'],
                            'Comments': metric['metrics']['comments'],
                            'Follows': metric['metrics']['follows'],
                            'Profile Visits': metric['metrics']['profile_visits'],
                            'Spend': metric['metrics']['spend'],
                            'Objective': campaign.get('objective', 'N/A')
                        })

                current_start = chunk_end + timedelta(days=1)

    if all_data:
        return pd.DataFrame(all_data)
    else:
        print(f"No data found for provided TikTok advertiser IDs ")
        return pd.DataFrame()

def fetch_linkedin_report(platforms):

    load_dotenv()
    ACCESS_TOKEN = os.getenv("LINKEDIN_ACCESS_TOKEN")
    if not ACCESS_TOKEN:
        raise ValueError("LinkedIn credentials are missing")
    BASE_URL = "https://api.linkedin.com/v2"
    HEADERS = {
        "Linkedin-Version": "202410",
        "Authorization": f"Bearer {ACCESS_TOKEN}"
    }

    def convert_to_date(timestamp):
        return datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d') if timestamp else None

    def get_campaign_groups(account_id):
        url = f"{BASE_URL}/adCampaignGroupsV2?q=search&search.status.values[0]=ACTIVE&search.account.values[0]=urn:li:sponsoredAccount:{account_id}"
        r = requests.get(url, headers=HEADERS)
        r.raise_for_status()
        return [
            {
                "name": g["name"],
                "id": g["id"],
                "start_date": convert_to_date(g["runSchedule"].get("start")),
                "end_date": convert_to_date(g["runSchedule"].get("end"))
            }
            for g in r.json().get("elements", [])
        ]

    def get_campaigns_in_group(group_id):
        url = f"{BASE_URL}/adCampaignsV2?q=search&search.campaignGroup.values[0]=urn:li:sponsoredCampaignGroup:{group_id}"
        r = requests.get(url, headers=HEADERS)
        r.raise_for_status()
        return [
            {
                "name": c.get("name", "Unnamed Campaign"),
                "objectiveType": c.get("objectiveType", "Unknown"),
                "status": c.get("status", "Unknown"),
                "id": c["id"]
            }
            for c in r.json().get("elements", [])
        ]

    def get_ad_insights(campaign_id):
        fields = ",".join([
            "impressions", "clicks", "follows", "reactions", "shares", "totalEngagements",
            "videoViews", "costInUsd", "comments", "viralClicks", "viralComments", "viralFollows",
            "viralReactions", "viralShares", "landingPageClicks", "pivotValues", "otherEngagements"
        ])
        url = (
            f"{BASE_URL}/adAnalyticsV2?q=analytics&pivot=CREATIVE&"
            f"dateRange.start.day=1&dateRange.start.month=1&dateRange.start.year=2021&"
            f"timeGranularity=ALL&campaigns=urn:li:sponsoredCampaign:{campaign_id}&fields={fields}"
        )
        r = requests.get(url, headers=HEADERS)
        r.raise_for_status()

        return [
            {
                "ad_creative_id": ad.get("pivotValues", [""])[0].split(":")[-1],
                "impressions": ad.get("impressions", 0),
                "clicks": ad.get("clicks", 0),
                "follows": ad.get("follows", 0),
                "reactions": ad.get("reactions", 0),
                "shares": ad.get("shares", 0),
                "totalEngagements": ad.get("totalEngagements", 0),
                "videoViews": ad.get("videoViews", 0),
                "viralClicks": ad.get("viralClicks", 0),
                "viralComments": ad.get("viralComments", 0),
                "viralFollows": ad.get("viralFollows", 0),
                "viralReactions": ad.get("viralReactions", 0),
                "viralShares": ad.get("viralShares", 0),
                "costInUsd": ad.get("costInUsd", 0.0),
                "comments": ad.get("comments", 0),
                "landingPageClicks": ad.get("landingPageClicks", 0),
                "Other clicks": ad.get("otherEngagements", 0),
            }
            for ad in r.json().get("elements", [])
        ]

    def get_ad_creative_name(ad_creative_id):
        url = f"{BASE_URL}/adCreativesV2/{ad_creative_id}"
        try:
            r = requests.get(url, headers=HEADERS)
            r.raise_for_status()
            ref = r.json().get("reference", "")
            if not ref:
                return "Ad CANCELED"
            return "UGC Post" if "ugcPost" in ref else "Sponsored Share"
        except requests.exceptions.HTTPError as e:
            return "AD Paused" if e.response and e.response.status_code == 403 else f"Error: {e}"

    rows = []

    for account_id, account_name in platforms.get("linkedin", []):
        try:
            for group in get_campaign_groups(account_id):
                for campaign in get_campaigns_in_group(group["id"]):
                    for ad in get_ad_insights(campaign["id"]):
                        # Skip ads with all zero metrics
                        if all(ad.get(k, 0) in [0, 0.0] for k in ["impressions", "clicks", "videoViews", "reactions", "shares"]):
                            continue

                        rows.append({
                            "Ad Account Name": account_name,
                            "Campaign Group": group["name"],
                            "Start Date": group["start_date"],
                            "End Date": group["end_date"],
                            "objectiveType": campaign["objectiveType"],
                            "Campaign Name": campaign["name"],
                            "Campaign Status": campaign["status"],
                            "Ad Creative Name": get_ad_creative_name(ad["ad_creative_id"]),
                            "Impressions": ad["impressions"],
                            "Clicks": ad["clicks"],
                            "Follows": ad["follows"],
                            "Reactions": ad["reactions"],
                            "Shares": ad["shares"],
                            "Total Engagements": ad["totalEngagements"],
                            "Views": ad["videoViews"],
                            "Cost in USD": ad["costInUsd"],
                            "Comments": ad["comments"],
                            "Landing Page Clicks": ad["landingPageClicks"],
                            "Total Social Actions": sum([
                                ad.get("viralReactions", 0), ad.get("Other clicks", 0),
                                ad.get("reactions", 0), ad.get("comments", 0),
                                ad.get("shares", 0), ad.get("follows", 0)
                            ])
                        })
        except Exception as e:
            print(f"⚠ Error processing LinkedIn account {account_name}: {e}")

    if rows:
        return pd.DataFrame(rows)
    else:
        print("⚠ No Linkedin Data Available")
        return pd.DataFrame()

def fetch_youtube_ads_report(platforms):
    load_dotenv()

    config = {
        "developer_token": os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN"),
        "client_id": os.getenv("GOOGLE_ADS_CLIENT_ID"),
        "client_secret": os.getenv("GOOGLE_ADS_CLIENT_SECRET"),
        "refresh_token": os.getenv("GOOGLE_ADS_REFRESH_TOKEN"),
        "use_proto_plus": True
    }

    if not all([config["developer_token"], config["client_id"], config["client_secret"], config["refresh_token"]]):
        raise ValueError("Google Ads credentials are missing")

    client = GoogleAdsClient.load_from_dict(config)

    yesterday = datetime.now() - timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")

    query = f'''
        SELECT
          campaign.name,
          ad_group.name,
          ad_group_ad.ad.name,
          metrics.impressions,
          metrics.clicks,
          metrics.ctr,
          metrics.video_views,
          metrics.cost_micros
        FROM ad_group_ad
        WHERE segments.date = '{date_str}'
          AND campaign.advertising_channel_type = 'VIDEO'
        LIMIT 1000
    '''

    all_data = []

    for customer_id, account_name in platforms.get("youtube", []):
        try:
            print(f"Fetching YouTube data for account: {account_name} ({customer_id})")
            ga_service = client.get_service("GoogleAdsService")
            response = ga_service.search(customer_id=customer_id, query=query)
            for row in response:
                ad_name = row.ad_group_ad.ad.name or "Unnamed"                
                spend = row.metrics.cost_micros or 0

                all_data.append({
                    'Ad Account Name': account_name,
                    'Campaign Name': row.campaign.name,
                    'Ad Group Name': row.ad_group.name,
                    'Ad Name': ad_name,
                    'Date': date_str,
                    'Impressions': row.metrics.impressions,
                    'Clicks': row.metrics.clicks,
                    'Video Views': row.metrics.video_views,
                    'Spend': spend / 1e6  # Convert micros to standard currency unit
                })

        except GoogleAdsException as ex:
            print(f"API error for YouTube account {account_name} ({customer_id}): {ex}")

    return pd.DataFrame(all_data) if all_data else pd.DataFrame()
