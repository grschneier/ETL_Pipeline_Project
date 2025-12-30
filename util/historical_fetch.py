import argparse
import json
import os
from datetime import datetime, timedelta
from typing import Dict, Any
import time

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import types

from extract import fetch_tiktok_report
from transform import preprocess_insta, preprocess_tiktok, preprocess_linkedin, preprocess_youtube
from load import create_table_if_not_exists, ensure_database_exists
from mapping import get_db_name

# Utilities ---------------------------------------------------------------

def load_mapping() -> Dict[str, Any]:
    with open('map.json', 'r') as f:
        return json.load(f)


def retry_request(session, url, params, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = session.get(url, params=params, timeout=30)
            if response.status_code >= 500:
                raise Exception(f"Server error {response.status_code}")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)


# Facebook historical fetch ----------------------------------------------

def fetch_facebook_report_range(platforms: Dict[str, Any], start_date: datetime, end_date: datetime,
                                chunk_days: int = 7, max_retries: int = 3) -> pd.DataFrame:
    from facebook_business.api import FacebookAdsApi
    import requests
    load_dotenv()
    access_token = os.getenv("FB_ACCESS_TOKEN")
    app_id = os.getenv("FB_APP_ID")
    app_secret = os.getenv("FB_APP_SECRET")
    FacebookAdsApi.init(app_id, app_secret, access_token)

    all_rows = []
    session = requests.Session()

    def process_actions(actions):
        action_dict = {}
        for action in actions:
            t = action.get('action_type')
            v = action.get('value', '0')
            if t:
                action_dict[t] = v
        return action_dict

    for ad_account_id, account_name in platforms.get('facebook', []):
        current_start = start_date
        while current_start <= end_date:
            chunk_end = min(current_start + timedelta(days=chunk_days - 1), end_date)
            params = {
                'fields': 'campaign_id,objective,adset_id,ad_name,adset_name,campaign_name,impressions,spend,reach,ad_id,actions,date_start,date_stop',
                'time_range': json.dumps({'since': current_start.strftime('%Y-%m-%d'),
                                         'until': chunk_end.strftime('%Y-%m-%d')}),
                'time_increment': 1,
                'limit': 5000,
                'level': 'ad',
                'breakdowns': 'publisher_platform',
                'access_token': access_token
            }
            next_page = None
            while True:
                if next_page:
                    params['after'] = next_page
                url = f"https://graph.facebook.com/v21.0/{ad_account_id}/insights"
                for attempt in range(max_retries):
                    try:
                        resp = session.get(url, params=params, timeout=30)
                        resp.raise_for_status()
                        data = resp.json()
                        break
                    except requests.HTTPError as http_err:
                        try:
                            error_content = resp.json()
                        except ValueError:
                            error_content = resp.text
                        print(f"Facebook API error response: {error_content}")
                        if attempt == max_retries - 1:
                            raise
                        time.sleep(2 ** attempt)
                    except Exception:
                        if attempt == max_retries - 1:
                            raise
                        time.sleep(2 ** attempt)
                for ad in data.get('data', []):
                    actions = process_actions(ad.get('actions', []))
                    all_rows.append({
                        'Ad Account Name': account_name,
                        'Campaign Name': ad.get('campaign_name'),
                        'Campaign ID': ad.get('campaign_id'),
                        'Ad Set Name': ad.get('adset_name'),
                        'Ad Set ID': ad.get('adset_id'),
                        'Ad Name': ad.get('ad_name'),
                        'Date Start': ad.get('date_start'),
                        'Date Stop': ad.get('date_stop'),
                        'Date': ad.get('date_start'),
                        'Amount Spent': ad.get('spend'),
                        'Impressions': ad.get('impressions'),
                        'Reach': ad.get('reach'),
                        'Link Clicks': actions.get('link_click', '0'),
                        'Post Engagements': actions.get('post_engagement', '0'),
                        'Post Shares': actions.get('post', '0'),
                        'Post Reactions': actions.get('post_reaction', '0'),
                        'Post Comments': actions.get('comment', '0'),
                        'Post Saves': actions.get('onsite_conversion.post_save', '0'),
                        '3-second Video Plays': actions.get('video_view', '0'),
                        'Platform': ad.get('publisher_platform'),
                        'Objective': ad.get('objective')
                    })
                next_page = data.get('paging', {}).get('cursors', {}).get('after')
                if not next_page:
                    break
            current_start = chunk_end + timedelta(days=1)
    return pd.DataFrame(all_rows)

# LinkedIn historical -----------------------------------------------------

def fetch_linkedin_report_range(platforms: Dict[str, Any], start_date: datetime, end_date: datetime) -> pd.DataFrame:
    import requests
    load_dotenv()
    ACCESS_TOKEN = os.getenv("LINKEDIN_ACCESS_TOKEN")
    BASE_URL = "https://api.linkedin.com/v2"
    HEADERS = {
        "Linkedin-Version": "202410",
        "Authorization": f"Bearer {ACCESS_TOKEN}"
    }

    rows = []
    for account_id, account_name in platforms.get("linkedin", []):
        url_campaign_groups = f"{BASE_URL}/adCampaignGroupsV2?q=search&search.status.values[0]=ACTIVE&search.account.values[0]=urn:li:sponsoredAccount:{account_id}"
        groups = requests.get(url_campaign_groups, headers=HEADERS).json().get('elements', [])
        for g in groups:
            group_id = g['id']
            url_campaigns = f"{BASE_URL}/adCampaignsV2?q=search&search.campaignGroup.values[0]=urn:li:sponsoredCampaignGroup:{group_id}"
            campaigns = requests.get(url_campaigns, headers=HEADERS).json().get('elements', [])
            for c in campaigns:
                fields = ",".join([
                    "impressions","clicks","follows","reactions","shares","totalEngagements",
                    "videoViews","costInUsd","comments","pivotValues","landingPageClicks"
                ])
                url_insights = (
                    f"{BASE_URL}/adAnalyticsV2?q=analytics&pivot=CREATIVE&timeGranularity=DAILY&dateRange.start.year={start_date.year}&dateRange.start.month={start_date.month}&dateRange.start.day={start_date.day}"
                    f"&dateRange.end.year={end_date.year}&dateRange.end.month={end_date.month}&dateRange.end.day={end_date.day}"
                    f"&campaigns=urn:li:sponsoredCampaign:{c['id']}&fields={fields}"
                )
                data = requests.get(url_insights, headers=HEADERS).json()
                for ad in data.get('elements', []):
                    rows.append({
                        'Ad Account Name': account_name,
                        'Campaign Name': c.get('name'),
                        'objectiveType': c.get('objectiveType'),
                        'Impressions': ad.get('impressions',0),
                        'Clicks': ad.get('clicks',0),
                        'Follows': ad.get('follows',0),
                        'Reactions': ad.get('reactions',0),
                        'Shares': ad.get('shares',0),
                        'Total Engagements': ad.get('totalEngagements',0),
                        'Views': ad.get('videoViews',0),
                        'Cost in USD': ad.get('costInUsd',0.0),
                        'Comments': ad.get('comments',0),
                        'Landing Page Clicks': ad.get('landingPageClicks',0)
                    })
    return pd.DataFrame(rows)

# YouTube historical ------------------------------------------------------

def fetch_youtube_ads_report_range(platforms: Dict[str, Any], start_date: datetime, end_date: datetime,
                                   chunk_days: int = 7) -> pd.DataFrame:
    from google.ads.googleads.client import GoogleAdsClient
    from google.ads.googleads.errors import GoogleAdsException
    load_dotenv()

    config = {
        "developer_token": os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN"),
        "client_id": os.getenv("GOOGLE_ADS_CLIENT_ID"),
        "client_secret": os.getenv("GOOGLE_ADS_CLIENT_SECRET"),
        "refresh_token": os.getenv("GOOGLE_ADS_REFRESH_TOKEN"),
        "use_proto_plus": True
    }
    client = GoogleAdsClient.load_from_dict(config)
    all_rows = []
    for customer_id, account_name in platforms.get("youtube", []):
        ga_service = client.get_service("GoogleAdsService")
        current_start = start_date
        while current_start <= end_date:
            chunk_end = min(current_start + timedelta(days=chunk_days - 1), end_date)
            query = f"""
                SELECT campaign.name, ad_group.name, ad_group_ad.ad.name,
                       metrics.impressions, metrics.clicks, metrics.video_views,
                       metrics.cost_micros
                FROM ad_group_ad
                WHERE segments.date BETWEEN '{current_start.strftime('%Y-%m-%d')}' AND '{chunk_end.strftime('%Y-%m-%d')}'
                  AND campaign.advertising_channel_type = 'VIDEO'
            """
            try:
                response = ga_service.search(customer_id=customer_id, query=query)
                for row in response:
                    spend = row.metrics.cost_micros or 0
                    all_rows.append({
                        'Ad Account Name': account_name,
                        'Campaign Name': row.campaign.name,
                        'Ad Set Name': row.ad_group.name,
                        'Ad Name': row.ad_group_ad.ad.name or 'Unnamed',
                        'Date': current_start.strftime('%Y-%m-%d'),
                        'Impressions': row.metrics.impressions,
                        'Clicks': row.metrics.clicks,
                        'Video Views': row.metrics.video_views,
                        'Spend': spend / 1e6
                    })
            except GoogleAdsException as ex:
                print(f"API error for account {account_name}: {ex}")
            current_start = chunk_end + timedelta(days=1)
    return pd.DataFrame(all_rows)

# ------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Fetch historical paid media data for a single client")
    parser.add_argument("client", help="Client name as in map.json")
    parser.add_argument("--start", dest="start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", dest="end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--output", dest="output", choices=["sql", "csv"], default="csv")
    parser.add_argument("--chunk-days", dest="chunk_days", type=int, default=7, help="Days per API request")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d")

    mapping = load_mapping()
    if args.client not in mapping:
        raise ValueError(f"Client {args.client} not found in mapping")
    platforms = mapping[args.client]

    print("Fetching Facebook data...")
    fb_df = fetch_facebook_report_range(platforms, start_date, end_date, args.chunk_days)
    fb_df = preprocess_insta(fb_df) if not fb_df.empty else fb_df

    print("Fetching TikTok data...")
    tk_df = fetch_tiktok_report(platforms, start_date=start_date, end_date=end_date, chunk_days=args.chunk_days)
    tk_df = preprocess_tiktok(tk_df) if not tk_df.empty else tk_df

    print("Fetching LinkedIn data...")
    li_df = fetch_linkedin_report_range(platforms, start_date, end_date)
    li_df = preprocess_linkedin(li_df) if not li_df.empty else li_df

    print("Fetching YouTube data...")
    yt_df = fetch_youtube_ads_report_range(platforms, start_date, end_date, args.chunk_days)
    yt_df = preprocess_youtube(yt_df) if not yt_df.empty else yt_df

    dfs = [df for df in [fb_df, tk_df, li_df, yt_df] if df is not None and not df.empty]
    if not dfs:
        print("No data fetched")
        return
    result = pd.concat(dfs, ignore_index=True)

    if args.output == "csv":
        output_file = f"{args.client.replace(' ', '_')}_paid_data.csv"
        result.to_csv(output_file, index=False)
        print(f"Data written to {output_file}")
    else:
        load_dotenv()
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        db = get_db_name(args.client)
        engine_base = create_engine(f"mysql+pymysql://{user}:{password}@{host}/")
        ensure_database_exists(engine_base, db)
        engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{db}")
        table_name = f"{args.client}_Paid_Data".replace(' ', '_')
        create_table_if_not_exists(engine, table_name)
        date_columns = ['Start Date', 'End Date', 'Date']
        dtype_dict = {col: types.Date for col in date_columns if col in result.columns}
        result.to_sql(table_name, engine, if_exists='append', index=False, dtype=dtype_dict)
        print(f"Data appended to MySQL table {table_name}")

if __name__ == "__main__":
    main()
