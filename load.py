from dotenv import load_dotenv
import os
import pandas as pd
import time
from mapping import *
from drive_monitor import *
from extract import fetch_facebook_report, fetch_tiktok_report, fetch_linkedin_report,fetch_youtube_ads_report
from transform import preprocess_insta, preprocess_tiktok, preprocess_linkedin, preprocess_youtube
from urllib.parse import quote_plus
from sqlalchemy import text, Table, Column, MetaData, create_engine, types, inspect
from sqlalchemy.types import String, Integer, Float, Date
from sqlalchemy.exc import OperationalError
from datetime import datetime, timedelta
from mapping import get_industry_for_client
import pymysql
from app_logging import ETLLogger


load_dotenv(dotenv_path="keys.env")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
CHUNK_DAYS = int(os.getenv("CHUNK_DAYS", "1"))

logger = ETLLogger(host=host, user=user, password=password)

def main():
    print("ETL pipeline starting...")
    run_id = f"load-job-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    start_time = datetime.now()
    success = True
    error_message = None

    try:
        load_dotenv()
        fb_access_token = os.getenv("FB_ACCESS_TOKEN")
        tiktok_access_token = os.getenv("TIKTOK_ACCESS_TOKEN")
        tiktok_app_id = os.getenv("TIKTOK_APP_ID")
        tiktok_secret = os.getenv("TIKTOK_SECRET")
        linkedin_access_token = os.getenv("LINKEDIN_ACCESS_TOKEN")
        ga_developer_token = os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN")
        ga_client_id = os.getenv("GOOGLE_ADS_CLIENT_ID")
        ga_client_secret = os.getenv("GOOGLE_ADS_CLIENT_SECRET")
        ga_refresh_token = os.getenv("GOOGLE_ADS_REFRESH_TOKEN")

        print("Generating mapping...")
        mapping = generate_mapping(fb_access_token, tiktok_access_token, tiktok_app_id, tiktok_secret, linkedin_access_token,ga_developer_token, ga_client_id, ga_client_secret, ga_refresh_token)
        print("Mapping generated successfully.")

        for i, j in mapping.items():
            non_empty_platforms = [platform for platform, accounts in j.items() if accounts]
            platforms_str = ' and '.join(non_empty_platforms)

            if non_empty_platforms:
                print(f"Fetching data for advertiser {i}'s {platforms_str} account(s)...")
            else:
                print(f"No active accounts found for advertiser {i}.")
                continue

            # Initialize dataframes for each platform
            facebook_data = None
            tiktok_data = None
            linkedin_data = None
            youtube_data = None 

            # Wrap each API fetch in timing and logging
            if 'facebook' in non_empty_platforms:
                start = time.time()
                try:
                    print(f"Calling Facebook API for {i}...")
                    facebook_data = fetch_facebook_report(j)
                    print("Facebook API success.")
                    facebook_data = preprocess_insta(facebook_data)
                    duration = round(time.time() - start, 2)
                    payload_size = facebook_data.memory_usage(deep=True).sum() if facebook_data is not None else 0
                    logger.log_api_call("Facebook", i, "facebook_endpoint", 200, True, duration, payload_size)
                except Exception as e:
                    duration = round(time.time() - start, 2)
                    logger.log_api_call("Facebook", i, "facebook_endpoint", 500, False, duration, 0, str(e))

            if 'tiktok' in non_empty_platforms:
                start = time.time()
                try:
                    print(f"Calling TikTok API for {i}...")
                    end_date = datetime.now() - timedelta(days=1)
                    start_date = end_date - timedelta(days=CHUNK_DAYS - 1)
                    tiktok_data = fetch_tiktok_report(j, start_date=start_date, end_date=end_date, chunk_days=CHUNK_DAYS)
                    print("TikTok API success.")
                   # print(tiktok_data.head())
                    tiktok_data = preprocess_tiktok(tiktok_data)
                   # print(tiktok_data.head())
                    duration = round(time.time() - start, 2)
                    payload_size = tiktok_data.memory_usage(deep=True).sum() if tiktok_data is not None else 0
                    logger.log_api_call("TikTok", i, "tiktok_endpoint", 200, True, duration, payload_size)
                except Exception as e:
                    duration = round(time.time() - start, 2)
                    logger.log_api_call("TikTok", i, "tiktok_endpoint", 500, False, duration, 0, str(e))

            if 'linkedin' in non_empty_platforms:
                start = time.time()
                try:
                    print(f"Calling LinkedIn API for {i}...")
                    linkedin_data = fetch_linkedin_report(j)
                    print("LinkedIn API success.")
                    linkedin_data = preprocess_linkedin(linkedin_data)
                    duration = round(time.time() - start, 2)
                    payload_size = linkedin_data.memory_usage(deep=True).sum() if linkedin_data is not None else 0
                    logger.log_api_call("LinkedIn", i, "linkedin_endpoint", 200, True, duration, payload_size)
                except Exception as e:
                    duration = round(time.time() - start, 2)
                    logger.log_api_call("LinkedIn", i, "linkedin_endpoint", 500, False, duration, 0, str(e))

            if 'youtube' in non_empty_platforms:
                start = time.time()
                try:
                    print(f"Calling YouTube API for {i}...")
                    youtube_data = fetch_youtube_ads_report(j)
                    print("YouTube API success.")
                    youtube_data = preprocess_youtube(youtube_data)
                    duration = round(time.time() - start, 2)
                    payload_size = youtube_data.memory_usage(deep=True).sum() if youtube_data is not None else 0
                    logger.log_api_call("YouTube", i, "youtube_endpoint", 200, True, duration, payload_size)
                except Exception as e:
                    duration = round(time.time() - start, 2)
                    logger.log_api_call("YouTube", i, "youtube_endpoint", 500, False, duration, 0, str(e))

            dfs_to_concat = [df for df in [facebook_data, tiktok_data, linkedin_data, youtube_data]
                 if df is not None and not df.empty and not df.isna().all().all()]

            if not dfs_to_concat:
                print(f"No data available to save for {i}")
                continue
            # Only runs if there's something to upload
            data_frames = pd.concat(dfs_to_concat, axis=0)
            # Clean and convert datetime fields
            for col in ['Start Date', 'End Date', 'Date']:
                if col in data_frames.columns:
                    data_frames[col] = pd.to_datetime(data_frames[col], errors='coerce').dt.date
                    #data_frames[col] = data_frames[col].dt.date  # optional: use if table expects DATE only

            file_name = f"{i}"
            db = get_db_name(file_name)

            date_columns = ['Start Date', 'End Date', 'Date']
            integer_cols = ['Post Saves', 'Reach', 'Follows']
            dtype_dict = {col: types.Date for col in date_columns}
            dtype_dict.update({col: types.Integer for col in integer_cols})

            base_engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/")
            ensure_database_exists(base_engine, db)

            db_engine_specific = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{db}")
            create_table_if_not_exists(db_engine_specific, f"{file_name}_Paid_Data")

            expected_columns = {
                'Ad Account Name', 'Campaign Name', 'Ad Set Name', 'Start Date', 'End Date', 'Date',
                'Ad Name', 'Spent', 'Impressions', 'Reach', 'Clicks', 'Post Engagements', 'Post Shares',
                'Post Reactions', 'Post Comments', 'Post Saves', '3-second Video Plays', 'Eng Minus Views',
                'Platform', 'Round', 'Audience', 'Influencer', 'Objective1', 'Objective', 'Placement',
                'Destination', 'Follows'
            }

            data_frames = data_frames[[col for col in data_frames.columns if col in expected_columns]]

            if not data_frames.empty:
                data_frames.to_sql(
                    f"{file_name}_Paid_Data",
                    db_engine_specific,
                    index=False,
                    if_exists='append',
                    dtype=dtype_dict,
                )
                route_data_to_industry_databases(data_frames, i)
                logger.log_rows_appended(
                    run_id,
                    i,
                    f"{file_name}_Paid_Data",
                    len(data_frames),
                )
                #data_frames.to_csv(f"{file_name}_Paid_Data.csv", index=False)
            else:
                print(f"⚠ No data to insert for table {file_name}_Paid_Data")

        drive_files = monitor_drive_folder(run_id, logger)
        if drive_files:
            print(f"Ingested {len(drive_files)} file(s) from Drive.")

    except Exception as e:
        success = False
        error_message = str(e)
        print("ETL Pipeline failed:", error_message)
    finally:
        end_time = datetime.now()
        logger.log_pipeline_run(run_id, start_time, end_time, success, error_message, gcp_job_url="https://console.cloud.google.com/run/")

def create_table_if_not_exists(engine, table_name):
    inspector = inspect(engine)
    if table_name in inspector.get_table_names():
        return

    metadata = MetaData()
    Table(
        table_name, metadata,
        Column('Ad Account Name', String(255)),
        Column('Campaign Name', String(255)),
        Column('Ad Set Name', String(255)),
        Column('Start Date', Date),
        Column('End Date', Date),
        Column('Date', Date),
        Column('Ad Name', String(255)),
        Column('Spent', Float),
        Column('Impressions', Integer),
        Column('Reach', Integer),
        Column('Clicks', Integer),
        Column('Post Engagements', Integer),
        Column('Post Shares', Integer),
        Column('Post Saves', Integer),
        Column('Post Reactions', Integer),
        Column('Post Comments', Integer),
        Column('3-second Video Plays', Integer),
        Column('Eng Minus Views', Integer),
        Column('Platform', String(100)),
        Column('Round', String(100)),
        Column('Audience', String(255)),
        Column('Influencer', String(255)),
        Column('Objective1', String(255)),
        Column('Objective', String(255)),
        Column('Placement', String(255)),
        Column('Destination', String(255)),
        Column('Follows', Integer),
    )
    metadata.create_all(engine)
    print(f"Table '{table_name}' created.")

def ensure_database_exists(base_engine, db_name):
    with base_engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{db_name}`"))

def route_data_to_industry_databases(df, client_name):
    industry = get_industry_for_client(client_name)
    industry_slug = industry.lower().replace(" ", "_")
    industry_db_name = f"{industry_slug}_industry_db"

    server_uri = f"mysql+pymysql://{user}:{password}@{host}"
    server_engine = create_engine(server_uri)

    try:
        with server_engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {industry_db_name}"))
    except OperationalError as e:
        print(f"Error creating databases: {e}")
        return

    #client_engine = create_engine(f"{server_uri}/{client_name}")
    industry_engine = create_engine(f"{server_uri}/{industry_db_name}")

    try:
        df = df.drop(columns=["Follows"], errors='ignore')
       #df.to_sql("client_data", con=client_engine, if_exists="append", index=False)
        df.to_sql("industry_data", con=industry_engine, if_exists="append", index=False)
    except Exception as e:
        print(f"Error writing data to databases: {e}")

if __name__ == "__main__":
    main()
