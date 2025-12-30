import os
import pandas as pd
from sqlalchemy import create_engine
from mapping import *
from dotenv import load_dotenv

# === CONFIGURE THESE ===
# Database connection info from environment variables
load_dotenv()
db_type = 'mysql'  # or 'postgresql'
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT', '3306')  # or 5432 for PostgreSQL
database = os.getenv('DB_NAME', 'jbl')  # Default database name
table_name = 'JBL (Harman) - Praytell_Paid_Data'
csv_file = 'JBL_everything.csv'
# =========================
# Load your file
df = pd.read_csv(csv_file)
# --- 1. Rename Columns ---
rename_map = {
    'Campaign name': 'Campaign Name',
    'Ad Set Name': 'Ad Set Name',
    'Ad name': 'Ad Name',
    'Platform': 'Platform',
    'Day': 'Date',
    'Reach': 'Reach',
    'Impressions': 'Impressions',
    'Spent': 'Spent',
    'Starts': 'Start Date',
    'Ends': 'End Date',
    'Clicks': 'Clicks',
    'Eng Minus Views': 'Eng Minus Views',
    'Post engagements': 'Post Engagements',
    'Post comments': 'Post Comments',
    'Post reactions': 'Post Reactions',
    'Post saves': 'Post Saves',
    'Post shares': 'Post Shares',
    '3-second Video plays': '3-second Video Plays',
    'Page engagement': 'Page Engagement',
    'Destination': 'Destination',
    'Influencer': 'Influencer',
    'Objective': 'Objective',
    'Objective1': 'Objective1',
    'Audience': 'Audience',
    'Placement': 'Placement',
    'Follows': 'Follows'
}

df.rename(columns=rename_map, inplace=True)

# --- 2. Fix Start and End Dates (remove second date if present) ---
for col in ['Start Date', 'End Date']:
    if col in df.columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.split(',', n=1)
            .str[0]
            .str.strip()
            .replace(["Unknown", ""], pd.NA)
        )
        df[col] = pd.to_datetime(df[col], errors='coerce')

# --- 3. Convert Date column to datetime ---
if 'Date' in df.columns:
    df['Date'] = (
        df['Date']
        .astype(str)
        .str.strip()
        .replace(["Unknown", ""], pd.NA)
    )
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
# Define only the columns that exist in the MySQL table
expected_columns = [
    'Campaign Name', 'Ad Set Name', 'Ad Name', 'Platform', 'Date',
    'Reach', 'Impressions', 'Spent', 'Start Date', 'End Date',
    'Clicks', 'Eng Minus Views', 'Post Engagements', 'Post Comments',
    'Post Reactions', 'Post Saves', 'Post Shares', '3-second Video Plays',
    'Destination', 'Influencer', 'Objective', 'Objective1',
    'Audience', 'Placement', 'Follows'
]

# Keep only those columns
df = df[[col for col in expected_columns if col in df.columns]]
# Create connection
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')

# Upload to MySQL
df.to_sql(table_name, con=engine, if_exists='append', index=False)

print(f"Successfully uploaded {len(df)} rows to `{table_name}` in `{database}`.")
