import pickle
import os
import io
import time
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.credentials import Credentials
from sqlalchemy import create_engine, types, inspect, text
from urllib.parse import quote_plus
import pandas as pd
from google.auth.transport.requests import Request
from mapping import *
import json
from datetime import datetime
import numpy as np
import logging
from app_logging import ETLLogger
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)

# Load database credentials from environment
load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_URL_PREFIX = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}"

# Calculates total engagement metrics for social media posts across platforms

def calculate_total_engagements(df):
    """
    Calculate total engagements based on platform with robust error handling.
    """
    platform = df['platform'].str.lower()
    linkedin_cols = ['organic likes', 'organic comments', 'total shares', 'poll votes']
    result = np.zeros(len(df))
    
    # LinkedIn engagement calculation
    linkedin_mask = (platform == 'linkedin')
    if linkedin_mask.any():
        missing_cols = [col for col in linkedin_cols if col not in df.columns]
        if not missing_cols:
            result[linkedin_mask] = df.loc[linkedin_mask, linkedin_cols].fillna(0).sum(axis=1)
    
    # Instagram engagement calculation
    instagram_mask = (platform == 'instagram')
    if instagram_mask.any() and 'engagements' in df.columns:
        result[instagram_mask] = df.loc[instagram_mask, 'engagements'].fillna(0)
    
    # Facebook/Twitter engagement calculation
    fb_twitter_mask = platform.isin(['facebook', 'twitter'])
    if fb_twitter_mask.any() and 'organic interactions' in df.columns:
        result[fb_twitter_mask] = df.loc[fb_twitter_mask, 'organic interactions'].fillna(0)
    
    return pd.Series(result, index=df.index, dtype='Int64')

# Parses dates

def get_date_info(date_input):
    """
    Process date information to get formatted month and quarter.
    Accepts either a date string in the '%d-%m-%Y' format or a datetime object.
    """
    # Convert string to datetime if necessary
    if isinstance(date_input, str):
        date_obj = datetime.strptime(date_input, '%d-%m-%Y')
    elif isinstance(date_input, datetime):
        date_obj = date_input
    else:
        raise ValueError("Unsupported date format. Please provide a date string in '%d-%m-%Y' format or a datetime object.")
    
    formatted_month = date_obj.strftime('%B')
    quarter = (date_obj.month - 1) // 3 + 1
    
    return {
        'formatted_month': f"{date_obj.year}-{date_obj.month} ({formatted_month})",
        'quarter': f"Q{quarter}-{date_obj.year}"
    }

# Processing different types of google sheets into the database
# df1 is the new spreadsheet name for the data copied from the spreadsheet

def preprocess_emplifi(df, filename):
    df1 = pd.DataFrame()
    df.columns = df.columns.str.lower()
    print(df.columns)
    print(df['date'].iloc[1])
    df1['# of Posts'] = pd.Series([1] * len(df), index=df.index, dtype="Int64")
  
    df1['Published Date']=pd.to_datetime(df['date'], errors='coerce').dt.date
    #errors=coerce means that it will store data in the incorrect format as null

    df1['Platform'] = df['platform'].astype('string')
    df1['Content type'] = df['content type'].astype('string')
    df1['Media Type'] = df['media type'].astype('string')
    
    # Handle Post Copy conditionally
    if 'content' in df.columns:
        df1['Post Copy'] = df['content'].astype('string')
    else:
        df1['Post Copy'] = df['post copy'].astype('string')
    
    # Handle Permalink conditionally
    if 'view on platform' in df.columns:
        df1['Permalink'] = df['view on platform'].astype('string')
    else:
        df1['Permalink'] = df['permalink'].astype('string')
    
    if 'ao' in filename or 'angry' in filename:
        df1['Organic Interactions']= df['organic interactions'].astype('Int64')
    else:
        df1['Total Interactions'] = df['organic interactions'].astype('Int64')
    df1['Sentiment'] = df['sentiment'].astype('string')
    df1['Positive Comments'] = df['positive comments'].astype('Int64')
    df1['Negative Comments'] = df['negative comments'].astype('Int64')
    df1['Neutral Comments'] = df['neutral comments'].astype('Int64')
    df1['Total Reactions'] = df['total reactions'].astype('Int64')
    df1['Likes'] = df['organic likes'].astype('Int64')
    df1['Comments'] = df['organic comments'].astype('Int64')
    df1['Total Comments'] = df['total comments'].astype('Int64')
    df1['Shares'] = df['total shares'].astype('Int64')
    df1['Saves'] = df['saves'].astype('Int64')

    # Handle Engagements
    if 'engagements' in df.columns:
        df1['Engagements'] = df['engagements'].astype('Int64')
    else:
        df1['Engagements'] = pd.NA

    # Reactions and other interaction columns
    df1['Like Reactions'] = df['reactions - like'].astype('Int64')
    df1['Love Reactions'] = df['reactions - love'].astype('Int64')
    df1['Haha Reactions'] = df['reactions - haha'].astype('Int64')
    df1['Wow Reactions'] = df['reactions - wow'].astype('Int64')
    df1['Sad Reactions'] = df['reactions - sad'].astype('Int64')
    df1['Angry Reactions'] = df['reactions - angry'].astype('Int64')
    df1['Impressions'] = df['organic impressions'].astype('Int64')
    df1['Total Likes'] = df['total likes'].astype('Int64')
    df1['Total Story Likes'] = df['total story likes'].astype('Int64')
    df1['Total Story Comments'] = df['total story comments'].astype('Int64')
    df1['Total Story Shares'] = df['total story shares'].astype('Int64')
    df1['Post Clicks'] = df['post clicks'].astype('Int64')
    df1['Photo Views'] = df['photo views'].astype('Int64')
    df1['Link Clicks'] = df['link clicks'].astype('Int64')
    df1['Video Play'] = df['video play'].astype('Int64')
    df1['Video Views'] = df['video view count'].astype('Int64')
    df1['10-Second Views - Organic'] = df['10-second views - organic'].astype('Int64')
    df1['30-Second Views - Organic'] = df['30-second views - organic'].astype('Int64')
    df1['Completed Video Views'] = df['completed video views'].astype('Int64')
    df1['Exits'] = df['exits'].astype('Int64')
    df1['Taps Back'] = df['taps back'].astype('Int64')
    df1['Taps Forward'] = df['taps forward'].astype('Int64')
    df1['Label'] = df['labels'].astype('string')
    df1['Profile Followers'] = df['profile followers'].astype('Int64')
    
    # Apply date information functions
    df1['Month'] = df['date'].apply(lambda x: get_date_info(x)['formatted_month'] if pd.notnull(x) else pd.NA)
    df1['Quarter'] = df['date'].apply(lambda x: get_date_info(x)['quarter'] if pd.notnull(x) else pd.NA)
  
    # Calculate total engagements
    if 'g-p' in filename.lower():
        print("file is ,",filename)

        df1['Poll Votes'] = df['poll votes'].astype('Int64')

        df1['total engagements'] = calculate_total_engagements(df)
    
    return df1

def Create_Service(client_secret_env_var_name, token_env_var_name, api_name, api_version, scopes):
    """Create a Google API service using OAuth credentials from env or disk."""
    creds = None
    token_file = f"token_{api_name}_{api_version}.json"

    # Prefer credentials from environment variable
    token_json = os.getenv(token_env_var_name)
    if token_json:
        try:
            creds = Credentials.from_authorized_user_info(json.loads(token_json), scopes)
        except Exception as e:
            print(f"Error loading credentials from {token_env_var_name}: {e}")
    elif os.path.exists(token_file):
        try:
            creds = Credentials.from_authorized_user_file(token_file, scopes)
        except Exception as e:
            print(f"Error loading token file: {e}")

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            secret_json = os.getenv(client_secret_env_var_name)
            if not secret_json:
                raise EnvironmentError(f"Missing env var: {client_secret_env_var_name}")
            try:
                client_config = json.loads(secret_json)
                flow = InstalledAppFlow.from_client_config(client_config, scopes)
                creds = flow.run_console()
            except Exception as e:
                raise RuntimeError(f"OAuth setup failed: {e}")

    # Cache credentials to disk regardless of source
    with open(token_file, "w") as token:
        token.write(creds.to_json())

    try:
        service = build(api_name, api_version, credentials=creds)
        print(f"{api_name} service created successfully")
        return service
    except Exception as e:
        print("Unable to create service.")
        print(e)
        return None
        
def add_columns_to_mysql_table(engine, table_name, df):
    # Mapping pandas dtypes to MySQL column types
    dtype_mapping = {
        'int64': 'BIGINT',
        'float64': 'DOUBLE',
        'object': 'TEXT',
        'datetime64[ns]': 'DATE',
        'bool': 'BOOLEAN',
        'int32': 'INT',
        'float32': 'FLOAT'
    }
    
    # Connection to execute ALTER TABLE commands
    with engine.connect() as connection:
        
        query_1 = f"SHOW COLUMNS FROM `{table_name}`"
        result = connection.execute(text(query_1))
        df1 = pd.DataFrame(result.fetchall(), columns=result.keys())
        df1.columns = df1.columns.str.lower()
        df.columns = df.columns.str.lower()
        
        
        for column in df.columns:
            try:
                # Infer the appropriate MySQL data type
                if column not in df1['field'].values:
                    
                    mysql_type = dtype_mapping.get(str(df[column].dtype), 'VARCHAR(255)')
                    print(mysql_type)

                    # Construct ALTER TABLE query
                    alter_query = text(f"""
                        ALTER TABLE `{table_name}` 
                        ADD COLUMN `{column}` {mysql_type}  
                        ;
                    """)
                    print(alter_query)

                    # Execute the ALTER TABLE command
                    connection.execute(alter_query)

                    

                    print(f"Added column {column} with type {mysql_type}")
                else:
                    continue
            
            except Exception as e:
                print(f"Error adding column {column}: {e}")
        
        # Commit the changes
        connection.commit()

# Load processed files from JSON with error handling for empty or invalid JSON
def load_processed_files():
    if os.path.exists(PROCESSED_FILES_JSON):
        try:
            with open(PROCESSED_FILES_JSON, "r") as f:
                return set(json.load(f))
        except json.JSONDecodeError:
            print("Warning: processed_files.json is empty or contains invalid JSON. Initializing as empty set.")
            return set()
    return set()

# Save processed files to JSON
def save_processed_files(processed_files):
    with open(PROCESSED_FILES_JSON, "w") as f:
        json.dump(list(processed_files), f)
        

PROCESSED_FILES_JSON = "processed_files.json"

# Initialize processed files set
processed_files = load_processed_files()

def process_file(file_id, file_name):
    try:
        # Retrieve file metadata and determine MIME type
        file = service.files().get(fileId=file_id, fields='mimeType, name').execute()
        mime_type = file.get('mimeType')

        # Choose download method based on file type
        if mime_type == 'application/vnd.google-apps.spreadsheet':
            request = service.files().export_media(fileId=file_id, mimeType='text/csv')
        elif mime_type == 'application/vnd.google-apps.document':
            request = service.files().export_media(fileId=file_id, mimeType='application/vnd.openxmlformats-officedocument.wordprocessingml.document')
            # file_name = file_name.replace('.csv', '.docx')
        # elif mime_type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
            # request = service.files().get_media(fileId=file_id)
        else:
            request = service.files().get_media(fileId=file_id)

        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(f'Download progress for {file_name}: {status.progress() * 100:.2f}%')

        # Process CSV and Excel files
        if file_name.endswith('.csv') or file_name.endswith('.xlsx'):
            fh.seek(0)
            if file_name.endswith('.csv'):
                df = pd.read_csv(fh,index_col=False)
            else:
                df = pd.read_excel(fh,index_col=False)
                print(df.head(10))
                print(df.columns)
            
            table_name = file_name.replace('.csv', '').replace('.xlsx', '')
            print("file_name is .,",table_name)
            if 'g-p' in file_name.lower():
                db_url_specific = f"{DB_URL_PREFIX}/g_p"
                db_engine_specific = create_engine(db_url_specific)
                df = preprocess_emplifi(df, table_name)                
                df.to_sql('G-P Historical Data1', db_engine_specific, index=False, if_exists='append')
                print(f"Data from {file_name} uploaded to MySQL table {table_name}.")
                
            elif 'ao' in file_name.lower() or 'angry' in file_name.lower():
                db_url_specific = f"{DB_URL_PREFIX}/angry_orchard"
                db_engine_specific = create_engine(db_url_specific)
                if  "historical" in file_name.lower():
                    df['Published Date'] =pd.to_datetime(df['Published Date'], errors='coerce').dt.date
                    date_columns = ['Published Date']
                    dtype_dict = {col: types.Date for col in date_columns}
                    df.to_sql("AO Historical Data1", db_engine_specific, index=False, if_exists='replace',dtype=dtype_dict)
                elif  "export" in file_name.lower():
                            query1=f"select * from `AO Historical Data`"
                            with db_engine_specific.connect() as connection:
                                result = connection.execute(text(query1))
                                df1 = pd.DataFrame(result.fetchall(), columns=result.keys())

                            df = preprocess_emplifi(df, table_name)     
                            print("Preprocessing Done")
                            df = pd.concat([df1, df], axis=0,ignore_index = True)           
                            df.to_sql('AO Historical Data1', db_engine_specific, index=False, if_exists='replace',dtype=dtype_dict)
                            print(f"Data from {file_name} uploaded to MySQL table {table_name}.")

            else:
            # Check if table exists
                try:
                # Get available databases and client name
                    dbs = get_available_db()
                    db = get_client_name(table_name)
                    table_name = f"{db} Historical Data"
                    
                    # Check if database exists
                    if db in dbs.values:
                        print(f"Client Name of File {file_name} found in database list")
                    else:
                        print(f"Client Name of File {file_name} not found in database list. Creating database {db}")
                       #create_database_if_not_exists(db)
                        print(f"Database {db} created successfully")
                    
                    # Process DataFrame columns
                    print(f"Processing data with columns: {df.columns}")
                    df.columns = df.columns.str.lower()
                    
                    # Handle date columns
                    if 'date' in df.columns:
                        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
                        date_columns = ['date']
                    else:
                        df['published date'] = pd.to_datetime(df['published date'], errors='coerce').dt.date
                        date_columns = ['published date']
                    
                    dtype_dict = {col: types.Date for col in date_columns} 
                    
                    # Setup database connection
                    db_url_specific = f"{DB_URL_PREFIX}/{db}"
                    db_engine_specific = create_engine(db_url_specific)
                    print("connection created")
                    
                    # Define table name
                    table_name = "Historical Data"
                    table_exists = False
                    # Check if table file_name is .,
                    inspector = inspect(db_engine_specific)
                    print("tables are like: ",inspector.get_table_names())
                    for i in inspector.get_table_names():
                        if table_name in i:
                            print("Yes There's a table named",i, "matched with", table_name)
                            table_exists = True
                            print(i)
                            add_columns_to_mysql_table(db_engine_specific,i,df)
                            df.to_sql(name=i, con=db_engine_specific, if_exists='append', dtype=dtype_dict,index=False)
                            print("Table replaced successfully")
                            break

                    if not table_exists:
                        print("No table named", table_name, "exists in the database")
                        df.to_sql(name=f"{db} Historical Data", con=db_engine_specific, if_exists='replace', dtype=dtype_dict,index=False)
                        print(f"Table named {db} historical Data successfully inserted")
                        table_exists = False

                except Exception as e:
                    print(f"Error When tried ingesting into database: {str(e)}")
                    return False    

        # Add the file name to processed_files and save immediately after each processing
        processed_files.add(file_name)
        save_processed_files(processed_files)  # Save after processing each file to avoid reprocessing on restart
        return True

    except Exception as e:
        print(f"Error processing file {file_name}: {e}")
        return False

def monitor_drive_folder(run_id=None, logger: ETLLogger = None):
    files_ingested = []
    while True:
        response = service.files().list(q=query).execute()
        files = response.get('files', [])
        print(pd.DataFrame(files))
        
        for file in files:
            file_id = file['id']
            file_name = file['name']
            if file_name not in processed_files:
                print("As of Now Processed Files Now these are ", processed_files)
                print(f"Processing new file: {file_name}")
                if process_file(file_id, file_name):
                    files_ingested.append(file_name)
            else:
                print(f"No New files to Process: ",end="\n")
                # print("As of Processed Files these are ")
                print(processed_files)
                print("So No New file is added so exiting")
        break

    logging.info(f"Files transferred to SQL ({len(files_ingested)}): {files_ingested}")
    if logger and run_id:
        logger.log_drive_files(run_id, files_ingested)
    return files_ingested


API_NAME = 'drive'
API_VERSION = 'v3'
SCOPES = ['https://www.googleapis.com/auth/drive']
service = Create_Service(
    "google_drive_client_secret",
    "google_drive_token",
    API_NAME,
    API_VERSION,
    SCOPES,
)

folder_id = "1YCPgHFsVIvhlzq932eaFYm_Nn_0iFbkA"
query = f"'{folder_id}' in parents and trashed = false"

if __name__ == "__main__":
    monitor_drive_folder()
