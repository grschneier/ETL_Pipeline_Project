from datetime import datetime
import re
from dateutil import parser
import pandas as pd
import json
import os

def date(date_string):
    """Convert date string to 'YYYY-MM-DD' format."""
    try:
        if isinstance(date_string, str):
            date_obj = pd.to_datetime(date_string, errors='coerce')
        else:
            date_obj = date_string

        # Check if date_obj is valid
        if pd.isnull(date_obj):
            raise ValueError("Invalid date format")
        return date_obj.strftime('%Y-%m-%d')

    except Exception as e:
        print("date string is :",date_string)
        print(f"Date conversion error: {e}")
        return None


def clean_adname(ad_name):
    # Replace '€™' with a single quote
    ad_name = ad_name.replace("€™", "'")
    # Remove any occurrences of 'urn:li:' and the following content
    if "urn:li:" in ad_name:
        ad_name = re.sub(r"urn:li:\S+", "", ad_name).strip()
    return ad_name

def extract_objective(campaign_name):
    """
    Extracts the campaign objective from a campaign name
    
    Args:
        campaign_name (str): Campaign name to analyze
        
    Returns:
        str: Extracted campaign objective
    """
    # Convert to lowercase for case-insensitive matching
    campaign_name = campaign_name.lower().strip()
    
    # Dictionary mapping keywords to objectives
    objective_mapping = {
        ("awareness", "reach"): "Awareness",
        ("traffic",): "Traffic",
        ("engagement",): "Engagement",
        ("profile visit",): "Profile Visit",
        ("lead",): "Lead",
        ("conversion",): "Conversion",
        ("search",): "Search",
        ("community interaction",): "Community Interaction",
        ("app",): "App",
        ("landing page view", "lpv", "high spending power"): "Landing Page View",
        ("video views", "video view", "view", "views"): "Video Views"
    }
    
    # Check for matches in the mapping at the start of the campaign name
    for keywords, objective in objective_mapping.items():
        if any(campaign_name.startswith(keyword) for keyword in keywords):
            return objective
    
    # If no match found at start, check anywhere in the name
    for keywords, objective in objective_mapping.items():
        if any(keyword in campaign_name for keyword in keywords):
            return objective
    
    # If no match found, try to split by dash and get first element
    try:
        first_element = campaign_name.split('-')[0].strip()
        if first_element:
            return first_element.title()
    except (IndexError, AttributeError) as e:
        print(f"Error in extract_objective: {e}")
        
    return "Not Found"


def extract_content(input_string):
    """
    Extracts content information from a formatted string, removing any objective keywords
    Args:
        input_string (str): String in format "Objective - Audience - Content"
    Returns:
        str: Cleaned content segment with objective keywords removed
    """
    # Dictionary of objective keywords to check against
    keywords = {
        "awareness", "reach", "traffic", "engagement", "profile visit",
        "lead", "conversion", "search", "community interaction", "app",
        "landing page view", "lpv", "high spending power", "video views","video",
        "video view", "view", "views", "interest/behavior"
    }
    
    # First split by major sections using dash with spaces
    parts = [part.strip() for part in input_string.lower().split(' - ')]
    
    # Check number of parts
    if len(parts) == 2:
        # Check each keyword
        for keyword in keywords:
            if keyword in input_string.lower():
                # Remove the keyword and return the cleaned string
                return input_string.lower().replace(keyword, '').strip().strip('-').strip()
        return parts[-1]
    elif len(parts) > 2:
        return parts[-1]
    elif len(parts) == 1:
        return parts[0]
    
    return "Not Found"


def extract_audience(input_string):
    """
    Extracts audience information from a formatted string
    
    Args:
        input_string (str): String in format "Traffic - Audience - Other Info"
        
    Returns:
        str: Extracted audience segment
    """
    # Split string by dashes and strip whitespace
    parts = [part.strip() for part in input_string.split('-')]
    
    # Return audience portion (second element) if it exists
    if len(parts) > 1:
        return parts[1]
    elif len(parts)==1:
        return parts[0]
    return "Not Found"

def extract_destination_from_adset(ad_name, ad_set_name, campaign_name, campaign_start_date):
    """Extract destination from ad set name or ad name."""
    try:
        if " - Copy" in ad_name and "jbl" in campaign_name and parser.parse(campaign_start_date).year == 2022:
            return "JBL.com"
            
        # Check if the campaign name contains 'apothic', return last part of ad_name
        if 'apothic' in campaign_name.lower():
            return ad_name.split('-')[-1].strip()

        # Extract the last segment after the final hyphen from the ad set name
        destination = ad_set_name.split('-')[-1].strip().lower()

        # Check if the destination contains an underscore ('_')
        if '_' in destination:
            return "Not Found"

        # Define placements and keywords to check
        placements = ["feed", "stories", "story", "post"]

        # Check for specific conditions in the destination
        if re.match(r'@', destination):
            return "Not Found"  # Return Not Found if '@' symbol is present
        elif destination in placements:
            return "Not Found"  # Return Not Found if in placements list
        elif "interest" in destination or "behavior" in destination:
            return "Not Found"  # Return Not Found if "interest" or "behavior" in destination
        elif "retargeting" in destination:
            return "Not Found"  # Return Not Found if "retargeting" in destination
        elif "lookalike" in destination:
            return "Not Found"  # Return Not Found if "lookalike" in destination
        elif "statsocial" in destination:
            return "Not Found"  # Return Not Found if "statsocial" in destination

        # if " - Copy" in ad_name and parser.parse(campaign_start_date).year == 2022:
        #     return "JBL.com"

        # If no conditions match, return the destination
        return destination if destination else "Not Found"

    except Exception as e:
        print(f"Error in extract_destination_from_adset: {e}")
        return "Not Found"

def extract_objective_from_campaign(campaign_name, campaign_start_date):
    """Extract objective from campaign name based on keywords."""
    try:
        campaign_name = campaign_name.lower()

        # Check if the campaign_start_date is a string and convert it to datetime
        if isinstance(campaign_start_date, str):
            try:
                campaign_start_date = parser.parse(campaign_start_date)
            except ValueError:
                print("For extracting objective campaign_start_date is not in valid format")
                return "Not Found"

        # Check if the campaign is from 2021 and if it contains "JBL"
        if campaign_start_date.year == 2021 and "jbl" in campaign_name:
            return "Traffic"

        if "awareness" in campaign_name or "reach" in campaign_name:
            return "Awareness"
        elif "traffic" in campaign_name:
            return "Traffic"
        elif "engagement" in campaign_name:
            return "Engagement"
        elif "profile visit" in campaign_name:
            return "Profile Visit"
        elif "lead" in campaign_name:
            return "Lead"
        elif "conversion" in campaign_name:
            return "Conversion"
        elif "search" in campaign_name:
            return "Search"
        elif "community interaction" in campaign_name:
            return "Community Interaction"
        elif "app" in campaign_name:
            return "App"
        elif "landing page view" in campaign_name or "lpv" in campaign_name or "high spending power" in campaign_name:
            return "Landing Page View"
        elif "video views" in campaign_name or "video view" in campaign_name or "view" in campaign_name or "views" in campaign_name:
            return "Video Views"
        else:
            return "Unknown"
    except Exception as e:
        print(f"Error in extract_objective_from_campaign: {e}")
        return "Not Found"

def extract_influencer_from_adname(ad_name, ad_set_name, campaign_start_date):
    """Extract influencer handle from ad name or ad set name."""
    try:
        # Convert timestamp if needed
        if not isinstance(campaign_start_date, str):
            campaign_start_date = campaign_start_date.strftime("%Y-%m-%d")

        # Older JBL campaigns (<2022): extract from ad set name
        if "jbl" in ad_set_name.lower() and parser.parse(campaign_start_date).year < 2022:
            match = re.search(r'@(\S+?)$', ad_set_name)
            if match:
                return match.group(1)

        # Else: extract from ad name
        match = re.search(r'@([^\s]+)', ad_name)
        if match:
            return match.group(1)

        return "Not Found"
    except Exception as e:
        print(f"Error in extract_influencer_from_adname: {e}")
        return "Not Found"
            
        

        # If no influencer is found, return Not Found
        return "Not Found"
    except Exception as e:
        print(f"Error in extract_influencer_from_adname: {e}")
        return "Not Found"

from dateutil import parser

def extract_audience_from_adset(ad_name, ad_set_name, campaign_name, campaign_start_date):
    """Extract audience from ad set name based on keywords."""
    try:
        # Ensure campaign_start_date is a datetime object
        if isinstance(campaign_start_date, str):
            campaign_start_date = parser.parse(campaign_start_date)

        # Check if the campaign is from 2021 and if it contains "JBL"
        if campaign_start_date.year == 2021 and "jbl" in campaign_name.lower():
            audience_keywords = ["interest", "retargeting", "lookalike", "statsocial", "broad"]
            audience = next(
                (
                    keyword for keyword in audience_keywords
                    if any(keyword in (item or "").lower() for item in [ad_name, ad_set_name, campaign_name])
                ),
                "Not Found"
            )
            return audience
        else:
            ad_set_name = (ad_set_name or "").lower()
            if "interest" in ad_set_name or "behavior" in ad_set_name:
                return "Interest/Behavior"
            elif "retargeting" in ad_set_name:
                return "Retargeting"
            elif "lookalike" in ad_set_name:
                return "Lookalike"
            elif "statsocial" in ad_set_name:
                return "Statsocial"
            elif "broad" in ad_set_name:
                return "Broad"
            else:
                return "Unknown"
    except Exception as e:
        print(f"Error in extract_audience_from_adset: {e}")
        return "Not Found"
    
def extract_placement_from_adset_fb(ad_name, ad_set_name, campaign_name, campaign_start_date):
    """Extract placement from ad name, ad set name, and campaign name."""
    try:
        # Normalize the date to datetime if it's not already
        if isinstance(campaign_start_date, str):
            campaign_start_date = parser.parse(campaign_start_date)

        # Check if campaign is JBL and from 2021
        if campaign_start_date.year == 2021 and "jbl" in campaign_name.lower():
            placements = ["feed", "stories", "story"]
            found_placements = []
            for placement in placements:
                if any(placement in item.lower() for item in [ad_name, ad_set_name, campaign_name]):
                    found_placements.append(placement)

            # Normalize stories to story
            if "stories" in found_placements:
                found_placements = ["story"]

            return f'IG {", ".join(found_placements)}' if found_placements else "Not Found"

        else:
            # Default case for newer or non-JBL campaigns
            placements = ["feed", "stories", "story"]
            found_placements = [placement for placement in placements if placement in ad_set_name.lower()]

            if "stories" in found_placements:
                found_placements = ["story"]

            return f'IG {", ".join(found_placements)}' if found_placements else "No Placement"

    except Exception as e:
        print(f"Error in extract_placement_from_adset_fb: {e}")
        return "Not Found"

def extract_round_from_adname(ad_name, ad_set_name, campaign_name, campaign_start_date):
    try:
        # Convert Timestamp to string if needed
        if not isinstance(campaign_start_date, str):
            campaign_start_date = campaign_start_date.strftime("%Y-%m-%d")

        if parser.parse(campaign_start_date).year == 2021 and "jbl" in campaign_name.lower():
            for source in [ad_name, ad_set_name, campaign_name]:
                match = re.search(r'V(\d+)', str(source), re.IGNORECASE)
                if match:
                    return f"Round {match.group(1)}"
            return "Not Found"
        else:
            match = re.search(r'V(\d+)', str(ad_name), re.IGNORECASE)
            return f"Round {match.group(1)}" if match else "Not Found"
    except Exception as e:
        print(f"Error in extracting_round: {e}")
        return "Not Found"

def preprocess_insta(df):
    """Preprocess Instagram DataFrame for specific fields."""
    try:
        required_columns = [
            'Campaign Name', 'Start Date', 'End Date', 'Ad Set Name', 'Ad Name',
            'Amount Spent', 'Impressions', 'Reach', 'Link Clicks', 'Post Engagements',
            'Post Shares', 'Post Reactions', 'Post Comments', 'Post Saves', '3-second Video Plays', 'Platform', 'Objective'
        ]

        # Check for missing columns
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise KeyError(f"Missing required columns: {missing_columns}")
        
        df['Amount Spent'] = df['Amount Spent'].astype(str).str.replace(r'[^0-9.]', '', regex=True)

        # Determine processing branch
        
        is_general = df['Ad Account Name'].str.contains('G-P', na=False).any()
        #print( df['Ad Account Name'].iloc[0])
        #print("genral:, ", is_general)

        # Common processing
        processed_df = pd.DataFrame({
            'Ad Account Name': df['Ad Account Name'],
            'Campaign Name': df['Campaign Name'],
            'Start Date': df['Start Date'].apply(date),
            'End Date': df['End Date'].apply(date),
            'Date' : pd.to_datetime(df['Date'], errors='coerce'),
            'Ad Set Name': df['Ad Set Name'],
            'Ad Name': df['Ad Name'],
            'Spent': pd.to_numeric(df['Amount Spent'], errors='coerce').fillna(0),
            'Impressions': pd.to_numeric(df['Impressions'], errors='coerce').fillna(0).astype(int),
            'Reach': pd.to_numeric(df['Reach'], errors='coerce').fillna(0).astype(int),
            'Clicks': pd.to_numeric(df['Link Clicks'], errors='coerce').fillna(0).astype(int),
            'Post Engagements': pd.to_numeric(df['Post Engagements'], errors='coerce').fillna(0).astype(int),
            'Post Shares': pd.to_numeric(df['Post Shares'], errors='coerce').fillna(0).astype(int),
            'Post Reactions': pd.to_numeric(df['Post Reactions'], errors='coerce').fillna(0).astype(int),
            'Post Comments': pd.to_numeric(df['Post Comments'], errors='coerce').fillna(0).astype(int),
            'Post Saves': pd.to_numeric(df['Post Saves'], errors='coerce').fillna(0).astype(int),
            '3-second Video Plays': pd.to_numeric(df['3-second Video Plays'], errors='coerce').fillna(0).astype(int),
            'Eng Minus Views': (
                pd.to_numeric(df['Post Shares'], errors='coerce').fillna(0).astype(int) +
                pd.to_numeric(df['Post Reactions'], errors='coerce').fillna(0).astype(int) +
                pd.to_numeric(df['Post Comments'], errors='coerce').fillna(0).astype(int) +
                pd.to_numeric(df['Post Saves'], errors='coerce').fillna(0).astype(int)
            ),
            'Platform': df['Platform']
        })

        # Conditional fields based on 'g-p' presence
        if not is_general:
            processed_df['Round'] = df.apply(lambda row: extract_round_from_adname(row['Ad Name'], row['Ad Set Name'], row['Campaign Name'], row['Start Date']), axis=1)
            processed_df['Audience'] = df.apply(lambda row: extract_audience_from_adset(row['Ad Name'], row['Ad Set Name'], row['Campaign Name'], row['Start Date']), axis=1)
            processed_df['Influencer'] = df.apply(lambda row: extract_influencer_from_adname(row['Ad Name'], row['Ad Set Name'], row['Start Date']), axis=1)
            processed_df['Objective'] = df.apply(lambda row: extract_objective_from_campaign(row['Campaign Name'], row['Start Date']), axis=1)
            processed_df['Objective1']=df['Objective']
            processed_df['Placement'] = df.apply(lambda row: extract_placement_from_adset_fb(row['Ad Name'], row['Ad Set Name'], row['Campaign Name'], row['Start Date']), axis=1)
            processed_df['Destination'] = df.apply(lambda row: extract_destination_from_adset(row['Ad Name'], row['Ad Set Name'], row['Campaign Name'], row['Start Date']), axis=1)
        else:
            processed_df['Audience'] = df['Ad Set Name'].apply(extract_audience)
            processed_df['Objective'] = df['Ad Set Name'].apply(extract_objective)
            processed_df['Objective1'] = df['Objective']
            processed_df['Content Name'] = df['Ad Set Name'].apply(extract_content)
        return processed_df

    except Exception as e:
        #print(f"Error in preprocessing Instagram data: {e}")
        return None



def preprocess_tiktok(df_tiktok):
    """Preprocess TikTok DataFrame for specific fields."""
    try:
        # Strip whitespace and convert all column names to lowercase for consistency
        df_tiktok.columns = df_tiktok.columns.str.strip().str.lower()

        # Define the required columns with cleaned names
        required_columns = [
            'campaign name', 'ad group name', 'schedule start time', 'schedule end time', 'date'
            'ad name', 'spend', 'impressions', 'reach', 'clicks', 'likes', 'comments',
            'shares', 'follows', 'video views (2s)', 'objective'
        ]

        # Ensure all required columns are present, if not create empty columns
        for col in required_columns:
            if col not in df_tiktok.columns:
                print(f"Warning: Missing required column: {col}. Creating empty column.")
                df_tiktok[col] = None

        # Preprocess DataFrame
        processed_df = pd.DataFrame({
            'Ad Account Name': df_tiktok.get('ad account name', pd.Series([None] * len(df_tiktok))),
            'Campaign Name': df_tiktok['campaign name'],
            'Ad Set Name': df_tiktok['ad group name'],
            'Start Date': pd.to_datetime(df_tiktok['schedule start time'], errors='coerce').dt.date,
            'End Date': pd.to_datetime(df_tiktok['schedule end time'], errors='coerce').dt.date,
            'Date': pd.to_datetime(df_tiktok['date'], errors='coerce').dt.date,
            'Ad Name': df_tiktok['ad name'],
            'Spent': pd.to_numeric(df_tiktok['spend'], errors='coerce').fillna(0),
            'Impressions': pd.to_numeric(df_tiktok['impressions'], errors='coerce').fillna(0).astype(int),
            'Reach': pd.to_numeric(df_tiktok['reach'], errors='coerce').fillna(0).astype(int),
            'Clicks': pd.to_numeric(df_tiktok['clicks'], errors='coerce').fillna(0).astype(int),
            'Post Engagements': (
                pd.to_numeric(df_tiktok['likes'], errors='coerce').fillna(0).astype(int) +
                pd.to_numeric(df_tiktok['comments'], errors='coerce').fillna(0).astype(int) +
                pd.to_numeric(df_tiktok['shares'], errors='coerce').fillna(0).astype(int) +
                pd.to_numeric(df_tiktok['follows'], errors='coerce').fillna(0).astype(int) +
                pd.to_numeric(df_tiktok['video views (2s)'], errors='coerce').fillna(0).astype(int)
            ),
            'Post Shares': pd.to_numeric(df_tiktok['shares'], errors='coerce').fillna(0).astype(int),
            'Post Reactions': pd.to_numeric(df_tiktok['likes'], errors='coerce').fillna(0).astype(int),
            'Post Comments': pd.to_numeric(df_tiktok['comments'], errors='coerce').fillna(0).astype(int),
            '3-second Video Plays': pd.to_numeric(df_tiktok['video views (2s)'], errors='coerce').fillna(0).astype(int),
            'Eng Minus Views': (
                pd.to_numeric(df_tiktok['likes'], errors='coerce').fillna(0).astype(int) +
                pd.to_numeric(df_tiktok['comments'], errors='coerce').fillna(0).astype(int) +
                pd.to_numeric(df_tiktok['shares'], errors='coerce').fillna(0).astype(int) +
                pd.to_numeric(df_tiktok['follows'], errors='coerce').fillna(0).astype(int)
            ),
            'Platform': 'TikTok',
            'Round': df_tiktok.apply(lambda row: extract_round_from_adname(row['ad name'], row['ad group name'], row['campaign name'], row['schedule start time']), axis=1),
            'Audience': df_tiktok.apply(lambda row: extract_audience_from_adset(row['ad name'], row['ad group name'], row['campaign name'], row['schedule start time']), axis=1),
            'Influencer': df_tiktok.apply(lambda row: extract_influencer_from_adname(row['ad name'], row['ad group name'], row['schedule start time']), axis=1),
            'Objective1': df_tiktok['objective'],
            'Objective': df_tiktok.apply(lambda row: extract_objective_from_campaign(row['campaign name'], row['schedule start time']), axis=1),
            'Placement': 'TikTok Feed',
            'Destination': df_tiktok.apply(
                lambda row: extract_destination_from_adset(
                    row['ad name'],
                    row['ad group name'],
                    row['campaign name'],
                    row['schedule start time']
                ),
                axis=1
            ),
            'Follows': pd.to_numeric(df_tiktok['follows'], errors='coerce').fillna(0).astype(int)
        })
        # Define columns to check for zero metrics
        zero_metric_cols = [
            'Spent', 'Impressions', 'Reach', 'Clicks', 'Post Reactions',
            'Post Comments', 'Post Shares', 'Follows', '3-second Video Plays'
        ]

        # Filter out rows where all of these columns are zero
        processed_df = processed_df[~(processed_df[zero_metric_cols].sum(axis=1) == 0)]
        return processed_df

    except Exception as e:
        print(f"Error in preprocessing TikTok data: {e}")
        return None


def preprocess_linkedin(df_linkedin):
    try:
        # Strip whitespace from column names
        df_linkedin.columns = df_linkedin.columns.str.strip()
        #print("Columns:", df_linkedin.columns)

        processed_df = pd.DataFrame({
            'Ad Account Name': df_linkedin['Ad Account Name'],
            'Campaign Name': df_linkedin['Campaign Group'],  # Correct column
            'Ad Set Name': df_linkedin['Campaign Name'],
            'Start Date': pd.to_datetime(df_linkedin['Start Date'], errors='coerce'),
            'End Date': df_linkedin['End Date'].fillna(pd.Timestamp(datetime.today())).infer_objects(copy=False),
            'Ad Name': df_linkedin['Ad Creative Name'].apply(clean_adname),
            'Spent': pd.to_numeric(df_linkedin['Cost in USD'], errors='coerce').fillna(0),
            'Impressions': pd.to_numeric(df_linkedin['Impressions'], errors='coerce').fillna(0).astype(int),
            'Clicks': pd.to_numeric(df_linkedin['Clicks'], errors='coerce').fillna(0).astype(int),
            'Post Engagements': (
                pd.to_numeric(df_linkedin['Total Engagements'], errors='coerce').fillna(0).astype(int) +
                pd.to_numeric(df_linkedin['Views'], errors='coerce').fillna(0).astype(int)
            ),
            'Post Shares': pd.to_numeric(df_linkedin['Shares'], errors='coerce').fillna(0).astype(int),
            'Post Reactions': pd.to_numeric(df_linkedin['Reactions'], errors='coerce').fillna(0).astype(int),
            'Post Comments': pd.to_numeric(df_linkedin['Comments'], errors='coerce').fillna(0).astype(int),
            '3-second Video Plays': pd.to_numeric(df_linkedin['Views'], errors='coerce').fillna(0).astype(int),
            'Eng Minus Views': pd.to_numeric(df_linkedin['Total Engagements'], errors='coerce').fillna(0).astype(int),
            'Content Name': df_linkedin['Campaign Name'].apply(extract_content),
            'Audience': df_linkedin['Campaign Name'].apply(extract_audience),
            'Objective': df_linkedin['Campaign Name'].apply(extract_objective),
            'Platform': "LinkedIn",
            'Objective1': df_linkedin['objectiveType'],
            'Follows': pd.to_numeric(df_linkedin['Follows'], errors='coerce').fillna(0).astype(int)
        })
        
        return processed_df

    except KeyError as ke:
        print(f"KeyError - Missing column: {ke}")
        return None
    except Exception as e:
        print(f"Error in preprocessing LinkedIn data: {e}")
        return None
def preprocess_youtube(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize raw YouTube Ads data for loading into the paid media schema.

    The function performs the following steps:
    - Validates input and renames key columns
    - Converts dates and numeric values
    - Logs rows missing critical identifiers
    - Populates derived fields such as Round and Audience
    - Returns a DataFrame with the expected column ordering
    """

    if df.empty:
        print("YouTube DataFrame is empty.")
        return df

    # Rename columns to align with schema
    df = df.rename(columns={
        "Ad Group Name": "Ad Set Name",
        "Video Views": "3-second Video Plays",
        "Spend": "Spent"
    })

    # Warn about any rows missing key identifiers
    id_fields = ['Ad Account Name', 'Campaign Name', 'Ad Name']
    missing_rows = df[df[id_fields].isnull().any(axis=1)]
    if not missing_rows.empty:
        print("⚠️ Warning: Rows with missing critical ID fields:")
        print(missing_rows)

    # Convert date columns
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df['Start Date'] = df['Date']
    df['End Date'] = df['Date']

    # Normalize numeric columns
    numeric_columns = ['Impressions', 'Clicks', '3-second Video Plays', 'Spent']
    if 'CTR' in df.columns:
        numeric_columns.append('CTR')
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    if 'Spent' in df.columns:
        df['Spent'] = df['Spent'].round(2)
        df = df[~((df['Spent'] == 0) & (df['Impressions'] == 0))]

    # Fill required but non-YouTube fields with default values
    df['Reach'] = None
    df['Post Engagements'] = None
    df['Post Shares'] = None
    df['Post Reactions'] = None
    df['Post Comments'] = None
    df['Post Saves'] = None
    df['Eng Minus Views'] = None
    df['Platform'] = 'YouTube'
    df['Round'] = df.apply(lambda row: extract_round_from_adname(row['Ad Name'], row['Ad Set Name'], row['Campaign Name'], row['Start Date']), axis=1)    
    df['Audience'] = df.apply(lambda row: extract_audience_from_adset(row['Ad Name'], row['Ad Set Name'], row['Campaign Name'], row['Start Date']), axis=1)
    df['Influencer'] = df.apply(lambda row: extract_influencer_from_adname(row['Ad Name'], row['Ad Set Name'], row['Start Date']), axis=1)
    df['Objective1'] = None
    df['Objective'] = df['Campaign Name'].apply(extract_objective)
    df['Placement'] = df.apply(lambda row: extract_placement_from_adset_fb(
        row['Ad Name'], row['Ad Set Name'], row['Campaign Name'], row['Start Date']
    ), axis=1)    
    df['Destination'] = df.apply(
        lambda row: extract_destination_from_adset(
            row['Ad Name'], row['Ad Set Name'], row['Campaign Name'], row['Start Date']
        ),
        axis=1
    )
    df['Follows'] = None

    # Ensure final column ordering
    final_columns = [
        'Ad Account Name', 'Campaign Name', 'Ad Set Name', 'Start Date', 'End Date', 'Date',
        'Ad Name', 'Spent', 'Impressions', 'Reach', 'Clicks', 'Post Engagements', 'Post Shares',
        'Post Reactions', 'Post Comments', 'Post Saves', '3-second Video Plays', 'Eng Minus Views',
        'Platform', 'Round', 'Audience', 'Influencer', 'Objective1', 'Objective', 'Placement',
        'Destination', 'Follows'
    ]

    return df[[col for col in final_columns if col in df.columns]]
