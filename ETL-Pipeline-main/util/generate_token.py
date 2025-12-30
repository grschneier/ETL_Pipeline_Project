from dotenv import load_dotenv
from google_auth_oauthlib.flow import InstalledAppFlow
import os

def main():
    load_dotenv()
    google_ads_client_id = os.getenv("GOOGLE_ADS_CLIENT_ID")
    google_ads_client_secret = os.getenv("GOOGLE_ADS_CLIENT_SECRET")

    flow = InstalledAppFlow.from_client_config(
        {
            "installed": {
                "client_id": google_ads_client_id,
                "client_secret": google_ads_client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token"
            }
        },
        scopes=["https://www.googleapis.com/auth/adwords"],
    )

    
    try:
        credentials = flow.run_console()  # For terminal-only
    except AttributeError:
        # Fallback to browser-based
        credentials = flow.run_local_server(port=8080, prompt="consent")

    print("\n✅ Your Refresh Token:")
    print(credentials.refresh_token)

if __name__ == "__main__":
    main()
