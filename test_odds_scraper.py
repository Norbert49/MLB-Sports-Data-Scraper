import pandas as pd
from datetime import datetime, timedelta
import logging
import os

# Assuming odds_scraper.py and data_exporter.py are in the same directory
from odds_scraper import OddsScraper
from data_exporter import DataExporter

# Configure logging for the test script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(UPLOAD_TEST)s - %(levelname)s - %(message)s')
logger = logging.getLogger('UPLOAD_TEST')

def get_upcoming_mlb_odds_and_upload():
    """
    Fetches all upcoming MLB fixtures with odds for the next few days
    and uploads them to the 'Betting_Odds' worksheet in Google Sheets.
    """
    # !!! IMPORTANT: Replace with your actual API key for The Odds API !!!
    odds_api_key = "2047a4084ad554bec8a70df2bc8e887a" 
    
    # Initialize your OddsScraper
    odds_scraper = OddsScraper(odds_api_key)
    
    # Initialize your DataExporter (ensure config.json and credentials.json are set up)
    # The DataExporter will attempt to set up Google Sheets on initialization
    data_exporter = DataExporter(config_file='config.json') 

    all_upcoming_odds_df = pd.DataFrame()

    # Define the range of dates to check (e.g., today + next 2 days)
    # This will capture the All-Star game and likely the first day of regular season games after the break.
    num_days_to_check = 2 # Check today and tomorrow
    dates_to_check = [(datetime.now() + timedelta(days=i)).strftime('%Y-%m-%d') 
                      for i in range(num_days_to_check)]

    logger.info(f"Checking for upcoming MLB odds for dates: {', '.join(dates_to_check)}")

    for target_date in dates_to_check:
        logger.info(f"Attempting to fetch odds for {target_date}...")
        odds_df_for_date = odds_scraper.fetch_all_mlb_odds_for_date(target_date)
        
        if not odds_df_for_date.empty:
            all_upcoming_odds_df = pd.concat([all_upcoming_odds_df, odds_df_for_date], ignore_index=True)
            logger.info(f"Retrieved {len(odds_df_for_date)} odds records for {target_date}.")
        else:
            logger.info(f"No odds retrieved for {target_date}.")
    
    if not all_upcoming_odds_df.empty:
        logger.info(f"Successfully retrieved a total of {len(all_upcoming_odds_df)} upcoming odds records from API calls.")
        
        # --- UPLOAD TO GOOGLE SHEETS ---
        logger.info("Attempting to upload retrieved odds data to Google Sheets...")
        
        # We need to pass dummy DataFrames for other sheets if we only want to update odds.
        # Ensure your upload_to_google_sheets method can handle empty DFs for other types.
        dummy_df = pd.DataFrame() 
        
        google_sheets_url = data_exporter.upload_to_google_sheets(
            batting_df=dummy_df,
            pitching_df=dummy_df,
            lineup_df=dummy_df,
            game_info_df=dummy_df,
            odds_df=all_upcoming_odds_df # This is the key DataFrame to upload
        )
        
        if google_sheets_url:
            logger.info(f"Odds data uploaded successfully to Google Sheets. URL: {google_sheets_url}")
            print(f"\n✅ SUCCESS: Betting odds uploaded to Google Sheets!")
            print(f"🔗 Google Sheets URL: {google_sheets_url}")
            print("\nCheck your 'Betting_Odds' tab in the spreadsheet.")
        else:
            logger.error("Failed to upload odds data to Google Sheets.")
            print("\n❌ FAILED to upload betting odds to Google Sheets. Check logs for errors.")
            
        # Optional: Save to CSV locally as well
        output_dir = "output_demonstration"
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = os.path.join(output_dir, f"mlb_upcoming_odds_for_demo_{timestamp}.csv")
        all_upcoming_odds_df.to_csv(csv_path, index=False)
        logger.info(f"Local CSV copy saved to: {csv_path}")

    else:
        logger.warning("No upcoming MLB odds data found for the selected date range. Nothing to upload.")
    
    logger.info("Demonstration script finished.")

if __name__ == "__main__":
    get_upcoming_mlb_odds_and_upload()