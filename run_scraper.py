#!/usr/bin/env python3
"""
MLB Data Scraper Pipeline Runner
This script demonstrates how to run the MLB data scraping pipeline
and provides an example for the Upwork application.
"""

import os
import sys
from datetime import datetime
from pipeline import MLBDataScraper # Corrected import path

def main():
    """Main function to run the MLB data scraping pipeline."""
    
    print("⚾ MLB Data Scraping Pipeline")
    print("="*50)
    print("Starting MLB data collection and Google Sheets upload...")
    print()
    
    credentials_path = 'credentials.json'
    if not os.path.exists(credentials_path):
        print(f"❌ Error: {credentials_path} file not found!")
        print("Please ensure you have your Google Sheets API credentials file.")
        sys.exit(1)
    

    test_task_url = None
    if len(sys.argv) > 2 and sys.argv[1] == '--test-url':
        test_task_url = sys.argv[2]
        print(f"Running for specific test URL: {test_task_url}")
    elif len(sys.argv) > 1 and sys.argv[1] == '--demo':
        demo_mode()
        sys.exit(0)
    else:
        print("Running for recent games (default mode)...")
    
    try:

        print("🔧 Initializing scraper...")
        scraper = MLBDataScraper(config_file='config.json')
        
        # Run the pipeline based on mode
        print("📊 Running data collection pipeline...")
        if test_task_url:
            results = scraper.run_pipeline(game_url_for_test=test_task_url)
        else:
            results = scraper.run_pipeline(days_back=2) # Default to last 2 days if no specific URL
        
        # Display results
        print("\n" + "="*50)
        print("📈 PIPELINE RESULTS")
        print("="*50)
        
        if results['success']:
            print("✅ Pipeline completed successfully!")
            print(f"📊 Games processed: {results['games_processed']}")
            print(f"⚾ Batting records: {results['batting_records']}")
            print(f"🥎 Pitching records: {results['pitching_records']}")
            print(f"📝 Lineup records: {results['lineup_records']}") # Display lineup records
            
            if results['google_sheets_url']:
                print(f"\n🔗 Google Sheets URL:")
                print(f"   {results['google_sheets_url']}")
                print("\n📝 You can now share this link with clients or stakeholders!")
            
            if results['csv_files']:
                print(f"\n📁 CSV Files Generated:")
                for csv_file in results['csv_files']:
                    if os.path.exists(csv_file):
                        print(f"   ✅ {csv_file}")
                    else:
                        print(f"   ❌ {csv_file} (not found)")
            
            # Display sample data (simplified as columns are dynamically scraped)
            print("\n📋 Data Structure Overview:")
            print("-" * 30)
            print("Batting stats columns: Player, Team, AB, R, H, RBI, BB, SO, AVG (and more depending on source)")
            print("Pitching stats columns: Pitcher, Team, IP, H, R, ER, BB, SO, ERA (and more depending on source)")
            print("Lineup columns: Player, Team, Batting Order, Position")
                
        else:
            print("❌ Pipeline failed!")
        
        # Display any errors
        if results['errors']:
            print(f"\n⚠️  Errors encountered:")
            for error in results['errors']:
                print(f"   - {error}")
        
        print("\n" + "="*50)
        print("🎯 Ready for Upwork submission!")
        print("="*50)
        
    except Exception as e:
        print(f"❌ Fatal error during pipeline execution: {e}")
        import traceback
        traceback.print_exc() # Print full traceback for debugging
        sys.exit(1)

def demo_mode():
    """Run a demo version with sample data for testing."""
    print("🧪 Running in DEMO mode...")
    print("This will create sample data for testing purposes in 'demo_output' folder and a Google Sheet named 'MLB Data Analysis - Upwork Demo'.")
    
    # Create sample data
    import pandas as pd
    
    # Sample batting data
    batting_sample = pd.DataFrame({
        'team': ['NYY', 'BOS', 'NYY', 'BOS'],
        'game_date': ['2025-07-10', '2025-07-10', '2025-07-10', '2025-07-10'],
        'player': ['Aaron Judge', 'Rafael Devers', 'Giancarlo Stanton', 'Xander Bogaerts'],
        'ab': [4, 5, 3, 4], 'r': [2, 1, 1, 0], 'h': [2, 2, 1, 1],
        'rbi': [3, 1, 1, 0], 'bb': [1, 0, 1, 1], 'so': [1, 1, 2, 0],
        'avg': [.311, .279, .245, .307]
    })
    
    # Sample pitching data
    pitching_sample = pd.DataFrame({
        'team': ['NYY', 'BOS'],
        'game_date': ['2025-07-10', '2025-07-10'],
        'pitcher': ['Gerrit Cole', 'Brayan Bello'],
        'ip': ['6.0', '5.1'], 'h': [5, 7], 'r': [2, 4], 'er': [2, 4],
        'bb': [1, 2], 'so': [8, 5], 'era': [3.12, 4.21]
    })

    # Sample lineup data (NEW)
    lineup_sample = pd.DataFrame({
        'team': ['NYY', 'NYY', 'NYY', 'BOS', 'BOS'],
        'game_date': ['2025-07-10'] * 5,
        'batting_order': [1, 2, 3, 1, 2],
        'player': ['Anthony Volpe', 'Aaron Judge', 'Juan Soto', 'Jarren Duran', 'Masataka Yoshida'],
        'position': ['SS', 'CF', 'RF', 'CF', 'LF']
    })
    
    try:
        scraper = MLBDataScraper(config_file='config.json') # Ensure config is loaded for output path etc.
        
        # Export sample data to CSVs
        csv_files_demo = scraper.export_to_csv(
            batting_sample, pitching_sample, lineup_sample, 'demo_output', for_test_task=False # Changed to False for demo
        )
        
        # Upload to Google Sheets
        google_url = scraper.upload_to_google_sheets(batting_sample, pitching_sample, lineup_sample)
        
        print(f"\n✅ Demo completed successfully!")
        print(f"📁 Sample CSV files generated: {', '.join(csv_files_demo)}")
        print(f"🔗 Google Sheets: {google_url}")
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == '--demo':
        demo_mode()
    else:
        main()