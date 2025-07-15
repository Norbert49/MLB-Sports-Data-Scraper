#!/usr/bin/env python3

import os
import sys
from datetime import datetime, timedelta
import pandas as pd
import re
from typing import Optional, Dict, List
import asyncio # Import asyncio for async operations
import aiohttp # Import aiohttp for async HTTP client session

from game_scraper import GameScraper
from data_exporter import DataExporter
from odds_scraper import OddsScraper
from mlb_insights_generator import MLBInsightsGenerator # Import MLBInsightsGenerator
import logging

# Configure logging at the module level
logging.basicConfig(level=logging.INFO, # Changed to INFO for less verbose default output, DEBUG is very verbose
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class MLBPipeline:
    def __init__(self, config_file: str = 'config.json'):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config_file = config_file 
        
        self.game_scraper = GameScraper(self.config_file)
        self.config = self.game_scraper.config 
        
        self.data_exporter = DataExporter(self.config_file)
        
        # Initialize OddsScraper only if API key is present in config
        odds_api_key = self.config.get('odds_api', {}).get('api_key')
        if odds_api_key:
            self.odds_scraper = OddsScraper(api_key=odds_api_key, config=self.config)
            self.logger.info("OddsScraper initialized in MLBPipeline.")
        else:
            self.logger.warning("Odds API key not found in config.json. Odds scraping will not be available in pipeline.")
            self.odds_scraper = None
        
        # Initialize MLBInsightsGenerator
        self.mlb_insights_generator = MLBInsightsGenerator() 
        self.logger.info("MLBInsightsGenerator initialized.")


    async def run_pipeline(self, days_back_override: Optional[int] = None, game_url_for_test: Optional[str] = None) -> Dict:
        results = {
            'success': False,
            'games_processed': 0,
            'batting_records': 0,
            'pitching_records': 0,
            'lineup_records': 0,
            'game_info_records': 0,
            'odds_records': 0,
            'insights_records': 0, 
            'csv_files': [],
            'google_sheets_url': '',
            'scores_sheet_url': '',
            'errors': []
        }
        
        try:
            all_batting_data = []
            all_pitching_data = []
            all_lineup_data = []
            all_game_info_data = []
            all_odds_data = []
            
            fetch_past_games_enabled = self.config.get('pipeline_settings', {}).get('fetch_past_games', {}).get('enabled', True)
            config_days_back = self.config.get('pipeline_settings', {}).get('fetch_past_games', {}).get('days_back', 1)
            days_back_to_use = days_back_override if days_back_override is not None else config_days_back

            if game_url_for_test:
                self.logger.info(f"Running pipeline for specific test URL: {game_url_for_test}.")
                batting_df, pitching_df, lineup_df, game_details_raw = self.game_scraper.scrape_box_score(game_url_for_test)
                
                game_date_match = re.search(r'\d{8}', game_url_for_test)
                current_game_date = datetime.now().strftime('%Y-%m-%d')
                if game_date_match:
                    try:
                        date_obj = datetime.strptime(game_date_match.group(), '%Y%m%d')
                        current_game_date = date_obj.strftime('%Y-%m-%d')
                    except ValueError:
                        self.logger.warning(f"Could not parse game date from test URL: {game_url_for_test}. Using current date.")

                if not batting_df.empty:
                    batting_df['game_date'] = current_game_date
                    all_batting_data.append(batting_df)
                if not pitching_df.empty:
                    pitching_df['game_date'] = current_game_date
                    all_pitching_data.append(pitching_df)
                
                if not lineup_df.empty:
                    lineup_df['game_date'] = current_game_date 
                    all_lineup_data.append(lineup_df)
                
                game_info_for_insights = {}
                if game_details_raw and 'game_info' in game_details_raw:
                    game_info_for_insights.update(game_details_raw['game_info'])
                    game_info_for_insights['game_date'] = current_game_date 
                    
                    if 'home_score' in game_info_for_insights and 'away_score' in game_info_for_insights:
                        home_score = int(game_info_for_insights['home_score'])
                        away_score = int(game_info_for_insights['away_score'])
                        if home_score > away_score:
                            game_info_for_insights['winner'] = game_info_for_insights.get('home_team')
                            game_info_for_insights['loser'] = game_info_for_insights.get('away_team')
                        elif away_score > home_score:
                            game_info_for_insights['winner'] = game_info_for_insights.get('away_team')
                            game_info_for_insights['loser'] = game_info_for_insights.get('home_team')
                        else:
                            game_info_for_insights['winner'] = 'Tie'
                            game_info_for_insights['loser'] = 'Tie'
                    
                    if 'pitchers' in game_details_raw and isinstance(game_details_raw['pitchers'], dict):
                        game_info_for_insights.update(game_details_raw['pitchers'])
                    
                    all_game_info_data.append(pd.DataFrame([game_info_for_insights])) 
                else:
                    self.logger.warning(f"No complete game_details found for URL: {game_url_for_test}. Skipping game info and odds.")

                home_team_br = game_info_for_insights.get('home_team')
                away_team_br = game_info_for_insights.get('away_team')
                
                if self.odds_scraper and home_team_br and away_team_br:
                    odds = self.odds_scraper.fetch_all_mlb_odds_for_date(current_game_date) # Removed AWAIT
                    
                    game_odds = odds[(odds['home_team_odds_api'].apply(self.odds_scraper._get_standardized_team_name) == self.odds_scraper._get_standardized_team_name(home_team_br)) &
                                     (odds['away_team_odds_api'].apply(self.odds_scraper._get_standardized_team_name) == self.odds_scraper._get_standardized_team_name(away_team_br))]
                    
                    if not game_odds.empty:
                        all_odds_data.append(game_odds)
                    else:
                        self.logger.info(f"No specific odds found for {home_team_br} vs {away_team_br} on {current_game_date}.")
                elif not self.odds_scraper:
                    self.logger.warning(f"Odds scraper not initialized. Skipping odds fetching for URL: {game_url_for_test}.")
                else:
                    self.logger.warning(f"Missing home/away team from game_details for odds scraping for URL: {game_url_for_test}.")

                results['games_processed'] = 1

            elif fetch_past_games_enabled:
                self.logger.info(f"Running pipeline for recent games, looking back {days_back_to_use} day(s).")
                games_summaries = self.game_scraper.get_recent_games(days_back=days_back_to_use)
                
                if games_summaries:
                    scores_url = self.data_exporter.export_scores_to_google_sheets(games_summaries)
                    results['scores_sheet_url'] = scores_url
                    self.logger.info(f"Scores/Matchups exported to: {scores_url}.")

                    self.logger.info(f"Scraping detailed box scores for {len(games_summaries)} games...")
                    
                    all_api_odds_for_range = pd.DataFrame()
                    if self.odds_scraper:
                        for i in range(days_back_to_use + 1):
                            date_to_fetch = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
                            day_odds = self.odds_scraper.fetch_all_mlb_odds_for_date(date_to_fetch) # Removed AWAIT
                            if not day_odds.empty:
                                all_api_odds_for_range = pd.concat([all_api_odds_for_range, day_odds], ignore_index=True)
                        if not all_api_odds_for_range.empty:
                            self.logger.info(f"Fetched {len(all_api_odds_for_range)} odds records for the last {days_back_to_use} day(s).")
                        else:
                            self.logger.info("No odds found for the specified past date range.")
                    else:
                        self.logger.warning("Odds scraper not initialized. Skipping past odds fetching.")


                    for game_info in games_summaries:
                        game_url = game_info.get('url')
                        current_game_date = game_info.get('date')
                        
                        if game_url:
                            self.logger.info(f"Processing game URL: {game_url}.")
                            batting_df, pitching_df, lineup_df, game_details_raw = self.game_scraper.scrape_box_score(game_url)
                            
                            if current_game_date:
                                if not batting_df.empty and 'game_date' not in batting_df.columns:
                                    batting_df['game_date'] = current_game_date
                                if not pitching_df.empty and 'game_date' not in pitching_df.columns:
                                    pitching_df['game_date'] = current_game_date
                                if not lineup_df.empty and 'game_date' not in lineup_df.columns: 
                                    lineup_df['game_date'] = current_game_date

                            if not batting_df.empty:
                                all_batting_data.append(batting_df)
                            if not pitching_df.empty:
                                all_pitching_data.append(pitching_df)
                            if not lineup_df.empty:
                                all_lineup_data.append(lineup_df)
                            
                            game_info_for_insights = {}
                            if game_details_raw and 'game_info' in game_details_raw:
                                game_info_for_insights.update(game_details_raw['game_info'])
                                game_info_for_insights['game_date'] = current_game_date 

                                if 'home_score' in game_info_for_insights and 'away_score' in game_info_for_insights:
                                    home_score = int(game_info_for_insights['home_score'])
                                    away_score = int(game_info_for_insights['away_score'])
                                    if home_score > away_score:
                                        game_info_for_insights['winner'] = game_info_for_insights.get('home_team')
                                        game_info_for_insights['loser'] = game_info_for_insights.get('away_team')
                                    elif away_score > home_score:
                                        game_info_for_insights['winner'] = game_info_for_insights.get('away_team')
                                        game_info_for_insights['loser'] = game_info_for_insights.get('home_team')
                                    else:
                                        game_info_for_insights['winner'] = 'Tie'
                                        game_info_for_insights['loser'] = 'Tie'

                                if 'pitchers' in game_details_raw and isinstance(game_details_raw['pitchers'], dict):
                                    game_info_for_insights.update(game_details_raw['pitchers'])

                                all_game_info_data.append(pd.DataFrame([game_info_for_insights]))
                            else:
                                self.logger.warning(f"No complete game_details found for game {game_info.get('away_team')} @ {game_info.get('home_team')} on {game_info.get('date')}. Skipping game info and odds.")

                            home_team_br = game_info_for_insights.get('home_team')
                            away_team_br = game_info_for_insights.get('away_team')
                            
                            if self.odds_scraper and current_game_date and home_team_br and away_team_br and not all_api_odds_for_range.empty:
                                home_team_standard = self.odds_scraper._get_standardized_team_name(home_team_br)
                                away_team_standard = self.odds_scraper._get_standardized_team_name(away_team_br)
                                
                                game_odds = all_api_odds_for_range[
                                    (all_api_odds_for_range['game_date_odds'] == current_game_date) &
                                    (all_api_odds_for_range['home_team_odds_api'].apply(self.odds_scraper._get_standardized_team_name) == home_team_standard) &
                                    (all_api_odds_for_range['away_team_odds_api'].apply(self.odds_scraper._get_standardized_team_name) == away_team_standard)
                                ]
                                
                                if not game_odds.empty:
                                    all_odds_data.append(game_odds)
                                    self.logger.info(f"Found odds for {home_team_br} vs {away_team_br} on {current_game_date}.")
                                else:
                                    self.logger.info(f"No matching odds found for {home_team_br} vs {away_team_br} on {current_game_date} in API response.")
                            elif not self.odds_scraper:
                                self.logger.debug("Odds scraper not initialized. Skipping odds fetching for this game.")
                            else:
                                self.logger.debug(f"Missing game date or team names from game_details for odds scraping for game {game_info.get('home_team')} vs {game_info.get('away_team')}.")

                            results['games_processed'] += 1
                        else:
                            self.logger.warning(f"Game info for {game_info.get('away_team')} @ {game_info.get('home_team')} on {game_info.get('date')} has no URL. Skipping detailed scrape.")
                else:
                    self.logger.info("No recent games found to scrape detailed data for.")
            else:
                self.logger.info("Fetching past games is disabled in config. Skipping detailed box score scraping.")

            # --- Fetch Upcoming Odds Section ---
            fetch_upcoming_odds_enabled = self.config.get('pipeline_settings', {}).get('fetch_upcoming_odds', {}).get('enabled', False)
            days_forward_for_odds = self.config.get('pipeline_settings', {}).get('fetch_upcoming_odds', {}).get('days_forward', 2)

            if fetch_upcoming_odds_enabled and self.odds_scraper:
                self.logger.info(f"Fetching upcoming MLB odds for the next {days_forward_for_odds} day(s).")
                for i in range(days_forward_for_odds + 1):
                    target_date = (datetime.now() + timedelta(days=i)).strftime('%Y-%m-%d')
                    self.logger.info(f"Attempting to fetch odds for upcoming games on {target_date}.")
                    upcoming_odds_df = self.odds_scraper.fetch_all_mlb_odds_for_date(target_date) # Removed AWAIT
                    if not upcoming_odds_df.empty:
                        all_odds_data.append(upcoming_odds_df)
                    else:
                        self.logger.info(f"No upcoming odds found for {target_date}.")
            elif fetch_upcoming_odds_enabled and not self.odds_scraper:
                self.logger.warning("Odds scraper not initialized, skipping fetching of upcoming odds.")
            elif not fetch_upcoming_odds_enabled:
                self.logger.info("Fetching upcoming odds is disabled in config.")

            # Combine all collected dataframes
            combined_batting = pd.concat(all_batting_data, ignore_index=True) if all_batting_data else pd.DataFrame()
            combined_pitching = pd.concat(all_pitching_data, ignore_index=True) if all_pitching_data else pd.DataFrame()
            combined_lineup = pd.concat(all_lineup_data, ignore_index=True) if all_lineup_data else pd.DataFrame()
            combined_game_info = pd.concat(all_game_info_data, ignore_index=True) if all_game_info_data else pd.DataFrame()
            
            # Deduplicate combined_odds based on game_id and commence_time_utc
            combined_odds_raw = pd.concat(all_odds_data, ignore_index=True) if all_odds_data else pd.DataFrame()
            if not combined_odds_raw.empty and 'odds_api_game_id' in combined_odds_raw.columns and 'commence_time_utc' in combined_odds_raw.columns:
                combined_odds = combined_odds_raw.drop_duplicates(subset=['odds_api_game_id', 'commence_time_utc'], keep='first')
            else:
                combined_odds = combined_odds_raw 
            
            # Generate LLM insights
            llm_insights_data = [] 
            if self.config.get('llm_insights', {}).get('enabled', False) and not combined_game_info.empty:
                self.logger.info("Generating LLM insights for collected games...")
                # Iterate through each unique game in combined_game_info to generate insights per game
                for _, game_row in combined_game_info.iterrows():
                    game_date = game_row.get('game_date')
                    home_team = game_row.get('home_team')
                    away_team = game_row.get('away_team')

                    if not game_date or not home_team or not away_team:
                        self.logger.warning(f"Skipping insights for incomplete game info: {game_row.to_dict()}")
                        continue

                    # Filter data for the current game
                    current_batting = combined_batting[(combined_batting['game_date'] == game_date) & 
                                                       ((combined_batting['team'] == home_team) | (combined_batting['team'] == away_team))]
                    current_pitching = combined_pitching[(combined_pitching['game_date'] == game_date) & 
                                                         ((combined_pitching['team'] == home_team) | (combined_pitching['team'] == away_team))]
                    current_lineup = combined_lineup[(combined_lineup['game_date'] == game_date) & 
                                                     ((combined_lineup['team'] == home_team) | (combined_lineup['team'] == away_team))]
                    current_game_details_dict = game_row.to_dict() 

                    # Generate insights for this specific game
                    game_specific_insights = self.mlb_insights_generator.generate_insights(
                        batting_df=current_batting,
                        pitching_df=current_pitching,
                        lineup_df=current_lineup,
                        game_details=current_game_details_dict 
                    )
                    
                    # Format insights for the sheet: one row per game, with all insights concatenated
                    formatted_notes = []
                    for category, notes_list in game_specific_insights.items():
                        if notes_list:
                            formatted_notes.append(f"**{category}**:\n" + "\n".join([f"- {note}" for note in notes_list]))
                    
                    if formatted_notes:
                        llm_insights_data.append({
                            'game_date': game_date,
                            'home_team': home_team,
                            'away_team': away_team,
                            'notes': "\n\n".join(formatted_notes)
                        })
                    else:
                        self.logger.info(f"No specific insights generated for {away_team} vs {home_team} on {game_date}.")

                if llm_insights_data:
                    results['insights_records'] = len(llm_insights_data)
                    self.logger.info(f"Generated {len(llm_insights_data)} LLM insights records.")
                else:
                    self.logger.warning("No LLM insights generated for any game.")
            elif self.config.get('llm_insights', {}).get('enabled', False) and combined_game_info.empty:
                self.logger.info("No game info available to generate LLM insights.")
            elif not self.config.get('llm_insights', {}).get('enabled', False):
                self.logger.info("LLM insights generation is disabled in config.")


            if self.config['data_export']['clean_data']:
                self.logger.info("Cleaning collected dataframes.")
                combined_batting = self.game_scraper.clean_data(combined_batting)
                combined_pitching = self.game_scraper.clean_data(combined_pitching)
                combined_lineup = self.game_scraper.clean_data(combined_lineup)
                combined_game_info = self.game_scraper.clean_data(combined_game_info)
                combined_odds = self.game_scraper.clean_data(combined_odds)

            results['batting_records'] = len(combined_batting)
            results['pitching_records'] = len(combined_pitching)
            results['lineup_records'] = len(combined_lineup)
            results['game_info_records'] = len(combined_game_info)
            results['odds_records'] = len(combined_odds)

            llm_insights_df = pd.DataFrame(llm_insights_data) if llm_insights_data else pd.DataFrame()

            if self.config['data_export'].get('export_to_csv', False) or game_url_for_test:
                self.logger.info("Exporting data to CSV.")
                csv_paths = self.data_exporter.export_to_csv(
                    combined_batting, combined_pitching, combined_lineup,
                    self.config['data_export']['output_directory'],
                    for_test_task=bool(game_url_for_test),
                    game_details_df=combined_game_info,
                    odds_df=combined_odds,
                    insights_df=llm_insights_df 
                )
                results['csv_files'] = csv_paths
            else:
                self.logger.info("CSV export is disabled in config.")

            if self.config['data_export'].get('upload_to_google_sheets', True):
                if not combined_batting.empty or not combined_pitching.empty or not combined_lineup.empty or not combined_game_info.empty or not combined_odds.empty or not llm_insights_df.empty:
                    self.logger.info("Uploading data to Google Sheets.")
                    google_sheets_url = await self.data_exporter.upload_to_google_sheets( 
                        combined_batting, combined_pitching, combined_lineup, combined_game_info, combined_odds, llm_insights_df 
                    )
                    results['google_sheets_url'] = google_sheets_url
                    self.logger.info("All detailed stats and betting odds uploaded to Google Sheets.")
                else:
                    self.logger.info("No detailed batting, pitching, lineup, game info, odds, or insights data to upload to Google Sheets.")
            else:
                self.logger.info("Google Sheets upload is disabled in config.")

            results['success'] = True
            self.logger.info("Pipeline completed successfully.")

        except Exception as e:
            error_msg = f"Pipeline failed: {e}"
            results['errors'].append(error_msg)
            self.logger.error(error_msg, exc_info=True)
        finally:
            await self.data_exporter.close_session() 
        return results

def main():
    print("⚾ MLB Data Scraping Pipeline")
    print("="*50)
    print("Starting MLB data collection and Google Sheets upload...")
    print()
    
    credentials_path = 'credentials.json'
    if not os.path.exists(credentials_path):
        print(f"❌ Error: {credentials_path} file not found!")
        print("Please ensure you have your Google Sheets API credentials file.")
        sys.exit(1)
    
    pipeline = MLBPipeline(config_file='config.json')

    test_task_url = None
    days_back_arg = None
    
    if len(sys.argv) > 1:
        if sys.argv[1] == '--test-url' and len(sys.argv) > 2:
            test_task_url = sys.argv[2]
            print(f"Running for specific test URL: {test_task_url}.")
        elif sys.argv[1] == '--days-back' and len(sys.argv) > 2:
            try:
                days_back_arg = int(sys.argv[2])
                print(f"Running for recent games, looking back {days_back_arg} day(s) (override).")
            except ValueError:
                print("Invalid value for --days-back. Using default from config.")
        elif sys.argv[1] == '--demo':
            asyncio.run(demo_mode(pipeline)) 
            sys.exit(0)
        else:
            print("Invalid command line arguments. Using default pipeline settings from config.")
            print("Usage: python main_pipeline.py [--test-url <url>] [--days-back <num_days>] [--demo]")
    else:
        print("Running with pipeline settings from config (default mode)...")
    
    try:
        print("📊 Running data collection pipeline...")
        results = asyncio.run(pipeline.run_pipeline(days_back_override=days_back_arg, game_url_for_test=test_task_url))
        
        print("\n" + "="*50)
        print("📈 PIPELINE RESULTS")
        print("="*50)
        
        if results['success']:
            print("✅ Pipeline completed successfully!")
            print(f"📊 Games processed: {results['games_processed']}")
            print(f"⚾ Batting records: {results['batting_records']}")
            print(f"🥎 Pitching records: {results['pitching_records']}")
            print(f"📝 Lineup records: {results['lineup_records']}")
            print(f"ℹ️ Game Info records: {results['game_info_records']}")
            print(f"💰 Betting Odds records: {results['odds_records']}")
            print(f"🧠 LLM Insights records: {results['insights_records']}") 

            if results['google_sheets_url']:
                print(f"\n🔗 Google Sheets URL:")
                print(f"    {results['google_sheets_url']}")
                print("\n📝 You can now share this link with clients or stakeholders!")
            
            if results['csv_files']:
                print(f"\n📁 CSV Files Generated:")
                for csv_file in results['csv_files']:
                    if os.path.exists(csv_file):
                        print(f"    ✅ {csv_file}")
                    else:
                        print(f"    ❌ {csv_file} (not found)")
            
            print("\n📋 Data Structure Overview:")
            print("-" * 30)
            print("Batting stats columns: game_date, team, player, player_id, AB, R, H, RBI, BB, SO, AVG, OBP, SLG, OPS, etc.")
            print("Pitching stats columns: game_date, team, player, player_id, IP, H, R, ER, BB, SO, ERA, HR, BF, Pit, etc.") # Changed 'pitcher' to 'player'
            print("Lineup columns: game_date, team, batting_order, player, position, player_id")
            print("Game Info columns: game_date, venue, attendance, game_duration, umpires, weather_conditions, field_condition, start_time, WP, LP, SV")
            print("Betting Odds columns: game_date_odds, home_team_odds_api, away_team_odds_api, moneyline_home, moneyline_away, spread_home, spread_home_odds, spread_away, spread_away_odds, total_over, total_over_odds, total_under, total_under_odds, odds_source")
            print("LLM Insights columns: game_date, home_team, away_team, notes") 
                
        else:
            print("❌ Pipeline failed!")
        
        if results['errors']:
            print(f"\n⚠️  Errors encountered:")
            for error in results['errors']:
                print(f"    - {error}")
        
        print("\n" + "="*50)
        print("🎯 Ready for Upwork submission!")
        print("="*50)
        
    except Exception as e:
        print(f"❌ Fatal error during pipeline execution: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

async def demo_mode(pipeline_instance: MLBPipeline): 
    print("🧪 Running in DEMO mode...")
    demo_batting = pd.DataFrame({'game_date': ['2023-07-15'], 'team': ['DemoTeam'], 'player': ['DemoBatter'], 'H': [3], 'AB': [4], 'RBI': [2], 'HR': [1], 'SO': [0], 'BB': [1], 'PA': [5], 'AVG': [0.750], 'OBP': [0.800], 'SLG': [1.500], 'OPS': [2.300]})
    demo_pitching = pd.DataFrame({'game_date': ['2023-07-15'], 'team': ['DemoTeam'], 'player': ['DemoPitcher'], 'IP': [6.0], 'H': [3], 'R': [1], 'ER': [1], 'BB': [1], 'SO': [7], 'ERA': [1.50]})
    demo_lineup = pd.DataFrame({'game_date': ['2023-07-15'], 'team': ['DemoTeam'], 'batting_order': [1], 'player': ['DemoBatter'], 'position': ['CF'], 'player_id': ['demobatt01']})
    demo_game_info = pd.DataFrame([{'game_date': '2023-07-15', 'venue': 'Demo Park', 'attendance': 10000, 'home_team': 'Demo Home', 'away_team': 'Demo Away', 'home_score': 5, 'away_score': 3, 'winner': 'Demo Home', 'loser': 'Demo Away', 'WP': 'WinPitcher', 'LP': 'LossPitcher', 'SV': 'SavePitcher'}])
    demo_odds = pd.DataFrame([{'game_date_odds': '2023-07-15', 'home_team_odds_api': 'Demo Home', 'away_team_odds_api': 'Demo Away', 'moneyline_home': -150, 'moneyline_away': 130, 'spread_home': -1.5, 'spread_home_odds': -110, 'spread_away': 1.5, 'spread_away_odds': -110, 'total_over': 8.5, 'total_over_odds': -110, 'total_under': 8.5, 'total_under_odds': -110, 'odds_source': 'DraftKings'}])

    # Generate insights for demo data
    demo_insights_generator = MLBInsightsGenerator()
    demo_game_info_dict = demo_game_info.iloc[0].to_dict() 
    llm_insights_data = demo_insights_generator.generate_insights(
        batting_df=demo_batting,
        pitching_df=demo_pitching,
        lineup_df=demo_lineup,
        game_details=demo_game_info_dict
    )
    
    # Format insights into a DataFrame for upload
    formatted_notes = []
    for category, notes_list in llm_insights_data.items():
        if notes_list:
            formatted_notes.append(f"**{category}**:\n" + "\n".join([f"- {note}" for note in notes_list]))
    
    demo_insights_df = pd.DataFrame([{
        'game_date': demo_game_info_dict.get('game_date'),
        'home_team': demo_game_info_dict.get('home_team'),
        'away_team': demo_game_info_dict.get('away_team'),
        'notes': "\n\n".join(formatted_notes) if formatted_notes else "No specific insights generated for demo game."
    }])


    try:
        print("Uploading dummy data to Google Sheets...")
        google_sheets_url = await pipeline_instance.data_exporter.upload_to_google_sheets( 
            demo_batting, demo_pitching, demo_lineup, demo_game_info, demo_odds, demo_insights_df 
        )
        if google_sheets_url:
            print(f"Demo data uploaded to: {google_sheets_url}")
        else:
            print("Failed to upload demo data.")
    except Exception as e:
        print(f"Error in demo_mode: {e}")

if __name__ == "__main__":
    main()