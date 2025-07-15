import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import json
import os
import logging
from typing import List, Dict, Union, Optional
from datetime import datetime
from gspread.exceptions import SpreadsheetNotFound, APIError 
import aiohttp # Import aiohttp for async client session

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class DataExporter:
    def __init__(self, config_file: str = 'config.json'):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config = self._load_config(config_file)
        self.output_directory = self.config['data_export'].get('output_directory', 'output')
        os.makedirs(self.output_directory, exist_ok=True)
        self.gc = self._authenticate_google_sheets()

        # Initialize worksheet attributes to None
        self.spreadsheet = None
        self.batting_ws = None
        self.pitching_ws = None
        self.lineup_ws = None
        self.game_info_ws = None
        self.odds_ws = None
        self.scores_ws = None 
        self.insights_ws = None # NEW: For LLM insights sheet

        # Initialize aiohttp session for async requests (used by LLMInsightsGenerator)
        self.session = None # Will be initialized lazily via get_session

        # Call setup_google_sheets_worksheets here to ensure they are always initialized
        if self.gc: 
            self._setup_google_sheets_worksheets()

    async def get_session(self):
        """Lazily initializes and returns an aiohttp ClientSession."""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
            self.logger.info("aiohttp ClientSession initialized.")
        return self.session

    async def close_session(self):
        """Closes the aiohttp ClientSession."""
        if self.session and not self.session.closed:
            await self.session.close()
            self.logger.info("aiohttp ClientSession closed.")

    def _load_config(self, config_file: str) -> Dict:
        """Loads configuration from a JSON file."""
        try:
            with open(config_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.error(f"Config file not found: {config_file}")
            raise
        except json.JSONDecodeError:
            self.logger.error(f"Error decoding JSON from config file: {config_file}")
            raise

    def _authenticate_google_sheets(self):
        """Authenticates with Google Sheets API using service account credentials."""
        try:
            scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"] 
            creds_path = 'credentials.json'
            if not os.path.exists(creds_path):
                self.logger.critical(f"Credentials file not found at '{creds_path}'. Please ensure it exists.")
                raise FileNotFoundError(f"Credentials file not found: {creds_path}")
            
            creds = Credentials.from_service_account_file(creds_path, scopes=scope)
            client = gspread.authorize(creds)
            self.logger.info("Successfully authenticated with Google Sheets API.")
            return client
        except FileNotFoundError as fnfe:
            self.logger.error(f"Authentication failed: {fnfe}")
            return None
        except Exception as e:
            self.logger.error(f"Google Sheets API authentication failed: {e}", exc_info=True)
            return None

    def _setup_google_sheets_worksheets(self):
        """
        Sets up the Google Spreadsheet and individual worksheets.
        Ensures the spreadsheet is publicly viewable and editable by anyone with the link.
        """
        spreadsheet_name = self.config['google_sheets'].get('spreadsheet_name', 'MLB_Baseball_Data')
        
        try:
            self.spreadsheet = self.gc.open(spreadsheet_name)
            self.logger.info(f"Opened existing Google Spreadsheet: '{spreadsheet_name}'")
        except gspread.SpreadsheetNotFound:
            self.logger.info(f"Spreadsheet '{spreadsheet_name}' not found. Creating a new one.")
            try:
                self.spreadsheet = self.gc.create(spreadsheet_name)
                self.logger.info(f"Created new Google Spreadsheet: '{spreadsheet_name}'")
            except Exception as e:
                self.logger.error(f"Error creating spreadsheet '{spreadsheet_name}': {e}", exc_info=True)
                self.spreadsheet = None
                return

        if not self.spreadsheet:
            self.logger.error("Failed to acquire or create Google Spreadsheet. Cannot set up worksheets.")
            return

        self.logger.info(f"Ensuring public EDIT permissions for spreadsheet '{spreadsheet_name}'...")
        try:
            # Check if 'anyone with the link' with 'writer' permission already exists
            permissions = self.spreadsheet.list_permissions()
            public_permission_exists = any(p['type'] == 'anyone' and p['role'] == 'writer' for p in permissions)

            if not public_permission_exists:
                self.spreadsheet.share(None, perm_type='anyone', role='writer') 
                self.logger.info(f"Successfully set 'Anyone with the link' to EDIT access for '{spreadsheet_name}'.")
            else:
                self.logger.info(f"'Anyone with the link' EDIT access already set for '{spreadsheet_name}'.")
        except APIError as e:
            self.logger.warning(f"Failed to set public EDIT permissions for '{spreadsheet_name}': {e.response.text}")
            self.logger.warning("You might need to manually set sharing to 'Anyone with the link can EDIT' via Google Sheets UI.")
        except Exception as e:
            self.logger.error(f"An unexpected error occurred while checking/setting permissions for '{spreadsheet_name}': {e}", exc_info=True)

        # Get or create individual worksheets
        worksheet_names_map = self.config['google_sheets'].get('worksheets', {
            'batting': 'Batting Stats',
            'pitching': 'Pitching Stats',
            'lineups': 'Lineup Info',
            'summary': 'Game Info',
            'betting': 'Betting Odds',
            'scores': 'Daily Scores',
            'insights': 'Game Insights' # NEW: Add insights worksheet name here
        })

        internal_attr_map = {
            'batting': 'batting_ws',
            'pitching': 'pitching_ws',
            'lineups': 'lineup_ws',
            'summary': 'game_info_ws',
            'betting': 'odds_ws',
            'scores': 'scores_ws',
            'insights': 'insights_ws' # NEW: Map to internal attribute
        }

        for config_key, attr_name in internal_attr_map.items():
            sheet_name = worksheet_names_map.get(config_key, config_key.replace('_', ' ').title())
            try:
                worksheet = self.spreadsheet.worksheet(sheet_name)
                setattr(self, attr_name, worksheet)
                self.logger.debug(f"Found worksheet: '{sheet_name}'")
            except gspread.WorksheetNotFound:
                self.logger.info(f"Worksheet '{sheet_name}' not found. Creating it.")
                try:
                    worksheet = self.spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="20")
                    setattr(self, attr_name, worksheet)
                    self.logger.info(f"Created new worksheet: '{sheet_name}'")
                except Exception as e:
                    self.logger.error(f"Error creating worksheet '{sheet_name}': {e}", exc_info=True)
                    setattr(self, attr_name, None)
            except Exception as e:
                self.logger.error(f"Error setting up worksheet '{sheet_name}': {e}", exc_info=True)
                setattr(self, attr_name, None)


    def _update_worksheet_from_df(self, worksheet: gspread.Worksheet, df: pd.DataFrame, sheet_name: str):
        """Updates a Google Sheet worksheet with DataFrame content, appending new data and deduplicating."""
        if df.empty:
            self.logger.info(f"No data to export to '{sheet_name}' worksheet.")
            return

        if worksheet is None:
            self.logger.error(f"Worksheet '{sheet_name}' is not initialized. Cannot export data.")
            return

        try:
            existing_data = []
            try:
                existing_data = worksheet.get_all_values()
            except APIError as e:
                self.logger.warning(f"Could not get existing data for '{sheet_name}' (API Error: {e}). Assuming empty sheet.")

            if not existing_data or not existing_data[0]:
                worksheet.update([df.columns.tolist()] + df.values.tolist())
                self.logger.info(f"Initialized and uploaded {len(df)} rows to '{sheet_name}' worksheet with headers.")
            else:
                existing_header = existing_data[0]
                existing_df = pd.DataFrame(existing_data[1:], columns=existing_header).astype(str)
                
                key_cols = []
                if sheet_name == self.config['google_sheets']['worksheets'].get('batting', 'Batting Stats'):
                    key_cols = ['game_date', 'player', 'team']
                elif sheet_name == self.config['google_sheets']['worksheets'].get('pitching', 'Pitching Stats'):
                    key_cols = ['game_date', 'player', 'team'] # Changed from 'pitcher' to 'player' for consistency if player column is used for pitchers
                elif sheet_name == self.config['google_sheets']['worksheets'].get('lineups', 'Lineup Info'):
                    key_cols = ['game_date', 'player', 'team', 'batting_order']
                elif sheet_name == self.config['google_sheets']['worksheets'].get('summary', 'Game Info'):
                    key_cols = ['game_date', 'home_team', 'away_team']
                elif sheet_name == self.config['google_sheets']['worksheets'].get('betting', 'Betting Odds'):
                    key_cols = ['game_date_odds', 'home_team_odds_api', 'away_team_odds_api']
                elif sheet_name == self.config['google_sheets']['worksheets'].get('scores', 'Daily Scores'):
                    key_cols = ['date', 'home_team', 'away_team']
                elif sheet_name == self.config['google_sheets']['worksheets'].get('insights', 'Game Insights'): # NEW: Insights key columns
                    key_cols = ['game_date', 'home_team', 'away_team']
                
                effective_key_cols = [col for col in key_cols if col in existing_df.columns and col in df.columns]

                if effective_key_cols and len(effective_key_cols) == len(key_cols):
                    df_aligned = df.reindex(columns=existing_header, fill_value=None).astype(str)
                    combined_df = pd.concat([existing_df, df_aligned], ignore_index=True)
                    combined_df = combined_df.drop_duplicates(subset=effective_key_cols, keep='last').astype(str)
                    
                    if combined_df.empty:
                        worksheet.clear()
                        worksheet.update([existing_header])
                        self.logger.info(f"Cleared '{sheet_name}' worksheet. No new unique data after deduplication.")
                    else:
                        worksheet.clear()
                        worksheet.resize(rows=combined_df.shape[0] + 1, cols=combined_df.shape[1])
                        worksheet.update([combined_df.columns.tolist()] + combined_df.values.tolist())
                        self.logger.info(f"Updated '{sheet_name}' worksheet with {len(combined_df)} unique rows.")
                else:
                    self.logger.warning(f"Key columns for deduplication not fully present or correctly identified for '{sheet_name}'. Appending data without smart deduplication.")
                    df_to_append = df.reindex(columns=existing_header, fill_value='').astype(str)
                    worksheet.append_rows(df_to_append.values.tolist())
                    self.logger.info(f"Appended {len(df)} rows to '{sheet_name}' worksheet.")

        except Exception as e:
            self.logger.error(f"Error updating '{sheet_name}' worksheet: {e}", exc_info=True)


    def export_to_csv(self, batting_df: pd.DataFrame, pitching_df: pd.DataFrame, 
                      lineup_df: pd.DataFrame, output_dir: str, for_test_task: bool = False,
                      game_details_df: Optional[pd.DataFrame] = None,
                      odds_df: Optional[pd.DataFrame] = None,
                      insights_df: Optional[pd.DataFrame] = None) -> List[str]: # NEW: Add insights_df
        """Exports scraped data to CSV files."""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        csv_paths = []
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        def save_df_to_csv(df, name):
            if not df.empty:
                if for_test_task:
                    filename = f"{name}_test_task.csv" 
                else:
                    filename = f"{name}_{timestamp}.csv"
                
                file_path = os.path.join(output_dir, filename)
                df.to_csv(file_path, index=False)
                csv_paths.append(file_path)
                self.logger.info(f"Data exported to CSV: {file_path}")
            else:
                self.logger.info(f"No data for {name}, skipping CSV export.")

        save_df_to_csv(batting_df, 'mlb_batting_stats')
        save_df_to_csv(pitching_df, 'mlb_pitching_stats')
        save_df_to_csv(lineup_df, 'mlb_lineup_info')
        if game_details_df is not None:
            save_df_to_csv(game_details_df, 'mlb_game_info')
        if odds_df is not None:
            save_df_to_csv(odds_df, 'mlb_betting_odds')
        if insights_df is not None: # NEW: Save insights to CSV
            save_df_to_csv(insights_df, 'mlb_game_insights')

        return csv_paths


    async def upload_to_google_sheets(self, batting_df: pd.DataFrame, pitching_df: pd.DataFrame, 
                                 lineup_df: pd.DataFrame, game_info_df: pd.DataFrame,
                                 odds_df: pd.DataFrame, insights_df: pd.DataFrame) -> str: # NEW: Add insights_df
        """Uploads all scraped DataFrames to respective Google Sheets."""
        if not self.gc or not self.spreadsheet:
            self.logger.error("Google Sheets client or spreadsheet not initialized. Cannot upload data.")
            return ""

        try:
            self._update_worksheet_from_df(self.batting_ws, batting_df, self.config['google_sheets']['worksheets'].get('batting', 'Batting Stats'))
            self._update_worksheet_from_df(self.pitching_ws, pitching_df, self.config['google_sheets']['worksheets'].get('pitching', 'Pitching Stats'))
            self._update_worksheet_from_df(self.lineup_ws, lineup_df, self.config['google_sheets']['worksheets'].get('lineups', 'Lineup Info'))
            self._update_worksheet_from_df(self.game_info_ws, game_info_df, self.config['google_sheets']['worksheets'].get('summary', 'Game Info'))
            self._update_worksheet_from_df(self.odds_ws, odds_df, self.config['google_sheets']['worksheets'].get('betting', 'Betting Odds'))
            self._update_worksheet_from_df(self.insights_ws, insights_df, self.config['google_sheets']['worksheets'].get('insights', 'Game Insights')) # NEW: Upload insights
            
            return self.spreadsheet.url
        except Exception as e:
            self.logger.error(f"Error uploading data to Google Sheets: {e}", exc_info=True)
            return ""

    def export_scores_to_google_sheets(self, games_summaries: List[Dict]) -> str:
        """Exports daily scores/matchups to a Google Sheet."""
        if not self.gc or not self.spreadsheet:
            self.logger.error("Google Sheets client or spreadsheet not initialized. Cannot export scores.")
            return ""

        if not games_summaries:
            self.logger.info("No game summaries to export to Daily_Scores sheet.")
            return self.spreadsheet.url if self.spreadsheet else ""

        scores_df = pd.DataFrame(games_summaries)
        
        if 'date' in scores_df.columns:
            try:
                scores_df['date'] = pd.to_datetime(scores_df['date']).dt.strftime('%Y-%m-%d')
            except Exception as e:
                self.logger.warning(f"Could not format 'date' column in scores_df: {e}")

        try:
            self._update_worksheet_from_df(self.scores_ws, scores_df, self.config['google_sheets']['worksheets'].get('scores', 'Daily Scores'))
            return self.spreadsheet.url
        except Exception as e:
            self.logger.error(f"Error exporting scores to Google Sheets: {e}", exc_info=True)
            return ""