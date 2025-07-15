import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import json
import os
import logging
from typing import List, Dict, Union, Optional
from datetime import datetime
from gspread.exceptions import SpreadsheetNotFound, APIError # Import specific exceptions

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
        self.scores_ws = None # For scores/matchups sheet

        # Call setup_google_sheets_worksheets here to ensure they are always initialized
        # when an instance of DataExporter is created.
        if self.gc: # Only attempt setup if authentication was successful
            self._setup_google_sheets_worksheets()


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
            # Corrected scope to include 'https://www.googleapis.com/auth/spreadsheets' for broader Sheets access
            scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"] 
            creds_path = 'credentials.json'
            # Check if credentials.json exists
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
        Called once during initialization.
        Ensures the spreadsheet is publicly viewable by anyone with the link.
        """
        spreadsheet_name = self.config['google_sheets'].get('spreadsheet_name', 'MLB_Baseball_Data')
        
        # Try to open the spreadsheet. If not found, create it.
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
                self.spreadsheet = None # Ensure spreadsheet is None if creation fails
                return # Exit if spreadsheet cannot be created

        if not self.spreadsheet: # If spreadsheet creation/opening failed, return
            self.logger.error("Failed to acquire or create Google Spreadsheet. Cannot set up worksheets.")
            return

        # --- IMPORTANT CHANGE: Ensure public sharing is set AFTER spreadsheet is acquired/created ---
        self.logger.info(f"Ensuring public view permissions for spreadsheet '{spreadsheet_name}'...")
        try:
            # Check if 'anyone with the link' permission already exists to avoid redundant calls
            permissions = self.spreadsheet.list_permissions()
            public_permission_exists = any(p['type'] == 'anyone' and p['role'] == 'writer' for p in permissions)

            if not public_permission_exists:
                
                self.spreadsheet.share(None, perm_type='anyone', role='writer')
                self.logger.info(f"Successfully set 'Anyone with the link' to view access for '{spreadsheet_name}'.")
            else:
                self.logger.info(f"'Anyone with the link' view access already set for '{spreadsheet_name}'.")
        except APIError as e:
            self.logger.warning(f"Failed to set public view permissions for '{spreadsheet_name}': {e.response.text}")
            self.logger.warning("You might need to manually set sharing to 'Anyone with the link can view' via Google Sheets UI.")
        except Exception as e:
            self.logger.error(f"An unexpected error occurred while checking/setting permissions for '{spreadsheet_name}': {e}", exc_info=True)
        # --- END IMPORTANT CHANGE ---


        # Get or create individual worksheets
        # Use worksheet names from config if available, otherwise default
        worksheet_names_map = self.config['google_sheets'].get('worksheets', {
            'batting': 'Batting Stats',
            'pitching': 'Pitching Stats',
            'lineups': 'Lineup Info',
            'summary': 'Game Summary', # This was 'Game_Info' in previous code, using 'Game Info' from config
            'betting': 'Betting Odds',
            'scores': 'Daily Scores' # This was 'Daily_Scores' in previous code, using 'Daily Scores' from config
        })

        # Map config keys to internal attribute names
        internal_attr_map = {
            'batting': 'batting_ws',
            'pitching': 'pitching_ws',
            'lineups': 'lineup_ws',
            'summary': 'game_info_ws',
            'betting': 'odds_ws',
            'scores': 'scores_ws'
        }

        for config_key, attr_name in internal_attr_map.items():
            sheet_name = worksheet_names_map.get(config_key, config_key.replace('_', ' ').title()) # Get name from config, or derive
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
                    setattr(self, attr_name, None) # Ensure it's None if creation fails
            except Exception as e:
                self.logger.error(f"Error setting up worksheet '{sheet_name}': {e}", exc_info=True)
                setattr(self, attr_name, None) # Ensure it's None if creation fails


    def _update_worksheet_from_df(self, worksheet: gspread.Worksheet, df: pd.DataFrame, sheet_name: str):
        """Updates a Google Sheet worksheet with DataFrame content, appending new data and deduplicating."""
        if df.empty:
            self.logger.info(f"No data to export to '{sheet_name}' worksheet.")
            return

        if worksheet is None:
            self.logger.error(f"Worksheet '{sheet_name}' is not initialized. Cannot export data.")
            return

        try:
            # Check for existing data and headers
            try:
                existing_data = worksheet.get_all_values()
            except gspread.exceptions.APIError as e:
                self.logger.warning(f"Could not get existing data for '{sheet_name}' (API Error: {e}). Assuming empty sheet.")
                existing_data = []

            if not existing_data or not existing_data[0]: # Sheet is empty or header is missing, add header and then data
                worksheet.update([df.columns.tolist()] + df.values.tolist())
                self.logger.info(f"Initialized and uploaded {len(df)} rows to '{sheet_name}' worksheet with headers.")
            else:
                existing_header = existing_data[0]
                # Read existing data into a DataFrame, handling potential empty rows or type issues
                existing_df = pd.DataFrame(existing_data[1:], columns=existing_header)
                # Convert all columns in existing_df to string to prevent comparison errors
                existing_df = existing_df.astype(str)
                
                # Identify key columns for deduplication
                key_cols = []
                # Use names from config for consistency
                if sheet_name == self.config['google_sheets']['worksheets'].get('batting', 'Batting Stats'):
                    key_cols = ['game_date', 'player', 'team']
                elif sheet_name == self.config['google_sheets']['worksheets'].get('pitching', 'Pitching Stats'):
                    key_cols = ['game_date', 'pitcher', 'team']
                elif sheet_name == self.config['google_sheets']['worksheets'].get('lineups', 'Lineup Info'):
                    key_cols = ['game_date', 'player', 'team', 'batting_order']
                elif sheet_name == self.config['google_sheets']['worksheets'].get('summary', 'Game Summary'):
                    key_cols = ['game_date', 'home_team', 'away_team']
                elif sheet_name == self.config['google_sheets']['worksheets'].get('betting', 'Betting Odds'):
                    key_cols = ['game_date_odds', 'home_team_odds_api', 'away_team_odds_api'] # Use API names for consistency
                elif sheet_name == self.config['google_sheets']['worksheets'].get('scores', 'Daily Scores'):
                    key_cols = ['date', 'home_team', 'away_team']
                
                # Filter out key columns that do not exist in the current DataFrame to avoid errors
                # And ensure all necessary key columns are present in *both* dataframes for deduplication
                effective_key_cols = [col for col in key_cols if col in existing_df.columns and col in df.columns]

                if effective_key_cols and len(effective_key_cols) == len(key_cols): # Ensure all intended key columns are present
                    # Align new DataFrame columns to existing header before concatenation
                    df_aligned = df.reindex(columns=existing_header, fill_value=None)
                    # Convert new DataFrame to string type for concatenation with existing_df
                    df_aligned = df_aligned.astype(str)

                    combined_df = pd.concat([existing_df, df_aligned], ignore_index=True)
                    combined_df = combined_df.drop_duplicates(subset=effective_key_cols, keep='last') # Keep latest entry
                    
                    # Ensure all values are string to prevent gspread type issues
                    combined_df = combined_df.astype(str)

                    # Update the entire sheet with the combined, deduplicated data
                    # Check if combined_df is empty after deduplication; if so, only clear.
                    if combined_df.empty:
                        worksheet.clear()
                        worksheet.update([existing_header]) # Re-add header if no data
                        self.logger.info(f"Cleared '{sheet_name}' worksheet. No new unique data after deduplication.")
                    else:
                        worksheet.clear() # Clear existing content
                        # Resize worksheet if necessary (gspread handles this for update method too)
                        # Ensure there are enough rows and columns for the combined data
                        # This avoids errors if the combined_df is larger than the initial sheet size
                        worksheet.resize(rows=combined_df.shape[0] + 1, cols=combined_df.shape[1])
                        worksheet.update([combined_df.columns.tolist()] + combined_df.values.tolist())
                        self.logger.info(f"Updated '{sheet_name}' worksheet with {len(combined_df)} unique rows.")
                else:
                    self.logger.warning(f"Key columns for deduplication not fully present or correctly identified for '{sheet_name}'. Appending data without smart deduplication.")
                    # Fallback: if keys are not suitable for merge, just append
                    # Ensure new DataFrame columns align with existing header
                    df_to_append = df.reindex(columns=existing_header, fill_value='')
                    # Convert all values to string to prevent gspread type issues
                    df_to_append = df_to_append.astype(str)
                    worksheet.append_rows(df_to_append.values.tolist())
                    self.logger.info(f"Appended {len(df)} rows to '{sheet_name}' worksheet.")

        except Exception as e:
            self.logger.error(f"Error updating '{sheet_name}' worksheet: {e}", exc_info=True)


    def export_to_csv(self, batting_df: pd.DataFrame, pitching_df: pd.DataFrame, 
                      lineup_df: pd.DataFrame, output_dir: str, for_test_task: bool = False,
                      game_details_df: Optional[pd.DataFrame] = None,
                      odds_df: Optional[pd.DataFrame] = None) -> List[str]:
        """Exports scraped data to CSV files."""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        csv_paths = []
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        def save_df_to_csv(df, name):
            if not df.empty:
                # Use a specific filename for test task that overwrites, otherwise use timestamped names
                if for_test_task:
                    filename = f"{name}_test_task.csv" # More specific filename for test task
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

        return csv_paths


    def upload_to_google_sheets(self, batting_df: pd.DataFrame, pitching_df: pd.DataFrame, 
                                 lineup_df: pd.DataFrame, game_info_df: pd.DataFrame,
                                 odds_df: pd.DataFrame) -> str:
        """Uploads all scraped DataFrames to respective Google Sheets."""
        # Check if Google client and spreadsheet are initialized by _setup_google_sheets_worksheets
        if not self.gc or not self.spreadsheet:
            self.logger.error("Google Sheets client or spreadsheet not initialized. Cannot upload data.")
            return ""

        try:
            # Use worksheet names from config for consistency
            self._update_worksheet_from_df(self.batting_ws, batting_df, self.config['google_sheets']['worksheets'].get('batting', 'Batting Stats'))
            self._update_worksheet_from_df(self.pitching_ws, pitching_df, self.config['google_sheets']['worksheets'].get('pitching', 'Pitching Stats'))
            self._update_worksheet_from_df(self.lineup_ws, lineup_df, self.config['google_sheets']['worksheets'].get('lineups', 'Lineup Info'))
            self._update_worksheet_from_df(self.game_info_ws, game_info_df, self.config['google_sheets']['worksheets'].get('summary', 'Game Summary'))
            self._update_worksheet_from_df(self.odds_ws, odds_df, self.config['google_sheets']['worksheets'].get('betting', 'Betting Odds'))
            
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
        
        # Ensure 'date' column is in YYYY-MM-DD format if it's not already
        if 'date' in scores_df.columns:
            try:
                scores_df['date'] = pd.to_datetime(scores_df['date']).dt.strftime('%Y-%m-%d')
            except Exception as e:
                self.logger.warning(f"Could not format 'date' column in scores_df: {e}")

        try:
            # Use worksheet name from config for consistency
            self._update_worksheet_from_df(self.scores_ws, scores_df, self.config['google_sheets']['worksheets'].get('scores', 'Daily Scores'))
            return self.spreadsheet.url
        except Exception as e:
            self.logger.error(f"Error exporting scores to Google Sheets: {e}", exc_info=True)
            return ""