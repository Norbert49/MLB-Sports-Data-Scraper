import json
import logging
import gspread
import pandas as pd
import numpy as np # <-- Make sure this is imported in base_scraper.py
from google.oauth2.service_account import Credentials # <-- Make sure this is imported if used
from typing import List, Dict, Optional, Any

class MLBDataScraperBase:
    """
    Base class for MLB data scraping operations, providing common utilities.
    """
    def __init__(self, config_file: str):
        self.config = self._load_config(config_file)
        self.logger = self._setup_logging()
        self.gsheet_client = self._initialize_google_sheets_client()

    def _load_config(self, config_file: str) -> Dict:
        """Loads configuration from a JSON file."""
        try:
            with open(config_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {config_file}")
        except json.JSONDecodeError:
            raise ValueError(f"Error decoding JSON from config file: {config_file}")

    def _setup_logging(self) -> logging.Logger:
        """Sets up logging for the scraper."""
        log_config = self.config['logging']
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(log_config['level'])

        # Prevent adding multiple handlers if already configured
        if not logger.handlers:
            formatter = logging.Formatter(log_config['format'])

            # File handler
            file_handler = logging.FileHandler(log_config['file'])
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
        return logger

    def _initialize_google_sheets_client(self) -> gspread.Client:
        """Initializes the Google Sheets client."""
        try:
            # Assuming you're using service account with a JSON key file
            # If you were previously using google.oauth2.service_account.Credentials
            # ensure that part of the code is also present or adjusted.
            gc = gspread.service_account(filename=self.config['credentials_file'])
            self.logger.info("Google Sheets client initialized successfully.")
            return gc
        except Exception as e:
            self.logger.error(f"Failed to initialize Google Sheets client: {e}")
            raise

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans a DataFrame by stripping whitespace from string columns
        and converting numeric columns, filling NaNs.
        """
        if df.empty:
            return df

        # Common cleaning for string columns
        string_cols = ['player', 'team', 'pitcher', 'position', 'player_id', 'pitcher_id', 'venue', 'game_duration', 'umpires', 'weather_conditions', 'field_condition', 'start_time', 'WP', 'LP', 'SV']
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        # Convert numeric columns, coercing errors to NaN and then filling
        cols_to_exclude_from_numeric = [col for col in string_cols if col in df.columns] + ['game_date', 'url']
        numeric_cols = [col for col in df.columns if col not in cols_to_exclude_from_numeric]
        
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df = df.fillna(0) # Fill NaN numeric values with 0

        return df

    # This method was missing and caused the AttributeError
    def _sanitize_for_json(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Converts DataFrame values to be JSON-compliant,
        specifically handling NaN, inf, and -inf by converting them to None.
        Also handles specific cases like 'Order' column containing None.
        """
        df_sanitized = df.copy()

        for column in df_sanitized.columns:
            # Check for numeric types (float, int, etc.)
            if pd.api.types.is_numeric_dtype(df_sanitized[column]):
                # Replace inf and -inf with NaN first
                df_sanitized[column] = df_sanitized[column].replace([np.inf, -np.inf], np.nan)
                # Then replace all NaNs (including those from inf/-inf) with None
                df_sanitized[column] = df_sanitized[column].where(pd.notna(df_sanitized[column]), None)
            
            # Explicitly handle potential 'Order' column if it's numeric but has Nones
            # This ensures it's treated as a nullable integer or string
            if column == 'Order':
                try:
                    # Convert to nullable integer, then replace potential NaNs with None
                    df_sanitized[column] = pd.to_numeric(df_sanitized[column], errors='coerce').astype('Int64') # Nullable Integer
                    df_sanitized[column] = df_sanitized[column].where(df_sanitized[column].notna(), None)
                except Exception:
                    # If it cannot be converted to numeric at all, treat as object and handle None
                    df_sanitized[column] = df_sanitized[column].apply(
                        lambda x: None if (pd.isna(x) or (isinstance(x, float) and np.isinf(x))) else x
                    )

            # For object (string/mixed type) columns, ensure no float NaN/inf values are left
            elif pd.api.types.is_object_dtype(df_sanitized[column]):
                df_sanitized[column] = df_sanitized[column].apply(
                    lambda x: None if (isinstance(x, float) and (np.isnan(x) or np.isinf(x))) else x
                )
        
        return df_sanitized


    def _update_worksheet(self, spreadsheet: gspread.Spreadsheet, worksheet_name: str, df: pd.DataFrame, data_type_name: str):
        """
        Helper to update or create a worksheet in Google Sheets.
        """
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
            worksheet.clear()
            self.logger.info(f"Cleared existing worksheet: '{worksheet_name}'.")
        except gspread.WorksheetNotFound:
            # Add a bit of extra rows/cols just in case
            worksheet = spreadsheet.add_worksheet(worksheet_name, rows=df.shape[0] + 50, cols=df.shape[1] + 5)
            self.logger.info(f"Created new worksheet: '{worksheet_name}'.")
        
        data_to_upload = [df.columns.values.tolist()] + df.values.tolist()
        worksheet.update(data_to_upload)
        self.logger.info(f"Uploaded {len(df)} {data_type_name} records to '{worksheet_name}'.")

    def export_scores_to_google_sheets(self, games: List[Dict]) -> str:
        """
        Exports game summaries (scores) to a dedicated Google Sheet.
        """
        try:
            sheet_name = self.config['google_sheet_name']
            try:
                spreadsheet = self.gsheet_client.open(sheet_name)
                self.logger.info(f"Opened existing spreadsheet: {sheet_name}.")
            except gspread.SpreadsheetNotFound:
                spreadsheet = self.gsheet_client.create(sheet_name)
                self.logger.info(f"Created new spreadsheet: {sheet_name}.")

            worksheet_name = self.config['google_sheets']['worksheets'].get('scores', 'Scores')
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
                worksheet.clear()
            except gspread.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(worksheet_name, 1000, 10) # Default size for scores

            df = pd.DataFrame(games)
            if not df.empty:
                # Sanitize the scores DataFrame as well for robustness
                df = self._sanitize_for_json(df) 
                self._update_worksheet(spreadsheet, worksheet_name, df, 'Scores/Matchups')
            else:
                self.logger.info("No scores/matchups to upload.")

            share_perm_type = self.config['google_sheets']['share_permissions']['type']
            share_role = self.config['google_sheets']['share_permissions']['role']
            try:
                permissions = spreadsheet.list_permissions()
                is_already_shared = any(p['type'] == share_perm_type and p['role'] == share_role for p in permissions)
                
                if not is_already_shared:
                    spreadsheet.share('', perm_type=share_perm_type, role=share_role, with_link=True)
                    self.logger.info(f"Spreadsheet '{sheet_name}' shared as '{share_role}' with '{share_perm_type}'.")
                else:
                    self.logger.info(f"Spreadsheet '{sheet_name}' already shared as '{share_role}' with '{share_perm_type}'. Skipping.")
            except Exception as share_e:
                self.logger.warning(f"Could not explicitly set share permissions for scores sheet: {share_e}")

            shareable_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet.id}/edit#gid=0"
            self.logger.info(f"Scores Spreadsheet URL: {shareable_url}")
            return shareable_url
        except Exception as e:
            self.logger.error(f"Error uploading scores to Google Sheets: {e}")
            raise