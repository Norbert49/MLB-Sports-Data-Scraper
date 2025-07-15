import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import re
import time
import logging
import gspread
import os # Added for os.makedirs
from typing import List, Dict, Optional

class MLBDataScraper:
    def __init__(self, config_file='config.json'):
        self.config = self._load_config(config_file)
        self.logger = self._setup_logging()
        self.gsheet_client = self._initialize_google_sheets_client()

    def _load_config(self, config_file):
        import json
        with open(config_file, 'r') as f:
            return json.load(f)

    def _setup_logging(self):
        log_config = self.config['logging']
        logger = logging.getLogger(__name__)
        logger.setLevel(log_config['level'])

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

    def _initialize_google_sheets_client(self):
        try:
            gc = gspread.service_account(filename=self.config['credentials_file'])
            self.logger.info("Google Sheets client initialized successfully")
            return gc
        except Exception as e:
            self.logger.error(f"Failed to initialize Google Sheets client: {e}")
            raise

    def get_recent_games(self, days_back: int = 1) -> List[Dict]:
        games = []
        base_url = self.config['scraping']['base_url']
        user_agent = self.config['scraping'].get('user_agent', 'Mozilla/5.0')
        headers = {'User-Agent': user_agent}
        
        FORCE_2024_FOR_TESTING = False # This was the line you needed to check

        current_system_date = datetime.now()
        if FORCE_2024_FOR_TESTING:
            target_year = 2024
            base_date_for_timedelta = datetime(target_year, current_system_date.month, current_system_date.day)
        else:
            target_year = current_system_date.year
            base_date_for_timedelta = current_system_date

        schedule_url = f"{base_url}/leagues/MLB/{target_year}-schedule.shtml"
        self.logger.info(f"Attempting to fetch yearly schedule from: {schedule_url}")

        try:
            response = requests.get(schedule_url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            self.logger.info(f"Successfully fetched yearly schedule for {target_year}.")

            for i in range(days_back):
                date_to_find = base_date_for_timedelta - timedelta(days=i)
                date_heading_text = date_to_find.strftime('%A, %B %d, %Y')
                self.logger.info(f"Searching for date heading: '{date_heading_text}'")
                
                date_h3 = soup.find('h3', string=date_heading_text)
                if date_h3:
                    self.logger.info(f"Found daily schedule heading for {date_heading_text}")
                    daily_games_container = date_h3.find_parent('div')
                    if daily_games_container:
                        self.logger.debug(f"Found daily games container for {date_heading_text}. Looking for <p class='game'> tags.")
                        game_paragraphs = daily_games_container.find_all('p', class_='game')
                        if game_paragraphs:
                            self.logger.debug(f"Found {len(game_paragraphs)} game paragraphs for {date_heading_text}. Iterating...")
                            for p_tag in game_paragraphs:
                                text = p_tag.text.strip().replace('\n', ' ')
                                matchup = re.match(r"(.+?)\s+\((\d+)\)\s+@\s+(.+?)\s+\((\d+)\)", text)
                                if matchup:
                                    away_team = matchup.group(1).strip()
                                    away_score = int(matchup.group(2))
                                    home_team = matchup.group(3).strip()
                                    home_score = int(matchup.group(4))
                                else:
                                    away_team = home_team = ""
                                    away_score = home_score = None

                                # FIXED: Updated regex for box score link
                                box_score_link = p_tag.find('a', href=re.compile(r'/boxes/[A-Z]{3}/[A-Z]{3}\d{9}\.shtml'))
                                game_url = base_url + box_score_link['href'] if box_score_link else None
                                
                                game_info = {
                                    'date': date_to_find.strftime('%Y-%m-%d'),
                                    'away_team': away_team,
                                    'away_score': away_score,
                                    'home_team': home_team,
                                    'home_score': home_score,
                                    'url': game_url
                                }
                                games.append(game_info)
                                self.logger.info(f"Collected: {away_team} ({away_score}) @ {home_team} ({home_score}) | Box: {game_url}")
                        else:
                            self.logger.warning(f"No game paragraphs (<p class='game'>) found within container for {date_heading_text}")
                    else:
                        self.logger.warning(f"Could not find parent container for date heading: {date_heading_text}")
                else:
                    self.logger.warning(f"No daily schedule heading found for {date_heading_text}")
                time.sleep(self.config['scraping']['delay_between_requests'])
            
            self.logger.info(f"Collected {len(games)} games for {days_back} day(s):")
            for g in games:
                self.logger.info(f"{g['away_team']} ({g['away_score']}) @ {g['home_team']} ({g['home_score']}) | Box: {g['url']}")

        except requests.exceptions.RequestException as e:
            self.logger.error(f"HTTP Error fetching yearly schedule or daily sections: {e}")
        except Exception as e:
            self.logger.error(f"Error parsing yearly schedule for {target_year}: {e}")
        return games

    def scrape_box_score(self, game_url: str) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict):
        self.logger.info(f"Scraping box score from: {game_url}")
        user_agent = self.config['scraping'].get('user_agent', 'Mozilla/5.0')
        headers = {'User-Agent': user_agent}
        retries = 0
        max_retries = self.config['scraping']['max_retries']
        delay = self.config['scraping']['delay_between_requests']

        while retries < max_retries:
            try:
                response = requests.get(game_url, headers=headers)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                self.logger.info(f"Successfully fetched box score for {game_url}")

                batting_data = self._parse_batting_stats(soup)
                pitching_data = self._parse_pitching_stats(soup)
                lineup_data = self._parse_lineup_data(soup)
                
                # NEW: Placeholder for Game-Level Info and Win/Loss/Save Pitchers
                game_info_details = self._parse_game_level_info(soup) # Implement this method
                win_loss_save_pitchers = self._parse_win_loss_save_pitchers(soup) # Implement this method
                
                # Combine into one dict for simpler return, if desired, or return separately
                # For now, just pass them through.
                all_game_details = {
                    'game_info': game_info_details,
                    'pitchers': win_loss_save_pitchers
                }

                return batting_data, pitching_data, lineup_data, all_game_details

            except requests.exceptions.RequestException as e:
                self.logger.error(f"HTTP Error scraping {game_url}: {e}. Retrying...")
                retries += 1
                time.sleep(delay * (retries + 1))
            except Exception as e:
                self.logger.error(f"Error parsing box score for {game_url}: {e}")
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {} # Return empty dict for game details
        self.logger.error(f"Failed to scrape {game_url} after {max_retries} retries.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    def _parse_batting_stats(self, soup: BeautifulSoup) -> pd.DataFrame:
        batting_dfs = []
        batting_tables = soup.find_all('table', id=re.compile(r'^batting'))
        self.logger.debug(f"Found {len(batting_tables)} batting tables.")

        for table in batting_tables:
            team_name_tag = table.find_previous_sibling('div', class_='section_heading').find('h2')
            team_name = team_name_tag.text.strip().replace(' Batting', '') if team_name_tag else 'Unknown Team'
            self.logger.debug(f"Processing batting table for team: {team_name}")

            data = []
            columns = [] # Use this to store data-stat values as column names

            header_row = table.find('thead').find('tr')
            if header_row:
                header_ths = header_row.find_all('th')
                for th in header_ths:
                    stat_name = th.get('data-stat')
                    if stat_name and stat_name != 'rank': # 'rank' is usually an empty column, skip it
                        columns.append(stat_name)
                self.logger.debug(f"Extracted batting headers (data-stat): {columns}")
            else:
                self.logger.warning(f"Could not find header row for batting table of {team_name}.")
                continue

            for row in table.find('tbody').find_all('tr', class_=lambda x: x != 'total_row'): # Exclude 'total_row'
                row_data = {}
                player_th = row.find('th', {'data-stat': 'player'})
                if player_th:
                    player_name = player_th.text.strip()
                    if player_name == 'Team Totals':
                        continue
                    row_data['player'] = player_name
                else:
                    self.logger.warning(f"Could not find player name (th[data-stat='player']) in a batting row for {team_name}. Skipping row.")
                    continue
                
                for col_stat in columns:
                    if col_stat == 'player':
                        continue
                    td_tag = row.find('td', {'data-stat': col_stat})
                    row_data[col_stat] = td_tag.text.strip() if td_tag else ''

                data.append(row_data)

            if data and columns:
                if 'player' in columns:
                    columns.remove('player')
                final_columns = ['player'] + columns

                df = pd.DataFrame(data, columns=final_columns)
                df['team'] = team_name
                
                for col in final_columns:
                    if col not in ['player', 'team']:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                
                batting_dfs.append(df)
            else:
                self.logger.warning(f"No valid data or headers found for batting table from {team_name}.")

        if batting_dfs:
            combined_df = pd.concat(batting_dfs, ignore_index=True)
            if 'game_date' not in combined_df.columns:
                 combined_df['game_date'] = datetime.now().strftime('%Y-%m-%d')
            
            ordered_cols = ['game_date', 'team', 'player'] + [col for col in combined_df.columns if col not in ['game_date', 'team', 'player']]
            return combined_df[ordered_cols]
        return pd.DataFrame()

    

    def _parse_game_level_info(self, soup: BeautifulSoup) -> Dict:
        """
        Parses general game information like venue, attendance, duration, umpires, weather.
        """
        game_details = {}
        
        # Look for the div containing game information (e.g., div with class 'scorebox_meta')
        meta_div = soup.find('div', id='content').find('div', class_='scorebox_meta')
        if meta_div:
            for p_tag in meta_div.find_all('p'):
                text = p_tag.get_text(separator=' ', strip=True)
                if 'Venue:' in text:
                    game_details['venue'] = text.replace('Venue:', '').strip()
                elif 'Attendance:' in text:
                    game_details['attendance'] = text.replace('Attendance:', '').replace(',', '').strip()
                elif 'Time of Game:' in text:
                    game_details['game_duration'] = text.replace('Time of Game:', '').strip()
                elif 'Umpires:' in text:
                    umpires_str = text.replace('Umpires:', '').strip()
                    game_details['umpires'] = [u.strip() for u in umpires_str.split(',')]
                elif 'Field Condition:' in text:
                    game_details['field_condition'] = text.replace('Field Condition:', '').strip()
                elif 'Start Time:' in text:
                    game_details['start_time'] = text.replace('Start Time:', '').strip()
                # Weather info is often in a specific paragraph after "Start Time"
                elif 'Weather:' in text:
                    game_details['weather_conditions'] = text.replace('Weather:', '').strip()

        self.logger.info(f"Extracted game-level details: {game_details}")
        return game_details

    def _parse_win_loss_save_pitchers(self, soup: BeautifulSoup) -> Dict:
        """
        Parses winning pitcher, losing pitcher, and save pitcher.
        """
        pitcher_roles = {
            'WP': None,
            'LP': None,
            'SV': None
        }
        
        # These are usually found in a <p> tag right after the main box score table,
        # or sometimes structured as data attributes.
        # Let's target the div with class 'scorebox_meta' or similar near the bottom
        # of the main score box.
        
        # Find the line that looks like 'WP: Yusei Kikuchi (W-5-6) • LP: Zac Gallen (L-7-10)'
        # or 'SV: Paul Sewald (12)'
        pitcher_info_p = soup.find('p', string=re.compile(r'WP: .* LP: .*|SV: .*'))
        if pitcher_info_p:
            info_text = pitcher_info_p.text.strip()
            
            # WP: Yusei Kikuchi (W-5-6)
            wp_match = re.search(r'WP: ([^ (]+(?: [^ (]+)*) \([^)]+\)', info_text)
            if wp_match:
                pitcher_roles['WP'] = wp_match.group(1).strip()
            
            # LP: Zac Gallen (L-7-10)
            lp_match = re.search(r'LP: ([^ (]+(?: [^ (]+)*) \([^)]+\)', info_text)
            if lp_match:
                pitcher_roles['LP'] = lp_match.group(1).strip()

            # SV: Paul Sewald (12)
            sv_match = re.search(r'SV: ([^ (]+(?: [^ (]+)*) \([^)]+\)', info_text)
            if sv_match:
                pitcher_roles['SV'] = sv_match.group(1).strip()

        self.logger.info(f"Extracted pitcher roles: {pitcher_roles}")
        return pitcher_roles

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        # Common cleaning for string columns
        string_cols = ['player', 'team', 'pitcher', 'position', 'player_id']
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        # Convert numeric columns, coercing errors to NaN and then filling
        # Exclude known string/object columns from numeric conversion attempt
        cols_to_exclude_from_numeric = ['player', 'team', 'game_date', 'pitcher', 'position', 'player_id', 'url']
        numeric_cols = [col for col in df.columns if col not in cols_to_exclude_from_numeric]
        
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df = df.fillna(0) # Fill NaN numeric values with 0 (or appropriate default)

        return df

    def export_to_csv(self, batting_df: pd.DataFrame, pitching_df: pd.DataFrame, lineup_df: pd.DataFrame, output_dir: str, for_test_task: bool = False, game_details_df: Optional[pd.DataFrame] = None) -> List[str]:
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_paths = []

        if for_test_task:
            # Combined data for the test task
            output_file_name = f"mlb_data_combined_test_task.csv"
            output_path = os.path.join(output_dir, output_file_name)
            
            combined_df = pd.DataFrame()
            if not batting_df.empty:
                batting_df['data_type'] = 'batting'
                combined_df = pd.concat([combined_df, batting_df], ignore_index=True)
            if not pitching_df.empty:
                pitching_df['data_type'] = 'pitching'
                combined_df = pd.concat([combined_df, pitching_df], ignore_index=True)
            if not lineup_df.empty:
                lineup_df['data_type'] = 'lineup'
                combined_df = pd.concat([combined_df, lineup_df], ignore_index=True)
            
            if not combined_df.empty:
                combined_df.to_csv(output_path, index=False)
                self.logger.info(f"Combined test task data exported to: {output_path}")
                csv_paths.append(output_path)
            else:
                self.logger.info("No data to export for combined test task CSV.")

        else:
            if not batting_df.empty:
                batting_file = f"mlb_batting_stats_{timestamp}.csv"
                batting_path = os.path.join(output_dir, batting_file)
                batting_df.to_csv(batting_path, index=False)
                self.logger.info(f"Batting stats exported to: {batting_path}")
                csv_paths.append(batting_path)

            if not pitching_df.empty:
                pitching_file = f"mlb_pitching_stats_{timestamp}.csv"
                pitching_path = os.path.join(output_dir, pitching_file)
                pitching_df.to_csv(pitching_path, index=False)
                self.logger.info(f"Pitching stats exported to: {pitching_path}")
                csv_paths.append(pitching_path)

            if not lineup_df.empty:
                lineup_file = f"mlb_lineup_stats_{timestamp}.csv"
                lineup_path = os.path.join(output_dir, lineup_file)
                lineup_df.to_csv(lineup_path, index=False)
                self.logger.info(f"Lineup stats exported to: {lineup_path}")
                csv_paths.append(lineup_path)
            
            if game_details_df is not None and not game_details_df.empty:
                game_info_file = f"mlb_game_info_{timestamp}.csv"
                game_info_path = os.path.join(output_dir, game_info_file)
                game_details_df.to_csv(game_info_path, index=False)
                self.logger.info(f"Game-level info exported to: {game_info_path}")
                csv_paths.append(game_info_path)

        return csv_paths


    def upload_to_google_sheets(self, batting_df: pd.DataFrame, pitching_df: pd.DataFrame, lineup_df: pd.DataFrame, game_details_df: Optional[pd.DataFrame] = None) -> str:
        try:
            sheet_name = self.config['google_sheet_name']
            try:
                spreadsheet = self.gsheet_client.open(sheet_name)
                self.logger.info(f"Opened existing spreadsheet: {sheet_name}")
            except gspread.SpreadsheetNotFound:
                spreadsheet = self.gsheet_client.create(sheet_name)
                self.logger.info(f"Created new spreadsheet: {sheet_name}")

            share_perm_type = self.config['google_sheets']['share_permissions']['type']
            share_role = self.config['google_sheets']['share_permissions']['role']
            try:
                spreadsheet.share('', perm_type=share_perm_type, role=share_role, with_link=True)
                self.logger.info(f"Spreadsheet '{sheet_name}' shared as '{share_role}' with '{share_perm_type}'.")
            except Exception as share_e:
                self.logger.warning(f"Could not explicitly set share permissions (might be already set or issue): {share_e}")

            if not batting_df.empty:
                worksheet_name = self.config['google_sheets']['worksheets'].get('batting', 'Batting Stats')
                self._update_worksheet(spreadsheet, worksheet_name, batting_df, 'Batting Stats')
            else:
                self.logger.info("No batting data to upload to Google Sheets.")

            if not pitching_df.empty:
                worksheet_name = self.config['google_sheets']['worksheets'].get('pitching', 'Pitching Stats')
                self._update_worksheet(spreadsheet, worksheet_name, pitching_df, 'Pitching Stats')
            else:
                self.logger.info("No pitching data to upload to Google Sheets.")

            if not lineup_df.empty:
                worksheet_name = self.config['google_sheets']['worksheets'].get('lineups', 'Lineups')
                self._update_worksheet(spreadsheet, worksheet_name, lineup_df, 'Lineups')
            else:
                self.logger.info("No lineup data to upload to Google Sheets.")
            
            if game_details_df is not None and not game_details_df.empty:
                worksheet_name = self.config['google_sheets']['worksheets'].get('game_info', 'Game Info')
                self._update_worksheet(spreadsheet, worksheet_name, game_details_df, 'Game Information')
            else:
                self.logger.info("No game-level information to upload to Google Sheets.")


            shareable_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet.id}/edit#gid=0"
            self.logger.info(f"Main Spreadsheet URL: {shareable_url}")
            return shareable_url

        except Exception as e:
            self.logger.error(f"Error uploading data to Google Sheets: {e}")
            raise

    def _update_worksheet(self, spreadsheet, worksheet_name, df, data_type_name):
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
            worksheet.clear()
            self.logger.info(f"Cleared existing worksheet: '{worksheet_name}'")
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(worksheet_name, 1000, df.shape[1] + 2)
            self.logger.info(f"Created new worksheet: '{worksheet_name}'")
        
        # Prepare data including headers
        data_to_upload = [df.columns.values.tolist()] + df.values.tolist()
        worksheet.update(data_to_upload)
        self.logger.info(f"Uploaded {len(df)} {data_type_name} records to '{worksheet_name}'.")

    def export_scores_to_google_sheets(self, games: List[Dict]) -> str:
        try:
            sheet_name = self.config['google_sheet_name']
            try:
                spreadsheet = self.gsheet_client.open(sheet_name)
                self.logger.info(f"Opened existing spreadsheet: {sheet_name}")
            except gspread.SpreadsheetNotFound:
                spreadsheet = self.gsheet_client.create(sheet_name)
                self.logger.info(f"Created new spreadsheet: {sheet_name}")

            worksheet_name = self.config['google_sheets']['worksheets'].get('scores', 'Scores')
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
                worksheet.clear()
            except gspread.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(worksheet_name, 1000, 10)

            df = pd.DataFrame(games)
            if not df.empty:
                worksheet.update([df.columns.values.tolist()] + df.values.tolist())
                self.logger.info(f"Uploaded {len(df)} scores/matchups to '{worksheet_name}'")
            else:
                self.logger.info("No scores/matchups to upload.")

            share_perm_type = self.config['google_sheets']['share_permissions']['type']
            share_role = self.config['google_sheets']['share_permissions']['role']
            try:
                spreadsheet.share('', perm_type=share_perm_type, role=share_role, with_link=True)
                self.logger.info(f"Spreadsheet '{sheet_name}' shared as '{share_role}' with '{share_perm_type}'.")
            except Exception as share_e:
                self.logger.warning(f"Could not explicitly set share permissions (might be already set or issue): {share_e}")

            shareable_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet.id}/edit#gid=0"
            self.logger.info(f"Scores Spreadsheet URL: {shareable_url}")
            return shareable_url
        except Exception as e:
            self.logger.error(f"Error uploading scores to Google Sheets: {e}")
            raise


    def run_pipeline(self, days_back: int = 1, game_url_for_test: Optional[str] = None) -> Dict:
        results = {
            'success': False,
            'games_processed': 0,
            'batting_records': 0,
            'pitching_records': 0,
            'lineup_records': 0,
            'game_info_records': 0, # Added for game-level info
            'csv_files': [],
            'google_sheets_url': '',
            'scores_sheet_url': '',
            'errors': []
        }
        try:
            all_batting_data = []
            all_pitching_data = []
            all_lineup_data = []
            all_game_info_data = [] # To store game-level details

            if game_url_for_test:
                self.logger.info(f"Running pipeline for specific test URL: {game_url_for_test}")
                batting_df, pitching_df, lineup_df, game_details = self.scrape_box_score(game_url_for_test)
                if not batting_df.empty:
                    all_batting_data.append(batting_df)
                if not pitching_df.empty:
                    all_pitching_data.append(pitching_df)
                if not lineup_df.empty:
                    all_lineup_data.append(lineup_df)
                
                # Add game-level details to a DataFrame for consistency
                if game_details:
                    # Assuming game_details includes 'game_info' and 'pitchers'
                    # You might want to flatten this or structure it based on how you want to store it.
                    # For simplicity, let's just use game_info for now and add pitcher roles later
                    combined_game_info = game_details['game_info']
                    combined_game_info.update(game_details['pitchers'])
                    
                    # Add a game_date to the game_info for consistency in df
                    # Extract from URL or use a default
                    game_date_match = re.search(r'\d{8}', game_url_for_test)
                    game_date_str = datetime.now().strftime('%Y-%m-%d')
                    if game_date_match:
                        date_obj = datetime.strptime(game_date_match.group(), '%Y%m%d')
                        game_date_str = date_obj.strftime('%Y-%m-%d')
                    combined_game_info['game_date'] = game_date_str

                    all_game_info_data.append(pd.DataFrame([combined_game_info]))

                results['games_processed'] = 1

            else:
                self.logger.info(f"Running pipeline for recent games, looking back {days_back} day(s).")
                games_summaries = self.get_recent_games(days_back=days_back)
                
                if games_summaries:
                    scores_url = self.export_scores_to_google_sheets(games_summaries)
                    results['scores_sheet_url'] = scores_url
                    self.logger.info(f"Scores/Matchups exported to: {scores_url}")

                    self.logger.info(f"Scraping detailed box scores for {len(games_summaries)} games...")
                    for game_info in games_summaries:
                        game_url = game_info.get('url')
                        if game_url:
                            self.logger.info(f"Processing game URL: {game_url}")
                            batting_df, pitching_df, lineup_df, game_details = self.scrape_box_score(game_url)
                            if not batting_df.empty:
                                all_batting_data.append(batting_df)
                            if not pitching_df.empty:
                                all_pitching_data.append(pitching_df)
                            if not lineup_df.empty:
                                all_lineup_data.append(lineup_df)
                            
                            if game_details:
                                combined_game_info = game_details['game_info']
                                combined_game_info.update(game_details['pitchers'])
                                combined_game_info['game_date'] = game_info.get('date') # Use date from game summary
                                all_game_info_data.append(pd.DataFrame([combined_game_info]))

                            results['games_processed'] += 1
                            time.sleep(self.config['scraping']['delay_between_requests'])
                        else:
                            self.logger.warning(f"Game info for {game_info.get('away_team')} @ {game_info.get('home_team')} on {game_info.get('date')} has no URL. Skipping detailed scrape.")
                else:
                    self.logger.info("No recent games found to scrape detailed data for.")

            combined_batting = pd.concat(all_batting_data, ignore_index=True) if all_batting_data else pd.DataFrame()
            combined_pitching = pd.concat(all_pitching_data, ignore_index=True) if all_pitching_data else pd.DataFrame()
            combined_lineup = pd.concat(all_lineup_data, ignore_index=True) if all_lineup_data else pd.DataFrame()
            combined_game_info = pd.concat(all_game_info_data, ignore_index=True) if all_game_info_data else pd.DataFrame()


            if self.config['data_export']['clean_data']:
                combined_batting = self.clean_data(combined_batting)
                combined_pitching = self.clean_data(combined_pitching)
                combined_lineup = self.clean_data(combined_lineup)
                combined_game_info = self.clean_data(combined_game_info) # Clean game info too

            results['batting_records'] = len(combined_batting)
            results['pitching_records'] = len(combined_pitching)
            results['lineup_records'] = len(combined_lineup)
            results['game_info_records'] = len(combined_game_info) # Update count

            if self.config['data_export'].get('export_to_csv', False) or game_url_for_test:
                 csv_paths = self.export_to_csv(combined_batting, combined_pitching, combined_lineup,
                                                 self.config['data_export']['output_directory'],
                                                 for_test_task=bool(game_url_for_test),
                                                 game_details_df=combined_game_info) # Pass game_details_df
                 results['csv_files'] = csv_paths

            if self.config['data_export'].get('upload_to_google_sheets', True):
                if not combined_batting.empty or not combined_pitching.empty or not combined_lineup.empty or not combined_game_info.empty:
                    google_sheets_url = self.upload_to_google_sheets(combined_batting, combined_pitching, combined_lineup, combined_game_info) # Pass game_details_df
                    results['google_sheets_url'] = google_sheets_url
                    self.logger.info("All detailed stats uploaded to Google Sheets.")
                else:
                    self.logger.info("No detailed batting, pitching, lineup, or game info data to upload to Google Sheets.")
            else:
                self.logger.info("Google Sheets upload is disabled in config.")

            results['success'] = True
            self.logger.info("Pipeline completed successfully")

        except Exception as e:
            error_msg = f"Pipeline failed: {e}"
            results['errors'].append(error_msg)
            self.logger.error(error_msg, exc_info=True)
        return results