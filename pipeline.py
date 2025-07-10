import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import gspread
import re
from oauth2client.service_account import ServiceAccountCredentials
import json
from datetime import datetime, timedelta
import time
import logging
import os
from typing import Dict, List, Optional, Tuple

class MLBDataScraper:
    """
    A comprehensive MLB data scraper that extracts box scores and lineups,
    then uploads the data to Google Sheets with shareable links.
    """

    def __init__(self, config_file: str = 'config.json'):
        self.config = self._load_config(config_file)
        self.setup_logging()
        self.gsheet_client = self._setup_google_sheets()

    def _load_config(self, config_file: str) -> Dict:
        try:
            with open(config_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.warning(f"Config file '{config_file}' not found. Using default configuration.")
            return {
                "credentials_file": "credentials.json",
                "google_sheet_name": "MLB Data Analysis",
                "scraping": {
                    "base_url": "https://www.baseball-reference.com",
                    "delay_between_requests": 2,
                    "max_retries": 3,
                    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                },
                "data_export": {
                    "include_timestamp": True,
                    "clean_data": True,
                    "output_directory": "output"
                },
                "google_sheets": {
                    "share_permissions": {
                        "type": "anyone",
                        "role": "reader"
                    },
                    "worksheets": {
                        "batting": "Batting Stats",
                        "pitching": "Pitching Stats",
                        "summary": "Game Summary",
                        "scores": "Scores"
                    }
                },
                "logging": {
                    "level": "INFO",
                    "file": "mlb_scraper.log",
                    "format": "%(asctime)s - %(levelname)s - %(message)s"
                }
            }

    def setup_logging(self):
        if not logging.getLogger(__name__).handlers:
            logging.basicConfig(
                level=logging.INFO,
                format=self.config['logging']['format'],
                handlers=[
                    logging.FileHandler(self.config['logging']['file']),
                    logging.StreamHandler()
                ]
            )
        self.logger = logging.getLogger(__name__)

    def _setup_google_sheets(self) -> gspread.Client:
        try:
            scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
            creds = ServiceAccountCredentials.from_json_keyfile_name(
                self.config['credentials_file'], scope
            )
            client = gspread.authorize(creds)
            self.logger.info("Google Sheets client initialized successfully")
            return client
        except FileNotFoundError:
            self.logger.error(f"Credentials file not found at {self.config['credentials_file']}. Please check your path.")
            raise
        except Exception as e:
            self.logger.error(f"Failed to setup Google Sheets client: {e}")
            raise

    def get_recent_games(self, days_back: int = 1) -> List[Dict]:
        games = []
        base_url = self.config['scraping']['base_url']
        user_agent = self.config['scraping'].get('user_agent', 'Mozilla/5.0')
        headers = {'User-Agent': user_agent}
        FORCE_2024_FOR_TESTING = True
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
                            for p_tag_index, p_tag in enumerate(game_paragraphs):
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
                                box_score_link = p_tag.find('a', href=re.compile(r'/boxes/[A-Z]{3}/\d{8}0\.shtml'))
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

    def scrape_box_score(self, game_url: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        user_agent = self.config['scraping'].get('user_agent', 'Mozilla/5.0')
        headers = {'User-Agent': user_agent}
        try:
            response = requests.get(game_url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            game_info = self._extract_game_info(soup)
            game_info['game_url'] = game_url
            batting_stats = self._extract_batting_stats(soup, game_info)
            pitching_stats = self._extract_pitching_stats(soup, game_info)
            lineups = self._extract_lineups(soup, game_info)
            self.logger.info(f"Successfully scraped box score, pitching, and lineups from {game_url}")
            return batting_stats, pitching_stats, lineups
        except Exception as e:
            self.logger.error(f"Error scraping data from {game_url}: {e}")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    def _extract_game_info(self, soup: BeautifulSoup) -> Dict:
        game_info = {}
        try:
            date_meta_tag = soup.find('meta', {'property': 'og:title'})
            if date_meta_tag and 'content' in date_meta_tag.attrs:
                title_text = date_meta_tag['content']
                date_match = re.search(r'([A-Za-z]+ \d{1,2}, \d{4})', title_text)
                if date_match:
                    game_info['game_date'] = date_match.group(1)
                else:
                    game_info['game_date'] = "Unknown Date"
            scorebox_teams = soup.find_all('div', class_='scorebox_meta')
            if len(scorebox_teams) > 1:
                game_info['visitor_team'] = scorebox_teams[0].find('a').text.strip() if scorebox_teams[0].find('a') else 'Unknown Visitor'
                game_info['home_team'] = scorebox_teams[1].find('a').text.strip() if scorebox_teams[1].find('a') else 'Unknown Home'
            else:
                game_info['visitor_team'] = 'Unknown Visitor'
                game_info['home_team'] = 'Unknown Home'
            game_id_match = re.search(r'/boxes/([A-Z]{3}/\d{8}0)\.shtml', soup.find('link', rel='canonical')['href']) if soup.find('link', rel='canonical') else None
            if game_id_match:
                game_info['game_id'] = game_id_match.group(1)
            else:
                game_info['game_id'] = 'Unknown'
        except Exception as e:
            self.logger.warning(f"Error extracting game info: {e}")
            game_info['game_date'] = "Unknown Date"
            game_info['visitor_team'] = "Unknown Visitor"
            game_info['home_team'] = "Unknown Home"
            game_info['game_id'] = "Unknown"
        return game_info

    def _extract_batting_stats(self, soup: BeautifulSoup, game_info: Dict) -> pd.DataFrame:
        batting_data = []
        team_abbr_home = game_info.get('home_team', 'Unknown')[:3].upper()
        team_abbr_visitor = game_info.get('visitor_team', 'Unknown')[:3].upper()
        game_id_part = game_info.get('game_id', 'Unknown')
        found_tables = []
        if game_info.get('game_id') != 'Unknown':
            for team_abbr in [team_abbr_home, team_abbr_visitor]:
                specific_id = f"box-{team_abbr}{game_id_part.split('/')[-1]}-batting"
                table = soup.find('table', id=specific_id)
                if table:
                    found_tables.append((table, team_abbr))
        if not found_tables:
            tables = soup.find_all('table', class_='stats_table')
            for table in tables:
                if 'id' in table.attrs and 'batting' in table.attrs['id']:
                    team_name = self._get_team_name_from_table(table)
                    found_tables.append((table, team_name))
        for table, team_abbr in found_tables:
            self.logger.debug(f"Processing batting table for team: {team_abbr} (ID: {table.get('id', 'N/A')})")
            try:
                df_list = pd.read_html(str(table))
                if df_list:
                    df = df_list[0]
                    df.columns = df.columns.droplevel(0) if isinstance(df.columns, pd.MultiIndex) else df.columns
                    df.columns = [col.lower().replace('.', '').strip() for col in df.columns]
                    df = df[df['player'].str.contains(r'^[A-Za-z]') == True].copy()
                    df['team'] = team_abbr
                    df['game_date'] = game_info.get('game_date', 'Unknown Date')
                    df['game_url'] = game_info.get('game_url', '')
                    batting_data.append(df)
                    self.logger.debug(f"Added {len(df)} batting records for {team_abbr}")
            except Exception as e:
                self.logger.warning(f"Could not parse batting table for {team_abbr} using pd.read_html: {e}")
                tbody = table.find('tbody')
                if tbody:
                    for row in tbody.find_all('tr'):
                        if 'class' in row.attrs and 'spacer' in row.attrs['class']:
                            continue
                        cells = row.find_all(['td', 'th'])
                        if len(cells) >= 8 and cells[0].name == 'th':
                            player_name = cells[0].text.strip()
                            if 'total' in player_name.lower() or 'team totals' in player_name.lower() or 'batting' in player_name.lower():
                                continue
                            player_data = {
                                'team': team_abbr,
                                'game_date': game_info.get('game_date', ''),
                                'game_url': game_info.get('game_url', ''),
                                'player': player_name,
                                'ab': self._safe_int(cells[1].text),
                                'r': self._safe_int(cells[2].text),
                                'h': self._safe_int(cells[3].text),
                                'rbi': self._safe_int(cells[4].text),
                                'bb': self._safe_int(cells[5].text),
                                'so': self._safe_int(cells[6].text),
                                'avg': self._safe_float(cells[7].text) if len(cells) > 7 else 0.0,
                            }
                            batting_data.append(player_data)
                            self.logger.debug(f"Manually parsed batting for {player_name}")
        return pd.concat(batting_data, ignore_index=True) if batting_data else pd.DataFrame()

    def _extract_pitching_stats(self, soup: BeautifulSoup, game_info: Dict) -> pd.DataFrame:
        pitching_data = []
        team_abbr_home = game_info.get('home_team', 'Unknown')[:3].upper()
        team_abbr_visitor = game_info.get('visitor_team', 'Unknown')[:3].upper()
        game_id_part = game_info.get('game_id', 'Unknown')
        found_tables = []
        if game_info.get('game_id') != 'Unknown':
            for team_abbr in [team_abbr_home, team_abbr_visitor]:
                specific_id = f"box-{team_abbr}{game_id_part.split('/')[-1]}-pitching"
                table = soup.find('table', id=specific_id)
                if table:
                    found_tables.append((table, team_abbr))
        if not found_tables:
            tables = soup.find_all('table', class_='stats_table')
            for table in tables:
                if 'id' in table.attrs and 'pitching' in table.attrs['id']:
                    team_name = self._get_team_name_from_table(table)
                    found_tables.append((table, team_name))
        for table, team_abbr in found_tables:
            self.logger.debug(f"Processing pitching table for team: {team_abbr} (ID: {table.get('id', 'N/A')})")
            try:
                df_list = pd.read_html(str(table))
                if df_list:
                    df = df_list[0]
                    df.columns = df.columns.droplevel(0) if isinstance(df.columns, pd.MultiIndex) else df.columns
                    df.columns = [col.lower().replace('.', '').strip() for col in df.columns]
                    df = df[df['pitcher'].str.contains(r'^[A-Za-z]') == True].copy()
                    df['team'] = team_abbr
                    df['game_date'] = game_info.get('game_date', 'Unknown Date')
                    df['game_url'] = game_info.get('game_url', '')
                    pitching_data.append(df)
                    self.logger.debug(f"Added {len(df)} pitching records for {team_abbr}")
            except Exception as e:
                self.logger.warning(f"Could not parse pitching table for {team_abbr} using pd.read_html: {e}")
                tbody = table.find('tbody')
                if tbody:
                    for row in tbody.find_all('tr'):
                        cells = row.find_all(['td', 'th'])
                        if len(cells) >= 7 and cells[0].name == 'th':
                            pitcher_name = cells[0].text.strip()
                            if 'total' in pitcher_name.lower() or 'team totals' in pitcher_name.lower():
                                continue
                            pitcher_data = {
                                'team': team_abbr,
                                'game_date': game_info.get('game_date', ''),
                                'game_url': game_info.get('game_url', ''),
                                'pitcher': pitcher_name,
                                'ip': cells[1].text.strip(),
                                'h': self._safe_int(cells[2].text),
                                'r': self._safe_int(cells[3].text),
                                'er': self._safe_int(cells[4].text),
                                'bb': self._safe_int(cells[5].text),
                                'so': self._safe_int(cells[6].text),
                                'era': self._safe_float(cells[7].text) if len(cells) > 7 else 0.0,
                            }
                            pitching_data.append(pitcher_data)
                            self.logger.debug(f"Manually parsed pitching for {pitcher_name}")
        return pd.concat(pitching_data, ignore_index=True) if pitching_data else pd.DataFrame()

    def _extract_lineups(self, soup: BeautifulSoup, game_info: Dict) -> pd.DataFrame:
        lineup_data = []
        lineup_divs = soup.find_all('div', id=re.compile(r'batting_order_.*_summary'))
        for div in lineup_divs:
            team_abbr_match = re.search(r'batting_order_([A-Z]{3})_summary', div.get('id', ''))
            team_abbr = team_abbr_match.group(1) if team_abbr_match else 'Unknown Team'
            players_in_order = div.find_all('p')
            for idx, p_tag in enumerate(players_in_order):
                player_line = p_tag.text.strip()
                match = re.match(r'(\d+)\. (.*?) ([A-Z]+)', player_line)
                if match:
                    order = int(match.group(1))
                    player_name = match.group(2).strip()
                    position = match.group(3).strip()
                    lineup_data.append({
                        'team': team_abbr,
                        'game_date': game_info.get('game_date', ''),
                        'game_url': game_info.get('game_url', ''),
                        'batting_order': order,
                        'player': player_name,
                        'position': position
                    })
                elif player_line and 'pitcher' in player_line.lower():
                    pitcher_match = re.search(r'\(P\) (.*)', player_line)
                    if pitcher_match:
                        player_name = pitcher_match.group(1).strip()
                        lineup_data.append({
                            'team': team_abbr,
                            'game_date': game_info.get('game_date', ''),
                            'game_url': game_info.get('game_url', ''),
                            'batting_order': 'N/A',
                            'player': player_name,
                            'position': 'P'
                        })
                else:
                    self.logger.debug(f"Could not parse lineup line: {player_line}")
        return pd.DataFrame(lineup_data)

    def _get_team_name_from_table(self, table) -> str:
        if 'id' in table.attrs:
            table_id = table.attrs['id']
            team_match = re.search(r'box-([A-Z]{3})\d{8}0-.*', table_id)
            if team_match:
                return team_match.group(1)
        caption = table.find('caption')
        if caption:
            return caption.text.split(' ')[0].strip()
        return 'Unknown'

    def _safe_int(self, value: str) -> int:
        try:
            return int(value.strip())
        except (ValueError, AttributeError):
            return 0

    def _safe_float(self, value: str) -> float:
        try:
            return float(value.strip())
        except (ValueError, AttributeError):
            return 0.0

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        df = df.dropna(how='all')
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                df[col] = df[col].fillna(0)
            elif pd.api.types.is_object_dtype(df[col]):
                df[col] = df[col].fillna('')
        if self.config['data_export']['include_timestamp']:
            df['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return df

    def upload_to_google_sheets(self, batting_df: pd.DataFrame,
                                 pitching_df: pd.DataFrame,
                                 lineup_df: pd.DataFrame) -> str:
        try:
            sheet_name = self.config['google_sheet_name']
            try:
                spreadsheet = self.gsheet_client.open(sheet_name)
                self.logger.info(f"Opened existing spreadsheet: {sheet_name}")
            except gspread.SpreadsheetNotFound:
                spreadsheet = self.gsheet_client.create(sheet_name)
                self.logger.info(f"Created new spreadsheet: {sheet_name}")
            if not batting_df.empty:
                worksheet_name = self.config['google_sheets']['worksheets'].get('batting', 'Batting Stats')
                try:
                    batting_worksheet = spreadsheet.worksheet(worksheet_name)
                    batting_worksheet.clear()
                except gspread.WorksheetNotFound:
                    batting_worksheet = spreadsheet.add_worksheet(worksheet_name, 1000, 26)
                batting_worksheet.update([batting_df.columns.values.tolist()] +
                                         batting_df.values.tolist())
                self.logger.info(f"Uploaded {len(batting_df)} batting records to '{worksheet_name}'")
            if not pitching_df.empty:
                worksheet_name = self.config['google_sheets']['worksheets'].get('pitching', 'Pitching Stats')
                try:
                    pitching_worksheet = spreadsheet.worksheet(worksheet_name)
                    pitching_worksheet.clear()
                except gspread.WorksheetNotFound:
                    pitching_worksheet = spreadsheet.add_worksheet(worksheet_name, 1000, 26)
                pitching_worksheet.update([pitching_df.columns.values.tolist()] +
                                         pitching_df.values.tolist())
                self.logger.info(f"Uploaded {len(pitching_df)} pitching records to '{worksheet_name}'")
            if not lineup_df.empty:
                worksheet_name = self.config['google_sheets']['worksheets'].get('lineups', 'Lineups')
                try:
                    lineup_worksheet = spreadsheet.worksheet(worksheet_name)
                    lineup_worksheet.clear()
                except gspread.WorksheetNotFound:
                    lineup_worksheet = spreadsheet.add_worksheet(worksheet_name, 500, 10)
                lineup_worksheet.update([lineup_df.columns.values.tolist()] +
                                        lineup_df.values.tolist())
                self.logger.info(f"Uploaded {len(lineup_df)} lineup records to '{worksheet_name}'")
            share_perm_type = self.config['google_sheets']['share_permissions']['type']
            share_role = self.config['google_sheets']['share_permissions']['role']
            try:
                spreadsheet.share('', perm_type=share_perm_type, role=share_role, with_link=True)
                self.logger.info(f"Spreadsheet '{sheet_name}' shared as '{share_role}' with '{share_perm_type}'.")
            except Exception as share_e:
                self.logger.warning(f"Could not explicitly set share permissions (might be already set or issue): {share_e}")
            shareable_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet.id}/edit#gid=0"
            self.logger.info(f"Spreadsheet URL: {shareable_url}")
            return shareable_url
        except Exception as e:
            self.logger.error(f"Error uploading to Google Sheets: {e}")
            raise

    def export_to_csv(self, batting_df: pd.DataFrame, pitching_df: pd.DataFrame,
                      lineup_df: pd.DataFrame, output_dir: str = 'output',
                      for_test_task: bool = False) -> List[str]:
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        generated_csv_files = []
        if for_test_task:
            combined_csv_path = os.path.join(output_dir, f'mlb_boxscore_lineup_TEST_TASK_{timestamp}.csv')
            with open(combined_csv_path, 'w', newline='') as f:
                f.write(f"--- MLB Box Score and Lineup Data for a Recent Game ---\n")
                f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                if not batting_df.empty:
                    f.write("=== Batting Stats ===\n")
                    cols_to_exclude = ['game_url', 'scraped_at']
                    batting_df.drop(columns=[col for col in cols_to_exclude if col in batting_df.columns], inplace=True)
                    batting_df.to_csv(f, index=False, mode='a')
                    f.write("\n\n")
                if not pitching_df.empty:
                    f.write("=== Pitching Stats ===\n")
                    cols_to_exclude = ['game_url', 'scraped_at']
                    pitching_df.drop(columns=[col for col in cols_to_exclude if col in pitching_df.columns], inplace=True)
                    pitching_df.to_csv(f, index=False, mode='a')
                    f.write("\n\n")
                if not lineup_df.empty:
                    f.write("=== Lineups ===\n")
                    cols_to_exclude = ['game_url', 'scraped_at']
                    lineup_df.drop(columns=[col for col in cols_to_exclude if col in lineup_df.columns], inplace=True)
                    lineup_df.to_csv(f, index=False, mode='a')
                    f.write("\n")
            generated_csv_files.append(combined_csv_path)
            self.logger.info(f"Combined test task CSV exported to: {combined_csv_path}")
        else:
            batting_csv = os.path.join(output_dir, f'mlb_batting_{timestamp}.csv')
            pitching_csv = os.path.join(output_dir, f'mlb_pitching_{timestamp}.csv')
            lineup_csv = os.path.join(output_dir, f'mlb_lineup_{timestamp}.csv')
            if not batting_df.empty:
                batting_df.to_csv(batting_csv, index=False)
                generated_csv_files.append(batting_csv)
                self.logger.info(f"Batting stats exported to: {batting_csv}")
            if not pitching_df.empty:
                pitching_df.to_csv(pitching_csv, index=False)
                generated_csv_files.append(pitching_csv)
                self.logger.info(f"Pitching stats exported to: {pitching_csv}")
            if not lineup_df.empty:
                lineup_df.to_csv(lineup_csv, index=False)
                generated_csv_files.append(lineup_csv)
                self.logger.info(f"Lineup stats exported to: {lineup_csv}")
        return generated_csv_files

    def run_pipeline(self, days_back: int = 1, game_url_for_test: Optional[str] = None) -> Dict:
        results = {
            'success': False,
            'games_processed': 0,
            'batting_records': 0,
            'pitching_records': 0,
            'lineup_records': 0,
            'csv_files': [],
            'google_sheets_url': '',
            'scores_sheet_url': '',
            'errors': []
        }
        try:
            all_batting_data = []
            all_pitching_data = []
            all_lineup_data = []
            if game_url_for_test:
                self.logger.info(f"Running pipeline for specific test URL: {game_url_for_test}")
                batting_df, pitching_df, lineup_df = self.scrape_box_score(game_url_for_test)
                if not batting_df.empty or not pitching_df.empty or not lineup_df.empty:
                    if self.config['data_export']['clean_data']:
                        batting_df = self.clean_data(batting_df)
                        pitching_df = self.clean_data(pitching_df)
                        lineup_df = self.clean_data(lineup_df)
                    results['batting_records'] = len(batting_df)
                    results['pitching_records'] = len(pitching_df)
                    results['lineup_records'] = len(lineup_df)
                    results['games_processed'] = 1
                    csv_paths = self.export_to_csv(batting_df, pitching_df, lineup_df,
                                                    self.config['data_export']['output_directory'],
                                                    for_test_task=True)
                    results['csv_files'] = csv_paths
                    results['success'] = True
                    self.logger.info("Test task pipeline completed successfully.")
                else:
                    results['errors'].append(f"Failed to scrape data from the provided test URL: {game_url_for_test}")
            else:
                games = self.get_recent_games(days_back)
                if not games:
                    results['errors'].append("No games found for the specified date range")
                    return results

                # Always export scores/matchups to Google Sheets
                scores_sheet_url = self.export_scores_to_google_sheets(games)
                results['scores_sheet_url'] = scores_sheet_url

                for game in games:
                    try:
                        if not game.get('url'):
                            self.logger.info(f"Skipping box score scrape for {game['away_team']} @ {game['home_team']} (no box score link)")
                            continue
                        batting_df, pitching_df, lineup_df = self.scrape_box_score(game['url'])
                        if not batting_df.empty:
                            all_batting_data.append(batting_df)
                        if not pitching_df.empty:
                            all_pitching_data.append(pitching_df)
                        if not lineup_df.empty:
                            all_lineup_data.append(lineup_df)
                        results['games_processed'] += 1
                        time.sleep(self.config['scraping']['delay_between_requests'])
                    except Exception as e:
                        error_msg = f"Error processing game {game['url']}: {e}"
                        results['errors'].append(error_msg)
                        self.logger.error(error_msg)
                combined_batting = pd.concat(all_batting_data, ignore_index=True) if all_batting_data else pd.DataFrame()
                combined_pitching = pd.concat(all_pitching_data, ignore_index=True) if all_pitching_data else pd.DataFrame()
                combined_lineup = pd.concat(all_lineup_data, ignore_index=True) if all_lineup_data else pd.DataFrame()
                if self.config['data_export']['clean_data']:
                    combined_batting = self.clean_data(combined_batting)
                    combined_pitching = self.clean_data(combined_pitching)
                    combined_lineup = self.clean_data(combined_lineup)
                results['batting_records'] = len(combined_batting)
                results['pitching_records'] = len(combined_pitching)
                results['lineup_records'] = len(combined_lineup)
                csv_paths = self.export_to_csv(combined_batting, combined_pitching, combined_lineup,
                                                self.config['data_export']['output_directory'],
                                                for_test_task=False)
                results['csv_files'] = csv_paths
                if not combined_batting.empty or not combined_pitching.empty or not combined_lineup.empty:
                    google_sheets_url = self.upload_to_google_sheets(combined_batting, combined_pitching, combined_lineup)
                    results['google_sheets_url'] = google_sheets_url
                results['success'] = True
                self.logger.info("Pipeline completed successfully")
        except Exception as e:
            error_msg = f"Pipeline failed: {e}"
            results['errors'].append(error_msg)
            self.logger.error(error_msg)
        return results