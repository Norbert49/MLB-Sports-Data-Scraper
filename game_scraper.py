import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import logging
import json
import re
from typing import Tuple, Dict, List, Any, Optional

# Import parsers
from batting_parser import BattingParser
from pitching_parser import PitchingParser
from lineup_parser import LineupParser
from game_info_parser import GameInfoParser

# Set up logging for the module
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class GameScraper:
    def __init__(self, config_file: str = 'config.json'):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config = self._load_config(config_file)
        
        # Initialize parsers
        self.batting_parser = BattingParser()
        self.pitching_parser = PitchingParser()
        self.lineup_parser = LineupParser()
        self.game_info_parser = GameInfoParser()

        # Get scraping settings from config
        self.base_url = self.config['scraping']['base_url'] # Corrected: Access 'scraping' key
        self.delay_between_requests = self.config['scraping'].get('delay_between_requests', 2)
        self.max_retries = self.config['scraping'].get('max_retries', 3)
        self.user_agent = self.config['scraping'].get('user_agent', 'Mozilla/5.0')
        self.force_test_year = self.config['scraping'].get('force_test_year', False)
        
        self.logger.info("GameScraper initialized successfully.")

    def _load_config(self, config_file: str) -> Dict:
        """Loads configuration from a JSON file."""
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
            self.logger.info(f"Configuration loaded from {config_file}.")
            return config
        except FileNotFoundError:
            self.logger.error(f"Config file not found at {config_file}")
            raise
        except json.JSONDecodeError:
            self.logger.error(f"Error decoding JSON from {config_file}")
            raise

    def _fetch_html(self, url: str) -> Optional[str]:
        """Fetches HTML content from a given URL with retries."""
        headers = {'User-Agent': self.user_agent}
        for attempt in range(self.max_retries):
            try:
                self.logger.debug(f"Fetching URL: {url} (Attempt {attempt + 1}/{self.max_retries})")
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
                return response.text
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request failed for {url}: {e}")
                if attempt < self.max_retries - 1:
                    self.logger.info(f"Retrying in {self.delay_between_requests} seconds...")
                    requests.time.sleep(self.delay_between_requests)
                else:
                    self.logger.error(f"Failed to fetch {url} after {self.max_retries} attempts.")
                    return None
        return None

    def get_recent_games(self, days_back: int = 1) -> List[Dict]:
        """
        Fetches a summary of recent MLB games from Baseball-Reference.com.
        Returns a list of dictionaries, each containing game date, teams, score, and URL.
        """
        all_games_summary = []
        today = datetime.now()

        for i in range(days_back + 1):
            target_date = today - timedelta(days=i)
            
            # If force_test_year is enabled, override the year to 2025
            year_to_use = 2025 if self.force_test_year else target_date.year
            
            # Construct the daily schedule URL
            schedule_url = f"{self.base_url}/boxes/{(target_date.strftime('%Y-%m-%d')).replace('-', '')}0.shtml"
            # Baseball-Reference's daily schedule page format:
            # e.g., https://www.baseball-reference.com/boxes/202507120.shtml
            # The actual daily schedule page is usually /daily/YYYY/MM/DD.shtml or similar.
            # Let's use the standard daily schedule page for more robust game finding.
            daily_schedule_url = f"{self.base_url}/boxes/?year={year_to_use}&month={target_date.month}&day={target_date.day}"
            
            self.logger.info(f"Fetching daily schedule for {target_date.strftime('%Y-%m-%d')} from {daily_schedule_url}")
            html_content = self._fetch_html(daily_schedule_url)

            if html_content:
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Find all game boxes on the daily schedule page
                game_summaries_divs = soup.find_all('div', class_='game_summaries')
                
                if not game_summaries_divs:
                    self.logger.info(f"No game summaries found for {target_date.strftime('%Y-%m-%d')}.")
                    continue

                for game_summary_div in game_summaries_divs:
                    box_score_link = game_summary_div.find('a', string='Box Score')
                    if box_score_link and 'href' in box_score_link.attrs:
                        game_url = self.base_url + box_score_link['href']
                        
                        # Extract teams and score
                        # Find the scorebox (usually a table or div with score info)
                        scorebox = game_summary_div.find('table', class_='teams')
                        if scorebox:
                            teams = scorebox.find_all('a')
                            scores = scorebox.find_all('td', class_='right')
                            
                            if len(teams) >= 2 and len(scores) >= 2:
                                away_team_name = teams[0].text.strip()
                                home_team_name = teams[1].text.strip()
                                away_score = scores[0].text.strip()
                                home_score = scores[1].text.strip()
                                
                                all_games_summary.append({
                                    'date': target_date.strftime('%Y-%m-%d'),
                                    'away_team': away_team_name,
                                    'home_team': home_team_name,
                                    'score': f"{away_score}-{home_score}",
                                    'url': game_url
                                })
                            else:
                                self.logger.warning(f"Could not parse teams/scores for a game summary on {target_date.strftime('%Y-%m-%d')}.")
                        else:
                            self.logger.warning(f"Could not find scorebox for a game summary on {target_date.strftime('%Y-%m-%d')}.")
            else:
                self.logger.warning(f"Could not fetch daily schedule for {target_date.strftime('%Y-%m-%d')}.")
        
        if not all_games_summary:
            self.logger.info(f"No game summaries found for the last {days_back} day(s).")
        else:
            self.logger.info(f"Found {len(all_games_summary)} game summaries for the last {days_back} day(s).")
        return all_games_summary


    def scrape_box_score(self, game_url: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict]:
        """
        Scrapes detailed box score data, lineups, and game information for a single MLB game.
        Returns DataFrames for batting, pitching, lineups, and a dictionary for game details.
        """
        self.logger.info(f"Scraping box score from: {game_url}")
        html_content = self._fetch_html(game_url)

        if not html_content:
            self.logger.error(f"Failed to retrieve HTML for game URL: {game_url}")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

        soup = BeautifulSoup(html_content, 'html.parser')
        self.logger.info(f"Successfully fetched box score for {game_url}")

        batting_df = self.batting_parser.parse_batting_stats(soup)
        pitching_df = self.pitching_parser.parse_pitching_stats(soup)
        lineup_df = self.lineup_parser.parse_lineups(soup)
        
        # Parse game-level info and pitcher roles
        game_info = self.game_info_parser.parse_game_level_info(soup)
        pitcher_roles = self.game_info_parser.parse_win_loss_save_pitchers(soup)
        
        # Combine game info and pitcher roles into a single dictionary
        game_details = {'game_info': game_info, 'pitchers': pitcher_roles}

        return batting_df, pitching_df, lineup_df, game_details

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Performs basic data cleaning: converts all columns to string type.
        This is a generic cleaner for export consistency.
        """
        if df.empty:
            return df
        
        # Convert all columns to string type to avoid GSheets API type issues
        # and ensure consistency in exported CSVs.
        return df.astype(str)