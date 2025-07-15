import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional

# Set up logging for the module
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class OddsScraper:
    def __init__(self, api_key: str, config: Optional[Dict] = None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.api_key = api_key
        self.config = config if config is not None else {} # Store the full config

        # Load parameters from config.json, with fallbacks to defaults
        # Access the 'odds_api' section of the config
        odds_config_section = self.config.get('odds_api', {})

        self.base_url = odds_config_section.get(
            'base_url', "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/"
        )
        self.regions = odds_config_section.get('regions', "us")
        self.markets = odds_config_section.get('markets', "h2h,spreads,totals")
        self.odds_format = odds_config_section.get('odds_format', "decimal")
        self.date_format = odds_config_section.get('date_format', "iso")
        
        # Load team name map from config, or use a comprehensive default if not provided
        self.team_name_map = odds_config_section.get('team_name_map', self._get_default_team_name_map())
        
        # Log loaded config for verification
        self.logger.info(f"OddsScraper initialized with API Key (last 4 digits): ...{self.api_key[-4:]}")
        self.logger.info(f"Base URL: {self.base_url}, Regions: {self.regions}, Markets: {self.markets}")

    def _get_default_team_name_map(self) -> Dict[str, str]:
        """Provides a comprehensive default mapping for MLB teams."""
        return {
            'ANA': 'Los Angeles Angels',
            'LAA': 'Los Angeles Angels',
            'AZ': 'Arizona Diamondbacks',
            'ARI': 'Arizona Diamondbacks',
            'ATL': 'Atlanta Braves',
            'BAL': 'Baltimore Orioles',
            'BOS': 'Boston Red Sox',
            'CHC': 'Chicago Cubs',
            'CWS': 'Chicago White Sox',
            'CHW': 'Chicago White Sox',
            'CIN': 'Cincinnati Reds',
            'CLE': 'Cleveland Guardians',
            'COL': 'Colorado Rockies',
            'DET': 'Detroit Tigers',
            'HOU': 'Houston Astros',
            'KC': 'Kansas City Royals',
            'KCR': 'Kansas City Royals',
            'LA': 'Los Angeles Dodgers', 
            'LAD': 'Los Angeles Dodgers',
            'MIA': 'Miami Marlins',
            'MIL': 'Milwaukee Brewers',
            'MIN': 'Minnesota Twins',
            'NYM': 'New York Mets',
            'NYY': 'New York Yankees',
            'OAK': 'Oakland Athletics',
            'PHI': 'Philadelphia Phillies',
            'PIT': 'Pittsburgh Pirates',
            'SD': 'San Diego Padres',
            'SDP': 'San Diego Padres',
            'SEA': 'Seattle Mariners',
            'SF': 'San Francisco Giants',
            'SFG': 'San Francisco Giants',
            'STL': 'St. Louis Cardinals',
            'TB': 'Tampa Bay Rays',
            'TBR': 'Tampa Bay Rays',
            'TEX': 'Texas Rangers',
            'TOR': 'Toronto Blue Jays',
            'WSH': 'Washington Nationals',
            'WAS': 'Washington Nationals',
            # Add mappings for "National League" and "American League" for All-Star Game
            "National League": "National League",
            "American League": "American League"
        }

    def _get_standardized_team_name(self, team_input: str) -> str:
        """
        Converts various team name formats to a standardized one based on the internal map.
        Performs case-insensitive lookup.
        """
        # First, try direct lookup in the map (case-sensitive as map keys might be specific)
        if team_input in self.team_name_map:
            return self.team_name_map[team_input]
        
        # Then, try case-insensitive lookup
        team_input_lower = team_input.lower()
        for key, value in self.team_name_map.items():
            if key.lower() == team_input_lower:
                return value
            if value.lower() == team_input_lower: # Check if input is already a standardized name
                return value
        
        self.logger.warning(f"No standardized mapping found for team: '{team_input}'. Using original name.")
        return team_input

    def fetch_all_mlb_odds_for_date(self, target_date: str) -> pd.DataFrame:
        """
        Fetches all available MLB odds for games whose commence_time matches the target_date.
        It uses parameters loaded from config (regions, markets, oddsFormat, dateFormat).
        target_date: Expected format 'YYYY-MM-DD' for date filtering.
        """
        all_odds_data = []

        params = {
            "apiKey": self.api_key,
            "regions": self.regions,
            "markets": self.markets,
            "oddsFormat": self.odds_format,
            "dateFormat": self.date_format
        }

        try:
            self.logger.info(f"Attempting to fetch odds for all MLB games for date {target_date}.")
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            json_data = response.json()

            if not json_data:
                self.logger.info(f"No games found in API response for MLB.")
                return pd.DataFrame()

            self.logger.debug(f"API response received: {len(json_data)} games.")

            try:
                target_dt_obj = datetime.strptime(target_date, '%Y-%m-%d').date()
            except ValueError:
                self.logger.error(f"Invalid target_date format: {target_date}. Expected YYYY-MM-DD.")
                return pd.DataFrame()

            for game in json_data:
                game_commence_time_str = game.get('commence_time')
                
                try:
                    game_commence_dt_obj = datetime.fromisoformat(game_commence_time_str.replace('Z', '+00:00')).date()
                except ValueError as e:
                    self.logger.warning(f"Could not parse commence_time '{game_commence_time_str}' for game ID {game.get('id')}: {e}")
                    continue

                if game_commence_dt_obj != target_dt_obj:
                    self.logger.debug(f"Skipping game {game.get('id')} ({game.get('home_team')} vs {game.get('away_team')}) - date mismatch: {game_commence_dt_obj} != {target_dt_obj}")
                    continue 
                
                home_team_api = game.get('home_team')
                away_team_api = game.get('away_team')

                home_team_standard = self._get_standardized_team_name(home_team_api)
                away_team_standard = self._get_standardized_team_name(away_team_api)

                odds_entry = {
                    'game_date_odds': target_date,
                    'home_team_odds_api': home_team_standard,
                    'away_team_odds_api': away_team_standard,
                    'odds_api_game_id': game.get('id'),
                    'commence_time_utc': game_commence_time_str
                }

                for bookmaker in game.get('bookmakers', []):
                    bookmaker_key = bookmaker.get('key')
                    if not bookmaker_key:
                        continue

                    for market in bookmaker.get('markets', []):
                        market_key = market.get('key')
                        if not market_key:
                            continue

                        if market_key == 'h2h':
                            for outcome in market.get('outcomes', []):
                                if outcome.get('name') == home_team_api:
                                    odds_entry[f'moneyline_home_{bookmaker_key}'] = outcome.get('price')
                                elif outcome.get('name') == away_team_api:
                                    odds_entry[f'moneyline_away_{bookmaker_key}'] = outcome.get('price')

                        elif market_key == 'spreads':
                            for outcome in market.get('outcomes', []):
                                if outcome.get('name') == home_team_api:
                                    odds_entry[f'spread_home_point_{bookmaker_key}'] = outcome.get('point')
                                    odds_entry[f'spread_home_price_{bookmaker_key}'] = outcome.get('price')
                                elif outcome.get('name') == away_team_api:
                                    odds_entry[f'spread_away_point_{bookmaker_key}'] = outcome.get('point')
                                    odds_entry[f'spread_away_price_{bookmaker_key}'] = outcome.get('price')

                        elif market_key == 'totals':
                            for outcome in market.get('outcomes', []):
                                if outcome.get('name') == 'Over':
                                    odds_entry[f'total_over_point_{bookmaker_key}'] = outcome.get('point')
                                    odds_entry[f'total_over_price_{bookmaker_key}'] = outcome.get('price')
                                elif outcome.get('name') == 'Under':
                                    odds_entry[f'total_under_point_{bookmaker_key}'] = outcome.get('point')
                                    odds_entry[f'total_under_price_{bookmaker_key}'] = outcome.get('price')
                
                all_odds_data.append(odds_entry)

            if not all_odds_data:
                self.logger.info(f"No MLB odds found for {target_date} after filtering by date.")
                return pd.DataFrame()

            odds_df = pd.DataFrame(all_odds_data)
            self.logger.info(f"Successfully retrieved and processed {len(odds_df)} odds records for {target_date}.")
            return odds_df

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching odds from The Odds API: {e}", exc_info=True)
            if 'response' in locals() and response is not None: # Check if response object exists
                self.logger.error(f"API Response Content: {response.text}")
        except json.JSONDecodeError as e:
            self.logger.error(f"Error decoding JSON response from The Odds API: {e}", exc_info=True)
        except Exception as e:
            self.logger.error(f"An unexpected error occurred in OddsScraper: {e}", exc_info=True)
        
        return pd.DataFrame()