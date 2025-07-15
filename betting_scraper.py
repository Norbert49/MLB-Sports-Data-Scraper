import requests
import time
import logging
from typing import Dict, Optional, List
from datetime import datetime, timedelta, date
import re
import json
import pandas as pd

# Set up logging to see debug information
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class MLBBettingScraper:
    """
    A dedicated scraper for MLB betting odds using a third-party API (The Odds API).
    This class handles fetching, parsing, and normalizing betting data.
    """

    def __init__(self, config: Dict):
        """
        Initializes the MLBBettingScraper with configuration details.

        Args:
            config (Dict): A dictionary containing 'betting_scraping' configuration,
                           including API key, base URL, delays, and team name mappings.
        """
        self.config = config.get('betting_scraping', {})
        self.api_key = self.config.get('api_key')
        self.api_base_url = self.config.get('betting_base_url', 'https://api.the-odds-api.com')
        self.delay_between_requests = self.config.get('delay_between_requests', 3)
        self.max_retries = self.config.get('max_retries', 3)
        # Consolidate team name mappings for robustness
        self.team_name_map = {
            # User-defined mappings
            **self.config.get('team_name_map', {}),
            # Common conversions (can be moved to config if preferred)
            'new york yankees': 'Yankees',
            'boston red sox': 'Red Sox',
            'los angeles dodgers': 'Dodgers',
            'san francisco giants': 'Giants',
            'chicago cubs': 'Cubs',
            'st. louis cardinals': 'Cardinals',
            'atlanta braves': 'Braves',
            'philadelphia phillies': 'Phillies',
            'houston astros': 'Astros',
            'tampa bay rays': 'Rays',
            'arizona diamondbacks': 'Diamondbacks',
            'baltimore orioles': 'Orioles',
            'cleveland guardians': 'Guardians',
            'colorado rockies': 'Rockies',
            'detroit tigers': 'Tigers',
            'kansas city royals': 'Royals',
            'los angeles angels': 'Angels',
            'miami marlins': 'Marlins',
            'milwaukee brewers': 'Brewers',
            'minnesota twins': 'Twins',
            'new york mets': 'Mets',
            'oakland athletics': 'Athletics',
            'pittsburgh pirates': 'Pirates',
            'san diego padres': 'Padres',
            'seattle mariners': 'Mariners',
            'texas rangers': 'Rangers',
            'toronto blue jays': 'Blue Jays',
            'washington nationals': 'Nationals',
            'cincinnati reds': 'Reds'
        }
        self.logger = logging.getLogger(__name__)

        if not self.api_key:
            self.logger.critical("API key for betting odds is not configured. Please provide 'api_key' in the config.")
            raise ValueError("API key is missing for betting odds scraper. Cannot proceed without it.")

    def _normalize_team_name(self, team_name: str) -> str:
        """
        Normalizes team names using the configured mapping to ensure consistency.
        The mapping is case-insensitive for lookup.
        """
        cleaned_name = team_name.strip().lower()
        
        # Exact match first
        if cleaned_name in self.team_name_map:
            return self.team_name_map[cleaned_name]
        
        # Partial match (e.g., 'yankees' for 'New York Yankees')
        for full_name_key, mapped_name_value in self.team_name_map.items():
            if cleaned_name in full_name_key or full_name_key in cleaned_name:
                return mapped_name_value
        
        self.logger.warning(f"No specific mapping found for team '{team_name}'. Using original name.")
        # Capitalize for better presentation if no mapping is found
        return team_name.strip() 

    def _teams_match(self, team1: str, team2: str) -> bool:
        """
        Checks if two team names refer to the same team, considering normalization.
        This provides a more robust comparison.
        """
        team1_normalized = self._normalize_team_name(team1).lower()
        team2_normalized = self._normalize_team_name(team2).lower()
        
        # Check for exact normalized match or if one is a substring of the other
        return team1_normalized == team2_normalized or \
               team1_normalized in team2_normalized or \
               team2_normalized in team1_normalized

    def _parse_american_odds(self, odds_value: Optional[float]) -> Optional[int]:
        """
        Converts decimal odds (provided by The Odds API) to American odds format.
        Returns None if input is invalid or conversion fails.
        """
        if odds_value is None or not isinstance(odds_value, (int, float)):
            return None
        
        try:
            if odds_value >= 2.00:
                # Positive American odds: (Decimal - 1) * 100
                return round((odds_value - 1) * 100)
            else:
                # Negative American odds: -100 / (Decimal - 1)
                return round(-100 / (odds_value - 1))
        except ZeroDivisionError:
            self.logger.error(f"Attempted to convert decimal odds {odds_value} resulting in division by zero.")
            return None
        except Exception as e:
            self.logger.warning(f"Could not convert decimal odds {odds_value} to American odds due to: {e}. Returning None.")
            return None

    def _fetch_odds_from_api(self, target_date: date) -> List[Dict]:
        """
        Fetches betting odds for MLB games around a specific date from The Odds API.
        This function is designed to collect odds for all relevant games.

        Args:
            target_date (date): The central date for which to fetch game odds.

        Returns:
            List[Dict]: A list of dictionaries, where each dictionary contains
                        normalized betting odds for a game. Returns an empty list
                        if no odds could be retrieved after retries.
        """
        all_game_odds = []

        sport_key = 'baseball_mlb'
        regions = 'us' 
        markets = 'h2h,spreads,totals' 
        # Add 'oddsFormat=decimal' explicitly as we convert it
        url = (f"{self.api_base_url}/v4/sports/{sport_key}/odds/"
               f"?apiKey={self.api_key}&regions={regions}&markets={markets}&oddsFormat=decimal")

        print(f"\n🔍 DEBUG: API URL being called: {url}")
        self.logger.info(f"Attempting to fetch odds from The Odds API for games around {target_date.strftime('%Y-%m-%d')}")

        for attempt in range(self.max_retries):
            print(f"\n🔄 DEBUG: Attempt {attempt + 1} of {self.max_retries}")
            try:
                response = requests.get(url, timeout=10)
                print(f"📡 DEBUG: Response status code: {response.status_code}")
                print(f"📊 DEBUG: Response headers: {dict(response.headers)}")
                
                response.raise_for_status()
                
                api_games = response.json()
                print(f"🎯 DEBUG: API response received: {len(api_games)} games total")
                
                # Print first game for debugging
                if api_games:
                    print(f"🔍 DEBUG: First game structure:")
                    print(json.dumps(api_games[0], indent=2))
                
                self.logger.debug(f"API response received: {len(api_games)} games.")

                games_processed_for_date_range = 0
                for i, game in enumerate(api_games):
                    print(f"\n🏟️  DEBUG: Processing game {i+1}/{len(api_games)}")
                    
                    api_away_team = game.get('away_team')
                    api_home_team = game.get('home_team')
                    commence_time_str = game.get('commence_time')

                    print(f"   Teams: {api_away_team} @ {api_home_team}")
                    print(f"   Commence time: {commence_time_str}")

                    if not all([api_away_team, api_home_team, commence_time_str]):
                        print(f"   ❌ Skipping game due to missing essential data")
                        self.logger.warning(f"Skipping game due to missing essential data: {game}")
                        continue

                    try:
                        game_commence_datetime = datetime.strptime(commence_time_str, '%Y-%m-%dT%H:%M:%SZ')
                        game_commence_date = game_commence_datetime.date()
                        print(f"   📅 Game date: {game_commence_date}")
                    except ValueError:
                        print(f"   ❌ Could not parse commence_time")
                        self.logger.error(f"Could not parse commence_time '{commence_time_str}'. Skipping game.")
                        continue

                    # Check if the game date is relevant (within a day of the target date)
                    date_difference = abs((game_commence_date - target_date).days)
                    print(f"   🗓️  Date difference: {date_difference} days from target {target_date}")
                    
                    if date_difference <= 1: 
                        games_processed_for_date_range += 1
                        print(f"   ✅ Game is within date range - processing...")
                        self.logger.info(f"Processing game: {api_away_team} @ {api_home_team} (API Commence Time: {game_commence_datetime})")
                        
                        odds_data = {
                            'game_date': game_commence_date.strftime('%Y-%m-%d'),
                            'home_team': self._normalize_team_name(api_home_team),
                            'away_team': self._normalize_team_name(api_away_team),
                            'moneyline_home': None,
                            'moneyline_away': None,
                            'spread_home': None,
                            'spread_home_odds': None,
                            'spread_away': None,
                            'spread_away_odds': None,
                            'total_over': None,
                            'total_over_odds': None,
                            'total_under': None,
                            'total_under_odds': None,
                            'source': 'The Odds API',
                            'scraped_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }

                        print(f"   📊 Processing {len(game.get('bookmakers', []))} bookmakers")
                        
                        # Process bookmakers and markets
                        for bookmaker_idx, bookmaker in enumerate(game.get('bookmakers', [])):
                            print(f"      📈 Bookmaker {bookmaker_idx + 1}: {bookmaker.get('title', 'Unknown')}")
                            
                            for market_idx, market in enumerate(bookmaker.get('markets', [])):
                                market_key = market['key']
                                print(f"         🎯 Market {market_idx + 1}: {market_key}")
                                
                                if market_key == 'h2h':
                                    for outcome in market.get('outcomes', []):
                                        outcome_name = outcome.get('name', '')
                                        price = outcome.get('price')
                                        print(f"            💰 {outcome_name}: {price} (decimal)")
                                        
                                        if self._teams_match(api_away_team, outcome_name):
                                            odds_data['moneyline_away'] = odds_data['moneyline_away'] or self._parse_american_odds(price)
                                            print(f"               → Away moneyline: {odds_data['moneyline_away']}")
                                        elif self._teams_match(api_home_team, outcome_name):
                                            odds_data['moneyline_home'] = odds_data['moneyline_home'] or self._parse_american_odds(price)
                                            print(f"               → Home moneyline: {odds_data['moneyline_home']}")
                                            
                                elif market_key == 'spreads':
                                    for outcome in market.get('outcomes', []):
                                        outcome_name = outcome.get('name', '')
                                        point = outcome.get('point')
                                        price = outcome.get('price')
                                        print(f"            📊 {outcome_name}: {point} @ {price}")
                                        
                                        if self._teams_match(api_away_team, outcome_name):
                                            if odds_data['spread_away'] is None:
                                                odds_data['spread_away'] = point
                                                odds_data['spread_away_odds'] = self._parse_american_odds(price)
                                                print(f"               → Away spread: {point} @ {odds_data['spread_away_odds']}")
                                        elif self._teams_match(api_home_team, outcome_name):
                                            if odds_data['spread_home'] is None:
                                                odds_data['spread_home'] = point
                                                odds_data['spread_home_odds'] = self._parse_american_odds(price)
                                                print(f"               → Home spread: {point} @ {odds_data['spread_home_odds']}")
                                                
                                elif market_key == 'totals':
                                    for outcome in market.get('outcomes', []):
                                        outcome_name = outcome.get('name', '').lower()
                                        point = outcome.get('point')
                                        price = outcome.get('price')
                                        print(f"            🎯 {outcome_name}: {point} @ {price}")
                                        
                                        if outcome_name == 'over':
                                            if odds_data['total_over'] is None:
                                                odds_data['total_over'] = point
                                                odds_data['total_over_odds'] = self._parse_american_odds(price)
                                                print(f"               → Over: {point} @ {odds_data['total_over_odds']}")
                                        elif outcome_name == 'under':
                                            if odds_data['total_under'] is None:
                                                odds_data['total_under'] = point
                                                odds_data['total_under_odds'] = self._parse_american_odds(price)
                                                print(f"               → Under: {point} @ {odds_data['total_under_odds']}")
                        
                        print(f"   ✅ Final odds data for this game:")
                        print(json.dumps(odds_data, indent=4))
                        all_game_odds.append(odds_data)
                    else:
                        print(f"   ⏭️  Game is outside date range - skipping")
                
                print(f"\n📊 SUMMARY: Processed {games_processed_for_date_range} games within 1 day of {target_date}")
                
                if games_processed_for_date_range > 0:
                    self.logger.info(f"Successfully processed {games_processed_for_date_range} games within 1 day of {target_date.strftime('%Y-%m-%d')}.")
                    return all_game_odds
                else:
                    self.logger.warning(f"No relevant games found via API for {target_date.strftime('%Y-%m-%d')} in this attempt. Retrying... (Attempt {attempt + 1})")
                    time.sleep(self.delay_between_requests)

            except requests.exceptions.HTTPError as e:
                print(f"❌ HTTP Error: {e.response.status_code}")
                print(f"   Response text: {e.response.text}")
                self.logger.error(f"HTTP error during API request (attempt {attempt + 1}): {e.response.status_code} - {e.response.text}")
                if e.response.status_code == 401:
                    self.logger.critical("API Key is invalid or expired. Please check your configuration.")
                    return []
                elif e.response.status_code == 429:
                    self.logger.warning("Rate limit hit. Waiting longer before retrying.")
                    time.sleep(self.delay_between_requests * 2)
                else:
                    time.sleep(self.delay_between_requests)
            except requests.exceptions.ConnectionError as e:
                print(f"❌ Connection Error: {e}")
                self.logger.error(f"Connection error during API request (attempt {attempt + 1}): {e}. Check internet connection.")
                time.sleep(self.delay_between_requests)
            except requests.exceptions.Timeout:
                print(f"❌ Timeout Error")
                self.logger.warning(f"API request timed out (attempt {attempt + 1}). Retrying...")
                time.sleep(self.delay_between_requests)
            except json.JSONDecodeError as e:
                print(f"❌ JSON Decode Error: {e}")
                self.logger.error(f"Failed to parse API response as JSON (attempt {attempt + 1}): {e}. Response: {response.text}")
                time.sleep(self.delay_between_requests)
            except Exception as e:
                print(f"❌ Unexpected Error: {e}")
                self.logger.error(f"An unexpected error occurred during API call (attempt {attempt + 1}): {e}", exc_info=True)
                time.sleep(self.delay_between_requests)
        
        print(f"❌ FAILED: Could not retrieve odds after {self.max_retries} attempts")
        self.logger.error(f"Failed to retrieve any betting odds from The Odds API after {self.max_retries} attempts.")
        return []

    def get_mlb_betting_odds(self, target_date_str: str) -> List[Dict]:
        """
        Main method to get betting odds for all games on or around a given date.

        Args:
            target_date_str (str): The date string in 'YYYY-MM-DD' format
                                   for which to retrieve betting odds.

        Returns:
            List[Dict]: A list of dictionaries, each representing betting odds
                        for a single MLB game. Returns an empty list if no
                        data can be retrieved.
        """
        try:
            target_date = datetime.strptime(target_date_str, '%Y-%m-%d').date()
        except ValueError:
            self.logger.error(f"Invalid target_date_str format: '{target_date_str}'. Expected YYYY-MM-DD.")
            return []
            
        all_odds = self._fetch_odds_from_api(target_date)
        return all_odds


# TEST SCRIPT
def load_config(config_path='config.json'):
    """
    Load configuration from JSON file.
    """
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        print(f"✅ Successfully loaded config from {config_path}")
        return config
    except FileNotFoundError:
        print(f"❌ ERROR: Config file '{config_path}' not found!")
        print("   Please make sure your config.json file is in the same directory.")
        return None
    except json.JSONDecodeError as e:
        print(f"❌ ERROR: Invalid JSON in config file: {e}")
        return None
    except Exception as e:
        print(f"❌ ERROR: Could not load config: {e}")
        return None

def test_mlb_scraper():
    """
    Test function to verify the MLB betting scraper is working correctly.
    """
    print("🚀 Starting MLB Betting Scraper Test\n")
    
    # Load configuration from JSON file
    config = load_config()
    if config is None:
        return
    
    # Check if betting_scraping section exists
    if 'betting_scraping' not in config:
        print("❌ ERROR: 'betting_scraping' section not found in config!")
        print("   Please add the betting_scraping configuration to your config.json")
        return
    
    # Check if API key is set
    api_key = config['betting_scraping'].get('api_key')
    if not api_key:
        print("❌ ERROR: API key not found in config!")
        print("   Please add 'api_key' to the 'betting_scraping' section in config.json")
        print("   Get your API key from: https://the-odds-api.com/")
        return
    
    print(f"🔑 API Key found: {api_key[:10]}...{api_key[-5:]} (masked for security)")
    print(f"🌐 Base URL: {config['betting_scraping'].get('betting_base_url', 'https://api.the-odds-api.com')}")
    
    try:
        # Initialize the scraper
        scraper = MLBBettingScraper(config)
        
        # Test with today's date
        today = datetime.now().date()
        target_date = today.strftime('%Y-%m-%d')
        
        print(f"🎯 Testing with date: {target_date}")
        print("=" * 50)
        
        # Get betting odds
        odds_data = scraper.get_mlb_betting_odds(target_date)
        
        print("\n" + "=" * 50)
        print("📊 TEST RESULTS:")
        print(f"   Total games found: {len(odds_data)}")
        
        if odds_data:
            print("   ✅ SUCCESS: Scraper is working!")
            print("\n📋 Sample results:")
            for i, game in enumerate(odds_data[:3]):  # Show first 3 games
                print(f"\n   Game {i+1}:")
                print(f"     📅 Date: {game['game_date']}")
                print(f"     🏟️  Teams: {game['away_team']} @ {game['home_team']}")
                print(f"     💰 Moneyline: Away {game['moneyline_away']}, Home {game['moneyline_home']}")
                print(f"     📊 Spread: Away {game['spread_away']} ({game['spread_away_odds']}), Home {game['spread_home']} ({game['spread_home_odds']})")
                print(f"     🎯 Total: Over {game['total_over']} ({game['total_over_odds']}), Under {game['total_under']} ({game['total_under_odds']})")
        else:
            print("   ❌ No games found - this could mean:")
            print("     - No MLB games scheduled for this date")
            print("     - API key issues")
            print("     - Network connectivity problems")
            print("     - MLB season timing (check if it's baseball season)")
            
    except Exception as e:
        print(f"❌ ERROR: {e}")
        print("   Check your API key and network connection")


if __name__ == "__main__":
    test_mlb_scraper()