import pandas as pd
from bs4 import BeautifulSoup, Comment
from datetime import datetime
import re
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class BattingParser:
    """
    Parses batting statistics from a BeautifulSoup object of a box score page.
    """
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def parse_batting_stats(self, soup: BeautifulSoup) -> pd.DataFrame:
        """
        Extracts batting statistics for all players from the given BeautifulSoup object.
        """
        batting_dfs = []

        # Look for all divs that may contain commented-out batting tables
        # The IDs are typically like 'all_box-ARI-batting' or 'all_ArizonaDiamondbacksbatting'
        batting_divs = soup.find_all('div', id=re.compile(r'all_.*batting'))
        batting_tables = []

        for div in batting_divs:
            # Extract <table> embedded inside comments
            # The actual table ID is often inside the comment, like 'box-ARI-batting' or 'ArizonaDiamondbacksbatting'
            for comment in div.find_all(string=lambda text: isinstance(text, Comment)):
                comment_soup = BeautifulSoup(comment, 'html.parser')
                table = comment_soup.find('table', id=re.compile(r'.*batting')) # More general regex for table ID
                if table:
                    batting_tables.append(table)

        self.logger.info(f"Found {len(batting_tables)} batting tables.")

        for table in batting_tables:
            table_id = table.get('id')

            team_name = 'UNKNOWN'
            team_abbr = 'UNKNOWN'

            if table_id:
                # Attempt to extract team name from table_id
                # Handle formats like 'box-ARI-batting' or 'ArizonaDiamondbacksbatting'
                
                # First, try to remove the 'batting' suffix (case-insensitive)
                cleaned_id = re.sub(r'batting$', '', table_id, flags=re.IGNORECASE)
                
                # Then, remove 'box-' prefix if present
                if cleaned_id.lower().startswith('box-'):
                    cleaned_id = cleaned_id[len('box-'):]
                
                # Now, standardize the cleaned ID to a common team abbreviation
                # This mapping can be expanded or moved to a central config/utility
                team_id_to_abbr_map = {
                    'arizonadiamondbacks': 'ARI',
                    'losangelesangels': 'LAA',
                    'atlanta': 'ATL', 'baltimore': 'BAL', 'boston': 'BOS',
                    'chicagocubs': 'CHC', 'chicagowhitesox': 'CHW',
                    'cincinnati': 'CIN', 'cleveland': 'CLE', 'colorado': 'COL',
                    'detroit': 'DET', 'houston': 'HOU', 'kansascity': 'KCR',
                    'losangelesdodgers': 'LAD', 'miami': 'MIA', 'milwaukee': 'MIL',
                    'minnesota': 'MIN', 'newyorkmets': 'NYM', 'newyorkyankees': 'NYY',
                    'oakland': 'OAK', 'philadelphia': 'PHI', 'pittsburgh': 'PIT',
                    'sandiego': 'SDP', 'seattle': 'SEA', 'sanfrancisco': 'SFG',
                    'stlouis': 'STL', 'tampabay': 'TBR', 'texas': 'TEX',
                    'toronto': 'TOR', 'washington': 'WSN'
                }
                
                # Convert to lowercase for mapping, then get the mapped value or original
                team_name = team_id_to_abbr_map.get(cleaned_id.lower(), cleaned_id)
                team_abbr = team_name.upper() # Use the mapped/cleaned name as abbreviation
            
            self.logger.info(f"Processing batting table for team: {team_name} (from ID: {table_id})")

            data = []
            columns = []

            thead = table.find('thead')
            if not thead:
                self.logger.warning(f"Could not find thead for batting table of {team_name}.")
                continue

            header_row = thead.find('tr')
            if header_row:
                header_ths = header_row.find_all('th')
                for th in header_ths:
                    stat_name = th.get('data-stat')
                    if stat_name and stat_name not in ['rank', 'details']:
                        columns.append(stat_name)
                self.logger.debug(f"Extracted batting headers (data-stat): {columns}")
            else:
                self.logger.warning(f"Could not find header row for batting table of {team_name}.")
                continue

            tbody = table.find('tbody')
            if not tbody:
                self.logger.warning(f"Could not find tbody for batting table of {team_name}.")
                continue

            total_rows = len(tbody.find_all('tr'))
            self.logger.info(f"Found {total_rows} total rows in tbody for {team_name}")

            for row in tbody.find_all('tr'):
                row_classes = row.get('class', [])
                if any(cls in ['total_row', 'spacer'] for cls in row_classes):
                    self.logger.debug(f"Skipping row with class: {row_classes}")
                    continue

                row_data = {}

                player_th = row.find('th', {'data-stat': 'player'})
                if not player_th:
                    self.logger.debug(f"Row without player th tag found in {team_name}, skipping.")
                    continue

                player_name = player_th.text.strip()
                if player_name in ['Team Totals', 'Team Total']:
                    self.logger.debug(f"Skipping team totals row")
                    continue
                if not player_name:
                    self.logger.debug(f"Skipping row with empty player name")
                    continue

                player_link_tag = player_th.find('a', href=True)
                player_id = None
                if player_link_tag:
                    href = player_link_tag['href']
                    player_id = href.split('/')[-1].replace('.shtml', '') if href else None

                row_data['player'] = player_name
                row_data['player_id'] = player_id

                for col_stat in columns:
                    if col_stat == 'player':
                        continue
                    td_tag = row.find('td', {'data-stat': col_stat})
                    if td_tag:
                        cell_value = td_tag.text.strip()
                        if cell_value == '' or cell_value == '--':
                            cell_value = None
                        row_data[col_stat] = cell_value
                    else:
                        row_data[col_stat] = None

                data.append(row_data)
                self.logger.debug(f"Added batting data for player: {player_name} (ID: {player_id}) - Team: {team_name}")

            if data and columns:
                if 'player' in columns:
                    columns.remove('player')
                final_columns = ['player', 'player_id'] + columns
                df = pd.DataFrame(data)
                existing_cols = [col for col in final_columns if col in df.columns]
                df = df[existing_cols]
                df['team'] = team_name
                ordered_cols = ['team'] + [col for col in df.columns if col != 'team']
                df = df[ordered_cols]
                self.logger.info(f"Successfully parsed {len(df)} batting records for team {team_name}")
                batting_dfs.append(df)
            else:
                self.logger.warning(f"No valid data or headers found for batting table from {team_name}.")

        if batting_dfs:
            combined_df = pd.concat(batting_dfs, ignore_index=True)
            self.logger.info(f"Combined batting data: {len(combined_df)} total records from {len(batting_dfs)} teams")
            return combined_df
        else:
            self.logger.warning("No batting data found in any tables.")
            return pd.DataFrame()

    def _convert_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert numeric columns to appropriate data types.
        This method can be called separately if needed for data cleaning.
        """
        if df.empty:
            return df

        numeric_columns = [
            'AB', 'R', 'H', 'RBI', 'BB', 'SO', 'PA', 'HR', '2B', '3B', 
            'SB', 'CS', 'HBP', 'SF', 'GDP', 'IBB'
        ]

        float_columns = [
            'AVG', 'OBP', 'SLG', 'OPS', 'BAbip', 'tOPS+', 'sOPS+', 'WPA', 
            'aLI', 'WPA+', 'WPA-', 'cWPA', 'acLI', 'cWPA-', 'RE24', 'PO'
        ]

        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        for col in float_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        return df