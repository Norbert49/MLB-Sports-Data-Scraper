import pandas as pd
from bs4 import BeautifulSoup, Comment
import re
import logging

class PitchingParser:
    """
    Parses pitching statistics from a BeautifulSoup object of a box score page.
    """
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def extract_team_name(self, table_id: str) -> str:
        """
        Extracts the team abbreviation from a pitching table ID.
        Expected formats: 'box-ARI-pitching', 'box-LAA-pitching', etc.
        """
        if not table_id:
            return 'UNKNOWN'
        
        # Regex to capture the team abbreviation from 'box-TEAM_ABBR-pitching'
        match = re.match(r'box-([A-Z]{2,3})-pitching', table_id, re.IGNORECASE)
        if match:
            return match.group(1).upper()
        
        # Fallback for other patterns if necessary, or if the ID is just the team name
        # e.g., 'ArizonaDiamondbackspitching' -> 'ARI'
        # This part of your original logic was okay but might be less precise.
        # Let's keep it as a fallback if the primary regex doesn't match.
        team_part = table_id.lower().replace('pitching', '').strip()
        
        # Remove common suffixes/prefixes if they exist in the ID but aren't part of the abbr
        team_part = team_part.replace('box-', '')
        team_part = team_part.replace('diamondbacks', 'ari') # Standardize common full names
        team_part = team_part.replace('angels', 'laa') # Example for Angels
        
        # Try to extract meaningful abbreviation from remaining part
        if len(team_part) >= 2: # At least 2 characters for an abbreviation
            # Prioritize capital letters if present (e.g., 'NYY' from 'newyorkyankees')
            abbr = ''.join([c for c in team_part if c.isupper()])
            if len(abbr) >= 2:
                return abbr[:3].upper() # Take up to 3 capital letters
            
            # If no capitals or not enough, take first few characters
            return team_part[:3].upper() # Take first 3 characters
        
        return team_part.upper() if team_part else 'UNK'


    def parse_pitching_stats(self, soup: BeautifulSoup) -> pd.DataFrame: # Renamed 'parse' to 'parse_pitching_stats'
        pitching_dfs = []
        pitching_tables_to_process = []

        self.logger.debug("Starting PitchingParser.parse_pitching_stats method.")

        # --- Primary Method: Look for pitching tables directly ---
        # More flexible regex patterns for different table ID formats
        # Prioritize tables with 'box-' prefix as they are standard box scores
        direct_pitching_tables = soup.find_all('table', id=re.compile(r'box-.*-pitching', re.IGNORECASE))
        
        self.logger.debug(f"Direct table search found {len(direct_pitching_tables)} tables matching 'box-*-pitching'.")
        
        for table in direct_pitching_tables:
            table_id = table.get('id', '')
            self.logger.debug(f"Found direct pitching table with ID: '{table_id}'")
            pitching_tables_to_process.append(table)

        # --- Secondary Method: Look inside HTML comments (for older/hidden tables) ---
        # This is crucial for Baseball-Reference where main tables are often commented out
        comments = soup.find_all(string=lambda text: isinstance(text, Comment))
        self.logger.debug(f"Found {len(comments)} total comments.")

        for comment_content in comments:
            # Only parse comments that likely contain pitching tables to save resources
            if 'pitching' in str(comment_content).lower() and '<table' in str(comment_content).lower():
                comment_soup = BeautifulSoup(str(comment_content), 'html.parser')
                
                # Look for tables within the comment's parsed HTML
                # Use a more specific regex for table IDs if possible, or broad if needed
                comment_tables = comment_soup.find_all('table', id=re.compile(r'.*pitching.*', re.IGNORECASE))
                for table in comment_tables:
                    table_id = table.get('id', '')
                    # Avoid adding duplicates if already found directly
                    if table not in pitching_tables_to_process:
                        self.logger.debug(f"Found pitching table '{table_id}' in comment.")
                        pitching_tables_to_process.append(table)

        self.logger.info(f"Total {len(pitching_tables_to_process)} pitching tables identified for parsing.")

        if not pitching_tables_to_process:
            self.logger.warning("No pitching tables found. Returning empty DataFrame.")
            return pd.DataFrame()

        # Process each table
        for table in pitching_tables_to_process:
            table_id = table.get('id', 'unknown')
            team_name = self.extract_team_name(table_id)
            self.logger.debug(f"Processing pitching table for team: {team_name} (from ID: {table_id})")

            # Find header row
            thead = table.find('thead')
            if not thead:
                # Fallback: try to find header row in tbody or directly in table if no thead
                tbody = table.find('tbody')
                if tbody:
                    header_row = tbody.find('tr') # Assume first row of tbody might be header if no thead
                else:
                    header_row = table.find('tr') # Fallback to first row of table
            else:
                header_row = thead.find('tr')

            if not header_row:
                self.logger.warning(f"No header row found for table {table_id}. Skipping.")
                continue

            # Extract column names
            columns = []
            for th in header_row.find_all(['th', 'td']): # Look in both th and td for headers
                stat = th.get('data-stat')
                if stat and stat not in ['rank', 'details']: # Exclude common non-stat columns
                    columns.append(stat)
                elif not stat:
                    # Fallback to text content if no data-stat, clean it
                    text = th.get_text(strip=True)
                    if text and text.lower() not in ['rk', '#', 'player']: # Exclude rank/player if no data-stat
                        columns.append(text.lower().replace(' ', '_').replace('.', '')) # Clean for column names

            if not columns:
                self.logger.warning(f"No valid columns found for table {table_id}. Skipping.")
                continue

            self.logger.debug(f"Columns extracted for {team_name}: {columns}")

            # Find data rows
            tbody = table.find('tbody')
            if not tbody:
                # If no tbody, assume all rows after header are data rows
                data_rows = table.find_all('tr')[1:] # Skip the first row (header)
            else:
                data_rows = tbody.find_all('tr')

            data = []
            for row in data_rows:
                row_classes = row.get('class', [])
                # Skip total rows and header rows that might appear in tbody
                if 'total_row' in row_classes or 'thead' in row_classes or row.find('th', scope='colgroup'):
                    continue

                # Find pitcher name (usually in a th tag with data-stat='player')
                pitcher_th = row.find('th', {'data-stat': 'player'})
                if not pitcher_th:
                    # Fallback: look for the first th or td if data-stat='player' not found
                    pitcher_th = row.find('th') or row.find('td')

                if not pitcher_th:
                    self.logger.debug(f"Skipping row without a primary pitcher tag in {team_name}.")
                    continue

                pitcher_name = pitcher_th.get_text(strip=True)
                if not pitcher_name or pitcher_name.lower() in ['team totals', 'total', 'totals']:
                    self.logger.debug(f"Skipping team totals or empty pitcher name row: '{pitcher_name}'")
                    continue

                # Extract player ID from link
                pitcher_link = pitcher_th.find('a', href=True)
                pitcher_id = None
                if pitcher_link:
                    href = pitcher_link['href']
                    if '/players/' in href:
                        pitcher_id = href.split('/')[-1].replace('.shtml', '')

                self.logger.debug(f"Processing pitcher: {pitcher_name} (ID: {pitcher_id})")

                row_data = {
                    'pitcher': pitcher_name,
                    'pitcher_id': pitcher_id
                }

                # Extract statistics from remaining cells
                # Ensure we skip the 'player' column if it's in the list of columns to process
                cells = row.find_all(['td', 'th']) # Get all cells in the row
                
                # Map data-stat to cell for robust extraction
                cell_map = {cell.get('data-stat'): cell for cell in cells if cell.get('data-stat')}
                
                for col_stat in columns:
                    if col_stat == 'player': # Player name already handled
                        continue
                    
                    cell = cell_map.get(col_stat) # Try to get by data-stat first
                    
                    # Fallback to sequential if data-stat not found or if it's the first column (pitcher)
                    # This part of your original logic was complex and prone to misalignment.
                    # Relying on data-stat is more robust.
                    
                    row_data[col_stat] = cell.get_text(strip=True) if cell else None

                data.append(row_data)

            if data:
                # Ensure 'pitcher' and 'pitcher_id' are always at the beginning, followed by other stats
                df = pd.DataFrame(data)
                # Filter columns to only include those actually found in the data and desired
                final_ordered_cols = ['pitcher', 'pitcher_id'] + [col for col in columns if col not in ['player']]
                
                # Reindex to ensure order and fill missing columns with None
                df = df.reindex(columns=final_ordered_cols)
                
                df['team'] = team_name
                # Reorder to put 'team' first
                df = df[['team'] + [col for col in df.columns if col != 'team']]
                
                pitching_dfs.append(df)
                self.logger.info(f"Successfully extracted {len(df)} pitching records for team {team_name}.")
            else:
                self.logger.warning(f"No data rows extracted for team {team_name} from table ID: {table_id}.")

        if pitching_dfs:
            combined_df = pd.concat(pitching_dfs, ignore_index=True)
            self.logger.info(f"Total pitching records combined: {len(combined_df)}")
            return combined_df
        else:
            self.logger.warning("No pitching dataframes were generated. Returning empty DataFrame.")
            return pd.DataFrame()