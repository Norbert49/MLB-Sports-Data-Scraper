import pandas as pd
from bs4 import BeautifulSoup, Comment
from datetime import datetime
import re
import logging
from typing import List, Dict, Optional, Tuple

class LineupParser:
    """
    Refined parser for starting lineup data from baseball box score pages.
    Specifically designed to handle the HTML structure shown in your inspection.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Pre-compile regex patterns for better performance
        self.player_link_pattern = re.compile(r'/players/[a-z]/[^/]+\.shtml')
        self.batting_order_pattern = re.compile(r'^\d+$')
        
    def parse_lineups(self, soup: BeautifulSoup, game_date_str: Optional[str] = None) -> pd.DataFrame: # Renamed 'parse' to 'parse_lineups' and added optional game_date_str
        """
        Extracts starting lineup data for both teams with improved error handling.
        
        Args:
            soup: BeautifulSoup object of the box score page
            game_date_str: Optional. Date string for the game. Will try to extract from soup if None.
            
        Returns:
            DataFrame with lineup data or empty DataFrame if parsing fails
        """
        try:
            # If game_date_str is not provided, try to extract it from the soup
            if game_date_str is None:
                # This logic might be in GameInfoParser, but for standalone LineupParser,
                # we can add a basic attempt here.
                meta_div = soup.find('div', id='content').find('div', class_='scorebox_meta')
                if meta_div:
                    game_date_tag = meta_div.find('p')
                    if game_date_tag:
                        game_date_match = re.search(r'([A-Za-z]+, \w+ \d{1,2}, \d{4})', game_date_tag.text)
                        if game_date_match:
                            try:
                                game_date_str = datetime.strptime(game_date_match.group(1), '%A, %B %d, %Y').strftime('%Y-%m-%d')
                            except ValueError:
                                self.logger.warning(f"Could not parse game date from scorebox meta for lineup: {game_date_match.group(1)}")
                if game_date_str is None:
                    self.logger.warning("Game date string not provided and could not be extracted from soup. Lineup data will have 'N/A' date.")
                    game_date_str = "N/A" # Default if date cannot be found

            self.debug_html_structure(soup) # Keep debug for now

            starting_lineups_section = self._find_lineups_section(soup)
            if not starting_lineups_section:
                self.logger.warning("Could not find starting lineups section")
                return pd.DataFrame()
                
            lineup_tables = self._get_lineup_tables(starting_lineups_section)
            if not lineup_tables:
                self.logger.warning("Could not find lineup tables")
                return pd.DataFrame()
                
            lineup_data = self._extract_lineup_data(lineup_tables, game_date_str)
            
            if lineup_data:
                df = self._create_dataframe(lineup_data)
                self.logger.info(f"Successfully parsed {len(lineup_data)} lineup entries")
                return df
            else:
                self.logger.warning("No lineup data extracted.")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Error parsing lineup data: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return pd.DataFrame()
    
    def _find_lineups_section(self, soup: BeautifulSoup) -> Optional[BeautifulSoup]:
        """
        Find the starting lineups section in HTML or comments.
        Based on your HTML structure, it should be in a div with id='div_lineups'.
        """
        # Try the exact ID from your HTML inspection
        starting_lineups_section = soup.find('div', id='div_lineups')
        if starting_lineups_section:
            self.logger.info("Found 'div_lineups' section in visible HTML.")
            return starting_lineups_section
        
        # Try alternative IDs
        alternative_ids = ['div_starting_lineups', 'starting_lineups', 'lineups']
        for lineup_id in alternative_ids:
            section = soup.find('div', id=lineup_id)
            if section:
                self.logger.info(f"Found lineup section with ID: {lineup_id}")
                return section
        
        # Search in comments
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment_str = str(comment)
            if 'div_lineups' in comment_str or 'Starting Lineups' in comment_str:
                try:
                    comment_soup = BeautifulSoup(comment_str, 'html.parser')
                    section = comment_soup.find('div', id='div_lineups')
                    if not section:
                        section = comment_soup.find('div', id='div_starting_lineups')
                    if section:
                        self.logger.info("Found lineup section in HTML comment.")
                        return section
                except Exception as e:
                    self.logger.warning(f"Error parsing comment: {str(e)}")
                    continue
        
        # Try to find by content - look for sections containing player links
        all_divs = soup.find_all('div')
        for div in all_divs:
            player_links = div.find_all('a', href=self.player_link_pattern)
            if len(player_links) >= 18:   # Should have ~18 players (9 per team)
                # Additional validation - check for lineup-related text
                div_text = div.get_text().lower()
                if any(keyword in div_text for keyword in ['lineup', 'starting', 'batting order']):
                    self.logger.info("Found lineup section by content analysis.")
                    return div
                    
        self.logger.warning("Could not find 'Starting Lineups' section anywhere.")
        return None
    
    def _get_lineup_tables(self, section: BeautifulSoup) -> List[BeautifulSoup]:
        """
        Extract lineup tables from the lineups section.
        Based on your HTML, there should be tables with class 'data_grid_box'.
        """
        tables = []
        
        # Look for tables with specific classes from your HTML
        table_classes = ['data_grid_box', 'lineup', 'lineups']
        for table_class in table_classes:
            class_tables = section.find_all('table', class_=table_class)
            tables.extend(class_tables)
            if class_tables:
                self.logger.info(f"Found {len(class_tables)} tables with class '{table_class}'")
        
        # Look for tables with specific IDs
        table_ids = ['lineups_1', 'lineups_2', 'lineup_1', 'lineup_2']
        for table_id in table_ids:
            table = section.find('table', id=table_id)
            if table and table not in tables:
                tables.append(table)
                self.logger.info(f"Found table with ID: {table_id}")
        
        # Fall back to any tables in the section
        if not tables:
            all_tables = section.find_all('table')
            tables.extend(all_tables)
            if all_tables:
                self.logger.info(f"Found {len(all_tables)} tables in lineup section (fallback)")
        
        # Additional check: look for div structures that might contain lineup data
        if not tables:
            data_grids = section.find_all('div', class_='data_grid')
            for grid in data_grids:
                grid_tables = grid.find_all('table')
                tables.extend(grid_tables)
                if grid_tables:
                    self.logger.info(f"Found {len(grid_tables)} tables in data_grid")
        
        if not tables:
            self.logger.warning("Could not find any lineup tables within section.")
            # Debug output
            self.logger.debug(f"Section HTML (first 1000 chars): {str(section)[:1000]}")
            
        return tables
    
    def _extract_lineup_data(self, tables: List[BeautifulSoup], game_date_str: str) -> List[Dict]:
        """
        Extract lineup data from all tables.
        """
        lineup_data_list = []
        
        for i, table in enumerate(tables):
            self.logger.info(f"Processing table {i+1}/{len(tables)}")
            
            # Extract team name
            team_name = self._extract_team_name(table, i)
            self.logger.info(f"Team name for table {i+1}: {team_name}")
            
            # Extract player data from table rows
            rows = self._get_table_rows(table)
            self.logger.info(f"Found {len(rows)} rows in table {i+1}")
            
            for row_idx, row in enumerate(rows):
                player_data = self._extract_player_data(row, team_name, game_date_str)
                if player_data:
                    lineup_data_list.append(player_data)
                    self.logger.info(f"Extracted: {player_data.get('batting_order', 'N/A')}. {player_data.get('player', 'N/A')} ({player_data.get('position', 'N/A')})")
                else:
                    self.logger.debug(f"No data from row {row_idx}: {row.get_text(strip=True)}")
                    
        return lineup_data_list
    
    def _get_table_rows(self, table: BeautifulSoup) -> List[BeautifulSoup]:
        """
        Get all data rows from a table, excluding headers.
        """
        rows = []
        
        # Try tbody first
        tbody = table.find('tbody')
        if tbody:
            rows = tbody.find_all('tr')
        else:
            # Fall back to all tr elements
            all_rows = table.find_all('tr')
            # Filter out header rows
            for row in all_rows:
                # Skip if row contains th elements (header row)
                if row.find('th'):
                    continue
                # Skip if row is empty or contains only whitespace
                if not row.get_text(strip=True):
                    continue
                rows.append(row)
        
        return rows
    
    def _extract_team_name(self, table: BeautifulSoup, table_index: int) -> str:
        """
        Extract team name from table caption or nearby elements.
        """
        # Try caption first
        caption = table.find('caption')
        if caption:
            team_name = caption.get_text(strip=True)
            if team_name and team_name.lower() != 'table':
                return team_name
        
        # Try nearby headers (h2, h3, etc.)
        for header_tag in ['h2', 'h3', 'h4', 'h5']:
            header = table.find_previous_sibling(header_tag)
            if header:
                team_name = header.get_text(strip=True)
                if team_name:
                    return team_name
        
        # Try parent div with team info
        parent = table.find_parent('div')
        if parent:
            for header_tag in ['h2', 'h3', 'h4', 'h5']:
                team_header = parent.find(header_tag)
                if team_header:
                    team_name = team_header.get_text(strip=True)
                    if team_name:
                        return team_name
        
        # Try to find team name in the table ID or class
        table_id = table.get('id', '')
        table_class = ' '.join(table.get('class', []))
        
        # Look for team abbreviations in ID/class
        team_abbrevs = ['ANA', 'LAA', 'NYY', 'BOS', 'TOR', 'BAL', 'TB', 'CLE', 'DET', 'KC', 'MIN', 'CWS', 'HOU', 'OAK', 'SEA', 'TEX',
                        'ATL', 'MIA', 'NYM', 'PHI', 'WAS', 'CHC', 'CIN', 'MIL', 'PIT', 'STL', 'ARI', 'COL', 'LAD', 'SD', 'SF']
        
        for abbrev in team_abbrevs:
            if abbrev.lower() in table_id.lower() or abbrev.lower() in table_class.lower():
                return abbrev
        
        # Fallback based on table position (assuming first is away, second is home)
        if table_index == 0:
            return "Away Team"
        elif table_index == 1:
            return "Home Team"
        else:
            return f"Team {table_index + 1}"
    
    def _extract_player_data(self, row: BeautifulSoup, team_name: str, game_date_str: str) -> Optional[Dict]:
        """
        Extract individual player data from a table row.
        Based on your HTML structure: <td>1.</td><td><a href="/players/k/martel01.shtml">Ketel Marte</a></td>
        """
        tds = row.find_all('td')
        if len(tds) < 2:
            return None
        
        # Extract batting order from first td
        batting_order = self._extract_batting_order_from_td(tds[0])
        
        # Extract player info from second td
        player_name, player_id = self._extract_player_info_from_td(tds[1])
        
        if not player_name or player_name.lower() == 'player': # Ensure it's not a header row
            return None
        
        # Extract position - might be in a separate td or within the player td
        position = self._extract_position_from_row(tds)
        
        self.logger.debug(f"Extracted: Order={batting_order}, Player={player_name}, Position={position}")
        
        return {
            'game_date': game_date_str,
            'team': team_name,
            'batting_order': batting_order,
            'player': player_name,
            'position': position,
            'player_id': player_id
        }
    
    def _extract_batting_order_from_td(self, td: BeautifulSoup) -> Optional[int]:
        """
        Extract batting order from the first td element.
        """
        text = td.get_text(strip=True)
        
        # Remove common suffixes like '.'
        text = text.rstrip('.')
        
        if text.isdigit():
            try:
                order = int(text)
                if 1 <= order <= 9: # Batting order typically 1-9
                    return order
            except ValueError:
                pass
        
        return None
    
    def _extract_player_info_from_td(self, td: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract player name and ID from player td element.
        """
        # Look for player link
        player_link = td.find('a', href=self.player_link_pattern)
        if player_link:
            player_name = player_link.get_text(strip=True)
            href = player_link.get('href', '')
            # Extract player ID from href like '/players/k/martel01.shtml'
            player_id = href.split('/')[-1].replace('.shtml', '') if href else None
            return player_name, player_id
        
        # Fallback: try any link
        any_link = td.find('a')
        if any_link:
            return any_link.get_text(strip=True), None
        
        # Last resort: just text content
        text = td.get_text(strip=True)
        if text:
            return text, None
        
        return None, None
    
    def _extract_position_from_row(self, tds: List[BeautifulSoup]) -> str:
        """
        Extract position from table row. Position might be in a separate td or within player td.
        """
        # Check if there's a third td with position (common in some formats)
        if len(tds) >= 3:
            pos_text = tds[2].get_text(strip=True)
            # Basic validation: position is usually 1-3 uppercase letters
            if pos_text and 1 <= len(pos_text) <= 3 and pos_text.isalpha() and pos_text.isupper():
                return pos_text
        
        # Look for span with position class (e.g., <span class="pos">SS</span>)
        for td in tds:
            pos_span = td.find('span', class_='pos')
            if pos_span:
                return pos_span.get_text(strip=True)
            
            # Look for position in parentheses within the text of any td
            text = td.get_text()
            pos_match = re.search(r'\(([A-Z]{1,3})\)', text)
            if pos_match:
                return pos_match.group(1)
        
        return 'N/A' # Default if position cannot be found
    
    def _create_dataframe(self, lineup_data: List[Dict]) -> pd.DataFrame:
        """
        Create and return properly formatted DataFrame.
        """
        if not lineup_data:
            return pd.DataFrame()
        
        df = pd.DataFrame(lineup_data)
        
        # Define column order
        ordered_cols = ['game_date', 'team', 'batting_order', 'player', 'position', 'player_id']
        
        # Ensure all columns exist and are in order
        for col in ordered_cols:
            if col not in df.columns:
                df[col] = None
        
        df = df[ordered_cols] # Reorder columns
        
        # Sort by team and batting order for consistent output
        df['batting_order'] = pd.to_numeric(df['batting_order'], errors='coerce') # Ensure numeric for sorting
        df = df.sort_values(['team', 'batting_order'], na_position='last')
        
        # Reset index
        df = df.reset_index(drop=True)
        
        return df
    
    def debug_html_structure(self, soup: BeautifulSoup) -> None:
        """
        Debug method to analyze HTML structure for troubleshooting.
        """
        print("=== LINEUP PARSER DEBUG ===")
        
        # 1. Check for main lineup section
        print("\n1. Looking for main lineup section:")
        lineup_section = soup.find('div', id='div_lineups')
        if lineup_section:
            print(f"   ✓ Found div_lineups section")
        else:
            print(f"   ✗ div_lineups section not found")
            
        # 2. Check for alternative sections
        print("\n2. Alternative lineup sections:")
        alternatives = ['div_starting_lineups', 'starting_lineups', 'lineups']
        for alt_id in alternatives:
            section = soup.find('div', id=alt_id)
            if section:
                print(f"   ✓ Found {alt_id}")
            else:
                print(f"   ✗ {alt_id} not found")
        
        # 3. Check comments
        print("\n3. Checking comments:")
        lineup_comments = 0
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            if 'lineup' in str(comment).lower():
                lineup_comments += 1
        print(f"   Found {lineup_comments} lineup-related comments")
        
        # 4. Check for tables
        print("\n4. Table analysis:")
        # Try to find the section first, then its tables
        target_section = self._find_lineups_section(soup)
        if target_section:
            tables = target_section.find_all('table')
            print(f"   Found {len(tables)} tables in identified lineup section")
            
            for i, table in enumerate(tables):
                caption = table.find('caption')
                table_id = table.get('id', 'No ID')
                table_class = table.get('class', [])
                caption_text = caption.get_text() if caption else 'No caption'
                print(f"     Table {i+1}: ID='{table_id}', Class={table_class}, Caption='{caption_text}'")
                
                # Check rows
                rows = table.find_all('tr')
                print(f"       Rows: {len(rows)}")
                
                if rows:
                    # Sample first data row (skip header if present)
                    sample_rows = [r for r in rows if not r.find('th')][:3] # Get up to 3 non-header rows
                    for row in sample_rows:
                        tds = row.find_all('td')
                        if tds:
                            row_text = ' | '.join([td.get_text(strip=True) for td in tds[:3]])
                            print(f"       Sample row: {row_text}")
        else:
            print("   No specific lineup section found to analyze tables within.")
            
        # 5. Check for player links across entire page
        print("\n5. Player links analysis:")
        player_links = soup.find_all('a', href=self.player_link_pattern)
        print(f"   Found {len(player_links)} player links total")
        
        if player_links:
            print("   Sample player links:")
            for i, link in enumerate(player_links[:5]):
                print(f"     {i+1}. {link.get_text()} -> {link.get('href')}")
        
        print("=== END DEBUG ===\n")
    
    def validate_lineup_data(self, df: pd.DataFrame) -> bool:
        """
        Validate the extracted lineup data for completeness and correctness.
        """
        if df.empty:
            self.logger.warning("Lineup DataFrame is empty")
            return False
        
        # Check for required columns
        required_cols = ['game_date', 'team', 'player', 'position']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            self.logger.warning(f"Missing required columns: {missing_cols}")
            return False
        
        # Check for reasonable number of players per team
        teams = df['team'].unique()
        self.logger.info(f"Found {len(teams)} teams: {list(teams)}")
        
        for team in teams:
            team_players = df[df['team'] == team]
            player_count = len(team_players)
            self.logger.info(f"Team {team}: {player_count} players")
            
            if player_count < 8 or player_count > 12: # Expecting around 9 players per team
                self.logger.warning(f"Unusual number of players for team {team}: {player_count}")
        
        # Check batting order completeness
        for team in teams:
            team_data = df[df['team'] == team]
            batting_orders = team_data['batting_order'].dropna().sort_values().tolist()
            if batting_orders:
                self.logger.info(f"Team {team} batting orders: {batting_orders}")
                # Should generally be 1-9 but may have gaps if pitchers are not explicitly ordered
                if len(batting_orders) > 0 and (min(batting_orders) < 1 or max(batting_orders) > 9):
                    self.logger.warning(f"Invalid batting order range for team {team}: {batting_orders}")
        
        return True