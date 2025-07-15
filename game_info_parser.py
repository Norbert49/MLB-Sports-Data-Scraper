import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import re
import logging
from typing import Dict, List

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class GameInfoParser:
    """
    Parses general game-level information and win/loss/save pitchers from a BeautifulSoup object.
    """
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def parse_game_level_info(self, soup: BeautifulSoup) -> Dict:
        """
        Parses general game information like venue, attendance, duration, umpires, weather.
        """
        game_details = {}
        
        meta_div = soup.find('div', id='content').find('div', class_='scorebox_meta')
        if meta_div:
            game_date_tag = meta_div.find('p')
            if game_date_tag:
                game_date_match = re.search(r'([A-Za-z]+, \w+ \d{1,2}, \d{4})', game_date_tag.text)
                if game_date_match:
                    try:
                        game_details['game_date'] = datetime.strptime(game_date_match.group(1), '%A, %B %d, %Y').strftime('%Y-%m-%d')
                    except ValueError:
                        self.logger.warning(f"Could not parse game date from scorebox meta: {game_date_match.group(1)}")
            
            for p_tag in meta_div.find_all('p'):
                text = p_tag.get_text(separator=' ', strip=True)
                if 'Start Time:' in text:
                    game_details['start_time'] = text.replace('Start Time:', '').strip()
                elif 'Time of Game:' in text:
                    game_details['game_duration'] = text.replace('Time of Game:', '').strip()
                elif 'Attendance:' in text:
                    game_details['attendance'] = text.replace('Attendance:', '').replace(',', '').strip()
                elif 'Venue:' in text:
                    game_details['venue'] = text.replace('Venue:', '').strip()
                elif 'Field Condition:' in text:
                    game_details['field_condition'] = text.replace('Field Condition:', '').strip()
                elif 'Weather:' in text:
                    game_details['weather_conditions'] = text.replace('Weather:', '').strip()
                elif 'Umpires:' in text:
                    umpires_str = text.replace('Umpires:', '').strip()
                    game_details['umpires'] = [u.strip() for u in umpires_str.split(',')]

        self.logger.info(f"Extracted game-level details: {game_details}")
        return game_details

    def parse_win_loss_save_pitchers(self, soup: BeautifulSoup) -> Dict:
        """
        Parses winning pitcher, losing pitcher, and save pitcher.
        """
        pitcher_roles = {
            'WP': None,
            'LP': None,
            'SV': None
        }
        
        linescore_table = soup.find('table', id='linescore')
        if linescore_table:
            pitcher_info_p = linescore_table.find_next_sibling('p')
            
            if pitcher_info_p and ("WP:" in pitcher_info_p.text or "LP:" in pitcher_info_p.text or "SV:" in pitcher_info_p.text):
                info_text = pitcher_info_p.text.strip()
                
                wp_match = re.search(r'WP:\s*([^(\*]+)\s*\(.*?\)', info_text)
                if wp_match:
                    pitcher_roles['WP'] = wp_match.group(1).strip()
                
                lp_match = re.search(r'LP:\s*([^(\*]+)\s*\(.*?\)', info_text)
                if lp_match:
                    pitcher_roles['LP'] = lp_match.group(1).strip()

                sv_match = re.search(r'SV:\s*([^(\*]+)\s*\(.*?\)\s*\*?', info_text) 
                if sv_match:
                    pitcher_roles['SV'] = sv_match.group(1).strip()
            else:
                self.logger.warning("Could not find pitcher roles information in the expected paragraph after linescore.")
        else:
            self.logger.warning("Could not find linescore table to locate pitcher roles.")

        self.logger.info(f"Extracted pitcher roles: {pitcher_roles}")
        return pitcher_roles