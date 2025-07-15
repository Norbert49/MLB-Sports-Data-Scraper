import pandas as pd
import logging
from typing import Dict, List, Any, Optional

class MLBInsightsGenerator:
    """
    Generates insightful comments and observations from parsed MLB box score data.
    Analyzes batting, pitching, lineup, and game details to highlight key events
    and player performances.
    """

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO) # Set logging level for this module

    def generate_insights(self, 
                          batting_df: pd.DataFrame, 
                          pitching_df: pd.DataFrame, 
                          lineup_df: pd.DataFrame, 
                          game_details: Dict[str, Any]) -> Dict[str, List[str]]:
        """
        Generates a dictionary of insights categorized by type.

        Args:
            batting_df (pd.DataFrame): DataFrame containing batting statistics.
            pitching_df (pd.DataFrame): DataFrame containing pitching statistics.
            lineup_df (pd.DataFrame): DataFrame containing starting lineup information.
            game_details (Dict[str, Any]): Dictionary with game-level details (e.g., scores, teams, winner).

        Returns:
            Dict[str, List[str]]: A dictionary where keys are insight categories
                                  (e.g., 'Game Summary', 'Batting Highlights', 'Pitching Highlights')
                                  and values are lists of insightful comments.
        """
        insights = {
            'Game Summary': [],
            'Batting Highlights': [],
            'Pitching Highlights': [],
            'Lineup Impact': [],
            'Anomalies & Notable Stats': []
        }

        self.logger.info("Generating game summary insights...")
        insights['Game Summary'].extend(self._get_game_summary_insights(game_details))
        
        self.logger.info("Generating batting highlights...")
        insights['Batting Highlights'].extend(self._get_batting_highlights(batting_df, lineup_df))

        self.logger.info("Generating pitching highlights...")
        insights['Pitching Highlights'].extend(self._get_pitching_highlights(pitching_df))
        
        self.logger.info("Checking for lineup impact...")
        insights['Lineup Impact'].extend(self._get_lineup_impact_insights(batting_df, lineup_df))

        self.logger.info("Identifying anomalies and notable stats...")
        insights['Anomalies & Notable Stats'].extend(self._get_anomalies_and_notable_stats(batting_df, pitching_df))

        # Filter out empty categories
        filtered_insights = {k: v for k, v in insights.items() if v}
        self.logger.info("Finished generating insights.")
        return filtered_insights

    # -------------------------------------------------------------------------
    # Private Helper Methods for Insight Generation
    # -------------------------------------------------------------------------

    def _get_game_summary_insights(self, game_details: Dict[str, Any]) -> List[str]:
        """Generates insights based on overall game results."""
        comments = []
        try:
            home_team = game_details.get('home_team', 'Home Team')
            away_team = game_details.get('away_team', 'Away Team')
            home_score = game_details.get('home_score')
            away_score = game_details.get('away_score')
            winner = game_details.get('winner')
            loser = game_details.get('loser')

            if winner and home_score is not None and away_score is not None:
                comments.append(f"⚾ The {winner} defeated the {loser} in a game ending {away_score}-{home_score}.")
                score_diff = abs(home_score - away_score)
                if score_diff >= 5:
                    comments.append("It was a **decisive victory**, indicating strong performance from the winning side.")
                elif score_diff <= 2:
                    comments.append("A **close contest** decided by just a few runs.")
                else:
                    comments.append("The game had a moderate run differential.")
            else:
                comments.append("Game summary details are incomplete.")

        except Exception as e:
            self.logger.error(f"Error generating game summary insights: {e}")
        return comments

    def _get_batting_highlights(self, batting_df: pd.DataFrame, lineup_df: pd.DataFrame) -> List[str]:
        """Generates insights focusing on individual and team batting performance."""
        comments = []
        if batting_df.empty:
            comments.append("No batting data available for analysis.")
            return comments

        # Ensure numeric columns are actually numeric
        for col in ['H', 'HR', 'RBI', 'SO', 'AB', 'BB', 'PA', 'AVG', 'OBP', 'SLG', 'OPS']:
            if col in batting_df.columns:
                batting_df[col] = pd.to_numeric(batting_df[col], errors='coerce')

        # Top performers
        top_hitters = batting_df.nlargest(3, 'H', default_value=0).dropna(subset=['H'])
        for _, player_row in top_hitters.iterrows():
            if player_row['H'] >= 3: # Players with 3 or more hits
                comments.append(f"✨ **{player_row['player']}** ({player_row['team']}) had a fantastic day at the plate with **{int(player_row['H'])} hits**.")
            elif player_row['H'] == 2 and player_row['AB'] >= 4:
                 comments.append(f"👍 **{player_row['player']}** ({player_row['team']}) contributed with **{int(player_row['H'])} hits**.")


        top_HRs = batting_df.nlargest(2, 'HR', default_value=0).dropna(subset=['HR'])
        for _, player_row in top_HRs.iterrows():
            if player_row['HR'] >= 1:
                comments.append(f"💥 **{player_row['player']}** ({player_row['team']}) launched **{int(player_row['HR'])} home run(s)**.")

        top_RBIs = batting_df.nlargest(2, 'RBI', default_value=0).dropna(subset=['RBI'])
        for _, player_row in top_RBIs.iterrows():
            if player_row['RBI'] >= 3:
                comments.append(f"💰 **{player_row['player']}** ({player_row['team']}) was a run-producing machine with **{int(player_row['RBI'])} RBI**.")
            elif player_row['RBI'] == 2:
                comments.append(f"💵 **{player_row['player']}** ({player_row['team']}) brought in **{int(player_row['RBI'])} runs**.")


        # Team batting performance
        team_totals = batting_df.groupby('team').agg(
            total_H=('H', 'sum'),
            total_R=('R', 'sum'),
            total_HR=('HR', 'sum')
        ).reset_index()

        for _, team_row in team_totals.iterrows():
            comments.append(f"📊 The **{team_row['team']}** accumulated **{int(team_row['total_H'])} total hits** and **{int(team_row['total_R'])} runs**.")
            if team_row['total_HR'] > 0:
                comments.append(f"The {team_row['team']} hit a total of **{int(team_row['total_HR'])} home run(s)**.")

        return comments

    def _get_pitching_highlights(self, pitching_df: pd.DataFrame) -> List[str]:
        """Generates insights focusing on individual and team pitching performance."""
        comments = []
        if pitching_df.empty:
            comments.append("No pitching data available for analysis.")
            return comments

        # Ensure numeric columns
        for col in ['IP', 'H', 'R', 'ER', 'BB', 'SO', 'ERA']:
            if col in pitching_df.columns:
                pitching_df[col] = pd.to_numeric(pitching_df[col], errors='coerce')

        # Identify starting pitchers (usually first pitcher for each team)
        # This assumes pitching_df is ordered by appearance or a clear starter indicator exists.
        # For simplicity, we'll look for pitchers with significant innings pitched.
        starting_pitchers = pitching_df[pitching_df['IP'] >= 4].sort_values(by='IP', ascending=False).drop_duplicates(subset='team')

        for _, pitcher_row in starting_pitchers.iterrows():
            comments.append(f"⚾ On the mound for **{pitcher_row['team']}**: **{pitcher_row['player']}** pitched **{pitcher_row['IP']} innings**, allowing **{int(pitcher_row['ER'])} earned run(s)** and striking out **{int(pitcher_row['SO'])} batters**.")
            if pitcher_row['SO'] >= 6:
                comments.append(f"  - A strong outing with **{int(pitcher_row['SO'])} strikeouts**.")
            if pitcher_row['ER'] == 0 and pitcher_row['IP'] >= 5:
                comments.append(f"  - **{pitcher_row['player']}** delivered a **shutout performance** over {pitcher_row['IP']} innings.")

        # Identify relief pitchers with notable performance (e.g., high strikeouts in few innings, or very low ER)
        relief_pitchers = pitching_df[pitching_df['IP'] < 4].sort_values(by='IP', ascending=False)
        for _, reliever_row in relief_pitchers.iterrows():
            if reliever_row['SO'] >= 3 and reliever_row['IP'] >= 1:
                comments.append(f"🔥 Relief pitcher **{reliever_row['player']}** ({reliever_row['team']}) had a dominant short stint with **{int(reliever_row['SO'])} strikeouts** in {reliever_row['IP']} innings.")
            if reliever_row['ER'] == 0 and reliever_row['IP'] >= 1:
                comments.append(f"🛡️ **{reliever_row['player']}** ({reliever_row['team']}) contributed with a **scoreless appearance**.")

        # Team pitching performance (total runs allowed, strikeouts)
        team_pitching_totals = pitching_df.groupby('team').agg(
            total_R_allowed=('R', 'sum'),
            total_ER_allowed=('ER', 'sum'),
            total_SO=('SO', 'sum')
        ).reset_index()

        for _, team_row in team_pitching_totals.iterrows():
            comments.append(f"📈 The **{team_row['team']}** pitching staff combined for **{int(team_row['total_SO'])} strikeouts** and allowed **{int(team_row['total_ER_allowed'])} earned runs**.")

        return comments

    def _get_lineup_impact_insights(self, batting_df: pd.DataFrame, lineup_df: pd.DataFrame) -> List[str]:
        """Analyzes how specific lineup positions performed."""
        comments = []
        if batting_df.empty or lineup_df.empty:
            return comments

        # Merge batting and lineup data
        merged_df = pd.merge(batting_df, lineup_df, on=['player', 'team'], how='inner', suffixes=('_batting', '_lineup'))

        # Ensure numeric columns
        for col in ['H', 'RBI', 'HR', 'SO', 'AB', 'BB', 'batting_order']:
            if col in merged_df.columns:
                merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce')
        
        # Focus on key batting order positions
        for team in merged_df['team'].unique():
            team_lineup = merged_df[merged_df['team'] == team].dropna(subset=['batting_order'])

            # Leadoff hitter (Batting Order 1)
            leadoff_hitter = team_lineup[team_lineup['batting_order'] == 1].iloc[0] if not team_lineup[team_lineup['batting_order'] == 1].empty else None
            if leadoff_hitter is not None and leadoff_hitter['H'] is not None and leadoff_hitter['BB'] is not None:
                comments.append(f"🚶‍♂️ **{leadoff_hitter['player']}** (leadoff for {team}) was on base {int(leadoff_hitter['H'] + leadoff_hitter['BB'])} times (H + BB).")
                if leadoff_hitter['OBP'] is not None and leadoff_hitter['OBP'] >= 0.400:
                    comments.append(f"  - Excellent OBP of {leadoff_hitter['OBP']:.3f} for the leadoff spot.")

            # Cleanup hitter (Batting Order 4)
            cleanup_hitter = team_lineup[team_lineup['batting_order'] == 4].iloc[0] if not team_lineup[team_lineup['batting_order'] == 4].empty else None
            if cleanup_hitter is not None and cleanup_hitter['H'] is not None and cleanup_hitter['RBI'] is not None:
                comments.append(f"💪 **{cleanup_hitter['player']}** (cleanup for {team}) went {int(cleanup_hitter['H'])} for {int(cleanup_hitter['AB'])} with {int(cleanup_hitter['RBI'])} RBI.")
                if cleanup_hitter['HR'] is not None and cleanup_hitter['HR'] >= 1:
                    comments.append(f"  - Also hit a home run from the cleanup spot.")

            # Bottom of the order (Batting Order 7, 8, 9)
            bottom_order = team_lineup[team_lineup['batting_order'].isin([7, 8, 9])]
            if not bottom_order.empty:
                bottom_order_hits = bottom_order['H'].sum()
                if bottom_order_hits >= 3:
                    comments.append(f"🔋 The bottom of the order for **{team}** contributed significantly with **{int(bottom_order_hits)} hits**.")
        
        return comments

    def _get_anomalies_and_notable_stats(self, batting_df: pd.DataFrame, pitching_df: pd.DataFrame) -> List[str]:
        """Identifies unusual or particularly interesting statistics."""
        comments = []

        # Batting anomalies
        if not batting_df.empty:
            batting_df['SO'] = pd.to_numeric(batting_df['SO'], errors='coerce')
            batting_df['AB'] = pd.to_numeric(batting_df['AB'], errors='coerce')

            # High Strikeouts
            high_SO_players = batting_df[batting_df['SO'] >= 3].dropna(subset=['SO'])
            for _, player_row in high_SO_players.iterrows():
                comments.append(f"📉 **{player_row['player']}** ({player_row['team']}) struggled at the plate with **{int(player_row['SO'])} strikeouts**.")
            
            # Perfect Game (if any player had 4+ AB and 0 hits)
            zero_hit_players = batting_df[(batting_df['H'] == 0) & (batting_df['AB'] >= 4)].dropna(subset=['H', 'AB'])
            for _, player_row in zero_hit_players.iterrows():
                comments.append(f"⚪ **{player_row['player']}** ({player_row['team']}) went 0 for {int(player_row['AB'])}.")

        # Pitching anomalies
        if not pitching_df.empty:
            pitching_df['IP'] = pd.to_numeric(pitching_df['IP'], errors='coerce')
            pitching_df['ER'] = pd.to_numeric(pitching_df['ER'], errors='coerce')
            pitching_df['BB'] = pd.to_numeric(pitching_df['BB'], errors='coerce')

            # High Walks
            high_BB_pitchers = pitching_df[pitching_df['BB'] >= 4].dropna(subset=['BB'])
            for _, pitcher_row in high_BB_pitchers.iterrows():
                comments.append(f"🚫 Pitcher **{pitcher_row['player']}** ({pitcher_row['team']}) had control issues, issuing **{int(pitcher_row['BB'])} walks**.")

            # Short outings with high ER
            bad_short_outings = pitching_df[(pitching_df['IP'] < 3) & (pitching_df['ER'] >= 3)].dropna(subset=['IP', 'ER'])
            for _, pitcher_row in bad_short_outings.iterrows():
                comments.append(f"🚨 Rough outing for **{pitcher_row['player']}** ({pitcher_row['team']}) with **{int(pitcher_row['ER'])} earned runs** in just {pitcher_row['IP']} innings.")

        return comments