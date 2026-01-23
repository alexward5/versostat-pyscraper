"""
Script to scrape FBref team player standard stats.

This script:
1. Scrapes the Premier League standings table
2. Extracts team URLs from the Squad column
3. For each team, scrapes their player standard stats
4. Combines all team data with a team identifier column

Usage:
    python fbref_team_players_standard.py --schema <schema_name>
"""

import argparse
import sys
from pathlib import Path
from typing import TypedDict

import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.classes.SportsRefScraper import SportsRefScraper
from src.utils.df_utils.inspect_df import inspect_df


class TeamData(TypedDict):
    squad: str
    url: str


BASE_URL = "https://fbref.com"
PREMIER_LEAGUE_URL = "https://fbref.com/en/comps/9/Premier-League-Stats"


def main(schema: str) -> None:
    """Main function to scrape and process team player stats."""
    
    # Initialize scraper
    scraper = SportsRefScraper()
    
    # Step 1: Scrape Premier League standings table
    print("Scraping Premier League standings...")
    standings_df = scraper.scrape_table(PREMIER_LEAGUE_URL, table_index=0)
    
    # Step 2: Extract team URLs and names from Squad column
    print("\nExtracting team URLs...")
    
    # Verify required columns exist
    if "squad" not in standings_df.columns or "squad_url" not in standings_df.columns:
        raise ValueError("Expected 'squad' and 'squad_url' columns not found in standings table")
    
    # Create team data: squad name + full URL
    teams_data: list[TeamData] = []
    for _, row in standings_df.iterrows():
        squad: str = str(row["squad"])
        squad_url: str = str(row["squad_url"])
        
        if squad_url:  # Only process rows with URLs
            full_url = BASE_URL + squad_url
            teams_data.append({
                "squad": squad,
                "url": full_url
            })
    
    print(f"Found {len(teams_data)} teams with URLs")
    
    # Step 2b: TESTING - Filter to only Arsenal
    print("\n[TESTING] Filtering to Arsenal only...")
    teams_data = [team for team in teams_data if team["squad"] == "Arsenal"]
    
    if not teams_data:
        raise ValueError("Arsenal not found in teams data")
    
    print(f"Filtered to {len(teams_data)} team(s): {[t['squad'] for t in teams_data]}")
    
    # Step 3: For each team, scrape player stats and add squad column
    all_player_data: list[pd.DataFrame] = []
    
    for team in teams_data:
        squad_name: str = team["squad"]
        team_url: str = team["url"]
        
        print(f"\nScraping player stats for {squad_name}...")
        print(f"URL: {team_url}")
        
        # Scrape team's player stats table (index 0)
        players_df = scraper.scrape_table(team_url, table_index=0)
        
        # Add squad column to identify which team these players belong to
        players_df["squad"] = squad_name
        
        all_player_data.append(players_df)
        
        # Step 3b: TESTING - Inspect Arsenal DataFrame
        if squad_name == "Arsenal":
            print(f"\n[TESTING] Inspecting {squad_name} player data:")
            inspect_df(players_df)
    
    print("\n" + "="*80)
    print("Script completed successfully!")
    print(f"Schema parameter received: {schema}")
    print("="*80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape FBref team player standard stats"
    )
    parser.add_argument(
        "--schema",
        type=str,
        required=True,
        help="Database schema name to use"
    )
    
    args = parser.parse_args()
    main(args.schema)
