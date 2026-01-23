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

from src.classes.PostgresClient import PostgresClient
from src.classes.SportsRefScraper import SportsRefScraper
from src.utils.df_utils.add_id_column import add_id_column
from src.utils.df_utils.build_table_columns import build_table_columns_from_df
from src.utils.df_utils.inspect_df import inspect_df
from src.utils.df_utils.sanitize_columns import sanitize_column_names


class TeamData(TypedDict):
    squad: str
    url: str


BASE_URL = "https://fbref.com"
PREMIER_LEAGUE_URL = "https://fbref.com/en/comps/9/Premier-League-Stats"
TABLE_NAME = "fbref_team_players_standard"
PRIMARY_KEY_COLUMN = "uuid"


def main(schema: str) -> None:
    """Main function to scrape and process team player stats."""

    # Step 0.5: Initialize database client and create schema
    print(f"Initializing database connection and schema '{schema}'...")
    db = PostgresClient()
    db.create_schema(schema)
    print(f"Schema '{schema}' ready")

    # Initialize scraper
    scraper = SportsRefScraper()

    # Track whether table has been created
    table_created = False

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
            teams_data.append({"squad": squad, "url": full_url})

    print(f"Found {len(teams_data)} teams with URLs")

    # Step 2b: TESTING - Filter to only Everton
    print("\n[TESTING] Filtering to Everton only...")
    teams_data = [team for team in teams_data if team["squad"] == "Everton"]

    if not teams_data:
        raise ValueError("Everton not found in teams data")

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

        # Sanitize column names for database compatibility
        players_df = sanitize_column_names(players_df)

        # Add squad column to identify which team these players belong to
        players_df["squad"] = squad_name

        # Step 4: Add UUID column based on player and squad
        players_df = add_id_column(
            players_df, source_columns=["player", "squad"], id_column_name=PRIMARY_KEY_COLUMN
        )

        # Move UUID column to the first position
        cols = [PRIMARY_KEY_COLUMN] + [
            col for col in players_df.columns if col != PRIMARY_KEY_COLUMN
        ]
        players_df = players_df[cols]

        all_player_data.append(players_df)

        # Step 3b: TESTING - Inspect Everton DataFrame
        if squad_name == "Everton":
            print(f"\n[TESTING] Inspecting {squad_name} player data:")
            inspect_df(players_df)

        # Step 5: Create table if it doesn't exist yet (only once)
        if not table_created:
            print(f"\nCreating table '{schema}.{TABLE_NAME}' if it doesn't exist...")
            columns = build_table_columns_from_df(players_df, PRIMARY_KEY_COLUMN)
            db.create_table(schema, TABLE_NAME, columns)
            table_created = True
            print(f"Table '{schema}.{TABLE_NAME}' ready")

        # Insert rows into the table
        print(f"Inserting {len(players_df)} rows for {squad_name}...")
        for _, row in players_df.iterrows():
            column_names = list(players_df.columns)
            row_values = [row[col] for col in column_names]
            db.insert_row(
                schema=schema,
                table_name=TABLE_NAME,
                column_names=column_names,
                row_values=row_values,
                update_on=PRIMARY_KEY_COLUMN,
            )
        print(f"Inserted {len(players_df)} rows for {squad_name}")

    # Close database connection
    db.close()

    print("\n" + "=" * 80)
    print("Script completed successfully!")
    print(f"Schema: {schema}")
    print(f"Table: {TABLE_NAME}")
    print(f"Total teams processed: {len(teams_data)}")
    print(f"Total rows inserted: {sum(len(df) for df in all_player_data)}")
    print("=" * 80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape FBref team player standard stats")
    parser.add_argument("--schema", type=str, required=True, help="Database schema name to use")

    args = parser.parse_args()
    main(args.schema)
