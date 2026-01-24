import argparse
from dataclasses import dataclass

import pandas as pd

from ...classes.PostgresClient import PostgresClient
from ...classes.SportsmonksAPI import SportsmonksAPI
from ...utils.df_utils.build_table_columns import build_table_columns_from_df
from ...utils.logger import setup_logger
from ..helpers import insert_dataframe_rows

logger = setup_logger(__name__)

TABLE_NAME = "sm_player_overall"
PRIMARY_KEY = "player_id"


@dataclass
class ProcessingState:
    """Tracks state across player processing iterations."""

    table_created: bool = False
    total_rows: int = 0
    reference_columns: list[str] | None = None


def main(schema: str, limit_teams: int | None = None) -> None:
    """Scrape Premier League player overall stats and load into database."""
    db = PostgresClient()
    db.create_schema(schema)

    api = SportsmonksAPI()

    # Get all Premier League teams
    teams = api.get_teams()
    if limit_teams:
        teams = teams[:limit_teams]
        logger.info("Limited to first %s teams for testing", limit_teams)

    logger.info("Processing %s teams", len(teams))
    state = ProcessingState()

    all_player_stats: list[dict[str, object]] = []

    for team in teams:
        team_id = team.get("id")
        team_name = team.get("name", "Unknown")
        logger.info_with_newline("Processing team: %s (ID: %s)...", team_name, team_id)

        if not team_id:
            logger.warning("Skipping team with no ID")
            continue

        # Get players for this team
        players = api.get_players_by_team(team_id)
        logger.info("Found %s players for %s", len(players), team_name)

        for player in players:
            player_id = player.get("player_id")
            if not player_id:
                continue

            try:
                # Fetch player statistics
                player_stats = api.get_player_statistics(player_id)

                # Add team info
                player_stats["team_id"] = team_id
                player_stats["team_name"] = team_name
                player_stats["season_id"] = api.current_season_id

                all_player_stats.append(player_stats)
                logger.info("  Processed player: %s", player_stats.get("player_name", player_id))

            except Exception as e:
                logger.error("Error processing player %s: %s", player_id, e)

    if not all_player_stats:
        logger.warning("No player statistics collected")
        db.close()
        return

    # Build DataFrame from all collected stats
    logger.info("Building DataFrame from %s player records...", len(all_player_stats))
    df = pd.DataFrame(all_player_stats)

    # Convert dtypes
    df = df.convert_dtypes()

    # Fill missing values: numeric with 0, others with empty string
    for col in df.select_dtypes(include=["number"]).columns:
        df[col] = df[col].fillna(0)
    df = df.fillna("")

    # Ensure primary key is first column
    if PRIMARY_KEY in df.columns:
        cols = [PRIMARY_KEY] + [c for c in df.columns if c != PRIMARY_KEY]
        df = df[cols]

    logger.info("DataFrame columns (%s): %s", len(df.columns), list(df.columns)[:20])

    # Create table and insert data
    columns = build_table_columns_from_df(df, PRIMARY_KEY)
    db.create_table(schema, TABLE_NAME, columns)

    insert_dataframe_rows(db, schema, TABLE_NAME, df, PRIMARY_KEY)
    state.total_rows = len(df)

    db.close()

    logger.info_with_newline("=" * 60)
    logger.info("Completed: %s teams, %s player rows", len(teams), state.total_rows)
    logger.info("Table: %s.%s", schema, TABLE_NAME)
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Sportmonks player overall stats")
    parser.add_argument("--schema", type=str, required=True, help="Database schema name to use")
    parser.add_argument(
        "--limit-teams",
        type=int,
        default=None,
        help="Limit to first N teams for testing",
    )
    args = parser.parse_args()
    main(args.schema, args.limit_teams)
