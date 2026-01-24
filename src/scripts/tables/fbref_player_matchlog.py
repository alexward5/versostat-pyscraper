import argparse
from dataclasses import dataclass

import pandas as pd

from ...classes.PostgresClient import PostgresClient
from ...classes.SportsRefScraper import SportsRefScraper
from ...utils.df_utils.add_id_column import add_id_column
from ...utils.df_utils.build_table_columns import build_table_columns_from_df
from ...utils.logger import setup_logger
from ..helpers import insert_dataframe_rows, reorder_columns, validate_column_schema

logger = setup_logger(__name__)

BASE_URL = "https://fbref.com"
PREMIER_LEAGUE_URL = f"{BASE_URL}/en/comps/9/Premier-League-Stats"
TABLE_NAME = "fbref_player_matchlog"
PRIMARY_KEY = "fbref_player_matchweek_uuid"
PLAYER_UUID = "fbref_player_uuid"


@dataclass
class ProcessingState:
    """Tracks state across player processing iterations."""

    table_created: bool = False
    total_rows: int = 0
    reference_columns: list[str] | None = None


def has_required_columns(df: pd.DataFrame, columns: set[str], context: str) -> bool:
    """Check if dataframe has required columns, logging a warning if not."""
    missing = columns - set(df.columns)
    if missing:
        logger.warning("Skipping %s: missing columns %s", context, missing)
        return False
    return True


def process_player_matchlog(
    scraper: SportsRefScraper,
    player_name: str,
    squad_name: str,
    matchlog_url: str,
) -> pd.DataFrame | None:
    """
    Scrape and process a single player's matchlog.
    Returns None if the player should be skipped.
    """
    matchlog_df = scraper.scrape_table(matchlog_url, table_index=0)

    if not has_required_columns(matchlog_df, {"comp"}, f"{player_name} ({squad_name})"):
        return None

    # Filter for Premier League matches only
    matchlog_df = matchlog_df[matchlog_df["comp"] == "Premier League"].copy()
    if matchlog_df.empty:
        logger.info("  No Premier League matches found for %s", player_name)
        return None

    if not has_required_columns(matchlog_df, {"round"}, f"{player_name} ({squad_name})"):
        return None

    # Add identifying columns
    matchlog_df["player"] = player_name
    matchlog_df["squad"] = squad_name

    # Add UUID columns
    matchlog_df = add_id_column(
        matchlog_df, source_columns=["player", "squad"], id_column_name=PLAYER_UUID
    )
    matchlog_df = add_id_column(
        matchlog_df, source_columns=["player", "squad", "round"], id_column_name=PRIMARY_KEY
    )

    return reorder_columns(matchlog_df, [PRIMARY_KEY, PLAYER_UUID])


def main(schema: str) -> None:
    """Scrape Premier League player matchlog data and load into database."""
    db = PostgresClient()
    db.create_schema(schema)

    scraper = SportsRefScraper()
    standings_df = scraper.scrape_table(PREMIER_LEAGUE_URL, table_index=0)

    required_cols = {"squad", "squad_url"}
    if not required_cols.issubset(standings_df.columns):
        raise ValueError(f"Missing required columns: {required_cols - set(standings_df.columns)}")

    teams = [
        {"squad": str(row["squad"]), "url": f"{BASE_URL}{row['squad_url']}"}
        for _, row in standings_df.iterrows()
        if row["squad_url"]
    ]

    logger.info("Processing %s teams", len(teams))
    state = ProcessingState()

    for team in teams:
        squad_name = team["squad"]
        logger.info_with_newline("Processing team: %s...", squad_name)

        team_df = scraper.scrape_table(team["url"], table_index=0)

        if not has_required_columns(team_df, {"player", "matches_url"}, squad_name):
            continue

        players = [
            {"name": str(row["player"]), "url": f"{BASE_URL}{row['matches_url']}"}
            for _, row in team_df.iterrows()
            if row["matches_url"]
        ]

        logger.info("Found %s players for %s", len(players), squad_name)

        for player in players:
            player_name = player["name"]
            logger.info("  Processing player: %s...", player_name)

            try:
                matchlog_df = process_player_matchlog(
                    scraper, player_name, squad_name, player["url"]
                )
                if matchlog_df is None:
                    continue

                # Set reference schema from first player, validate subsequent players match
                context = f"{player_name} ({squad_name})"
                if state.reference_columns is None:
                    state.reference_columns = list[str](matchlog_df.columns)
                    logger.info(
                        "Reference schema set from %s: %s columns",
                        context,
                        len(state.reference_columns),
                    )
                else:
                    validate_column_schema(matchlog_df, state.reference_columns, context)
                    matchlog_df.columns = state.reference_columns

                if not state.table_created:
                    columns = build_table_columns_from_df(matchlog_df, PRIMARY_KEY)
                    db.create_table(schema, TABLE_NAME, columns)
                    state.table_created = True

                insert_dataframe_rows(db, schema, TABLE_NAME, matchlog_df, PRIMARY_KEY)
                state.total_rows += len(matchlog_df)
                logger.info("  Inserted %s rows for %s", len(matchlog_df), player_name)

            except Exception as e:
                logger.error("Error processing %s (%s): %s", player_name, squad_name, e)

    db.close()

    logger.info_with_newline("=" * 60)
    logger.info("Completed: %s teams, %s total rows", len(teams), state.total_rows)
    logger.info("Table: %s.%s", schema, TABLE_NAME)
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape FBref player matchlog data")
    parser.add_argument("--schema", type=str, required=True, help="Database schema name to use")
    args = parser.parse_args()
    main(args.schema)
