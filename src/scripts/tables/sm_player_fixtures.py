"""
Sportmonks Player Fixtures Scraper.

NOTE: This script requires fixture-level data which may not be available
on all Sportmonks API plans. Run with caution and expect potential API errors.
"""
import argparse
from dataclasses import dataclass
from typing import Any

import pandas as pd

from ...classes.PostgresClient import PostgresClient
from ...classes.SportsmonksAPI import SportsmonksAPI
from ...utils.df_utils.build_table_columns import build_table_columns_from_df
from ...utils.logger import setup_logger
from ..helpers import insert_dataframe_rows

logger = setup_logger(__name__)

TABLE_NAME = "sm_player_fixtures"
PRIMARY_KEY = "player_fixture_id"


@dataclass
class ProcessingState:
    """Tracks state across fixture processing iterations."""

    table_created: bool = False
    total_rows: int = 0


def flatten_player_fixture_stats(
    player_id: int,
    player_name: str,
    fixture_id: int,
    fixture_data: dict[str, Any],
    api: SportsmonksAPI,
) -> dict[str, Any]:
    """Flatten player statistics from a fixture into a single row."""
    flat: dict[str, Any] = {
        "player_fixture_id": f"{player_id}_{fixture_id}",
        "player_id": player_id,
        "player_name": player_name,
        "fixture_id": fixture_id,
        "season_id": api.current_season_id,
    }

    # Add fixture metadata if available
    if fixture_data:
        flat["fixture_date"] = fixture_data.get("starting_at", "")
        flat["home_team_id"] = fixture_data.get("participants", [{}])[0].get("id", "")
        flat["away_team_id"] = (
            fixture_data.get("participants", [{}])[1].get("id", "")
            if len(fixture_data.get("participants", [])) > 1
            else ""
        )

    # Flatten statistics
    statistics = fixture_data.get("statistics", [])
    flat.update(api.flatten_statistics(statistics))

    return flat


def main(schema: str, limit_fixtures: int | None = None) -> None:
    """Scrape Premier League player fixture stats and load into database.

    NOTE: This endpoint may require a higher Sportmonks plan.
    """
    db = PostgresClient()
    db.create_schema(schema)

    api = SportsmonksAPI()

    # Try to get fixtures
    fixtures = api.get_fixtures_by_season()

    if not fixtures:
        logger.warning("No fixtures available - this endpoint may require a plan upgrade")
        db.close()
        return

    if limit_fixtures:
        fixtures = fixtures[:limit_fixtures]
        logger.info("Limited to first %s fixtures for testing", limit_fixtures)

    logger.info("Processing %s fixtures", len(fixtures))
    state = ProcessingState()

    all_player_fixture_stats: list[dict[str, object]] = []

    for fixture in fixtures:
        fixture_id = fixture.get("id")
        logger.info("Processing fixture ID: %s...", fixture_id)

        if not fixture_id:
            continue

        try:
            # Get fixture details with statistics
            fixture_details = api.get_fixture_statistics(fixture_id)

            if not fixture_details:
                logger.warning("No statistics for fixture %s", fixture_id)
                continue

            # Extract player statistics from fixture
            # Structure varies by API response - adapt as needed
            lineups = fixture_details.get("lineups", [])
            for lineup in lineups:
                player_id = lineup.get("player_id")
                player_name = lineup.get("player_name", "")

                if player_id:
                    player_stats = flatten_player_fixture_stats(
                        player_id, player_name, fixture_id, fixture_details, api
                    )
                    all_player_fixture_stats.append(player_stats)

        except Exception as e:
            logger.error("Error processing fixture %s: %s", fixture_id, e)

    if not all_player_fixture_stats:
        logger.warning("No player fixture statistics collected")
        db.close()
        return

    # Build DataFrame
    logger.info("Building DataFrame from %s records...", len(all_player_fixture_stats))
    df = pd.DataFrame(all_player_fixture_stats)
    df = df.convert_dtypes()

    # Fill missing values
    for col in df.select_dtypes(include=["number"]).columns:
        df[col] = df[col].fillna(0)
    df = df.fillna("")

    # Ensure primary key is first
    if PRIMARY_KEY in df.columns:
        cols = [PRIMARY_KEY] + [c for c in df.columns if c != PRIMARY_KEY]
        df = df[cols]

    # Create table and insert
    columns = build_table_columns_from_df(df, PRIMARY_KEY)
    db.create_table(schema, TABLE_NAME, columns)

    insert_dataframe_rows(db, schema, TABLE_NAME, df, PRIMARY_KEY)
    state.total_rows = len(df)

    db.close()

    logger.info_with_newline("=" * 60)
    logger.info("Completed: %s fixtures, %s rows inserted", len(fixtures), state.total_rows)
    logger.info("Table: %s.%s", schema, TABLE_NAME)
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Sportmonks player fixture stats")
    parser.add_argument("--schema", type=str, required=True, help="Database schema name to use")
    parser.add_argument(
        "--limit-fixtures",
        type=int,
        default=None,
        help="Limit to first N fixtures for testing",
    )
    args = parser.parse_args()
    main(args.schema, args.limit_fixtures)
