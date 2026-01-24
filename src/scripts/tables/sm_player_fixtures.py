"""
Sportmonks Player Fixtures Scraper.

NOTE: This script requires the 'lineups' include which is not available on the
current Sportmonks API plan. The lineups include provides per-player statistics
for each fixture.

To use this script, you need to upgrade to a plan that includes:
- lineups include on fixtures endpoint
- Player-level fixture statistics

Current plan provides:
- Fixtures list (working)
- Team-level fixture statistics (working - see sm_team_fixtures.py)
- Participants/scores (working)

Missing for player fixtures:
- lineups include (Error 5002: "You do not have access to the 'lineups' include")

When the plan is upgraded, this script will need to be updated to:
1. Fetch fixtures with 'lineups.player' and 'lineups.statistics' includes
2. Extract player-level stats from each fixture's lineup data
3. Build rows with player_id, fixture_id, and their per-match statistics
"""
import argparse
from dataclasses import dataclass
from typing import Any

import pandas as pd

from ...classes.PostgresClient import PostgresClient
from ...classes.SportmonksAPI import SportmonksAPI
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


def check_lineups_access(api: SportmonksAPI) -> bool:
    """Check if the current API plan has access to lineups include."""
    return api.check_lineups_access()


def main(schema: str, limit_fixtures: int | None = None) -> None:
    """Scrape Premier League player fixture stats and load into database.

    NOTE: This endpoint requires the 'lineups' include which may not be
    available on all API plans.
    """
    db = PostgresClient()
    db.create_schema(schema)

    api = SportmonksAPI()

    # Check if we have lineups access
    logger.info("Checking API access for lineups include...")
    if not check_lineups_access(api):
        logger.error("=" * 60)
        logger.error("PLAN UPGRADE REQUIRED")
        logger.error("=" * 60)
        logger.error("The 'lineups' include is not available on your current plan.")
        logger.error("This include is required to fetch player-level fixture statistics.")
        logger.error("")
        logger.error("Current plan supports:")
        logger.error("  - Team fixture statistics (use sm_team_fixtures.py)")
        logger.error("  - Player overall statistics (use sm_player_overall.py)")
        logger.error("")
        logger.error("To get player fixture stats, upgrade to a plan that includes:")
        logger.error("  - 'lineups' include on fixtures endpoint")
        logger.error("=" * 60)
        db.close()
        return

    # If we get here, lineups access is available
    # TODO: Implement full player fixture extraction when plan is upgraded
    logger.info("Lineups access confirmed - proceeding with player fixtures extraction")

    fixtures = api.get_fixtures(include_future=False)

    if not fixtures:
        logger.warning("No completed fixtures found")
        db.close()
        return

    if limit_fixtures:
        fixtures = sorted(fixtures, key=lambda x: x.get("starting_at", ""), reverse=True)
        fixtures = fixtures[:limit_fixtures]
        logger.info("Limited to %s most recent fixtures for testing", limit_fixtures)

    logger.info("Processing %s completed fixtures", len(fixtures))
    state = ProcessingState()

    all_player_fixture_stats: list[dict[str, Any]] = []

    for i, fixture in enumerate(fixtures):
        fixture_id = fixture.get("id")
        if not fixture_id:
            continue

        try:
            # Fetch fixture with lineups
            fixture_data = api.get_fixture_with_lineups(fixture_id)

            lineups = fixture_data.get("lineups", [])
            participants = {
                p.get("id"): p.get("name")
                for p in fixture_data.get("participants", [])
            }

            for lineup in lineups:
                player_id = lineup.get("player_id")
                team_id = lineup.get("team_id")
                player_data = lineup.get("player", {})

                if not player_id:
                    continue

                row: dict[str, Any] = {
                    "player_fixture_id": f"{player_id}_{fixture_id}",
                    "player_id": player_id,
                    "player_name": player_data.get("display_name", player_data.get("name", "")),
                    "team_id": team_id,
                    "team_name": participants.get(team_id, ""),
                    "fixture_id": fixture_id,
                    "fixture_date": fixture_data.get("starting_at", ""),
                    "season_id": api.current_season_id,
                    "position": lineup.get("position", ""),
                    "jersey_number": lineup.get("jersey_number"),
                    "is_starting": lineup.get("type_id") == 11,  # 11 = starting XI
                }

                # Flatten player statistics from this fixture
                statistics = lineup.get("statistics", [])
                row.update(api.flatten_statistics(statistics))

                all_player_fixture_stats.append(row)

            if (i + 1) % 10 == 0:
                logger.info("Processed %s/%s fixtures...", i + 1, len(fixtures))

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

    logger.info("DataFrame columns (%s): %s", len(df.columns), list(df.columns)[:15])

    # Create table and insert
    columns = build_table_columns_from_df(df, PRIMARY_KEY)
    db.create_table(schema, TABLE_NAME, columns)

    insert_dataframe_rows(db, schema, TABLE_NAME, df, PRIMARY_KEY)
    state.total_rows = len(df)

    db.close()

    logger.info_with_newline("=" * 60)
    logger.info("Completed: %s fixtures, %s player-fixture rows", len(fixtures), state.total_rows)
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
