import argparse
from dataclasses import dataclass
from typing import Any

import pandas as pd

from ...classes.PostgresClient import PostgresClient
from ...classes.SportmonksAPI import SportmonksAPI
from ...utils.df_utils.build_table_columns import build_table_columns_from_df
from ...utils.logger import setup_logger
from ...utils import insert_dataframe_rows

logger = setup_logger(__name__)

TABLE_NAME = "sm_player_fixtures"
PRIMARY_KEY = "player_fixture_id"


@dataclass
class ProcessingState:
    """Tracks state across fixture processing iterations."""

    table_created: bool = False
    total_rows: int = 0


def build_player_fixture_row(
    lineup: dict[str, Any],
    fixture_data: dict[str, Any],
    participants: dict[int, str],
    api: SportmonksAPI,
) -> dict[str, Any] | None:
    """Build a single row for a player's fixture statistics."""
    player_id = lineup.get("player_id")
    if not player_id:
        return None

    fixture_id = fixture_data.get("id")
    team_id: int | None = lineup.get("team_id")
    team_name = participants.get(team_id, "") if team_id is not None else ""

    # Base player/fixture info
    row: dict[str, Any] = {
        "player_fixture_id": f"{player_id}_{fixture_id}",
        "player_id": player_id,
        "player_name": lineup.get("player_name", ""),
        "team_id": team_id,
        "team_name": team_name,
        "fixture_id": fixture_id,
        "fixture_date": fixture_data.get("starting_at", ""),
        "season_id": api.current_season_id,
        "position_id": lineup.get("position_id"),
        "jersey_number": lineup.get("jersey_number"),
        "formation_position": lineup.get("formation_position"),
        "is_starting": lineup.get("type_id") == 11,  # 11 = starting XI, 12 = bench
    }

    # Flatten player statistics from this fixture
    details = lineup.get("details", [])
    row.update(api.flatten_lineup_details(details))

    return row


def main(schema: str, limit_fixtures: int | None = None) -> None:
    """Scrape Premier League player fixture stats and load into database."""
    db = PostgresClient()
    db.create_schema(schema)

    api = SportmonksAPI()

    # Get completed fixtures only
    fixtures = api.get_fixtures(include_future=False)

    if not fixtures:
        logger.warning("No completed fixtures found")
        db.close()
        return

    if limit_fixtures:
        # Sort by date descending and take most recent N fixtures
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
            # Fetch fixture with lineups and details
            fixture_data = api.get_fixture_with_lineups(fixture_id)

            lineups = fixture_data.get("lineups", [])
            participants = {
                p.get("id"): p.get("name") for p in fixture_data.get("participants", [])
            }

            for lineup in lineups:
                row = build_player_fixture_row(lineup, fixture_data, participants, api)
                if row:
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

    # Fill missing values based on dtype
    for col in df.columns:
        if df[col].dtype == "boolean":
            df[col] = df[col].fillna(False)
        elif df[col].dtype in ["Int64", "Float64", "int64", "float64"]:
            df[col] = df[col].fillna(0)
        elif df[col].dtype == "object" or str(df[col].dtype).startswith("string"):
            df[col] = df[col].fillna("")

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
