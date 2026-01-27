import argparse
from typing import Any

import pandas as pd

from ...classes.PostgresClient import PostgresClient
from ...classes.SportmonksAPI import SportmonksAPI
from ...utils.df_utils.build_table_columns import build_table_columns_from_df
from ...utils.df_utils.prepare_for_insert import prepare_for_insert
from ...utils.logger import setup_logger

logger = setup_logger(__name__)

TABLE_NAME = "sm_player_fixtures"
PRIMARY_KEY = "player_fixture_id"


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
        "is_starting": lineup.get("type_id") == 11,
    }

    details = lineup.get("details", [])
    row.update(api.flatten_lineup_details(details))

    return row


def main(schema: str, limit_fixtures: int | None = None) -> None:
    """Scrape Premier League player fixture stats and load into database."""
    db = PostgresClient()
    db.create_schema(schema)

    api = SportmonksAPI()

    fixtures = api.get_completed_fixtures(limit=limit_fixtures)

    if not fixtures:
        logger.warning("No completed fixtures found")
        db.close()
        return

    logger.info("Processing %s completed fixtures", len(fixtures))

    all_player_fixture_stats: list[dict[str, Any]] = []

    for i, fixture in enumerate(fixtures):
        fixture_id = fixture.get("id")
        if not fixture_id:
            continue

        try:
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

    logger.info("Building DataFrame from %s records...", len(all_player_fixture_stats))
    df = pd.DataFrame(all_player_fixture_stats)
    df = prepare_for_insert(df, PRIMARY_KEY)

    logger.info("DataFrame columns (%s): %s", len(df.columns), list(df.columns)[:15])

    columns = build_table_columns_from_df(df, PRIMARY_KEY)
    db.create_table(schema, TABLE_NAME, columns)

    db.insert_dataframe(schema, TABLE_NAME, df, PRIMARY_KEY)

    db.close()

    logger.info_with_newline("=" * 60)
    logger.info("Completed: %s fixtures, %s player-fixture rows", len(fixtures), len(df))
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
