import argparse
from typing import Any

import pandas as pd

from ...classes.PostgresClient import PostgresClient
from ...classes.SportmonksAPI import SportmonksAPI
from ...utils.df_utils import add_id_column, standardize_to_date, transform_column
from ...utils.df_utils.build_table_columns import generate_column_definitions
from ...utils.df_utils.prepare_for_insert import prepare_for_insert
from ...utils.logger import log_script_complete, log_script_start, setup_logger, should_log_progress

logger = setup_logger(__name__)

TABLE_NAME = "sm_player_fixtures"
PRIMARY_KEY = "player_fixture_uuid"


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
    log_script_start(__name__)

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
    total_fixtures = len(fixtures)

    for i, fixture in enumerate(fixtures):
        fixture_id = fixture.get("id")
        if not fixture_id:
            continue

        if should_log_progress(i + 1, total_fixtures):
            logger.info(
                "Progress: %s/%s (%d%%)", i + 1, total_fixtures, int((i + 1) / total_fixtures * 100)
            )

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

        except Exception as e:
            logger.error("Error processing fixture %s: %s", fixture_id, e)

    if not all_player_fixture_stats:
        logger.warning("No player fixture statistics collected")
        db.close()
        return

    logger.info("Building DataFrame from %s records...", len(all_player_fixture_stats))
    df = pd.DataFrame(all_player_fixture_stats)

    df = add_id_column(df, source_columns=["player_id", "fixture_id"], id_column_name=PRIMARY_KEY)
    df = transform_column(df, "fixture_date", standardize_to_date)

    df = prepare_for_insert(df, PRIMARY_KEY)

    logger.info("DataFrame columns (%s): %s", len(df.columns), list(df.columns)[:15])

    column_definitions = generate_column_definitions(df, PRIMARY_KEY)
    db.create_table(schema, TABLE_NAME, column_definitions)

    db.insert_dataframe(schema, TABLE_NAME, df, PRIMARY_KEY)

    db.close()

    log_script_complete(
        __name__,
        schema=schema,
        table_name=TABLE_NAME,
        total_fixtures=len(fixtures),
        total_player_fixture_rows=len(df),
    )


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
