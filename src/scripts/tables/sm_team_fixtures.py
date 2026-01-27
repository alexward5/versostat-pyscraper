import argparse
from typing import Any

import pandas as pd

from ...classes.PostgresClient import PostgresClient
from ...classes.SportmonksAPI import SportmonksAPI
from ...utils.df_utils.build_table_columns import build_table_columns_from_df
from ...utils.df_utils.prepare_for_insert import prepare_for_insert
from ...utils.logger import log_script_complete, log_script_start, setup_logger, should_log_progress

logger = setup_logger(__name__)

TABLE_NAME = "sm_team_fixtures"
PRIMARY_KEY = "team_fixture_id"


def build_team_fixture_row(
    fixture_data: dict[str, Any],
    team_id: int,
    team_name: str,
    opponent_id: int,
    opponent_name: str,
    is_home: bool,
    api: SportmonksAPI,
) -> dict[str, Any]:
    """Build a single row for a team's fixture statistics."""
    fixture_id = fixture_data.get("id")

    row: dict[str, Any] = {
        "team_fixture_id": f"{team_id}_{fixture_id}",
        "team_id": team_id,
        "team_name": team_name,
        "fixture_id": fixture_id,
        "season_id": api.current_season_id,
        "fixture_date": fixture_data.get("starting_at", ""),
        "is_home": is_home,
        "opponent_id": opponent_id,
        "opponent_name": opponent_name,
    }

    score_data = api.get_fixture_score(fixture_data, team_id)
    row.update(score_data)

    opponent_score = api.get_fixture_score(fixture_data, opponent_id)
    row["goals_conceded"] = opponent_score.get("goals_scored")

    goals_scored = row.get("goals_scored")
    goals_conceded = row.get("goals_conceded")
    if goals_scored is not None and goals_conceded is not None:
        if goals_scored > goals_conceded:
            row["result"] = "W"
        elif goals_scored < goals_conceded:
            row["result"] = "L"
        else:
            row["result"] = "D"
    else:
        row["result"] = ""

    stats = api.flatten_fixture_team_stats(fixture_data, team_id)
    row.update(stats)

    return row


def main(schema: str, limit_fixtures: int | None = None) -> None:
    """Scrape Premier League team fixture stats and load into database."""
    log_script_start(__name__)
    
    db = PostgresClient()
    db.create_schema(schema)

    api = SportmonksAPI()

    fixtures = api.get_completed_fixtures(limit=limit_fixtures)

    if not fixtures:
        logger.warning("No completed fixtures found")
        db.close()
        return

    total_fixtures = len(fixtures)
    logger.info("Processing %s completed fixtures", total_fixtures)

    all_team_fixture_stats: list[dict[str, object]] = []

    for i, fixture in enumerate(fixtures):
        fixture_id = fixture.get("id")

        if not fixture_id:
            continue

        if should_log_progress(i + 1, total_fixtures):
            logger.info("Progress: %s/%s (%d%%)", i + 1, total_fixtures, int((i + 1) / total_fixtures * 100))

        try:
            fixture_data = api.get_fixture_with_stats(fixture_id)

            if not fixture_data:
                logger.warning("No data for fixture %s", fixture_id)
                continue

            participants = fixture_data.get("participants", [])
            if len(participants) < 2:
                logger.warning("Fixture %s has incomplete participant data", fixture_id)
                continue

            home_team = participants[0]
            away_team = participants[1]

            home_id = home_team.get("id")
            home_name = home_team.get("name", "")
            away_id = away_team.get("id")
            away_name = away_team.get("name", "")

            home_row = build_team_fixture_row(
                fixture_data, home_id, home_name, away_id, away_name, True, api
            )
            all_team_fixture_stats.append(home_row)

            away_row = build_team_fixture_row(
                fixture_data, away_id, away_name, home_id, home_name, False, api
            )
            all_team_fixture_stats.append(away_row)

        except Exception as e:
            logger.error("Error processing fixture %s: %s", fixture_id, e)

    if not all_team_fixture_stats:
        logger.warning("No team fixture statistics collected")
        db.close()
        return

    logger.info("Building DataFrame from %s records...", len(all_team_fixture_stats))
    df = pd.DataFrame(all_team_fixture_stats)
    df = prepare_for_insert(df, PRIMARY_KEY)

    logger.info("DataFrame columns (%s): %s", len(df.columns), list(df.columns)[:15])

    columns = build_table_columns_from_df(df, PRIMARY_KEY)
    db.create_table(schema, TABLE_NAME, columns)

    db.insert_dataframe(schema, TABLE_NAME, df, PRIMARY_KEY)

    db.close()

    log_script_complete(
        __name__,
        schema=schema,
        table_name=TABLE_NAME,
        total_fixtures=len(fixtures),
        total_team_fixture_rows=len(df)
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Sportmonks team fixture stats")
    parser.add_argument("--schema", type=str, required=True, help="Database schema name to use")
    parser.add_argument(
        "--limit-fixtures",
        type=int,
        default=None,
        help="Limit to first N fixtures for testing",
    )
    args = parser.parse_args()
    main(args.schema, args.limit_fixtures)
