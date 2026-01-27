from typing import Any


import argparse

import pandas as pd

from ...classes.PostgresClient import PostgresClient
from ...classes.SportmonksAPI import SportmonksAPI
from ...utils.df_utils.build_table_columns import build_table_columns_from_df
from ...utils.df_utils.prepare_for_insert import prepare_for_insert
from ...utils.logger import log_script_complete, log_script_start, setup_logger, should_log_progress

logger = setup_logger(__name__)

TABLE_NAME = "sm_team_overall"
PRIMARY_KEY = "team_id"


def main(schema: str, limit_teams: int | None = None) -> None:
    """Scrape Premier League team overall stats and load into database."""
    log_script_start(__name__)

    db = PostgresClient()
    db.create_schema(schema)

    api = SportmonksAPI()

    teams = api.get_teams()
    if limit_teams:
        teams = teams[:limit_teams]
        logger.info("Limited to first %s teams for testing", limit_teams)

    total_teams = len(teams)
    logger.info("Processing %s teams", total_teams)

    all_team_stats: list[dict[str, object]] = []

    for idx, team in enumerate[dict[str, Any]](teams):
        team_id = team.get("id")

        if should_log_progress(idx + 1, total_teams):
            logger.info(
                "Progress: %s/%s (%d%%)", idx + 1, total_teams, int((idx + 1) / total_teams * 100)
            )

        if not team_id:
            logger.warning("Skipping team with no ID")
            continue

        try:
            team_stats = api.get_team_statistics(team_id)

            team_stats["season_id"] = api.current_season_id

            all_team_stats.append(team_stats)

        except Exception as e:
            logger.error("Error processing team %s: %s", team_id, e)

    if not all_team_stats:
        logger.warning("No team statistics collected")
        db.close()
        return

    logger.info("Building DataFrame from %s team records...", len(all_team_stats))
    df = pd.DataFrame(all_team_stats)
    df = prepare_for_insert(df, PRIMARY_KEY)

    logger.info("DataFrame columns (%s): %s", len(df.columns), list(df.columns)[:20])

    columns = build_table_columns_from_df(df, PRIMARY_KEY)
    db.create_table(schema, TABLE_NAME, columns)

    db.insert_dataframe(schema, TABLE_NAME, df, PRIMARY_KEY)

    db.close()

    log_script_complete(
        __name__, schema=schema, table_name=TABLE_NAME, total_teams=len(teams), total_rows=len(df)
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Sportmonks team overall stats")
    parser.add_argument("--schema", type=str, required=True, help="Database schema name to use")
    parser.add_argument(
        "--limit-teams",
        type=int,
        default=None,
        help="Limit to first N teams for testing",
    )
    args = parser.parse_args()
    main(args.schema, args.limit_teams)
