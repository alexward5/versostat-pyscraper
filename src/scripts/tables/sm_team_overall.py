import argparse

import pandas as pd

from ...classes.PostgresClient import PostgresClient
from ...classes.SportmonksAPI import SportmonksAPI
from ...utils.df_utils.build_table_columns import build_table_columns_from_df
from ...utils.df_utils.prepare_for_insert import prepare_for_insert
from ...utils.logger import setup_logger

logger = setup_logger(__name__)

TABLE_NAME = "sm_team_overall"
PRIMARY_KEY = "team_id"


def main(schema: str, limit_teams: int | None = None) -> None:
    """Scrape Premier League team overall stats and load into database."""
    db = PostgresClient()
    db.create_schema(schema)

    api = SportmonksAPI()

    teams = api.get_teams()
    if limit_teams:
        teams = teams[:limit_teams]
        logger.info("Limited to first %s teams for testing", limit_teams)

    logger.info("Processing %s teams", len(teams))

    all_team_stats: list[dict[str, object]] = []

    for team in teams:
        team_id = team.get("id")
        team_name = team.get("name", "Unknown")
        logger.info("Processing team: %s (ID: %s)...", team_name, team_id)

        if not team_id:
            logger.warning("Skipping team with no ID")
            continue

        try:
            team_stats = api.get_team_statistics(team_id)

            team_stats["season_id"] = api.current_season_id

            all_team_stats.append(team_stats)
            logger.info("  Processed team: %s", team_stats.get("team_name", team_id))

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

    logger.info_with_newline("=" * 60)
    logger.info("Completed: %s teams, %s rows inserted", len(teams), len(df))
    logger.info("Table: %s.%s", schema, TABLE_NAME)
    logger.info("=" * 60)


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
