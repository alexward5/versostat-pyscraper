import argparse

import pandas as pd

from ...classes.FPL_API import FPL_API
from ...classes.PostgresClient import PostgresClient
from ...utils.df_utils import prepare_for_insert, serialize_nested_data
from ...utils.df_utils.build_table_columns import build_table_columns_from_df
from ...utils.logger import setup_logger

logger = setup_logger(__name__)

TABLE_NAME = "fpl_player"
PRIMARY_KEY = "id"


def main(schema: str) -> None:
    """Fetch FPL player data and load into database."""
    db = PostgresClient()
    db.create_schema(schema)

    api = FPL_API()
    logger.info("Fetching players from FPL API...")
    all_players = api.get_players()

    # Fetch teams to create team id -> team name mapping
    logger.info("Fetching teams from FPL API...")
    teams = api.get_teams()
    team_id_to_name: dict[int, str] = {t["id"]: t["name"] for t in teams}
    logger.info("Built team mapping for %s teams", len(team_id_to_name))

    # Filter to only selectable players who have played this season
    players = [p for p in all_players if p.get("minutes", 0) > 0 and p.get("can_select", False)]
    logger.info(
        "Retrieved %s players (%s selectable with minutes > 0)", len(all_players), len(players)
    )

    df = pd.DataFrame(players)

    # Add team_name column by mapping team id to team name
    df["team_name"] = df["team"].map(team_id_to_name)

    df = serialize_nested_data(df)
    df = prepare_for_insert(df, PRIMARY_KEY)

    columns = build_table_columns_from_df(df, PRIMARY_KEY)
    db.create_table(schema, TABLE_NAME, columns)

    db.insert_dataframe(schema, TABLE_NAME, df, PRIMARY_KEY)

    db.close()

    logger.info_with_newline("=" * 60)
    logger.info("Completed: %s players inserted", len(df))
    logger.info("Table: %s.%s", schema, TABLE_NAME)
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch FPL player data")
    parser.add_argument("--schema", type=str, required=True, help="Database schema name to use")
    args = parser.parse_args()
    main(args.schema)
