import argparse
import json

import numpy as np
import pandas as pd

from ...classes.FPL_API import FPL_API
from ...classes.PostgresClient import PostgresClient
from ...utils.df_utils.build_table_columns import build_table_columns_from_df
from ...utils.logger import setup_logger
from ..helpers import insert_dataframe_rows, reorder_columns

logger = setup_logger(__name__)

TABLE_NAME = "fpl_player"
PRIMARY_KEY = "id"


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and prepare DataFrame for database insertion."""
    # Detect nested dict/list columns by checking first value, then serialize to JSON
    for col in df.columns:
        non_null = df[col].dropna()
        if len(non_null) > 0 and isinstance(non_null.iloc[0], (dict, list)):
            df[col] = df[col].apply(json.dumps)

    df = df.convert_dtypes()

    for col in df.select_dtypes(include=np.number).columns:
        df[col] = df[col].fillna(0)

    return df.fillna("")


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

    # Filter to only players who have played this season
    players = [p for p in all_players if p.get("minutes", 0) > 0]
    logger.info("Retrieved %s players (%s with minutes > 0)", len(all_players), len(players))

    df = pd.DataFrame(players)

    # Add team_str column by mapping team id to team name
    df["team_str"] = df["team"].map(team_id_to_name)

    df = clean_dataframe(df)
    df = reorder_columns(df, [PRIMARY_KEY])

    columns = build_table_columns_from_df(df, PRIMARY_KEY)
    db.create_table(schema, TABLE_NAME, columns)

    insert_dataframe_rows(db, schema, TABLE_NAME, df, PRIMARY_KEY)

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
