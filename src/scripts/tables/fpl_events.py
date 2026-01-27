import argparse

import pandas as pd

from ...classes.FPL_API import FPL_API
from ...classes.PostgresClient import PostgresClient
from ...utils.df_utils import prepare_for_insert, serialize_nested_data
from ...utils.df_utils.build_table_columns import build_table_columns_from_df
from ...utils.logger import setup_logger
from ...utils import insert_dataframe_rows

logger = setup_logger(__name__)

TABLE_NAME = "fpl_events"
PRIMARY_KEY = "id"


def main(schema: str) -> None:
    """Fetch FPL events (gameweeks) data and load into database."""
    db = PostgresClient()
    db.create_schema(schema)

    api = FPL_API()
    logger.info("Fetching events from FPL API...")
    events = api.get_events()
    logger.info("Retrieved %s events", len(events))

    df = pd.DataFrame(events)
    df = serialize_nested_data(df)
    df = prepare_for_insert(df, PRIMARY_KEY)

    columns = build_table_columns_from_df(df, PRIMARY_KEY)
    db.create_table(schema, TABLE_NAME, columns)

    insert_dataframe_rows(db, schema, TABLE_NAME, df, PRIMARY_KEY)

    db.close()

    logger.info_with_newline("=" * 60)
    logger.info("Completed: %s events inserted", len(df))
    logger.info("Table: %s.%s", schema, TABLE_NAME)
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch FPL events data")
    parser.add_argument("--schema", type=str, required=True, help="Database schema name to use")
    args = parser.parse_args()
    main(args.schema)
