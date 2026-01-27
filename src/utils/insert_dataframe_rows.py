from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from ..classes.PostgresClient import PostgresClient


def insert_dataframe_rows(
    db: "PostgresClient", schema: str, table_name: str, df: pd.DataFrame, primary_key: str
) -> None:
    """Insert all rows from a dataframe into the database."""
    columns = list[str](df.columns)
    for _, row in df.iterrows():
        db.insert_row(
            schema=schema,
            table_name=table_name,
            column_names=columns,
            row_values=list[Any](row),
            update_on=primary_key,
        )
