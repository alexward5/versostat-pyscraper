import pandas as pd

from src.utils.util import map_pandas_dtype_to_postgres


def build_table_columns_from_df(df: pd.DataFrame, primary_key: str) -> list[str]:
    """Build PostgreSQL column definitions from a DataFrame schema."""
    columns: list[str] = []

    for col_name in df.columns:
        col_name_str: str = str(col_name)
        dtype = df[col_name_str].dtype
        pg_type = map_pandas_dtype_to_postgres(dtype)

        col_def = f"{col_name_str} {pg_type} NOT NULL"

        if col_name_str == primary_key:
            col_def += " PRIMARY KEY"

        columns.append(col_def)

    return columns
