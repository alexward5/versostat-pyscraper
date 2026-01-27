import pandas as pd

from src.utils.pg_utils.map_pandas_dtype_to_postgres import map_pandas_dtype_to_postgres


def generate_column_definitions(df: pd.DataFrame, primary_key: str) -> list[str]:
    """Generate PostgreSQL column definition strings from a DataFrame schema.
    
    Returns a list of column definition strings like:
    ["id INTEGER NOT NULL PRIMARY KEY", "name TEXT NOT NULL"]
    """
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
