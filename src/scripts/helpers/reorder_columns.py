import pandas as pd


def reorder_columns(df: pd.DataFrame, first_columns: list[str]) -> pd.DataFrame:
    """Move specified columns to the front of the dataframe."""
    remaining = [col for col in df.columns if col not in first_columns]
    return df[first_columns + remaining]
