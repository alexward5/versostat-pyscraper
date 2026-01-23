import pandas as pd


def inspect_df(df: pd.DataFrame) -> None:
    """Print a comprehensive summary of a DataFrame including shape, columns, dtypes, and sample rows."""
    print(f"\nDataFrame shape: {df.shape}")
    print(f"Columns ({len(df.columns)}): {list(df.columns)}")
    print(f"\nData types:\n{df.dtypes}")
    print(f"\nFirst 5 rows:\n{df.head()}")
    print(f"\nLast 5 rows:\n{df.tail()}")

