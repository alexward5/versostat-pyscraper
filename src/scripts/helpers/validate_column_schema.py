import pandas as pd


def validate_column_schema(
    df: pd.DataFrame, reference_columns: list[str], context: str
) -> None:
    """Raise an error if the dataframe doesn't match the expected column count."""
    if len(df.columns) != len(reference_columns):
        raise ValueError(
            f"Column count mismatch for {context}: expected {len(reference_columns)}, "
            + f"got {len(df.columns)}. Columns: {list(df.columns)}"
        )
