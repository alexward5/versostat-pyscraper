import uuid

import pandas as pd


def add_id_column(
    df: pd.DataFrame,
    source_columns: list[str],
    id_column_name: str = "uuid",
) -> pd.DataFrame:
    """Add a deterministic UUID5-based ID column generated from one or more existing columns"""
    # Validate source columns exist
    missing_columns = [col for col in source_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Source columns not found in DataFrame: {', '.join(missing_columns)}")

    # Create concatenated string from source columns and generate UUID5
    def generate_id(row: pd.Series) -> str:
        values = [str(row[col]) for col in source_columns]
        concatenated = "".join(values)
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, concatenated))

    df[id_column_name] = df.apply(generate_id, axis=1)

    return df
