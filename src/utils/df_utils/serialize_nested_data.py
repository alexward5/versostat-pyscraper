import json

import pandas as pd


def serialize_nested_data(df: pd.DataFrame) -> pd.DataFrame:
    """Serialize nested dict/list columns to JSON strings for database storage."""
    for col in df.columns:
        non_null = df[col].dropna()
        if len(non_null) > 0 and isinstance(non_null.iloc[0], (dict, list)):
            df[col] = df[col].apply(json.dumps)
    return df
