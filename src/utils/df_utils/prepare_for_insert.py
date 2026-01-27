import pandas as pd


def prepare_for_insert(df: pd.DataFrame, primary_key: str) -> pd.DataFrame:
    """Prepare DataFrame for database insertion by converting dtypes, filling NAs, and reordering columns."""
    df = df.convert_dtypes()

    # Fill missing values based on dtype
    for col in df.columns:
        dtype_str = str(df[col].dtype)
        if dtype_str == "boolean":
            df[col] = df[col].fillna(False)
        elif any(t in dtype_str.lower() for t in ["int", "float"]):
            df[col] = df[col].fillna(0)
        else:
            df[col] = df[col].fillna("")

    # Ensure primary key is first column
    if primary_key in df.columns:
        cols = [primary_key] + [c for c in df.columns if c != primary_key]
        df = df[cols]

    return df
