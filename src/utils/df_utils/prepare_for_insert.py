from typing import Any

import pandas as pd


def prepare_for_insert(df: pd.DataFrame, primary_key: str) -> pd.DataFrame:
    """Prepare DataFrame for database insertion by converting dtypes, filling NAs, and reordering columns."""
    # Store original numeric dtypes before convert_dtypes
    numeric_cols: dict[str, Any] = {}
    for col in df.columns:
        col_str = str(col)
        series = df[col_str]
        if pd.api.types.is_numeric_dtype(series):
            numeric_cols[col_str] = series.dtype

    df = df.convert_dtypes()

    # Restore numeric dtypes that may have been converted to string
    for col, original_dtype in numeric_cols.items():
        series = df[col]
        if pd.api.types.is_string_dtype(series):
            try:
                # Try to convert back to numeric
                converted = pd.to_numeric(series, errors="coerce")
                if "int" in str(original_dtype).lower():
                    df[col] = converted.astype("Int64")
                else:
                    df[col] = converted.astype("Float64")
            except (ValueError, TypeError):
                # If conversion fails, keep as string
                pass

    # Attempt to convert string columns that contain numeric values
    for col in df.columns:
        col_str = str(col)
        series = df[col_str]

        # Skip columns we already processed or that aren't strings
        if col_str in numeric_cols or not pd.api.types.is_string_dtype(series):
            continue

        # Try to convert string columns to numeric
        try:
            # Attempt numeric conversion
            converted = pd.to_numeric(series, errors="coerce")

            # Check if conversion was successful for most values
            # (at least 50% of non-null values should be convertible)
            non_null_count = series.notna().sum()
            if non_null_count > 0:
                converted_count = converted.notna().sum()
                if converted_count / non_null_count >= 0.5:
                    # Determine if it's integer or float. Use float if any value was
                    # written as a decimal string (e.g. "0.00") or has a fractional part.
                    non_null_converted = converted.dropna()
                    looks_like_decimal = series.astype(str).str.contains(r"\.", na=False).any()
                    if (
                        len(non_null_converted) > 0
                        and (non_null_converted % 1 == 0).all()
                        and not looks_like_decimal
                    ):
                        df[col_str] = converted.astype("Int64")
                    else:
                        df[col_str] = converted.astype("Float64")
        except (ValueError, TypeError):
            # If conversion fails, keep as string
            pass

    # Fill missing values based on dtype
    for col in df.columns:
        col_str = str(col)
        series = df[col_str]

        dtype_str = str(series.dtype)
        if dtype_str == "boolean":
            df[col_str] = series.fillna(False)
        elif any(t in dtype_str.lower() for t in ["int", "float"]):
            df[col_str] = series.fillna(0)
        else:
            df[col_str] = series.fillna("")

    # Ensure primary key is first column
    if primary_key in df.columns:
        cols = [str(primary_key)] + [str(c) for c in df.columns if str(c) != primary_key]
        df = df[cols]

    return df
