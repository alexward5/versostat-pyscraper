import pandas as pd


def sanitize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sanitize DataFrame column names for database compatibility.

    Replaces special characters with descriptive text:
    - '+' becomes '_plus_'
    - '-' becomes '_minus_'
    - Other special characters become '_'
    """
    sanitized_columns: list[str] = []

    for col in df.columns:
        col_str = str(col)
        col_str = col_str.replace("+", "_plus_")
        col_str = col_str.replace("-", "_minus_")

        # Replace any other remaining special characters with underscore (keep only alphanumeric and underscore)
        sanitized = "".join(c if c.isalnum() or c == "_" else "_" for c in col_str)

        # Remove duplicate underscores
        while "__" in sanitized:
            sanitized = sanitized.replace("__", "_")

        # Remove leading/trailing underscores
        sanitized = sanitized.strip("_")

        sanitized_columns.append(sanitized)

    df.columns = sanitized_columns
    return df
