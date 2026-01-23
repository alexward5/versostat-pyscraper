from typing import Any, Callable

import pandas as pd


def transform_column(
    df: pd.DataFrame,
    column: str,
    transform_fn: Callable[[Any], Any],
) -> pd.DataFrame:
    """Apply a transformation function to every cell in a column"""
    df[column] = df[column].apply(transform_fn)

    return df
