import pandas as pd

from ..logger import setup_logger

logger = setup_logger(__name__)


def inspect_df(df: pd.DataFrame) -> None:
    """Print a comprehensive summary of a DataFrame including shape, columns, dtypes, and sample rows"""
    logger.info_with_newline("DataFrame shape: %s", df.shape)
    logger.info("Columns (%s): %s", len(df.columns), list(df.columns))
    logger.info_with_newline("Data types:\n%s", df.dtypes)
    logger.info_with_newline("First 5 rows:\n%s", df.head())
    logger.info_with_newline("Last 5 rows:\n%s", df.tail())
