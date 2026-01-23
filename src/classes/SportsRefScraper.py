import os
import time
import warnings
from io import StringIO

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup, Tag
from dotenv import load_dotenv
from zenrows import ZenRowsClient  # type: ignore

from ..utils.logger import setup_logger

load_dotenv(".env.local")

logger = setup_logger(__name__)


class SportsRefScraper:
    RATE_LIMIT_SECONDS = 8

    def __init__(self):
        api_key = os.getenv("ZENROWS_API_KEY")
        if not api_key:
            raise ValueError("ZENROWS_API_KEY not found in environment variables")
        self._client = ZenRowsClient(api_key)
        self._zenrows_params = {
            "js_render": "true",
            "antibot": "true",
            "premium_proxy": "true",
            "proxy_country": "us",
        }
        self._last_request_time: float = 0

    def scrape_table(self, url: str, table_index: int = 0, retries: int = 3) -> pd.DataFrame:
        for attempt in range(retries):
            try:
                html = self._fetch_html(url)
                return self._parse_table(html, table_index)
            except (ValueError, IndexError) as e:
                logger.warning("Scraping failed on attempt %s/%s: %s", attempt + 1, retries, e)
                if attempt < retries - 1:
                    sleep_time = 30 * (2**attempt)
                    logger.info("Retrying in %s seconds...", sleep_time)
                    time.sleep(sleep_time)
                else:
                    logger.error("Failed to scrape table after %s attempts", retries)
                    raise

        raise ValueError("Failed to scrape table after all retries")

    def _fetch_html(self, url: str) -> str:
        self._wait_for_rate_limit()
        response = self._client.get(url, params=self._zenrows_params, timeout=60)  # type: ignore
        self._last_request_time = time.time()

        if response.status_code == 200:
            return response.text

        raise ValueError(f"Failed to fetch HTML: status={response.status_code}")

    def _wait_for_rate_limit(self) -> None:
        elapsed = time.time() - self._last_request_time
        if elapsed < self.RATE_LIMIT_SECONDS:
            wait_time = self.RATE_LIMIT_SECONDS - elapsed
            logger.info("Rate limit: waiting %.1fs before next request...", wait_time)
            time.sleep(wait_time)

    def _parse_table(self, html: str, table_index: int) -> pd.DataFrame:
        soup = BeautifulSoup(html, "lxml")
        tables = soup.select("table.stats_table")  # type: ignore

        if not tables:
            raise ValueError("No stats tables found on page")
        if table_index >= len(tables):
            raise IndexError(f"Table index {table_index} out of range (found {len(tables)} tables)")

        target_table = tables[table_index]
        df = pd.read_html(StringIO(str(target_table)))[0]

        # Flatten multi-level column headers
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [
                "_".join(
                    str(level)
                    for level in col
                    if str(level).strip() and not str(level).startswith("Unnamed")
                )
                for col in df.columns
            ]

        # Store original columns for header row detection, then convert to snake_case
        original_columns = {str(col) for col in df.columns}
        df.columns = [self._to_snake_case(col) for col in df.columns]

        # Add URL columns before filtering to ensure index alignment
        self._add_url_columns(df, target_table)

        # Filter non-data rows and clean values
        df = self._filter_non_data_rows(df, original_columns).reset_index(drop=True)
        return self._clean_df_values(df)

    def _add_url_columns(self, df: pd.DataFrame, table_soup: Tag) -> None:
        tbody = table_soup.find("tbody")
        if not tbody or not isinstance(tbody, Tag):
            return

        rows = tbody.find_all("tr", recursive=False)

        # Handle mismatch by padding with empty URL rows if DataFrame has more rows
        # This happens when pandas includes rows that will be filtered later
        if len(rows) > len(df):
            logger.warning(
                "More HTML rows (%s) than DataFrame rows (%s). Skipping URL extraction.",
                len(rows),
                len(df),
            )
            return

        # Extract URLs into grid, handling colspan
        urls_grid: list[list[str]] = []
        for row in rows:
            row_urls: list[str] = []
            for cell in row.find_all(["td", "th"]):
                link = cell.find("a", href=True)
                url = link["href"] if link else ""
                row_urls.extend([url] * int(cell.get("colspan", 1)))
            urls_grid.append(row_urls)

        # Pad with empty rows if DataFrame has more rows (for rows that will be filtered later)
        while len(urls_grid) < len(df):
            urls_grid.append([""] * len(urls_grid[0]) if urls_grid else [])

        # Insert URL columns for columns that have at least one URL
        original_cols = list(df.columns)
        offset = 0
        for col_idx, col_name in enumerate(original_cols):
            col_urls = [row[col_idx] if col_idx < len(row) else "" for row in urls_grid]
            if any(col_urls):
                df.insert(col_idx + 1 + offset, f"{col_name}_url", col_urls)
                offset += 1

    def _filter_non_data_rows(self, df: pd.DataFrame, original_columns: set[str]) -> pd.DataFrame:
        # Filter out rows where ANY cell contains a header value or "total"
        is_header_row = pd.Series([False] * len(df), index=df.index)

        # Only check non-URL columns for header values
        non_url_columns = [col for col in df.columns if not col.endswith("_url")]

        # Check for "total" in any non-URL column (case-insensitive)
        for col in non_url_columns:
            col_vals = df[col].fillna("").astype(str).str.strip()
            is_header_row |= col_vals.str.lower().str.contains("total", regex=False)

        # Filter duplicate header rows, which sometimes appear in the middle of tables
        if len(non_url_columns) > 0:
            first_col = non_url_columns[0]
            first_col_vals = df[first_col].fillna("").astype(str).str.strip()
            # If first column contains any original column name, it's likely a header row
            is_header_row |= first_col_vals.isin(original_columns)

        return df[~is_header_row]

    def _clean_df_values(self, df: pd.DataFrame) -> pd.DataFrame:
        # Convert columns to numeric where appropriate
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=FutureWarning)
            for col in df.columns:
                # Keep completely empty columns as string type
                if df[col].isna().all():
                    df[col] = ""
                    continue

                numeric_col = pd.to_numeric(df[col], errors="coerce")
                # Only convert if all non-null values successfully converted to numeric
                if df[col].notna().sum() == numeric_col.notna().sum():
                    df[col] = numeric_col

        df = df.convert_dtypes()

        # Fill missing values: numeric with 0, others with empty string
        for col in df.select_dtypes(include=np.number).columns:
            df[col] = df[col].fillna(0)
        return df.fillna("")

    def _to_snake_case(self, text: str) -> str:
        return text.strip().replace(" ", "_").replace("/", "_").lower()
