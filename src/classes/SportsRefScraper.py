import os
import time
import warnings
from io import StringIO

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup, Tag
from dotenv import load_dotenv
from zenrows import ZenRowsClient  # type: ignore

load_dotenv(".env.local")


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

    def scrape_table(self, url: str, table_index: int = 0) -> pd.DataFrame:
        html = self._fetch_html(url)
        return self._parse_table(html, table_index)

    def _fetch_html(self, url: str, retries: int = 3) -> str:
        for attempt in range(retries):
            try:
                self._wait_for_rate_limit()
                response = self._client.get(url, params=self._zenrows_params, timeout=60)  # type: ignore
                self._last_request_time = time.time()

                if response.status_code == 200 and "table" in response.text:
                    return response.text

                print(
                    f"Failed: status={response.status_code}, has_table={'table' in response.text}"
                )
            except Exception as e:
                print(f"Error while retrieving page content: {e}")

            sleep_time = 10 * (attempt + 1)
            print(f"Retry {attempt + 1}/{retries}. Sleeping for {sleep_time} seconds...")
            time.sleep(sleep_time)

        raise ValueError("Failed to fetch HTML after all retries")

    def _wait_for_rate_limit(self) -> None:
        elapsed = time.time() - self._last_request_time
        if elapsed < self.RATE_LIMIT_SECONDS:
            wait_time = self.RATE_LIMIT_SECONDS - elapsed
            print(f"Rate limit: waiting {wait_time:.1f}s before next request...")
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
            print(
                f"Warning: More HTML rows ({len(rows)}) than DataFrame rows ({len(df)}). Skipping URL extraction."
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
        first_col = df.columns[0]
        first_vals = df[first_col].fillna("").astype(str)
        mask = ~first_vals.str.lower().str.contains("total") & ~first_vals.isin(original_columns)
        return df[mask]

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
