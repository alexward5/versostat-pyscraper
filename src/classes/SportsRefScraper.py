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
    def __init__(self):
        api_key = os.getenv("ZENROWS_API_KEY")
        if not api_key:
            raise ValueError("ZENROWS_API_KEY not found in environment variables")
        self._client = ZenRowsClient(api_key)
        self._zenrows_params = {
            "antibot": "true",
            "wait_for": "table.stats_table",
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
                response = self._client.get(url, params=self._zenrows_params)  # type: ignore
                self._last_request_time = time.time()

                if response.status_code != 200:
                    print(f"Failed with status {response.status_code}. Retrying...")
                elif "table" not in response.text:
                    print("Response missing expected content. Retrying...")
                else:
                    return response.text

            except Exception as e:
                print(f"Error while retrieving page content: {e}")

            sleep_time = 10 * (attempt + 1)
            print(f"Retry {attempt + 1}/{retries}. Sleeping for {sleep_time} seconds...")
            time.sleep(sleep_time)

        raise ValueError("Failed to fetch HTML after all retries")

    def _wait_for_rate_limit(self) -> None:
        RATE_LIMIT_SECONDS = 8

        elapsed = time.time() - self._last_request_time

        if elapsed < RATE_LIMIT_SECONDS:
            wait_time = RATE_LIMIT_SECONDS - elapsed
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

        urls_data = self._extract_table_urls(target_table)

        df = pd.read_html(StringIO(str(target_table)))[0]

        # Handle multi-level column headers by flattening and combining headers
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [
                "_".join(
                    str(level)
                    for level in col
                    if str(level).strip() and not str(level).startswith("Unnamed")
                )
                for col in df.columns
            ]

        # Store original column names before converting to snake_case (for header row detection)
        original_columns = set[str](str(col) for col in df.columns)

        df.columns = [self._to_snake_case(col) for col in df.columns]

        # Add temporary column to track original row indices before filtering
        df["_original_row_idx"] = range(len(df))

        df = self._filter_non_data_rows(df, original_columns)

        # Add URL columns after filtering rows (before reset_index)
        df = self._add_url_columns(df, urls_data)

        # Remove temporary tracking column and reset index
        df = df.drop(columns=["_original_row_idx"])
        df = df.reset_index(drop=True)

        df = self._set_df_dtypes(df)
        df = self._fill_df_missing_values(df)

        return df

    def _extract_table_urls(self, table: Tag) -> dict[int, dict[int, str]]:
        urls_data: dict[int, dict[int, str]] = {}

        # Find all rows in tbody (data rows)
        tbody = table.find("tbody")
        if not tbody or not isinstance(tbody, Tag):
            return urls_data

        rows = tbody.find_all("tr")

        for row_idx, row in enumerate(rows):
            cells = row.find_all(["td", "th"])

            for col_idx, cell in enumerate(cells):
                # Find first <a> tag with href in this cell
                link = cell.find("a", href=True)
                if link:
                    if row_idx not in urls_data:
                        urls_data[row_idx] = {}
                    urls_data[row_idx][col_idx] = link["href"]

        return urls_data

    def _add_url_columns(
        self, df: pd.DataFrame, urls_data: dict[int, dict[int, str]]
    ) -> pd.DataFrame:
        if not urls_data:
            return df

        # Determine which columns have URLs
        columns_with_urls: set[int] = set[int]()
        for row_urls in urls_data.values():
            columns_with_urls.update(row_urls.keys())

        if not columns_with_urls:
            return df

        # Exclude the temporary tracking column from URL processing
        columns_with_urls = {idx for idx in columns_with_urls if idx < len(df.columns) - 1}

        # Add URL columns
        for col_idx in sorted(columns_with_urls):
            col_name = df.columns[col_idx]
            url_col_name = f"{col_name}_url"

            # Create URL column with data aligned to filtered rows using _original_row_idx
            url_values: list[str] = []
            for _, row in df.iterrows():
                original_row_idx = int(row["_original_row_idx"])
                if original_row_idx in urls_data:
                    url = urls_data[original_row_idx].get(col_idx, "")
                else:
                    url = ""
                url_values.append(url)

            # Insert URL column right after the data column
            col_position = df.columns.get_loc(col_name)
            insert_position = (
                int(col_position) + 1 if isinstance(col_position, (int, np.integer)) else 0
            )
            df.insert(insert_position, url_col_name, url_values)

        return df

    def _filter_non_data_rows(self, df: pd.DataFrame, original_columns: set[str]) -> pd.DataFrame:
        first_col = df.columns[0]

        def is_data_row(row: pd.Series) -> bool:
            first_val = str(row[first_col]) if pd.notna(row[first_col]) else ""

            # Filter rows containing "total" in the first column (case-insensitive)
            if "total" in first_val.lower():
                return False

            # Filter repeated header rows (first column value matches a column name)
            if first_val in original_columns:
                return False

            return True

        return df[df.apply(is_data_row, axis=1)]

    def _set_df_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        # Infer and set appropriate data types for each column
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=FutureWarning)
            # Try to convert each column to numeric, keep original if conversion fails
            for col in df.columns:
                numeric_col = pd.to_numeric(df[col], errors="coerce")
                # Only use numeric version if most values were successfully converted
                if numeric_col.notna().sum() >= len(df) * 0.5:
                    df[col] = numeric_col
        df = df.convert_dtypes()

        return df

    def _fill_df_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        # Fill missing values: numeric columns with 0, others with empty strings
        for numeric_column in df.select_dtypes(include=np.number).columns:
            df[numeric_column] = df[numeric_column].fillna(0)
        df = df.fillna("")

        return df

    def _to_snake_case(self, text: str) -> str:
        return text.strip().replace(" ", "_").replace("/", "_").lower()

    def inspect_df(self, df: pd.DataFrame) -> None:
        print(f"\nDataFrame shape: {df.shape}")
        print(f"Columns ({len(df.columns)}): {list(df.columns)}")
        print(f"\nData types:\n{df.dtypes}")
        print(f"\nFirst 5 rows:\n{df.head()}")
        print(f"\nLast 5 rows:\n{df.tail()}")
