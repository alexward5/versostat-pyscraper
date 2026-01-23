import os
from io import StringIO

import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from zenrows import ZenRowsClient

load_dotenv(".env.local")


class SportsRefScraper:
    def __init__(self):
        api_key = os.getenv("ZENROWS_API_KEY")
        if not api_key:
            raise ValueError("ZENROWS_API_KEY not found in environment variables")
        self._client = ZenRowsClient(api_key)

    def scrape_table(self, url: str, table_index: int = 0) -> pd.DataFrame:
        html = self._fetch_html(url)
        return self._parse_table(html, table_index)

    def _fetch_html(self, url: str) -> str:
        response = self._client.get(url, params={"js_render": "true"})
        response.raise_for_status()
        return response.text

    def _parse_table(self, html: str, table_index: int) -> pd.DataFrame:
        soup = BeautifulSoup(html, "lxml")
        tables = soup.select("table.stats_table")

        if not tables:
            raise ValueError("No stats tables found on page")

        if table_index >= len(tables):
            raise IndexError(f"Table index {table_index} out of range (found {len(tables)} tables)")

        target_table = tables[table_index]

        # Sports Reference tables often have nested/commented HTML - extract clean table
        df = pd.read_html(StringIO(str(target_table)))[0]

        # Handle multi-level column headers by flattening
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[-1] if col[-1] != col[0] else col[0] for col in df.columns]

        # Drop any rows that are repeated headers (common in SR tables)
        if "Rk" in df.columns:
            df = df[df["Rk"] != "Rk"]

        # Reset index after filtering
        df = df.reset_index(drop=True)

        return df
