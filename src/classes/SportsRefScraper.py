import os
from io import StringIO
from time import sleep

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
        self._zenrows_params = {
            "antibot": "true",
            "wait_for": "table.stats_table",
            "premium_proxy": "true",
            "proxy_country": "us",
        }

    def scrape_table(self, url: str, table_index: int = 0) -> pd.DataFrame:
        html = self._fetch_html(url)
        return self._parse_table(html, table_index)

    def _fetch_html(self, url: str, retries: int = 3) -> str:
        for attempt in range(retries):
            try:
                response = self._client.get(url, params=self._zenrows_params)

                if response.status_code != 200:
                    print(f"Failed with status {response.status_code}. Retrying...")
                elif "table" not in response.text:
                    print("Response missing expected content. Retrying...")
                else:
                    sleep(8)
                    return response.text

            except Exception as e:
                print(f"Error while retrieving page content: {e}")

            sleep_time = 10 * (attempt + 1)
            print(f"Retry {attempt + 1}/{retries}. Sleeping for {sleep_time} seconds...")
            sleep(sleep_time)

        raise ValueError("Failed to fetch HTML after all retries")

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
