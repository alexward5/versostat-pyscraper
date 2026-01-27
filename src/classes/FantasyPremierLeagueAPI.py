import time
from typing import Any, cast

import requests

from ..utils.logger import setup_logger

logger = setup_logger(__name__)


class FantasyPremierLeagueAPI:
    """Client for the Fantasy Premier League API."""

    BASE_URL = "https://fantasy.premierleague.com/api"
    RATE_LIMIT_SECONDS = 3

    def __init__(self) -> None:
        self._last_request_time: float = 0
        self._session = requests.Session()
        self._session.headers.update(
            {"User-Agent": "Mozilla/5.0 (compatible; FPL-Scraper/1.0)"}
        )

    def _wait_for_rate_limit(self) -> None:
        elapsed = time.time() - self._last_request_time
        if elapsed < self.RATE_LIMIT_SECONDS:
            wait_time = self.RATE_LIMIT_SECONDS - elapsed
            time.sleep(wait_time)

    def _fetch_json(self, endpoint: str, retries: int = 3) -> dict[str, Any]:
        """Fetch JSON from the FPL API with retries."""
        url = f"{self.BASE_URL}/{endpoint}"

        for attempt in range(retries):
            try:
                self._wait_for_rate_limit()
                response = self._session.get(url, timeout=30)
                self._last_request_time = time.time()

                if response.status_code == 200:
                    return cast(dict[str, Any], response.json())

                raise ValueError(f"API request failed: status={response.status_code}")

            except (requests.RequestException, ValueError) as e:
                logger.warning("Request failed on attempt %s/%s: %s", attempt + 1, retries, e)
                if attempt < retries - 1:
                    sleep_time = 10 * (2**attempt)
                    logger.info("Retrying in %s seconds...", sleep_time)
                    time.sleep(sleep_time)
                else:
                    logger.error("Failed to fetch %s after %s attempts", endpoint, retries)
                    raise

        raise ValueError(f"Failed to fetch {endpoint} after all retries")

    def get_bootstrap_static(self) -> dict[str, Any]:
        """Fetch the main bootstrap-static endpoint containing players, events, teams."""
        return self._fetch_json("bootstrap-static/")

    def get_players(self) -> list[dict[str, Any]]:
        """Get all players (elements) from bootstrap-static."""
        data = self.get_bootstrap_static()
        return cast(list[dict[str, Any]], data["elements"])

    def get_events(self) -> list[dict[str, Any]]:
        """Get all gameweek events from bootstrap-static."""
        data = self.get_bootstrap_static()
        return cast(list[dict[str, Any]], data["events"])

    def get_teams(self) -> list[dict[str, Any]]:
        """Get all teams from bootstrap-static."""
        data = self.get_bootstrap_static()
        return cast(list[dict[str, Any]], data["teams"])

    def get_player_summary(self, player_id: int) -> dict[str, Any]:
        """Fetch detailed player summary including history by gameweek."""
        return self._fetch_json(f"element-summary/{player_id}/")
