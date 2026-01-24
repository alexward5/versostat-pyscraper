import json
import os
import re
import time
from typing import Any, cast

import requests
from dotenv import load_dotenv

from ..utils.logger import setup_logger

load_dotenv(".env.local")

logger = setup_logger(__name__)


class SportsmonksAPI:
    """Client for Sportmonks Football API v3."""

    BASE_URL = "https://api.sportmonks.com/v3/football"
    CORE_URL = "https://api.sportmonks.com/v3/core"
    RATE_LIMIT_SECONDS = 1.0
    PREMIER_LEAGUE_ID = 8  # Premier League league ID

    def __init__(self) -> None:
        api_key = os.getenv("SPORTMONKS_API_KEY")
        if not api_key:
            raise ValueError("SPORTMONKS_API_KEY not found in environment variables")

        self._api_key = api_key
        self._last_request_time: float = 0
        self._types_cache: dict[int, str] = {}
        self._season_id: int | None = None

        # Fetch and cache types on initialization
        self._fetch_types()
        # Detect current season
        self._season_id = self._get_current_season_id()
        logger.info("Initialized SportsmonksAPI with season ID: %s", self._season_id)

    @property
    def current_season_id(self) -> int:
        """Return the current Premier League season ID."""
        if self._season_id is None:
            self._season_id = self._get_current_season_id()
        return self._season_id

    def _get_current_season_id(self) -> int:
        """Fetch the current Premier League 2025/2026 season ID."""
        response = self._make_request(
            f"/leagues/{self.PREMIER_LEAGUE_ID}",
            params={"include": "currentSeason"},
        )
        data = response.get("data", {})
        # API returns "currentseason" (no underscore, lowercase)
        current_season = data.get("currentseason")
        if not current_season:
            raise ValueError("Could not determine current Premier League season")

        season_id = current_season.get("id")
        season_name = current_season.get("name", "Unknown")
        logger.info("Detected current season: %s (ID: %s)", season_name, season_id)
        return int(season_id)

    def _fetch_types(self) -> None:
        """Fetch all types from API and cache the mapping of type_id -> name."""
        logger.info("Fetching stat types from API...")
        all_types: list[dict[str, Any]] = []
        page = 1

        while True:
            response = self._make_request(
                "/types",
                params={"per_page": 50, "page": page},
                use_core_url=True,
            )
            data = response.get("data", [])
            all_types.extend(data)

            pagination = response.get("pagination", {})
            if not pagination.get("has_more", False):
                break
            page += 1

        for type_info in all_types:
            type_id = type_info.get("id")
            type_name = type_info.get("name", "")
            if type_id is not None and type_name:
                self._types_cache[int(type_id)] = self._to_snake_case(type_name)

        logger.info("Cached %s stat types", len(self._types_cache))

    def get_type_name(self, type_id: int) -> str:
        """Get the human-readable name for a type ID."""
        return self._types_cache.get(type_id, f"type_{type_id}")

    def _wait_for_rate_limit(self) -> None:
        """Wait if needed to respect rate limit."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.RATE_LIMIT_SECONDS:
            wait_time = self.RATE_LIMIT_SECONDS - elapsed
            time.sleep(wait_time)

    def _make_request(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        use_core_url: bool = False,
    ) -> dict[str, Any]:
        """Make an API request with rate limiting."""
        self._wait_for_rate_limit()

        base = self.CORE_URL if use_core_url else self.BASE_URL
        url = f"{base}{endpoint}"
        request_params = params.copy() if params else {}
        request_params["api_token"] = self._api_key

        response = requests.get(url, params=request_params, timeout=30)
        self._last_request_time = time.time()

        if response.status_code != 200:
            raise ValueError(
                f"API request failed: {response.status_code} - {response.text}"
            )

        return dict(response.json())

    def _make_paginated_request(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        per_page: int = 50,
    ) -> list[dict[str, Any]]:
        """Make paginated API requests and return all data."""
        all_data: list[dict[str, Any]] = []
        page = 1
        request_params = params.copy() if params else {}
        request_params["per_page"] = per_page

        while True:
            request_params["page"] = page
            response = self._make_request(endpoint, request_params)

            data: list[dict[str, Any]] | dict[str, Any] = response.get("data", [])
            if isinstance(data, list):
                all_data.extend(data)
            else:
                all_data.append(data)

            pagination = response.get("pagination", {})
            if not pagination.get("has_more", False):
                break
            page += 1

        return all_data

    def _to_snake_case(self, text: str) -> str:
        """Convert text to snake_case with database-compatible sanitization."""
        text = str(text).strip()

        # Handle camelCase by inserting underscores before uppercase letters
        text = re.sub(r"([a-z])([A-Z])", r"\1_\2", text)

        # Handle special characters
        text = text.replace("+", "_plus_")
        text = text.replace("-", "_")
        text = text.replace(" ", "_")
        text = text.replace("/", "_")

        # Replace any remaining special characters with underscore
        text = "".join(c if c.isalnum() or c == "_" else "_" for c in text)

        # Remove duplicate underscores
        while "__" in text:
            text = text.replace("__", "_")

        # Remove leading/trailing underscores and convert to lowercase
        return text.strip("_").lower()

    def get_teams_by_season(self, season_id: int | None = None) -> list[dict[str, Any]]:
        """Get all teams in a season (defaults to current season)."""
        sid = season_id or self.current_season_id
        logger.info("Fetching teams for season ID: %s", sid)

        teams = self._make_paginated_request(
            f"/seasons/{sid}",
            params={"include": "teams"},
        )

        # The response wraps teams in the season data
        if teams and len(teams) > 0:
            season_data = teams[0]
            team_list: list[dict[str, Any]] = season_data.get("teams", [])
            logger.info("Found %s teams", len(team_list))
            return team_list

        return []

    def get_players_by_team(
        self,
        team_id: int,
        season_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """Get all players for a team in a season."""
        sid = season_id or self.current_season_id
        logger.info("Fetching players for team ID: %s, season ID: %s", team_id, sid)

        response = self._make_request(
            f"/teams/{team_id}",
            params={
                "include": "players",
                "filters": f"playerSeasonId:{sid}",
            },
        )

        data = response.get("data", {})
        players = data.get("players", [])
        logger.info("Found %s players for team %s", len(players), team_id)
        return list(players)

    def get_player_season_statistics(
        self,
        player_id: int,
        season_id: int | None = None,
    ) -> dict[str, Any]:
        """Get player overall stats for a season with resolved stat names."""
        sid = season_id or self.current_season_id

        response = self._make_request(
            f"/players/{player_id}",
            params={
                "include": "statistics.details",
                "filters": f"statisticSeasons:{sid}",
            },
        )

        data = response.get("data", {})
        return self._flatten_player_data(data)

    def get_team_season_statistics(
        self,
        team_id: int,
        season_id: int | None = None,
    ) -> dict[str, Any]:
        """Get team overall stats for a season with resolved stat names."""
        sid = season_id or self.current_season_id

        response = self._make_request(
            f"/teams/{team_id}",
            params={
                "include": "statistics.details",
                "filters": f"statisticSeasons:{sid}",
            },
        )

        data = response.get("data", {})
        return self._flatten_team_data(data)

    def _flatten_player_data(self, player_data: dict[str, Any]) -> dict[str, Any]:
        """Flatten player data including statistics into a single dict."""
        flat: dict[str, Any] = {
            "player_id": player_data.get("id"),
            "player_name": player_data.get("display_name", player_data.get("name", "")),
            "common_name": player_data.get("common_name", ""),
            "first_name": player_data.get("firstname", ""),
            "last_name": player_data.get("lastname", ""),
            "position_id": player_data.get("position_id"),
            "nationality_id": player_data.get("nationality_id"),
            "date_of_birth": player_data.get("date_of_birth", ""),
            "height": player_data.get("height"),
            "weight": player_data.get("weight"),
        }

        # Flatten statistics
        statistics = player_data.get("statistics", [])
        flat.update(self.flatten_statistics(statistics))

        return flat

    def _flatten_team_data(self, team_data: dict[str, Any]) -> dict[str, Any]:
        """Flatten team data including statistics into a single dict."""
        flat: dict[str, Any] = {
            "team_id": team_data.get("id"),
            "team_name": team_data.get("name", ""),
            "short_code": team_data.get("short_code", ""),
            "founded": team_data.get("founded"),
            "venue_id": team_data.get("venue_id"),
        }

        # Flatten statistics
        statistics = team_data.get("statistics", [])
        flat.update(self.flatten_statistics(statistics))

        return flat

    def flatten_statistics(self, statistics: list[dict[str, Any]]) -> dict[str, Any]:
        """Flatten statistics array with type resolution."""
        flat: dict[str, Any] = {}

        for stat_group in statistics:
            details = stat_group.get("details", [])
            for detail in details:
                type_id = detail.get("type_id")
                value = detail.get("value")

                if type_id is None:
                    continue

                stat_name = self.get_type_name(type_id)

                # Handle value types
                if isinstance(value, dict):
                    # Flatten nested value objects like {"total": 10, "average": 2.5}
                    value_dict = cast(dict[str, Any], value)
                    for sub_key, sub_val in value_dict.items():
                        col_name = f"{stat_name}_{self._to_snake_case(sub_key)}"
                        # Convert complex types to JSON strings
                        flat[col_name] = self._sanitize_value(sub_val)
                elif isinstance(value, list):
                    # Convert lists to JSON strings
                    flat[stat_name] = json.dumps(value)
                else:
                    flat[stat_name] = value

        return flat

    def _sanitize_value(self, value: Any) -> Any:
        """Convert complex types (dict, list) to JSON strings for database storage."""
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        return value

    def get_fixtures_by_season(
        self,
        season_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """Get all fixtures for a season (may require higher plan)."""
        sid = season_id or self.current_season_id
        logger.info("Fetching fixtures for season ID: %s", sid)

        try:
            fixtures = self._make_paginated_request(
                f"/fixtures",
                params={"filters": f"fixtureSeasons:{sid}"},
            )
            logger.info("Found %s fixtures", len(fixtures))
            return fixtures
        except ValueError as e:
            logger.warning("Fixtures endpoint may require plan upgrade: %s", e)
            return []

    def get_fixture_statistics(
        self,
        fixture_id: int,
    ) -> dict[str, Any]:
        """Get statistics for a specific fixture (may require higher plan)."""
        try:
            response = self._make_request(
                f"/fixtures/{fixture_id}",
                params={"include": "statistics.details"},
            )
            return dict(response.get("data", {}))
        except ValueError as e:
            logger.warning("Fixture statistics may require plan upgrade: %s", e)
            return {}
