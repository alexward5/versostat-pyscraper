import os
import re
import time
from typing import Any, cast

import requests
from dotenv import load_dotenv

from ..utils.logger import setup_logger

load_dotenv(".env.local")

logger = setup_logger(__name__)


class SportmonksAPI:
    """Client for Sportmonks Football API v3."""

    BASE_URL = "https://api.sportmonks.com/v3/football"
    CORE_URL = "https://api.sportmonks.com/v3/core"
    RATE_LIMIT_SECONDS = 1.0
    PREMIER_LEAGUE_ID = 8

    def __init__(self) -> None:
        api_key = os.getenv("SPORTMONKS_API_KEY")
        if not api_key:
            raise ValueError("SPORTMONKS_API_KEY not found in environment variables")

        self._api_key = api_key
        self._last_request_time: float = 0
        self._types_cache: dict[int, str] = {}
        self._season_id: int | None = None

        self._fetch_types()
        self._season_id = self._get_current_season_id()
        logger.info("Initialized SportmonksAPI with season ID: %s", self._season_id)

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
            raise ValueError(f"API request failed: {response.status_code} - {response.text}")

        return cast(dict[str, Any], response.json())

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

    def get_teams(self) -> list[dict[str, Any]]:
        """Get all teams in the current season."""
        logger.info("Fetching teams for season ID: %s", self.current_season_id)

        teams = self._make_paginated_request(
            f"/seasons/{self.current_season_id}",
            params={"include": "teams"},
        )

        # Response wraps teams in the season data
        if teams and len(teams) > 0:
            season_data = teams[0]
            team_list: list[dict[str, Any]] = season_data.get("teams", [])
            logger.info("Found %s teams", len(team_list))
            return team_list

        return []

    def get_players_by_team(self, team_id: int) -> list[dict[str, Any]]:
        """Get all players for a team in the current season."""
        logger.info("Fetching players for team ID: %s", team_id)

        response = self._make_request(
            f"/teams/{team_id}",
            params={
                "include": "players",
                "filters": f"playerSeasonId:{self.current_season_id}",
            },
        )

        data = response.get("data", {})
        players: list[dict[str, Any]] = data.get("players", [])
        logger.info("Found %s players for team %s", len(players), team_id)
        return players

    def get_player_statistics(self, player_id: int) -> dict[str, Any]:
        """Get player overall stats for the current season with resolved stat names."""
        response = self._make_request(
            f"/players/{player_id}",
            params={
                "include": "statistics.details",
                "filters": f"playerStatisticSeasons:{self.current_season_id}",
            },
        )

        data = response.get("data", {})
        return self._flatten_player_data(data)

    def get_team_statistics(self, team_id: int) -> dict[str, Any]:
        """Get team overall stats for the current season with resolved stat names."""
        response = self._make_request(
            f"/teams/{team_id}",
            params={
                "include": "statistics.details",
                "filters": f"teamStatisticSeasons:{self.current_season_id}",
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

                # Recursively flatten the value into individual columns
                self._flatten_value(flat, stat_name, value)

        return flat

    def _flatten_value(
        self, flat: dict[str, Any], prefix: str, value: Any, max_depth: int = 3
    ) -> None:
        """Recursively flatten nested values into flat dictionary.

        Handles structures like:
        - {"count": 10, "average": 2.5} -> prefix_count, prefix_average
        - {"all": {"count": 4}, "home": {"count": 2}} -> prefix_all_count, prefix_home_count
        - Lists are skipped (not stored)
        - Primitives are stored directly
        """
        if max_depth <= 0:
            if not isinstance(value, (dict, list)):
                flat[prefix] = value
            return

        if isinstance(value, dict):
            value_dict = cast(dict[str, Any], value)
            for sub_key, sub_val in value_dict.items():
                col_name = f"{prefix}_{self._to_snake_case(sub_key)}"
                self._flatten_value(flat, col_name, sub_val, max_depth - 1)
        elif isinstance(value, list):
            # Don't store data that appears in a list
            pass
        else:
            # Primitive value (int, float, str, bool, None)
            flat[prefix] = value

    def get_fixtures(self, include_future: bool = True) -> list[dict[str, Any]]:
        """Get all fixtures for the current season.

        Args:
            include_future: If True, include future fixtures. If False, only return
                           fixtures that have already been played (have statistics).
        """
        logger.info("Fetching fixtures for season ID: %s", self.current_season_id)

        fixtures = self._make_paginated_request(
            "/fixtures",
            params={"filters": f"fixtureSeasons:{self.current_season_id}"},
        )
        logger.info("Found %s total fixtures", len(fixtures))

        if not include_future:
            # Filter to only completed fixtures (state_id 5 = finished)
            # Also check for fixtures with result_info to catch edge cases
            completed = [f for f in fixtures if f.get("state_id") == 5 or f.get("result_info")]
            logger.info("Filtered to %s completed fixtures", len(completed))
            return completed

        return fixtures

    def get_completed_fixtures(self, limit: int | None = None) -> list[dict[str, Any]]:
        """Get completed fixtures, optionally limited to the most recent N and sorted by date descending."""
        fixtures = self.get_fixtures(include_future=False)
        return sorted(fixtures, key=lambda x: x.get("starting_at", ""), reverse=True)[:limit]

    def get_fixture_with_stats(self, fixture_id: int) -> dict[str, Any]:
        """Get a fixture with participants, statistics, and scores."""
        response = self._make_request(
            f"/fixtures/{fixture_id}",
            params={"include": "participants;statistics;scores"},
        )
        return response.get("data", {})

    def flatten_fixture_team_stats(
        self, fixture_data: dict[str, Any], team_id: int
    ) -> dict[str, Any]:
        """Flatten fixture statistics for a specific team into a dict.

        Fixture stats have structure: {type_id, participant_id, data: {value}, location}
        """
        flat: dict[str, Any] = {}

        statistics = fixture_data.get("statistics", [])
        for stat in statistics:
            if stat.get("participant_id") != team_id:
                continue

            type_id = stat.get("type_id")
            if type_id is None:
                continue

            stat_name = self.get_type_name(type_id)
            value = stat.get("data", {}).get("value")
            flat[stat_name] = value

        return flat

    def get_fixture_score(self, fixture_data: dict[str, Any], team_id: int) -> dict[str, Any]:
        """Extract score information for a specific team from fixture data."""
        scores = fixture_data.get("scores", [])

        result: dict[str, Any] = {
            "goals_scored": None,
            "goals_1st_half": None,
            "goals_2nd_half": None,
        }

        for score in scores:
            if score.get("participant_id") != team_id:
                continue

            description = score.get("description", "")
            goals = score.get("score", {}).get("goals")

            if description == "CURRENT":
                result["goals_scored"] = goals
            elif description == "1ST_HALF":
                result["goals_1st_half"] = goals
            elif description == "2ND_HALF":
                result["goals_2nd_half"] = goals

        return result

    def get_fixture_with_lineups(self, fixture_id: int) -> dict[str, Any]:
        """Get a fixture with lineups and player details/statistics."""
        response = self._make_request(
            f"/fixtures/{fixture_id}",
            params={"include": "lineups.details;participants"},
        )
        return response.get("data", {})

    def flatten_lineup_details(self, details: list[dict[str, Any]]) -> dict[str, Any]:
        """Flatten lineup details (player fixture stats) into a dict.

        Lineup details have structure: {type_id, data: {value}}
        This is simpler than season statistics which have nested details.
        """
        flat: dict[str, Any] = {}

        for detail in details:
            type_id = detail.get("type_id")
            if type_id is None:
                continue

            stat_name = self.get_type_name(type_id)
            value = detail.get("data", {}).get("value")
            flat[stat_name] = value

        return flat
