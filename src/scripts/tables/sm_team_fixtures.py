"""
Sportmonks Team Fixtures Scraper.

Scrapes per-fixture team statistics for all completed Premier League matches
in the current season.
"""
import argparse
from dataclasses import dataclass
from typing import Any

import pandas as pd

from ...classes.PostgresClient import PostgresClient
from ...classes.SportmonksAPI import SportmonksAPI
from ...utils.df_utils.build_table_columns import build_table_columns_from_df
from ...utils.logger import setup_logger
from ..helpers import insert_dataframe_rows

logger = setup_logger(__name__)

TABLE_NAME = "sm_team_fixtures"
PRIMARY_KEY = "team_fixture_id"


@dataclass
class ProcessingState:
    """Tracks state across fixture processing iterations."""

    table_created: bool = False
    total_rows: int = 0


def build_team_fixture_row(
    fixture_data: dict[str, Any],
    team_id: int,
    team_name: str,
    opponent_id: int,
    opponent_name: str,
    is_home: bool,
    api: SportmonksAPI,
) -> dict[str, Any]:
    """Build a single row for a team's fixture statistics."""
    fixture_id = fixture_data.get("id")

    # Base fixture info
    row: dict[str, Any] = {
        "team_fixture_id": f"{team_id}_{fixture_id}",
        "team_id": team_id,
        "team_name": team_name,
        "fixture_id": fixture_id,
        "season_id": api.current_season_id,
        "fixture_date": fixture_data.get("starting_at", ""),
        "is_home": is_home,
        "opponent_id": opponent_id,
        "opponent_name": opponent_name,
    }

    # Add score data
    score_data = api.get_fixture_score(fixture_data, team_id)
    row.update(score_data)

    # Add opponent score
    opponent_score = api.get_fixture_score(fixture_data, opponent_id)
    row["goals_conceded"] = opponent_score.get("goals_scored")

    # Determine result
    goals_scored = row.get("goals_scored")
    goals_conceded = row.get("goals_conceded")
    if goals_scored is not None and goals_conceded is not None:
        if goals_scored > goals_conceded:
            row["result"] = "W"
        elif goals_scored < goals_conceded:
            row["result"] = "L"
        else:
            row["result"] = "D"
    else:
        row["result"] = ""

    # Add flattened statistics
    stats = api.flatten_fixture_team_stats(fixture_data, team_id)
    row.update(stats)

    return row


def main(schema: str, limit_fixtures: int | None = None) -> None:
    """Scrape Premier League team fixture stats and load into database."""
    db = PostgresClient()
    db.create_schema(schema)

    api = SportmonksAPI()

    # Get completed fixtures only (no future games)
    fixtures = api.get_fixtures(include_future=False)

    if not fixtures:
        logger.warning("No completed fixtures found")
        db.close()
        return

    if limit_fixtures:
        # Sort by date descending and take most recent N fixtures
        fixtures = sorted(fixtures, key=lambda x: x.get("starting_at", ""), reverse=True)
        fixtures = fixtures[:limit_fixtures]
        logger.info("Limited to %s most recent fixtures for testing", limit_fixtures)

    logger.info("Processing %s completed fixtures", len(fixtures))
    state = ProcessingState()

    all_team_fixture_stats: list[dict[str, object]] = []

    for i, fixture in enumerate(fixtures):
        fixture_id = fixture.get("id")

        if not fixture_id:
            continue

        try:
            # Fetch fixture with full details
            fixture_data = api.get_fixture_with_stats(fixture_id)

            if not fixture_data:
                logger.warning("No data for fixture %s", fixture_id)
                continue

            # Extract participants
            participants = fixture_data.get("participants", [])
            if len(participants) < 2:
                logger.warning("Fixture %s has incomplete participant data", fixture_id)
                continue

            home_team = participants[0]
            away_team = participants[1]

            home_id = home_team.get("id")
            home_name = home_team.get("name", "")
            away_id = away_team.get("id")
            away_name = away_team.get("name", "")

            # Build row for home team
            home_row = build_team_fixture_row(
                fixture_data, home_id, home_name, away_id, away_name, True, api
            )
            all_team_fixture_stats.append(home_row)

            # Build row for away team
            away_row = build_team_fixture_row(
                fixture_data, away_id, away_name, home_id, home_name, False, api
            )
            all_team_fixture_stats.append(away_row)

            if (i + 1) % 10 == 0:
                logger.info("Processed %s/%s fixtures...", i + 1, len(fixtures))

        except Exception as e:
            logger.error("Error processing fixture %s: %s", fixture_id, e)

    if not all_team_fixture_stats:
        logger.warning("No team fixture statistics collected")
        db.close()
        return

    # Build DataFrame
    logger.info("Building DataFrame from %s records...", len(all_team_fixture_stats))
    df = pd.DataFrame(all_team_fixture_stats)
    df = df.convert_dtypes()

    # Fill missing values
    for col in df.select_dtypes(include=["number"]).columns:
        df[col] = df[col].fillna(0)
    df = df.fillna("")

    # Ensure primary key is first
    if PRIMARY_KEY in df.columns:
        cols = [PRIMARY_KEY] + [c for c in df.columns if c != PRIMARY_KEY]
        df = df[cols]

    logger.info("DataFrame columns (%s): %s", len(df.columns), list(df.columns)[:15])

    # Create table and insert
    columns = build_table_columns_from_df(df, PRIMARY_KEY)
    db.create_table(schema, TABLE_NAME, columns)

    insert_dataframe_rows(db, schema, TABLE_NAME, df, PRIMARY_KEY)
    state.total_rows = len(df)

    db.close()

    logger.info_with_newline("=" * 60)
    logger.info("Completed: %s fixtures, %s team-fixture rows", len(fixtures), state.total_rows)
    logger.info("Table: %s.%s", schema, TABLE_NAME)
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Sportmonks team fixture stats")
    parser.add_argument("--schema", type=str, required=True, help="Database schema name to use")
    parser.add_argument(
        "--limit-fixtures",
        type=int,
        default=None,
        help="Limit to first N fixtures for testing",
    )
    args = parser.parse_args()
    main(args.schema, args.limit_fixtures)
