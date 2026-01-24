import argparse
from typing import Any, TypedDict

from ...classes.PostgresClient import PostgresClient
from ...classes.SportsRefScraper import SportsRefScraper
from ...utils.df_utils.add_id_column import add_id_column
from ...utils.df_utils.build_table_columns import build_table_columns_from_df
from ...utils.df_utils.sanitize_columns import sanitize_column_names
from ...utils.logger import setup_logger

logger = setup_logger(__name__)


class TeamData(TypedDict):
    squad: str
    url: str


class PlayerData(TypedDict):
    player: str
    squad: str
    url: str


BASE_URL = "https://fbref.com"
PREMIER_LEAGUE_URL = "https://fbref.com/en/comps/9/Premier-League-Stats"
TABLE_NAME = "fbref_player_matchlog"
PRIMARY_KEY_COLUMN = "fbref_player_matchweek_uuid"
PLAYER_UUID_COLUMN = "fbref_player_uuid"

# TODO: Remove these test limits before production
TEST_LIMIT_TEAMS = 1  # Set to None to process all teams
TEST_LIMIT_PLAYERS = 1  # Set to None to process all players


def main(schema: str) -> None:
    """Scrape Premier League player matchlog data and load into database."""
    db = PostgresClient()
    db.create_schema(schema)

    scraper = SportsRefScraper()
    standings_df = scraper.scrape_table(PREMIER_LEAGUE_URL, table_index=0)

    if "squad" not in standings_df.columns or "squad_url" not in standings_df.columns:
        raise ValueError("Expected 'squad' and 'squad_url' columns not found in standings table")

    teams_data: list[TeamData] = [
        {"squad": str(row["squad"]), "url": BASE_URL + str(row["squad_url"])}
        for _, row in standings_df.iterrows()
        if row["squad_url"]
    ]

    # TODO: Remove this test limit before production
    if TEST_LIMIT_TEAMS:
        logger.warning("TEST MODE: Processing only %s team(s)", TEST_LIMIT_TEAMS)
        teams_data = teams_data[:TEST_LIMIT_TEAMS]

    logger.info("Processing %s teams", len(teams_data))

    table_created = False
    total_rows = 0
    reference_columns = None

    for team in teams_data:
        logger.info_with_newline("Processing team: %s...", team["squad"])

        # Scrape team page to get player data
        team_df = scraper.scrape_table(team["url"], table_index=0)
        team_df = sanitize_column_names(team_df)

        # Extract player data from team page
        if "player" not in team_df.columns or "matches_url" not in team_df.columns:
            logger.warning(
                "Skipping %s: missing 'player' or 'matches_url' columns", team["squad"]
            )
            continue

        players_data: list[PlayerData] = [
            {
                "player": str(row["player"]),
                "squad": team["squad"],
                "url": BASE_URL + str(row["matches_url"]),
            }
            for _, row in team_df.iterrows()
            if row["matches_url"]
        ]

        # TODO: Remove this test limit before production
        if TEST_LIMIT_PLAYERS:
            logger.warning("TEST MODE: Processing only %s player(s) per team", TEST_LIMIT_PLAYERS)
            players_data = players_data[:TEST_LIMIT_PLAYERS]

        logger.info("Found %s players for %s", len(players_data), team["squad"])

        for player_data in players_data:
            logger.info("  Processing player: %s...", player_data["player"])

            try:
                # Scrape player matchlog page
                matchlog_df = scraper.scrape_table(player_data["url"], table_index=0)
                matchlog_df = sanitize_column_names(matchlog_df)

                # Filter for Premier League matches only
                if "comp" not in matchlog_df.columns:
                    logger.warning(
                        "Skipping %s (%s): missing 'comp' column",
                        player_data["player"],
                        player_data["squad"],
                    )
                    continue

                matchlog_df = matchlog_df[matchlog_df["comp"] == "Premier League"].copy()

                if len(matchlog_df) == 0:
                    logger.info(
                        "  No Premier League matches found for %s", player_data["player"]
                    )
                    continue

                # Add player and squad columns
                matchlog_df["player"] = player_data["player"]
                matchlog_df["squad"] = player_data["squad"]

                # Add fbref_player_uuid (Player + Squad)
                matchlog_df = add_id_column(
                    matchlog_df,
                    source_columns=["player", "squad"],
                    id_column_name=PLAYER_UUID_COLUMN,
                )

                # Add fbref_player_matchweek_uuid as primary key (Player + Squad + Round)
                if "round" not in matchlog_df.columns:
                    logger.warning(
                        "Skipping %s (%s): missing 'round' column",
                        player_data["player"],
                        player_data["squad"],
                    )
                    continue

                matchlog_df = add_id_column(
                    matchlog_df,
                    source_columns=["player", "squad", "round"],
                    id_column_name=PRIMARY_KEY_COLUMN,
                )

                # Reorder columns to put UUIDs first
                other_columns = [
                    col
                    for col in matchlog_df.columns
                    if col not in [PRIMARY_KEY_COLUMN, PLAYER_UUID_COLUMN]
                ]
                matchlog_df = matchlog_df[[PRIMARY_KEY_COLUMN, PLAYER_UUID_COLUMN] + other_columns]

                # Use column names from first table for all subsequent tables
                if reference_columns is None:
                    reference_columns = list[str](matchlog_df.columns)
                    logger.info(
                        "Reference columns set from %s (%s): %s columns",
                        player_data["player"],
                        player_data["squad"],
                        len(reference_columns),
                    )
                else:
                    # Ensure column count matches reference
                    if len(matchlog_df.columns) != len(reference_columns):
                        raise ValueError(
                            (
                                f"Column count mismatch for {player_data['player']} ({player_data['squad']}): "
                                f"expected {len(reference_columns)} columns but got {len(matchlog_df.columns)}. "
                                f"Current columns: {list(matchlog_df.columns)}"
                            )
                        )
                    # Use reference column names instead of scraped column names
                    matchlog_df.columns = reference_columns

                # Create table on first successful player
                if not table_created:
                    columns = build_table_columns_from_df(matchlog_df, PRIMARY_KEY_COLUMN)
                    db.create_table(schema, TABLE_NAME, columns)
                    table_created = True

                # Insert rows
                for _, row in matchlog_df.iterrows():
                    db.insert_row(
                        schema=schema,
                        table_name=TABLE_NAME,
                        column_names=list[str](matchlog_df.columns),
                        row_values=list[Any](row),
                        update_on=PRIMARY_KEY_COLUMN,
                    )

                total_rows += len(matchlog_df)
                logger.info("  Inserted %s rows for %s", len(matchlog_df), player_data["player"])

            except Exception as e:
                logger.error(
                    "Error processing %s (%s): %s",
                    player_data["player"],
                    player_data["squad"],
                    e,
                )
                continue

    db.close()

    logger.info_with_newline("=" * 60)
    logger.info("Completed: %s teams, %s total rows", len(teams_data), total_rows)
    logger.info("Table: %s.%s", schema, TABLE_NAME)
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape FBref player matchlog data")
    parser.add_argument("--schema", type=str, required=True, help="Database schema name to use")

    args = parser.parse_args()
    main(args.schema)
