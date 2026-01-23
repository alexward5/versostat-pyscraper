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


BASE_URL = "https://fbref.com"
PREMIER_LEAGUE_URL = "https://fbref.com/en/comps/9/Premier-League-Stats"
TABLE_NAME = "fbref_team_players_standard"
PRIMARY_KEY_COLUMN = "fbref_player_uuid"


def main(schema: str) -> None:
    """Scrape Premier League team player stats and load into database."""
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

    logger.info("Processing %s teams", len(teams_data))

    table_created = False
    total_rows = 0
    reference_columns = None

    for team in teams_data:
        logger.info_with_newline("Processing %s...", team["squad"])

        players_df = scraper.scrape_table(team["url"], table_index=0)
        players_df = sanitize_column_names(players_df)
        players_df["squad"] = team["squad"]
        players_df = add_id_column(
            players_df, source_columns=["player", "squad"], id_column_name=PRIMARY_KEY_COLUMN
        )

        # Reorder columns to put UUID first
        players_df = players_df[
            [PRIMARY_KEY_COLUMN] + [col for col in players_df.columns if col != PRIMARY_KEY_COLUMN]
        ]

        # Use column names from first table for all subsequent tables
        if reference_columns is None:
            reference_columns = list[str](players_df.columns)
            logger.info(
                "Reference columns set from %s: %s columns", team["squad"], len(reference_columns)
            )
        else:
            # Ensure column count matches reference
            if len(players_df.columns) != len(reference_columns):
                raise ValueError(
                    (
                        f"Column count mismatch for {team['squad']}: "
                        f"expected {len(reference_columns)} columns but got {len(players_df.columns)}. "
                        f"Current columns: {list(players_df.columns)}"
                    )
                )
            # Use reference column names instead of scraped column names
            players_df.columns = reference_columns

        if not table_created:
            columns = build_table_columns_from_df(players_df, PRIMARY_KEY_COLUMN)
            db.create_table(schema, TABLE_NAME, columns)
            table_created = True

        for _, row in players_df.iterrows():
            db.insert_row(
                schema=schema,
                table_name=TABLE_NAME,
                column_names=list[str](players_df.columns),
                row_values=list[Any](row),
                update_on=PRIMARY_KEY_COLUMN,
            )

        total_rows += len(players_df)
        logger.info("Inserted %s rows", len(players_df))

    db.close()

    logger.info_with_newline("=" * 60)
    logger.info("Completed: %s teams, %s total rows", len(teams_data), total_rows)
    logger.info("Table: %s.%s", schema, TABLE_NAME)
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape FBref team player standard stats")
    parser.add_argument("--schema", type=str, required=True, help="Database schema name to use")

    args = parser.parse_args()
    main(args.schema)
