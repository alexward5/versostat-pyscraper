import argparse

from ...classes.PostgresClient import PostgresClient
from ...classes.SportsRefScraper import SportsRefScraper
from ...utils.df_utils.add_id_column import add_id_column
from ...utils.df_utils.build_table_columns import build_table_columns_from_df
from ...utils.logger import setup_logger
from ...utils import insert_dataframe_rows
from ...utils.df_utils import reorder_columns, validate_column_schema

logger = setup_logger(__name__)

BASE_URL = "https://fbref.com"
PREMIER_LEAGUE_URL = f"{BASE_URL}/en/comps/9/Premier-League-Stats"
TABLE_NAME = "fbref_team_players_standard"
PRIMARY_KEY = "fbref_player_uuid"


def main(schema: str) -> None:
    """Scrape Premier League team player stats and load into database."""
    db = PostgresClient()
    db.create_schema(schema)

    scraper = SportsRefScraper()
    standings_df = scraper.scrape_table(PREMIER_LEAGUE_URL, table_index=0)

    required_cols = {"squad", "squad_url"}
    if not required_cols.issubset(standings_df.columns):
        raise ValueError(f"Missing required columns: {required_cols - set(standings_df.columns)}")

    teams = [
        {"squad": str(row["squad"]), "url": f"{BASE_URL}{row['squad_url']}"}
        for _, row in standings_df.iterrows()
        if row["squad_url"]
    ]

    logger.info("Processing %s teams", len(teams))

    table_created = False
    total_rows = 0
    reference_columns: list[str] | None = None

    for team in teams:
        squad_name = team["squad"]
        logger.info_with_newline("Processing %s...", squad_name)

        players_df = scraper.scrape_table(team["url"], table_index=0)
        players_df["squad"] = squad_name
        players_df = add_id_column(
            players_df, source_columns=["player", "squad"], id_column_name=PRIMARY_KEY
        )
        players_df = reorder_columns(players_df, [PRIMARY_KEY])

        # Set reference schema from first team, validate subsequent teams match
        if reference_columns is None:
            reference_columns = list[str](players_df.columns)
            logger.info(
                "Reference schema set from %s: %s columns", squad_name, len(reference_columns)
            )
        else:
            validate_column_schema(players_df, reference_columns, squad_name)
            players_df.columns = reference_columns

        if not table_created:
            columns = build_table_columns_from_df(players_df, PRIMARY_KEY)
            db.create_table(schema, TABLE_NAME, columns)
            table_created = True

        insert_dataframe_rows(db, schema, TABLE_NAME, players_df, PRIMARY_KEY)
        total_rows += len(players_df)
        logger.info("Inserted %s rows", len(players_df))

    db.close()

    logger.info_with_newline("=" * 60)
    logger.info("Completed: %s teams, %s total rows", len(teams), total_rows)
    logger.info("Table: %s.%s", schema, TABLE_NAME)
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape FBref team player standard stats")
    parser.add_argument("--schema", type=str, required=True, help="Database schema name to use")
    args = parser.parse_args()
    main(args.schema)
