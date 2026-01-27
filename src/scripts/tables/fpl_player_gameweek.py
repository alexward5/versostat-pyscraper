import argparse
from typing import Any

import pandas as pd

from ...classes.FantasyPremierLeagueAPI import FantasyPremierLeagueAPI
from ...classes.PostgresClient import PostgresClient
from ...utils.df_utils import add_id_column, standardize_to_date, transform_column
from ...utils.df_utils.build_table_columns import generate_column_definitions
from ...utils.df_utils.prepare_for_insert import prepare_for_insert
from ...utils.logger import log_script_complete, log_script_start, setup_logger, should_log_progress

logger = setup_logger(__name__)

TABLE_NAME = "fpl_player_gameweek"
PRIMARY_KEY = "uuid"


def process_player_history(history: list[dict[str, Any]], player_id: int) -> pd.DataFrame | None:
    """Process a single player's gameweek history into a DataFrame."""
    if not history:
        return None

    df = pd.DataFrame(history)
    df = add_id_column(df, source_columns=["element", "round"], id_column_name=PRIMARY_KEY)
    df = transform_column(df, "kickoff_time", standardize_to_date)
    df = prepare_for_insert(df, PRIMARY_KEY)

    return df


def main(schema: str) -> None:
    """Fetch FPL player gameweek history and load into database."""
    log_script_start(__name__)
    
    db = PostgresClient()
    db.create_schema(schema)

    fpl_api = FantasyPremierLeagueAPI()

    logger.info("Fetching player list from FPL API...")
    bootstrap = fpl_api.get_bootstrap_static()
    all_players: list[dict[str, Any]] = bootstrap["elements"]

    # Filter to only selectable players who have played this season
    players = [p for p in all_players if p.get("minutes", 0) > 0 and p.get("can_select", False)]
    logger.info(
        "Processing %s selectable players with minutes > 0 (%s total)",
        len(players),
        len(all_players),
    )

    table_created = False
    total_rows = 0
    total_players = len(players)

    for idx, player in enumerate[dict[str, Any]](players):
        player_id: int = player["id"]
        player_name: str = player.get("web_name", f"Player {player_id}")

        if should_log_progress(idx + 1, total_players):
            logger.info("Progress: %s/%s (%d%%)", idx + 1, total_players, int((idx + 1) / total_players * 100))

        try:
            summary = fpl_api.get_player_summary(player_id)
            history: list[dict[str, Any]] = summary.get("history", [])

            if not history:
                continue

            history_df = process_player_history(history, player_id)
            if history_df is None:
                continue

            if not table_created:
                column_definitions = generate_column_definitions(history_df, PRIMARY_KEY)
                db.create_table(schema, TABLE_NAME, column_definitions)
                table_created = True

            db.insert_dataframe(schema, TABLE_NAME, history_df, PRIMARY_KEY)
            total_rows += len(history_df)

        except Exception as e:
            logger.error("Error processing %s (id=%s): %s", player_name, player_id, e)

    db.close()

    log_script_complete(
        __name__,
        schema=schema,
        table_name=TABLE_NAME,
        total_players=total_players,
        total_gameweek_rows=total_rows
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch FPL player gameweek history")
    parser.add_argument("--schema", type=str, required=True, help="Database schema name to use")
    args = parser.parse_args()
    main(args.schema)
