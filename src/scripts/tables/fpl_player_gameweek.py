import argparse
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from ...classes.FPL_API import FPL_API
from ...classes.PostgresClient import PostgresClient
from ...utils.df_utils.add_id_column import add_id_column
from ...utils.df_utils.build_table_columns import build_table_columns_from_df
from ...utils.logger import setup_logger
from ..helpers import insert_dataframe_rows, reorder_columns

logger = setup_logger(__name__)

TABLE_NAME = "fpl_player_gameweek"
PRIMARY_KEY = "uuid"


@dataclass
class ProcessingState:
    """Tracks state across player processing iterations."""

    table_created: bool = False
    total_rows: int = 0


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and prepare DataFrame for database insertion."""
    df = df.convert_dtypes()

    for col in df.select_dtypes(include=np.number).columns:
        df[col] = df[col].fillna(0)

    return df.fillna("")


def process_player_history(history: list[dict[str, Any]], player_id: int) -> pd.DataFrame | None:
    """Process a single player's gameweek history into a DataFrame."""
    if not history:
        return None

    df = pd.DataFrame(history)

    df = add_id_column(df, source_columns=["element", "round"], id_column_name=PRIMARY_KEY)
    df = clean_dataframe(df)
    df = reorder_columns(df, [PRIMARY_KEY])

    return df


def main(schema: str) -> None:
    """Fetch FPL player gameweek history and load into database."""
    db = PostgresClient()
    db.create_schema(schema)

    api = FPL_API()

    logger.info("Fetching player list from FPL API...")
    bootstrap = api.get_bootstrap_static()
    all_players: list[dict[str, Any]] = bootstrap["elements"]

    # Filter to only selectable players who have played this season
    players = [p for p in all_players if p.get("minutes", 0) > 0 and p.get("can_select", False)]
    logger.info("Processing %s selectable players with minutes > 0 (%s total)", len(players), len(all_players))

    state = ProcessingState()
    total_players = len(players)

    for idx, player in enumerate[dict[str, Any]](players):
        player_id: int = player["id"]
        player_name: str = player.get("web_name", f"Player {player_id}")

        if (idx + 1) % 50 == 0 or idx == 0:
            logger.info_with_newline(
                "Progress: %s/%s players (%.1f%%)",
                idx + 1,
                total_players,
                (idx + 1) / total_players * 100,
            )

        try:
            summary = api.get_player_summary(player_id)
            history: list[dict[str, Any]] = summary.get("history", [])

            if not history:
                continue

            history_df = process_player_history(history, player_id)
            if history_df is None:
                continue

            if not state.table_created:
                columns = build_table_columns_from_df(history_df, PRIMARY_KEY)
                db.create_table(schema, TABLE_NAME, columns)
                state.table_created = True

            insert_dataframe_rows(db, schema, TABLE_NAME, history_df, PRIMARY_KEY)
            state.total_rows += len(history_df)

        except Exception as e:
            logger.error("Error processing %s (id=%s): %s", player_name, player_id, e)

    db.close()

    logger.info_with_newline("=" * 60)
    logger.info("Completed: %s players, %s total gameweek rows", total_players, state.total_rows)
    logger.info("Table: %s.%s", schema, TABLE_NAME)
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch FPL player gameweek history")
    parser.add_argument("--schema", type=str, required=True, help="Database schema name to use")
    args = parser.parse_args()
    main(args.schema)
