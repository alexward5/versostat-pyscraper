import argparse

from ...classes.PostgresClient import PostgresClient
from ...utils.logger import log_script_complete, log_script_start, setup_logger

logger = setup_logger(__name__)

VIEW_NAME = "mv_player"


def main(schema: str) -> None:
    """Create materialized view from fpl_player with selected columns and derived fields."""
    log_script_start(__name__)

    db = PostgresClient()

    # Verify required tables exist
    required_tables = ["fpl_player"]
    for table in required_tables:
        if not db.table_exists(schema, table):
            raise ValueError(
                f"Required table {schema}.{table} does not exist. Please run the table creation scripts first."
            )

    view_query = f"""
        SELECT
            fpl_player.id as fpl_player_id,
            fpl_player.web_name as fpl_web_name,
            fpl_player.team_name as fpl_team_name,
            CASE
                WHEN fpl_player.element_type = 1 THEN 'GK'
                WHEN fpl_player.element_type = 2 THEN 'DEF'
                WHEN fpl_player.element_type = 3 THEN 'MID'
                WHEN fpl_player.element_type = 4 THEN 'FWD'
            END as fpl_player_position,
            ROUND(CAST(fpl_player.now_cost AS DECIMAL) / 10, 1) as fpl_player_cost,
            fpl_player.selected_by_percent as fpl_selected_by_percent
        FROM {schema}.fpl_player fpl_player
        WHERE fpl_player.element_type != 1
    """

    logger.info("Creating materialized view: %s.%s", schema, VIEW_NAME)
    db.create_materialized_view(schema, VIEW_NAME, view_query)

    # Create indexes for better query performance
    logger.info("Creating indexes on materialized view...")
    db.create_index(
        schema=schema,
        table_name=VIEW_NAME,
        index_name=f"idx_{VIEW_NAME}_fpl_player_id",
        columns=["fpl_player_id"],
    )

    db.close()

    log_script_complete(
        __name__,
        schema=schema,
        table_name=VIEW_NAME,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create mv_player materialized view")
    parser.add_argument("--schema", type=str, required=True, help="Database schema name to use")
    args = parser.parse_args()
    main(args.schema)
