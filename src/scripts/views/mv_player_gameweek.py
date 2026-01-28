import argparse

from ...classes.PostgresClient import PostgresClient
from ...utils.logger import log_script_complete, log_script_start, setup_logger

logger = setup_logger(__name__)

VIEW_NAME = "mv_player_gameweek"


def main(schema: str) -> None:
    """Create materialized view joining FPL and Sportmonks player gameweek data."""
    log_script_start(__name__)

    db = PostgresClient()

    # Verify required tables exist
    required_tables = ["fpl_player_gameweek", "sm_player_fixtures", "crosswalk_player_id", "fpl_player"]
    for table in required_tables:
        if not db.table_exists(schema, table):
            raise ValueError(
                f"Required table {schema}.{table} does not exist. Please run the table creation scripts first."
            )

    view_query = f"""
        SELECT 
            crosswalk_player_id.fpl_player_id,
            fpl_player_gameweek.minutes as fpl_minutes,
            fpl_player_gameweek.round as fpl_round,
            fpl_player_gameweek.total_points as fpl_total_points,
            fpl_player_gameweek.goals_scored as fpl_goals_scored,
            fpl_player_gameweek.assists as fpl_assists,
            fpl_player_gameweek.bps as fpl_bps,
            fpl_player_gameweek.clean_sheets as fpl_clean_sheet,
            fpl_player_gameweek.defensive_contribution as fpl_defensive_contribution,
            fpl_player_gameweek.expected_goals as fpl_expected_goals,
            fpl_player_gameweek.expected_assists as fpl_expected_assists,
            fpl_player_gameweek.expected_goal_involvements as fpl_xgi,
            sm_player_fixtures.shots_on_target as sm_shots_on_target,
            sm_player_fixtures.big_chances_created as sm_big_chances_created,
            sm_player_fixtures.key_passes as sm_key_passes,
            CASE 
                WHEN fpl_player.element_type = 1 THEN (CAST(fpl_player_gameweek.expected_goals AS DECIMAL) * 10) + (CAST(fpl_player_gameweek.expected_assists AS DECIMAL) * 3)
                WHEN fpl_player.element_type = 2 THEN (CAST(fpl_player_gameweek.expected_goals AS DECIMAL) * 6) + (CAST(fpl_player_gameweek.expected_assists AS DECIMAL) * 3)
                WHEN fpl_player.element_type = 3 THEN (CAST(fpl_player_gameweek.expected_goals AS DECIMAL) * 5) + (CAST(fpl_player_gameweek.expected_assists AS DECIMAL) * 3)
                WHEN fpl_player.element_type = 4 THEN (CAST(fpl_player_gameweek.expected_goals AS DECIMAL) * 4) + (CAST(fpl_player_gameweek.expected_assists AS DECIMAL) * 3)
                ELSE NULL
            END as calc_xgap
        FROM {schema}.fpl_player_gameweek fpl_player_gameweek
        JOIN {schema}.crosswalk_player_id crosswalk_player_id
            ON fpl_player_gameweek.element::text = crosswalk_player_id.fpl_player_id
        JOIN {schema}.fpl_player fpl_player
            ON crosswalk_player_id.fpl_player_id = fpl_player.id
        JOIN {schema}.sm_player_fixtures sm_player_fixtures
            ON crosswalk_player_id.sm_player_id = sm_player_fixtures.player_id::text
            AND fpl_player_gameweek.kickoff_time = sm_player_fixtures.fixture_date
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
    parser = argparse.ArgumentParser(description="Create mv_player_gameweek materialized view")
    parser.add_argument("--schema", type=str, required=True, help="Database schema name to use")
    args = parser.parse_args()
    main(args.schema)
