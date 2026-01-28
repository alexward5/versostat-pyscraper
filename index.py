import argparse
import inspect
from typing import Any, Literal

from src.scripts.tables import crosswalk_player_id, fpl_events, fpl_player, fpl_player_gameweek, fpl_teams
from src.scripts.tables import (
    sm_player_fixtures,
    sm_player_overall,
    sm_team_fixtures,
    sm_team_overall,
)
from src.scripts.views import mv_player_gameweek
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

SCRIPT_MAP = {
    "fpl_events": fpl_events.main,
    "fpl_player": fpl_player.main,
    "fpl_player_gameweek": fpl_player_gameweek.main,
    "fpl_teams": fpl_teams.main,
    "sm_player_fixtures": sm_player_fixtures.main,
    "sm_player_overall": sm_player_overall.main,
    "sm_team_fixtures": sm_team_fixtures.main,
    "sm_team_overall": sm_team_overall.main,
    "crosswalk_player_id": crosswalk_player_id.main,
    "mv_player_gameweek": mv_player_gameweek.main,
}

FPL_SCRIPTS = ["fpl_events", "fpl_player", "fpl_player_gameweek", "fpl_teams"]
SM_SCRIPTS = ["sm_player_fixtures", "sm_player_overall", "sm_team_fixtures", "sm_team_overall"]
VIEW_SCRIPTS = ["mv_player_gameweek"]
ALL_SCRIPTS = FPL_SCRIPTS + SM_SCRIPTS + ["crosswalk_player_id"] + VIEW_SCRIPTS


def run_scripts(
    schema: str,
    scripts: list[str] | Literal["all", "tables", "views", "fpl", "sm"] = "all",
    **kwargs: Any,
) -> None:
    """Run table/view scripts. Pass 'all', 'tables', 'views', 'fpl', 'sm', or a list of script names."""
    scripts_to_run: list[str]
    if scripts == "all":
        scripts_to_run = ALL_SCRIPTS
    elif scripts == "tables":
        scripts_to_run = FPL_SCRIPTS + SM_SCRIPTS + ["crosswalk_player_id"]
    elif scripts == "views":
        scripts_to_run = VIEW_SCRIPTS
    elif scripts == "fpl":
        scripts_to_run = FPL_SCRIPTS
    elif scripts == "sm":
        scripts_to_run = SM_SCRIPTS
    else:
        invalid_scripts = [s for s in scripts if s not in SCRIPT_MAP]
        if invalid_scripts:
            raise ValueError(f"Invalid script names: {invalid_scripts}")
        scripts_to_run = scripts

    logger.info("=" * 60)
    logger.info("Starting pipeline for schema: %s", schema)
    logger.info("Scripts to run (%s): %s", len(scripts_to_run), scripts_to_run)
    logger.info("=" * 60)
    logger.newline()

    successful: list[str] = []
    failed: list[str] = []

    for script_name in scripts_to_run:
        try:
            script_func = SCRIPT_MAP[script_name]
            sig = inspect.signature(script_func)
            supported_params = set(sig.parameters.keys()) - {"schema"}
            filtered_kwargs: dict[str, Any] = {
                k: v for k, v in kwargs.items() if k in supported_params
            }

            script_func(schema, **filtered_kwargs)
            successful.append(script_name)
            logger.info_with_newline("✓ Successfully completed: %s", script_name)

        except Exception as e:
            failed.append(script_name)
            logger.error("✗ Failed: %s - Error: %s", script_name, str(e))
            logger.exception(e)

    logger.newline()
    logger.info("=" * 60)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 60)
    logger.info("Schema: %s", schema)
    logger.info(
        "Total: %s / Successful: %s / Failed: %s", len(scripts_to_run), len(successful), len(failed)
    )

    if successful:
        logger.info("Successful scripts:")
        for script in successful:
            logger.info("  ✓ %s", script)

    if failed:
        logger.info("Failed scripts:")
        for script in failed:
            logger.info("  ✗ %s", script)

    logger.info("=" * 60)


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Run table/view scripts",
    )

    parser.add_argument("--schema", type=str, required=True, help="Database schema name")
    parser.add_argument(
        "--scripts",
        nargs="+",
        default=["all"],
        help='Scripts to run: "all", "tables", "views", "fpl", "sm", or specific names',
    )
    parser.add_argument(
        "--limit-fixtures", type=int, help="Limit fixtures (for SM fixture scripts)"
    )
    parser.add_argument("--limit-teams", type=int, help="Limit teams (for SM overall scripts)")

    args = parser.parse_args()

    scripts_param = (
        args.scripts[0]
        if len(args.scripts) == 1 and args.scripts[0] in ["all", "tables", "views", "fpl", "sm"]
        else args.scripts
    )

    kwargs = {}
    if args.limit_fixtures is not None:
        kwargs["limit_fixtures"] = args.limit_fixtures
    if args.limit_teams is not None:
        kwargs["limit_teams"] = args.limit_teams

    run_scripts(args.schema, scripts=scripts_param, **kwargs)


if __name__ == "__main__":
    main()
