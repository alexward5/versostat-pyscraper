import argparse
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
from rapidfuzz import fuzz, process  # type: ignore[import-untyped]

from ...classes.PostgresClient import PostgresClient
from ...utils.df_utils.build_table_columns import build_table_columns_from_df
from ...utils.logger import setup_logger
from ..helpers import insert_dataframe_rows

logger = setup_logger(__name__)

TABLE_NAME = "crosswalk_player_id"
PRIMARY_KEY = "fpl_player_id"
PLAYER_MATCH_THRESHOLD = 80  # Minimum score to accept a player match
TEAM_MATCH_THRESHOLD = 80  # Minimum score to accept a team match

# Manual mappings for team name abbreviations/nicknames that fuzzy matching can't handle
MANUAL_TEAM_MAPPINGS: dict[str, str] = {
    "Man Utd": "Manchester United",
    "Man United": "Manchester United",
    "Spurs": "Tottenham Hotspur",
    "Tottenham": "Tottenham Hotspur",
}


def fuzzy_extract_one(
    query: str,
    choices: list[str],
    scorer: Any = fuzz.WRatio,
) -> tuple[str, float, int] | None:
    """
    Typed wrapper for rapidfuzz.process.extractOne.
    Returns (match, score, index) or None if no match found.
    """
    result: tuple[str, float, int] | None = process.extractOne(query, choices, scorer=scorer)

    return result


@dataclass
class MatchStats:
    """Tracks matching statistics."""

    matched: int = 0
    unmatched: int = 0
    unmatched_players: list[dict[str, Any]] = field(default_factory=list)


def get_fpl_name_variants(row: dict[str, Any]) -> list[str]:
    """Generate name variants from FPL player row."""
    variants: list[str] = []

    first_name = str(row.get("first_name", "") or "").strip()
    second_name = str(row.get("second_name", "") or "").strip()
    full_name = f"{first_name} {second_name}".strip()
    if full_name:
        variants.append(full_name)

    web_name = str(row.get("web_name", "") or "").strip()
    if web_name and web_name != full_name:
        variants.append(web_name)

    return variants


def get_sm_name_variants(row: dict[str, Any]) -> list[str]:
    """Generate name variants from SM player row."""
    variants: list[str] = []

    player_name = str(row.get("player_name", "") or "").strip()
    if player_name:
        variants.append(player_name)

    first_name = str(row.get("first_name", "") or "").strip()
    last_name = str(row.get("last_name", "") or "").strip()
    full_name = f"{first_name} {last_name}".strip()
    if full_name and full_name != player_name:
        variants.append(full_name)

    common_name = str(row.get("common_name", "") or "").strip()
    if common_name and common_name != player_name:
        variants.append(common_name)

    return variants


def match_team_names(fpl_teams: list[str], sm_teams: list[str]) -> dict[str, str]:
    """
    Build FPL -> SM team name mapping using manual mappings and fuzzy matching.
    Raises ValueError if any FPL team cannot be matched to an SM team.
    """
    team_mapping: dict[str, str] = {}
    unmatched: list[str] = []
    sm_teams_set = set[str](sm_teams)

    for fpl_team in fpl_teams:
        # First check manual mappings for known abbreviations/nicknames
        if fpl_team in MANUAL_TEAM_MAPPINGS:
            mapped_team = MANUAL_TEAM_MAPPINGS[fpl_team]
            if mapped_team in sm_teams_set:
                team_mapping[fpl_team] = mapped_team
                logger.info("Team match: '%s' -> '%s' (manual)", fpl_team, mapped_team)
                continue

        # Fall back to fuzzy matching
        result = fuzzy_extract_one(fpl_team, sm_teams, scorer=fuzz.WRatio)
        if result and result[1] >= TEAM_MATCH_THRESHOLD:
            matched_team: str = result[0]
            score: float = result[1]
            team_mapping[fpl_team] = matched_team
            logger.info("Team match: '%s' -> '%s' (score: %.1f)", fpl_team, matched_team, score)
        else:
            unmatched.append(fpl_team)

    if unmatched:
        raise ValueError(f"Could not match FPL teams to SM teams: {unmatched}")

    return team_mapping


def build_sm_team_index(
    sm_players: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Group SM players by their team_name."""
    team_index: dict[str, list[dict[str, Any]]] = {}

    for player in sm_players:
        team_name = str(player.get("team_name", "") or "")
        if team_name not in team_index:
            team_index[team_name] = []
        team_index[team_name].append(player)

    return team_index


def find_best_match(
    fpl_player: dict[str, Any],
    sm_candidates: list[dict[str, Any]],
    threshold: int,
) -> tuple[dict[str, Any], float] | None:
    """
    Find the best SM player match for an FPL player.
    Returns (sm_player, score) or None if no match above threshold.
    """
    fpl_variants = get_fpl_name_variants(fpl_player)
    if not fpl_variants:
        return None

    best_match: tuple[dict[str, Any], float] | None = None

    for sm_player in sm_candidates:
        sm_variants = get_sm_name_variants(sm_player)
        if not sm_variants:
            continue

        # Compare each FPL variant against each SM variant
        for fpl_name in fpl_variants:
            result = fuzzy_extract_one(fpl_name, sm_variants, scorer=fuzz.WRatio)
            if result and result[1] >= threshold:
                match_score: float = result[1]
                if best_match is None or match_score > best_match[1]:
                    best_match = (sm_player, match_score)

    return best_match


def main(schema: str) -> None:
    """Build crosswalk table linking FPL players to SM players."""
    db = PostgresClient()
    db.create_schema(schema)

    # Query source tables
    logger.info("Querying source tables from schema: %s", schema)
    fpl_players = db.query_table(schema, "fpl_player")
    sm_players = db.query_table(schema, "sm_player_overall")

    logger.info("Found %s FPL players", len(fpl_players))
    logger.info("Found %s SM players", len(sm_players))

    if not fpl_players or not sm_players:
        raise ValueError("One or both source tables are empty")

    # Extract unique team names
    fpl_teams = list[str](
        set[str](str(p.get("team_name", "")) for p in fpl_players if p.get("team_name"))
    )
    sm_teams = list[str](
        set[str](str(p.get("team_name", "")) for p in sm_players if p.get("team_name"))
    )

    logger.info_with_newline("Found %s FPL teams, %s SM teams", len(fpl_teams), len(sm_teams))

    # Match team names
    logger.info("Matching team names...")
    team_mapping = match_team_names(fpl_teams, sm_teams)
    logger.info_with_newline("Successfully matched all %s teams", len(team_mapping))

    # Build SM player index by team
    sm_team_index = build_sm_team_index(sm_players)

    # Match players
    logger.info("Matching players...")
    crosswalk_rows: list[dict[str, Any]] = []
    stats = MatchStats()

    for fpl_player in fpl_players:
        fpl_team = str(fpl_player.get("team_name", "") or "")
        fpl_player_id = fpl_player.get("id")  # FPL uses 'id' column

        if not fpl_team or fpl_team not in team_mapping:
            stats.unmatched += 1
            stats.unmatched_players.append(
                {"name": get_fpl_name_variants(fpl_player), "team": fpl_team, "reason": "no team"}
            )
            continue

        sm_team = team_mapping[fpl_team]
        sm_candidates = sm_team_index.get(sm_team, [])

        if not sm_candidates:
            stats.unmatched += 1
            stats.unmatched_players.append(
                {
                    "name": get_fpl_name_variants(fpl_player),
                    "team": fpl_team,
                    "reason": "no SM candidates",
                }
            )
            continue

        match_result = find_best_match(fpl_player, sm_candidates, PLAYER_MATCH_THRESHOLD)

        if match_result:
            sm_player, _score = match_result
            crosswalk_rows.append(
                {
                    "fpl_player_id": fpl_player_id,
                    "sm_player_id": sm_player.get("player_id"),
                }
            )
            stats.matched += 1
        else:
            stats.unmatched += 1
            fpl_names = get_fpl_name_variants(fpl_player)
            stats.unmatched_players.append(
                {
                    "name": fpl_names[0] if fpl_names else "unknown",
                    "team": fpl_team,
                    "reason": "no player match",
                }
            )

    # Log match statistics
    logger.info_with_newline("=" * 60)
    logger.info("Match Statistics:")
    logger.info("  Matched: %s", stats.matched)
    logger.info("  Unmatched: %s", stats.unmatched)
    logger.info(
        "  Match rate: %.1f%%",
        (stats.matched / (stats.matched + stats.unmatched) * 100) if stats.matched else 0,
    )

    # Log unmatched players
    if stats.unmatched_players:
        logger.warning_with_newline("Unmatched FPL players:")
        for player in stats.unmatched_players:
            logger.warning("  %s (%s) - %s", player["name"], player["team"], player["reason"])

    if not crosswalk_rows:
        logger.warning("No matches found - crosswalk table not created")
        db.close()
        return

    # Create and populate crosswalk table
    crosswalk_df = pd.DataFrame(crosswalk_rows)
    columns = build_table_columns_from_df(crosswalk_df, PRIMARY_KEY)

    db.drop_table(schema, TABLE_NAME)
    db.create_table(schema, TABLE_NAME, columns)
    insert_dataframe_rows(db, schema, TABLE_NAME, crosswalk_df, PRIMARY_KEY)

    db.close()

    logger.info_with_newline("=" * 60)
    logger.info("Completed: %s crosswalk entries created", len(crosswalk_rows))
    logger.info("Table: %s.%s", schema, TABLE_NAME)
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build FPL to SM player crosswalk table")
    parser.add_argument("--schema", type=str, required=True, help="Database schema name to use")
    args = parser.parse_args()
    main(args.schema)
