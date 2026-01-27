from typing import Any

from dateutil import parser


def standardize_to_date(value: Any) -> str:
    """Standardize various date/datetime formats to YYYY-MM-DD format."""
    if not value or value == "":
        return ""

    try:
        dt = parser.parse(str(value))
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""
