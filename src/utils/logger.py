import logging
import sys
from typing import Any, Optional


class NewlineFormatter(logging.Formatter):
    """Custom formatter that adds newline before the entire log message when requested."""

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        formatted = super().format(record)
        if hasattr(record, "newline_before") and getattr(record, "newline_before", False):
            return "\n" + formatted
        return formatted


class LoggerWithNewline(logging.Logger):
    """Custom logger that supports adding newlines before messages."""

    def info_with_newline(self, msg: str, *args: Any, **kwargs: Any) -> None:
        kwargs["extra"] = kwargs.get("extra", {})
        kwargs["extra"]["newline_before"] = True
        self.info(msg, *args, **kwargs)

    def warning_with_newline(self, msg: str, *args: Any, **kwargs: Any) -> None:
        kwargs["extra"] = kwargs.get("extra", {})
        kwargs["extra"]["newline_before"] = True
        self.warning(msg, *args, **kwargs)

    def error_with_newline(self, msg: str, *args: Any, **kwargs: Any) -> None:
        kwargs["extra"] = kwargs.get("extra", {})
        kwargs["extra"]["newline_before"] = True
        self.error(msg, *args, **kwargs)

    def debug_with_newline(self, msg: str, *args: Any, **kwargs: Any) -> None:
        kwargs["extra"] = kwargs.get("extra", {})
        kwargs["extra"]["newline_before"] = True
        self.debug(msg, *args, **kwargs)


def setup_logger(name: str) -> LoggerWithNewline:
    logging.setLoggerClass(LoggerWithNewline)
    logger = logging.getLogger(name)

    if not logger.handlers:
        if not logging.root.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(
                NewlineFormatter(
                    fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
            )
            logging.root.addHandler(handler)
            logging.root.setLevel(logging.INFO)

    return logger  # type: ignore


def log_script_start(script_name: str) -> None:
    """Log standardized script start header."""
    logger = logging.getLogger(script_name)
    logger.info("=" * 60)
    logger.info("Running: %s", script_name)
    logger.info("=" * 60)


def log_script_complete(script_name: str, schema: str, table_name: str, **stats: Any) -> None:
    """Log standardized script completion summary."""
    logger = logging.getLogger(script_name)
    logger.info("=" * 60)
    logger.info("Completed: %s | Table: %s.%s | %s", script_name, schema, table_name, " | ".join(f"{k}={v}" for k, v in stats.items()))
    logger.info("=" * 60)


def should_log_progress(current: int, total: int, interval: Optional[int] = None) -> bool:
    """Return True if progress should be logged. Auto-calculates smart intervals based on total count."""
    # Always log first and last item
    if current == 1 or current == total:
        return True
    
    # Determine interval
    if interval is None:
        if total <= 20:
            interval = 1  # Log every item for small lists
        elif total <= 50:
            interval = 5
        elif total <= 200:
            interval = 10
        else:
            interval = 25
    
    # Log at interval checkpoints
    return current % interval == 0
