import logging
import sys
from typing import Any


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
