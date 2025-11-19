from __future__ import annotations

import sys

from loguru import logger

from syntheca.config import settings


def configure_logging() -> None:
    # Remove default handlers and add new handlers to stderr and a rotating file
    logger.remove()
    # Ensure log dir exists
    log_file = settings.log_file
    log_file.parent.mkdir(parents=True, exist_ok=True)

    # Standard formatted stderr
    logger.add(sys.stderr, level="INFO", backtrace=False, diagnose=False)

    # Rotating file log for debugging and persistence
    logger.add(str(log_file), rotation="10 MB", retention="14 days", level="DEBUG")


configure_logging()
