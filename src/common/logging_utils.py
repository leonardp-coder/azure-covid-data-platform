
"""
Reusable logging utilities for the Azure COVID Data Engineering project.
"""

import logging


LOG_LEVEL = logging.INFO

LOG_FORMAT = (
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)


def get_logger(name: str) -> logging.Logger:
    """
    Create and return a configured logger.

    Reuses existing handlers to prevent duplicate log messages.
    """

    logger = logging.getLogger(name)

    logger.setLevel(LOG_LEVEL)
    logger.propagate = False

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(LOG_LEVEL)

        formatter = logging.Formatter(LOG_FORMAT)
        handler.setFormatter(formatter)

        logger.addHandler(handler)

    return logger
