"""
Utility functions for the application.
"""
from __future__ import annotations

import logging

from rich.logging import RichHandler

from py_load_pubmedcentral.config import settings


def get_db_adapter() -> "PostgreSQLAdapter":
    """
    Creates a PostgreSQLAdapter instance from the application settings.
    """
    from py_load_pubmedcentral.db import PostgreSQLAdapter

    connection_params = {
        "dbname": settings.db_name,
        "user": settings.db_user,
        "password": settings.db_password,
        "host": settings.db_host,
        "port": settings.db_port,
    }
    return PostgreSQLAdapter(connection_params)


def get_logger(name: str) -> logging.Logger:
    """
    Configures and returns a logger with RichHandler.

    Args:
        name: The name for the logger, typically __name__.

    Returns:
        A configured logger instance.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    handler = RichHandler(
        rich_tracebacks=True,
        show_time=True,
        show_level=True,
        log_time_format="[%X]",
    )
    logger.setLevel(settings.log_level.upper())
    logger.addHandler(handler)
    logger.propagate = False
    return logger
