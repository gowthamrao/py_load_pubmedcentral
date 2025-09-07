"""
Utility functions for the application.
"""
from __future__ import annotations

import os

from py_load_pubmedcentral.db import PostgreSQLAdapter


def get_db_adapter() -> PostgreSQLAdapter:
    """
    Creates a PostgreSQLAdapter instance from environment variables.
    """
    connection_params = {
        "dbname": os.environ.get("DB_NAME", "pmc_db"),
        "user": os.environ.get("DB_USER", "user"),
        "password": os.environ.get("DB_PASSWORD", "password"),
        "host": os.environ.get("DB_HOST", "localhost"),
        "port": os.environ.get("DB_PORT", "5432"),
    }
    return PostgreSQLAdapter(connection_params)
