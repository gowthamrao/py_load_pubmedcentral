"""
Utility functions for the application.
"""
from __future__ import annotations

from py_load_pubmedcentral.config import settings
from py_load_pubmedcentral.db import PostgreSQLAdapter


def get_db_adapter() -> PostgreSQLAdapter:
    """
    Creates a PostgreSQLAdapter instance from the application settings.
    """
    connection_params = {
        "dbname": settings.db_name,
        "user": settings.db_user,
        "password": settings.db_password,
        "host": settings.db_host,
        "port": settings.db_port,
    }
    return PostgreSQLAdapter(connection_params)
