"""
Pytest configuration and shared fixtures.
"""
import pytest
from typing import Any, Generator
from py_load_pubmedcentral.db import PostgreSQLAdapter
from py_load_pubmedcentral.config import settings
from pytest_postgresql.janitor import DatabaseJanitor


@pytest.fixture(scope="session")
def test_db_adapter(postgresql_proc: Any) -> Generator[PostgreSQLAdapter, None, None]:
    """
    Creates a temporary database for the test session, and provides a
    connected adapter to it.
    """
    # Use the DatabaseJanitor to create and drop a database for the session
    janitor = DatabaseJanitor(
        user=postgresql_proc.user,
        host=postgresql_proc.host,
        port=postgresql_proc.port,
        password=postgresql_proc.password,
        dbname="tests",  # The name for our temporary database
        version=postgresql_proc.version,
    )
    janitor.init()

    # Create the connection info dict for our new database
    db_connection_info = {
        "host": postgresql_proc.host,
        "port": postgresql_proc.port,
        "user": postgresql_proc.user,
        "password": postgresql_proc.password,
        "dbname": "tests",
    }

    # Override the application's default settings
    settings.db_host = db_connection_info["host"]
    settings.db_port = db_connection_info["port"]
    settings.db_user = db_connection_info["user"]
    settings.db_password = db_connection_info["password"]
    settings.db_name = db_connection_info["dbname"]

    # Create and connect the adapter
    adapter = PostgreSQLAdapter(connection_params=db_connection_info)
    adapter.connect()

    yield adapter

    # Teardown: close the connection and drop the database
    adapter.close()
    janitor.drop()


from pathlib import Path

def pytest_configure(config):
    """
    Register a custom marker for integration tests.
    """
    config.addinivalue_line(
        "markers", "integration: mark a test as an integration test"
    )


SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "pmc_schema.sql"


@pytest.fixture(scope="function")
def db_with_schema(test_db_adapter: PostgreSQLAdapter):
    """
    Fixture to provide a database adapter with a clean, pre-loaded schema
    for each test function.
    """
    # Drop and recreate the public schema to ensure a clean slate for each test
    with test_db_adapter.conn.cursor() as cursor:
        cursor.execute("DROP SCHEMA public CASCADE;")
        cursor.execute("CREATE SCHEMA public;")
    test_db_adapter.conn.commit()

    # Load the schema
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        sql_script = f.read()
    test_db_adapter.execute_sql(sql_script)

    yield test_db_adapter
    # Teardown is handled by the session-level fixture (test_db_adapter),
    # which drops the entire test database.
