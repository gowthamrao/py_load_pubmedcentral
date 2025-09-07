"""
Integration test for the full data pipeline.

This test requires a running PostgreSQL instance and the following
environment variables to be set for the test database:
- DB_NAME
- DB_USER
- DB_PASSWORD
- DB_HOST
- DB_PORT
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest
from typer.testing import CliRunner

from py_load_pubmedcentral.cli import app
from py_load_pubmedcentral.utils import get_db_adapter

# Create a CliRunner instance to invoke the CLI commands
runner = CliRunner()

# Path to the test data
TEST_DATA_DIR = Path(__file__).parent
TEST_SCHEMA_PATH = TEST_DATA_DIR.parent / "schemas" / "pmc_schema.sql"
TEST_ARCHIVE_PATH = TEST_DATA_DIR / "test_archive.tar.gz"


@pytest.fixture(scope="module")
def db_adapter():
    """Pytest fixture to provide a configured and connected database adapter."""
    # Check if DB credentials are set, otherwise skip the test
    if not all(os.environ.get(k) for k in ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"]):
        pytest.skip("Database environment variables not set, skipping integration tests.")

    adapter = get_db_adapter()
    adapter.connect()
    yield adapter
    adapter.close()


def cleanup_db(adapter):
    """Helper to drop tables for a clean slate."""
    with adapter.conn.cursor() as cursor:
        cursor.execute("""
            DROP TABLE IF EXISTS pmc_articles_content;
            DROP TABLE IF EXISTS pmc_articles_metadata;
            DROP TABLE IF EXISTS sync_history;
        """)
    adapter.conn.commit()


def test_full_pipeline_end_to_end(db_adapter):
    """
    Tests the entire pipeline:
    1. Initialize the schema.
    2. Run a full load from a local .tar.gz file.
    3. Verify the data is correctly loaded in the database.
    """
    # --- 1. Cleanup and Initialization ---
    cleanup_db(db_adapter)

    # Run the 'initialize' command
    init_result = runner.invoke(app, ["initialize", "--schema", str(TEST_SCHEMA_PATH)])
    assert init_result.exit_code == 0
    assert "Database schema initialized successfully" in init_result.stdout

    # --- 2. Run Full Load ---
    # We use a file:// URL to test the acquisition logic with a local file.
    archive_url = TEST_ARCHIVE_PATH.as_uri()

    load_result = runner.invoke(app, ["full-load", archive_url])
    assert load_result.exit_code == 0
    assert "Phase 1 complete. Total records processed: 2" in load_result.stdout
    assert "Phase 2 complete. Full load process finished successfully" in load_result.stdout

    # --- 3. Verification ---
    with db_adapter.conn.cursor() as cursor:
        # Check metadata table
        cursor.execute("SELECT COUNT(*) FROM pmc_articles_metadata;")
        count = cursor.fetchone()[0]
        assert count == 2

        # Check content table
        cursor.execute("SELECT COUNT(*) FROM pmc_articles_content;")
        count = cursor.fetchone()[0]
        assert count == 2

        # Check for a specific record
        cursor.execute("SELECT title FROM pmc_articles_metadata WHERE pmcid = 'PMC12345';")
        title = cursor.fetchone()[0]
        assert title == "A Comprehensive Test of the JATS Parser"

    # --- 4. Final Cleanup ---
    cleanup_db(db_adapter)
