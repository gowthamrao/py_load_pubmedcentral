"""
End-to-end integration test for the full-load command.
"""
from __future__ import annotations

import tarfile
from pathlib import Path

import pytest
from typer.testing import CliRunner

from py_load_pubmedcentral.cli import app
from py_load_pubmedcentral.models import ArticleFileInfo
from py_load_pubmedcentral.db import PostgreSQLAdapter

# Define paths relative to this test file
TESTS_DIR = Path(__file__).parent
TEST_ARCHIVE_PATH = (TESTS_DIR / "test_data" / "end_to_end_archive.tar.gz").resolve()
SCHEMA_PATH = (TESTS_DIR.parent / "schemas" / "pmc_schema.sql").resolve()


@pytest.mark.integration
def test_full_load_end_to_end(test_db_adapter: PostgreSQLAdapter, mocker):
    """
    Tests the `full-load` command from end to end.

    This test:
    1.  Uses a real, temporary PostgreSQL database instance.
    2.  Initializes the database schema.
    3.  Mocks the S3 data source to prevent network calls and use a local
        test archive instead.
    4.  Runs the `pmc-sync full-load` command.
    5.  Verifies that the data from the test archive is correctly loaded
        into the database.
    """
    # --- 1. Setup: Initialize Schema ---
    # The test_db_adapter fixture already provides a clean, session-scoped
    # database. We just need to create the schema in it.
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        sql_script = f.read()
    test_db_adapter.execute_sql(sql_script)

    # --- 2. Setup: Mock the Data Source ---
    # Mock the return value for the master file list to match the test data
    mock_article_list = [
        ArticleFileInfo(
            file_path="end_to_end_archive.tar.gz",
            pmcid="PMC001",
            pmid="10001",  # Corrected PMID
            last_updated=None,
            is_retracted=False,
        ),
        ArticleFileInfo(
            file_path="end_to_end_archive.tar.gz",
            pmcid="PMC002",
            pmid="10002",  # Corrected PMID
            last_updated=None,
            is_retracted=False,
        ),
    ]
    mocker.patch(
        "py_load_pubmedcentral.cli.S3DataSource.get_article_file_list",
        return_value=mock_article_list,
    )

    # Mock the download function to simply return the path to our local test archive
    mocker.patch(
        "py_load_pubmedcentral.cli._download_archive_worker",
        return_value=TEST_ARCHIVE_PATH,
    )

    # --- 3. Execution: Run the CLI command ---
    runner = CliRunner()
    # We specify a low number of workers since this is a small, local test
    result = runner.invoke(
        app,
        [
            "full-load",
            "--source",
            "s3",
            "--download-workers",
            "1",
            "--parsing-workers",
            "1",
        ],
        catch_exceptions=False  # Set to False to see the full traceback on error
    )

    # --- 4. Verification ---
    print("\n--- CLI RUNNER OUTPUT ---")
    print(f"Exit Code: {result.exit_code}")
    print("\nSTDOUT:")
    print(result.stdout)
    print("--- END CLI RUNNER OUTPUT ---\n")

    assert result.exit_code == 0

    # Verify data in pmc_articles_metadata table
    with test_db_adapter.conn.cursor() as cursor:
        cursor.execute("SELECT pmcid, pmid, title FROM pmc_articles_metadata ORDER BY pmcid;")
        metadata_rows = cursor.fetchall()
        assert len(metadata_rows) == 2, f"Expected 2 metadata rows, but found {len(metadata_rows)}"
        # Corrected assertions
        assert metadata_rows[0] == ("PMC001", 10001, "The Science of Baseline Data")
        assert metadata_rows[1] == ("PMC002", 10002, "An Article to be Retracted")

    # Verify data in pmc_articles_content table
    with test_db_adapter.conn.cursor() as cursor:
        cursor.execute("SELECT pmcid, body_text FROM pmc_articles_content ORDER BY pmcid;")
        content_rows = cursor.fetchall()
        assert len(content_rows) == 2
        assert content_rows[0][0] == "PMC001"
        assert "This is the full text of the first article (PMC001)." in content_rows[0][1]
        assert content_rows[1][0] == "PMC002"
        assert "This is the body of the second article (PMC002)." in content_rows[1][1]
