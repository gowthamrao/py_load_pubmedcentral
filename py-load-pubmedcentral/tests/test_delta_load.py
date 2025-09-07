"""
Integration test for the delta load data pipeline.
"""
from __future__ import annotations

import os
import tarfile
import time
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from py_load_pubmedcentral.cli import app
from py_load_pubmedcentral.models import ArticleFileInfo
from py_load_pubmedcentral.utils import get_db_adapter

# --- Test Setup ---
runner = CliRunner()
TEST_DATA_DIR = Path(__file__).parent
TEST_SCHEMA_PATH = TEST_DATA_DIR.parent / "schemas" / "pmc_schema.sql"

# --- Test Data Paths ---
# We will create these archives and files programmatically in the test setup.
BASELINE_ARCHIVE_PATH = TEST_DATA_DIR / "baseline_archive.tar.gz"
DELTA_ARCHIVE_PATH = TEST_DATA_DIR / "delta_archive.tar.gz"
BASELINE_XML_PATH = TEST_DATA_DIR / "PMC_baseline.xml"
UPDATED_XML_PATH = TEST_DATA_DIR / "PMC_updated.xml"
NEW_XML_PATH = TEST_DATA_DIR / "PMC_new.xml"

# --- XML Content ---
BASELINE_XML_CONTENT = """
<article>
  <front>
    <article-meta>
      <article-id pub-id-type="pmc">PMC001</article-id>
      <title-group><article-title>Baseline Article Title</article-title></title-group>
    </article-meta>
  </front>
  <body><p>This is the original text.</p></body>
</article>
"""

UPDATED_XML_CONTENT = """
<article>
  <front>
    <article-meta>
      <article-id pub-id-type="pmc">PMC001</article-id>
      <title-group><article-title>Updated Article Title</article-title></title-group>
    </article-meta>
  </front>
  <body><p>This is the updated text.</p></body>
</article>
"""

NEW_XML_CONTENT = """
<article>
  <front>
    <article-meta>
      <article-id pub-id-type="pmc">PMC002</article-id>
      <title-group><article-title>New Article Title</article-title></title-group>
    </article-meta>
  </front>
  <body><p>This is a new article.</p></body>
</article>
"""


def create_test_files():
    """Create the XML and tar.gz files needed for the test."""
    BASELINE_XML_PATH.write_text(BASELINE_XML_CONTENT)
    UPDATED_XML_PATH.write_text(UPDATED_XML_CONTENT)
    NEW_XML_PATH.write_text(NEW_XML_CONTENT)

    # Create baseline archive
    with tarfile.open(BASELINE_ARCHIVE_PATH, "w:gz") as tar:
        tar.add(BASELINE_XML_PATH, arcname=BASELINE_XML_PATH.name)

    # Create delta archive (contains both an updated and a new file)
    with tarfile.open(DELTA_ARCHIVE_PATH, "w:gz") as tar:
        tar.add(UPDATED_XML_PATH, arcname=UPDATED_XML_PATH.name)
        tar.add(NEW_XML_PATH, arcname=NEW_XML_PATH.name)


def cleanup_test_files():
    """Remove the files created during the test."""
    for p in [
        BASELINE_ARCHIVE_PATH, DELTA_ARCHIVE_PATH, BASELINE_XML_PATH,
        UPDATED_XML_PATH, NEW_XML_PATH
    ]:
        if p.exists():
            p.unlink()


@pytest.fixture(scope="module")
def db_adapter():
    """Pytest fixture for the database adapter."""
    if not all(os.environ.get(k) for k in ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"]):
        pytest.skip("Database environment variables not set.")
    adapter = get_db_adapter()
    adapter.connect()
    yield adapter
    adapter.close()


def cleanup_db(adapter):
    """Helper to drop tables."""
    with adapter.conn.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS pmc_articles_content, pmc_articles_metadata, sync_history;")
    adapter.conn.commit()


@patch("py_load_pubmedcentral.cli.S3DataSource")
def test_delta_load_pipeline(mock_data_source, db_adapter):
    """
    Tests the delta load pipeline:
    1. Create test data files and archives.
    2. Initialize the schema.
    3. Mock a "full load" run.
    4. Run the "delta-load" command.
    5. Verify that new data is inserted and existing data is updated.
    6. Clean up created files and database tables.
    """
    try:
        # --- 1. Setup Test Data ---
        create_test_files()

        # --- Timestamps for the test ---
        # We need to control time to simulate the delta window.
        time_before_full_load = datetime.utcnow()
        time.sleep(1) # Ensure timestamps are distinct
        full_load_update_ts = datetime.utcnow()
        time.sleep(1)
        delta_load_update_ts = datetime.utcnow()

        # --- 2. Mock Data Source Setup ---
        mock_instance = mock_data_source.return_value

        # Define the article info for baseline and delta
        baseline_info = ArticleFileInfo(
            file_path=str(BASELINE_ARCHIVE_PATH.name),
            pmcid="PMC001",
            pmid=None,
            last_updated=full_load_update_ts
        )
        updated_info = ArticleFileInfo(
            file_path=str(DELTA_ARCHIVE_PATH.name),
            pmcid="PMC001", # Same PMCID as baseline
            pmid=None,
            last_updated=delta_load_update_ts # Newer timestamp
        )
        new_info = ArticleFileInfo(
            file_path=str(DELTA_ARCHIVE_PATH.name),
            pmcid="PMC002", # New PMCID
            pmid=None,
            last_updated=delta_load_update_ts
        )

        # The mock will return different values on subsequent calls
        mock_instance.get_article_file_list.side_effect = [
            [baseline_info],  # First call (full-load)
            [baseline_info, updated_info, new_info], # Second call (delta-load)
        ]

        def mock_download(file_identifier, _):
            if file_identifier == str(BASELINE_ARCHIVE_PATH.name):
                return BASELINE_ARCHIVE_PATH
            if file_identifier == str(DELTA_ARCHIVE_PATH.name):
                return DELTA_ARCHIVE_PATH
            raise FileNotFoundError(file_identifier)

        mock_instance.download_file.side_effect = mock_download

        # --- 3. DB Init and Full Load ---
        cleanup_db(db_adapter)
        init_result = runner.invoke(app, ["initialize", "--schema", str(TEST_SCHEMA_PATH)])
        assert init_result.exit_code == 0

        full_load_result = runner.invoke(app, ["full-load", "--source", "s3"])
        assert full_load_result.exit_code == 0
        assert "Full baseline load process finished successfully" in full_load_result.stdout

        # Verify initial state
        with db_adapter.conn.cursor() as cursor:
            cursor.execute("SELECT title FROM pmc_articles_metadata WHERE pmcid = 'PMC001';")
            assert cursor.fetchone()[0] == "Baseline Article Title"
            cursor.execute("SELECT COUNT(*) FROM pmc_articles_metadata;")
            assert cursor.fetchone()[0] == 1

        # --- 4. Run Delta Load ---
        delta_load_result = runner.invoke(app, ["delta-load", "--source", "s3"])
        assert delta_load_result.exit_code == 0
        assert "Found 2 new/updated articles to process" in delta_load_result.stdout
        assert "Discovered 1 unique archives to process" in delta_load_result.stdout # Both articles are in the same archive
        assert "Database upsert complete" in delta_load_result.stdout

        # --- 5. Verification ---
        with db_adapter.conn.cursor() as cursor:
            # Check that the total count is now 2
            cursor.execute("SELECT COUNT(*) FROM pmc_articles_metadata;")
            assert cursor.fetchone()[0] == 2

            # Check that the baseline article was updated
            cursor.execute("SELECT title, source_last_updated FROM pmc_articles_metadata WHERE pmcid = 'PMC001';")
            record = cursor.fetchone()
            assert record[0] == "Updated Article Title"
            assert record[1].replace(tzinfo=None) > full_load_update_ts

            # Check that the new article was inserted
            cursor.execute("SELECT title FROM pmc_articles_metadata WHERE pmcid = 'PMC002';")
            assert cursor.fetchone()[0] == "New Article Title"

            # Check sync_history
            cursor.execute("SELECT COUNT(*) FROM sync_history WHERE status = 'SUCCESS';")
            assert cursor.fetchone()[0] == 2
            cursor.execute("SELECT COUNT(*) FROM sync_history WHERE run_type = 'DELTA' AND status = 'SUCCESS';")
            assert cursor.fetchone()[0] == 1


    finally:
        # --- 6. Cleanup ---
        cleanup_db(db_adapter)
        cleanup_test_files()
