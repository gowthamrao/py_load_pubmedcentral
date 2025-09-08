"""
Integration test for the NEW delta load data pipeline.
"""
from __future__ import annotations

import os
import tarfile
import time
import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from typer.testing import CliRunner

from py_load_pubmedcentral.cli import app
from py_load_pubmedcentral.models import ArticleFileInfo
from py_load_pubmedcentral.acquisition import IncrementalUpdateInfo
from py_load_pubmedcentral.utils import get_db_adapter

# --- Test Setup ---
runner = CliRunner()
TEST_DATA_DIR = Path(__file__).parent
TEST_SCHEMA_PATH = TEST_DATA_DIR.parent / "schemas" / "pmc_schema.sql"

# --- Test Data Paths ---
BASELINE_ARCHIVE_PATH = TEST_DATA_DIR / "baseline_archive.tar.gz"
DELTA_ARCHIVE_PATH = TEST_DATA_DIR / "delta_archive.tar.gz"
BASELINE_FILE_LIST_PATH = TEST_DATA_DIR / "baseline_file_list.csv"
DELTA_FILE_LIST_PATH = TEST_DATA_DIR / "delta_file_list.csv"

# --- XML Content ---
XML_CONTENTS = {
    "PMC001_baseline.xml": """
<article><front><article-meta><article-id pub-id-type="pmc">PMC001</article-id><title-group><article-title>Baseline Article</article-title></title-group></article-meta></front><body><p>Baseline.</p></body></article>""",
    "PMC001_updated.xml": """
<article><front><article-meta><article-id pub-id-type="pmc">PMC001</article-id><title-group><article-title>Updated Article</article-title></title-group></article-meta></front><body><p>Updated.</p></body></article>""",
    "PMC002_new.xml": """
<article><front><article-meta><article-id pub-id-type="pmc">PMC002</article-id><title-group><article-title>New Article</article-title></title-group></article-meta></front><body><p>New.</p></body></article>""",
    "PMC003_retracted.xml": """
<article><front><article-meta><article-id pub-id-type="pmc">PMC003</article-id><title-group><article-title>Retracted Article</article-title></title-group></article-meta></front><body><p>Retracted.</p></body></article>""",
}


def create_test_data(tmp_path: Path):
    """Create all necessary files for the test in a temporary directory."""
    xml_paths = {}
    for name, content in XML_CONTENTS.items():
        p = tmp_path / name
        p.write_text(content)
        xml_paths[name] = p

    # Create baseline file list and archive
    with open(BASELINE_FILE_LIST_PATH, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["File", "Accession ID", "Last Updated"])
        writer.writerow([f"{BASELINE_ARCHIVE_PATH.name}/{xml_paths['PMC001_baseline.xml'].name}", "PMC001", "2025-01-01 10:00:00"])
        writer.writerow([f"{BASELINE_ARCHIVE_PATH.name}/{xml_paths['PMC003_retracted.xml'].name}", "PMC003", "2025-01-01 10:00:00"])

    with tarfile.open(BASELINE_ARCHIVE_PATH, "w:gz") as tar:
        tar.add(xml_paths["PMC001_baseline.xml"], arcname=xml_paths["PMC001_baseline.xml"].name)
        tar.add(xml_paths["PMC003_retracted.xml"], arcname=xml_paths["PMC003_retracted.xml"].name)


    # Create delta file list and archive
    with open(DELTA_FILE_LIST_PATH, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["File", "Accession ID", "Last Updated", "Retracted"])
        writer.writerow([f"{DELTA_ARCHIVE_PATH.name}/{xml_paths['PMC001_updated.xml'].name}", "PMC001", "2025-09-07 12:00:00", "false"])
        writer.writerow([f"{DELTA_ARCHIVE_PATH.name}/{xml_paths['PMC002_new.xml'].name}", "PMC002", "2025-09-07 12:00:00", "false"])
        writer.writerow([f"{DELTA_ARCHIVE_PATH.name}/{xml_paths['PMC003_retracted.xml'].name}", "PMC003", "2025-09-07 12:00:00", "true"])

    with tarfile.open(DELTA_ARCHIVE_PATH, "w:gz") as tar:
        tar.add(xml_paths["PMC001_updated.xml"], arcname=xml_paths["PMC001_updated.xml"].name)
        tar.add(xml_paths["PMC002_new.xml"], arcname=xml_paths["PMC002_new.xml"].name)
        tar.add(xml_paths["PMC003_retracted.xml"], arcname=xml_paths["PMC003_retracted.xml"].name)


def cleanup_test_files():
    """Remove the files created during the test."""
    for p in [BASELINE_ARCHIVE_PATH, DELTA_ARCHIVE_PATH, BASELINE_FILE_LIST_PATH, DELTA_FILE_LIST_PATH]:
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
        cursor.execute("DROP TABLE IF EXISTS pmc_articles_content, pmc_articles_metadata, sync_history CASCADE;")
    adapter.conn.commit()


@patch("py_load_pubmedcentral.cli.get_db_adapter")
@patch("py_load_pubmedcentral.cli.S3DataSource")
def test_delta_load_pipeline(mock_s3_data_source, mock_get_db_adapter, db_adapter, tmp_path):
    """
    Tests the refactored delta load pipeline.
    """
    # --- Setup ---
    cleanup_db(db_adapter)
    create_test_data(tmp_path)
    mock_get_db_adapter.return_value = db_adapter # Use the real db adapter for the test

    # --- Mock DataSource ---
    mock_source_instance = MagicMock()

    # Mocks for the initial `full-load`
    baseline_infos = list(db_adapter.stream_article_infos_from_file_list(str(BASELINE_FILE_LIST_PATH)))
    mock_source_instance.get_article_file_list.return_value = baseline_infos

    # Mocks for the `delta-load`
    update_date = datetime(2025, 9, 7, tzinfo=timezone.utc)
    mock_source_instance.get_incremental_updates.return_value = [
        IncrementalUpdateInfo(
            archive_path=str(DELTA_ARCHIVE_PATH),
            file_list_path=str(DELTA_FILE_LIST_PATH),
            date=update_date
        )
    ]
    # The delta command also checks the master retraction list
    mock_source_instance.get_retracted_pmcids.return_value = ["PMC003"]
    # The delta command streams from the daily file list
    delta_infos = list(db_adapter.stream_article_infos_from_file_list(str(DELTA_FILE_LIST_PATH)))
    mock_source_instance.stream_article_infos_from_file_list.return_value = delta_infos

    def mock_download(file_identifier, _):
        if file_identifier == str(BASELINE_ARCHIVE_PATH):
            return BASELINE_ARCHIVE_PATH
        if file_identifier == str(DELTA_ARCHIVE_PATH):
            return DELTA_ARCHIVE_PATH
        raise FileNotFoundError(f"File not found in mock: {file_identifier}")
    mock_source_instance.download_file.side_effect = mock_download

    mock_s3_data_source.return_value = mock_source_instance


    try:
        # --- 1. Initialize DB ---
        init_result = runner.invoke(app, ["initialize", "--schema", str(TEST_SCHEMA_PATH)])
        assert init_result.exit_code == 0, init_result.stdout

        # --- 2. Run Full Load ---
        # We need to run a full load to have a "last successful run" timestamp.
        full_load_result = runner.invoke(app, ["full-load", "--source", "s3", "--download-workers=1", "--parsing-workers=1"])
        assert full_load_result.exit_code == 0, full_load_result.stdout

        # Verify initial state (PMC001 and PMC003 are loaded)
        with db_adapter.conn.cursor() as cursor:
            cursor.execute("SELECT title, is_retracted FROM pmc_articles_metadata WHERE pmcid = 'PMC001';")
            assert cursor.fetchone() == ("Baseline Article", False)
            cursor.execute("SELECT title, is_retracted FROM pmc_articles_metadata WHERE pmcid = 'PMC003';")
            assert cursor.fetchone() == ("Retracted Article", False) # Not retracted yet
            cursor.execute("SELECT COUNT(*) FROM pmc_articles_metadata;")
            assert cursor.fetchone()[0] == 2

        # --- 3. Run Delta Load ---
        delta_load_result = runner.invoke(app, ["delta-load", "--source", "s3"])
        assert delta_load_result.exit_code == 0, delta_load_result.stdout
        assert "Found 1 incremental archives to process" in delta_load_result.stdout
        assert "Upserting TSV files" in delta_load_result.stdout
        assert "Marked 1 articles as retracted" in delta_load_result.stdout

        # --- 4. Verification ---
        with db_adapter.conn.cursor() as cursor:
            # Total count is now 3 (PMC001, PMC002, PMC003)
            cursor.execute("SELECT COUNT(*) FROM pmc_articles_metadata;")
            assert cursor.fetchone()[0] == 3

            # Check that PMC001 was updated
            cursor.execute("SELECT title FROM pmc_articles_metadata WHERE pmcid = 'PMC001';")
            assert cursor.fetchone()[0] == "Updated Article"

            # Check that PMC002 was inserted
            cursor.execute("SELECT title FROM pmc_articles_metadata WHERE pmcid = 'PMC002';")
            assert cursor.fetchone()[0] == "New Article"

            # Check that PMC003 was marked as retracted
            cursor.execute("SELECT is_retracted FROM pmc_articles_metadata WHERE pmcid = 'PMC003';")
            assert cursor.fetchone()[0] is True

            # Check sync_history
            cursor.execute("SELECT COUNT(*) FROM sync_history WHERE status = 'SUCCESS';")
            assert cursor.fetchone()[0] == 2
            cursor.execute("SELECT run_type FROM sync_history WHERE status = 'SUCCESS' ORDER BY end_time DESC LIMIT 1;")
            assert cursor.fetchone()[0] == "DELTA"

    finally:
        # --- 5. Cleanup ---
        cleanup_test_files()
        cleanup_db(db_adapter)
