"""
Integration test for the retraction handling in the delta load pipeline.
"""
from __future__ import annotations

import os
import tarfile
import time
from datetime import datetime
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

# --- Test Data ---
ARTICLE_TO_KEEP_XML = """
<article><front><article-meta><article-id pub-id-type="pmc">PMC_KEEP</article-id><title-group><article-title>To Keep</article-title></title-group></article-meta></front></article>
"""
ARTICLE_TO_RETRACT_XML = """
<article><front><article-meta><article-id pub-id-type="pmc">PMC_RETRACT</article-id><title-group><article-title>To Retract</article-title></title-group></article-meta></front></article>
"""

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

@pytest.fixture
def test_files(tmp_path):
    """Creates temporary XML and archive files for a test run."""
    xml_keep_path = tmp_path / "keep.xml"
    xml_retract_path = tmp_path / "retract.xml"
    xml_keep_path.write_text(ARTICLE_TO_KEEP_XML)
    xml_retract_path.write_text(ARTICLE_TO_RETRACT_XML)

    archive_path = tmp_path / "test_archive.tar.gz"
    with tarfile.open(archive_path, "w:gz") as tar:
        tar.add(xml_keep_path, arcname=xml_keep_path.name)
        tar.add(xml_retract_path, arcname=xml_retract_path.name)

    return {"archive": archive_path}

@patch("py_load_pubmedcentral.cli.S3DataSource")
def test_retraction_pipeline(mock_s3_data_source, db_adapter, test_files):
    """
    Tests that the delta-load command correctly handles retracted articles.
    """
    try:
        # 1. Initialize DB
        cleanup_db(db_adapter)
        init_result = runner.invoke(app, ["initialize", "--schema", str(TEST_SCHEMA_PATH)])
        assert init_result.exit_code == 0

        # 2. Mock DataSource for both full and delta loads
        mock_instance = mock_s3_data_source.return_value

        full_load_timestamp = datetime.utcnow()
        time.sleep(1) # Ensure timestamps are distinct
        delta_load_timestamp = datetime.utcnow()

        # Define article lists for the two runs
        initial_articles = [
            ArticleFileInfo(file_path="test_archive.tar.gz", pmcid="PMC_KEEP", last_updated=full_load_timestamp),
            ArticleFileInfo(file_path="test_archive.tar.gz", pmcid="PMC_RETRACT", last_updated=full_load_timestamp, is_retracted=False),
        ]
        delta_articles_list = [
            ArticleFileInfo(file_path="test_archive.tar.gz", pmcid="PMC_KEEP", last_updated=full_load_timestamp), # Unchanged
            ArticleFileInfo(file_path="test_archive.tar.gz", pmcid="PMC_RETRACT", last_updated=delta_load_timestamp, is_retracted=True), # Now retracted
        ]

        # Configure side effects for sequential calls
        mock_instance.get_article_file_list.side_effect = [initial_articles, delta_articles_list]
        mock_instance.get_retracted_pmcids.side_effect = [[], ["PMC_RETRACT_FILE"]] # First empty, then has one
        mock_instance.download_file.return_value = test_files["archive"]

        # 3. Run the initial full load
        full_load_result = runner.invoke(app, ["full-load", "--source", "s3"])
        assert full_load_result.exit_code == 0
        assert "Full baseline load process finished successfully" in full_load_result.stdout

        # Verify initial state: both articles exist and are not retracted
        with db_adapter.conn.cursor() as cursor:
            cursor.execute("SELECT pmcid, is_retracted FROM pmc_articles_metadata ORDER BY pmcid;")
            records = cursor.fetchall()
            assert records == [("PMC_KEEP", False), ("PMC_RETRACT", False)]

        # 4. Run the delta load
        delta_result = runner.invoke(app, ["delta-load", "--source", "s3"])
        assert delta_result.exit_code == 0

        # Assert CLI output
        assert "Found 2 unique PMCIDs marked for retraction" in delta_result.stdout
        # The DB only contains PMC_RETRACT, so only 1 row is actually updated
        assert "Marked 1 articles as retracted" in delta_result.stdout

        # 5. Verify final state
        with db_adapter.conn.cursor() as cursor:
            cursor.execute("SELECT pmcid, is_retracted FROM pmc_articles_metadata ORDER BY pmcid;")
            final_records = cursor.fetchall()
            assert ("PMC_KEEP", False) in final_records
            assert ("PMC_RETRACT", True) in final_records

            # Verify metrics in sync_history table
            cursor.execute("SELECT metrics FROM sync_history WHERE run_type = 'DELTA';")
            metrics = cursor.fetchone()[0]
            assert metrics["total_articles_retracted"] == 1
            # The retracted article was also "updated", so it should not be in the upsert count.
            assert metrics["total_articles_upserted"] == 0

    finally:
        # Cleanup
        cleanup_db(db_adapter)
