"""
Integration tests for the DatabaseAdapter and its data transformation logic.
"""
from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple

import pytest

from py_load_pubmedcentral.db import PostgreSQLAdapter
from py_load_pubmedcentral.models import PmcArticlesContent, PmcArticlesMetadata
from py_load_pubmedcentral.parser import parse_jats_xml

# Path to the test data file, relative to this test file's location.
TEST_XML_PATH = Path(__file__).parent / "test.xml"


def test_validate_schema_success(db_with_schema: PostgreSQLAdapter):
    """Tests that schema validation passes when the schema is correct."""
    db_with_schema.validate_schema()  # Should not raise an exception


def test_validate_schema_missing_table(db_with_schema: PostgreSQLAdapter):
    """Tests that schema validation fails if a table is missing."""
    # The CASCADE is necessary because pmc_articles_content has a foreign key
    # constraint that depends on pmc_articles_metadata.
    db_with_schema.execute_sql("DROP TABLE pmc_articles_metadata CASCADE;")
    with pytest.raises(RuntimeError, match=r"Table 'pmc_articles_metadata' does not exist"):
        db_with_schema.validate_schema()


def test_validate_schema_missing_column(db_with_schema: PostgreSQLAdapter):
    """Tests that schema validation fails if a column is missing."""
    db_with_schema.execute_sql("ALTER TABLE pmc_articles_metadata DROP COLUMN is_retracted;")
    with pytest.raises(RuntimeError, match=r"Column 'is_retracted' does not exist"):
        db_with_schema.validate_schema()


def test_start_and_end_run(db_with_schema: PostgreSQLAdapter):
    """Tests starting and ending a run in the sync_history table."""
    # Start the run
    run_id = db_with_schema.start_run(run_type="FULL")
    assert isinstance(run_id, int)

    # Check that the run is in progress
    with db_with_schema.conn.cursor() as cursor:
        cursor.execute("SELECT status FROM sync_history WHERE run_id = %s;", (run_id,))
        status = cursor.fetchone()[0]
        assert status == "RUNNING"

    # End the run successfully
    metrics = {"articles_processed": 100}
    db_with_schema.end_run(run_id, "SUCCESS", metrics, "last_file.xml")

    # Check the final state
    with db_with_schema.conn.cursor() as cursor:
        cursor.execute(
            "SELECT status, metrics, last_file_processed FROM sync_history WHERE run_id = %s;",
            (run_id,),
        )
        status, final_metrics, last_file = cursor.fetchone()
        assert status == "SUCCESS"
        assert final_metrics == metrics
        assert last_file == "last_file.xml"


def test_get_last_successful_run_info(db_with_schema: PostgreSQLAdapter):
    """Tests retrieving the last successful run's information."""
    # Should be None at first
    assert db_with_schema.get_last_successful_run_info("DELTA") is None

    # Add a failed run and a successful run
    now = datetime.now(timezone.utc)
    # Use a slightly older timestamp for the desired record to ensure ORDER BY works
    successful_run_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    db_with_schema.execute_sql(
        f"""
        INSERT INTO sync_history (run_type, status, start_time, end_time, last_file_processed)
        VALUES
            ('DELTA', 'SUCCESS', '{now.isoformat()}', '{now.isoformat()}', 'file2.xml'),
            ('DELTA', 'FAILED', '{now.isoformat()}', '{now.isoformat()}', 'file1.xml'),
            ('DELTA', 'SUCCESS', '{successful_run_time.isoformat()}', '{successful_run_time.isoformat()}', 'correct_file.xml'),
            ('FULL', 'SUCCESS', '{now.isoformat()}', '{now.isoformat()}', 'file3.xml');
    """
    )

    # Get the last successful DELTA run (should be the one with the latest end_time)
    end_time, last_file = db_with_schema.get_last_successful_run_info("DELTA")
    assert last_file == "file2.xml"
    assert end_time.date() == now.date()


def test_handle_deletions(db_with_schema: PostgreSQLAdapter):
    """Tests marking articles as retracted."""
    # Insert some dummy data
    db_with_schema.execute_sql(
        f"""
        INSERT INTO pmc_articles_metadata (pmcid, is_retracted, sync_timestamp)
        VALUES
            ('PMC1', FALSE, '{datetime.now(timezone.utc).isoformat()}'),
            ('PMC2', FALSE, '{datetime.now(timezone.utc).isoformat()}'),
            ('PMC3', TRUE, '{datetime.now(timezone.utc).isoformat()}');
    """
    )

    # Retract PMC1 and PMC2. PMC3 is already retracted, so it shouldn't be counted.
    updated_count = db_with_schema.handle_deletions(["PMC1", "PMC2", "PMC3"])
    assert updated_count == 2

    with db_with_schema.conn.cursor() as cursor:
        cursor.execute("SELECT pmcid, is_retracted FROM pmc_articles_metadata ORDER BY pmcid;")
        results = cursor.fetchall()

    assert results == [("PMC1", True), ("PMC2", True), ("PMC3", True)]


def test_handle_deletions_no_input(db_with_schema: PostgreSQLAdapter):
    """Tests that handle_deletions does nothing with an empty list."""
    updated_count = db_with_schema.handle_deletions([])
    assert updated_count == 0


def _get_test_models() -> List[Tuple[PmcArticlesMetadata, PmcArticlesContent]]:
    """Helper function to parse the test XML and return a list of models."""
    with open(TEST_XML_PATH, "rb") as f:
        return list(parse_jats_xml(f))


def test_writing_models_to_tsv_file():
    """
    Tests that Pydantic models are correctly written to a TSV file
    in a format suitable for PostgreSQL's COPY command.
    """
    all_models = _get_test_models()
    metadata_models = [m[0] for m in all_models]
    content_models = [m[1] for m in all_models]

    # Don't need a real connection for this test
    adapter = PostgreSQLAdapter(connection_params={})

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        metadata_tsv_path = tmp_path / "metadata.tsv"
        content_tsv_path = tmp_path / "content.tsv"

        # Test Metadata to TSV File
        metadata_columns = list(PmcArticlesMetadata.model_fields.keys())
        with open(metadata_tsv_path, "w", encoding="utf-8") as f:
            adapter.write_models_to_tsv_file(metadata_models, metadata_columns, f)

        with open(metadata_tsv_path, "r", encoding="utf-8") as f:
            metadata_lines = f.read().strip().split("\n")

        assert len(metadata_lines) == 2
        line1_cols = metadata_lines[0].split("\t")
        assert line1_cols[0] == "PMC12345"
        journal_info = json.loads(line1_cols[6])
        assert journal_info["name"] == "Journal of Test Medicine"
        line2_cols = metadata_lines[1].split("\t")
        assert line2_cols[1] == r"\N"  # Check for NULL value

        # Test Content to TSV File
        content_columns = list(PmcArticlesContent.model_fields.keys())
        with open(content_tsv_path, "w", encoding="utf-8") as f:
            adapter.write_models_to_tsv_file(content_models, content_columns, f)

        with open(content_tsv_path, "r", encoding="utf-8") as f:
            content_lines = f.read().strip().split("\n")

        assert len(content_lines) == 2
        content1_cols = content_lines[0].split("\t")
        assert "<article" in content1_cols[1]
