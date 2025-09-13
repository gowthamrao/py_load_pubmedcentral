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


def test_bulk_load_native(db_with_schema: PostgreSQLAdapter):
    """Tests the native bulk loading of a TSV file."""
    all_models = _get_test_models()
    metadata_models = [m[0] for m in all_models]

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        metadata_tsv_path = tmp_path / "metadata.tsv"

        metadata_columns = list(PmcArticlesMetadata.model_fields.keys())
        with open(metadata_tsv_path, "w", encoding="utf-8") as f:
            db_with_schema.write_models_to_tsv_file(metadata_models, metadata_columns, f)

        db_with_schema.bulk_load_native(str(metadata_tsv_path), "pmc_articles_metadata")

        with db_with_schema.conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM pmc_articles_metadata;")
            count = cursor.fetchone()[0]
            assert count == 2


def test_bulk_upsert_and_update_state(db_with_schema: PostgreSQLAdapter):
    """Tests the bulk_upsert_and_update_state function."""
    all_models = _get_test_models()
    metadata_models = [m[0] for m in all_models]
    content_models = [m[1] for m in all_models]

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        metadata_tsv_path = tmp_path / "metadata.tsv"
        content_tsv_path = tmp_path / "content.tsv"

        metadata_columns = list(PmcArticlesMetadata.model_fields.keys())
        with open(metadata_tsv_path, "w", encoding="utf-8") as f:
            db_with_schema.write_models_to_tsv_file(metadata_models, metadata_columns, f)

        content_columns = list(PmcArticlesContent.model_fields.keys())
        with open(content_tsv_path, "w", encoding="utf-8") as f:
            db_with_schema.write_models_to_tsv_file(content_models, content_columns, f)

        run_id = db_with_schema.start_run(run_type="DELTA")
        db_with_schema.bulk_upsert_and_update_state(
            run_id, str(metadata_tsv_path), str(content_tsv_path), "file1.xml"
        )

        with db_with_schema.conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM pmc_articles_metadata;")
            count = cursor.fetchone()[0]
            assert count == 2

            cursor.execute("SELECT last_file_processed FROM sync_history WHERE run_id = %s;", (run_id,))
            last_file = cursor.fetchone()[0]
            assert last_file == "file1.xml"


def test_bulk_insert_from_files_for_full_load(db_with_schema: PostgreSQLAdapter):
    """Tests the bulk_insert_from_files_for_full_load function."""
    all_models1 = _get_test_models()
    metadata_models1 = [m[0] for m in all_models1]
    content_models1 = [m[1] for m in all_models1]

    # Create a second set of models with different pmcids
    all_models2 = _get_test_models()
    metadata_models2 = [m[0] for m in all_models2]
    content_models2 = [m[1] for m in all_models2]
    for m in metadata_models2:
        m.pmcid = f"{m.pmcid}_2"
    for m in content_models2:
        m.pmcid = f"{m.pmcid}_2"


    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        metadata_tsv_path1 = tmp_path / "metadata1.tsv"
        content_tsv_path1 = tmp_path / "content1.tsv"
        metadata_tsv_path2 = tmp_path / "metadata2.tsv"
        content_tsv_path2 = tmp_path / "content2.tsv"

        metadata_columns = list(PmcArticlesMetadata.model_fields.keys())
        with open(metadata_tsv_path1, "w", encoding="utf-8") as f:
            db_with_schema.write_models_to_tsv_file(metadata_models1, metadata_columns, f)
        with open(metadata_tsv_path2, "w", encoding="utf-8") as f:
            db_with_schema.write_models_to_tsv_file(metadata_models2, metadata_columns, f)

        content_columns = list(PmcArticlesContent.model_fields.keys())
        with open(content_tsv_path1, "w", encoding="utf-8") as f:
            db_with_schema.write_models_to_tsv_file(content_models1, content_columns, f)
        with open(content_tsv_path2, "w", encoding="utf-8") as f:
            db_with_schema.write_models_to_tsv_file(content_models2, content_columns, f)

        db_with_schema.bulk_insert_from_files_for_full_load(
            [str(metadata_tsv_path1), str(metadata_tsv_path2)],
            [str(content_tsv_path1), str(content_tsv_path2)],
        )

        with db_with_schema.conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM pmc_articles_metadata;")
            count = cursor.fetchone()[0]
            assert count == 4


def test_close(test_db_adapter: PostgreSQLAdapter):
    """Tests that the close method closes the connection."""
    # Create a new adapter instance to avoid closing the shared connection
    adapter = PostgreSQLAdapter(connection_params=test_db_adapter.connection_params)
    adapter.connect()
    assert adapter.conn is not None
    adapter.close()
    assert adapter.conn is None


def test_end_run_no_last_file(db_with_schema: PostgreSQLAdapter):
    """Tests ending a run without providing a last_file_processed."""
    run_id = db_with_schema.start_run(run_type="DELTA")
    db_with_schema.end_run(run_id, "SUCCESS", {"articles": 0})

    with db_with_schema.conn.cursor() as cursor:
        cursor.execute(
            "SELECT status, last_file_processed FROM sync_history WHERE run_id = %s;",
            (run_id,),
        )
        status, last_file = cursor.fetchone()
        assert status == "SUCCESS"
        assert last_file is None


def test_update_run_state(db_with_schema: PostgreSQLAdapter):
    """Tests the update_run_state function."""
    run_id = db_with_schema.start_run(run_type="DELTA")
    db_with_schema.update_run_state(run_id, "file1.xml")

    with db_with_schema.conn.cursor() as cursor:
        cursor.execute("SELECT last_file_processed FROM sync_history WHERE run_id = %s;", (run_id,))
        last_file = cursor.fetchone()[0]
        assert last_file == "file1.xml"


def test_bulk_upsert_articles_full_load(db_with_schema: PostgreSQLAdapter):
    """Tests the bulk_upsert_articles function with is_full_load=True."""
    all_models = _get_test_models()
    metadata_models = [m[0] for m in all_models]
    content_models = [m[1] for m in all_models]

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        metadata_tsv_path = tmp_path / "metadata.tsv"
        content_tsv_path = tmp_path / "content.tsv"

        metadata_columns = list(PmcArticlesMetadata.model_fields.keys())
        with open(metadata_tsv_path, "w", encoding="utf-8") as f:
            db_with_schema.write_models_to_tsv_file(metadata_models, metadata_columns, f)

        content_columns = list(PmcArticlesContent.model_fields.keys())
        with open(content_tsv_path, "w", encoding="utf-8") as f:
            db_with_schema.write_models_to_tsv_file(content_models, content_columns, f)

        db_with_schema.bulk_upsert_articles(
            str(metadata_tsv_path), str(content_tsv_path), is_full_load=True
        )

        with db_with_schema.conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM pmc_articles_metadata;")
            count = cursor.fetchone()[0]
            assert count == 2
