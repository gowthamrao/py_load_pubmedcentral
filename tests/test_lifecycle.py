"""
Integration test for a complex article lifecycle: update then retract.
"""
from __future__ import annotations

import os
import tarfile
import time
from datetime import datetime, timezone, timedelta
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


# --- Test Data ---

# Article A will be updated and then retracted
ARTICLE_A_V1_XML = """
<article>
    <front>
        <journal-meta></journal-meta>
        <article-meta>
            <article-id pub-id-type="pmc">PMC_LIFECYCLE</article-id>
            <title-group><article-title>Version 1 Title</article-title></title-group>
        </article-meta>
    </front>
    <body><p>This is the first version of the article.</p></body>
</article>
"""

ARTICLE_A_V2_XML = """
<article>
    <front>
        <journal-meta></journal-meta>
        <article-meta>
            <article-id pub-id-type="pmc">PMC_LIFECYCLE</article-id>
            <title-group><article-title>Version 2 Title</article-title></title-group>
        </article-meta>
    </front>
    <body><p>This is the SECOND version of the article with updated content.</p></body>
</article>
"""

# Article B is a control that should not change
ARTICLE_B_XML = """
<article>
    <front>
        <journal-meta></journal-meta>
        <article-meta>
            <article-id pub-id-type="pmc">PMC_CONTROL</article-id>
            <title-group><article-title>Control Article</article-title></title-group>
        </article-meta>
    </front>
    <body><p>This article should not change.</p></body>
</article>
"""


@pytest.fixture(scope="module")
def db_adapter():
    """Pytest fixture for the database adapter."""
    os.environ.setdefault("PMC_DB_HOST", "localhost")
    os.environ.setdefault("PMC_DB_USER", "postgres")
    os.environ.setdefault("PMC_DB_PASSWORD", "postgres")
    os.environ.setdefault("PMC_DB_NAME", "pmc")

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
def test_archives(tmp_path):
    """Creates temporary XML and archive files for the lifecycle test."""
    xml_a_v1 = tmp_path / "a_v1.xml"
    xml_a_v1.write_text(ARTICLE_A_V1_XML)
    xml_b = tmp_path / "b.xml"
    xml_b.write_text(ARTICLE_B_XML)
    archive_v1_path = tmp_path / "archive_v1.tar.gz"
    with tarfile.open(archive_v1_path, "w:gz") as tar:
        tar.add(xml_a_v1, arcname="a.xml")
        tar.add(xml_b, arcname="b.xml")

    xml_a_v2 = tmp_path / "a_v2.xml"
    xml_a_v2.write_text(ARTICLE_A_V2_XML)
    archive_v2_path = tmp_path / "archive_v2.tar.gz"
    with tarfile.open(archive_v2_path, "w:gz") as tar:
        tar.add(xml_a_v2, arcname="a.xml")

    return {"v1": archive_v1_path, "v2": archive_v2_path}


@patch("py_load_pubmedcentral.cli.S3DataSource")
def test_article_update_and_retraction_lifecycle(mock_s3_data_source, db_adapter, test_archives):
    """
    Tests a full article lifecycle:
    1. Initial load.
    2. An update to the article.
    3. A retraction of the article.
    """
    try:
        cleanup_db(db_adapter)
        init_result = runner.invoke(app, ["initialize", "--schema", str(TEST_SCHEMA_PATH)])
        assert init_result.exit_code == 0

        mock_instance = mock_s3_data_source.return_value
        ts_initial = datetime.now(timezone.utc)
        time.sleep(0.01)
        ts_update = datetime.now(timezone.utc)
        time.sleep(0.01)
        ts_retraction = datetime.now(timezone.utc)

        # === STAGE 1: Initial Full Load ===
        initial_articles = [
            ArticleFileInfo(file_path="archive_v1.tar.gz", pmcid="PMC_LIFECYCLE", last_updated=ts_initial),
            ArticleFileInfo(file_path="archive_v1.tar.gz", pmcid="PMC_CONTROL", last_updated=ts_initial),
        ]
        mock_instance.get_article_file_list.return_value = initial_articles
        mock_instance.get_retracted_pmcids.return_value = []
        mock_instance.download_file.return_value = test_archives["v1"]
        full_load_result = runner.invoke(app, ["full-load", "--source", "s3"])
        assert full_load_result.exit_code == 0, full_load_result.stdout

        with db_adapter.conn.cursor() as cursor:
            cursor.execute("SELECT pmcid, title, is_retracted FROM pmc_articles_metadata ORDER BY pmcid;")
            records = cursor.fetchall()
            assert records == [
                ("PMC_CONTROL", "Control Article", False),
                ("PMC_LIFECYCLE", "Version 1 Title", False),
            ]
            cursor.execute("SELECT status FROM sync_history WHERE run_type = 'FULL';")
            run_status = cursor.fetchone()[0]
            assert run_status == "SUCCESS"

        # === STAGE 2: Delta Load with an Update ===
        update_info = IncrementalUpdateInfo(archive_path="archive_v2.tar.gz", file_list_path="filelist_v2.csv", date=ts_update.date())
        articles_for_update = [ArticleFileInfo(file_path="archive_v2.tar.gz", pmcid="PMC_LIFECYCLE", last_updated=ts_update)]

        # === STAGE 3: Delta Load with a Retraction ===
        retraction_info = IncrementalUpdateInfo(archive_path="archive_v2.tar.gz", file_list_path="filelist_v3.csv", date=ts_retraction.date())
        articles_for_retraction = [ArticleFileInfo(file_path="archive_v2.tar.gz", pmcid="PMC_LIFECYCLE", last_updated=ts_retraction, is_retracted=True)]

        mock_instance.get_incremental_updates.side_effect = [[update_info], [retraction_info]]
        mock_instance.stream_article_infos_from_file_list.side_effect = [articles_for_update, articles_for_retraction]
        mock_instance.download_file.side_effect = [test_archives["v2"], test_archives["v2"]] # download archive for update, and for retraction
        mock_instance.get_retracted_pmcids.return_value = []

        # Run Stage 2
        delta_result_1 = runner.invoke(app, ["delta-load", "--source", "s3"])
        assert delta_result_1.exit_code == 0, delta_result_1.stdout

        with db_adapter.conn.cursor() as cursor:
            cursor.execute("SELECT pmcid, title, is_retracted FROM pmc_articles_metadata ORDER BY pmcid;")
            records = cursor.fetchall()
            assert records == [
                ("PMC_CONTROL", "Control Article", False),
                ("PMC_LIFECYCLE", "Version 2 Title", False),
            ]
            cursor.execute("SELECT body_text FROM pmc_articles_content WHERE pmcid = 'PMC_LIFECYCLE';")
            body = cursor.fetchone()[0]
            assert "SECOND version" in body
            cursor.execute("SELECT status, metrics FROM sync_history WHERE run_type = 'DELTA' ORDER BY start_time DESC LIMIT 1;")
            run_status, metrics = cursor.fetchone()
            assert run_status == "SUCCESS"
            assert metrics["total_articles_upserted"] == 1

        # Run Stage 3
        delta_result_2 = runner.invoke(app, ["delta-load", "--source", "s3"])
        assert delta_result_2.exit_code == 0, delta_result_2.stdout

        with db_adapter.conn.cursor() as cursor:
            cursor.execute("SELECT pmcid, title, is_retracted FROM pmc_articles_metadata ORDER BY pmcid;")
            records = cursor.fetchall()
            assert records == [
                ("PMC_CONTROL", "Control Article", False),
                ("PMC_LIFECYCLE", "Version 2 Title", True),
            ]
            cursor.execute("SELECT status, metrics FROM sync_history WHERE run_type = 'DELTA' ORDER BY start_time DESC LIMIT 1;")
            run_status, metrics = cursor.fetchone()
            assert run_status == "SUCCESS"
            assert metrics["total_articles_retracted"] == 1
    finally:
        cleanup_db(db_adapter)
