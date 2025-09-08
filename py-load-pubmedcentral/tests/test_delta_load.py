"""
Integration test for the delta-load command.
"""
import io
import tarfile
import textwrap
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List

from typer.testing import CliRunner

from py_load_pubmedcentral.acquisition import IncrementalUpdateInfo, S3DataSource
from py_load_pubmedcentral.cli import app
from py_load_pubmedcentral.config import settings
from py_load_pubmedcentral.db import PostgreSQLAdapter
from py_load_pubmedcentral.models import (
    ArticleFileInfo,
    Contributor,
    JournalInfo,
    PmcArticlesContent,
    PmcArticlesMetadata,
)

# A sample JATS XML template we can use to generate test files.
XML_TEMPLATE = textwrap.dedent(
    """\
    <article dtd-version="1.1" xmlns:xlink="http://www.w3.org/1999/xlink">
      <front>
        <journal-meta>
          <journal-id journal-id-type="nlm-ta">Test J</journal-id>
          <journal-title-group>
            <journal-title>Test Journal</journal-title>
          </journal-title-group>
          <issn pub-type="epub">0000-0000</issn>
          <publisher>
            <publisher-name>Test Publisher</publisher-name>
          </publisher>
        </journal-meta>
        <article-meta>
          <article-id pub-id-type="pmc">{pmcid}</article-id>
          <article-id pub-id-type="pmid">{pmid}</article-id>
          <article-id pub-id-type="doi">{doi}</article-id>
          <title-group>
            <article-title>{title}</article-title>
          </title-group>
          <pub-date pub-type="epub" date-type="pub">
            <day>01</day>
            <month>01</month>
            <year>2024</year>
          </pub-date>
          <abstract><p>This is an abstract.</p></abstract>
        </article-meta>
      </front>
      <body>
        <p>This is the body text.</p>
      </body>
    </article>
    """
)


def _create_fake_tar_gz(
    tmp_path: Path, archive_name: str, articles: List[dict]
) -> Path:
    """Helper to create a .tar.gz file with specified XML contents."""
    archive_path = tmp_path / archive_name
    with tarfile.open(archive_path, "w:gz") as tar:
        for article in articles:
            xml_content = XML_TEMPLATE.format(**article).encode("utf-8")
            tarinfo = tarfile.TarInfo(name=f"{article['pmcid']}.xml")
            tarinfo.size = len(xml_content)
            tar.addfile(tarinfo, io.BytesIO(xml_content))
    return archive_path


def _setup_initial_db_state(db_adapter: PostgreSQLAdapter, tmp_path: Path):
    """Sets up the DB with a full load record and two initial articles."""
    # 1. Initialize schema
    schema_path = Path("schemas/pmc_schema.sql")
    with open(schema_path, "r", encoding="utf-8") as f:
        db_adapter.execute_sql(f.read())

    # 2. Add a successful FULL run to history
    full_load_time = datetime.now(timezone.utc) - timedelta(days=2)
    with db_adapter.conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO sync_history (run_type, start_time, end_time, status)
            VALUES ('FULL', %s, %s, 'SUCCESS');
            """,
            (full_load_time, full_load_time),
        )
        db_adapter.conn.commit()

    # 3. Add two articles to the database using the adapter's bulk loading
    #    to better simulate the actual application flow.
    initial_metadata = [
        PmcArticlesMetadata(
            pmcid="PMC01", pmid=111, doi="10.0001/test.1", title="Initial Article 1",
            source_last_updated=full_load_time - timedelta(days=1),
            sync_timestamp=datetime.now(timezone.utc),
            publication_date=datetime(2024, 1, 1).date(),
            is_retracted=False,
            journal_info=JournalInfo(name="Test Journal"),
            contributors=[Contributor(name="John Doe")]
        ),
        PmcArticlesMetadata(
            pmcid="PMC02", pmid=222, doi="10.0001/test.2", title="Initial Article 2",
            source_last_updated=full_load_time - timedelta(days=1),
            sync_timestamp=datetime.now(timezone.utc),
            publication_date=datetime(2024, 1, 1).date(),
            is_retracted=False,
            journal_info=JournalInfo(name="Test Journal"),
            contributors=[Contributor(name="Jane Doe")]
        ),
    ]
    initial_content = [
        PmcArticlesContent(pmcid="PMC01", body_text="Old body text", raw_jats_xml="<article/>"),
        PmcArticlesContent(pmcid="PMC02", body_text="Old body text", raw_jats_xml="<article/>"),
    ]

    meta_tsv_path = tmp_path / "init_meta.tsv"
    content_tsv_path = tmp_path / "init_content.tsv"
    with open(meta_tsv_path, "w", encoding="utf-8") as f:
        db_adapter.write_models_to_tsv_file(initial_metadata, list(PmcArticlesMetadata.model_fields.keys()), f)
    with open(content_tsv_path, "w", encoding="utf-8") as f:
        db_adapter.write_models_to_tsv_file(initial_content, list(PmcArticlesContent.model_fields.keys()), f)

    db_adapter.bulk_load_native(str(meta_tsv_path), "pmc_articles_metadata")
    db_adapter.bulk_load_native(str(content_tsv_path), "pmc_articles_content")


def test_delta_load_pipeline(postgresql, mocker, tmp_path):
    """
    Tests the end-to-end delta-load pipeline.
    Scenario:
    1. DB has 2 articles (PMC01, PMC02) from a previous full load.
    2. Master retraction list retracts PMC02.
    3. A daily update package arrives, updating PMC01 and adding PMC03.
    4. A second daily update package arrives, which contains a retraction for PMC03.
    """
    # --- 1. Setup ---
    runner = CliRunner()

    # Mock the global settings to point to the test database
    mocker.patch.object(settings, "db_host", postgresql.info.host)
    mocker.patch.object(settings, "db_port", postgresql.info.port)
    mocker.patch.object(settings, "db_user", postgresql.info.user)
    mocker.patch.object(settings, "db_password", postgresql.info.password)
    mocker.patch.object(settings, "db_name", postgresql.info.dbname)

    # Now, any call to get_db_adapter() will use the test DB
    db_params = {
        "host": settings.db_host,
        "port": settings.db_port,
        "user": settings.db_user,
        "password": settings.db_password,
        "dbname": settings.db_name,
    }
    db_adapter = PostgreSQLAdapter(db_params)
    db_adapter.connect()

    _setup_initial_db_state(db_adapter, tmp_path)

    # --- 2. Mock S3 Data Source ---
    archive_name_1 = f"test_delta_1.incr.{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.tar.gz"
    archive_name_2 = f"test_delta_2.incr.{(datetime.now(timezone.utc) + timedelta(days=1)).strftime('%Y-%m-%d')}.tar.gz"
    archive_path_1 = f"oa_bulk/oa_comm/xml/{archive_name_1}"
    archive_path_2 = f"oa_bulk/oa_comm/xml/{archive_name_2}"


    mocker.patch.object(S3DataSource, "get_retracted_pmcids", return_value=["PMC02"])
    mocker.patch.object(
        S3DataSource,
        "get_incremental_updates",
        return_value=[
            IncrementalUpdateInfo(
                archive_path=archive_path_1,
                file_list_path=archive_path_1.replace(".tar.gz", ".filelist.csv"),
                date=datetime.now(timezone.utc),
            ),
            IncrementalUpdateInfo(
                archive_path=archive_path_2,
                file_list_path=archive_path_2.replace(".tar.gz", ".filelist.csv"),
                date=datetime.now(timezone.utc) + timedelta(days=1),
            ),
        ],
    )

    updated_article_1_time = datetime.now(timezone.utc).replace(microsecond=0)
    fake_archive_1 = _create_fake_tar_gz(
        tmp_path,
        archive_name_1,
        [
            {"pmcid": "PMC01", "pmid": 111, "doi": "10.0001/test.1.v2", "title": "Updated Article 1"},
            {"pmcid": "PMC03", "pmid": 333, "doi": "10.0001/test.3", "title": "New Article 3"},
        ],
    )
    fake_archive_2 = _create_fake_tar_gz(
        tmp_path,
        archive_name_2,
        [{"pmcid": "PMC03", "pmid": 333, "doi": "10.0001/test.3", "title": "New Article 3"}],
    )

    mocker.patch.object(S3DataSource, "download_file", side_effect=[fake_archive_1, fake_archive_2])

    def mock_stream_file_list(self, file_list_path: str):
        if archive_path_1.replace(".tar.gz", ".filelist.csv") in file_list_path:
            yield ArticleFileInfo(file_path="PMC01.xml", pmcid="PMC01", last_updated=updated_article_1_time, is_retracted=False)
            yield ArticleFileInfo(file_path="PMC03.xml", pmcid="PMC03", last_updated=updated_article_1_time, is_retracted=False)
        elif archive_path_2.replace(".tar.gz", ".filelist.csv") in file_list_path:
            yield ArticleFileInfo(file_path="PMC03.xml", pmcid="PMC03", last_updated=updated_article_1_time, is_retracted=True)
        else:
            # Must be an empty generator for python < 3.8
            return
            yield
    mocker.patch.object(S3DataSource, "stream_article_infos_from_file_list", mock_stream_file_list)

    # --- 3. Run the CLI command ---
    result = runner.invoke(app, ["delta-load", "--source", "s3", "--parsing-workers", "1"])
    assert result.exit_code == 0, result.stdout
    # Check for the new loading message from the refactored CLI
    assert "-> Loading" in result.stdout
    # Check that the final retraction handling still happens
    assert "retracted based on daily lists" in result.stdout

    # --- 4. Assert Final Database State ---
    with db_adapter.conn.cursor() as cursor:
        cursor.execute("SELECT status, last_file_processed FROM sync_history WHERE run_type = 'DELTA'")
        delta_run = cursor.fetchone()
        assert delta_run is not None
        assert delta_run[0] == "SUCCESS"
        assert delta_run[1] == archive_path_2

        cursor.execute("SELECT pmcid, title, is_retracted, source_last_updated, doi FROM pmc_articles_metadata ORDER BY pmcid")
        final_articles = cursor.fetchall()
        assert len(final_articles) == 3

        # PMC01: Updated
        assert final_articles[0][0] == "PMC01"
        assert final_articles[0][1] == "Updated Article 1"
        assert final_articles[0][2] is False
        assert final_articles[0][3].replace(microsecond=0) == updated_article_1_time
        assert final_articles[0][4] == "10.0001/test.1.v2"

        # PMC02: Retracted by master list
        assert final_articles[1][0] == "PMC02"
        assert final_articles[1][1] == "Initial Article 2"
        assert final_articles[1][2] is True

        # PMC03: Added, then retracted by daily list
        assert final_articles[2][0] == "PMC03"
        assert final_articles[2][1] == "New Article 3"
        assert final_articles[2][2] is True

    db_adapter.close()
