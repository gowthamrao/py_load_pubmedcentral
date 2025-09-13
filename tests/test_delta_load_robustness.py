import tarfile
import textwrap
import io
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typer.testing import CliRunner
import pytest
from py_load_pubmedcentral.acquisition import IncrementalUpdateInfo, S3DataSource
from py_load_pubmedcentral.cli import app, _parse_delta_archive_worker
from py_load_pubmedcentral.config import settings
from py_load_pubmedcentral.db import PostgreSQLAdapter
from py_load_pubmedcentral.models import ArticleFileInfo

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


def _create_fake_tar_gz(tmp_path: Path, archive_name: str, articles: list[dict]) -> Path:
    """Helper to create a .tar.gz file with specified XML contents."""
    archive_path = tmp_path / archive_name
    with tarfile.open(archive_path, "w:gz") as tar:
        for article in articles:
            xml_content = XML_TEMPLATE.format(**article).encode("utf-8")
            tarinfo = tarfile.TarInfo(name=f"{article['pmcid']}.xml")
            tarinfo.size = len(xml_content)
            tar.addfile(tarinfo, io.BytesIO(xml_content))
    return archive_path


def _setup_initial_db_state(db_adapter: PostgreSQLAdapter):
    """Sets up the DB with a full load record and one initial article."""
    schema_path = Path("schemas/pmc_schema.sql")
    with open(schema_path, "r", encoding="utf-8") as f:
        db_adapter.execute_sql(f.read())
    with db_adapter.conn.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE pmc_articles_metadata, pmc_articles_content, sync_history RESTART IDENTITY;")
    db_adapter.conn.commit()

    full_load_time = datetime.now(timezone.utc) - timedelta(days=5)
    with db_adapter.conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO sync_history (run_type, start_time, end_time, status)
            VALUES ('FULL', %s, %s, 'SUCCESS');
            """,
            (full_load_time, full_load_time),
        )
        db_adapter.conn.commit()


def test_delta_load_failure_and_recovery(postgresql, mocker, tmp_path):
    """
    Tests that the delta-load command can recover from a failure.
    Scenario:
    1. A `delta-load` is initiated with two daily updates.
    2. The parsing of the first update package succeeds.
    3. The parsing of the second update package fails.
    4. The first run is marked as FAILED, but the state reflects that the first
       package was processed.
    5. A subsequent `delta-load` run is initiated.
    6. The second run skips the first package and successfully processes the second.
    7. The final database state is correct.
    """
    # --- 1. Setup ---
    runner = CliRunner()
    mocker.patch.object(settings, "db_host", postgresql.info.host)
    mocker.patch.object(settings, "db_port", postgresql.info.port)
    mocker.patch.object(settings, "db_user", postgresql.info.user)
    mocker.patch.object(settings, "db_password", postgresql.info.password)
    mocker.patch.object(settings, "db_name", postgresql.info.dbname)

    db_params = {
        "host": settings.db_host,
        "port": settings.db_port,
        "user": settings.db_user,
        "password": settings.db_password,
        "dbname": settings.db_name,
    }
    db_adapter = PostgreSQLAdapter(db_params)
    db_adapter.connect()
    _setup_initial_db_state(db_adapter)

    # --- 2. Mock S3 Data Source and create fake data ---
    now = datetime.now(timezone.utc)
    archive_name_1 = f"delta_1.{(now - timedelta(days=1)).strftime('%Y-%m-%d')}.tar.gz"
    archive_name_2 = f"delta_2.{now.strftime('%Y-%m-%d')}.tar.gz"
    archive_path_1 = f"oa_bulk/{archive_name_1}"
    archive_path_2 = f"oa_bulk/{archive_name_2}"

    updates = [
        IncrementalUpdateInfo(archive_path=archive_path_1, file_list_path="f1.csv", date=now - timedelta(days=1)),
        IncrementalUpdateInfo(archive_path=archive_path_2, file_list_path="f2.csv", date=now),
    ]
    mocker.patch.object(S3DataSource, "get_incremental_updates", return_value=updates)
    mocker.patch.object(S3DataSource, "get_retracted_pmcids", return_value=[])

    fake_archive_1 = _create_fake_tar_gz(tmp_path, archive_name_1, [{"pmcid": "PMC01", "pmid": 111, "doi": "10.1", "title": "Article 1"}])
    fake_archive_2 = _create_fake_tar_gz(tmp_path, archive_name_2, [{"pmcid": "PMC02", "pmid": 222, "doi": "10.2", "title": "Article 2"}])

    def mock_download_file(self, archive_path, tmp_path_arg):
        if archive_path == archive_path_1:
            return fake_archive_1
        if archive_path == archive_path_2:
            return fake_archive_2
        return None

    mocker.patch.object(S3DataSource, "download_file", mock_download_file)

    def mock_stream_file_list(self, file_list_path: str):
        if file_list_path == "f1.csv":
            yield ArticleFileInfo(file_path="PMC01.xml", pmcid="PMC01", last_updated=now, is_retracted=False)
        elif file_list_path == "f2.csv":
            yield ArticleFileInfo(file_path="PMC02.xml", pmcid="PMC02", last_updated=now, is_retracted=False)
        else:
            return; yield
    mocker.patch.object(S3DataSource, "stream_article_infos_from_file_list", mock_stream_file_list)

    # --- 3. Simulate Failure on the Second Archive ---
    original_parser = _parse_delta_archive_worker
    call_count = 0
    def failing_parser_worker(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        # The second argument to the worker is the IncrementalUpdateInfo
        if args[1].archive_path == archive_path_2:
            raise ValueError("Simulating a parsing failure")
        return original_parser(*args, **kwargs)

    mocker.patch("py_load_pubmedcentral.cli._parse_delta_archive_worker", side_effect=failing_parser_worker)

    # --- 4. Run the CLI command (expecting failure) ---
    result1 = runner.invoke(app, ["delta-load", "--source", "s3", "--parsing-workers", "1"])
    assert result1.exit_code == 1

    # --- 5. Verify State After Failure ---
    with db_adapter.conn.cursor() as cursor:
        cursor.execute("SELECT status, last_file_processed FROM sync_history WHERE run_type = 'DELTA' ORDER BY start_time DESC LIMIT 1")
        last_run = cursor.fetchone()
        assert last_run is not None
        assert last_run[0] == "FAILED"
        assert last_run[1] == archive_path_1  # Should have recorded the last successful file

        cursor.execute("SELECT COUNT(*) FROM pmc_articles_metadata")
        assert cursor.fetchone()[0] == 1  # Only the first article should be loaded

    # --- 6. Run again (expecting success) ---
    mocker.patch("py_load_pubmedcentral.cli._parse_delta_archive_worker", side_effect=original_parser) # Restore original

    result2 = runner.invoke(app, ["delta-load", "--source", "s3", "--parsing-workers", "1"])
    assert result2.exit_code == 0, result2.stdout

    # --- 7. Verify Final State ---
    with db_adapter.conn.cursor() as cursor:
        cursor.execute("SELECT status, last_file_processed FROM sync_history WHERE run_type = 'DELTA' ORDER BY start_time DESC LIMIT 1")
        last_run = cursor.fetchone()
        assert last_run is not None
        assert last_run[0] == "SUCCESS"
        assert last_run[1] == archive_path_2

        cursor.execute("SELECT pmcid FROM pmc_articles_metadata ORDER BY pmcid")
        final_articles = [row[0] for row in cursor.fetchall()]
        assert final_articles == ["PMC01", "PMC02"]

    db_adapter.close()
