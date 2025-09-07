"""
Command-line interface for py-load-pubmedcentral.
"""
from __future__ import annotations

import tempfile
from pathlib import Path
from enum import Enum

import typer

import collections
from datetime import timezone
from urllib.parse import urljoin

from py_load_pubmedcentral.acquisition import (
    DataSource,
    NcbiFtpDataSource,
    S3DataSource,
    stream_and_parse_tar_gz_archive,
)
from py_load_pubmedcentral.models import PmcArticlesContent, PmcArticlesMetadata
from py_load_pubmedcentral.utils import get_db_adapter

app = typer.Typer(
    help="A high-throughput pipeline for synchronizing PubMed Central into a relational database."
)


@app.command()
def initialize(
    db_schema_file: Path = typer.Option(
        "py-load-pubmedcentral/schemas/pmc_schema.sql",
        "--schema",
        "-s",
        help="Path to the SQL file containing the database schema.",
        exists=True,
        dir_okay=False,
    )
):
    """
    Initialize the database schema by executing an SQL script.
    """
    typer.echo("--- Initializing Database Schema ---")
    adapter = None
    try:
        adapter = get_db_adapter()
        typer.echo(f"Connecting to PostgreSQL at {adapter.connection_params['host']}...")

        typer.echo(f"Reading schema from '{db_schema_file}'...")
        with open(db_schema_file, "r", encoding="utf-8") as f:
            sql_script = f.read()

        adapter.execute_sql(sql_script)

        typer.secho("Database schema initialized successfully.", fg=typer.colors.GREEN)

    except FileNotFoundError:
        typer.secho(f"Error: Schema file not found at '{db_schema_file}'", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    except Exception as e:
        typer.secho(f"An error occurred during database initialization: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    finally:
        if adapter:
            adapter.close()


class DataSourceName(str, Enum):
    ftp = "ftp"
    s3 = "s3"


@app.command()
def full_load(
    batch_size: int = typer.Option(5000, "--batch-size", "-b", help="Number of records to report progress by."),
    source: DataSourceName = typer.Option(
        DataSourceName.s3,
        "--source",
        help="The data source to use (ftp or s3).",
        case_sensitive=False,
    ),
):
    """
    Execute a full (baseline) load by discovering and processing all
    baseline archives from the selected data source.
    """
    typer.echo(f"--- Starting Full Baseline Load from source: {source.value} ---")
    adapter = get_db_adapter()

    data_source: DataSource
    if source == DataSourceName.s3:
        data_source = S3DataSource()
    else:
        data_source = NcbiFtpDataSource()

    run_id = None
    status = "FAILED"  # Assume failure unless explicitly marked as success
    files_processed_count = 0
    total_records_count = 0

    try:
        run_id = adapter.start_run(run_type="FULL")
        typer.echo(f"Sync history started for run_id: {run_id}")

        typer.echo("Getting article file list from NCBI...")
        article_file_list = data_source.get_article_file_list()
        if not article_file_list:
            typer.secho("No article files found. Exiting.", fg=typer.colors.YELLOW)
            status = "SUCCESS"  # Not a failure, just nothing to do.
            raise typer.Exit()

        typer.echo(f"Found {len(article_file_list)} article archives to process.")

        # Group articles by their archive path to process each archive only once.
        articles_by_archive = collections.defaultdict(list)
        for article in article_file_list:
            articles_by_archive[article.file_path].append(article)
        typer.echo(f"Discovered {len(articles_by_archive)} unique archives to process.")

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            typer.echo(f"Using temporary directory: {tmp_path}")

            for i, (archive_path, articles_in_archive) in enumerate(articles_by_archive.items()):
                if source == DataSourceName.ftp:
                    file_identifier = urljoin(NcbiFtpDataSource.BASE_URL, archive_path)
                else:
                    file_identifier = archive_path

                typer.echo(f"\n--- Processing archive {i+1} of {len(articles_by_archive)}: {file_identifier} ---")

                try:
                    verified_path = data_source.download_file(file_identifier, tmp_path)
                except Exception as e:
                    typer.secho(f"Failed to download/verify {file_identifier}. Error: {e}", fg=typer.colors.RED)
                    continue

                # Create a lookup map for the articles within this specific archive.
                article_info_lookup = {info.pmcid: info for info in articles_in_archive}
                records_in_archive = 0
                metadata_tsv_path = tmp_path / "metadata.tsv"
                content_tsv_path = tmp_path / "content.tsv"

                # Use the new parsing function signature.
                article_generator = stream_and_parse_tar_gz_archive(
                    verified_path,
                    article_info_lookup=article_info_lookup,
                )
                metadata_columns = list(PmcArticlesMetadata.model_fields.keys())
                content_columns = list(PmcArticlesContent.model_fields.keys())

                with open(metadata_tsv_path, "w", encoding="utf-8") as meta_f, \
                     open(content_tsv_path, "w", encoding="utf-8") as content_f:
                    for metadata, content in article_generator:
                        meta_f.write(adapter._prepare_tsv_row(metadata, metadata_columns))
                        content_f.write(adapter._prepare_tsv_row(content, content_columns))
                        records_in_archive += 1
                        if records_in_archive % batch_size == 0:
                            typer.echo(f"  ...processed {records_in_archive} records from this archive...")

                typer.secho(f"Parsed {records_in_archive} records from this archive.", fg=typer.colors.GREEN)

                if records_in_archive > 0:
                    typer.echo("Loading TSV files into the database...")
                    # For a full load, we use bulk_load_native, which is faster than upserting.
                    adapter.bulk_load_native(str(metadata_tsv_path), "pmc_articles_metadata")
                    adapter.bulk_load_native(str(content_tsv_path), "pmc_articles_content")
                    typer.secho("Database load complete for this archive.", fg=typer.colors.GREEN)

                files_processed_count += 1
                total_records_count += records_in_archive
                verified_path.unlink()
                metadata_tsv_path.unlink()
                content_tsv_path.unlink()

        typer.secho("\nFull baseline load process finished successfully.", fg=typer.colors.GREEN)
        status = "SUCCESS"

    except Exception as e:
        typer.secho(f"A critical error occurred: {e}", fg=typer.colors.RED)
        # Status is already 'FAILED', so we just let the finally block handle it
    finally:
        if adapter:
            if run_id:
                metrics = {
                    "files_processed": files_processed_count,
                    "total_records": total_records_count,
                }
                typer.echo(f"Updating sync_history for run_id {run_id} with status '{status}'...")
                adapter.end_run(run_id, status, metrics)
            adapter.close()


@app.command()
def delta_load(
    batch_size: int = typer.Option(5000, "--batch-size", "-b", help="Number of records to report progress by."),
    source: DataSourceName = typer.Option(
        DataSourceName.s3,
        "--source",
        help="The data source to use (ftp or s3).",
        case_sensitive=False,
    ),
):
    """
    Execute an incremental (delta) load from the last known successful run.
    """
    typer.echo("--- Starting Delta Load ---")
    adapter = get_db_adapter()
    data_source: DataSource = S3DataSource() if source == DataSourceName.s3 else NcbiFtpDataSource()

    run_id = None
    status = "FAILED"
    files_processed_count = 0
    total_records_count = 0

    try:
        # 1. Get the timestamp of the last successful run to define the delta window.
        last_sync_time = adapter.get_last_successful_run_timestamp()
        if last_sync_time is None:
            typer.secho("No successful previous run found. Please run a `full-load` first.", fg=typer.colors.RED)
            raise typer.Exit(code=1)

        # Ensure timestamps are timezone-aware (UTC) for correct comparison.
        last_sync_time_utc = last_sync_time.replace(tzinfo=timezone.utc)
        typer.echo(f"Looking for updates since last successful sync at {last_sync_time_utc.isoformat()}")

        run_id = adapter.start_run(run_type="DELTA")
        typer.echo(f"Sync history started for run_id: {run_id}")

        # 2. Get the full article list and filter for new/updated articles.
        typer.echo("Getting full article file list from source...")
        all_articles = data_source.get_article_file_list()
        delta_articles = [
            article for article in all_articles
            if article.last_updated and article.last_updated.replace(tzinfo=timezone.utc) > last_sync_time_utc
        ]

        if not delta_articles:
            typer.secho("No new or updated articles found. Nothing to do.", fg=typer.colors.GREEN)
            status = "SUCCESS"
            return  # Graceful exit, finally block will handle logging.

        typer.echo(f"Found {len(delta_articles)} new/updated articles to process.")

        # 3. Group articles by archive path to download each archive only once.
        articles_by_archive = collections.defaultdict(list)
        for article in delta_articles:
            articles_by_archive[article.file_path].append(article)
        typer.echo(f"Discovered {len(articles_by_archive)} unique archives to process.")

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            for i, (archive_path, articles_in_archive) in enumerate(articles_by_archive.items()):
                if source == DataSourceName.ftp:
                    file_identifier = urljoin(NcbiFtpDataSource.BASE_URL, archive_path)
                else:
                    file_identifier = archive_path

                typer.echo(f"\n--- Processing archive {i+1} of {len(articles_by_archive)}: {file_identifier} ---")
                try:
                    verified_path = data_source.download_file(file_identifier, tmp_path)
                except Exception as e:
                    typer.secho(f"Failed to download/verify {file_identifier}. Error: {e}", fg=typer.colors.RED)
                    continue

                article_info_lookup = {info.pmcid: info for info in articles_in_archive}
                records_in_archive = 0
                metadata_tsv_path = tmp_path / "metadata.tsv"
                content_tsv_path = tmp_path / "content.tsv"

                article_generator = stream_and_parse_tar_gz_archive(verified_path, article_info_lookup)
                metadata_columns = list(PmcArticlesMetadata.model_fields.keys())
                content_columns = list(PmcArticlesContent.model_fields.keys())

                with open(metadata_tsv_path, "w", encoding="utf-8") as meta_f, \
                     open(content_tsv_path, "w", encoding="utf-8") as content_f:
                    for metadata, content in article_generator:
                        meta_f.write(adapter._prepare_tsv_row(metadata, metadata_columns))
                        content_f.write(adapter._prepare_tsv_row(content, content_columns))
                        records_in_archive += 1
                        if records_in_archive % batch_size == 0:
                            typer.echo(f"  ...processed {records_in_archive} records from this archive...")

                typer.secho(f"Parsed {records_in_archive} targeted records from this archive.", fg=typer.colors.GREEN)

                if records_in_archive > 0:
                    typer.echo("Upserting TSV files into the database...")
                    adapter.bulk_upsert_articles(str(metadata_tsv_path), str(content_tsv_path))
                    typer.secho("Database upsert complete for this archive.", fg=typer.colors.GREEN)

                files_processed_count += 1
                total_records_count += records_in_archive
                verified_path.unlink()
                metadata_tsv_path.unlink()
                content_tsv_path.unlink()

        status = "SUCCESS"

    except Exception as e:
        typer.secho(f"A critical error occurred during delta-load: {e}", fg=typer.colors.RED)
    finally:
        if adapter:
            if run_id:
                metrics = {"archives_processed": files_processed_count, "total_articles_upserted": total_records_count}
                typer.echo(f"Updating sync_history for run_id {run_id} with status '{status}'...")
                adapter.end_run(run_id, status, metrics)
            adapter.close()


if __name__ == "__main__":
    app()
