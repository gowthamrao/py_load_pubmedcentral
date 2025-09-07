"""
Command-line interface for py-load-pubmedcentral.
"""
from __future__ import annotations

import tempfile
from pathlib import Path
from enum import Enum
import concurrent.futures
import os
import uuid

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
from py_load_pubmedcentral.models import PmcArticlesContent, PmcArticlesMetadata, ArticleFileInfo
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


def _download_archive_worker(
    file_identifier: str,
    source_name: DataSourceName,
    tmp_path: Path,
) -> Path | None:
    """Helper function to download a single archive in a thread pool."""
    # Each thread needs its own data source instance
    data_source: DataSource = S3DataSource() if source_name == "s3" else NcbiFtpDataSource()
    try:
        return data_source.download_file(file_identifier, tmp_path)
    except Exception as e:
        typer.secho(
            f"Failed to download/verify {file_identifier}. Error: {e}",
            fg=typer.colors.RED,
            err=True,
        )
        return None


def _parse_archive_worker(
    verified_path: Path,
    articles_in_archive: list[ArticleFileInfo],
    tmp_path: Path,
) -> tuple[Path, Path, int] | None:
    """Helper function to parse a single archive in a process pool."""
    adapter = get_db_adapter()
    article_info_lookup = {info.pmcid: info for info in articles_in_archive}
    records_in_archive = 0
    # Use UUID to ensure TSV filenames are unique across processes
    run_uuid = uuid.uuid4()
    metadata_tsv_path = tmp_path / f"metadata_{run_uuid}.tsv"
    content_tsv_path = tmp_path / f"content_{run_uuid}.tsv"

    try:
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

        return metadata_tsv_path, content_tsv_path, records_in_archive
    except Exception as e:
        typer.secho(
            f"Failed to parse archive {verified_path.name}. Error: {e}",
            fg=typer.colors.RED,
            err=True,
        )
        return None
    finally:
        if adapter:
            adapter.close()
        verified_path.unlink() # Clean up the downloaded archive


@app.command()
def full_load(
    source: DataSourceName = typer.Option(
        DataSourceName.s3, "--source", help="The data source to use (ftp or s3).", case_sensitive=False
    ),
    download_workers: int = typer.Option(
        4, "--download-workers", help="Number of parallel workers for downloading archives."
    ),
    parsing_workers: int = typer.Option(
        os.cpu_count(), "--parsing-workers", help="Number of parallel workers for parsing archives."
    ),
):
    """
    Execute a full (baseline) load by discovering and processing all
    baseline archives from the selected data source in parallel.
    """
    typer.echo(f"--- Starting Full Baseline Load from source: {source.value} ---")
    adapter = get_db_adapter()
    data_source: DataSource = S3DataSource() if source == DataSourceName.s3 else NcbiFtpDataSource()

    run_id = None
    status = "FAILED"
    total_archives_processed = 0
    total_records_count = 0
    generated_tsv_files = []

    try:
        run_id = adapter.start_run(run_type="FULL")
        typer.echo(f"Sync history started for run_id: {run_id}")

        typer.echo("Clearing existing article data for a clean baseline load...")
        truncate_sql = "TRUNCATE TABLE pmc_articles_content, pmc_articles_metadata RESTART IDENTITY;"
        adapter.execute_sql(truncate_sql)
        typer.secho("Existing data cleared.", fg=typer.colors.YELLOW)

        typer.echo("Getting article file list from NCBI...")
        article_file_list = data_source.get_article_file_list()
        if not article_file_list:
            typer.secho("No article files found. Exiting.", fg=typer.colors.YELLOW)
            status = "SUCCESS"
            raise typer.Exit()

        articles_by_archive = collections.defaultdict(list)
        for article in article_file_list:
            articles_by_archive[article.file_path].append(article)
        typer.echo(f"Discovered {len(articles_by_archive)} unique archives to process.")

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            typer.echo(f"Using temporary directory: {tmp_path}")

            # --- Phase 1: Parallel Downloading ---
            downloaded_archives = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=download_workers) as executor:
                future_to_archive = {}
                for archive_path in articles_by_archive.keys():
                    file_identifier = urljoin(NcbiFtpDataSource.BASE_URL, archive_path) if source == DataSourceName.ftp else archive_path
                    future = executor.submit(_download_archive_worker, file_identifier, source, tmp_path)
                    future_to_archive[future] = archive_path

                typer.echo(f"Downloading {len(future_to_archive)} archives with {download_workers} workers...")
                for future in concurrent.futures.as_completed(future_to_archive):
                    archive_path = future_to_archive[future]
                    result_path = future.result()
                    if result_path:
                        downloaded_archives[archive_path] = result_path

            typer.secho(f"Successfully downloaded {len(downloaded_archives)} archives.", fg=typer.colors.GREEN)

            # --- Phase 2: Parallel Parsing ---
            with concurrent.futures.ProcessPoolExecutor(max_workers=parsing_workers) as executor:
                future_to_path = {
                    executor.submit(_parse_archive_worker, downloaded_path, articles_by_archive[archive_path], tmp_path): downloaded_path
                    for archive_path, downloaded_path in downloaded_archives.items()
                }
                typer.echo(f"Parsing {len(future_to_path)} archives with {parsing_workers} workers...")
                for future in concurrent.futures.as_completed(future_to_path):
                    result = future.result()
                    if result:
                        meta_path, content_path, records_count = result
                        generated_tsv_files.append((meta_path, content_path))
                        total_records_count += records_count
                        total_archives_processed += 1
                        typer.echo(f"  ...parsed {records_count} records from archive.")

            typer.secho(f"Successfully parsed {total_archives_processed} archives.", fg=typer.colors.GREEN)

            # --- Phase 3: Sequential Loading ---
            typer.echo(f"Loading data from {len(generated_tsv_files) * 2} TSV files into the database...")
            for i, (meta_path, content_path) in enumerate(generated_tsv_files):
                typer.echo(f"  Loading file set {i+1} of {len(generated_tsv_files)}...")
                adapter.bulk_load_native(str(meta_path), "pmc_articles_metadata")
                adapter.bulk_load_native(str(content_path), "pmc_articles_content")
                meta_path.unlink()
                content_path.unlink()

            typer.secho("Database loading complete.", fg=typer.colors.GREEN)

        status = "SUCCESS"

    except Exception as e:
        typer.secho(f"A critical error occurred: {e}", fg=typer.colors.RED)
    finally:
        if adapter:
            if run_id:
                metrics = {
                    "archives_processed": total_archives_processed,
                    "total_records_loaded": total_records_count,
                    "download_workers": download_workers,
                    "parsing_workers": parsing_workers,
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
    total_retracted_count = 0

    try:
        # 1. Get the timestamp of the last successful run to define the delta window.
        last_sync_time = adapter.get_last_successful_run_timestamp()
        if last_sync_time is None:
            typer.secho("No successful previous run found. Please run a `full-load` first.", fg=typer.colors.RED)
            raise typer.Exit(code=1)

        last_sync_time_utc = last_sync_time.replace(tzinfo=timezone.utc)
        typer.echo(f"Looking for updates since last successful sync at {last_sync_time_utc.isoformat()}")

        run_id = adapter.start_run(run_type="DELTA")
        typer.echo(f"Sync history started for run_id: {run_id}")

        # 2. Get all article metadata from the source.
        typer.echo("Getting full article file list from source...")
        all_articles = data_source.get_article_file_list()

        # 3. Handle Retractions
        typer.echo("Identifying retracted articles...")
        # Source 1: The `retractions.csv` file.
        retracted_from_file = set(data_source.get_retracted_pmcids())
        # Source 2: Articles marked as retracted in the main file list.
        retracted_from_list = {article.pmcid for article in all_articles if article.is_retracted}

        all_retracted_pmcids = list(retracted_from_file.union(retracted_from_list))

        if all_retracted_pmcids:
            typer.echo(f"Found {len(all_retracted_pmcids)} unique PMCIDs marked for retraction. Updating database...")
            updated_rows = adapter.handle_deletions(all_retracted_pmcids)
            total_retracted_count = updated_rows
            typer.secho(f"Marked {updated_rows} articles as retracted in the database.", fg=typer.colors.YELLOW)
        else:
            typer.echo("No retracted articles found.")

        # 4. Filter for new and updated articles for processing.
        # An article is part of the delta if it was updated since the last sync
        # AND it is NOT retracted. Retracted articles are handled separately.
        delta_articles = [
            article for article in all_articles
            if not article.is_retracted and (
                article.last_updated and
                article.last_updated.replace(tzinfo=timezone.utc) > last_sync_time_utc
            )
        ]

        if not delta_articles:
            typer.secho("No new or updated articles found to process.", fg=typer.colors.GREEN)
            status = "SUCCESS"
            return

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
                metrics = {
                    "archives_processed": files_processed_count,
                    "total_articles_upserted": total_records_count,
                    "total_articles_retracted": total_retracted_count,
                }
                typer.echo(f"Updating sync_history for run_id {run_id} with status '{status}'...")
                adapter.end_run(run_id, status, metrics)
            adapter.close()


if __name__ == "__main__":
    app()
