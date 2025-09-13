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
import contextlib

import typer

import collections
from datetime import timezone
from urllib.parse import urljoin

from py_load_pubmedcentral.acquisition import (
    DataSource,
    IncrementalUpdateInfo,
    NcbiFtpDataSource,
    S3DataSource,
)
from py_load_pubmedcentral.config import settings
from py_load_pubmedcentral.parser import stream_and_parse_tar_gz_archive
from py_load_pubmedcentral.models import PmcArticlesContent, PmcArticlesMetadata, ArticleFileInfo
from py_load_pubmedcentral.utils import get_db_adapter, get_logger


logger = get_logger(__name__)

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
        dir_okay=False,
    )
):
    """
    Initialize the database schema by executing an SQL script.
    """
    logger.info("--- Initializing Database Schema ---")
    adapter = None
    try:
        adapter = get_db_adapter()
        logger.info(f"Connecting to PostgreSQL at {adapter.connection_params['host']}...")

        logger.info(f"Reading schema from '{db_schema_file}'...")
        with open(db_schema_file, "r", encoding="utf-8") as f:
            sql_script = f.read()

        adapter.execute_sql(sql_script)

        logger.info("Database schema initialized successfully.")

    except FileNotFoundError:
        logger.error(f"Error: Schema file not found at '{db_schema_file}'")
        raise typer.Exit(code=1)
    except Exception as e:
        logger.error(f"An error occurred during database initialization: {e}", exc_info=True)
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
    except Exception:
        logger.error(
            f"Failed to download/verify {file_identifier}",
            exc_info=True
        )
        return None


def _parse_archive_worker(
    verified_path: Path | str,
    article_info_lookup: dict[str, ArticleFileInfo],
    tmp_path: Path,
) -> tuple[Path, Path, int] | None:
    """
    Helper function to parse a single archive in a process pool.
    This is now decoupled from the database adapter's implementation details.
    """
    verified_path = Path(verified_path)
    adapter = get_db_adapter()
    # Use UUID to ensure TSV filenames are unique across processes
    run_uuid = uuid.uuid4()
    metadata_tsv_path = tmp_path / f"metadata_{run_uuid}.tsv"
    content_tsv_path = tmp_path / f"content_{run_uuid}.tsv"

    try:
        article_generator = stream_and_parse_tar_gz_archive(
            str(verified_path),
            article_info_lookup,
        )
        # Buffer articles from this archive in memory. This is a trade-off
        # for cleaner separation of concerns. An archive is unlikely to be
        # so large as to exhaust memory in a single process.
        all_metadata, all_content = zip(*article_generator)
        records_in_archive = len(all_metadata)

        if records_in_archive > 0:
            # Use the adapter to write the models to TSV files, encapsulating
            # the database-specific formatting logic within the adapter.
            metadata_columns = list(PmcArticlesMetadata.model_fields.keys())
            with open(metadata_tsv_path, "w", encoding="utf-8") as f:
                adapter.write_models_to_tsv_file(all_metadata, metadata_columns, f)

            content_columns = list(PmcArticlesContent.model_fields.keys())
            with open(content_tsv_path, "w", encoding="utf-8") as f:
                adapter.write_models_to_tsv_file(all_content, content_columns, f)

        return metadata_tsv_path, content_tsv_path, records_in_archive
    except Exception:
        logger.error(
            f"Failed to parse archive {verified_path.name}",
            exc_info=True
        )
        return None
    finally:
        if adapter:
            adapter.close()
        # Clean up the downloaded archive after parsing is complete
        if verified_path and verified_path.exists():
            verified_path.unlink()


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
    logger.info(f"--- Starting Full Baseline Load from source: {source.value} ---")
    adapter = get_db_adapter()
    data_source: DataSource = S3DataSource() if source == DataSourceName.s3 else NcbiFtpDataSource()

    run_id = None
    status = "FAILED"
    total_archives_processed = 0
    total_records_count = 0
    generated_tsv_files = []

    try:
        logger.info("Validating database schema...")
        adapter.validate_schema()

        run_id = adapter.start_run(run_type="FULL")
        logger.info(f"Sync history started for run_id: {run_id}")

        logger.info("Clearing existing article data for a clean baseline load...")
        truncate_sql = "TRUNCATE TABLE pmc_articles_content, pmc_articles_metadata RESTART IDENTITY;"
        adapter.execute_sql(truncate_sql)
        logger.warning("Existing data cleared.")

        logger.info("Getting article file list from NCBI...")
        article_file_list = data_source.get_article_file_list()
        if not article_file_list:
            logger.warning("No article files found. Exiting.")
            status = "SUCCESS"
            raise typer.Exit()

        articles_by_archive = collections.defaultdict(list)
        for article in article_file_list:
            articles_by_archive[article.file_path].append(article)
        logger.info(f"Discovered {len(articles_by_archive)} unique archives to process.")

        with _get_staging_directory() as tmp_path:
            logger.info(f"Using staging directory: {tmp_path}")

            # --- Phase 1: Parallel Downloading ---
            downloaded_archives = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=download_workers) as executor:
                future_to_archive = {}
                for archive_path in articles_by_archive.keys():
                    file_identifier = urljoin(NcbiFtpDataSource.BASE_URL, archive_path) if source == DataSourceName.ftp else archive_path
                    future = executor.submit(_download_archive_worker, file_identifier, source, tmp_path)
                    future_to_archive[future] = archive_path

                logger.info(f"Downloading {len(future_to_archive)} archives with {download_workers} workers...")
                for future in concurrent.futures.as_completed(future_to_archive):
                    archive_path = future_to_archive[future]
                    result_path = future.result()
                    if result_path:
                        downloaded_archives[archive_path] = result_path

            logger.info(f"Successfully downloaded {len(downloaded_archives)} archives.")

            # --- Phase 2: Parallel Parsing ---
            if parsing_workers > 1:
                with concurrent.futures.ProcessPoolExecutor(max_workers=parsing_workers) as executor:
                    future_to_path = {}
                    for archive_path, downloaded_path in downloaded_archives.items():
                        # Create the lookup dict for this specific archive
                        article_info_lookup = {info.pmcid: info for info in articles_by_archive[archive_path]}
                        future = executor.submit(_parse_archive_worker, downloaded_path, article_info_lookup, tmp_path)
                        future_to_path[future] = downloaded_path
                    logger.info(f"Parsing {len(future_to_path)} archives with {parsing_workers} workers...")
                    for future in concurrent.futures.as_completed(future_to_path):
                        result = future.result()
                        if result:
                            meta_path, content_path, records_count = result
                            generated_tsv_files.append((meta_path, content_path))
                            total_records_count += records_count
                            total_archives_processed += 1
                            logger.info(f"  ...parsed {records_count} records from archive.")
            else:
                # Run sequentially if only one worker is specified
                logger.info("Parsing archives sequentially with 1 worker...")
                for archive_path, downloaded_path in downloaded_archives.items():
                    article_info_lookup = {info.pmcid: info for info in articles_by_archive[archive_path]}
                    result = _parse_archive_worker(downloaded_path, article_info_lookup, tmp_path)
                    if result:
                        meta_path, content_path, records_count = result
                        generated_tsv_files.append((meta_path, content_path))
                        total_records_count += records_count
                        total_archives_processed += 1
                        logger.info(f"  ...parsed {records_count} records from archive {archive_path}.")

            logger.info(f"Successfully parsed {total_archives_processed} archives, yielding {total_records_count} records.")

            # --- Phase 3: Optimized Loading ---
            # No aggregation step. We load the generated TSV files directly.
            if generated_tsv_files:
                logger.info(f"Loading data from {len(generated_tsv_files)} batches into the database...")
                # The new method handles the transaction and staging tables internally.
                metadata_files, content_files = zip(*generated_tsv_files)
                adapter.bulk_insert_from_files_for_full_load(
                    metadata_file_paths=[str(p) for p in metadata_files if p],
                    content_file_paths=[str(p) for p in content_files if p],
                )
            else:
                logger.info("No new records were parsed, skipping database load.")

            logger.info("Database loading complete.")

        status = "SUCCESS"

    except Exception:
        logger.critical("A critical error occurred during full_load", exc_info=True)
    finally:
        if adapter:
            if run_id:
                metrics = {
                    "archives_processed": total_archives_processed,
                    "total_records_loaded": total_records_count,
                    "download_workers": download_workers,
                    "parsing_workers": parsing_workers,
                }
                logger.info(f"Updating sync_history for run_id {run_id} with status '{status}'...")
                adapter.end_run(run_id, status, metrics)
            adapter.close()


def _parse_delta_archive_worker(
    verified_path: Path | str,
    update_info: IncrementalUpdateInfo,
    source_name: DataSourceName,
    tmp_path: Path,
) -> tuple[Path, Path, int, list[str], str] | None:
    """
    Helper function to parse a single delta archive in a process pool.
    This handles getting the file list and parsing. It returns the list of
    retracted PMCIDs instead of processing them directly.
    """
    verified_path = Path(verified_path)

    # Each process needs its own instances
    adapter = get_db_adapter()
    data_source: DataSource = S3DataSource() if source_name == "s3" else NcbiFtpDataSource()

    run_uuid = uuid.uuid4()
    metadata_tsv_path = tmp_path / f"metadata_{run_uuid}.tsv"
    content_tsv_path = tmp_path / f"content_{run_uuid}.tsv"
    records_in_archive = 0

    try:
        # 1. Get the file list for this specific daily package.
        articles_in_archive = []
        retracted_pmcids = []
        try:
            daily_file_list_stream = data_source.stream_article_infos_from_file_list(update_info.file_list_path)
            for article_info in daily_file_list_stream:
                articles_in_archive.append(article_info)
                if article_info.is_retracted:
                    retracted_pmcids.append(article_info.pmcid)
        except Exception:
            logger.error(f"Could not parse file list {update_info.file_list_path}. Skipping archive.", exc_info=True)
            return None

        # 2. Filter out retracted articles from parsing for this archive
        articles_to_process = [info for info in articles_in_archive if not info.is_retracted]
        if not articles_to_process:
            # Still return the archive path and retractions so we can mark this file as "processed"
            return None, None, 0, retracted_pmcids, update_info.archive_path

        # 3. Parse the archive and write to TSV.
        article_info_lookup = {info.pmcid: info for info in articles_to_process}
        article_generator = stream_and_parse_tar_gz_archive(str(verified_path), article_info_lookup)

        # Buffer articles from this archive in memory to decouple parsing from writing.
        buffered_articles = list(article_generator)
        records_in_archive = len(buffered_articles)

        if records_in_archive > 0:
            all_metadata, all_content = zip(*buffered_articles)

            metadata_columns = list(PmcArticlesMetadata.model_fields.keys())
            with open(metadata_tsv_path, "w", encoding="utf-8") as f:
                adapter.write_models_to_tsv_file(all_metadata, metadata_columns, f)

            content_columns = list(PmcArticlesContent.model_fields.keys())
            with open(content_tsv_path, "w", encoding="utf-8") as f:
                adapter.write_models_to_tsv_file(all_content, content_columns, f)

        return metadata_tsv_path, content_tsv_path, records_in_archive, retracted_pmcids, update_info.archive_path

    except Exception:
        logger.error(f"Failed to parse delta archive {verified_path.name}", exc_info=True)
        return None
    finally:
        if adapter:
            adapter.close()
        if verified_path and verified_path.exists():
            verified_path.unlink()


@contextlib.contextmanager
def _get_staging_directory() -> Path:
    """
    A context manager that provides a staging directory for temporary files.
    If settings.staging_dir is configured, it uses that directory and does not
    clean it up. Otherwise, it creates and uses a temporary directory that
    is automatically cleaned up on exit.
    """
    if settings.staging_dir:
        # Use the user-configured directory
        staging_path = settings.staging_dir
        staging_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Using persistent staging directory: {staging_path}")
        yield staging_path
    else:
        # Use a temporary directory that will be cleaned up
        with tempfile.TemporaryDirectory() as tmpdir:
            logger.info(f"Using temporary staging directory: {tmpdir}")
            yield Path(tmpdir)


@app.command()
def delta_load(
    source: DataSourceName = typer.Option(
        DataSourceName.s3,
        "--source",
        help="The data source to use (ftp or s3).",
        case_sensitive=False,
    ),
    download_workers: int = typer.Option(
        4, "--download-workers", help="Number of parallel workers for downloading archives."
    ),
    parsing_workers: int = typer.Option(
        os.cpu_count(), "--parsing-workers", help="Number of parallel workers for parsing archives."
    ),
):
    """
    Execute an incremental (delta) load using daily update packages
    from the last known successful run.
    """
    logger.info("--- Starting Delta Load ---")
    adapter = get_db_adapter()
    data_source: DataSource = S3DataSource() if source == DataSourceName.s3 else NcbiFtpDataSource()

    run_id = None
    status = "FAILED"
    total_archives_processed = 0
    total_records_upserted = 0
    total_retracted_count = 0
    last_processed_file = None
    daily_retractions_to_process = []

    try:
        logger.info("Validating database schema...")
        adapter.validate_schema()

        # 1. Get the info of the last successful run to define the delta window.
        last_run_info = adapter.get_last_successful_run_info()
        if last_run_info:
            last_sync_time, last_processed_file = last_run_info
            logger.info(f"Looking for updates since last successful sync at {last_sync_time.isoformat()}.")
            if last_processed_file:
                logger.info(f"Resuming after last processed file: {last_processed_file}")
        else:
            # If no previous delta load, get time from the last full load.
            last_full_load_run = adapter.get_last_successful_run_info("FULL")
            if not last_full_load_run:
                logger.error("No successful 'FULL' load found. Please run a `full-load` first.")
                raise typer.Exit(code=1)
            last_sync_time, _ = last_full_load_run
            logger.info(f"No previous DELTA sync found. Looking for updates since last FULL load at {last_sync_time.isoformat()}.")

        run_id = adapter.start_run(run_type="DELTA")
        logger.info(f"Sync history started for run_id: {run_id}")

        # 2. Handle Retractions from the main retractions.csv file.
        logger.info("Processing full retraction list...")
        master_retracted_pmcids = data_source.get_retracted_pmcids()
        if master_retracted_pmcids:
            updated_rows = adapter.handle_deletions(master_retracted_pmcids)
            total_retracted_count += updated_rows
            logger.warning(f"Marked {updated_rows} articles as retracted based on master list.")

        # 3. Find new daily incremental update packages since the last run.
        last_sync_time_utc = last_sync_time.replace(tzinfo=timezone.utc)
        incremental_updates = data_source.get_incremental_updates(since=last_sync_time_utc)

        # Filter out any files that were already processed in the last run.
        if last_processed_file:
            try:
                # Find the index of the last processed file and slice the list
                last_file_index = [u.archive_path for u in incremental_updates].index(last_processed_file)
                incremental_updates = incremental_updates[last_file_index + 1:]
            except ValueError:
                # This can happen if the last processed file is no longer listed, which is fine.
                pass

        if not incremental_updates:
            logger.info("No new incremental updates found.")
            status = "SUCCESS"
            # Pass the master retraction count to the metrics
            metrics = {"total_articles_retracted": total_retracted_count}
            adapter.end_run(run_id, status, metrics)
            return

        logger.info(f"Found {len(incremental_updates)} incremental archives to process.")

        with _get_staging_directory() as tmp_path:
            logger.info(f"Using staging directory: {tmp_path}")

            # --- Phase 1: Parallel Downloading ---
            downloaded_archives = {} # Maps archive_path to (local_path, update_info)
            with concurrent.futures.ThreadPoolExecutor(max_workers=download_workers) as executor:
                future_to_update = {
                    executor.submit(_download_archive_worker, update.archive_path, source, tmp_path): update
                    for update in incremental_updates
                }
                logger.info(f"Downloading {len(future_to_update)} archives with {download_workers} workers...")
                for future in concurrent.futures.as_completed(future_to_update):
                    update_info = future_to_update[future]
                    result_path = future.result()
                    if result_path:
                        downloaded_archives[update_info.archive_path] = (result_path, update_info)

            logger.info(f"Successfully downloaded {len(downloaded_archives)} archives.")

            # --- Phase 2: Parallel Parsing and Transactional Loading ---
            if parsing_workers > 1:
                with concurrent.futures.ProcessPoolExecutor(max_workers=parsing_workers) as executor:
                    future_to_update_info = {
                        executor.submit(_parse_delta_archive_worker, local_path, update_info, source, tmp_path): update_info
                        for local_path, update_info in downloaded_archives.values()
                    }
                    logger.info(f"Parsing and loading {len(future_to_update_info)} archives with {parsing_workers} workers...")
                    # Process futures in submission order to ensure we process days sequentially
                    sorted_futures = [f for f, u in sorted(future_to_update_info.items(), key=lambda item: item[1].date)]

                    for future in sorted_futures:
                        result = future.result()
                        if result:
                            meta_path, content_path, records_count, retracted_pmcids, processed_archive_path = result

                            # If there are records to load, call the transactional method
                            if records_count > 0:
                                logger.info(f"  -> Loading {records_count} records from {processed_archive_path}...")
                                adapter.bulk_upsert_and_update_state(
                                    run_id=run_id,
                                    metadata_file_path=str(meta_path),
                                    content_file_path=str(content_path),
                                    file_processed=processed_archive_path,
                                )
                                meta_path.unlink()
                                content_path.unlink()
                            else:
                                # If the archive was processed but had no data (e.g., only retractions),
                                # still update the state to mark it as processed.
                                logger.info(f"  -> No new records in {processed_archive_path}, updating state.")
                                adapter.update_run_state(run_id, processed_archive_path)

                            # Collect retractions to be processed at the end
                            if retracted_pmcids:
                                daily_retractions_to_process.extend(retracted_pmcids)

                            total_records_upserted += records_count
                            total_archives_processed += 1
                            # State is updated transactionally
            else:
                logger.info("Parsing and loading archives sequentially with 1 worker...")
                # Sort by date to process sequentially
                sorted_archives = sorted(downloaded_archives.items(), key=lambda item: item[1][1].date)
                for archive_path, (local_path, update_info) in sorted_archives:
                    result = _parse_delta_archive_worker(local_path, update_info, source, tmp_path)
                    if result:
                        meta_path, content_path, records_count, retracted_pmcids, processed_archive_path = result
                        if records_count > 0:
                            logger.info(f"  -> Loading {records_count} records from {processed_archive_path}...")
                            adapter.bulk_upsert_and_update_state(
                                run_id=run_id,
                                metadata_file_path=str(meta_path),
                                content_file_path=str(content_path),
                                file_processed=processed_archive_path,
                            )
                            meta_path.unlink()
                            content_path.unlink()
                        else:
                            logger.info(f"  -> No new records in {processed_archive_path}, updating state.")
                            adapter.update_run_state(run_id, processed_archive_path)

                        if retracted_pmcids:
                            daily_retractions_to_process.extend(retracted_pmcids)

                        total_records_upserted += records_count
                        total_archives_processed += 1
                        # State is updated transactionally

            logger.info(f"Successfully processed {total_archives_processed} archives.")

            # --- Phase 3: Final Retraction Handling ---
            if daily_retractions_to_process:
                logger.info(f"Processing {len(daily_retractions_to_process)} retractions from daily file lists...")
                unique_daily_retractions = list(set(daily_retractions_to_process))
                updated_rows = adapter.handle_deletions(unique_daily_retractions)
                total_retracted_count += updated_rows
                logger.warning(f"Marked {updated_rows} articles as retracted based on daily lists.")

        status = "SUCCESS"

    except Exception:
        status = "FAILED"
        logger.critical("A critical error occurred during delta-load", exc_info=True)
        raise typer.Exit(code=1)
    finally:
        if adapter:
            if run_id:
                metrics = {
                    "archives_processed": total_archives_processed,
                    "total_articles_upserted": total_records_upserted,
                    "total_articles_retracted": total_retracted_count,
                    "download_workers": download_workers,
                    "parsing_workers": parsing_workers,
                }
                logger.info(f"Updating sync_history for run_id {run_id} with status '{status}'...")
                adapter.end_run(run_id, status, metrics)
            adapter.close()


if __name__ == "__main__":
    app()
