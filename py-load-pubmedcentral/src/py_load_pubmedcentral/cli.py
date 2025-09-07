"""
Command-line interface for py-load-pubmedcentral.
"""
from __future__ import annotations

import tempfile
from pathlib import Path

import typer

from py_load_pubmedcentral.acquisition import (
    NcbiFtpDataSource,
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


@app.command()
def full_load(
    batch_size: int = typer.Option(5000, "--batch-size", "-b", help="Number of records to report progress by."),
):
    """
    Execute a full (baseline) load by discovering and processing all
    baseline archives from the NCBI FTP source.
    """
    typer.echo("--- Starting Full Baseline Load ---")
    adapter = get_db_adapter()
    data_source = NcbiFtpDataSource()
    run_id = None
    status = "FAILED"  # Assume failure unless explicitly marked as success
    files_processed_count = 0
    total_records_count = 0

    try:
        run_id = adapter.start_run(run_type="FULL")
        typer.echo(f"Sync history started for run_id: {run_id}")

        typer.echo("Discovering baseline files from NCBI...")
        baseline_files = data_source.list_baseline_files()
        if not baseline_files:
            typer.secho("No baseline files found. Exiting.", fg=typer.colors.YELLOW)
            status = "SUCCESS" # Not a failure, just nothing to do.
            raise typer.Exit()

        typer.echo(f"Found {len(baseline_files)} baseline archives to process.")

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            typer.echo(f"Using temporary directory: {tmp_path}")

            for i, file_url in enumerate(baseline_files):
                typer.echo(f"\n--- Processing file {i+1} of {len(baseline_files)}: {file_url} ---")

                try:
                    verified_path = data_source.download_file(file_url, tmp_path)
                except Exception as e:
                    typer.secho(f"Failed to download/verify {file_url}. Error: {e}", fg=typer.colors.RED)
                    continue

                records_in_batch = 0
                metadata_tsv_path = tmp_path / "metadata.tsv"
                content_tsv_path = tmp_path / "content.tsv"

                article_generator = stream_and_parse_tar_gz_archive(verified_path)
                metadata_columns = list(PmcArticlesMetadata.model_fields.keys())
                content_columns = list(PmcArticlesContent.model_fields.keys())

                with open(metadata_tsv_path, "w", encoding="utf-8") as meta_f, \
                     open(content_tsv_path, "w", encoding="utf-8") as content_f:
                    for metadata, content in article_generator:
                        meta_f.write(adapter._prepare_tsv_row(metadata, metadata_columns))
                        content_f.write(adapter._prepare_tsv_row(content, content_columns))
                        records_in_batch += 1
                        if records_in_batch % batch_size == 0:
                            typer.echo(f"  ...processed {records_in_batch} records...")

                typer.secho(f"Parsed {records_in_batch} records from this batch.", fg=typer.colors.GREEN)

                if records_in_batch > 0:
                    typer.echo("Loading TSV files into the database...")
                    adapter.bulk_load_native(str(metadata_tsv_path), "pmc_articles_metadata")
                    adapter.bulk_load_native(str(content_tsv_path), "pmc_articles_content")
                    typer.secho("Database load complete for this batch.", fg=typer.colors.GREEN)

                files_processed_count += 1
                total_records_count += records_in_batch
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
def delta_load():
    """
    Execute an incremental (delta) load from the last known state.
    (This is a placeholder for future implementation).
    """
    typer.echo("--- Starting Delta Load ---")
    typer.echo("Identifying new/updated/deleted files since last run...")
    typer.secho("Delta load logic is not yet implemented.", fg=typer.colors.YELLOW)


if __name__ == "__main__":
    app()
