"""
Command-line interface for py-load-pubmedcentral.
"""
from __future__ import annotations

import tempfile
from pathlib import Path

import typer

from py_load_pubmedcentral.acquisition import stream_and_parse_tar_gz_archive
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
    tar_gz_url: str = typer.Argument(..., help="URL of the .tar.gz archive to load."),
    batch_size: int = typer.Option(5000, "--batch-size", "-b", help="Number of records to report progress by."),
):
    """
    Execute a full (baseline) load from a .tar.gz archive URL.
    """
    typer.echo(f"--- Starting Full Load for {tar_gz_url} ---")
    adapter = get_db_adapter()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        metadata_tsv_path = tmp_path / "metadata.tsv"
        content_tsv_path = tmp_path / "content.tsv"
        typer.echo(f"Using temporary directory for TSV files: {tmp_path}")

        # --- Phase 1: Download, Parse, and Write to TSV ---
        typer.echo("Phase 1: Streaming archive, parsing XML, and generating intermediate TSV files...")
        total_processed = 0
        try:
            # Get the generator for parsed articles from the new acquisition module
            article_generator = stream_and_parse_tar_gz_archive(tar_gz_url)

            metadata_columns = list(PmcArticlesMetadata.model_fields.keys())
            content_columns = list(PmcArticlesContent.model_fields.keys())

            with open(metadata_tsv_path, "w", encoding="utf-8") as meta_f, \
                 open(content_tsv_path, "w", encoding="utf-8") as content_f:

                for metadata, content in article_generator:
                    meta_row = adapter._prepare_tsv_row(metadata, metadata_columns)
                    content_row = adapter._prepare_tsv_row(content, content_columns)
                    meta_f.write(meta_row)
                    content_f.write(content_row)

                    total_processed += 1
                    if total_processed % batch_size == 0:
                        typer.echo(f"Processed {total_processed} records...")

            if total_processed == 0:
                typer.secho("Warning: No articles were found or processed from the archive.", fg=typer.colors.YELLOW)
            else:
                typer.secho(f"Phase 1 complete. Total records processed: {total_processed}", fg=typer.colors.GREEN)

        except Exception as e:
            typer.secho(f"An unexpected error occurred during Phase 1 (Acquisition/Parsing): {e}", fg=typer.colors.RED)
            raise typer.Exit(code=1)

        # --- Phase 2: Load TSV into Database ---
        if total_processed > 0:
            typer.echo("\nPhase 2: Loading TSV files into the database...")
            try:
                typer.echo(f"Loading metadata from {metadata_tsv_path} into pmc_articles_metadata...")
                adapter.bulk_load_native(str(metadata_tsv_path), "pmc_articles_metadata")
                typer.echo("Metadata load complete.")

                typer.echo(f"Loading content from {content_tsv_path} into pmc_articles_content...")
                adapter.bulk_load_native(str(content_tsv_path), "pmc_articles_content")
                typer.echo("Content load complete.")

                typer.secho("Phase 2 complete. Full load process finished successfully.", fg=typer.colors.GREEN)

            except Exception as e:
                typer.secho(f"An unexpected error occurred during Phase 2 (Loading): {e}", fg=typer.colors.RED)
                raise typer.Exit(code=1)
            finally:
                adapter.close()
        else:
            typer.echo("Skipping database load as no records were processed.")


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
