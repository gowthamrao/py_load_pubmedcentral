"""
Command-line interface for py-load-pubmedcentral.
"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import List

import typer

from py_load_pubmedcentral.parser import parse_jats_xml
from py_load_pubmedcentral.models import PmcArticlesMetadata, PmcArticlesContent


app = typer.Typer(
    help="A high-throughput pipeline for synchronizing PubMed Central into a relational database."
)


@app.command()
def initialize(
    db_schema_file: str = typer.Option(
        "schemas/pmc_schema.sql",
        "--schema",
        "-s",
        help="Path to the SQL file containing the database schema.",
    )
):
    """
    Initialize the database schema (tables, indexes, etc.).
    """
    typer.echo("--- Initializing Database Schema ---")
    adapter = get_db_adapter()
    typer.echo(f"Connecting to PostgreSQL at {adapter.connection_params['host']}...")

    # In a real scenario, we would read the SQL file and execute it.
    # adapter.connect()
    # with open(db_schema_file, 'r') as f:
    #     adapter.execute_sql(f.read())
    # adapter.conn.close()

    typer.echo(f"Simulating execution of schema from '{db_schema_file}'...")
    typer.echo("Schema initialization complete (simulation).")


import tempfile
from pathlib import Path

from py_load_pubmedcentral.models import PmcArticlesMetadata, PmcArticlesContent


@app.command()
def full_load(
    xml_file: Path = typer.Argument(..., help="Path to the JATS XML file to load.", exists=True),
    batch_size: int = typer.Option(5000, "--batch-size", "-b", help="Number of records to process per batch."),
):
    """
    Execute a full (baseline) load from a single XML source file.
    This command first parses the XML and writes to intermediate TSV files,
    then connects to the database to perform a bulk load. This separation
    avoids potential memory conflicts between the XML parsing and database libraries.
    """
    typer.echo(f"--- Starting Full Load for {xml_file} ---")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        metadata_tsv_path = tmp_path / "metadata.tsv"
        content_tsv_path = tmp_path / "content.tsv"
        typer.echo(f"Using temporary directory for TSV files: {tmp_path}")

        # --- Phase 1: Parse XML and Write to TSV ---
        # No database connection is active in this phase.
        typer.echo("Phase 1: Parsing XML and generating intermediate TSV files...")
        try:
            from pydantic import BaseModel
            def _prepare_tsv_row(model: BaseModel, columns: List[str]) -> str:
                row_values = []
                for col in columns:
                    value = getattr(model, col)
                    if value is None:
                        row_values.append(r"\N")
                    elif isinstance(value, str):
                        escaped_value = value.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n")
                        row_values.append(escaped_value)
                    elif isinstance(value, BaseModel):
                        row_values.append(value.model_dump_json())
                    else:
                        row_values.append(str(value))
                return "\t".join(row_values) + "\n"

            with open(xml_file, "rb") as f, \
                 open(metadata_tsv_path, "w", encoding="utf-8") as meta_f, \
                 open(content_tsv_path, "w", encoding="utf-8") as content_f:

                parser = parse_jats_xml(f)

                metadata_models, content_models = [], []
                total_processed = 0

                metadata_columns = list(PmcArticlesMetadata.model_fields.keys())
                content_columns = list(PmcArticlesContent.model_fields.keys())

                for i, (metadata, content) in enumerate(parser):
                    meta_f.write(_prepare_tsv_row(metadata, metadata_columns))
                    content_f.write(_prepare_tsv_row(content, content_columns))
                    total_processed += 1
                    if total_processed % batch_size == 0:
                        typer.echo(f"Processed {total_processed} records...")

            typer.secho(f"Phase 1 complete. Total records processed: {total_processed}", fg=typer.colors.GREEN)

            typer.secho(f"Phase 1 complete. Total records processed: {total_processed}", fg=typer.colors.GREEN)

        except Exception as e:
            typer.secho(f"An unexpected error occurred during Phase 1 (Parsing): {e}", fg=typer.colors.RED)
            raise typer.Exit(code=1)

        # --- Phase 2: Load TSV into Database ---
        # Now, we connect to the database by performing local imports.
        typer.echo("\nPhase 2: Loading TSV files into the database...")
        try:
            from py_load_pubmedcentral.db import PostgreSQLAdapter

            def get_db_adapter() -> PostgreSQLAdapter:
                """Creates a PostgreSQLAdapter instance from environment variables."""
                connection_params = {
                    "dbname": os.environ.get("DB_NAME", "pmc_db"),
                    "user": os.environ.get("DB_USER", "user"),
                    "password": os.environ.get("DB_PASSWORD", "password"),
                    "host": os.environ.get("DB_HOST", "localhost"),
                    "port": os.environ.get("DB_PORT", "5432"),
                }
                return PostgreSQLAdapter(connection_params)

            adapter = get_db_adapter()

            typer.echo(f"Loading metadata from {metadata_tsv_path}...")
            adapter.bulk_load_native(str(metadata_tsv_path), "pmc_articles_metadata")

            typer.echo(f"Loading content from {content_tsv_path}...")
            adapter.bulk_load_native(str(content_tsv_path), "pmc_articles_content")

            typer.secho("Phase 2 complete. Full load process finished successfully (simulation).", fg=typer.colors.GREEN)

        except Exception as e:
            typer.secho(f"An unexpected error occurred during Phase 2 (Loading): {e}", fg=typer.colors.RED)
            raise typer.Exit(code=1)


@app.command()
def delta_load():
    """
    Execute an incremental (delta) load from the last known state.
    (This is a placeholder for future implementation).
    """
    typer.echo("--- Starting Delta Load ---")
    typer.echo("Identifying new/updated/deleted files since last run...")
    # 1. Get last state from sync_history table
    # 2. Download new files from FTP/S3
    # 3. Process files:
    #    - Load new data to staging table
    #    - Execute UPSERT from staging to main
    #    - Handle retractions/deletions
    # 4. Update sync_history table
    typer.secho("Delta load logic is not yet implemented.", fg=typer.colors.YELLOW)


if __name__ == "__main__":
    app()
