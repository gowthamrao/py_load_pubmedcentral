"""
Database adapter interface and implementations for loading data.
"""
from __future__ import annotations

import io
import json
from abc import ABC, abstractmethod
from datetime import datetime
from typing import IO, Iterable, List, Optional

import psycopg2
from pydantic import BaseModel

from py_load_pubmedcentral.models import PmcArticlesMetadata


class DatabaseAdapter(ABC):
    """Abstract Base Class for database connectors."""

    @abstractmethod
    def connect(self, connection_params: dict):
        """Establish a connection to the database."""
        raise NotImplementedError

    @abstractmethod
    def validate_schema(self):
        """Validate that the target schema exists and is correctly configured."""
        raise NotImplementedError

    @abstractmethod
    def bulk_load_native(self, file_like_object: IO[str], target_table: str):
        """
        Load data from a file-like object using the database's native
        bulk loading mechanism.
        """
        raise NotImplementedError

    @abstractmethod
    def execute_upsert(self, staging_table: str, main_table: str):
        """
        Execute an UPSERT/MERGE operation from a staging table to a main table.
        """
        raise NotImplementedError

    @abstractmethod
    def handle_deletions(self, deletion_list: List[str]):
        """Handle records that need to be deleted or marked as retracted."""
        raise NotImplementedError

    @abstractmethod
    def update_state(self, last_file_processed: str):
        """Update the synchronization state in the database."""
        raise NotImplementedError

    @abstractmethod
    def execute_sql(self, sql_statement: str):
        """Execute a raw SQL statement."""
        raise NotImplementedError


class PostgreSQLAdapter(DatabaseAdapter):
    """Database adapter for PostgreSQL."""

    def __init__(self, connection_params: dict):
        self.connection_params = connection_params
        self.conn = None

    def connect(self):
        """Establish a connection to the PostgreSQL database."""
        self.conn = psycopg2.connect(**self.connection_params)

    def validate_schema(self):
        """
        (Placeholder) Validates that the required tables exist in the database.
        """
        print("Validating schema...")
        # In a real implementation, this would check for the existence of
        # pmc_articles_metadata, pmc_articles_content, and sync_history tables.
        pass

    def _prepare_tsv_row(self, model: BaseModel, columns: List[str]) -> str:
        """
        Converts a Pydantic model into a single, escaped, tab-separated string.
        """
        row_values = []
        for col in columns:
            value = getattr(model, col)
            if value is None:
                row_values.append(r"\N")  # Use \N for NULL in PostgreSQL COPY
            elif isinstance(value, str):
                # Escape tabs, newlines, and backslashes
                escaped_value = value.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n")
                row_values.append(escaped_value)
            elif isinstance(value, BaseModel):
                # For nested Pydantic models (JSONB fields)
                row_values.append(value.model_dump_json())
            else:
                row_values.append(str(value))
        return "\t".join(row_values) + "\n"


    def write_models_to_tsv_file(
        self, models: Iterable[BaseModel], columns: List[str], tsv_file: IO[str]
    ):
        """
        Transforms an iterable of Pydantic models and writes them to an
        open file handle as TSV rows.

        Args:
            models: An iterable of Pydantic models.
            columns: The list of column names, in order.
            tsv_file: An open file handle to write the TSV rows to.
        """
        for model in models:
            tsv_row = self._prepare_tsv_row(model, columns)
            tsv_file.write(tsv_row)

    def bulk_load_native(self, file_path: str, target_table: str):
        """
        Loads data into a PostgreSQL table from a TSV file path
        using the COPY command. This is highly efficient.

        Args:
            file_path: The path to the TSV file to load.
            target_table: The name of the target table in the database.
        """
        if not self.conn:
            self.connect()

        # SQL statement for bulk loading from STDIN
        sql = f"COPY {target_table} FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')"

        with self.conn.cursor() as cursor:
            with open(file_path, "r", encoding="utf-8") as f:
                # copy_expert is the most flexible and powerful way to use COPY
                cursor.copy_expert(sql, f)

        # Commit the transaction to make the changes persistent
        self.conn.commit()

    def start_run(self, run_type: str) -> int:
        """
        Creates a new record in the sync_history table to mark the start of a run.

        Args:
            run_type: The type of run, e.g., 'FULL' or 'DELTA'.

        Returns:
            The run_id for the newly created record.
        """
        if not self.conn:
            self.connect()

        sql = """
            INSERT INTO sync_history (run_type, start_time, status)
            VALUES (%s, %s, %s)
            RETURNING run_id;
        """
        with self.conn.cursor() as cursor:
            cursor.execute(sql, (run_type, datetime.utcnow(), "RUNNING"))
            run_id = cursor.fetchone()[0]
            self.conn.commit()
        return run_id

    def end_run(self, run_id: int, status: str, metrics: Optional[dict] = None):
        """
        Updates a sync_history record to mark the end of a run.

        Args:
            run_id: The ID of the run to update.
            status: The final status, e.g., 'SUCCESS' or 'FAILED'.
            metrics: A dictionary of metrics to store as JSON.
        """
        if not self.conn:
            self.connect()

        sql = """
            UPDATE sync_history
            SET end_time = %s, status = %s, metrics = %s
            WHERE run_id = %s;
        """
        metrics_json = json.dumps(metrics) if metrics else None
        with self.conn.cursor() as cursor:
            cursor.execute(sql, (datetime.utcnow(), status, metrics_json, run_id))
            self.conn.commit()

    def execute_upsert(self, staging_table: str, main_table: str):
        """
        (Placeholder) Executes an INSERT...ON CONFLICT...DO UPDATE operation.
        """
        raise NotImplementedError("Delta load logic is not yet implemented.")

    def handle_deletions(self, deletion_list: List[str]):
        """(Placeholder) Handles article retractions."""
        raise NotImplementedError("Delta load logic is not yet implemented.")

    def update_state(self, last_file_processed: str):
        """(Placeholder) Updates the sync_history table."""
        raise NotImplementedError("Delta load logic is not yet implemented.")

    def execute_sql(self, sql_statement: str):
        """Executes a multi-statement SQL string."""
        if not self.conn:
            self.connect()
        with self.conn.cursor() as cursor:
            cursor.execute(sql_statement)
        self.conn.commit()

    def close(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
