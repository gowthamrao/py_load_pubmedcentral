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

from py_load_pubmedcentral.models import PmcArticlesContent, PmcArticlesMetadata


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
    def get_last_successful_run_info(self, run_type: str = "DELTA") -> Optional[tuple[datetime, str]]:
        """
        Retrieves the end_time and last_file_processed from the last successful run
        of a specific type.
        """
        raise NotImplementedError

    @abstractmethod
    def bulk_upsert_articles(self, metadata_file_path: str, content_file_path: str):
        """
        Atomically upserts article data from intermediate files into the
        metadata and content tables.
        """
        raise NotImplementedError

    @abstractmethod
    def handle_deletions(self, deletion_list: List[str]):
        """Handle records that need to be deleted or marked as retracted."""
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

    def end_run(
        self,
        run_id: int,
        status: str,
        metrics: Optional[dict] = None,
        last_file_processed: Optional[str] = None,
    ):
        """
        Updates a sync_history record to mark the end of a run.

        Args:
            run_id: The ID of the run to update.
            status: The final status, e.g., 'SUCCESS' or 'FAILED'.
            metrics: A dictionary of metrics to store as JSON.
            last_file_processed: The name/path of the last file processed in the run.
        """
        if not self.conn:
            self.connect()

        sql = """
            UPDATE sync_history
            SET end_time = %s, status = %s, metrics = %s, last_file_processed = %s
            WHERE run_id = %s;
        """
        metrics_json = json.dumps(metrics) if metrics else None
        with self.conn.cursor() as cursor:
            cursor.execute(
                sql, (datetime.utcnow(), status, metrics_json, last_file_processed, run_id)
            )
            self.conn.commit()

    def bulk_upsert_articles(self, metadata_file_path: str, content_file_path: str):
        """
        Performs a transactional bulk "upsert" (insert or update) for article
        data. It uses temporary tables and PostgreSQL's INSERT...ON CONFLICT
        command to efficiently merge new and updated data.
        """
        if not self.conn:
            self.connect()

        metadata_columns = list(PmcArticlesMetadata.model_fields.keys())
        content_columns = list(PmcArticlesContent.model_fields.keys())

        with self.conn.cursor() as cursor:
            # 1. Create temp tables that are automatically dropped on commit
            cursor.execute("CREATE TEMP TABLE staging_metadata (LIKE pmc_articles_metadata) ON COMMIT DROP;")
            cursor.execute("CREATE TEMP TABLE staging_content (LIKE pmc_articles_content) ON COMMIT DROP;")

            # 2. Bulk load data from files into the staging tables
            sql_copy = "COPY {} FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')"
            with open(metadata_file_path, "r", encoding="utf-8") as f:
                cursor.copy_expert(sql_copy.format("staging_metadata"), f)
            with open(content_file_path, "r", encoding="utf-8") as f:
                cursor.copy_expert(sql_copy.format("staging_content"), f)

            # 3. Upsert metadata, returning the pmcids of rows that were
            #    actually inserted or updated.
            metadata_cols_str = ", ".join(metadata_columns)
            update_cols_str = ", ".join([f"{col} = EXCLUDED.{col}" for col in metadata_columns if col != 'pmcid'])

            upsert_metadata_sql = f"""
                WITH upserted AS (
                    INSERT INTO pmc_articles_metadata ({metadata_cols_str})
                    SELECT * FROM staging_metadata
                    ON CONFLICT (pmcid) DO UPDATE SET
                        {update_cols_str}
                    WHERE pmc_articles_metadata.source_last_updated IS NULL
                       OR pmc_articles_metadata.source_last_updated < EXCLUDED.source_last_updated
                    RETURNING pmcid
                )
                SELECT pmcid FROM upserted;
            """
            cursor.execute(upsert_metadata_sql)
            affected_pmcids = [row[0] for row in cursor.fetchall()]

            if not affected_pmcids:
                self.conn.commit()
                return

            # 4. Upsert content only for the articles whose metadata was affected.
            #    This ensures data consistency.
            content_cols_str = ", ".join(content_columns)
            update_content_cols_str = ", ".join([f"{col} = EXCLUDED.{col}" for col in content_columns if col != 'pmcid'])
            pmcids_tuple = tuple(affected_pmcids)

            upsert_content_sql = f"""
                INSERT INTO pmc_articles_content ({content_cols_str})
                SELECT {content_cols_str} FROM staging_content
                WHERE pmcid IN %s
                ON CONFLICT (pmcid) DO UPDATE SET
                    {update_content_cols_str};
            """
            cursor.execute(upsert_content_sql, (pmcids_tuple,))

        self.conn.commit()

    def get_last_successful_run_info(self, run_type: str = "DELTA") -> Optional[tuple[datetime, str]]:
        """
        Retrieves the end_time and last_file_processed from the last successful run
        of a specific type.

        This is used as the starting point for a delta load to ensure exactly-once
        processing and to know which files are safe to skip.

        Args:
            run_type: The type of run to look for ('DELTA' or 'FULL').

        Returns:
            A tuple containing (end_time, last_file_processed) or None if no
            successful run is found.
        """
        if not self.conn:
            self.connect()

        sql = """
            SELECT end_time, last_file_processed FROM sync_history
            WHERE status = 'SUCCESS' AND run_type = %s
            ORDER BY end_time DESC
            LIMIT 1;
        """
        with self.conn.cursor() as cursor:
            cursor.execute(sql, (run_type,))
            result = cursor.fetchone()
            if result:
                return result[0], result[1]
        return None

    def handle_deletions(self, pmcids_to_retract: List[str]) -> int:
        """
        Marks a list of articles as retracted in the database.

        This is an idempotent operation that sets `is_retracted = TRUE` for
        the given list of PMCIDs.

        Args:
            pmcids_to_retract: A list of PMCID strings to mark as retracted.

        Returns:
            The number of rows that were updated.
        """
        if not self.conn:
            self.connect()

        if not pmcids_to_retract:
            return 0

        sql = """
            UPDATE pmc_articles_metadata
            SET is_retracted = TRUE,
                sync_timestamp = %s
            WHERE pmcid = ANY(%s) AND is_retracted = FALSE;
        """
        with self.conn.cursor() as cursor:
            # We pass the list of PMCIDs as a tuple for the `ANY` operator
            cursor.execute(sql, (datetime.utcnow(), pmcids_to_retract))
            updated_rows = cursor.rowcount
            self.conn.commit()

        return updated_rows

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
