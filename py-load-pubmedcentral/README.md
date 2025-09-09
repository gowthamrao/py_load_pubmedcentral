# py-load-pubmedcentral

A high-throughput, scalable, and reliable pipeline for synchronizing the PubMed Central (PMC) Open Access (OA) Subset and the Author Manuscript Dataset into a remote PostgreSQL database.

This package provides a command-line interface (`pmc-sync`) to manage the end-to-end process of downloading, parsing, and loading PMC data.

## Features

- **High-Performance Processing**: Uses `lxml` with iterative parsing for low memory usage and multi-core processing for high throughput.
- **Efficient Database Loading**: Leverages PostgreSQL's native `COPY` command for rapid data ingestion.
- **Robust Synchronization**: Supports both full baseline loads and incremental delta loads to keep the database up-to-date.
- **Resilient Downloads**: Automatically retries on transient network errors.
- **Extensible Design**: Built with a modular adapter pattern to potentially support other database backends in the future.
- **Multiple Data Sources**: Can acquire data from both NCBI's FTP server and the AWS S3 Open Data bucket.

## Installation

This project is managed with Poetry.

1.  Clone the repository.
2.  Install dependencies:
    ```bash
    poetry install
    ```

## Configuration

The application is configured via environment variables. The following variables are required for database connectivity:

| Environment Variable | Description                        | Default Value |
| -------------------- | ---------------------------------- | ------------- |
| `PMC_DB_HOST`        | The hostname of the database.      | `localhost`   |
| `PMC_DB_PORT`        | The port of the database.          | `5432`        |
| `PMC_DB_USER`        | The database user.                 | `postgres`    |
| `PMC_DB_PASSWORD`    | The password for the database user.| `postgres`    |
| `PMC_DB_NAME`        | The name of the database.          | `pmc`         |

You can set these variables in your shell or use a `.env` file in the project root directory.

## Usage

The command-line tool `pmc-sync` is the main entry point for all operations.

### 1. Initialize the Database

Before running any data loading, you must initialize the database schema. This will create the necessary tables and indexes.

```bash
poetry run pmc-sync initialize
```

By default, it looks for the schema file at `py-load-pubmedcentral/schemas/pmc_schema.sql`.

### 2. Perform a Full Load

A full load will erase any existing data and load the entire PMC Open Access baseline dataset. This can take a significant amount of time and resources.

```bash
# Using the default S3 data source
poetry run pmc-sync full-load

# Or specifying the FTP data source
poetry run pmc-sync full-load --source ftp
```

### 3. Perform a Delta Load

A delta load will find and process only the incremental updates that have been released since the last successful run. This should be run periodically (e.g., daily) to keep the database synchronized.

```bash
poetry run pmc-sync delta-load
```

## Testing

This project uses `pytest` for testing. To run the full test suite, including integration tests that require Docker:

1.  Ensure you have Docker installed and running.
2.  Install the development dependencies:
    ```bash
    poetry install
    ```
3.  Run the tests:
    ```bash
    poetry run pytest
    ```

The test suite includes:
- Unit tests for individual components.
- Integration tests that use a temporary PostgreSQL database in a Docker container to validate database interactions.
- A full end-to-end test for the `full-load` command to ensure the entire pipeline is working correctly.
