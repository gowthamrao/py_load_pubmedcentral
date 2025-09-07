--
-- DDL for the py-load-pubmedcentral database schema.
-- Based on the Functional Requirements Document (FRD).
--

-- Table 1: pmc_articles_metadata (Core queryable metadata)
CREATE TABLE IF NOT EXISTS pmc_articles_metadata (
    pmcid VARCHAR(20) PRIMARY KEY,
    pmid BIGINT NULL,
    doi VARCHAR(255) NULL,
    title TEXT,
    abstract_text TEXT,
    publication_date DATE,
    journal_info JSONB,
    contributors JSONB,
    license_info JSONB,
    is_retracted BOOLEAN NOT NULL DEFAULT FALSE,
    source_last_updated TIMESTAMPTZ,
    sync_timestamp TIMESTAMPTZ NOT NULL
);

-- Table 2: pmc_articles_content (Full representation, separated for performance)
CREATE TABLE IF NOT EXISTS pmc_articles_content (
    pmcid VARCHAR(20) PRIMARY KEY,
    raw_jats_xml TEXT, -- Using TEXT for broader compatibility, XML type is also an option
    body_text TEXT,
    CONSTRAINT fk_pmcid
        FOREIGN KEY(pmcid)
        REFERENCES pmc_articles_metadata(pmcid)
        ON DELETE CASCADE
);

-- Table 3: sync_history (State Management)
CREATE TABLE IF NOT EXISTS sync_history (
    run_id SERIAL PRIMARY KEY,
    run_type VARCHAR(10) NOT NULL, -- 'FULL' or 'DELTA'
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ,
    status VARCHAR(20) NOT NULL, -- 'SUCCESS', 'FAILED', 'RUNNING'
    last_file_processed VARCHAR(255),
    metrics JSONB
);

-- Add indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_pmid ON pmc_articles_metadata(pmid);
CREATE INDEX IF NOT EXISTS idx_doi ON pmc_articles_metadata(doi);
CREATE INDEX IF NOT EXISTS idx_pub_date ON pmc_articles_metadata(publication_date);
CREATE INDEX IF NOT EXISTS idx_sync_history_status ON sync_history(status, run_type);
