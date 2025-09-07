"""
Pydantic models for representing PubMed Central article data.

These models correspond to the database schema defined in the FRD.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class Contributor(BaseModel):
    """Represents a single contributor (e.g., author)."""
    name: str
    affiliation: Optional[str] = None
    orcid: Optional[str] = None


class JournalInfo(BaseModel):
    """Represents information about the journal."""
    name: str
    issn: Optional[str] = None
    publisher: Optional[str] = None


class LicenseInfo(BaseModel):
    """Represents licensing information for the article."""
    license_type: Optional[str] = Field(None, alias="type")
    url: Optional[str] = None


class PmcArticlesMetadata(BaseModel):
    """
    Represents the core queryable metadata for a PMC article.
    Corresponds to the 'pmc_articles_metadata' table.
    """
    pmcid: str = Field(..., description="Primary Key. (e.g., PMC123456).")
    pmid: Optional[int] = Field(None, description="PubMed ID (nullable).")
    doi: Optional[str] = Field(None, description="Digital Object Identifier (nullable).")
    title: str = Field(..., description="Article Title.")
    abstract_text: Optional[str] = Field(None, description="Extracted abstract.")
    publication_date: Optional[date] = Field(None, description="Date of publication.")
    journal_info: JournalInfo = Field(..., description="Nested structure for Journal Name, ISSN, Publisher.")
    contributors: List[Contributor] = Field(..., description="Array of objects: Author Name, Affiliation, ORCID.")
    license_info: Optional[LicenseInfo] = Field(None, description="Details of the OA license.")
    is_retracted: bool = Field(..., description="Flag based on NCBI metadata file.")
    source_last_updated: Optional[datetime] = Field(None, description="Timestamp provided by NCBI source.")
    sync_timestamp: datetime = Field(..., description="Timestamp when this record was last synchronized.")


class PmcArticlesContent(BaseModel):
    """
    Represents the full content of a PMC article.
    Corresponds to the 'pmc_articles_content' table.
    """
    pmcid: str = Field(..., description="Foreign Key to pmc_metadata. Primary Key.")
    raw_jats_xml: str = Field(..., description="The complete, original JATS XML source.")
    body_text: Optional[str] = Field(None, description="Extracted, cleaned full text from the <body>.")


class ArticleFileInfo(BaseModel):
    """Represents metadata for a single article archive from the NCBI file list."""

    file_path: str
    pmcid: str
    pmid: Optional[str] = None
    last_updated: Optional[datetime] = None
