"""
Data acquisition module for downloading and extracting PMC data.
"""
from __future__ import annotations

import tarfile
import hashlib
import csv
import re
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import IO, Generator, List, Optional, Tuple
from urllib.parse import urljoin

import requests
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from lxml import etree

from py_load_pubmedcentral.models import (
    ArticleFileInfo,
    PmcArticlesContent,
    PmcArticlesMetadata,
)
from py_load_pubmedcentral.parser import parse_jats_xml


class DataSource(ABC):
    """Abstract Base Class for a data source (e.g., FTP, S3)."""

    @abstractmethod
    def get_article_file_list(self) -> List[ArticleFileInfo]:
        """
        Retrieves the complete list of article packages and their metadata.

        Returns:
            A list of ArticleFileInfo objects.
        """
        raise NotImplementedError

    @abstractmethod
    def get_retracted_pmcids(self) -> List[str]:
        """
        Retrieves the list of retracted PMCIDs from the source.

        Returns:
            A list of PMCID strings.
        """
        raise NotImplementedError

    @abstractmethod
    def download_file(self, url: str, destination_dir: Path) -> Path:
        """
        Downloads a file, verifies its integrity, and saves it locally.

        Args:
            url: The URL of the file to download.
            destination_dir: The local directory to save the file in.

        Returns:
            The Path to the downloaded file.
        """
        raise NotImplementedError


class NcbiFtpDataSource(DataSource):
    """Data source for the NCBI FTP server (via HTTPS)."""

    BASE_URL = "https://ftp.ncbi.nlm.nih.gov/pub/pmc/"
    FILE_LIST_URL = urljoin(BASE_URL, "oa_file_list.csv")
    RETRACTIONS_URL = urljoin(BASE_URL, "retractions.csv")

    def get_article_file_list(self) -> List[ArticleFileInfo]:
        """
        Downloads and parses the master file list (oa_file_list.csv) from
        the NCBI FTP server to get metadata for all article packages.
        This implementation is robust to changes in column order by using
        csv.DictReader.
        """
        print(f"Downloading master file list from {self.FILE_LIST_URL}...")
        try:
            response = requests.get(self.FILE_LIST_URL, stream=True)
            response.raise_for_status()
            # Use iter_lines and decode to handle streaming text
            lines = (line.decode('utf-8') for line in response.iter_lines())
            # Use DictReader for robust column handling
            reader = csv.DictReader(lines)

            file_list = []
            for row in reader:
                try:
                    # Normalize keys by stripping whitespace
                    row = {k.strip(): v for k, v in row.items()}

                    # Check for required fields
                    if not row.get("File") or not row.get("Accession ID"):
                        print(f"Skipping malformed row (missing required fields): {row}")
                        continue

                    is_retracted = row.get("Retracted", "").lower() in ('true', 'y', 'yes')
                    last_updated_str = row.get("Last Updated")
                    last_updated = (
                        datetime.strptime(last_updated_str, "%Y-%m-%d %H:%M:%S")
                        if last_updated_str else None
                    )

                    pmid = row.get("PMID")
                    file_info = ArticleFileInfo(
                        file_path=row["File"],
                        pmcid=row["Accession ID"],
                        pmid=pmid if pmid else None,
                        last_updated=last_updated,
                        is_retracted=is_retracted,
                    )
                    file_list.append(file_info)
                except (ValueError, TypeError) as e:
                    print(f"Skipping malformed row {row}: {e}")
                    continue

            print(f"Successfully parsed {len(file_list)} records from the file list.")
            return file_list

        except requests.RequestException as e:
            print(f"Failed to download file list: {e}")
            return []

    def get_retracted_pmcids(self) -> List[str]:
        """
        Downloads and parses the retractions.csv file from the NCBI FTP server.
        """
        print(f"Downloading retractions list from {self.RETRACTIONS_URL}...")
        try:
            response = requests.get(self.RETRACTIONS_URL)
            response.raise_for_status()
            response.encoding = 'utf-8'  # Ensure correct decoding
            lines = response.text.strip().splitlines()
            reader = csv.reader(lines)

            # Check for a header and skip it
            header = next(reader)
            if 'PMCID' not in header[0]:
                # If no header, process the first line as data
                return [header[0]] + [row[0] for row in reader if row]
            else:
                # If header exists, just process the rest
                return [row[0] for row in reader if row]

        except requests.RequestException as e:
            print(f"Failed to download retractions file: {e}")
            return []

    def download_file(self, url: str, destination_dir: Path) -> Path:
        """
        Downloads a file from a URL, verifies its MD5 checksum, and saves
        it to a local directory.

        Args:
            url: The URL of the .tar.gz file to download.
            destination_dir: The local directory to save the file in.

        Returns:
            The Path to the verified, downloaded file.

        Raises:
            IOError: If the downloaded file's checksum does not match the
                     expected checksum.
        """
        # 1. Download the main file
        local_filename = url.split('/')[-1]
        destination_path = destination_dir / local_filename
        print(f"Downloading {url} to {destination_path}...")
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(destination_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        # 2. Download the checksum file
        md5_url = url + ".md5"
        print(f"Downloading checksum from {md5_url}...")
        md5_response = requests.get(md5_url)
        md5_response.raise_for_status()

        # Expected format: "MD5(filename.tar.gz)= a1b2c3d4...\n"
        match = re.search(r"=\s*([a-f0-9]{32})", md5_response.text)
        if not match:
            raise IOError(f"Could not parse MD5 checksum from {md5_url}")
        expected_checksum = match.group(1)
        print(f"Expected checksum: {expected_checksum}")

        # 3. Calculate checksum of the downloaded file
        print(f"Calculating checksum for {destination_path}...")
        hasher = hashlib.md5()
        with open(destination_path, 'rb') as f:
            while chunk := f.read(8192):
                hasher.update(chunk)
        actual_checksum = hasher.hexdigest()
        print(f"Actual checksum:   {actual_checksum}")

        # 4. Compare checksums
        if actual_checksum != expected_checksum:
            # Clean up the corrupt file before raising
            destination_path.unlink()
            raise IOError(
                f"Checksum mismatch for {local_filename}. "
                f"Expected {expected_checksum}, got {actual_checksum}."
            )

        print(f"Checksum verified for {local_filename}.")
        return destination_path


class S3DataSource(DataSource):
    """Data source for the AWS S3 Open Data bucket (s3://pmc-oa-opendata)."""

    BUCKET_NAME = "pmc-oa-opendata"
    FILE_LIST_KEY = "oa_file_list.csv"
    RETRACTIONS_KEY = "retractions.csv"

    def __init__(self):
        self.s3 = boto3.client("s3", config=Config(signature_version="unsigned"))

    def get_article_file_list(self) -> List[ArticleFileInfo]:
        """
        Downloads and parses the master file list (oa_file_list.csv) from
        the S3 bucket, robust to column order changes using csv.DictReader.
        """
        print(f"Downloading master file list from S3 bucket: {self.BUCKET_NAME}")
        try:
            response = self.s3.get_object(Bucket=self.BUCKET_NAME, Key=self.FILE_LIST_KEY)
            # Use iter_lines and decode for streaming text from S3
            lines = (line.decode('utf-8') for line in response["Body"].iter_lines())
            reader = csv.DictReader(lines)

            file_list = []
            for row in reader:
                try:
                    # Normalize keys by stripping whitespace
                    row = {k.strip(): v for k, v in row.items()}

                    if not row.get("File") or not row.get("Accession ID"):
                        print(f"Skipping malformed row (missing required fields): {row}")
                        continue

                    is_retracted = row.get("Retracted", "").lower() in ('true', 'y', 'yes')
                    last_updated_str = row.get("Last Updated")
                    last_updated = (
                        datetime.strptime(last_updated_str, "%Y-%m-%d %H:%M:%S")
                        if last_updated_str else None
                    )

                    pmid = row.get("PMID")
                    file_info = ArticleFileInfo(
                        file_path=row["File"],
                        pmcid=row["Accession ID"],
                        pmid=pmid if pmid else None,
                        last_updated=last_updated,
                        is_retracted=is_retracted,
                    )
                    file_list.append(file_info)
                except (ValueError, TypeError) as e:
                    print(f"Skipping malformed row {row}: {e}")
                    continue

            print(f"Successfully parsed {len(file_list)} records from the file list.")
            return file_list

        except ClientError as e:
            print(f"Failed to download file list from S3: {e}")
            return []

    def get_retracted_pmcids(self) -> List[str]:
        """
        Downloads and parses the retractions.csv file from the S3 bucket.
        """
        print(f"Downloading retractions list from S3 bucket: {self.BUCKET_NAME}")
        try:
            response = self.s3.get_object(Bucket=self.BUCKET_NAME, Key=self.RETRACTIONS_KEY)
            lines = (line.decode('utf-8') for line in response["Body"].iter_lines())
            reader = csv.reader(lines)

            # Check for a header and skip it
            header = next(reader)
            if 'PMCID' not in header[0]:
                # If no header, process the first line as data
                return [header[0]] + [row[0] for row in reader if row]
            else:
                # If header exists, just process the rest
                return [row[0] for row in reader if row]

        except ClientError as e:
            # It's possible the retractions file doesn't exist, which is not a fatal error.
            if e.response['Error']['Code'] == 'NoSuchKey':
                print("No retractions.csv file found in S3 bucket. Continuing without it.")
                return []
            print(f"Failed to download retractions file from S3: {e}")
            return []

    def download_file(self, url: str, destination_dir: Path) -> Path:
        """
        Downloads a file from S3, verifies its integrity using server-side
        checksums, and saves it locally.

        Args:
            url: The S3 key of the object to download.
            destination_dir: The local directory to save the file in.

        Returns:
            The Path to the verified, downloaded file.

        Raises:
            IOError: If the download fails due to a client error or if the
                     destination directory does not exist.
        """
        s3_key = url  # Treat the URL parameter as the S3 key for this source
        local_filename = s3_key.split('/')[-1]
        destination_path = destination_dir / local_filename

        print(f"Downloading s3://{self.BUCKET_NAME}/{s3_key} to {destination_path}...")
        try:
            self.s3.download_file(
                Bucket=self.BUCKET_NAME,
                Key=s3_key,
                Filename=str(destination_path),
                ExtraArgs={"ChecksumMode": "ENABLED"},
            )
            print(f"Successfully downloaded and verified {local_filename}.")
            return destination_path
        except ClientError as e:
            # A ClientError can occur for various reasons, e.g., object not found
            # or checksum validation failure.
            print(f"Failed to download {s3_key}: {e}")
            raise IOError(f"Failed to download {s3_key} from S3.") from e


def stream_and_parse_tar_gz_archive(
    tar_gz_path: Path,
    article_info_lookup: dict[str, ArticleFileInfo],
) -> Generator[Tuple[PmcArticlesMetadata, PmcArticlesContent], None, None]:
    """
    Opens a local .tar.gz archive, finds XML files for target articles,
    parses them, and yields data models.

    This function streams the archive extraction. It parses each article,
    checks if its PMCID is in the lookup, and if so, enriches the article
    with metadata from the lookup (like the correct `source_last_updated`
    timestamp) before yielding it.

    Args:
        tar_gz_path: The local path to the .tar.gz archive to process.
        article_info_lookup: A dictionary mapping PMCID to ArticleFileInfo.
                             This determines which articles to process and provides
                             their metadata.

    Yields:
        A tuple of (PmcArticlesMetadata, PmcArticlesContent) for each targeted
        and successfully parsed article in the archive.
    """
    with tarfile.open(name=tar_gz_path, mode="r|gz") as tar:
        for member in tar:
            if member.isfile() and member.name.lower().endswith((".xml", ".nxml")):
                xml_file_obj = tar.extractfile(member)
                if not xml_file_obj:
                    continue

                try:
                    # We parse the XML, then decide if we want to keep it.
                    # This is more robust than relying on filenames.
                    # The is_retracted status is now passed from the file list info.
                    for metadata, content in parse_jats_xml(
                        xml_file_obj,
                        is_retracted=False, # Default, will be overridden below
                    ):
                        # Check if the parsed article is one we're looking for
                        if metadata.pmcid in article_info_lookup:
                            article_info = article_info_lookup[metadata.pmcid]
                            # Populate the metadata from our source of truth
                            metadata.source_last_updated = article_info.last_updated
                            metadata.is_retracted = article_info.is_retracted
                            yield metadata, content

                except etree.XMLSyntaxError as e:
                    print(f"Skipping malformed XML file {member.name}: {e}")
                    continue
                finally:
                    xml_file_obj.close()
