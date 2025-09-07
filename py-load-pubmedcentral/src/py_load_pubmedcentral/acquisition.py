"""
Data acquisition module for downloading and extracting PMC data.
"""
from __future__ import annotations

import tarfile
import hashlib
import csv
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
    def list_baseline_files(self) -> List[str]:
        """
        Lists the URLs of the baseline (full) dataset archives.

        Returns:
            A list of URLs pointing to the .tar.gz files.
        """
        raise NotImplementedError

    @abstractmethod
    def get_article_file_list(self) -> List[ArticleFileInfo]:
        """
        Retrieves the complete list of article packages and their metadata.

        Returns:
            A list of ArticleFileInfo objects.
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

    def get_article_file_list(self) -> List[ArticleFileInfo]:
        """
        Downloads and parses the master file list (oa_file_list.csv) from
        the NCBI FTP server to get metadata for all article packages.

        Returns:
            A list of ArticleFileInfo objects for all articles.
        """
        print(f"Downloading master file list from {self.FILE_LIST_URL}...")
        try:
            response = requests.get(self.FILE_LIST_URL, stream=True)
            response.raise_for_status()

            # Decode the content as text and read it with the csv module
            # Using iterator to avoid loading the whole file into memory at once
            lines = (line.decode('utf-8') for line in response.iter_lines())
            reader = csv.reader(lines)

            # Skip header row
            header = next(reader)
            print(f"Parsing CSV with header: {header}")

            # Expected header: ['File', 'Accession ID', 'PMID', 'Last Updated']
            # We map this to our Pydantic model
            file_list = []
            for row in reader:
                try:
                    # Some PMIDs might be missing or empty strings
                    pmid_val = row[2] if row[2] and row[2].strip() else None

                    # Parse timestamp, e.g., '2023-12-15 13:31:48'
                    last_updated_val = datetime.strptime(row[3], "%Y-%m-%d %H:%M:%S") if row[3] else None

                    file_info = ArticleFileInfo(
                        file_path=row[0],
                        pmcid=row[1],
                        pmid=pmid_val,
                        last_updated=last_updated_val,
                    )
                    file_list.append(file_info)
                except (IndexError, ValueError, TypeError) as e:
                    print(f"Skipping malformed row {row}: {e}")
                    continue

            print(f"Successfully parsed {len(file_list)} records from the file list.")
            return file_list

        except requests.RequestException as e:
            print(f"Failed to download file list: {e}")
            # Depending on desired robustness, could implement retries here
            return []

    def list_baseline_files(self) -> List[str]:
        """
        Provides a list of full URLs to all article package archives.

        This implementation uses the master file list as the source of truth.
        """
        article_infos = self.get_article_file_list()

        # The file paths in the CSV are relative to the FTP root's /pub/pmc/
        # e.g., 'oa_package/a5/39/PMC10534341.tar.gz'
        # We construct the full URL for each.
        return [urljoin(self.BASE_URL, info.file_path) for info in article_infos]

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

    def __init__(self):
        # The PMC S3 bucket is public, so we don't need credentials.
        # We can also add retry logic here if needed, via botocore.config.Config
        # For now, we use the default unsigned configuration.
        self.s3 = boto3.client("s3", config=Config(signature_version="unsigned"))

    def get_article_file_list(self) -> List[ArticleFileInfo]:
        """
        Downloads and parses the master file list (oa_file_list.csv) from
        the S3 bucket to get metadata for all article packages.

        Returns:
            A list of ArticleFileInfo objects for all articles.
        """
        print(f"Downloading master file list from S3 bucket: {self.BUCKET_NAME}")
        try:
            response = self.s3.get_object(Bucket=self.BUCKET_NAME, Key=self.FILE_LIST_KEY)
            # get_object returns a StreamingBody, which we can iterate over
            # to avoid loading the whole file into memory.
            lines = (line.decode('utf-8') for line in response["Body"].iter_lines())
            reader = csv.reader(lines)

            # Skip header row
            header = next(reader)
            print(f"Parsing CSV with header: {header}")

            file_list = []
            for row in reader:
                try:
                    pmid_val = row[2] if row[2] and row[2].strip() else None
                    last_updated_val = datetime.strptime(row[3], "%Y-%m-%d %H:%M:%S") if row[3] else None

                    file_info = ArticleFileInfo(
                        file_path=row[0],
                        pmcid=row[1],
                        pmid=pmid_val,
                        last_updated=last_updated_val,
                    )
                    file_list.append(file_info)
                except (IndexError, ValueError, TypeError) as e:
                    print(f"Skipping malformed row {row}: {e}")
                    continue

            print(f"Successfully parsed {len(file_list)} records from the file list.")
            return file_list

        except ClientError as e:
            print(f"Failed to download file list from S3: {e}")
            return []

    def list_baseline_files(self) -> List[str]:
        """
        Lists the S3 keys of all baseline .tar.gz archives in the bucket.

        This uses a paginator to efficiently handle the large number of
        objects in the PMC S3 bucket.

        Returns:
            A list of S3 object keys for all .tar.gz files found.
        """
        print(f"Listing baseline .tar.gz files in S3 bucket: {self.BUCKET_NAME}...")
        baseline_files = []
        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.BUCKET_NAME)

            for page in pages:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        key = obj["Key"]
                        if key.lower().endswith(".tar.gz"):
                            baseline_files.append(key)

            print(f"Found {len(baseline_files)} baseline files.")
            return baseline_files

        except ClientError as e:
            print(f"Failed to list files in S3 bucket: {e}")
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


import re


def stream_and_parse_tar_gz_archive(
    tar_gz_path: Path,
    source_last_updated: Optional[datetime],
    is_retracted: bool,
) -> Generator[Tuple[PmcArticlesMetadata, PmcArticlesContent], None, None]:
    """
    Opens a local .tar.gz archive, extracts XML files in memory,
    and parses them, yielding data models for each article.

    This function streams the archive extraction to keep memory usage low.

    Args:
        tar_gz_path: The local path to the .tar.gz archive to process.
        source_last_updated: The timestamp from the source metadata.
        is_retracted: The retraction status from the source metadata.

    Yields:
        A tuple of (PmcArticlesMetadata, PmcArticlesContent) for each
        article found and successfully parsed in the archive.
    """
    # tarfile can open a file path directly and will handle decompression.
    with tarfile.open(name=tar_gz_path, mode="r|gz") as tar:
        # Iterate through each member (file) in the tar archive
        for member in tar:
            if member.isfile() and member.name.lower().endswith((".xml", ".nxml")):
                # extractfile() returns a file-like object for reading the member's content.
                # This is read into memory, but only one file at a time.
                xml_file_obj = tar.extractfile(member)
                if xml_file_obj:
                    try:
                        # The parser expects a file-like object, which we have.
                        # We 'yield from' to pass on the generator's output.
                        yield from parse_jats_xml(
                            xml_file_obj,
                            source_last_updated=source_last_updated,
                            is_retracted=is_retracted,
                        )
                    except etree.XMLSyntaxError as e:
                        # Log the error for the specific file and continue
                        print(f"Skipping malformed XML file {member.name}: {e}")
                        continue
                    finally:
                        xml_file_obj.close()
