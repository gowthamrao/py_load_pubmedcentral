"""
Data acquisition module for downloading and extracting PMC data.
"""
from __future__ import annotations

import hashlib
import csv
import re
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, List
from urllib.parse import urljoin

import requests
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from py_load_pubmedcentral.models import (
    ArticleFileInfo,
)


class IncrementalUpdateInfo(BaseModel):
    """Represents a single incremental update package found on the source."""

    archive_path: str  # URL or S3 key for the .tar.gz file
    file_list_path: str  # URL or S3 key for the .filelist.csv file
    date: datetime


class DataSource(ABC):
    """Abstract Base Class for a data source (e.g., FTP, S3)."""

    @abstractmethod
    def get_article_file_list(self) -> List[ArticleFileInfo]:
        """
        Retrieves the complete list of article packages and their metadata for a full load.

        Returns:
            A list of ArticleFileInfo objects.
        """
        raise NotImplementedError

    @abstractmethod
    def get_incremental_updates(self, since: datetime) -> List[IncrementalUpdateInfo]:
        """
        Retrieves a list of incremental update packages since a given date.

        Args:
            since: The timestamp of the last successful run.

        Returns:
            A list of IncrementalUpdateInfo objects for packages needing processing.
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

    @abstractmethod
    def stream_article_infos_from_file_list(self, file_list_path: str) -> Generator[ArticleFileInfo, None, None]:
        """
        Downloads and streams ArticleFileInfo records from a given file list path (URL or S3 key).
        """
        raise NotImplementedError


class NcbiFtpDataSource(DataSource):
    """Data source for the NCBI FTP server (via HTTPS)."""

    BASE_URL = "https://ftp.ncbi.nlm.nih.gov/pub/pmc/"
    FILE_LIST_URL = urljoin(BASE_URL, "oa_file_list.csv")
    RETRACTIONS_URL = urljoin(BASE_URL, "retractions.csv")
    BULK_DIR = "oa_bulk/"
    LICENSE_DIRS = ["oa_comm", "oa_noncomm", "oa_other"]
    CONTENT_DIRS = ["xml", "txt"]

    # Regex to find hrefs in Apache's default directory listing.
    HREF_RE = re.compile(r'href="([^"]+)"')
    # Regex to parse incremental file names.
    INCR_RE = re.compile(r".+\.incr\.(\d{4}-\d{2}-\d{2})\.tar\.gz$")

    def get_article_file_list(self) -> List[ArticleFileInfo]:
        """
        Downloads and parses the master file list (oa_file_list.csv) from
        the NCBI FTP server to get metadata for all article packages.
        """
        print(f"Downloading master file list from {self.FILE_LIST_URL}...")
        all_infos = list(self.stream_article_infos_from_file_list(self.FILE_LIST_URL))
        print(f"Successfully parsed {len(all_infos)} records from the master file list.")
        return all_infos

    def get_incremental_updates(self, since: datetime) -> List[IncrementalUpdateInfo]:
        """
        Finds and returns incremental update packages on the FTP server newer than `since`.
        """
        updates = []
        since_date = since.date()
        print(f"Searching for FTP incremental updates since {since_date.isoformat()}...")

        for license_dir in self.LICENSE_DIRS:
            # We are only interested in XML content for now.
            for content_dir in ["xml"]:
                # Construct the full URL for the directory to be listed
                dir_url = urljoin(self.BASE_URL, f"{self.BULK_DIR}{license_dir}/{content_dir}/")
                print(f"  Listing directory: {dir_url}")

                try:
                    response = requests.get(dir_url)
                    response.raise_for_status()
                except requests.RequestException as e:
                    print(f"Could not list directory {dir_url}: {e}")
                    continue

                # Find all potential archive files in the HTML response
                filenames = self.HREF_RE.findall(response.text)
                for filename in filenames:
                    match = self.INCR_RE.match(filename)
                    if not match:
                        continue

                    file_date_str = match.group(1)
                    file_date = datetime.strptime(file_date_str, "%Y-%m-%d").date()

                    if file_date > since_date:
                        archive_path = urljoin(dir_url, filename)
                        file_list_path = archive_path.replace(".tar.gz", ".filelist.csv")
                        updates.append(
                            IncrementalUpdateInfo(
                                archive_path=archive_path,
                                file_list_path=file_list_path,
                                date=datetime.combine(file_date, datetime.min.time(), tzinfo=timezone.utc),
                            )
                        )

        # Sort updates by date, oldest first, to ensure sequential processing.
        updates.sort(key=lambda u: u.date)
        print(f"Found {len(updates)} new incremental updates on FTP source.")
        return updates

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

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=60),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(requests.RequestException),
        reraise=True,
    )
    def download_file(self, url: str, destination_dir: Path) -> Path:
        """
        Downloads a file from a URL, verifies its MD5 checksum, and saves
        it to a local directory. Retries on transient network errors.
        """
        local_filename = url.split('/')[-1]
        destination_path = destination_dir / local_filename
        print(f"Downloading {url} to {destination_path}...")
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(destination_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        # Checksum verification for .tar.gz files only
        if url.endswith(".tar.gz"):
            md5_url = url + ".md5"
            print(f"Downloading checksum from {md5_url}...")
            md5_response = requests.get(md5_url)
            if md5_response.status_code == 404:
                print(f"Warning: No MD5 checksum file found at {md5_url}. Skipping verification.")
                return destination_path
            md5_response.raise_for_status()

            match = re.search(r"=\s*([a-f0-9]{32})", md5_response.text)
            if not match:
                raise IOError(f"Could not parse MD5 checksum from {md5_url}")
            expected_checksum = match.group(1)

            hasher = hashlib.md5()
            with open(destination_path, 'rb') as f:
                while chunk := f.read(8192):
                    hasher.update(chunk)
            actual_checksum = hasher.hexdigest()

            if actual_checksum != expected_checksum:
                destination_path.unlink()
                raise IOError(
                    f"Checksum mismatch for {local_filename}. "
                    f"Expected {expected_checksum}, got {actual_checksum}."
                )
            print(f"Checksum verified for {local_filename}.")

        return destination_path

    def stream_article_infos_from_file_list(self, file_list_path: str) -> Generator[ArticleFileInfo, None, None]:
        """
        Downloads and streams ArticleFileInfo records from a given file list URL.
        """
        try:
            response = requests.get(file_list_path, stream=True)
            response.raise_for_status()
            lines = (line.decode('utf-8') for line in response.iter_lines())
            reader = csv.DictReader(lines)

            for row in reader:
                try:
                    row = {k.strip(): v for k, v in row.items()}
                    if not row.get("File") or not row.get("Accession ID"):
                        continue
                    last_updated_str = row.get("Last Updated")
                    pmid = row.get("PMID")
                    yield ArticleFileInfo(
                        file_path=row["File"],
                        pmcid=row["Accession ID"],
                        pmid=pmid if pmid else None,
                        last_updated=(
                            datetime.strptime(last_updated_str, "%Y-%m-%d %H:%M:%S")
                            if last_updated_str else None
                        ),
                        is_retracted=row.get("Retracted", "").lower() in ('true', 'y', 'yes'),
                    )
                except (ValueError, TypeError):
                    continue
        except requests.RequestException as e:
            print(f"Failed to stream or parse file list from {file_list_path}: {e}")
            return


class S3DataSource(DataSource):
    """Data source for the AWS S3 Open Data bucket (s3://pmc-oa-opendata)."""

    BUCKET_NAME = "pmc-oa-opendata"
    FILE_LIST_KEY = "oa_file_list.csv"
    RETRACTIONS_KEY = "retractions.csv"
    BULK_DIR = "oa_bulk/"
    LICENSE_DIRS = ["oa_comm", "oa_noncomm", "oa_other"]
    CONTENT_DIRS = ["xml", "txt"]

    # Regex to parse incremental file names from S3 keys.
    INCR_RE = re.compile(r".+\.incr\.(\d{4}-\d{2}-\d{2})\.tar\.gz$")

    def __init__(self):
        self.s3 = boto3.client("s3", config=Config(signature_version="unsigned"))

    def get_article_file_list(self) -> List[ArticleFileInfo]:
        """
        Downloads and parses the master file list (oa_file_list.csv) from the S3 bucket.
        """
        print(f"Downloading master file list from S3 bucket: {self.BUCKET_NAME}")
        all_infos = list(self.stream_article_infos_from_file_list(self.FILE_LIST_KEY))
        print(f"Successfully parsed {len(all_infos)} records from the master file list.")
        return all_infos

    def get_incremental_updates(self, since: datetime) -> List[IncrementalUpdateInfo]:
        """
        Finds and returns incremental update packages on S3 newer than `since`.
        """
        updates = []
        since_date = since.date()
        paginator = self.s3.get_paginator("list_objects_v2")
        print(f"Searching for S3 incremental updates since {since_date.isoformat()}...")

        for license_dir in self.LICENSE_DIRS:
            # We are only interested in XML content for now.
            for content_dir in ["xml"]:
                prefix = f"{self.BULK_DIR}{license_dir}/{content_dir}/"
                print(f"  Listing objects with prefix: s3://{self.BUCKET_NAME}/{prefix}")

                try:
                    pages = paginator.paginate(Bucket=self.BUCKET_NAME, Prefix=prefix)
                    for page in pages:
                        for obj in page.get("Contents", []):
                            key = obj["Key"]
                            match = self.INCR_RE.match(key)
                            if not match:
                                continue

                            file_date_str = match.group(1)
                            file_date = datetime.strptime(file_date_str, "%Y-%m-%d").date()

                            if file_date > since_date:
                                archive_path = key
                                file_list_path = archive_path.replace(".tar.gz", ".filelist.csv")
                                updates.append(
                                    IncrementalUpdateInfo(
                                        archive_path=archive_path,
                                        file_list_path=file_list_path,
                                        date=datetime.combine(file_date, datetime.min.time(), tzinfo=timezone.utc),
                                    )
                                )
                except ClientError as e:
                    print(f"Could not list objects with prefix {prefix}: {e}")
                    continue

        updates.sort(key=lambda u: u.date)
        print(f"Found {len(updates)} new incremental updates on S3 source.")
        return updates

    def get_retracted_pmcids(self) -> List[str]:
        """
        Downloads and parses the retractions.csv file from the S3 bucket.
        """
        print(f"Downloading retractions list from S3 bucket: {self.BUCKET_NAME}")
        try:
            response = self.s3.get_object(Bucket=self.BUCKET_NAME, Key=self.RETRACTIONS_KEY)
            lines = (line.decode('utf-8') for line in response["Body"].iter_lines())
            reader = csv.reader(lines)

            header = next(reader)
            if 'PMCID' not in header[0]:
                return [header[0]] + [row[0] for row in reader if row]
            else:
                return [row[0] for row in reader if row]
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                print("No retractions.csv file found in S3 bucket. Continuing without it.")
                return []
            print(f"Failed to download retractions file from S3: {e}")
            return []

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=60),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(ClientError),
        reraise=True,
    )
    def download_file(self, url: str, destination_dir: Path) -> Path:
        """
        Downloads a file from S3, verifying its integrity using its ETag (MD5 hash).
        Retries on transient AWS client errors.
        """
        s3_key = url
        local_filename = s3_key.split('/')[-1]
        destination_path = destination_dir / local_filename

        expected_checksum = None
        # Only perform checksum validation for the actual data archives.
        if s3_key.endswith(".tar.gz"):
            print(f"Getting ETag for s3://{self.BUCKET_NAME}/{s3_key}...")
            try:
                metadata = self.s3.head_object(Bucket=self.BUCKET_NAME, Key=s3_key)
                etag = metadata.get("ETag", "").strip('"')

                # S3 ETags for multipart uploads are not MD5 hashes. They contain a hyphen.
                # In this case, we cannot perform a simple MD5 check.
                if "-" in etag:
                    print(f"Warning: Skipping ETag check for multipart upload file {s3_key}.")
                else:
                    expected_checksum = etag
            except ClientError as e:
                print(f"Could not retrieve ETag for {s3_key}: {e}. Download will proceed without verification.")

        print(f"Downloading s3://{self.BUCKET_NAME}/{s3_key} to {destination_path}...")
        try:
            self.s3.download_file(
                Bucket=self.BUCKET_NAME,
                Key=s3_key,
                Filename=str(destination_path),
            )
        except ClientError as e:
            # Clean up partially downloaded file if it exists
            if destination_path.exists():
                destination_path.unlink()
            raise IOError(f"Failed to download {s3_key} from S3.") from e

        # If we have a checksum, verify the downloaded file.
        if expected_checksum:
            print(f"Verifying checksum for {local_filename}...")
            hasher = hashlib.md5()
            with open(destination_path, 'rb') as f:
                while chunk := f.read(8192):
                    hasher.update(chunk)
            actual_checksum = hasher.hexdigest()

            if actual_checksum != expected_checksum:
                destination_path.unlink()  # Clean up the corrupt file
                raise IOError(
                    f"Checksum mismatch for {local_filename}. "
                    f"Expected {expected_checksum}, got {actual_checksum}."
                )
            print(f"Checksum verified for {local_filename}.")

        print(f"Successfully downloaded {local_filename}.")
        return destination_path

    def stream_article_infos_from_file_list(self, file_list_path: str) -> Generator[ArticleFileInfo, None, None]:
        """
        Downloads and streams ArticleFileInfo records from a given file list S3 key.
        """
        try:
            response = self.s3.get_object(Bucket=self.BUCKET_NAME, Key=file_list_path)
            lines = (line.decode('utf-8') for line in response["Body"].iter_lines())
            reader = csv.DictReader(lines)
            for row in reader:
                try:
                    row = {k.strip(): v for k, v in row.items()}
                    if not row.get("File") or not row.get("Accession ID"):
                        continue
                    last_updated_str = row.get("Last Updated")
                    pmid = row.get("PMID")
                    yield ArticleFileInfo(
                        file_path=row["File"],
                        pmcid=row["Accession ID"],
                        pmid=pmid if pmid else None,
                        last_updated=(
                            datetime.strptime(last_updated_str, "%Y-%m-%d %H:%M:%S")
                            if last_updated_str else None
                        ),
                        is_retracted=row.get("Retracted", "").lower() in ('true', 'y', 'yes'),
                    )
                except (ValueError, TypeError):
                    continue
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                print(f"File list not found at s3://{self.BUCKET_NAME}/{file_list_path}")
            else:
                print(f"Failed to stream or parse file list from S3 key {file_list_path}: {e}")
            return


