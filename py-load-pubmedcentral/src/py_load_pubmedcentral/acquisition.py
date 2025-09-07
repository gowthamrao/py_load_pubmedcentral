"""
Data acquisition module for downloading and extracting PMC data.
"""
from __future__ import annotations

import tarfile
import hashlib
import re
from abc import ABC, abstractmethod
from pathlib import Path
from typing import IO, Generator, List, Tuple
from urllib.parse import urljoin

import requests
from lxml import etree, html

from py_load_pubmedcentral.models import PmcArticlesContent, PmcArticlesMetadata
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
    OA_BULK_PATH = "oa_bulk/"

    def list_baseline_files(self) -> List[str]:
        """
        Lists all .tar.gz files from the commercial use baseline directory.
        e.g., oa_bulk/oa_comm/xml/oa_comm_xml.baseline.2023-12-12.tar.gz
        """
        # Commercial use subset is a good default.
        # Other subsets include 'oa_non_comm' and 'oa_other'
        comm_use_url = urljoin(urljoin(self.BASE_URL, self.OA_BULK_PATH), "oa_comm/xml/")
        response = requests.get(comm_use_url)
        response.raise_for_status()

        tree = html.fromstring(response.content)
        # Regex to find baseline files. It avoids 'incr' and matches the date pattern.
        baseline_pattern = re.compile(r"oa_comm_xml\.baseline\.\d{4}-\d{2}-\d{2}\.tar\.gz$")

        archive_urls = []
        for element, attribute, link, pos in tree.iterlinks():
            if baseline_pattern.search(link):
                full_url = urljoin(comm_use_url, link)
                archive_urls.append(full_url)

        return archive_urls

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


def stream_and_parse_tar_gz_archive(
    tar_gz_path: Path,
) -> Generator[Tuple[PmcArticlesMetadata, PmcArticlesContent], None, None]:
    """
    Opens a local .tar.gz archive, extracts XML files in memory,
    and parses them, yielding data models for each article.

    This function streams the archive extraction to keep memory usage low.

    Args:
        tar_gz_path: The local path to the .tar.gz archive to process.

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
                        yield from parse_jats_xml(xml_file_obj)
                    except etree.XMLSyntaxError as e:
                        # Log the error for the specific file and continue
                        print(f"Skipping malformed XML file {member.name}: {e}")
                        continue
                    finally:
                        xml_file_obj.close()
