"""
Data acquisition module for downloading and extracting PMC data.
"""
from __future__ import annotations

import tarfile
from typing import IO, Generator, Tuple

import requests
from lxml import etree

from py_load_pubmedcentral.models import PmcArticlesContent, PmcArticlesMetadata
from py_load_pubmedcentral.parser import parse_jats_xml


def stream_and_parse_tar_gz_archive(
    url: str,
) -> Generator[Tuple[PmcArticlesMetadata, PmcArticlesContent], None, None]:
    """
    Downloads a .tar.gz archive from a URL, extracts XML files in memory,
    and parses them, yielding data models for each article.

    This function streams the download and the archive extraction to keep
    memory usage low, making it suitable for very large archives.

    Args:
        url: The URL of the .tar.gz archive to process.

    Yields:
        A tuple of (PmcArticlesMetadata, PmcArticlesContent) for each
        article found and successfully parsed in the archive.
    """
    # The 'stream=True' parameter is crucial for not loading the whole file into memory.
    with requests.get(url, stream=True) as response:
        response.raise_for_status()  # Will raise an exception for 4xx/5xx status codes

        # tarfile can open a file-like object in stream mode.
        # response.raw is the raw byte stream.
        with tarfile.open(fileobj=response.raw, mode="r|gz") as tar:
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
