"""
JATS XML parser using lxml.iterparse for high performance and low memory usage.
"""
from __future__ import annotations

import io
from datetime import datetime, timezone
from typing import IO, Generator, Optional, Tuple

from lxml import etree

import tarfile
from pathlib import Path

from py_load_pubmedcentral.models import (
    ArticleFileInfo,
    Contributor,
    JournalInfo,
    LicenseInfo,
    PmcArticlesContent,
    PmcArticlesMetadata,
)


def _get_text(element: etree._Element, path: str) -> Optional[str]:
    """Safely get text content from an element using an XPath."""
    node = element.find(path)
    return node.text.strip() if node is not None and node.text else None


def _get_full_text(element: etree._Element, path: str) -> Optional[str]:
    """Safely get all text within an element, including mixed content."""
    node = element.find(path)
    if node is None:
        return None
    # "string()" XPath function concatenates all descendant text nodes.
    return " ".join(etree.tostring(node, method="text", encoding="unicode").split())


def _extract_ids(article_element: etree._Element) -> dict:
    """Extracts PMCID, PMID, and DOI from the article element."""
    ids = {"pmcid": None, "pmid": None, "doi": None}
    for article_id in article_element.findall(".//front/article-meta/article-id"):
        id_type = article_id.get("pub-id-type")
        if id_type == "pmc":
            ids["pmcid"] = article_id.text
        elif id_type == "pmid":
            ids["pmid"] = article_id.text
        elif id_type == "doi":
            ids["doi"] = article_id.text
    return ids


def _extract_contributors(article_element: etree._Element) -> list[Contributor]:
    """Extracts contributor information."""
    contributors = []
    contrib_group = article_element.find(".//front/article-meta/contrib-group")
    if contrib_group is None:
        return contributors

    for contrib in contrib_group.findall("contrib[@contrib-type='author']"):
        name_element = contrib.find("name")
        if name_element is not None:
            surname = _get_text(name_element, "surname") or ""
            given_names = _get_text(name_element, "given-names") or ""
            name = f"{given_names} {surname}".strip()

            affiliation = _get_full_text(contrib, "aff")
            orcid_element = contrib.find("contrib-id[@contrib-id-type='orcid']")
            orcid = orcid_element.text if orcid_element is not None else None

            contributors.append(
                Contributor(name=name, affiliation=affiliation, orcid=orcid)
            )
    return contributors


def parse_jats_xml(
    xml_file: IO[bytes],
) -> Generator[Tuple[PmcArticlesMetadata, PmcArticlesContent], None, None]:
    """
    Parses a JATS XML file (or file-like object) and yields data models.

    This function is a generator that uses `lxml.iterparse` to process the XML
    incrementally, keeping memory usage low. The caller is responsible for
    populating the `source_last_updated` and `is_retracted` fields on the
    yielded metadata model.

    Args:
        xml_file: A file-like object opened in binary mode.

    Yields:
        A tuple containing (PmcArticlesMetadata, PmcArticlesContent) for each
        <article> element found in the input stream.
    """
    context = etree.iterparse(xml_file, events=("end",), tag="article", recover=True)
    for event, elem in context:
        try:
            # --- Extract Metadata ---
            ids = _extract_ids(elem)
            pmcid = ids.get("pmcid")
            if not pmcid:
                # If there's no PMCID, we cannot process this record.
                # Log this event in a real application.
                continue

            meta = elem.find("front/article-meta")
            journal_meta = elem.find("front/journal-meta")

            title = _get_full_text(meta, "title-group/article-title") or "No Title Found"
            abstract = _get_full_text(meta, "abstract")

            # Publication date (handle various formats)
            pub_date_element = meta.find("pub-date")
            pub_date = None
            if pub_date_element is not None:
                year = _get_text(pub_date_element, "year")
                month = _get_text(pub_date_element, "month") or "1"
                day = _get_text(pub_date_element, "day") or "1"
                if year:
                    pub_date = datetime.strptime(f"{year}-{month}-{day}", "%Y-%m-%d").date()

            # Journal Info
            journal_info = JournalInfo(
                name=_get_text(journal_meta, "journal-title-group/journal-title") or "N/A",
                issn=_get_text(journal_meta, "issn"),
                publisher=_get_text(journal_meta, "publisher/publisher-name"),
            )

            # License Info
            license_info = None
            permissions = meta.find("permissions")
            if permissions is not None:
                license_element = permissions.find("license")
                if license_element is not None:
                    license_info = LicenseInfo(
                        type=license_element.get("license-type"),
                        url=license_element.get("{http://www.w3.org/1999/xlink}href"),
                    )


            # --- Construct Models ---
            metadata_model = PmcArticlesMetadata(
                pmcid=pmcid,
                pmid=int(ids["pmid"]) if ids.get("pmid") and ids["pmid"].isdigit() else None,
                doi=ids.get("doi"),
                title=title,
                abstract_text=abstract,
                publication_date=pub_date,
                journal_info=journal_info,
                contributors=_extract_contributors(elem),
                license_info=license_info,
                is_retracted=False,  # Default, caller must populate this
                source_last_updated=None,  # Caller must populate this
                sync_timestamp=datetime.now(timezone.utc),
            )

            content_model = PmcArticlesContent(
                pmcid=pmcid,
                raw_jats_xml=etree.tostring(elem, encoding="unicode"),
                body_text=_get_full_text(elem, "body"),
            )

            yield (metadata_model, content_model)

        except Exception as e:
            # In a real application, use structured logging here.
            print(f"Error processing an article: {e}")
            # The 'recover=True' in iterparse helps, but we add this for safety.
            continue
        finally:
            # It's crucial to clear the element to free memory.
            # The ancestor-clearing part is complex and can be fragile.
            # elem.clear() is the most important part.
            elem.clear()

    del context


def stream_and_parse_tar_gz_archive(
    tar_gz_path: Path,
    article_info_lookup: dict[str, ArticleFileInfo],
) -> Generator[Tuple[PmcArticlesMetadata, PmcArticlesContent], None, None]:
    """
    Opens a local .tar.gz archive, finds XML files for target articles,
    parses them, and yields data models.
    """
    with tarfile.open(name=tar_gz_path, mode="r|gz") as tar:
        for member in tar:
            if member.isfile() and member.name.lower().endswith((".xml", ".nxml")):
                xml_file_obj = tar.extractfile(member)
                if not xml_file_obj:
                    continue

                try:
                    for metadata, content in parse_jats_xml(xml_file_obj):
                        if metadata.pmcid in article_info_lookup:
                            article_info = article_info_lookup[metadata.pmcid]
                            metadata.source_last_updated = article_info.last_updated
                            metadata.is_retracted = article_info.is_retracted
                            yield metadata, content
                except etree.XMLSyntaxError as e:
                    print(f"Skipping malformed XML file {member.name}: {e}")
                    continue
                finally:
                    if xml_file_obj:
                        xml_file_obj.close()
