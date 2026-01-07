"""Resolve DataONE dataset identifiers to downloadable URLs with metadata."""

from __future__ import annotations

import logging
from urllib.parse import quote

import requests
from d1_client.mnclient_2_0 import MemberNodeClient_2_0

logger = logging.getLogger(__name__)


class DataONEResolver:
    """Resolves DataONE dataset identifiers to data objects."""

    def __init__(self, member_node: str = "https://arcticdata.io/metacat/d1/mn"):
        """Initialize resolver.

        Args:
            member_node: DataONE member node base URL
        """
        self.member_node = member_node
        self.client = MemberNodeClient_2_0(base_url=member_node)

    def resolve_dataset(self, dataset_identifier: str) -> list[dict]:
        """Resolve a dataset identifier to its data objects.

        Args:
            dataset_identifier: Dataset/package PID

        Returns:
            List of data objects with metadata
        """
        msg = "Resolving dataset: {dataset_identifier}"
        logger.info(msg)

        # Query Solr for objects in this dataset
        solr_url = f"{self.member_node}/v2/query/solr/"
        params = {
            "q": f'resourceMap:"{dataset_identifier}"',
            "fl": "id,title,formatId,size,fileName,abstract,description",
            "rows": 100,
            "wt": "json",
        }

        try:
            response = requests.get(solr_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            docs = data.get("response", {}).get("docs", [])

            if not docs:
                msg = "No objects found for dataset {dataset_identifier}"
                logger.warning(msg)
                return []

            # Process each document
            data_objects = []
            for doc in docs:
                obj_id = doc.get("id")
                format_id = doc.get("formatId", "")

                # Skip metadata objects (EML, resource maps)
                if self._is_metadata_object(format_id):
                    continue

                # Build object info
                obj_info = {
                    "identifier": obj_id,
                    "url": self._build_object_url(obj_id),
                    "filename": self._get_filename(doc),
                    "format_id": format_id,
                    "size": doc.get("size", 0),
                    "entity_name": self._get_entity_name(doc),
                    "entity_description": self._get_entity_description(doc),
                }

                data_objects.append(obj_info)

            msg = "Found {len(data_objects)} data objects in dataset"
            logger.info(msg)
            return data_objects

        except Exception:
            msg = "Failed to resolve dataset: {e}"
            logger.error(msg)
            raise

    def _is_metadata_object(self, format_id: str) -> bool:
        """Check if a format ID indicates a metadata object."""
        metadata_formats = [
            "eml://",
            "http://www.openarchives.org/ore/terms",  # codespell:ignore
            "FGDC",
            "http://ns.dataone.org/metadata/schema/onedcx",
        ]
        return any(fmt in format_id for fmt in metadata_formats)

    def _build_object_url(self, identifier: str) -> str:
        """Build the download URL for an object.

        Args:
            identifier: Object PID

        Returns:
            Full URL to download the object
        """
        encoded_pid = quote(identifier, safe="")
        return f"{self.member_node}/v2/object/{encoded_pid}"

    def _get_filename(self, doc: dict) -> str:
        """Extract filename from Solr document."""
        if filename := doc.get("fileName"):
            if isinstance(filename, list):
                return filename[0]
            return filename

        # Fallback: derive from identifier
        obj_id = doc.get("id", "unknown")
        return obj_id.split(":")[-1]

    def _get_entity_name(self, doc: dict) -> str:
        """Extract entity name from Solr document."""
        if title := doc.get("title"):
            if isinstance(title, list):
                return title[0]
            return str(title)

        # Fallback to filename
        return self._get_filename(doc)

    def _get_entity_description(self, doc: dict) -> str:
        """Extract entity description from Solr document."""
        # Try abstract first
        if abstract := doc.get("abstract"):
            if isinstance(abstract, list):
                return abstract[0]
            return str(abstract)

        # Try description
        if description := doc.get("description"):
            if isinstance(description, list):
                return description[0]
            return str(description)

        return ""


def resolve_dataone_input(
    dataset_identifier: str,
    member_node: str = "https://arcticdata.io/metacat/d1/mn",
) -> list[dict]:
    """Resolve a DataONE dataset to its data objects.

    Args:
        dataset_identifier: Dataset/package PID
        member_node: DataONE member node URL

    Returns:
        List of data objects with URLs and metadata
    """
    resolver = DataONEResolver(member_node)
    return resolver.resolve_dataset(dataset_identifier)
