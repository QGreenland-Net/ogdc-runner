"""Resolve DataONE dataset identifiers to downloadable URLs with metadata."""

from __future__ import annotations

import logging
import os
from typing import Any
from urllib.parse import quote

import requests
from d1_client.mnclient_2_0 import MemberNodeClient_2_0

from ogdc_runner.exceptions import OgdcMissingEnvvar

logger = logging.getLogger(__name__)
DATAONE_NODE_URL = os.environ.get("DATAONE_NODE_URL")
if DATAONE_NODE_URL is None:
    msg = "Must have DATAONE_NODE_URL envvar set"
    raise OgdcMissingEnvvar(msg)


class DataONEResolver:
    """Resolves DataONE dataset identifiers to data objects."""

    def __init__(self) -> None:
        """Initialize resolver.

        Args:11
            member_node: DataONE member node base URL
            Defaults to value of DATAONE_NODE_URL envvar.
        """
        self.member_node = DATAONE_NODE_URL
        self.client = MemberNodeClient_2_0(base_url=DATAONE_NODE_URL)

    def resolve_dataset(self, dataset_identifier: str) -> list[dict[str, Any]]:
        """Resolve a dataset/package identifier to its data objects.

        Args:
            dataset_identifier: Dataset package PID (resource_map_urn:uuid:... format)

        Returns:
            List of data objects found in the package
        """
        msg = f"Resolving dataset: {dataset_identifier}"
        logger.info(msg)

        # TODO: update this after chatting with Rushiraj
        if not dataset_identifier.startswith("resource_map_urn:uuid:"):
            raise ValueError(
                f"Invalid package identifier: {dataset_identifier}. "
                f"Expected format: resource_map_urn:uuid:... "
                f"(This should be a package/resource map ID, not a data object ID)"
            )

        # Query Solr for objects in this dataset
        solr_url = f"{self.member_node}/v2/query/solr/"
        params = {
            "q": f'resourceMap:"{dataset_identifier}" AND -formatType:METADATA',
            "fl": "id,title,formatId,size,fileName,abstract,description",
            "rows": 100,
            "wt": "json",
        }

        try:
            response = requests.get(solr_url, params=params, timeout=30)  # type: ignore[arg-type]
            response.raise_for_status()
            data = response.json()

            docs = data.get("response", {}).get("docs", [])

            if not docs:
                msg = f"No objects found for dataset {dataset_identifier}"
                logger.warning(msg)
                return []

            # Process each document
            data_objects = []
            for doc in docs:
                obj_id = doc.get("id")

                # Build object info
                obj_info = {
                    "identifier": obj_id,
                    "url": self._build_object_url(obj_id),
                    "filename": self._get_filename(doc),
                    "format_id": doc.get("format_id", ""),
                    "size": doc.get("size", 0),
                    "entity_name": self._get_entity_name(doc),
                    "entity_description": "",  # will be generated during publishing
                }

                data_objects.append(obj_info)

            msg = f"Found {len(data_objects)} data objects in dataset"
            logger.info(msg)
            return data_objects

        except Exception as e:
            msg = f"Failed to resolve dataset: {e}"
            logger.error(msg)
            raise

    def _build_object_url(self, identifier: str) -> str:
        """Build the download URL for an object.

        Args:
            identifier: Object PID

        Returns:
            Full URL to download the object
        """
        encoded_pid = quote(identifier, safe="")
        return f"{self.member_node}/v2/object/{encoded_pid}"

    def _get_filename(self, doc: dict[str, Any]) -> str:
        """Extract filename from Solr document."""
        if filename := doc.get("fileName"):
            if isinstance(filename, list):
                return str(filename[0])
            return str(filename)

        # Fallback: derive from identifier
        obj_id = doc.get("id")
        if not obj_id:
            error_msg = "DataONE object missing required 'id' field"
            raise ValueError(error_msg)

        return str(obj_id).split(":")[-1]

    def _get_entity_name(self, doc: dict[str, Any]) -> str:
        """Extract entity name from Solr document."""
        if title := doc.get("title"):
            if isinstance(title, list):
                return str(title[0])
            return str(title)

        # Fallback to filename
        return self._get_filename(doc)


def resolve_dataone_input(
    dataset_identifier: str,
) -> list[dict[str, Any]]:
    """Resolve a DataONE dataset to its data objects.

    Args:
        dataset_identifier: Dataset/package PID
        member_node: DataONE member node URL

    Returns:
        List of data objects with URLs and metadata
    """
    resolver = DataONEResolver()
    return resolver.resolve_dataset(dataset_identifier)
