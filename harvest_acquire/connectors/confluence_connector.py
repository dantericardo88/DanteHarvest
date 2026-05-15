"""
ConfluenceConnector — ingest Confluence pages via REST API v2.

Constitutional guarantees:
- Fail-closed: ConnectorError on auth failure or missing credentials
- Local-first: pages written as markdown artifacts to disk
- Zero-ambiguity: returns list of artifact IDs, never None
"""

from __future__ import annotations

import base64
import json
import time
from typing import Any, Dict, List, Optional
from uuid import uuid4

from harvest_acquire.connectors.base_connector import BaseConnector, ConnectorError


class ConfluenceConnector(BaseConnector):
    """
    Ingest pages from a Confluence space using Basic auth (user:api_token).

    Usage:
        connector = ConfluenceConnector(
            base_url="https://myorg.atlassian.net",
            email="me@example.com",
            api_token="ATATT...",
        )
        artifact_ids = connector.ingest(space_key="ENG", max_pages=50)
    """

    connector_name = "confluence"

    def __init__(
        self,
        base_url: Optional[str] = None,
        email: Optional[str] = None,
        api_token: Optional[str] = None,
        storage_root: str = "storage",
    ):
        super().__init__(storage_root)
        import os
        self._base_url = (base_url or os.environ.get("CONFLUENCE_BASE_URL", "")).rstrip("/")
        self._email = email or os.environ.get("CONFLUENCE_EMAIL", "")
        self._api_token = api_token or os.environ.get("CONFLUENCE_API_TOKEN", "")
        if not self._base_url or not self._email or not self._api_token:
            raise ConnectorError(
                "Confluence credentials required. Set CONFLUENCE_BASE_URL, "
                "CONFLUENCE_EMAIL, CONFLUENCE_API_TOKEN env vars."
            )
        _creds = f"{self._email}:{self._api_token}".encode()
        self._auth_header = "Basic " + base64.b64encode(_creds).decode()

    def ingest(
        self,
        space_key: Optional[str] = None,
        page_ids: Optional[List[str]] = None,
        max_pages: int = 100,
        label: Optional[str] = None,
        **kwargs: Any,
    ) -> List[str]:
        """
        Ingest pages from a Confluence space or explicit page IDs.

        Args:
            space_key: Confluence space key (e.g. "ENG")
            page_ids: Explicit page IDs (overrides space_key)
            max_pages: Maximum pages to ingest
            label: Filter pages by label

        Returns:
            List of artifact IDs.
        """
        if not space_key and not page_ids:
            raise ConnectorError("Either space_key or page_ids is required.")

        target_ids = page_ids or self._list_space_pages(space_key or "", max_pages, label)

        artifact_ids = []
        for pid in target_ids[:max_pages]:
            try:
                page = self._get_page(pid)
                content = self._page_to_markdown(page)
                artifact_id = str(uuid4())
                meta = {
                    "connector": "confluence",
                    "page_id": pid,
                    "title": page.get("title"),
                    "space_key": space_key,
                    "version": page.get("version", {}).get("number"),
                    "fetched_at": time.time(),
                }
                self._write_artifact(artifact_id, content, meta)
                artifact_ids.append(artifact_id)
            except Exception:
                continue  # Per-page errors never abort the batch

        return artifact_ids

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _request(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        import urllib.request, urllib.parse
        url = f"{self._base_url}/wiki/api/v2/{path.lstrip('/')}"
        if params:
            url += "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(
            url,
            headers={
                "Authorization": self._auth_header,
                "Accept": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read())
        except Exception as e:
            raise ConnectorError(f"Confluence API error for {path}: {e}") from e

    def _list_space_pages(self, space_key: str, max_pages: int, label: Optional[str]) -> List[str]:
        params: Dict[str, Any] = {"spaceKey": space_key, "limit": min(max_pages, 250), "status": "current"}
        if label:
            params["label"] = label
        result = self._request("pages", params)
        return [p["id"] for p in result.get("results", [])]

    def _get_page(self, page_id: str) -> Dict[str, Any]:
        return self._request(f"pages/{page_id}", {"body-format": "storage"})

    def _page_to_markdown(self, page: Dict[str, Any]) -> str:
        title = page.get("title", "Untitled")
        # body.storage.value contains HTML; convert to plain text
        body_html = page.get("body", {}).get("storage", {}).get("value", "")
        import re
        text = re.sub(r"<[^>]+>", " ", body_html)
        text = re.sub(r"\s+", " ", text).strip()
        space = page.get("spaceId", "")
        version = page.get("version", {}).get("number", "")
        return (
            f"# {title}\n\n"
            f"**Space:** {space}  \n"
            f"**Version:** {version}\n\n"
            f"{text}\n"
        )
