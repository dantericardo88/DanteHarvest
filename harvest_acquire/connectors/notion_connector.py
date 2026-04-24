"""
NotionConnector — ingest Notion pages/databases into the artifact store.

Uses the Notion REST API directly (no external SDK, zero extra deps).
Requires a Notion integration token with read access.

Usage:
    connector = NotionConnector(token="secret_...", storage_root="storage")
    artifact_ids = connector.ingest(page_id="abc123")
    artifact_ids = connector.ingest(database_id="xyz456")

CLI:
    harvest ingest notion --page-id abc123 --token $NOTION_TOKEN
    harvest ingest notion --database-id xyz456 --token $NOTION_TOKEN
"""

from __future__ import annotations

import json
import urllib.request
from typing import Any, Dict, List, Optional
from uuid import uuid4

from harvest_acquire.connectors.base_connector import BaseConnector, ConnectorError

NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"


class NotionConnector(BaseConnector):
    connector_name = "notion"

    def __init__(self, token: str, storage_root: str = "storage"):
        super().__init__(storage_root=storage_root)
        if not token:
            raise ConnectorError("Notion token required")
        self._token = token
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Notion-Version": NOTION_VERSION,
            "Content-Type": "application/json",
        }

    def ingest(
        self,
        page_id: Optional[str] = None,
        database_id: Optional[str] = None,
        **kwargs: Any,
    ) -> List[str]:
        if page_id:
            return self._ingest_page(page_id)
        elif database_id:
            return self._ingest_database(database_id)
        raise ConnectorError("Provide page_id or database_id")

    def _get(self, endpoint: str) -> Dict[str, Any]:
        url = f"{NOTION_API}/{endpoint}"
        req = urllib.request.Request(url, headers=self._headers)
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            raise ConnectorError(f"Notion API error at {endpoint}: {e}") from e

    def _post(self, endpoint: str, body: dict) -> Dict[str, Any]:
        url = f"{NOTION_API}/{endpoint}"
        data = json.dumps(body).encode()
        req = urllib.request.Request(url, data=data, headers=self._headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            raise ConnectorError(f"Notion API error at {endpoint}: {e}") from e

    def _ingest_page(self, page_id: str) -> List[str]:
        page_meta = self._get(f"pages/{page_id}")
        blocks = self._get_all_blocks(page_id)
        markdown = self._blocks_to_markdown(blocks)
        aid = str(uuid4())
        title = self._extract_title(page_meta)
        self._write_artifact(aid, markdown, meta={
            "source": "notion",
            "page_id": page_id,
            "title": title,
            "url": page_meta.get("url", ""),
        })
        return [aid]

    def _ingest_database(self, database_id: str) -> List[str]:
        result = self._post(f"databases/{database_id}/query", {})
        pages = result.get("results", [])
        artifact_ids = []
        while True:
            for page in pages:
                aids = self._ingest_page(page["id"])
                artifact_ids.extend(aids)
            if not result.get("has_more"):
                break
            result = self._post(f"databases/{database_id}/query", {"start_cursor": result["next_cursor"]})
            pages = result.get("results", [])
        return artifact_ids

    def _get_all_blocks(self, block_id: str) -> List[Dict[str, Any]]:
        blocks = []
        cursor = None
        while True:
            endpoint = f"blocks/{block_id}/children"
            if cursor:
                endpoint += f"?start_cursor={cursor}"
            result = self._get(endpoint)
            for b in result.get("results", []):
                blocks.append(b)
                if b.get("has_children"):
                    blocks.extend(self._get_all_blocks(b["id"]))
            if not result.get("has_more"):
                break
            cursor = result.get("next_cursor")
        return blocks

    def _blocks_to_markdown(self, blocks: List[Dict[str, Any]]) -> str:
        lines = []
        for block in blocks:
            btype = block.get("type", "")
            content = block.get(btype, {})
            rich_text = content.get("rich_text", [])
            text = "".join(rt.get("plain_text", "") for rt in rich_text)
            if btype == "heading_1":
                lines.append(f"# {text}")
            elif btype == "heading_2":
                lines.append(f"## {text}")
            elif btype == "heading_3":
                lines.append(f"### {text}")
            elif btype == "bulleted_list_item":
                lines.append(f"- {text}")
            elif btype == "numbered_list_item":
                lines.append(f"1. {text}")
            elif btype == "code":
                lang = content.get("language", "")
                lines.append(f"```{lang}\n{text}\n```")
            elif btype == "paragraph":
                lines.append(text)
            elif btype == "divider":
                lines.append("---")
        return "\n\n".join(line for line in lines if line.strip())

    def _extract_title(self, page_meta: Dict[str, Any]) -> str:
        props = page_meta.get("properties", {})
        for key in ("Name", "Title", "title"):
            prop = props.get(key, {})
            title_list = prop.get("title", [])
            if title_list:
                return "".join(t.get("plain_text", "") for t in title_list)
        return page_meta.get("id", "untitled")
