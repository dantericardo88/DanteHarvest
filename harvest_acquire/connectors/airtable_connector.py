"""Airtable connector — harvest_acquire.connectors.airtable_connector."""
from __future__ import annotations

import json
import time
import urllib.request
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional

from harvest_acquire.connectors.base_connector import BaseConnector, ConnectorRecord


@dataclass
class AirtableConnectorConfig:
    api_key: str                    # Personal Access Token
    base_id: str                    # "appXXXXXXXXXXXXXX"
    table_name: str
    title_field: str = "Name"
    content_fields: Optional[List[str]] = None  # fields to concatenate as content
    max_records: int = 1000
    filter_formula: str = ""


class AirtableConnector(BaseConnector):
    """
    Harvest records from an Airtable base via REST API v0.

    Usage:
        config = AirtableConnectorConfig(
            api_key="patXXX",
            base_id="appXXX",
            table_name="Tasks",
            title_field="Name",
        )
        for record in AirtableConnector(config).fetch():
            print(record.title)
    """

    SOURCE_TYPE = "airtable_record"
    BASE_URL = "https://api.airtable.com/v0"

    def __init__(self, config: AirtableConnectorConfig):
        self._config = config

    def fetch(self) -> Iterator[ConnectorRecord]:
        offset = None
        fetched = 0

        while fetched < self._config.max_records:
            params: Dict[str, Any] = {
                "pageSize": min(100, self._config.max_records - fetched),
            }
            if self._config.filter_formula:
                params["filterByFormula"] = self._config.filter_formula
            if offset:
                params["offset"] = offset

            url = (
                f"{self.BASE_URL}/{self._config.base_id}/"
                f"{urllib.parse.quote(self._config.table_name)}"
                f"?{urllib.parse.urlencode(params)}"
            )
            data = self._get(url)
            records = data.get("records", [])
            if not records:
                break

            for rec in records:
                if fetched >= self._config.max_records:
                    break
                fields = rec.get("fields", {})
                title = str(fields.get(self._config.title_field, rec["id"]))
                content_parts = []
                for cf in (self._config.content_fields or []):
                    val = fields.get(cf)
                    if val:
                        content_parts.append(f"{cf}: {val}")
                content = "\n".join(content_parts) if content_parts else str(fields)
                yield ConnectorRecord(
                    record_id=rec["id"],
                    title=title,
                    content=content,
                    source_url=f"https://airtable.com/{self._config.base_id}/{urllib.parse.quote(self._config.table_name)}/{rec['id']}",
                    source_type=self.SOURCE_TYPE,
                    metadata={
                        "base_id": self._config.base_id,
                        "table": self._config.table_name,
                        "created_time": rec.get("createdTime", ""),
                        "fields": {k: str(v) for k, v in fields.items()},
                    },
                    fetched_at=time.time(),
                )
                fetched += 1

            offset = data.get("offset")
            if not offset:
                break

    def _get(self, url: str) -> dict:
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {self._config.api_key}",
            "Content-Type": "application/json",
            "User-Agent": "DanteHarvest/1.0",
        })
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
