"""Jira connector — harvest_acquire.connectors.jira_connector."""
from __future__ import annotations

import json
import time
import urllib.request
import urllib.parse
import base64
from dataclasses import dataclass
from typing import Iterator, List, Optional

from harvest_acquire.connectors.base_connector import BaseConnector, ConnectorRecord


@dataclass
class JiraConnectorConfig:
    base_url: str                   # e.g. "https://yourorg.atlassian.net"
    email: str
    api_token: str
    project_key: str
    max_issues: int = 500
    jql_extra: str = ""            # additional JQL filter e.g. "status != Done"
    fields: str = "summary,description,status,assignee,priority,created,updated"


class JiraConnector(BaseConnector):
    """
    Harvest issues from a Jira Cloud project via REST API v3.

    Usage:
        config = JiraConnectorConfig(
            base_url="https://acme.atlassian.net",
            email="user@example.com",
            api_token="ATATT...",
            project_key="ENG",
        )
        connector = JiraConnector(config)
        for record in connector.fetch():
            print(record.title)
    """

    SOURCE_TYPE = "jira_issue"

    def __init__(self, config: JiraConnectorConfig):
        self._config = config
        creds = f"{config.email}:{config.api_token}"
        self._auth = base64.b64encode(creds.encode()).decode()

    def fetch(self) -> Iterator[ConnectorRecord]:
        start_at = 0
        page_size = min(100, self._config.max_issues)
        fetched = 0

        while fetched < self._config.max_issues:
            jql = f"project={self._config.project_key}"
            if self._config.jql_extra:
                jql += f" AND {self._config.jql_extra}"
            jql += " ORDER BY updated DESC"

            params = urllib.parse.urlencode({
                "jql": jql,
                "startAt": start_at,
                "maxResults": page_size,
                "fields": self._config.fields,
            })
            url = f"{self._config.base_url}/rest/api/3/search?{params}"
            data = self._get(url)
            issues = data.get("issues", [])
            if not issues:
                break

            for issue in issues:
                if fetched >= self._config.max_issues:
                    break
                fields = issue.get("fields", {})
                title = fields.get("summary", "(no summary)")
                desc_doc = fields.get("description") or {}
                content = self._extract_description(desc_doc)
                status = (fields.get("status") or {}).get("name", "")
                priority = (fields.get("priority") or {}).get("name", "")
                assignee_data = fields.get("assignee") or {}
                assignee = assignee_data.get("displayName", "")
                yield ConnectorRecord(
                    record_id=issue["key"],
                    title=f"[{issue['key']}] {title}",
                    content=content,
                    source_url=f"{self._config.base_url}/browse/{issue['key']}",
                    source_type=self.SOURCE_TYPE,
                    metadata={
                        "issue_key": issue["key"],
                        "status": status,
                        "priority": priority,
                        "assignee": assignee,
                        "project": self._config.project_key,
                        "created": fields.get("created", ""),
                        "updated": fields.get("updated", ""),
                    },
                    fetched_at=time.time(),
                )
                fetched += 1

            start_at += len(issues)
            total = data.get("total", 0)
            if start_at >= total:
                break

    def _get(self, url: str) -> dict:
        req = urllib.request.Request(url, headers={
            "Authorization": f"Basic {self._auth}",
            "Accept": "application/json",
            "User-Agent": "DanteHarvest/1.0",
        })
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())

    def _extract_description(self, doc: dict) -> str:
        """Extract plain text from Atlassian Document Format (ADF)."""
        if not doc or not isinstance(doc, dict):
            return ""
        texts = []
        for block in doc.get("content", []):
            for inline in block.get("content", []):
                if inline.get("type") == "text":
                    texts.append(inline.get("text", ""))
        return " ".join(texts)
