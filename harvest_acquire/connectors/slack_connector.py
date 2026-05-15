"""
SlackConnector — ingest Slack channel messages via Slack Web API.

Constitutional guarantees:
- Fail-closed: ConnectorError on auth failure or missing token
- Local-first: messages written to disk as markdown artifacts
- Zero-ambiguity: returns list of artifact IDs, never None
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional
from uuid import uuid4

from harvest_acquire.connectors.base_connector import BaseConnector, ConnectorError


class SlackConnector(BaseConnector):
    """
    Ingest messages from a Slack channel using the Slack Web API.

    Requires SLACK_BOT_TOKEN environment variable or explicit token.

    Usage:
        connector = SlackConnector(token="xoxb-...", storage_root="storage")
        artifact_ids = connector.ingest(channel_id="C0123ABC", limit=100)
    """

    connector_name = "slack"
    required_env_vars = ["SLACK_BOT_TOKEN", "SLACK_TOKEN"]

    def __init__(self, token: Optional[str] = None, storage_root: str = "storage"):
        super().__init__(storage_root)
        import os
        self._token = token or os.environ.get("SLACK_BOT_TOKEN", "")
        if not self._token:
            raise ConnectorError(
                "Slack token required. Set SLACK_BOT_TOKEN env var or pass token= parameter."
            )

    def ingest(
        self,
        channel_id: str = "",
        channel_name: Optional[str] = None,
        limit: int = 200,
        oldest: Optional[str] = None,
        latest: Optional[str] = None,
        **kwargs: Any,
    ) -> List[str]:
        """
        Fetch messages from a Slack channel and write as artifacts.

        Args:
            channel_id: Slack channel ID (e.g. "C0123ABC")
            channel_name: Optional human-readable name for metadata
            limit: Max messages to fetch (max 1000 per Slack API)
            oldest: Oldest timestamp (Unix epoch float as string)
            latest: Latest timestamp (Unix epoch float as string)

        Returns:
            List of artifact IDs written to disk.
        """
        if not channel_id:
            raise ConnectorError("channel_id is required for SlackConnector.ingest()")

        messages = self._fetch_messages(channel_id, limit=limit, oldest=oldest, latest=latest)
        if not messages:
            return []

        artifact_ids = []
        for msg in messages:
            artifact_id = str(uuid4())
            content = self._message_to_markdown(msg, channel_id, channel_name)
            meta = {
                "connector": "slack",
                "channel_id": channel_id,
                "channel_name": channel_name,
                "ts": msg.get("ts"),
                "user": msg.get("user"),
                "thread_ts": msg.get("thread_ts"),
                "fetched_at": time.time(),
            }
            self._write_artifact(artifact_id, content, meta)
            artifact_ids.append(artifact_id)

        return artifact_ids

    def _fetch_messages(
        self,
        channel_id: str,
        limit: int,
        oldest: Optional[str],
        latest: Optional[str],
    ) -> List[Dict[str, Any]]:
        try:
            import urllib.request
            import urllib.parse
            params: Dict[str, Any] = {"channel": channel_id, "limit": min(limit, 1000)}
            if oldest:
                params["oldest"] = oldest
            if latest:
                params["latest"] = latest
            url = "https://slack.com/api/conversations.history?" + urllib.parse.urlencode(params)
            req = urllib.request.Request(
                url,
                headers={"Authorization": f"Bearer {self._token}"},
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode())
            if not data.get("ok"):
                raise ConnectorError(f"Slack API error: {data.get('error', 'unknown')}")
            return data.get("messages", [])
        except ConnectorError:
            raise
        except Exception as e:
            raise ConnectorError(f"Failed to fetch Slack messages: {e}") from e

    def _message_to_markdown(
        self,
        msg: Dict[str, Any],
        channel_id: str,
        channel_name: Optional[str],
    ) -> str:
        ts = msg.get("ts", "")
        user = msg.get("user", "unknown")
        text = msg.get("text", "")
        channel_label = channel_name or channel_id
        return (
            f"# Slack Message — #{channel_label}\n\n"
            f"**User:** {user}  \n"
            f"**Timestamp:** {ts}\n\n"
            f"{text}\n"
        )
