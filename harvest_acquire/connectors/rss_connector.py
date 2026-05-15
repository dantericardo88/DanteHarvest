"""RSS/Atom feed connector — harvest_acquire.connectors.rss_connector."""
from __future__ import annotations

import re
import time
import urllib.request
from dataclasses import dataclass, field
from typing import Iterator, List, Optional
from xml.etree import ElementTree as ET

from harvest_acquire.connectors.base_connector import BaseConnector, ConnectorRecord


@dataclass
class RSSConnectorConfig:
    feed_url: str
    max_items: int = 100
    timeout_s: float = 15.0
    include_content: bool = True


class RSSConnector(BaseConnector):
    """
    Harvest articles from RSS and Atom feeds.

    Usage:
        connector = RSSConnector(RSSConnectorConfig(feed_url="https://example.com/rss"))
        for record in connector.fetch():
            print(record.title, record.source_url)
    """

    SOURCE_TYPE = "rss_feed"

    def __init__(self, config: RSSConnectorConfig):
        self._config = config

    def fetch(self) -> Iterator[ConnectorRecord]:
        xml = self._download(self._config.feed_url)
        root = ET.fromstring(xml)
        ns = {"atom": "http://www.w3.org/2005/Atom"}

        # Detect RSS vs Atom
        if root.tag == "rss" or root.tag.endswith("}rss"):
            yield from self._parse_rss(root)
        elif "feed" in root.tag:
            yield from self._parse_atom(root, ns)
        else:
            yield from self._parse_rss(root)  # best-effort fallback

    def _parse_rss(self, root: ET.Element) -> Iterator[ConnectorRecord]:
        count = 0
        for item in root.iter("item"):
            if count >= self._config.max_items:
                break
            title = self._text(item, "title")
            url = self._text(item, "link")
            description = self._text(item, "description") or ""
            content = self._strip_html(description)
            pub_date = self._text(item, "pubDate") or ""
            yield ConnectorRecord(
                record_id=url or f"rss-{count}",
                title=title or "(no title)",
                content=content,
                source_url=url or self._config.feed_url,
                source_type=self.SOURCE_TYPE,
                metadata={"pub_date": pub_date, "feed_url": self._config.feed_url},
                fetched_at=time.time(),
            )
            count += 1

    def _parse_atom(self, root: ET.Element, ns: dict) -> Iterator[ConnectorRecord]:
        count = 0
        for entry in root.iter("{http://www.w3.org/2005/Atom}entry"):
            if count >= self._config.max_items:
                break
            title_el = entry.find("{http://www.w3.org/2005/Atom}title")
            link_el = entry.find("{http://www.w3.org/2005/Atom}link")
            summary_el = entry.find("{http://www.w3.org/2005/Atom}summary")
            content_el = entry.find("{http://www.w3.org/2005/Atom}content")
            title = title_el.text if title_el is not None else "(no title)"
            url = link_el.get("href", "") if link_el is not None else ""
            raw = (content_el or summary_el)
            content = self._strip_html(raw.text or "") if raw is not None else ""
            yield ConnectorRecord(
                record_id=url or f"atom-{count}",
                title=title or "(no title)",
                content=content,
                source_url=url or self._config.feed_url,
                source_type=self.SOURCE_TYPE,
                metadata={"feed_url": self._config.feed_url},
                fetched_at=time.time(),
            )
            count += 1

    def _download(self, url: str) -> str:
        req = urllib.request.Request(url, headers={"User-Agent": "DanteHarvest/1.0"})
        with urllib.request.urlopen(req, timeout=self._config.timeout_s) as resp:
            return resp.read().decode("utf-8", errors="replace")

    def _text(self, el: ET.Element, tag: str) -> Optional[str]:
        child = el.find(tag)
        return child.text.strip() if child is not None and child.text else None

    def _strip_html(self, html: str) -> str:
        return re.sub(r"<[^>]+>", "", html).strip()
