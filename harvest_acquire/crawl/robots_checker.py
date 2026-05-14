"""
RobotsChecker — parse robots.txt and enforce crawling rules.

Harvested from: Scrapy RobotsTxtMiddleware + crawlee-python robots patterns.

Parses robots.txt for a given domain:
- Enforces `Disallow` rules for a given User-Agent (with wildcard fallback to *)
- Respects `Crawl-delay` directive with async sleep between requests
- Caches robots.txt per domain to minimise network calls
- Graceful fallback: if robots.txt is unreachable, all URLs are allowed (allow-first)

Constitutional guarantees:
- Local-first: pure stdlib; no external robots parser dependency
- Fail-open: unreachable robots.txt allows crawling (conservative but practical)
- Zero-ambiguity: is_allowed() always returns bool, never None
- Append-only chain: never mutates caller's state; crawl-delay returned as float
"""

from __future__ import annotations

import asyncio
import re
import time
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from urllib.parse import urlparse


_USER_AGENT = "HarvestBot/1.0 (+https://github.com/danteharvest)"


@dataclass
class _RobotsRules:
    """Parsed rules for a single User-Agent group."""
    disallow: List[str] = field(default_factory=list)
    allow: List[str] = field(default_factory=list)
    crawl_delay: float = 0.0


def _fetch_robots(url: str, timeout: int = 10) -> Optional[str]:
    """
    Fetch robots.txt text.  Returns None if unreachable (fail-open).
    Never raises — callers treat None as 'allow all'.
    """
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception:
        return None


def _parse_robots(text: str, user_agent: str) -> _RobotsRules:
    """
    Parse robots.txt and return rules applicable to *user_agent*.

    Matching priority (RFC 9309 §2.2.2):
    1. Exact match on User-agent line (case-insensitive)
    2. Wildcard (*) group
    3. No match → allow all with delay 0

    Rules inside the matched group are accumulated (multiple Disallow/Allow
    lines per group are supported).
    """
    ua_lower = user_agent.lower()

    # Collect all groups: list of (agents: List[str], rules: _RobotsRules)
    groups: List[tuple[List[str], _RobotsRules]] = []
    current_agents: List[str] = []
    current_rules = _RobotsRules()
    in_group = False

    for raw_line in text.splitlines():
        # Strip inline comments
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            if in_group and current_agents:
                groups.append((current_agents, current_rules))
                current_agents = []
                current_rules = _RobotsRules()
                in_group = False
            continue

        lower = line.lower()

        if lower.startswith("user-agent:"):
            agent = line.split(":", 1)[1].strip()
            if in_group and not current_agents:
                pass  # already reset
            elif in_group and current_agents:
                # New User-agent block without blank line separator — flush
                groups.append((current_agents, current_rules))
                current_agents = []
                current_rules = _RobotsRules()
            current_agents.append(agent.lower())
            in_group = True

        elif lower.startswith("disallow:") and in_group:
            path = line.split(":", 1)[1].strip()
            if path:
                current_rules.disallow.append(path)

        elif lower.startswith("allow:") and in_group:
            path = line.split(":", 1)[1].strip()
            if path:
                current_rules.allow.append(path)

        elif lower.startswith("crawl-delay:") and in_group:
            try:
                current_rules.crawl_delay = float(line.split(":", 1)[1].strip())
            except ValueError:
                pass

    # Flush last group
    if in_group and current_agents:
        groups.append((current_agents, current_rules))

    # Find best match: exact > wildcard > empty
    exact_match: Optional[_RobotsRules] = None
    wildcard_match: Optional[_RobotsRules] = None

    for agents, rules in groups:
        for agent in agents:
            if agent == ua_lower:
                exact_match = rules
            elif agent == "*":
                wildcard_match = rules

    return exact_match or wildcard_match or _RobotsRules()


def _path_matches(path: str, pattern: str) -> bool:
    """
    Check whether *path* matches a robots.txt Disallow/Allow *pattern*.

    Supports:
    - Prefix matching (standard)
    - Wildcard `*` inside pattern (extended standard)
    - End-of-string anchor `$`
    """
    # Anchor check
    anchored = pattern.endswith("$")
    pat = pattern.rstrip("$")

    # Convert robots wildcard to regex
    regex_pat = re.escape(pat).replace(r"\*", ".*")
    if anchored:
        regex_pat += "$"

    return bool(re.match(regex_pat, path))


class RobotsChecker:
    """
    Fetch and cache robots.txt rules; enforce allow/disallow for a given
    User-Agent before each crawl request.

    Usage:
        checker = RobotsChecker(user_agent="HarvestBot/1.0")
        allowed = checker.is_allowed("https://example.com/page")
        delay   = checker.crawl_delay("https://example.com")

    Usage (async, with polite delay):
        async def fetch_with_politeness(url):
            if not checker.is_allowed(url):
                return None
            await checker.async_respect_delay(url)
            return await _fetch(url)

    The checker caches robots.txt per domain so each domain is only fetched once.
    """

    def __init__(
        self,
        user_agent: str = "HarvestBot",
        timeout: int = 10,
    ):
        self._user_agent = user_agent
        self._timeout = timeout
        # Cache: domain → _RobotsRules
        self._cache: Dict[str, _RobotsRules] = {}
        # Timestamp of last request per domain (for crawl-delay enforcement)
        self._last_fetch: Dict[str, float] = {}

    def _get_rules(self, url: str) -> _RobotsRules:
        """Return cached (or freshly fetched) rules for url's domain."""
        parsed = urlparse(url)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        if origin not in self._cache:
            robots_url = f"{origin}/robots.txt"
            text = _fetch_robots(robots_url, timeout=self._timeout)
            if text:
                self._cache[origin] = _parse_robots(text, self._user_agent)
            else:
                # Unreachable → allow all (fail-open)
                self._cache[origin] = _RobotsRules()
        return self._cache[origin]

    def is_allowed(self, url: str) -> bool:
        """
        Return True iff *url* is allowed for this checker's User-Agent.

        Applies Allow rules before Disallow (longer match wins per RFC 9309).
        Zero-ambiguity: always returns bool.
        """
        parsed = urlparse(url)
        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"

        rules = self._get_rules(url)

        # If no rules at all, allow
        if not rules.disallow and not rules.allow:
            return True

        # Find best matching Allow and Disallow by longest prefix
        best_allow_len = -1
        best_disallow_len = -1

        for pat in rules.allow:
            if _path_matches(path, pat):
                best_allow_len = max(best_allow_len, len(pat))

        for pat in rules.disallow:
            if _path_matches(path, pat):
                best_disallow_len = max(best_disallow_len, len(pat))

        if best_allow_len >= 0 and best_allow_len >= best_disallow_len:
            return True
        if best_disallow_len >= 0:
            return False
        return True

    def crawl_delay(self, url: str) -> float:
        """
        Return Crawl-delay (seconds) from robots.txt for url's domain.
        Zero-ambiguity: always returns float (0.0 if not specified).
        """
        return self._get_rules(url).crawl_delay

    def respect_delay(self, url: str) -> None:
        """
        Block (synchronously) for any remaining crawl-delay since the last
        request to url's domain.  Safe to call before each fetch.
        """
        parsed = urlparse(url)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        delay = self.crawl_delay(url)
        if delay <= 0:
            return
        last = self._last_fetch.get(origin, 0.0)
        elapsed = time.time() - last
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_fetch[origin] = time.time()

    async def async_respect_delay(self, url: str) -> None:
        """
        Async version of respect_delay.  Yields control to the event loop
        rather than blocking the thread.
        """
        parsed = urlparse(url)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        delay = self.crawl_delay(url)
        if delay <= 0:
            return
        last = self._last_fetch.get(origin, 0.0)
        elapsed = time.time() - last
        remaining = delay - elapsed
        if remaining > 0:
            await asyncio.sleep(remaining)
        self._last_fetch[origin] = time.time()

    def clear_cache(self) -> None:
        """Clear cached robots rules (useful for testing or domain re-crawl)."""
        self._cache.clear()
        self._last_fetch.clear()
