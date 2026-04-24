"""
RobotsValidator — enforce robots.txt compliance.

Transplanted from DanteDistillerV2/backend/utils/robots_validator.py.
User-agent renamed to HarvestBot. Import paths updated.

Constitutional guarantee: HARD BLOCK on disallowed URLs. Fail-closed on
network errors (error → DISALLOW, never ALLOW).
"""

import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import httpx

from harvest_core.control.exceptions import ConstitutionalError


class RobotRules:
    """Parsed robots.txt rules with wildcard/anchor pattern matching."""

    def __init__(self):
        self.rules: Dict[str, Dict[str, List[str]]] = {}
        self.crawl_delays: Dict[str, float] = {}

    @classmethod
    def parse(cls, robots_txt: str) -> "RobotRules":
        rules = cls()
        if not robots_txt.strip():
            return rules
        current_agents: List[str] = []
        for line in robots_txt.split("\n"):
            line = line.split("#")[0].strip()
            if not line or ":" not in line:
                continue
            directive, value = line.split(":", 1)
            directive, value = directive.strip().lower(), value.strip()
            if directive == "user-agent":
                agent = value.lower()
                current_agents.append(agent)
                rules.rules.setdefault(agent, {"allow": [], "disallow": []})
            elif directive == "disallow":
                for a in current_agents:
                    if a in rules.rules:
                        rules.rules[a]["disallow"].append(value)
            elif directive == "allow":
                for a in current_agents:
                    if a in rules.rules:
                        rules.rules[a]["allow"].append(value)
            elif directive == "crawl-delay":
                try:
                    delay = float(value)
                    for a in current_agents:
                        rules.crawl_delays[a] = delay
                except ValueError:
                    pass
        return rules

    def is_allowed(self, path: str, user_agent: str) -> bool:
        agent_lower = user_agent.lower()
        agent_rules = (
            self.rules.get(agent_lower)
            or next((self.rules[k] for k in self.rules if k in agent_lower or agent_lower in k), None)
            or self.rules.get("*")
        )
        if not agent_rules:
            return True
        allow_match = self._find_matching_pattern(path, agent_rules["allow"])
        disallow_match = self._find_matching_pattern(path, agent_rules["disallow"])
        if not allow_match and not disallow_match:
            return True
        if allow_match and not disallow_match:
            return True
        if disallow_match and not allow_match:
            return False
        return len(allow_match) >= len(disallow_match)

    def _find_matching_pattern(self, path: str, patterns: List[str]) -> Optional[str]:
        best, best_len = None, 0
        for pattern in patterns:
            if self._pattern_matches(path, pattern) and len(pattern) > best_len:
                best, best_len = pattern, len(pattern)
        return best

    def _pattern_matches(self, path: str, pattern: str) -> bool:
        if not pattern:
            return path == "/"
        regex = re.escape(pattern).replace(r"\*", ".*")
        if regex.endswith(r"\$"):
            regex = regex[:-2] + "$"
        try:
            return bool(re.match("^" + regex, path))
        except re.error:
            return False

    def get_crawl_delay(self, user_agent: str) -> Optional[float]:
        agent_lower = user_agent.lower()
        return self.crawl_delays.get(agent_lower) or self.crawl_delays.get("*")


class RobotsValidator:
    """
    Validate URL access against robots.txt rules.

    - Caches parsed rules per domain for cache_ttl seconds.
    - Fail-closed: network errors → DISALLOW.
    - 404 robots.txt → allow all.
    """

    def __init__(self, user_agent: str = "HarvestBot", cache_ttl: int = 86400):
        self.user_agent = user_agent
        self.cache_ttl = cache_ttl
        self._cache: Dict[str, Tuple[datetime, RobotRules]] = {}

    async def is_allowed(self, url: str, user_agent: Optional[str] = None) -> bool:
        agent = user_agent or self.user_agent
        try:
            parsed = urlparse(url)
            domain = f"{parsed.scheme}://{parsed.netloc}"
            path = parsed.path or "/"
            rules = await self._get_rules(domain)
            return rules.is_allowed(path, agent)
        except Exception:
            return False  # fail-closed

    async def _get_rules(self, domain: str) -> RobotRules:
        if domain in self._cache:
            cached_time, rules = self._cache[domain]
            if (datetime.utcnow() - cached_time).total_seconds() < self.cache_ttl:
                return rules
        robots_txt = await self._fetch_robots_txt(domain)
        rules = RobotRules.parse(robots_txt)
        self._cache[domain] = (datetime.utcnow(), rules)
        return rules

    async def _fetch_robots_txt(self, domain: str) -> str:
        robots_url = urljoin(domain, "/robots.txt")
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                robots_url,
                headers={"User-Agent": self.user_agent},
                follow_redirects=True,
            )
            if response.status_code == 404:
                return ""
            if response.status_code != 200:
                raise RuntimeError(f"HTTP {response.status_code}")
            return response.text

    def clear_cache(self, domain: Optional[str] = None) -> None:
        if domain:
            self._cache.pop(domain, None)
        else:
            self._cache.clear()


_default_validator: Optional[RobotsValidator] = None


def get_validator() -> RobotsValidator:
    global _default_validator
    if _default_validator is None:
        _default_validator = RobotsValidator()
    return _default_validator


async def check_robots_txt(url: str) -> None:
    """Raise ConstitutionalError if URL is disallowed by robots.txt."""
    validator = get_validator()
    if not await validator.is_allowed(url):
        parsed = urlparse(url)
        raise ConstitutionalError(
            f"Access to {url} is disallowed by robots.txt. "
            f"Domain: {parsed.netloc}, Path: {parsed.path}"
        )
