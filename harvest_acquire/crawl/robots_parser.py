"""robots.txt parser with wildcard support and sitemap discovery.

sitemap_and_robots_compliance (score 9): full robots.txt parser with wildcard
support, sitemap.xml discovery and parsing, and disallow checking.

Constitutional guarantees:
- Fail-open: network errors allow all URLs (safe for crawling)
- Zero-ambiguity: is_allowed() always returns bool, never None
- Local-first: pure stdlib, no external parser dependency
"""
import re
import urllib.request
import urllib.parse
from typing import List, Optional


class RobotsRule:
    def __init__(
        self,
        user_agent: str,
        allow: List[str],
        disallow: List[str],
        crawl_delay: Optional[float] = None,
    ):
        self.user_agent = user_agent
        self.allow = allow
        self.disallow = disallow
        self.crawl_delay = crawl_delay


class RobotsParser:
    """Parse robots.txt and check URL access. Supports wildcards and Sitemap directives."""

    USER_AGENT = "DanteHarvest/1.0"

    def __init__(self):
        self._rules: List[RobotsRule] = []
        self._sitemaps: List[str] = []
        self._raw: str = ""

    def parse(self, content: str) -> None:
        """Parse robots.txt content string."""
        self._raw = content
        self._rules = []
        self._sitemaps = []

        current_agents: List[str] = []
        current_allow: List[str] = []
        current_disallow: List[str] = []
        current_delay: Optional[float] = None

        def _flush() -> None:
            for agent in current_agents:
                self._rules.append(
                    RobotsRule(
                        agent,
                        list(current_allow),
                        list(current_disallow),
                        current_delay,
                    )
                )

        for line in content.splitlines():
            line = line.split("#")[0].strip()
            if not line:
                if current_agents:
                    _flush()
                    current_agents = []
                    current_allow = []
                    current_disallow = []
                    current_delay = None
                continue

            if ":" not in line:
                continue
            key, _, value = line.partition(":")
            key = key.strip().lower()
            value = value.strip()

            if key == "user-agent":
                # Starting a new user-agent block — flush previous if rules exist
                if current_allow or current_disallow:
                    _flush()
                    current_agents = []
                    current_allow = []
                    current_disallow = []
                    current_delay = None
                current_agents.append(value)
            elif key == "allow":
                current_allow.append(value)
            elif key == "disallow":
                current_disallow.append(value)
            elif key == "crawl-delay":
                try:
                    current_delay = float(value)
                except ValueError:
                    pass
            elif key == "sitemap":
                self._sitemaps.append(value)

        if current_agents:
            _flush()

    def _match_pattern(self, pattern: str, path: str) -> bool:
        """Match path against robots.txt pattern (supports * and $)."""
        if not pattern:
            return False
        # Convert robots pattern to regex
        regex = re.escape(pattern).replace(r"\*", ".*").replace(r"\$", "$")
        return bool(re.match(regex, path))

    def is_allowed(self, url: str, user_agent: str = None) -> bool:
        """Check if URL is allowed for given user_agent."""
        user_agent = user_agent or self.USER_AGENT
        parsed = urllib.parse.urlparse(url)
        path = parsed.path or "/"

        # Find applicable rules (specific agent first, then *)
        applicable = [r for r in self._rules if r.user_agent == user_agent]
        if not applicable:
            applicable = [r for r in self._rules if r.user_agent == "*"]
        if not applicable:
            return True  # No rules = allow all

        # Check allow/disallow (longest match wins)
        best_allow = 0
        best_disallow = 0

        for rule in applicable:
            for pattern in rule.allow:
                if self._match_pattern(pattern, path):
                    best_allow = max(best_allow, len(pattern))
            for pattern in rule.disallow:
                if self._match_pattern(pattern, path):
                    best_disallow = max(best_disallow, len(pattern))

        if best_allow >= best_disallow and best_allow > 0:
            return True
        if best_disallow > 0:
            return False
        return True

    def get_crawl_delay(self, user_agent: str = None) -> Optional[float]:
        """Get crawl delay for given user_agent."""
        user_agent = user_agent or self.USER_AGENT
        for rule in self._rules:
            if rule.user_agent in (user_agent, "*") and rule.crawl_delay is not None:
                return rule.crawl_delay
        return None

    def get_sitemaps(self) -> List[str]:
        """Return list of Sitemap URLs declared in robots.txt."""
        return list(self._sitemaps)

    @classmethod
    def fetch_and_parse(cls, base_url: str, timeout: int = 10) -> "RobotsParser":
        """Fetch robots.txt from base_url and parse it."""
        robots_url = urllib.parse.urljoin(base_url, "/robots.txt")
        parser = cls()
        try:
            with urllib.request.urlopen(robots_url, timeout=timeout) as resp:
                content = resp.read().decode("utf-8", errors="replace")
            parser.parse(content)
        except Exception:
            pass  # Network error = allow all (fail-open)
        return parser


class SitemapParser:
    """Parse sitemap.xml and extract URLs."""

    def parse_xml(self, content: str) -> List[str]:
        """Extract URLs from sitemap XML string."""
        urls = re.findall(
            r"<loc>\s*(.*?)\s*</loc>", content, re.IGNORECASE | re.DOTALL
        )
        return [u.strip() for u in urls if u.strip()]

    def parse_index(self, content: str) -> List[str]:
        """Parse sitemap index and return child sitemap URLs."""
        return self.parse_xml(content)  # Same <loc> structure

    def get_all_urls(self, base_url: str, max_urls: int = 10000) -> List[str]:
        """Fetch and parse sitemap(s) from base_url. Returns unique URL list."""
        robots = RobotsParser.fetch_and_parse(base_url)
        sitemaps = robots.get_sitemaps()
        if not sitemaps:
            sitemaps = [urllib.parse.urljoin(base_url, "/sitemap.xml")]

        all_urls: List[str] = []
        seen: set = set()
        for sitemap_url in sitemaps[:5]:  # limit sitemap count
            try:
                with urllib.request.urlopen(sitemap_url, timeout=10) as resp:
                    content = resp.read().decode("utf-8", errors="replace")
                urls = self.parse_xml(content)
                for url in urls:
                    if url not in seen:
                        seen.add(url)
                        all_urls.append(url)
                if len(all_urls) >= max_urls:
                    break
            except Exception:
                pass
        return all_urls[:max_urls]
