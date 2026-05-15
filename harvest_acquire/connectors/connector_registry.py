"""
ConnectorRegistry — zero-config discovery and instantiation of all Harvest connectors.

Supports zero-cost discovery: callers can learn which connectors are available
based solely on environment variables, without importing connector modules or
making any network calls.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass
class ConnectorStatus:
    """Availability status for a single connector."""

    name: str
    available: bool
    missing_env_vars: List[str]
    config_hint: str


class ConnectorNotAvailableError(Exception):
    """Raised when a connector's required credentials are absent from the environment."""

    def __init__(self, name: str, status: ConnectorStatus) -> None:
        self.connector_name = name
        self.status = status
        super().__init__(
            f"Connector '{name}' is not available. {status.config_hint}"
        )


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class ConnectorRegistry:
    """Registry of all available connectors with auto-discovery capability.

    Supports zero-config discovery: lists which connectors are available
    based on environment variables, without requiring credentials upfront.

    Discovery is ZERO-COST — only checks os.environ, no imports or network calls.
    """

    # Map connector_name -> list of env-var alternatives (any one present = available).
    # An empty list means the connector requires no token (always available).
    CONNECTOR_ENV_MAP: Dict[str, List[str]] = {
        "github":     ["GITHUB_TOKEN", "GH_TOKEN"],
        "slack":      ["SLACK_BOT_TOKEN", "SLACK_TOKEN"],
        "gdrive":     ["GOOGLE_SERVICE_ACCOUNT_JSON", "GDRIVE_TOKEN", "GOOGLE_APPLICATION_CREDENTIALS"],
        "confluence": ["CONFLUENCE_TOKEN", "CONFLUENCE_API_TOKEN"],
        "jira":       ["JIRA_TOKEN", "JIRA_API_TOKEN"],
        "airtable":   ["AIRTABLE_API_KEY", "AIRTABLE_TOKEN"],
        "notion":     ["NOTION_TOKEN", "NOTION_API_KEY"],
        "s3":         ["AWS_ACCESS_KEY_ID", "AWS_DEFAULT_PROFILE"],
        "postgres":   ["DATABASE_URL", "POSTGRES_URL", "PG_DSN"],
        "gitlab":     ["GITLAB_TOKEN", "CI_JOB_TOKEN"],
        "rss":        [],   # No token required — always available
        "crawlee":    [],   # No token required — always available
    }

    # Lazy map: connector_name -> (module_path, class_name)
    _CONNECTOR_CLASS_MAP: Dict[str, tuple[str, str]] = {
        "github":     ("harvest_acquire.connectors.github_connector",     "GitHubConnector"),
        "slack":      ("harvest_acquire.connectors.slack_connector",      "SlackConnector"),
        "gdrive":     ("harvest_acquire.connectors.gdrive_connector",     "GDriveConnector"),
        "confluence": ("harvest_acquire.connectors.confluence_connector", "ConfluenceConnector"),
        "jira":       ("harvest_acquire.connectors.jira_connector",       "JiraConnector"),
        "airtable":   ("harvest_acquire.connectors.airtable_connector",   "AirtableConnector"),
        "notion":     ("harvest_acquire.connectors.notion_connector",     "NotionConnector"),
        "s3":         ("harvest_acquire.connectors.s3_connector",         "S3Connector"),
        "postgres":   ("harvest_acquire.connectors.postgres_connector",   "PostgresConnector"),
        "gitlab":     ("harvest_acquire.connectors.gitlab_connector",     "GitLabConnector"),
        "rss":        ("harvest_acquire.connectors.rss_connector",        "RSSConnector"),
        "crawlee":    ("harvest_acquire.crawl.crawlee_adapter",           "CrawleeAdapter"),
    }

    # ---------------------------------------------------------------------------
    # Discovery (zero-cost — env-only)
    # ---------------------------------------------------------------------------

    @classmethod
    def discover_available(cls) -> Dict[str, ConnectorStatus]:
        """Return availability status for every connector without loading them.

        Only inspects os.environ — no imports, no network, no side effects.

        Returns:
            Dict mapping connector_name -> ConnectorStatus.
        """
        result: Dict[str, ConnectorStatus] = {}
        for name, env_vars in cls.CONNECTOR_ENV_MAP.items():
            if not env_vars:
                result[name] = ConnectorStatus(
                    name=name,
                    available=True,
                    missing_env_vars=[],
                    config_hint="No credentials required.",
                )
            else:
                present = [v for v in env_vars if os.environ.get(v)]
                missing = [v for v in env_vars if not os.environ.get(v)]
                available = len(present) > 0
                hint = (
                    f"Set one of: {', '.join(env_vars)}"
                    if not available
                    else f"Credential found via {present[0]}."
                )
                result[name] = ConnectorStatus(
                    name=name,
                    available=available,
                    missing_env_vars=missing if not available else [],
                    config_hint=hint,
                )
        return result

    @classmethod
    def available_names(cls) -> List[str]:
        """Return connector names that have at least one credential present."""
        return [name for name, st in cls.discover_available().items() if st.available]

    @classmethod
    def unavailable_names(cls) -> List[str]:
        """Return connector names whose credentials are missing."""
        return [name for name, st in cls.discover_available().items() if not st.available]

    # ---------------------------------------------------------------------------
    # Instantiation (lazy import)
    # ---------------------------------------------------------------------------

    @classmethod
    def get_connector(cls, name: str) -> object:
        """Instantiate and return a connector by name.

        Raises:
            ConnectorNotAvailableError: if credentials are missing (not ConnectorError).
            KeyError: if name is not a known connector.
        """
        statuses = cls.discover_available()
        if name not in statuses:
            raise KeyError(f"Unknown connector: {name!r}. Known: {list(cls.CONNECTOR_ENV_MAP)}")
        status = statuses[name]
        if not status.available:
            raise ConnectorNotAvailableError(name, status)
        return cls._import_connector(name)

    @classmethod
    def get_connector_or_none(cls, name: str) -> Optional[object]:
        """Like get_connector but returns None instead of raising on missing creds."""
        try:
            return cls.get_connector(name)
        except ConnectorNotAvailableError:
            return None

    # ---------------------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------------------

    @classmethod
    def summary(cls) -> str:
        """Human-readable availability summary.

        Example: '2/12 connectors available (rss, crawlee)'
        """
        statuses = cls.discover_available()
        total = len(statuses)
        available = [name for name, st in statuses.items() if st.available]
        count = len(available)
        names_str = ", ".join(available) if available else "none"
        return f"{count}/{total} connectors available ({names_str})"

    # ---------------------------------------------------------------------------
    # Internal
    # ---------------------------------------------------------------------------

    @classmethod
    def _import_connector(cls, name: str) -> object:
        """Lazy-import and return a connector instance (no-arg construction)."""
        import importlib
        module_path, class_name = cls._CONNECTOR_CLASS_MAP[name]
        module = importlib.import_module(module_path)
        connector_cls = getattr(module, class_name)
        # Connectors with no required env vars can be constructed with defaults.
        # Others are imported but not instantiated here — caller provides config.
        # For the registry, we return the class itself for token-required connectors
        # so callers can instantiate with their own credentials.
        return connector_cls
