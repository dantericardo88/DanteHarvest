"""Tests for ConnectorRegistry — zero-config discovery and instantiation."""

from __future__ import annotations

import os
import pytest

from harvest_acquire.connectors.connector_registry import (
    ConnectorNotAvailableError,
    ConnectorRegistry,
    ConnectorStatus,
)


# ---------------------------------------------------------------------------
# discover_available
# ---------------------------------------------------------------------------


def test_discover_available_returns_dict():
    result = ConnectorRegistry.discover_available()
    assert isinstance(result, dict)
    assert len(result) > 0


def test_discover_all_connectors_present():
    result = ConnectorRegistry.discover_available()
    expected = {
        "github", "slack", "gdrive", "confluence", "jira",
        "airtable", "notion", "s3", "postgres", "gitlab",
        "rss", "crawlee",
    }
    assert expected == set(result.keys())


def test_discover_available_values_are_connector_status():
    result = ConnectorRegistry.discover_available()
    for name, status in result.items():
        assert isinstance(status, ConnectorStatus), f"{name} value is not ConnectorStatus"
        assert isinstance(status.available, bool)
        assert isinstance(status.missing_env_vars, list)
        assert isinstance(status.config_hint, str)


# ---------------------------------------------------------------------------
# Zero-token connectors always available
# ---------------------------------------------------------------------------


def test_rss_always_available(monkeypatch):
    # Remove any accidental env vars to ensure clean state
    monkeypatch.delenv("SOME_RSS_TOKEN", raising=False)
    result = ConnectorRegistry.discover_available()
    assert result["rss"].available is True
    assert result["rss"].missing_env_vars == []


def test_crawlee_always_available(monkeypatch):
    result = ConnectorRegistry.discover_available()
    assert result["crawlee"].available is True
    assert result["crawlee"].missing_env_vars == []


# ---------------------------------------------------------------------------
# Token-gated connectors
# ---------------------------------------------------------------------------


def test_github_unavailable_without_token(monkeypatch):
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GH_TOKEN", raising=False)
    result = ConnectorRegistry.discover_available()
    assert result["github"].available is False
    assert len(result["github"].missing_env_vars) > 0


def test_github_available_with_token(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_test_token")
    result = ConnectorRegistry.discover_available()
    assert result["github"].available is True
    assert result["github"].missing_env_vars == []


def test_github_available_with_gh_token_alias(monkeypatch):
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.setenv("GH_TOKEN", "ghp_alias_token")
    result = ConnectorRegistry.discover_available()
    assert result["github"].available is True


def test_slack_unavailable_without_token(monkeypatch):
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    monkeypatch.delenv("SLACK_TOKEN", raising=False)
    result = ConnectorRegistry.discover_available()
    assert result["slack"].available is False


def test_slack_available_with_token(monkeypatch):
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test")
    result = ConnectorRegistry.discover_available()
    assert result["slack"].available is True


# ---------------------------------------------------------------------------
# available_names / unavailable_names
# ---------------------------------------------------------------------------


def test_available_names_returns_list():
    names = ConnectorRegistry.available_names()
    assert isinstance(names, list)


def test_unavailable_names_returns_list():
    names = ConnectorRegistry.unavailable_names()
    assert isinstance(names, list)


def test_available_names_subset_of_all():
    all_names = set(ConnectorRegistry.CONNECTOR_ENV_MAP.keys())
    available = set(ConnectorRegistry.available_names())
    assert available.issubset(all_names)


def test_unavailable_names_no_overlap_with_available():
    available = set(ConnectorRegistry.available_names())
    unavailable = set(ConnectorRegistry.unavailable_names())
    assert available.isdisjoint(unavailable)


def test_available_and_unavailable_cover_all():
    all_names = set(ConnectorRegistry.CONNECTOR_ENV_MAP.keys())
    available = set(ConnectorRegistry.available_names())
    unavailable = set(ConnectorRegistry.unavailable_names())
    assert available | unavailable == all_names


def test_rss_in_available_names():
    available = ConnectorRegistry.available_names()
    assert "rss" in available


def test_crawlee_in_available_names():
    available = ConnectorRegistry.available_names()
    assert "crawlee" in available


# ---------------------------------------------------------------------------
# get_connector / get_connector_or_none
# ---------------------------------------------------------------------------


def test_get_connector_or_none_missing_creds_returns_none(monkeypatch):
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GH_TOKEN", raising=False)
    result = ConnectorRegistry.get_connector_or_none("github")
    assert result is None


def test_get_connector_or_none_rss_returns_not_none():
    result = ConnectorRegistry.get_connector_or_none("rss")
    assert result is not None


def test_get_connector_or_none_crawlee_returns_not_none():
    result = ConnectorRegistry.get_connector_or_none("crawlee")
    assert result is not None


def test_get_connector_raises_for_unknown_name():
    with pytest.raises(KeyError):
        ConnectorRegistry.get_connector("nonexistent_connector_xyz")


def test_get_connector_raises_not_available_for_missing_creds(monkeypatch):
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GH_TOKEN", raising=False)
    with pytest.raises(ConnectorNotAvailableError):
        ConnectorRegistry.get_connector("github")


def test_get_connector_returns_class_for_rss():
    cls = ConnectorRegistry.get_connector("rss")
    assert cls is not None
    # Should be importable as a class
    assert callable(cls)


# ---------------------------------------------------------------------------
# ConnectorNotAvailableError
# ---------------------------------------------------------------------------


def test_connector_not_available_error_has_hint(monkeypatch):
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    monkeypatch.delenv("SLACK_TOKEN", raising=False)
    with pytest.raises(ConnectorNotAvailableError) as exc_info:
        ConnectorRegistry.get_connector("slack")
    err = exc_info.value
    # The error message must contain a config hint pointing to env vars
    assert "SLACK" in str(err) or "Set one of" in str(err)
    assert err.connector_name == "slack"
    assert isinstance(err.status, ConnectorStatus)
    assert err.status.config_hint != ""


def test_connector_not_available_error_is_exception(monkeypatch):
    monkeypatch.delenv("GITLAB_TOKEN", raising=False)
    monkeypatch.delenv("CI_JOB_TOKEN", raising=False)
    with pytest.raises(Exception):
        ConnectorRegistry.get_connector("gitlab")


# ---------------------------------------------------------------------------
# summary()
# ---------------------------------------------------------------------------


def test_summary_returns_string():
    s = ConnectorRegistry.summary()
    assert isinstance(s, str)


def test_summary_shows_count():
    s = ConnectorRegistry.summary()
    # Must contain a fraction like "2/12"
    total = len(ConnectorRegistry.CONNECTOR_ENV_MAP)
    assert f"/{total}" in s


def test_summary_contains_rss():
    s = ConnectorRegistry.summary()
    assert "rss" in s


def test_summary_contains_crawlee():
    s = ConnectorRegistry.summary()
    assert "crawlee" in s


def test_summary_zero_creds_environment(monkeypatch):
    """In a stripped environment only token-free connectors show as available."""
    token_vars = [
        "GITHUB_TOKEN", "GH_TOKEN",
        "SLACK_BOT_TOKEN", "SLACK_TOKEN",
        "GOOGLE_SERVICE_ACCOUNT_JSON", "GDRIVE_TOKEN", "GOOGLE_APPLICATION_CREDENTIALS",
        "CONFLUENCE_TOKEN", "CONFLUENCE_API_TOKEN",
        "JIRA_TOKEN", "JIRA_API_TOKEN",
        "AIRTABLE_API_KEY", "AIRTABLE_TOKEN",
        "NOTION_TOKEN", "NOTION_API_KEY",
        "AWS_ACCESS_KEY_ID", "AWS_DEFAULT_PROFILE",
        "DATABASE_URL", "POSTGRES_URL", "PG_DSN",
        "GITLAB_TOKEN", "CI_JOB_TOKEN",
    ]
    for var in token_vars:
        monkeypatch.delenv(var, raising=False)

    s = ConnectorRegistry.summary()
    # Only rss and crawlee are available — so "2/12"
    assert "2/12" in s


# ---------------------------------------------------------------------------
# try_connect() on connector classes
# ---------------------------------------------------------------------------


def test_try_connect_rss_available():
    from harvest_acquire.connectors.rss_connector import RSSConnector
    status = RSSConnector.try_connect()
    assert isinstance(status, ConnectorStatus)
    assert status.available is True


def test_try_connect_slack_unavailable_without_token(monkeypatch):
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    monkeypatch.delenv("SLACK_TOKEN", raising=False)
    from harvest_acquire.connectors.slack_connector import SlackConnector
    status = SlackConnector.try_connect()
    assert status.available is False
    assert "SLACK" in status.config_hint or "Set one of" in status.config_hint


def test_try_connect_slack_available_with_token(monkeypatch):
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test")
    from harvest_acquire.connectors.slack_connector import SlackConnector
    status = SlackConnector.try_connect()
    assert status.available is True


def test_try_connect_github_unavailable_without_token(monkeypatch):
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GH_TOKEN", raising=False)
    from harvest_acquire.connectors.github_connector import GitHubConnector
    status = GitHubConnector.try_connect()
    assert status.available is False


def test_try_connect_does_not_raise(monkeypatch):
    """try_connect must never raise — even with no env vars set."""
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GH_TOKEN", raising=False)
    from harvest_acquire.connectors.github_connector import GitHubConnector
    # Should not raise
    status = GitHubConnector.try_connect()
    assert status is not None
