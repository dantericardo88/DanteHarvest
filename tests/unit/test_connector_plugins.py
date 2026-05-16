"""
Tests for the ConnectorPlugin framework added to ConnectorRegistry:
- register_plugin stores a plugin class
- connector_plugin decorator auto-registers
- list_plugins returns registered plugins
- get_plugin raises KeyError for unknown names
- discover_all includes built_in and plugins keys
- load_plugins_from_directory counts newly registered plugins
"""

from __future__ import annotations

import os
import sys
import textwrap
from pathlib import Path

import pytest

from harvest_acquire.connectors.connector_registry import (
    ConnectorPlugin,
    ConnectorRegistry,
    connector_plugin,
)


# ---------------------------------------------------------------------------
# Helpers — isolate plugin registry between tests
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def clean_plugin_registry():
    """Snapshot and restore the plugin registry around each test."""
    before = dict(ConnectorRegistry._plugin_registry)
    yield
    ConnectorRegistry._plugin_registry.clear()
    ConnectorRegistry._plugin_registry.update(before)


# ---------------------------------------------------------------------------
# ConnectorPlugin base class
# ---------------------------------------------------------------------------

class TestConnectorPluginBase:
    def test_is_available_no_env_vars(self):
        class AlwaysOn(ConnectorPlugin):
            name = "always_on"
            required_env_vars = []

        assert AlwaysOn().is_available() is True

    def test_is_available_missing_env_var(self, monkeypatch):
        monkeypatch.delenv("_TEST_MISSING_VAR", raising=False)

        class NeedsEnv(ConnectorPlugin):
            name = "needs_env"
            required_env_vars = ["_TEST_MISSING_VAR"]

        assert NeedsEnv().is_available() is False

    def test_is_available_env_var_set(self, monkeypatch):
        monkeypatch.setenv("_TEST_PRESENT_VAR", "secret")

        class NeedsEnv(ConnectorPlugin):
            name = "needs_env2"
            required_env_vars = ["_TEST_PRESENT_VAR"]

        assert NeedsEnv().is_available() is True

    def test_get_config_hint_no_vars(self):
        class Free(ConnectorPlugin):
            name = "free"
            required_env_vars = []

        assert "No credentials" in Free().get_config_hint()

    def test_get_config_hint_with_vars(self):
        class Gated(ConnectorPlugin):
            name = "gated"
            required_env_vars = ["GATED_KEY"]

        hint = Gated().get_config_hint()
        assert "GATED_KEY" in hint

    def test_connect_raises_not_implemented(self):
        class Stub(ConnectorPlugin):
            name = "stub"

        with pytest.raises(NotImplementedError):
            Stub().connect()

    def test_fetch_raises_not_implemented(self):
        class Stub(ConnectorPlugin):
            name = "stub2"

        with pytest.raises(NotImplementedError):
            Stub().fetch("query")


# ---------------------------------------------------------------------------
# ConnectorRegistry.register_plugin
# ---------------------------------------------------------------------------

class TestRegisterPlugin:
    def test_stores_plugin_class(self):
        class MyPlugin(ConnectorPlugin):
            name = "my_plugin"

        ConnectorRegistry.register_plugin(MyPlugin)
        assert "my_plugin" in ConnectorRegistry._plugin_registry
        assert ConnectorRegistry._plugin_registry["my_plugin"] is MyPlugin

    def test_raises_without_name(self):
        class Unnamed(ConnectorPlugin):
            name = ""

        with pytest.raises(ValueError, match="name"):
            ConnectorRegistry.register_plugin(Unnamed)

    def test_raises_without_name_attribute(self):
        class NoName(ConnectorPlugin):
            pass

        # name defaults to "" from base class — should still raise
        with pytest.raises(ValueError):
            ConnectorRegistry.register_plugin(NoName)

    def test_overwrite_existing_plugin(self):
        class PlugA(ConnectorPlugin):
            name = "overwrite_me"

        class PlugB(ConnectorPlugin):
            name = "overwrite_me"

        ConnectorRegistry.register_plugin(PlugA)
        ConnectorRegistry.register_plugin(PlugB)
        assert ConnectorRegistry._plugin_registry["overwrite_me"] is PlugB


# ---------------------------------------------------------------------------
# connector_plugin decorator
# ---------------------------------------------------------------------------

class TestConnectorPluginDecorator:
    def test_decorator_registers_class(self):
        @connector_plugin
        class DecoratedPlugin(ConnectorPlugin):
            name = "decorated_plugin"

        assert "decorated_plugin" in ConnectorRegistry._plugin_registry

    def test_decorator_returns_original_class(self):
        @connector_plugin
        class ReturnedPlugin(ConnectorPlugin):
            name = "returned_plugin"

        assert ReturnedPlugin.name == "returned_plugin"
        # Class is still usable after decoration
        instance = ReturnedPlugin()
        assert instance.is_available() is True

    def test_decorator_raises_for_nameless_plugin(self):
        with pytest.raises(ValueError):
            @connector_plugin
            class BadPlugin(ConnectorPlugin):
                name = ""


# ---------------------------------------------------------------------------
# ConnectorRegistry.get_plugin
# ---------------------------------------------------------------------------

class TestGetPlugin:
    def test_returns_registered_class(self):
        class FindMe(ConnectorPlugin):
            name = "find_me"

        ConnectorRegistry.register_plugin(FindMe)
        assert ConnectorRegistry.get_plugin("find_me") is FindMe

    def test_raises_key_error_for_unknown_name(self):
        with pytest.raises(KeyError, match="not_registered"):
            ConnectorRegistry.get_plugin("not_registered")

    def test_error_message_lists_available(self):
        class Listed(ConnectorPlugin):
            name = "listed_plugin"

        ConnectorRegistry.register_plugin(Listed)
        with pytest.raises(KeyError) as exc_info:
            ConnectorRegistry.get_plugin("unknown_xyz")
        # The error message should mention available plugins
        assert "listed_plugin" in str(exc_info.value)


# ---------------------------------------------------------------------------
# ConnectorRegistry.list_plugins
# ---------------------------------------------------------------------------

class TestListPlugins:
    def test_empty_when_no_plugins_registered(self):
        # Clean fixture ensures no extra plugins
        assert ConnectorRegistry.list_plugins() == []

    def test_returns_registered_plugins(self):
        class PlugX(ConnectorPlugin):
            name = "plug_x"
            required_env_vars = []

        ConnectorRegistry.register_plugin(PlugX)
        plugins = ConnectorRegistry.list_plugins()
        assert len(plugins) == 1
        assert plugins[0]["name"] == "plug_x"
        assert plugins[0]["available"] is True

    def test_availability_reflects_env(self, monkeypatch):
        monkeypatch.delenv("_TEST_PLUG_Y_KEY", raising=False)

        class PlugY(ConnectorPlugin):
            name = "plug_y"
            required_env_vars = ["_TEST_PLUG_Y_KEY"]

        ConnectorRegistry.register_plugin(PlugY)
        plugins = ConnectorRegistry.list_plugins()
        assert plugins[0]["available"] is False

        monkeypatch.setenv("_TEST_PLUG_Y_KEY", "value")
        plugins = ConnectorRegistry.list_plugins()
        assert plugins[0]["available"] is True

    def test_multiple_plugins_all_listed(self):
        for i in range(3):
            class _P(ConnectorPlugin):
                name = f"multi_plug_{i}"
            _P.name = f"multi_plug_{i}"
            ConnectorRegistry.register_plugin(_P)

        plugins = ConnectorRegistry.list_plugins()
        names = {p["name"] for p in plugins}
        assert {"multi_plug_0", "multi_plug_1", "multi_plug_2"}.issubset(names)


# ---------------------------------------------------------------------------
# ConnectorRegistry.discover_all
# ---------------------------------------------------------------------------

class TestDiscoverAll:
    def test_has_built_in_key(self):
        result = ConnectorRegistry.discover_all()
        assert "built_in" in result

    def test_has_plugins_key(self):
        result = ConnectorRegistry.discover_all()
        assert "plugins" in result

    def test_has_total_key(self):
        result = ConnectorRegistry.discover_all()
        assert "total" in result

    def test_total_matches_sum(self):
        result = ConnectorRegistry.discover_all()
        assert result["total"] == len(result["built_in"]) + len(result["plugins"])

    def test_total_increases_after_plugin_registered(self):
        before = ConnectorRegistry.discover_all()["total"]

        class Extra(ConnectorPlugin):
            name = "extra_for_total"

        ConnectorRegistry.register_plugin(Extra)
        after = ConnectorRegistry.discover_all()["total"]
        assert after == before + 1

    def test_built_in_contains_known_connectors(self):
        result = ConnectorRegistry.discover_all()
        assert "github" in result["built_in"]
        assert "rss" in result["built_in"]


# ---------------------------------------------------------------------------
# ConnectorRegistry.load_plugins_from_directory
# ---------------------------------------------------------------------------

class TestLoadPluginsFromDirectory:
    def test_nonexistent_directory_returns_zero(self, tmp_path):
        count = ConnectorRegistry.load_plugins_from_directory(
            str(tmp_path / "does_not_exist")
        )
        assert count == 0

    def test_empty_directory_returns_zero(self, tmp_path):
        count = ConnectorRegistry.load_plugins_from_directory(str(tmp_path))
        assert count == 0

    def test_loads_plugin_from_file(self, tmp_path):
        plugin_src = textwrap.dedent("""
            from harvest_acquire.connectors.connector_registry import (
                ConnectorPlugin, ConnectorRegistry
            )

            class _FilePlugin(ConnectorPlugin):
                name = "file_loaded_plugin"
                required_env_vars = []

            ConnectorRegistry.register_plugin(_FilePlugin)
        """)
        (tmp_path / "my_plugin.py").write_text(plugin_src, encoding="utf-8")

        count = ConnectorRegistry.load_plugins_from_directory(str(tmp_path))
        assert count == 1
        assert "file_loaded_plugin" in ConnectorRegistry._plugin_registry

    def test_skips_underscore_files(self, tmp_path):
        plugin_src = textwrap.dedent("""
            from harvest_acquire.connectors.connector_registry import (
                ConnectorPlugin, ConnectorRegistry
            )

            class _SkippedPlugin(ConnectorPlugin):
                name = "should_not_be_loaded"

            ConnectorRegistry.register_plugin(_SkippedPlugin)
        """)
        (tmp_path / "__private.py").write_text(plugin_src, encoding="utf-8")

        count = ConnectorRegistry.load_plugins_from_directory(str(tmp_path))
        assert count == 0
        assert "should_not_be_loaded" not in ConnectorRegistry._plugin_registry

    def test_malformed_file_does_not_raise(self, tmp_path):
        (tmp_path / "broken.py").write_text("this is not valid python !!!", encoding="utf-8")
        # Must not raise
        count = ConnectorRegistry.load_plugins_from_directory(str(tmp_path))
        assert count == 0
