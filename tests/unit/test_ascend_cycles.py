"""
Ascend v3 — cycle verification tests.

Covers:
1. action_layer.py — BrowserActionType, BrowserAction, ActionResult, ActionLayer
2. pii_patterns.py — EXTENDED_PATTERNS, register_extended_patterns()
3. event_bus.py (updated) — AlertRule, dedup_window, dead_letters
4. playwright_pool.py (updated) — DEVICE_PROFILES, fingerprint_profile, health()
5. replay_harness.py (updated) — ActionType enum, action_handlers routing
6. connectors — PostgresConnector, GitLabConnector
"""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ===========================================================================
# Cycle 1 — agent_browser_infrastructure: ActionLayer
# ===========================================================================

class TestBrowserActionType:
    def test_all_types_have_string_values(self):
        from harvest_acquire.browser.action_layer import BrowserActionType
        for t in BrowserActionType:
            assert isinstance(t.value, str)

    def test_from_dict_roundtrip(self):
        from harvest_acquire.browser.action_layer import BrowserAction, BrowserActionType
        d = {"type": "click", "selector": "#btn", "timeout_ms": 3000}
        action = BrowserAction.from_dict(d)
        assert action.type == BrowserActionType.CLICK
        assert action.selector == "#btn"
        assert action.timeout_ms == 3000

    def test_to_dict_has_required_keys(self):
        from harvest_acquire.browser.action_layer import BrowserAction, BrowserActionType
        a = BrowserAction(type=BrowserActionType.FILL, selector="#field", value="hello")
        d = a.to_dict()
        assert d["type"] == "fill"
        assert d["selector"] == "#field"
        assert d["value"] == "hello"

    def test_action_result_success_always_bool(self):
        from harvest_acquire.browser.action_layer import ActionResult
        r = ActionResult(action_type="click", success=True)
        assert isinstance(r.success, bool)

    def test_action_sequence_result_success_is_all_passed(self):
        from harvest_acquire.browser.action_layer import ActionResult, ActionSequenceResult
        seq = ActionSequenceResult(actions=[
            ActionResult("click", success=True),
            ActionResult("fill", success=True),
        ])
        assert seq.success is True
        assert seq.failed_count == 0

    def test_action_sequence_fails_if_any_fail(self):
        from harvest_acquire.browser.action_layer import ActionResult, ActionSequenceResult
        seq = ActionSequenceResult(actions=[
            ActionResult("click", success=True),
            ActionResult("navigate", success=False, error="404"),
        ])
        assert seq.success is False
        assert seq.failed_count == 1


@pytest.mark.asyncio
async def test_action_layer_click_success():
    from harvest_acquire.browser.action_layer import ActionLayer, BrowserAction, BrowserActionType
    page = AsyncMock()
    page.click = AsyncMock()
    page.content = AsyncMock(return_value="<html></html>")
    layer = ActionLayer(page, capture_snapshots=False)
    result = await layer.execute(BrowserAction(type=BrowserActionType.CLICK, selector="#btn"))
    assert result.success is True
    page.click.assert_called_once_with("#btn", timeout=5000)


@pytest.mark.asyncio
async def test_action_layer_click_missing_selector_fails():
    from harvest_acquire.browser.action_layer import ActionLayer, BrowserAction, BrowserActionType
    page = AsyncMock()
    layer = ActionLayer(page, capture_snapshots=False)
    result = await layer.execute(BrowserAction(type=BrowserActionType.CLICK))
    assert result.success is False
    assert "selector" in result.error


@pytest.mark.asyncio
async def test_action_layer_fill():
    from harvest_acquire.browser.action_layer import ActionLayer, BrowserAction, BrowserActionType
    page = AsyncMock()
    page.fill = AsyncMock()
    layer = ActionLayer(page, capture_snapshots=False)
    result = await layer.execute(BrowserAction(type=BrowserActionType.FILL, selector="#f", value="hello"))
    assert result.success is True


@pytest.mark.asyncio
async def test_action_layer_evaluate():
    from harvest_acquire.browser.action_layer import ActionLayer, BrowserAction, BrowserActionType
    page = AsyncMock()
    page.evaluate = AsyncMock(return_value=42)
    layer = ActionLayer(page, capture_snapshots=False)
    result = await layer.execute(BrowserAction(type=BrowserActionType.EVALUATE, value="1+1"))
    assert result.success is True
    assert result.evaluate_result == 42


@pytest.mark.asyncio
async def test_action_layer_unknown_action_fails_closed():
    from harvest_acquire.browser.action_layer import ActionLayer, BrowserAction, BrowserActionType
    page = AsyncMock()
    layer = ActionLayer(page, capture_snapshots=False)
    # Force an invalid type string via object construction
    action = BrowserAction(type=BrowserActionType.WAIT, value="500")
    action.type = "not_a_real_type"  # type: ignore[assignment]
    result = await layer.execute(action)
    assert result.success is False


@pytest.mark.asyncio
async def test_action_layer_execute_sequence_stops_on_failure():
    from harvest_acquire.browser.action_layer import ActionLayer, BrowserAction, BrowserActionType
    page = AsyncMock()
    page.click = AsyncMock(side_effect=Exception("element not found"))
    page.content = AsyncMock(return_value="<html></html>")
    page.url = "https://example.com"
    layer = ActionLayer(page, capture_snapshots=False)
    seq = await layer.execute_sequence([
        BrowserAction(type=BrowserActionType.CLICK, selector="#missing"),
        BrowserAction(type=BrowserActionType.FILL, selector="#f", value="x"),
    ])
    assert seq.success is False
    assert len(seq.actions) == 1  # stopped after first failure


# ===========================================================================
# Cycle 2 — redaction_accuracy: pii_patterns
# ===========================================================================

class TestPiiPatterns:
    def test_extended_patterns_non_empty(self):
        from harvest_core.rights.pii_patterns import EXTENDED_PATTERNS
        assert len(EXTENDED_PATTERNS) >= 10

    def test_stripe_live_key_detected(self):
        from harvest_core.rights.pii_patterns import EXTENDED_PATTERNS
        # construct to avoid literal secret pattern triggering repo scanners
        text = "key = " + "sk" + "_live_" + "a" * 24
        assert EXTENDED_PATTERNS["stripe_key"].search(text)

    def test_stripe_test_key_detected(self):
        from harvest_core.rights.pii_patterns import EXTENDED_PATTERNS
        # construct to avoid literal secret pattern triggering repo scanners
        text = "STRIPE_KEY=" + "pk" + "_test_" + "a" * 24
        assert EXTENDED_PATTERNS["stripe_key"].search(text)

    def test_google_api_key_detected(self):
        from harvest_core.rights.pii_patterns import EXTENDED_PATTERNS
        text = "AIzaSyD-9tSrke72I6e8xample1234567890abc"
        assert EXTENDED_PATTERNS["google_api_key"].search(text)

    def test_intl_phone_uk_detected(self):
        from harvest_core.rights.pii_patterns import EXTENDED_PATTERNS
        text = "call +44 7911 123456 for support"
        assert EXTENDED_PATTERNS["intl_phone"].search(text)

    def test_intl_phone_india_detected(self):
        from harvest_core.rights.pii_patterns import EXTENDED_PATTERNS
        text = "contact +91 98765 43210"
        assert EXTENDED_PATTERNS["intl_phone"].search(text)

    def test_iban_detected(self):
        from harvest_core.rights.pii_patterns import EXTENDED_PATTERNS
        text = "IBAN: GB29NWBK60161331926819"
        assert EXTENDED_PATTERNS["iban"].search(text)

    def test_sendgrid_key_detected(self):
        from harvest_core.rights.pii_patterns import EXTENDED_PATTERNS
        text = "SG.abcdefghijklmnopqrstuv.abcdefghijklmnopqrstuvwxyzabcdefghijklmnopq"
        assert EXTENDED_PATTERNS["sendgrid_key"].search(text)

    def test_register_extended_patterns_is_idempotent(self):
        from harvest_core.rights.pii_patterns import register_extended_patterns
        register_extended_patterns()
        register_extended_patterns()  # second call should not raise or duplicate
        from harvest_core.rights.redaction_scanner import _PATTERNS
        stripe_count = sum(1 for k in _PATTERNS if k == "stripe_key")
        assert stripe_count == 1

    def test_register_wires_into_scanner(self):
        from harvest_core.rights.pii_patterns import register_extended_patterns
        register_extended_patterns()
        from harvest_core.rights.redaction_scanner import RedactionScanner
        scanner = RedactionScanner()
        result = scanner.scan("key = " + "sk" + "_live_" + "a" * 24)
        assert result.redaction_required


# ===========================================================================
# Cycle 3 — monitoring_and_alerting: AlertRule, dedup, dead_letters
# ===========================================================================

@pytest.mark.asyncio
async def test_alert_rule_fires_at_threshold():
    from harvest_core.monitoring.event_bus import AlertRule, HarvestEventBus
    fired = []

    async def on_alert(_event, _data):
        fired.append(True)

    bus = HarvestEventBus()
    rule = AlertRule(
        event="crawl.failed",
        threshold=3,
        window_secs=60,
        handler=on_alert,
        name="test-rule",
        dedup_cooldown_secs=0,
    )
    bus.add_rule(rule)

    for _ in range(3):
        await bus.emit("crawl.failed", {"url": f"https://x.com/{_}"})
    await asyncio.sleep(0.05)
    assert len(fired) >= 1


@pytest.mark.asyncio
async def test_alert_rule_does_not_fire_below_threshold():
    from harvest_core.monitoring.event_bus import AlertRule, HarvestEventBus
    fired = []

    async def on_alert(_event, _data):
        fired.append(True)

    bus = HarvestEventBus()
    rule = AlertRule(
        event="crawl.failed",
        threshold=5,
        window_secs=60,
        handler=on_alert,
        name="high-threshold",
    )
    bus.add_rule(rule)
    await bus.emit("crawl.failed", {"url": "https://x.com"})
    await bus.emit("crawl.failed", {"url": "https://y.com"})
    await asyncio.sleep(0.05)
    assert fired == []


@pytest.mark.asyncio
async def test_event_bus_dedup_suppresses_duplicates():
    from harvest_core.monitoring.event_bus import HarvestEventBus
    received = []

    async def handler(_event, data):
        received.append(data)

    bus = HarvestEventBus(dedup_window_secs=10.0)
    bus.on("crawl.completed", handler)

    data = {"url": "https://example.com", "pages": 5}
    await bus.emit("crawl.completed", data)
    await bus.emit("crawl.completed", data)   # duplicate within window
    assert len(received) == 1


@pytest.mark.asyncio
async def test_event_bus_dedup_zero_allows_all():
    from harvest_core.monitoring.event_bus import HarvestEventBus
    received = []

    async def handler(_event, _data):
        received.append(True)

    bus = HarvestEventBus(dedup_window_secs=0)
    bus.on("crawl.completed", handler)
    await bus.emit("crawl.completed", {"url": "https://x.com"})
    await bus.emit("crawl.completed", {"url": "https://x.com"})
    assert len(received) == 2


@pytest.mark.asyncio
async def test_dead_letters_recorded_on_handler_failure():
    from harvest_core.monitoring.event_bus import HarvestEventBus

    async def bad_handler(_event, _data):
        raise ValueError("boom")

    bus = HarvestEventBus()
    bus.on("crawl.failed", bad_handler)
    await bus.emit("crawl.failed", {"url": "https://x.com"})
    assert len(bus.dead_letters()) == 1
    assert "boom" in bus.dead_letters()[0][2]


def test_add_remove_rule():
    from harvest_core.monitoring.event_bus import AlertRule, HarvestEventBus
    bus = HarvestEventBus()
    rule = AlertRule(event="crawl.failed", threshold=3, window_secs=60,
                     handler=lambda e, d: None, name="my-rule")
    bus.add_rule(rule)
    assert len(bus.list_rules()) == 1
    removed = bus.remove_rule("my-rule")
    assert removed is True
    assert bus.list_rules() == []


def test_alert_rule_count_in_window():
    from harvest_core.monitoring.event_bus import AlertRule
    rule = AlertRule(event="x", threshold=3, window_secs=1,
                     handler=lambda e, d: None, name="r")
    rule.record()
    rule.record()
    assert rule.count_in_window() == 2


# ===========================================================================
# Cycle 4 — browser_infrastructure: DEVICE_PROFILES, fingerprint, health()
# ===========================================================================

def test_device_profiles_have_required_keys():
    from harvest_acquire.browser.playwright_pool import DEVICE_PROFILES
    for name, profile in DEVICE_PROFILES.items():
        assert "user_agent" in profile, f"{name} missing user_agent"
        assert "viewport" in profile, f"{name} missing viewport"
        assert "timezone_id" in profile, f"{name} missing timezone_id"


def test_device_profiles_at_least_4():
    from harvest_acquire.browser.playwright_pool import DEVICE_PROFILES
    assert len(DEVICE_PROFILES) >= 4


def test_playwright_pool_accepts_fingerprint_profile():
    from harvest_acquire.browser.playwright_pool import PlaywrightPool
    pool = PlaywrightPool(fingerprint_profile="chrome_windows")
    assert pool.fingerprint_profile == "chrome_windows"


def test_playwright_pool_fingerprint_none_by_default():
    from harvest_acquire.browser.playwright_pool import PlaywrightPool
    pool = PlaywrightPool()
    assert pool.fingerprint_profile is None


def test_playwright_pool_health_returns_dict():
    from harvest_acquire.browser.playwright_pool import PlaywrightPool
    pool = PlaywrightPool()
    h = pool.health()
    assert "open" in h
    assert "browser_count" in h
    assert "browsers" in h
    assert isinstance(h["browsers"], list)


def test_browser_slot_uptime_and_idle():
    from harvest_acquire.browser.playwright_pool import _BrowserSlot
    slot = _BrowserSlot(browser=MagicMock())
    assert slot.uptime_secs() >= 0
    assert slot.idle_secs() >= 0


# ===========================================================================
# Cycle 5 — replay_harness_fidelity: ActionType routing
# ===========================================================================

def test_action_type_from_action_click():
    from harvest_index.registry.replay_harness import ActionType
    assert ActionType.from_action("click #btn") == ActionType.CLICK


def test_action_type_from_action_navigate():
    from harvest_index.registry.replay_harness import ActionType
    assert ActionType.from_action("navigate:https://example.com") == ActionType.NAVIGATE


def test_action_type_from_action_unknown():
    from harvest_index.registry.replay_harness import ActionType
    assert ActionType.from_action("do_something_weird") == ActionType.UNKNOWN


def test_action_type_from_empty_string():
    from harvest_index.registry.replay_harness import ActionType
    assert ActionType.from_action("") == ActionType.UNKNOWN


@pytest.mark.asyncio
async def test_replay_harness_routes_to_action_handler():
    from harvest_index.registry.replay_harness import ReplayHarness
    from harvest_distill.packs.pack_schemas import WorkflowPack, PackStep, EvalSummary

    click_calls = []

    async def click_handler(action, step_id, context, action_type, **kw):
        click_calls.append(action)
        return {"passed": True}

    pack = WorkflowPack(
        pack_id="rh-test",
        title="Routing Test",
        goal="test",
        steps=[PackStep(id="s1", action="click #btn")],
        eval_summary=EvalSummary(replay_pass_rate=1.0, sample_size=1),
    )

    harness = ReplayHarness(action_handlers={"click": click_handler})
    report = await harness.replay(pack, run_id="run-rh-1")
    assert report.pass_rate == 1.0
    assert len(click_calls) == 1


@pytest.mark.asyncio
async def test_replay_harness_falls_back_to_executor_for_unrouted_type():
    from harvest_index.registry.replay_harness import ReplayHarness
    from harvest_distill.packs.pack_schemas import WorkflowPack, PackStep, EvalSummary

    executor_calls = []

    async def executor(action, step_id, context, action_type, **kw):
        executor_calls.append(action_type)
        return {"passed": True}

    pack = WorkflowPack(
        pack_id="rh-fallback",
        title="Fallback Test",
        goal="test",
        steps=[PackStep(id="s1", action="scroll down")],
        eval_summary=EvalSummary(replay_pass_rate=1.0, sample_size=1),
    )

    # action_handlers has no "scroll" entry → falls back to executor
    harness = ReplayHarness(
        step_executor=executor,
        action_handlers={"click": AsyncMock(return_value={"passed": True})},
    )
    report = await harness.replay(pack, run_id="run-rh-2")
    assert "scroll" in executor_calls


# ===========================================================================
# Cycle 6 — connector_breadth: PostgresConnector, GitLabConnector
# ===========================================================================

class TestPostgresConnector:
    def test_raises_connector_error_without_psycopg2(self):
        from harvest_acquire.connectors.postgres_connector import PostgresConnector
        conn = PostgresConnector(dsn="postgresql://localhost/test")
        with patch.dict("sys.modules", {"psycopg2": None}):
            with pytest.raises(Exception):
                conn.ingest(tables=["orders"])

    def test_dsn_host_extraction(self):
        from harvest_acquire.connectors.postgres_connector import PostgresConnector
        conn = PostgresConnector(dsn="postgresql://user:pass@db.example.com:5432/mydb")
        assert conn._dsn_host() == "db.example.com"

    def test_dsn_host_fallback_on_bad_dsn(self):
        from harvest_acquire.connectors.postgres_connector import PostgresConnector
        conn = PostgresConnector(dsn="not-a-url")
        host = conn._dsn_host()
        assert isinstance(host, str)

    def test_table_to_markdown_format(self):
        from harvest_acquire.connectors.postgres_connector import PostgresConnector
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            conn = PostgresConnector(dsn="postgresql://localhost/test", storage_root=tmp)
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.__enter__ = lambda s: s
            mock_cursor.__exit__ = MagicMock(return_value=False)
            # First cursor call: columns
            # Second cursor call: data rows
            mock_cursor.fetchall.side_effect = [
                [("id",), ("name",), ("email",)],
                [(1, "Alice", "alice@example.com"), (2, "Bob", "bob@example.com")],
            ]
            mock_conn.cursor.return_value = mock_cursor
            md = conn._table_to_markdown(mock_conn, "public", "users", 100)
            assert "# public.users" in md
            assert "| id | name | email |" in md
            assert "Alice" in md


class TestGitLabConnector:
    def test_requires_token(self):
        from harvest_acquire.connectors.gitlab_connector import GitLabConnector, ConnectorError
        with pytest.raises(ConnectorError):
            GitLabConnector(token="")

    def test_build_url_no_params(self):
        from harvest_acquire.connectors.gitlab_connector import GitLabConnector
        conn = GitLabConnector(token="tok", base_url="https://gitlab.com")
        url = conn._build_url("/api/v4/projects")
        assert url == "https://gitlab.com/api/v4/projects"

    def test_build_url_with_params(self):
        from harvest_acquire.connectors.gitlab_connector import GitLabConnector
        conn = GitLabConnector(token="tok")
        url = conn._build_url("/api/v4/projects", {"per_page": 20, "search": "myrepo"})
        assert "per_page=20" in url
        assert "search=myrepo" in url

    def test_ingest_filters_by_extension(self):
        from harvest_acquire.connectors.gitlab_connector import GitLabConnector
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            conn = GitLabConnector(token="tok", storage_root=tmp)
            with patch.object(conn, "_list_tree", return_value=[
                {"type": "blob", "path": "src/main.py", "name": "main.py"},
                {"type": "blob", "path": "src/README.md", "name": "README.md"},
                {"type": "tree", "path": "src", "name": "src"},
            ]):
                with patch.object(conn, "_get_file_content", return_value="print('hello')"):
                    ids = conn.ingest("group/repo", extensions=[".py"])
            assert len(ids) == 1

    def test_ingest_skips_failed_files(self):
        from harvest_acquire.connectors.gitlab_connector import GitLabConnector, ConnectorError
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            conn = GitLabConnector(token="tok", storage_root=tmp)
            with patch.object(conn, "_list_tree", return_value=[
                {"type": "blob", "path": "a.py", "name": "a.py"},
                {"type": "blob", "path": "b.py", "name": "b.py"},
            ]):
                with patch.object(conn, "_get_file_content", side_effect=[
                    "content a",
                    ConnectorError("404"),
                ]):
                    ids = conn.ingest("group/repo")
            assert len(ids) == 1
