"""
Phase 6 — Monitoring event bus tests.

Verifies:
1. HarvestEventBus subscribe/unsubscribe/emit
2. Fan-out to multiple handlers
3. Handler failures are isolated (fail-open)
4. WebhookSink HMAC signing and verification
5. EmailSink filters on event type
"""

import hmac
import hashlib
from unittest.mock import MagicMock, patch
import pytest

from harvest_core.monitoring.event_bus import HarvestEventBus, WebhookSink, EmailSink, HARVEST_EVENTS


# ---------------------------------------------------------------------------
# HarvestEventBus
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_event_bus_subscribe_and_emit():
    bus = HarvestEventBus()
    received = []

    async def handler(event, data):
        received.append((event, data))

    bus.on("crawl.completed", handler)
    await bus.emit("crawl.completed", {"url": "https://x.com", "pages": 3})
    assert len(received) == 1
    assert received[0][0] == "crawl.completed"
    assert received[0][1]["pages"] == 3


@pytest.mark.asyncio
async def test_event_bus_no_handlers_silent():
    bus = HarvestEventBus()
    # No handlers — should not raise
    await bus.emit("pack.promoted", {"pack_id": "p1"})


@pytest.mark.asyncio
async def test_event_bus_fan_out_multiple_handlers():
    bus = HarvestEventBus()
    counts = [0, 0]

    async def h1(_event, _data):
        counts[0] += 1

    async def h2(_event, _data):
        counts[1] += 1

    bus.on("ingest.completed", h1)
    bus.on("ingest.completed", h2)
    await bus.emit("ingest.completed", {})
    assert counts == [1, 1]


@pytest.mark.asyncio
async def test_event_bus_unsubscribe():
    bus = HarvestEventBus()
    calls = []

    async def handler(event, data):
        calls.append(event)

    bus.on("crawl.started", handler)
    bus.off("crawl.started", handler)
    await bus.emit("crawl.started", {})
    assert calls == []


@pytest.mark.asyncio
async def test_event_bus_handler_failure_isolated():
    bus = HarvestEventBus()
    good_calls = []

    async def bad_handler(_event, _data):
        raise RuntimeError("handler exploded")

    async def good_handler(event, _data):
        good_calls.append(event)

    bus.on("eval.completed", bad_handler)
    bus.on("eval.completed", good_handler)
    # Should not raise — fail-open
    await bus.emit("eval.completed", {"score": 0.9})
    assert good_calls == ["eval.completed"]


@pytest.mark.asyncio
async def test_event_bus_sync_handler_works():
    bus = HarvestEventBus()
    received = []

    def sync_handler(event, _data):
        received.append(event)

    bus.on("pack.rejected", sync_handler)
    await bus.emit("pack.rejected", {"reason": "low score"})
    assert received == ["pack.rejected"]


def test_event_bus_handler_count():
    bus = HarvestEventBus()
    bus.on("crawl.started", lambda e, d: None)
    bus.on("crawl.started", lambda e, d: None)
    bus.on("pack.promoted", lambda e, d: None)
    assert bus.handler_count("crawl.started") == 2
    assert bus.handler_count() == 3


def test_harvest_events_contains_expected():
    assert "crawl.started" in HARVEST_EVENTS
    assert "crawl.completed" in HARVEST_EVENTS
    assert "crawl.failed" in HARVEST_EVENTS
    assert "pack.promoted" in HARVEST_EVENTS
    assert "pack.rejected" in HARVEST_EVENTS
    assert "ingest.completed" in HARVEST_EVENTS
    assert "ingest.failed" in HARVEST_EVENTS


# ---------------------------------------------------------------------------
# WebhookSink HMAC
# ---------------------------------------------------------------------------

def test_webhook_sink_sign():
    sink = WebhookSink(url="https://hooks.example.com", secret="mysecret")
    payload = b'{"event": "test"}'
    sig = sink.sign(payload)
    expected = hmac.new(b"mysecret", payload, hashlib.sha256).hexdigest()
    assert sig == expected


def test_webhook_sink_verify_valid():
    sink = WebhookSink(url="https://hooks.example.com", secret="mysecret")
    payload = b'{"event": "test"}'
    sig = f"sha256={sink.sign(payload)}"
    assert sink.verify(payload, sig) is True


def test_webhook_sink_verify_invalid():
    sink = WebhookSink(url="https://hooks.example.com", secret="mysecret")
    payload = b'{"event": "test"}'
    assert sink.verify(payload, "sha256=wrongsignature") is False


def test_webhook_sink_no_secret_verify_always_true():
    sink = WebhookSink(url="https://hooks.example.com")
    assert sink.verify(b"anything", "anything") is True


@pytest.mark.asyncio
async def test_webhook_sink_posts_with_hmac():
    sink = WebhookSink(url="https://hooks.example.com/h", secret="sec")
    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_urlopen.return_value.__enter__ = MagicMock(return_value=mock_resp)
        mock_urlopen.return_value.__exit__ = MagicMock(return_value=False)
        await sink("crawl.completed", {"pages": 5})
        assert mock_urlopen.called
        req = mock_urlopen.call_args[0][0]
        # urllib.request.Request lowercases header names
        assert any("harvest-signature" in k.lower() for k in req.headers)


@pytest.mark.asyncio
async def test_webhook_sink_failure_does_not_raise():
    sink = WebhookSink(url="https://hooks.example.com/bad")
    with patch("urllib.request.urlopen", side_effect=ConnectionRefusedError("refused")):
        # Should not raise — fail-open
        await sink("crawl.failed", {"error": "timeout"})


# ---------------------------------------------------------------------------
# EmailSink
# ---------------------------------------------------------------------------

def test_email_sink_importable():
    sink = EmailSink(smtp_host="localhost", to="ops@example.com")
    assert sink.smtp_host == "localhost"


@pytest.mark.asyncio
async def test_email_sink_filters_events():
    sink = EmailSink(
        smtp_host="localhost",
        to="ops@example.com",
        events=frozenset({"crawl.failed"}),
    )
    sent = []
    with patch.object(sink, "_send", side_effect=lambda m: sent.append(m)):
        await sink("crawl.completed", {})  # not in filter — should not send
        await sink("crawl.failed", {"error": "net timeout"})  # in filter — should send
    assert len(sent) == 1


@pytest.mark.asyncio
async def test_email_sink_failure_does_not_raise():
    sink = EmailSink(smtp_host="localhost", to="ops@example.com")
    with patch.object(sink, "_send", side_effect=ConnectionRefusedError("refused")):
        # Should not raise — fail-open
        await sink("ingest.failed", {})
