"""
Unit tests for WebhookDispatcher.

Fully mocked — no real network calls.  CI-safe.
Uses pytest-asyncio (asyncio_mode=auto) for async tests.
Uses unittest.mock to patch aiohttp internals.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from harvest_distill.export.webhook_dispatcher import (
    DispatchResult,
    WebhookDispatcher,
)
from harvest_distill.packs.dante_agents_contract import HarvestHandoff


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def make_handoff(pack_id: str = "wf-001") -> HarvestHandoff:
    return HarvestHandoff(
        handoff_id=f"hh-{pack_id}",
        pack_id=pack_id,
        pack_type="workflowPack",
        domain="accounting",
        receipt_id="rcpt-123",
        confidence_score=0.98,
        exported_at="2026-01-01T00:00:00",
        pack_json={"title": "Invoice Workflow"},
        consumption_hints={"agent_role": "executor"},
    )


def make_dispatcher(**kwargs) -> WebhookDispatcher:
    defaults = {
        "url": "https://hooks.example.com/harvest",
        "secret": "test-secret",
        "max_retries": 3,
        "base_delay": 0.0,  # no delay in tests
        "timeout": 5.0,
    }
    defaults.update(kwargs)
    return WebhookDispatcher(**defaults)


def _mock_post_response(status: int):
    """Build a mock aiohttp response context manager returning the given status."""
    resp = MagicMock()
    resp.status = status
    resp.__aenter__ = AsyncMock(return_value=resp)
    resp.__aexit__ = AsyncMock(return_value=False)
    return resp


def _mock_session(status: int):
    """Build a mock aiohttp ClientSession whose post() returns a response with given status."""
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    session.post = MagicMock(return_value=_mock_post_response(status))
    return session


# ---------------------------------------------------------------------------
# Successful dispatch
# ---------------------------------------------------------------------------

class TestSuccessfulDispatch:
    async def test_returns_dispatch_result_on_200(self):
        dispatcher = make_dispatcher()
        handoff = make_handoff()

        with patch("harvest_distill.export.webhook_dispatcher.aiohttp") as mock_aiohttp:
            mock_aiohttp.ClientTimeout = MagicMock(return_value=None)
            mock_aiohttp.TCPConnector = MagicMock(return_value=None)
            mock_aiohttp.ClientSession = MagicMock(return_value=_mock_session(200))

            result = await dispatcher.dispatch(handoff)

        assert isinstance(result, DispatchResult)
        assert result.success is True
        assert result.status_code == 200
        assert result.attempts == 1
        assert result.handoff_id == handoff.handoff_id

    async def test_returns_success_on_201(self):
        dispatcher = make_dispatcher()
        handoff = make_handoff()

        with patch("harvest_distill.export.webhook_dispatcher.aiohttp") as mock_aiohttp:
            mock_aiohttp.ClientTimeout = MagicMock(return_value=None)
            mock_aiohttp.TCPConnector = MagicMock(return_value=None)
            mock_aiohttp.ClientSession = MagicMock(return_value=_mock_session(201))

            result = await dispatcher.dispatch(handoff)

        assert result.success is True
        assert result.status_code == 201

    async def test_dispatch_log_records_attempt(self):
        dispatcher = make_dispatcher()
        handoff = make_handoff()

        with patch("harvest_distill.export.webhook_dispatcher.aiohttp") as mock_aiohttp:
            mock_aiohttp.ClientTimeout = MagicMock(return_value=None)
            mock_aiohttp.TCPConnector = MagicMock(return_value=None)
            mock_aiohttp.ClientSession = MagicMock(return_value=_mock_session(200))
            await dispatcher.dispatch(handoff)

        assert len(dispatcher.dispatch_log) == 1
        log_entry = dispatcher.dispatch_log[0]
        assert log_entry["handoff_id"] == handoff.handoff_id
        assert log_entry["status_code"] == 200
        assert log_entry["attempt"] == 1

    async def test_duration_ms_is_float(self):
        dispatcher = make_dispatcher()
        handoff = make_handoff()

        with patch("harvest_distill.export.webhook_dispatcher.aiohttp") as mock_aiohttp:
            mock_aiohttp.ClientTimeout = MagicMock(return_value=None)
            mock_aiohttp.TCPConnector = MagicMock(return_value=None)
            mock_aiohttp.ClientSession = MagicMock(return_value=_mock_session(200))
            result = await dispatcher.dispatch(handoff)

        assert isinstance(result.duration_ms, float)
        assert result.duration_ms >= 0


# ---------------------------------------------------------------------------
# Retry behaviour — patching _post directly to avoid aiohttp complexity
# ---------------------------------------------------------------------------

class TestRetryBehaviour:
    async def test_retries_on_500(self):
        """Dispatcher should retry on 500 and succeed on 200."""
        dispatcher = make_dispatcher(max_retries=3)
        handoff = make_handoff()

        call_count = {"n": 0}
        statuses = [500, 500, 200]

        async def fake_post(headers, payload_bytes):
            idx = call_count["n"]
            call_count["n"] += 1
            return statuses[min(idx, len(statuses) - 1)], None

        dispatcher._post = fake_post
        result = await dispatcher.dispatch(handoff)
        assert result.success is True
        assert result.attempts == 3
        assert result.status_code == 200

    async def test_all_retries_exhausted_returns_failure(self):
        dispatcher = make_dispatcher(max_retries=3)
        handoff = make_handoff()

        async def always_500(headers, payload_bytes):
            return 500, None

        dispatcher._post = always_500
        result = await dispatcher.dispatch(handoff)
        assert result.success is False
        assert result.attempts == 3
        assert result.status_code == 500

    async def test_network_error_retried(self):
        dispatcher = make_dispatcher(max_retries=2)
        handoff = make_handoff()

        call_count = {"n": 0}

        async def flaky(headers, payload_bytes):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise ConnectionError("network down")
            return 200, None

        dispatcher._post = flaky
        result = await dispatcher.dispatch(handoff)
        assert result.success is True
        assert result.attempts == 2

    async def test_dispatch_log_records_all_attempts(self):
        dispatcher = make_dispatcher(max_retries=3)
        handoff = make_handoff()

        async def always_500(headers, payload_bytes):
            return 500, None

        dispatcher._post = always_500
        await dispatcher.dispatch(handoff)
        assert len(dispatcher.dispatch_log) == 3
        for i, entry in enumerate(dispatcher.dispatch_log, 1):
            assert entry["attempt"] == i

    async def test_single_retry_config(self):
        dispatcher = make_dispatcher(max_retries=1)
        handoff = make_handoff()

        async def always_fail(headers, payload_bytes):
            return 503, None

        dispatcher._post = always_fail
        result = await dispatcher.dispatch(handoff)
        assert result.success is False
        assert result.attempts == 1


# ---------------------------------------------------------------------------
# HMAC signing (sync — no async needed)
# ---------------------------------------------------------------------------

class TestHMACSignature:
    def test_signature_header_present_when_secret_set(self):
        dispatcher = make_dispatcher(secret="my-secret")
        payload = b'{"pack_id": "wf-001"}'
        headers = dispatcher._build_headers("hh-001", payload)
        assert WebhookDispatcher.SIGNATURE_HEADER in headers
        assert headers[WebhookDispatcher.SIGNATURE_HEADER].startswith("sha256=")

    def test_signature_header_absent_when_no_secret(self):
        dispatcher = make_dispatcher(secret=None)
        payload = b'{"pack_id": "wf-001"}'
        headers = dispatcher._build_headers("hh-001", payload)
        assert WebhookDispatcher.SIGNATURE_HEADER not in headers

    def test_signature_is_correct_hmac_sha256(self):
        secret = "test-secret"
        dispatcher = make_dispatcher(secret=secret)
        payload = b'{"pack_id": "wf-001"}'
        headers = dispatcher._build_headers("hh-001", payload)

        expected = hmac.new(
            secret.encode(), payload, hashlib.sha256
        ).hexdigest()
        assert headers[WebhookDispatcher.SIGNATURE_HEADER] == f"sha256={expected}"

    def test_idempotency_header_always_present(self):
        dispatcher = make_dispatcher()
        headers = dispatcher._build_headers("hh-xyz", b"payload")
        assert headers[WebhookDispatcher.IDEMPOTENCY_HEADER] == "hh-xyz"

    def test_content_type_is_json(self):
        dispatcher = make_dispatcher()
        headers = dispatcher._build_headers("hh-001", b"payload")
        assert headers["Content-Type"] == "application/json"


# ---------------------------------------------------------------------------
# verify_signature static helper (sync)
# ---------------------------------------------------------------------------

class TestVerifySignature:
    def test_valid_signature_returns_true(self):
        secret = "shared-secret"
        payload = b'{"data": "important"}'
        sig = "sha256=" + hmac.new(
            secret.encode(), payload, hashlib.sha256
        ).hexdigest()
        assert WebhookDispatcher.verify_signature(payload, secret, sig) is True

    def test_wrong_secret_returns_false(self):
        payload = b'{"data": "important"}'
        real_secret = "correct"
        wrong_secret = "wrong"
        sig = "sha256=" + hmac.new(
            real_secret.encode(), payload, hashlib.sha256
        ).hexdigest()
        assert WebhookDispatcher.verify_signature(payload, wrong_secret, sig) is False

    def test_tampered_payload_returns_false(self):
        secret = "shared-secret"
        payload = b'{"data": "important"}'
        sig = "sha256=" + hmac.new(
            secret.encode(), payload, hashlib.sha256
        ).hexdigest()
        tampered = b'{"data": "tampered"}'
        assert WebhookDispatcher.verify_signature(tampered, secret, sig) is False

    def test_missing_sha256_prefix_returns_false(self):
        payload = b"data"
        assert WebhookDispatcher.verify_signature(payload, "secret", "abc123") is False


# ---------------------------------------------------------------------------
# DispatchResult dataclass (sync)
# ---------------------------------------------------------------------------

class TestDispatchResult:
    def test_dispatch_result_fields(self):
        r = DispatchResult(
            handoff_id="hh-001",
            url="https://example.com",
            success=True,
            attempts=1,
            status_code=200,
            error=None,
            duration_ms=42.0,
        )
        assert r.handoff_id == "hh-001"
        assert r.success is True
        assert r.duration_ms == 42.0
