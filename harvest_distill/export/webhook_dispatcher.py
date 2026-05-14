"""
WebhookDispatcher — POST HarvestHandoff JSON to a configured webhook URL on pack promotion.

Uses aiohttp for async HTTP, exponential backoff for retries, and HMAC-SHA256
for request signing.

Constitutional guarantees:
- Fail-closed: a signing secret must be provided or HMAC header is omitted with explicit opt-out.
- Retry-safe: idempotency header (X-Handoff-ID) is set on every attempt.
- Audit-friendly: all attempt outcomes are logged to self.dispatch_log.

Usage:
    dispatcher = WebhookDispatcher(
        url="https://example.com/hooks/harvest",
        secret="my-shared-secret",
        max_retries=3,
    )
    result = await dispatcher.dispatch(handoff)
    print(result.status_code, result.attempts)
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

try:
    import aiohttp
    _AIOHTTP_AVAILABLE = True
except ImportError:
    _AIOHTTP_AVAILABLE = False

from harvest_distill.packs.dante_agents_contract import HarvestHandoff


@dataclass
class DispatchResult:
    """Result of a single webhook dispatch attempt sequence."""
    handoff_id: str
    url: str
    success: bool
    attempts: int
    status_code: Optional[int]
    error: Optional[str]
    duration_ms: float


@dataclass
class _AttemptRecord:
    attempt: int
    status_code: Optional[int]
    error: Optional[str]
    elapsed_ms: float


class WebhookDispatchError(Exception):
    """Raised when all retries are exhausted."""


class WebhookDispatcher:
    """
    POST HarvestHandoff JSON to a webhook URL with retry and HMAC signing.

    Args:
        url:         Target webhook URL.
        secret:      HMAC-SHA256 signing secret.  If None, no signature header is sent.
        max_retries: Maximum number of POST attempts (including first attempt).
        base_delay:  Base delay in seconds for exponential backoff (default 1.0).
        timeout:     Per-request timeout in seconds (default 10).
    """

    SIGNATURE_HEADER = "X-DanteHarvest-Signature"
    IDEMPOTENCY_HEADER = "X-Handoff-ID"

    def __init__(
        self,
        url: str,
        secret: Optional[str] = None,
        max_retries: int = 3,
        base_delay: float = 1.0,
        timeout: float = 10.0,
    ) -> None:
        self.url = url
        self.secret = secret
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.timeout = timeout
        self.dispatch_log: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def dispatch(self, handoff: HarvestHandoff) -> DispatchResult:
        """
        Dispatch a HarvestHandoff to the configured webhook URL.

        Retries up to max_retries times with exponential backoff on non-2xx responses
        or network errors.

        Returns:
            DispatchResult with final status.

        Raises:
            ImportError: if aiohttp is not installed.
        """
        if not _AIOHTTP_AVAILABLE:
            raise ImportError(
                "aiohttp is required for WebhookDispatcher. "
                "Run: pip install aiohttp"
            )

        payload = handoff.to_json(indent=None)
        payload_bytes = payload.encode("utf-8")
        headers = self._build_headers(handoff.handoff_id, payload_bytes)

        start = time.monotonic()
        attempts = 0
        last_status: Optional[int] = None
        last_error: Optional[str] = None

        for attempt in range(1, self.max_retries + 1):
            attempts = attempt
            attempt_start = time.monotonic()

            try:
                status, error = await self._post(headers, payload_bytes)
            except Exception as exc:
                status = None
                error = str(exc)

            elapsed_ms = (time.monotonic() - attempt_start) * 1000
            last_status = status
            last_error = error

            self.dispatch_log.append({
                "handoff_id": handoff.handoff_id,
                "attempt": attempt,
                "status_code": status,
                "error": error,
                "elapsed_ms": round(elapsed_ms, 2),
            })

            if status is not None and 200 <= status < 300:
                # Success
                total_ms = (time.monotonic() - start) * 1000
                return DispatchResult(
                    handoff_id=handoff.handoff_id,
                    url=self.url,
                    success=True,
                    attempts=attempts,
                    status_code=status,
                    error=None,
                    duration_ms=round(total_ms, 2),
                )

            # Exponential backoff before next retry
            if attempt < self.max_retries:
                delay = self.base_delay * (2 ** (attempt - 1))
                await asyncio.sleep(delay)

        total_ms = (time.monotonic() - start) * 1000
        return DispatchResult(
            handoff_id=handoff.handoff_id,
            url=self.url,
            success=False,
            attempts=attempts,
            status_code=last_status,
            error=last_error,
            duration_ms=round(total_ms, 2),
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _post(
        self,
        headers: Dict[str, str],
        payload_bytes: bytes,
    ) -> tuple[Optional[int], Optional[str]]:
        """Execute a single POST request. Returns (status_code, error_msg)."""
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.post(
                self.url,
                data=payload_bytes,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as resp:
                return resp.status, None

    def _build_headers(self, handoff_id: str, payload_bytes: bytes) -> Dict[str, str]:
        headers: Dict[str, str] = {
            "Content-Type": "application/json",
            self.IDEMPOTENCY_HEADER: handoff_id,
        }
        if self.secret:
            sig = hmac.new(
                self.secret.encode("utf-8"),
                payload_bytes,
                hashlib.sha256,
            ).hexdigest()
            headers[self.SIGNATURE_HEADER] = f"sha256={sig}"
        return headers

    # ------------------------------------------------------------------
    # Signature verification helper (for receivers / tests)
    # ------------------------------------------------------------------

    @staticmethod
    def verify_signature(
        payload_bytes: bytes,
        secret: str,
        signature_header: str,
    ) -> bool:
        """
        Verify an incoming HMAC-SHA256 signature.

        Args:
            payload_bytes:    Raw request body bytes.
            secret:           Shared secret.
            signature_header: Value of X-DanteHarvest-Signature header.

        Returns:
            True if the signature matches, False otherwise.
        """
        if not signature_header.startswith("sha256="):
            return False
        expected = hmac.new(
            secret.encode("utf-8"),
            payload_bytes,
            hashlib.sha256,
        ).hexdigest()
        received = signature_header.removeprefix("sha256=")
        return hmac.compare_digest(expected, received)
