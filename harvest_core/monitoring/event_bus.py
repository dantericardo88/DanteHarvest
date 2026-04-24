"""
HarvestEventBus — async fan-out event system with webhook and email sinks.

Harvested from: Crawlee EventManager pattern (Apache-2.0).
Translated to Python; extended with HMAC-signed webhook sink.

Constitutional guarantees:
- Local-first: no external message broker required
- Fail-open: sink failures are logged, never re-raised (diagnostics must not kill production)
- Zero-ambiguity: emit() returns after all handlers complete (or timeout)
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import smtplib
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from email.mime.text import MIMEText
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

HARVEST_EVENTS = frozenset({
    "crawl.started",
    "crawl.completed",
    "crawl.failed",
    "eval.completed",
    "pack.promoted",
    "pack.rejected",
    "ingest.completed",
    "ingest.failed",
    "replay.completed",
    "replay.failed",
    "schedule.triggered",
    "schedule.failed",
})

Handler = Callable[[str, Dict[str, Any]], Any]


# ---------------------------------------------------------------------------
# AlertRule — threshold-based alerting with dedup window
# ---------------------------------------------------------------------------

@dataclass
class AlertRule:
    """
    Fire an alert handler when an event fires >= threshold times within window_secs.

    Example — alert after 3 crawl failures in 60 seconds:
        rule = AlertRule(
            event="crawl.failed",
            threshold=3,
            window_secs=60,
            handler=my_alert_fn,
            name="high-crawl-failure-rate",
        )
        bus.add_rule(rule)
    """
    event: str
    threshold: int
    window_secs: float
    handler: Handler
    name: str = ""
    # Internal state — do not set manually
    _timestamps: Deque[float] = field(default_factory=deque, repr=False)
    _last_fired: float = field(default=0.0, repr=False)
    dedup_cooldown_secs: float = 60.0  # min seconds between repeated alerts

    def record(self) -> bool:
        """Record an event occurrence. Returns True if threshold is now breached."""
        now = time.time()
        self._timestamps.append(now)
        # Evict timestamps outside the window
        cutoff = now - self.window_secs
        while self._timestamps and self._timestamps[0] < cutoff:
            self._timestamps.popleft()
        if len(self._timestamps) >= self.threshold:
            if now - self._last_fired >= self.dedup_cooldown_secs:
                self._last_fired = now
                return True
        return False

    def count_in_window(self) -> int:
        now = time.time()
        cutoff = now - self.window_secs
        return sum(1 for t in self._timestamps if t >= cutoff)


class HarvestEventBus:
    """
    Async event bus with subscribe/unsubscribe/emit, deduplication, and AlertRules.

    Usage:
        bus = HarvestEventBus()
        bus.on("crawl.completed", my_handler)
        await bus.emit("crawl.completed", {"url": "https://x.com", "pages": 5})

    Handler signature:
        async def my_handler(event: str, data: dict) -> None: ...

    Dedup usage (suppress duplicate events within a window):
        bus = HarvestEventBus(dedup_window_secs=5.0)

    AlertRule usage:
        rule = AlertRule(event="crawl.failed", threshold=3, window_secs=60, handler=my_fn)
        bus.add_rule(rule)
    """

    def __init__(self, handler_timeout_secs: float = 10.0, dedup_window_secs: float = 0.0):
        self._handlers: Dict[str, List[Handler]] = defaultdict(list)
        self._timeout = handler_timeout_secs
        self._dedup_window = dedup_window_secs
        self._dedup_cache: Dict[str, float] = {}  # fingerprint → last_seen
        self._rules: List[AlertRule] = []
        self._dead_letters: Deque[Tuple[str, Dict[str, Any], str]] = deque(maxlen=200)

    def on(self, event: str, handler: Handler) -> None:
        self._handlers[event].append(handler)

    def off(self, event: str, handler: Handler) -> None:
        handlers = self._handlers.get(event, [])
        if handler in handlers:
            handlers.remove(handler)

    async def emit(self, event: str, data: Dict[str, Any]) -> None:
        # Dedup check — suppress events with same fingerprint within the window
        if self._dedup_window > 0:
            fingerprint = f"{event}:{hash(json.dumps(data, sort_keys=True, default=str))}"
            now = time.time()
            last = self._dedup_cache.get(fingerprint, 0.0)
            if now - last < self._dedup_window:
                return
            self._dedup_cache[fingerprint] = now
            # Evict stale entries to prevent unbounded growth
            if len(self._dedup_cache) > 10_000:
                cutoff = now - self._dedup_window * 2
                self._dedup_cache = {k: v for k, v in self._dedup_cache.items() if v > cutoff}

        # Alert rule evaluation
        for rule in self._rules:
            if rule.event == event and rule.record():
                asyncio.create_task(
                    self._call_handler(rule.handler, event, {"event": event, "data": data, "rule": rule.name})
                )

        handlers = list(self._handlers.get(event, []))
        if not handlers:
            return
        envelope = {"event": event, "timestamp": time.time(), "data": data}
        tasks = [asyncio.create_task(self._call_handler(h, event, envelope)) for h in handlers]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _call_handler(self, handler: Handler, event: str, envelope: Dict[str, Any]) -> None:
        try:
            result = handler(event, envelope["data"])
            if asyncio.iscoroutine(result):
                await asyncio.wait_for(result, timeout=self._timeout)
        except asyncio.TimeoutError:
            logger.warning("EventBus: handler %s timed out on '%s'", handler, event)
            self._dead_letters.append((event, envelope.get("data", {}), "timeout"))
        except Exception as e:
            logger.warning("EventBus: handler %s raised on '%s': %s", handler, event, e)
            self._dead_letters.append((event, envelope.get("data", {}), str(e)))

    def handler_count(self, event: Optional[str] = None) -> int:
        if event:
            return len(self._handlers.get(event, []))
        return sum(len(v) for v in self._handlers.values())

    def add_rule(self, rule: AlertRule) -> None:
        self._rules.append(rule)

    def remove_rule(self, name: str) -> bool:
        before = len(self._rules)
        self._rules = [r for r in self._rules if r.name != name]
        return len(self._rules) < before

    def list_rules(self) -> List[AlertRule]:
        return list(self._rules)

    def dead_letters(self) -> List[Tuple[str, Dict[str, Any], str]]:
        return list(self._dead_letters)


# ---------------------------------------------------------------------------
# Webhook sink — HMAC-signed HTTP POST (Crawlee EventManager pattern)
# ---------------------------------------------------------------------------

class WebhookSink:
    """
    POST event data to a webhook URL with optional HMAC-SHA256 signature.

    Signature header: X-Harvest-Signature: sha256=<hex>
    Secret is shared between sender and receiver for verification.

    Usage:
        sink = WebhookSink(url="https://hooks.example.com/harvest", secret="s3cr3t")
        bus.on("crawl.completed", sink)
    """

    def __init__(self, url: str, secret: Optional[str] = None, timeout: int = 10):
        self.url = url
        self._secret = secret
        self._timeout = timeout

    async def __call__(self, event: str, data: Dict[str, Any]) -> None:
        payload = json.dumps({"event": event, "timestamp": time.time(), "data": data}).encode()
        headers = {"Content-Type": "application/json"}
        if self._secret:
            sig = hmac.new(self._secret.encode(), payload, hashlib.sha256).hexdigest()
            headers["X-Harvest-Signature"] = f"sha256={sig}"
        try:
            import urllib.request
            req = urllib.request.Request(self.url, data=payload, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                if resp.status >= 400:
                    logger.warning("WebhookSink: %s returned %d", self.url, resp.status)
        except Exception as e:
            logger.warning("WebhookSink: POST to %s failed: %s", self.url, e)

    def sign(self, payload: bytes) -> str:
        """Compute HMAC signature for the given payload."""
        if not self._secret:
            return ""
        return hmac.new(self._secret.encode(), payload, hashlib.sha256).hexdigest()

    def verify(self, payload: bytes, signature: str) -> bool:
        """Verify an inbound HMAC signature."""
        if not self._secret:
            return True
        expected = self.sign(payload)
        return hmac.compare_digest(f"sha256={expected}", signature)


# ---------------------------------------------------------------------------
# Email sink — SMTP (stdlib smtplib, zero extra deps)
# ---------------------------------------------------------------------------

class EmailSink:
    """
    Send event notifications via SMTP.

    Usage:
        sink = EmailSink(smtp_host="smtp.example.com", to="ops@example.com",
                         from_="harvest@example.com", events={"crawl.failed"})
        bus.on("crawl.failed", sink)
    """

    def __init__(
        self,
        smtp_host: str,
        to: str,
        from_: str = "harvest@localhost",
        smtp_port: int = 25,
        events: Optional[frozenset] = None,
    ):
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.to = to
        self.from_ = from_
        self.events = events  # if set, only send for these events

    async def __call__(self, event: str, data: Dict[str, Any]) -> None:
        if self.events and event not in self.events:
            return
        subject = f"[Harvest] {event}"
        body = json.dumps({"event": event, "data": data}, indent=2)
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = self.from_
        msg["To"] = self.to
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._send, msg.as_string())
        except Exception as e:
            logger.warning("EmailSink: failed to send for event '%s': %s", event, e)

    def _send(self, msg_str: str) -> None:
        with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=10) as s:
            s.sendmail(self.from_, [self.to], msg_str)
