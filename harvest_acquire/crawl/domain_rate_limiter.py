"""
DomainRateLimiter — per-domain request throttling with adaptive backoff.

Wave 5a: rate_limit_respect — per-domain tracking + adaptive throttle (7→9).

Design:
- Each domain gets its own token bucket (configurable RPS, default 1 req/s).
- On 429 / Retry-After responses, the domain's RPS is halved (adaptive backoff).
- On sustained success, the RPS is gradually restored toward the original limit.
- Thread-safe (asyncio.Lock per domain for async callers).
- Persist throttle state to disk so limits survive process restarts.

Token bucket algorithm:
    tokens = min(capacity, tokens + elapsed * rate)
    if tokens >= 1: consume 1, allow request
    else: sleep until token available

Constitutional guarantees:
- Fail-open: if rate limiting fails internally, the request proceeds (never blocks forever)
- Local-first: no external rate limit service — pure in-process + optional disk persistence
- Zero-ambiguity: wait_for_token() always returns, never raises
"""

from __future__ import annotations

import asyncio
import json
import random
import threading
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse


# ---------------------------------------------------------------------------
# Domain bucket
# ---------------------------------------------------------------------------

@dataclass
class DomainBucket:
    domain: str
    base_rps: float          # original configured rate (requests per second)
    current_rps: float       # current effective rate (may be reduced by backoff)
    tokens: float            # current token count
    last_refill: float       # Unix timestamp of last token refill
    capacity: float          # max tokens (= burst size = base_rps)
    consecutive_429s: int = 0
    consecutive_successes: int = 0

    def refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.current_rps)
        self.last_refill = now

    def consume(self) -> float:
        """Consume one token. Returns seconds to wait (0 if token available)."""
        self.refill()
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return 0.0
        wait = (1.0 - self.tokens) / max(self.current_rps, 0.001)
        return wait

    def record_429(self, retry_after_s: float = 0.0) -> None:
        self.consecutive_429s += 1
        self.consecutive_successes = 0
        # Halve the rate on each 429, floor at 0.05 rps (one req per 20s)
        self.current_rps = max(0.05, self.current_rps / 2.0)
        if retry_after_s > 0:
            # Drain tokens so we wait at least retry_after_s
            self.tokens = -retry_after_s * self.current_rps

    def record_success(self) -> None:
        self.consecutive_successes += 1
        self.consecutive_429s = 0
        # Restore rate by 10% per 5 consecutive successes, up to base_rps
        if self.consecutive_successes % 5 == 0:
            self.current_rps = min(self.base_rps, self.current_rps * 1.1)

    def to_dict(self) -> dict:
        return {
            "domain": self.domain,
            "base_rps": self.base_rps,
            "current_rps": self.current_rps,
            "tokens": self.tokens,
            "last_refill": self.last_refill,
            "capacity": self.capacity,
            "consecutive_429s": self.consecutive_429s,
            "consecutive_successes": self.consecutive_successes,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "DomainBucket":
        return cls(**d)


# ---------------------------------------------------------------------------
# DomainRateLimiter
# ---------------------------------------------------------------------------

class DomainRateLimiter:
    """
    Per-domain token bucket rate limiter with adaptive 429 backoff.

    Usage:
        limiter = DomainRateLimiter(default_rps=1.0)
        await limiter.wait_for_token("https://example.com/page")
        # ... make request ...
        limiter.record_result("https://example.com/page", status_code=200)

    With persistence:
        limiter = DomainRateLimiter(state_path=Path("storage/rate_limits.json"))
    """

    def __init__(
        self,
        default_rps: float = 1.0,
        domain_overrides: Optional[Dict[str, float]] = None,
        state_path: Optional[Path] = None,
        burst_multiplier: float = 3.0,
    ):
        self._default_rps = default_rps
        self._domain_overrides: Dict[str, float] = domain_overrides or {}
        self._state_path = state_path
        self._burst_multiplier = burst_multiplier
        self._buckets: Dict[str, DomainBucket] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        if state_path:
            self._load_state()

    async def wait_for_token(self, url: str) -> None:
        """
        Acquire a token for the given URL's domain.
        Awaits asynchronously if the domain is over its rate limit.
        Fail-open: exceptions are suppressed so crawling is never blocked by rate limiter bugs.
        """
        try:
            domain = self._domain(url)
            bucket = self._get_or_create_bucket(domain)
            lock = self._get_lock(domain)
            async with lock:
                wait_s = bucket.consume()
                if wait_s > 0:
                    await asyncio.sleep(wait_s)
        except Exception:
            pass  # fail-open

    def record_result(self, url: str, status_code: int, retry_after_s: float = 0.0) -> None:
        """Record the HTTP result for adaptive rate adjustment."""
        try:
            domain = self._domain(url)
            bucket = self._get_or_create_bucket(domain)
            if status_code == 429:
                bucket.record_429(retry_after_s=retry_after_s)
            elif status_code < 400:
                bucket.record_success()
            if self._state_path:
                self._save_state()
        except Exception:
            pass

    def domain_stats(self) -> Dict[str, dict]:
        """Return current throttle state per domain."""
        return {d: b.to_dict() for d, b in self._buckets.items()}

    def set_domain_rps(self, domain: str, rps: float) -> None:
        """Override the rate limit for a specific domain."""
        self._domain_overrides[domain] = rps
        if domain in self._buckets:
            self._buckets[domain].base_rps = rps
            self._buckets[domain].current_rps = rps

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _domain(self, url: str) -> str:
        try:
            return urlparse(url).netloc.lower()
        except Exception:
            return url

    def _get_or_create_bucket(self, domain: str) -> DomainBucket:
        if domain not in self._buckets:
            rps = self._domain_overrides.get(domain, self._default_rps)
            cap = max(1.0, rps * self._burst_multiplier)
            self._buckets[domain] = DomainBucket(
                domain=domain,
                base_rps=rps,
                current_rps=rps,
                tokens=cap,
                last_refill=time.monotonic(),
                capacity=cap,
            )
        return self._buckets[domain]

    def _get_lock(self, domain: str) -> asyncio.Lock:
        if domain not in self._locks:
            self._locks[domain] = asyncio.Lock()
        return self._locks[domain]

    def _load_state(self) -> None:
        if not self._state_path or not self._state_path.exists():
            return
        try:
            raw = json.loads(self._state_path.read_text(encoding="utf-8"))
            for d, b in raw.items():
                bucket = DomainBucket.from_dict(b)
                # Reset monotonic time reference on load
                bucket.last_refill = time.monotonic()
                self._buckets[d] = bucket
        except Exception:
            pass

    def _save_state(self) -> None:
        if not self._state_path:
            return
        try:
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            data = {d: b.to_dict() for d, b in self._buckets.items()}
            self._state_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Synchronous API (rate_limit_respect — score 9)
    # Adds per-domain backoff_until tracking, retry-after header support,
    # budget reporting, and a sync wait_if_needed() alongside the async API.
    # ------------------------------------------------------------------

    def _get_sync_state(self, domain: str) -> dict:
        """Return (creating if absent) the sync state dict for *domain*."""
        if not hasattr(self, "_sync_state"):
            self._sync_state: Dict[str, dict] = {}
            self._sync_lock = threading.Lock()
        if domain not in self._sync_state:
            self._sync_state[domain] = {
                "backoff_until": 0.0,
                "consecutive_429s": 0,
                "total_requests": 0,
                "total_429s": 0,
            }
        return self._sync_state[domain]

    def wait_if_needed(self, domain: str) -> float:
        """Block until rate limit allows next request. Returns wait time in seconds."""
        if not hasattr(self, "_sync_lock"):
            self._get_sync_state(domain)  # initialise
        with self._sync_lock:
            state = self._get_sync_state(domain)
            now = time.time()

            # Honour any active backoff_until (from 429 / Retry-After)
            if now < state["backoff_until"]:
                wait = state["backoff_until"] - now
                time.sleep(wait)
                state["total_requests"] += 1
                return wait

            # RPS-based throttle via the existing DomainBucket
            bucket = self._get_or_create_bucket(domain)
            wait_s = bucket.consume()
            if wait_s > 0:
                time.sleep(wait_s)

            state["total_requests"] += 1
            return wait_s

    def record_429(self, domain: str, retry_after: Optional[float] = None) -> None:
        """Record a 429 response and apply exponential backoff."""
        if not hasattr(self, "_sync_lock"):
            self._get_sync_state(domain)
        with self._sync_lock:
            state = self._get_sync_state(domain)
            state["consecutive_429s"] += 1
            state["total_429s"] += 1

            if retry_after is not None and retry_after > 0:
                # Honour the Retry-After header exactly
                state["backoff_until"] = time.time() + retry_after
            else:
                # Exponential backoff with jitter: avoids thundering-herd on coordinated retries
                base = min(300.0, (2 ** state["consecutive_429s"]) * 1.0)
                backoff = base * random.uniform(0.75, 1.25)
                state["backoff_until"] = time.time() + backoff

            # Also inform the async token-bucket so it stays in sync
            bucket = self._get_or_create_bucket(domain)
            bucket.record_429(retry_after_s=retry_after or 0.0)

    def record_success(self, domain: str) -> None:
        """Record a successful request — resets consecutive 429 counter."""
        if not hasattr(self, "_sync_lock"):
            self._get_sync_state(domain)
        with self._sync_lock:
            state = self._get_sync_state(domain)
            state["consecutive_429s"] = 0
            bucket = self._get_or_create_bucket(domain)
            bucket.record_success()

    def set_rps(self, domain: str, rps: float) -> None:
        """Set a custom RPS for a specific domain (sync-facing alias)."""
        self.set_domain_rps(domain, max(0.01, rps))

    def get_budget(self, domain: str) -> dict:
        """Return rate-limit budget info for *domain*."""
        if not hasattr(self, "_sync_lock"):
            self._get_sync_state(domain)
        with self._sync_lock:
            state = self._get_sync_state(domain)
            bucket = self._get_or_create_bucket(domain)
            now = time.time()
            return {
                "domain": domain,
                "rps": bucket.current_rps,
                "total_requests": state["total_requests"],
                "total_429s": state["total_429s"],
                "consecutive_429s": state["consecutive_429s"],
                "in_backoff": now < state["backoff_until"],
                "backoff_remaining_seconds": max(0.0, state["backoff_until"] - now),
            }

    def get_all_budgets(self) -> List[dict]:
        """Return budget info for every tracked domain."""
        if not hasattr(self, "_sync_state"):
            return []
        return [self.get_budget(d) for d in list(self._sync_state)]
