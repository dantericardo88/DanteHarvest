"""Tests for harvest_acquire.crawl.domain_rate_limiter."""
import asyncio
import pytest
import time


def test_domain_bucket_consume_returns_zero_when_tokens_available():
    from harvest_acquire.crawl.domain_rate_limiter import DomainBucket
    bucket = DomainBucket(
        domain="example.com",
        base_rps=10.0,
        current_rps=10.0,
        tokens=5.0,
        last_refill=time.monotonic(),
        capacity=10.0,
    )
    wait = bucket.consume()
    assert wait == 0.0
    assert bucket.tokens == 4.0


def test_domain_bucket_consume_returns_wait_when_empty():
    from harvest_acquire.crawl.domain_rate_limiter import DomainBucket
    bucket = DomainBucket(
        domain="example.com",
        base_rps=1.0,
        current_rps=1.0,
        tokens=0.0,
        last_refill=time.monotonic(),
        capacity=1.0,
    )
    wait = bucket.consume()
    assert wait > 0


def test_domain_bucket_refill_adds_tokens():
    from harvest_acquire.crawl.domain_rate_limiter import DomainBucket
    bucket = DomainBucket(
        domain="x.com",
        base_rps=10.0,
        current_rps=10.0,
        tokens=0.0,
        last_refill=time.monotonic() - 1.0,  # 1 second ago
        capacity=10.0,
    )
    bucket.refill()
    assert bucket.tokens > 0


def test_domain_bucket_record_429_halves_rps():
    from harvest_acquire.crawl.domain_rate_limiter import DomainBucket
    bucket = DomainBucket(
        domain="x.com",
        base_rps=4.0,
        current_rps=4.0,
        tokens=4.0,
        last_refill=time.monotonic(),
        capacity=4.0,
    )
    bucket.record_429()
    assert bucket.current_rps == pytest.approx(2.0)
    assert bucket.consecutive_429s == 1


def test_domain_bucket_record_429_floor_at_005():
    from harvest_acquire.crawl.domain_rate_limiter import DomainBucket
    bucket = DomainBucket(
        domain="x.com", base_rps=0.1, current_rps=0.1,
        tokens=0.0, last_refill=time.monotonic(), capacity=0.1,
    )
    for _ in range(10):
        bucket.record_429()
    assert bucket.current_rps >= 0.05


def test_domain_bucket_record_success_restores_rate():
    from harvest_acquire.crawl.domain_rate_limiter import DomainBucket
    bucket = DomainBucket(
        domain="x.com", base_rps=4.0, current_rps=1.0,
        tokens=1.0, last_refill=time.monotonic(), capacity=4.0,
    )
    for _ in range(5):
        bucket.record_success()
    assert bucket.current_rps > 1.0


def test_rate_limiter_creates_bucket_on_demand():
    from harvest_acquire.crawl.domain_rate_limiter import DomainRateLimiter
    limiter = DomainRateLimiter(default_rps=5.0)
    bucket = limiter._get_or_create_bucket("test.com")
    assert bucket.domain == "test.com"
    assert bucket.base_rps == 5.0


def test_rate_limiter_domain_overrides():
    from harvest_acquire.crawl.domain_rate_limiter import DomainRateLimiter
    limiter = DomainRateLimiter(default_rps=1.0, domain_overrides={"fast.com": 10.0})
    bucket = limiter._get_or_create_bucket("fast.com")
    assert bucket.base_rps == 10.0


@pytest.mark.asyncio
async def test_rate_limiter_wait_for_token_no_wait_full_bucket():
    from harvest_acquire.crawl.domain_rate_limiter import DomainRateLimiter
    limiter = DomainRateLimiter(default_rps=100.0)
    t0 = time.monotonic()
    await limiter.wait_for_token("https://example.com/page")
    elapsed = time.monotonic() - t0
    assert elapsed < 0.5  # should not wait when bucket is full


def test_rate_limiter_record_result_success():
    from harvest_acquire.crawl.domain_rate_limiter import DomainRateLimiter
    limiter = DomainRateLimiter(default_rps=1.0)
    limiter._get_or_create_bucket("ok.com")
    limiter.record_result("https://ok.com/page", status_code=200)
    bucket = limiter._buckets["ok.com"]
    assert bucket.consecutive_successes == 1


def test_rate_limiter_record_result_429():
    from harvest_acquire.crawl.domain_rate_limiter import DomainRateLimiter
    limiter = DomainRateLimiter(default_rps=2.0)
    limiter._get_or_create_bucket("slow.com")
    limiter.record_result("https://slow.com/", status_code=429)
    bucket = limiter._buckets["slow.com"]
    assert bucket.consecutive_429s == 1
    assert bucket.current_rps < 2.0


def test_rate_limiter_domain_stats():
    from harvest_acquire.crawl.domain_rate_limiter import DomainRateLimiter
    limiter = DomainRateLimiter(default_rps=1.0)
    limiter._get_or_create_bucket("stats.com")
    stats = limiter.domain_stats()
    assert "stats.com" in stats


def test_rate_limiter_set_domain_rps():
    from harvest_acquire.crawl.domain_rate_limiter import DomainRateLimiter
    limiter = DomainRateLimiter(default_rps=1.0)
    limiter._get_or_create_bucket("update.com")
    limiter.set_domain_rps("update.com", 5.0)
    assert limiter._buckets["update.com"].base_rps == 5.0


def test_rate_limiter_persist_and_load(tmp_path):
    from harvest_acquire.crawl.domain_rate_limiter import DomainRateLimiter
    state_path = tmp_path / "rate_state.json"
    limiter = DomainRateLimiter(default_rps=2.0, state_path=state_path)
    limiter._get_or_create_bucket("persist.com")
    limiter._save_state()

    limiter2 = DomainRateLimiter(default_rps=2.0, state_path=state_path)
    assert "persist.com" in limiter2._buckets
