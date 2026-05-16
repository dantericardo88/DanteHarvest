"""
Unit tests for DomainRateLimiter synchronous API.

Covers: rate_limit_respect dimension (score 9)
- Per-domain backoff on 429
- Retry-After header honoring
- Success resets consecutive counter
- get_budget() returns required keys
- set_rps() changes domain RPS
"""
import time
import pytest

from harvest_acquire.crawl.domain_rate_limiter import DomainRateLimiter


@pytest.fixture()
def limiter():
    return DomainRateLimiter(default_rps=10.0)  # fast RPS so wait_if_needed is near-instant


class TestRecord429:
    def test_sets_backoff_until_in_future(self, limiter):
        limiter.record_429("example.com")
        budget = limiter.get_budget("example.com")
        assert budget["in_backoff"] is True
        assert budget["backoff_remaining_seconds"] > 0

    def test_exponential_backoff_grows_with_consecutive_429s(self, limiter):
        limiter.record_429("slow.com")
        first = limiter.get_budget("slow.com")["backoff_remaining_seconds"]
        # Force the state to increment without sleeping
        limiter._get_sync_state("slow.com")["consecutive_429s"] += 1
        limiter._get_sync_state("slow.com")["backoff_until"] = 0.0  # reset so next call recalculates
        limiter.record_429("slow.com")
        second = limiter.get_budget("slow.com")["backoff_remaining_seconds"]
        assert second > first  # each 429 doubles the backoff

    def test_retry_after_sets_exact_backoff(self, limiter):
        limiter.record_429("api.com", retry_after=60.0)
        budget = limiter.get_budget("api.com")
        # Should be close to 60s (within 1s of processing time)
        assert 58 < budget["backoff_remaining_seconds"] <= 60

    def test_increments_total_429s(self, limiter):
        limiter.record_429("cnt.com")
        limiter.record_429("cnt.com")
        budget = limiter.get_budget("cnt.com")
        assert budget["total_429s"] == 2

    def test_increments_consecutive_429s(self, limiter):
        limiter.record_429("seq.com")
        limiter.record_429("seq.com")
        budget = limiter.get_budget("seq.com")
        assert budget["consecutive_429s"] == 2


class TestRecordSuccess:
    def test_resets_consecutive_429s(self, limiter):
        limiter.record_429("ok.com")
        limiter.record_429("ok.com")
        limiter.record_success("ok.com")
        budget = limiter.get_budget("ok.com")
        assert budget["consecutive_429s"] == 0

    def test_does_not_clear_total_429s(self, limiter):
        limiter.record_429("hist.com")
        limiter.record_success("hist.com")
        budget = limiter.get_budget("hist.com")
        assert budget["total_429s"] == 1  # history preserved

    def test_success_on_fresh_domain_does_not_error(self, limiter):
        # Should not raise even if domain has never been seen
        limiter.record_success("new.com")
        budget = limiter.get_budget("new.com")
        assert budget["consecutive_429s"] == 0


class TestGetBudget:
    def test_returns_required_keys(self, limiter):
        limiter.get_budget("check.com")  # ensure domain is created
        budget = limiter.get_budget("check.com")
        for key in ("domain", "rps", "total_requests", "total_429s",
                    "consecutive_429s", "in_backoff", "backoff_remaining_seconds"):
            assert key in budget, f"Missing key: {key}"

    def test_domain_matches_requested(self, limiter):
        budget = limiter.get_budget("match.com")
        assert budget["domain"] == "match.com"

    def test_fresh_domain_not_in_backoff(self, limiter):
        budget = limiter.get_budget("fresh.com")
        assert budget["in_backoff"] is False
        assert budget["backoff_remaining_seconds"] == 0.0


class TestSetRps:
    def test_changes_domain_rps(self, limiter):
        limiter.set_rps("rate.com", 5.0)
        budget = limiter.get_budget("rate.com")
        assert budget["rps"] == 5.0

    def test_clamps_to_minimum(self, limiter):
        limiter.set_rps("tiny.com", 0.0)
        budget = limiter.get_budget("tiny.com")
        assert budget["rps"] >= 0.01  # never below minimum


class TestGetAllBudgets:
    def test_returns_list(self, limiter):
        limiter.record_429("a.com")
        limiter.record_429("b.com")
        budgets = limiter.get_all_budgets()
        assert isinstance(budgets, list)
        assert len(budgets) >= 2

    def test_empty_before_any_sync_calls(self):
        fresh = DomainRateLimiter()
        assert fresh.get_all_budgets() == []


class TestWaitIfNeeded:
    def test_returns_float(self, limiter):
        result = limiter.wait_if_needed("fast.com")
        assert isinstance(result, float)

    def test_increments_total_requests(self, limiter):
        limiter.wait_if_needed("req.com")
        limiter.wait_if_needed("req.com")
        budget = limiter.get_budget("req.com")
        assert budget["total_requests"] >= 2
