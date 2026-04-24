"""Tests for RetryPolicy — exponential backoff retry wrapper."""

import pytest
from harvest_acquire.urls.retry_policy import RetryPolicy
from harvest_core.control.exceptions import AcquisitionError
from harvest_core.provenance.chain_writer import ChainWriter


async def _succeed(**kwargs):
    return "ok"


async def _always_fail(**kwargs):
    raise AcquisitionError("permanent failure")


def make_fast_policy(max_retries: int = 3) -> RetryPolicy:
    return RetryPolicy(max_retries=max_retries).with_no_sleep()


@pytest.mark.asyncio
async def test_immediate_success():
    policy = make_fast_policy()
    result = await policy.execute(_succeed, run_id="r1")
    assert result == "ok"


@pytest.mark.asyncio
async def test_success_on_second_attempt():
    calls = []

    async def _fail_once(**kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise AcquisitionError("transient")
        return "recovered"

    policy = make_fast_policy(max_retries=2)
    result = await policy.execute(_fail_once, run_id="r1")
    assert result == "recovered"
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_exhaustion_raises_last_exception():
    policy = make_fast_policy(max_retries=2)
    with pytest.raises(AcquisitionError, match="permanent failure"):
        await policy.execute(_always_fail, run_id="r1")


@pytest.mark.asyncio
async def test_zero_retries_fails_immediately():
    calls = []

    async def _fail(**kwargs):
        calls.append(1)
        raise AcquisitionError("no retry")

    policy = make_fast_policy(max_retries=0)
    with pytest.raises(AcquisitionError):
        await policy.execute(_fail, run_id="r1")
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_chain_signal_emitted_on_retry(tmp_path):
    writer = ChainWriter(tmp_path / "chain.jsonl", "run-1")
    policy = RetryPolicy(max_retries=1, base_delay=0.001, jitter=0.0)
    with pytest.raises(AcquisitionError):
        await policy.execute(_always_fail, run_id="run-1", chain_writer=writer)
    entries = writer.read_all()
    retry_entries = [e for e in entries if e.signal == "acquire.retry"]
    assert len(retry_entries) == 1
    assert retry_entries[0].data["attempt"] == 1


@pytest.mark.asyncio
async def test_kwargs_passed_to_fn():
    received = {}

    async def _capture(url=None, timeout=None, **kwargs):
        received["url"] = url
        received["timeout"] = timeout
        return "done"

    policy = make_fast_policy()
    await policy.execute(_capture, run_id="r1", url="http://example.com", timeout=30)
    assert received["url"] == "http://example.com"
    assert received["timeout"] == 30
