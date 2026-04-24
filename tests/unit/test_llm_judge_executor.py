"""Unit tests for LLMJudgeExecutor — all LLM calls mocked."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from harvest_index.registry.llm_judge_executor import (
    LLMJudgeExecutor,
    StepJudgment,
    make_llm_judge,
)


# ---------------------------------------------------------------------------
# StepJudgment schema
# ---------------------------------------------------------------------------

def test_step_judgment_passed():
    j = StepJudgment(reasoning="Looks good.", passed=True, score=0.9)
    assert j.passed is True
    assert j.score == 0.9


def test_step_judgment_score_clamps():
    with pytest.raises(Exception):
        StepJudgment(reasoning="x", passed=True, score=1.5)


def test_step_judgment_score_zero():
    j = StepJudgment(reasoning="Nothing matched.", passed=False, score=0.0)
    assert j.passed is False


# ---------------------------------------------------------------------------
# No expected_outcome → structural pass
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_no_expected_outcome_passes_structurally():
    executor = LLMJudgeExecutor(fallback_pass_on_no_expected=True)
    step = MagicMock()
    step.expected_outcome = None
    result = await executor("navigate:https://example.com", "s1", context={"current_step": step})
    assert result["passed"] is True
    assert result["error"] is None


@pytest.mark.asyncio
async def test_no_expected_outcome_fails_when_fallback_disabled():
    executor = LLMJudgeExecutor(fallback_pass_on_no_expected=False)
    step = MagicMock()
    step.expected_outcome = None
    result = await executor("click:#btn", "s1", context={"current_step": step})
    assert result["passed"] is False


@pytest.mark.asyncio
async def test_no_step_in_context_passes_structurally():
    executor = LLMJudgeExecutor(fallback_pass_on_no_expected=True)
    result = await executor("click:#btn", "s1", context={})
    assert result["passed"] is True


# ---------------------------------------------------------------------------
# With expected_outcome → LLM judgment
# ---------------------------------------------------------------------------

def _make_executor_with_mock_judge(judgment: StepJudgment) -> LLMJudgeExecutor:
    executor = LLMJudgeExecutor()
    async def _mock_judge(prompt: str) -> StepJudgment:
        return judgment
    executor._judge_fn = _mock_judge
    return executor


@pytest.mark.asyncio
async def test_llm_judge_passes_when_judgment_passed():
    judgment = StepJudgment(reasoning="Title found on page.", passed=True, score=0.95)
    executor = _make_executor_with_mock_judge(judgment)
    step = MagicMock()
    step.expected_outcome = "Page title is 'Dashboard'"
    result = await executor("navigate:https://app.example.com", "s1", context={"current_step": step})
    assert result["passed"] is True
    assert result["output"]["score"] == 0.95
    assert "Title found" in result["output"]["reasoning"]


@pytest.mark.asyncio
async def test_llm_judge_fails_when_judgment_failed():
    judgment = StepJudgment(reasoning="Title not found.", passed=False, score=0.1)
    executor = _make_executor_with_mock_judge(judgment)
    step = MagicMock()
    step.expected_outcome = "Page title is 'Dashboard'"
    result = await executor("navigate:https://app.example.com", "s1", context={"current_step": step})
    assert result["passed"] is False
    assert result["error"] is not None
    assert "Judge:" in result["error"]


@pytest.mark.asyncio
async def test_llm_judge_error_returns_failed():
    executor = LLMJudgeExecutor()
    async def _failing_judge(prompt: str):
        raise RuntimeError("API timeout")
    executor._judge_fn = _failing_judge
    step = MagicMock()
    step.expected_outcome = "some outcome"
    result = await executor("click:#x", "s1", context={"current_step": step})
    assert result["passed"] is False
    assert "API timeout" in result["error"]


# ---------------------------------------------------------------------------
# ImportError when no LLM SDK installed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_import_error_when_no_sdk():
    executor = LLMJudgeExecutor()
    # Force _judge_fn to None so _build_judge runs
    executor._judge_fn = None
    step = MagicMock()
    step.expected_outcome = "something"
    with patch.dict("sys.modules", {"langchain_anthropic": None, "anthropic": None}):
        result = await executor("click:#x", "s1", context={"current_step": step})
    assert result["passed"] is False
    assert "not installed" in result["error"] or "ImportError" in result["error"] or result["error"] is not None


# ---------------------------------------------------------------------------
# make_llm_judge factory
# ---------------------------------------------------------------------------

def test_make_llm_judge_returns_executor():
    judge = make_llm_judge(model="claude-sonnet-4-6")
    assert isinstance(judge, LLMJudgeExecutor)
    assert judge._model == "claude-sonnet-4-6"


# ---------------------------------------------------------------------------
# Actual output passed through context
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_actual_output_from_context():
    captured_prompt = {}
    async def _capture_judge(prompt: str) -> StepJudgment:
        captured_prompt["value"] = prompt
        return StepJudgment(reasoning="ok", passed=True, score=1.0)

    executor = LLMJudgeExecutor()
    executor._judge_fn = _capture_judge
    step = MagicMock()
    step.expected_outcome = "button clicked"
    ctx = {"current_step": step, "last_output": {"clicked": True}}
    await executor("click:#btn", "s1", context=ctx)
    assert "clicked" in captured_prompt["value"] or "button clicked" in captured_prompt["value"]
