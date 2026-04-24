"""
LLMJudgeExecutor — LLM-as-judge step evaluator for ReplayHarness.

Harvested from: langchain-ai/open_deep_research (MIT) — structured output evaluator pattern.

Replaces _noop_executor with a real evaluation mechanism:
- When PackStep.expected_outcome is set: calls an LLM to judge pass/fail with reasoning
- When no expected_outcome: structural pass (step is considered non-evaluable)
- Returns score 0.0–1.0 alongside pass/fail for the ReplayReport

Constitutional guarantees:
- Local-first: defaults to Anthropic API (requires ANTHROPIC_API_KEY); no server infra needed
- Fail-closed: LLM call failures return {"passed": False, "error": "..."} — never silently pass
- Zero-ambiguity: every judgment carries .reasoning (chain-of-thought) to prevent hallucination
- Zero-dependency fallback: if neither langchain-anthropic nor anthropic is installed, raises
  ImportError with install instructions rather than silently using noop
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Judgment schema — reasoning MUST precede the verdict (anti-hallucination)
# ---------------------------------------------------------------------------

class StepJudgment(BaseModel):
    reasoning: str = Field(
        description=(
            "Step-by-step analysis of whether the actual output satisfies "
            "the expected_outcome. Think before concluding."
        )
    )
    passed: bool = Field(
        description="True if the output satisfies the expected_outcome, False otherwise."
    )
    score: float = Field(
        ge=0.0,
        le=1.0,
        description=(
            "Confidence score 0.0–1.0. "
            "1.0 = exact semantic match. "
            "0.8 = strong match with minor differences. "
            "0.5 = partial match — key elements present but incomplete. "
            "0.3 = marginal match. "
            "0.0 = complete failure or no match."
        ),
    )


_JUDGE_PROMPT = """You are evaluating a browser or desktop automation step.

Step action: {action}
Expected outcome: {expected_outcome}
Actual output (truncated to 2000 chars): {actual_output}

Assess whether the actual output satisfies the expected outcome.
Be strict: partial matches score 0.3–0.6. Exact semantic matches score 0.8–1.0.
A step with no actual output fails unless the expected_outcome also allows no output."""


def _build_judge(model_name: str):
    """
    Return a callable that takes a prompt and returns StepJudgment.
    Tries langchain-anthropic first, then raw anthropic SDK, then raises.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("HARVEST_LLM_KEY")

    # Strategy 1: langchain-anthropic (structured_output auto-validates Pydantic)
    try:
        from langchain_anthropic import ChatAnthropic
        llm = ChatAnthropic(model=model_name, api_key=api_key, max_tokens=1024)
        structured = llm.with_structured_output(StepJudgment)

        async def _call_lc(prompt: str) -> StepJudgment:
            return await structured.ainvoke([{"role": "user", "content": prompt}])

        return _call_lc
    except ImportError:
        pass

    # Strategy 2: raw Anthropic SDK with JSON mode
    try:
        import anthropic
        import json

        client = anthropic.AsyncAnthropic(api_key=api_key)

        async def _call_raw(prompt: str) -> StepJudgment:
            msg = await client.messages.create(
                model=model_name,
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt + "\n\nRespond with JSON matching: {reasoning: str, passed: bool, score: float}"}],
            )
            raw = msg.content[0].text
            data = json.loads(raw[raw.index("{"):raw.rindex("}") + 1])
            return StepJudgment(**data)

        return _call_raw
    except ImportError:
        pass

    raise ImportError(
        "LLMJudgeExecutor requires either langchain-anthropic or anthropic. "
        "Run: pip install langchain-anthropic  OR  pip install anthropic"
    )


class LLMJudgeExecutor:
    """
    LLM-as-judge step executor for ReplayHarness.

    Usage:
        executor = LLMJudgeExecutor()
        harness = ReplayHarness(chain_writer, step_executor=executor)
        report = await harness.replay(pack, run_id="run-001")

    Steps WITH expected_outcome get judged by the LLM.
    Steps WITHOUT expected_outcome pass structurally (non-evaluable).
    """

    def __init__(
        self,
        model: str = "claude-sonnet-4-6",
        fallback_pass_on_no_expected: bool = True,
    ):
        self._model = model
        self._fallback_pass = fallback_pass_on_no_expected
        self._judge_fn = None

    def _get_judge(self):
        if self._judge_fn is None:
            self._judge_fn = _build_judge(self._model)
        return self._judge_fn

    async def __call__(
        self,
        action: str,
        step_id: str,
        context: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        ctx = context or {}
        step = ctx.get("current_step")
        expected_outcome = getattr(step, "expected_outcome", None) if step else None
        actual_output = kwargs.get("output") or ctx.get("last_output")

        # No expected_outcome → structural pass (step not evaluable by LLM)
        if not expected_outcome:
            if self._fallback_pass:
                return {
                    "passed": True,
                    "output": {"score": 1.0, "reasoning": "No expected_outcome declared — structural pass."},
                    "error": None,
                }
            return {
                "passed": False,
                "output": None,
                "error": "No expected_outcome on step and fallback_pass_on_no_expected=False",
            }

        prompt = _JUDGE_PROMPT.format(
            action=action,
            expected_outcome=expected_outcome,
            actual_output=str(actual_output)[:2000],
        )

        try:
            judge = self._get_judge()
            judgment: StepJudgment = await judge(prompt)
        except ImportError as e:
            return {"passed": False, "output": None, "error": str(e)}
        except Exception as e:
            return {"passed": False, "output": None, "error": f"LLM judge error: {e}"}

        return {
            "passed": judgment.passed,
            "output": {"score": judgment.score, "reasoning": judgment.reasoning},
            "error": None if judgment.passed else f"Judge: {judgment.reasoning[:300]}",
        }


# ---------------------------------------------------------------------------
# Convenience factory for common usage
# ---------------------------------------------------------------------------

def make_llm_judge(model: str = "claude-sonnet-4-6") -> LLMJudgeExecutor:
    """Return a configured LLMJudgeExecutor instance."""
    return LLMJudgeExecutor(model=model)
