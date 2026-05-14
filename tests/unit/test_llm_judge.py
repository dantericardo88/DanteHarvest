"""Tests for LLMJudge — LLM-as-judge evaluation harness with RAGAS metrics."""

import pytest

from harvest_core.evaluation.llm_judge import (
    LLMJudge,
    JudgeVerdict,
    EvalRunSummary,
    MetricScore,
    _faithfulness_score,
    _relevance_score,
    _context_recall_score,
    _oracle_pass_score,
    _exact_match_score,
)


# ---------------------------------------------------------------------------
# Metric unit tests
# ---------------------------------------------------------------------------

class TestFaithfulnessScore:
    def test_fully_grounded_answer(self):
        context = "The invoice total is 1250 dollars for services rendered in January."
        answer = "The invoice total is 1250 dollars."
        score = _faithfulness_score(answer, context)
        assert score >= 0.8

    def test_ungrounded_answer(self):
        context = "Weather report: sunny skies in Seattle."
        answer = "The invoice requires payment by net-30 terms."
        score = _faithfulness_score(answer, context)
        assert score <= 0.3

    def test_empty_answer_returns_one(self):
        score = _faithfulness_score("", "some context")
        assert score == 1.0

    def test_score_in_range(self):
        score = _faithfulness_score("some answer", "some context")
        assert 0.0 <= score <= 1.0


class TestRelevanceScore:
    def test_relevant_answer(self):
        question = "What is the invoice total?"
        answer = "The invoice total is $1,250."
        score = _relevance_score(answer, question)
        assert score > 0.3

    def test_empty_inputs(self):
        assert _relevance_score("", "question") == 0.0
        assert _relevance_score("answer", "") == 0.0

    def test_score_in_range(self):
        score = _relevance_score("any answer", "any question")
        assert 0.0 <= score <= 1.0


class TestContextRecallScore:
    def test_full_recall(self):
        context = "Invoice 1250 payment due January"
        expected = "1250 payment"
        score = _context_recall_score(context, expected)
        assert score == 1.0

    def test_zero_recall(self):
        context = "weather sunny"
        expected = "invoice total payment"
        score = _context_recall_score(context, expected)
        assert score == 0.0

    def test_empty_expected_returns_one(self):
        score = _context_recall_score("any context", "")
        assert score == 1.0


class TestOraclePassScore:
    def test_all_rules_pass(self):
        # Use words without punctuation splitting them so regex matches cleanly
        output = "Invoice total is 1250 dollars due by January 15."
        rules = ["1250", "invoice", "january"]
        score = _oracle_pass_score(output, rules)
        assert score == 1.0

    def test_partial_rules_pass(self):
        # Only "invoice" matches; "january" and "payment" are absent
        output = "Invoice total due."
        rules = ["invoice", "january", "payment"]
        score = _oracle_pass_score(output, rules)
        assert score == pytest.approx(1 / 3, rel=0.01)

    def test_no_rules_returns_one(self):
        score = _oracle_pass_score("any output", [])
        assert score == 1.0

    def test_regex_rules(self):
        output = "Order #12345 confirmed."
        rules = [r"Order #\d+"]
        score = _oracle_pass_score(output, rules)
        assert score == 1.0

    def test_invalid_regex_treated_as_keyword(self):
        output = "[bracket]"
        rules = ["[bracket]"]  # invalid regex
        score = _oracle_pass_score(output, rules)
        assert score == 1.0

    def test_case_insensitive(self):
        output = "Invoice processed successfully."
        rules = ["INVOICE", "SUCCESSFULLY"]
        score = _oracle_pass_score(output, rules)
        assert score == 1.0


class TestExactMatchScore:
    def test_exact_match(self):
        score = _exact_match_score("$1,250.00", ["$1,250.00"])
        assert score == 1.0

    def test_no_match(self):
        score = _exact_match_score("$1,250", ["$1,000"])
        assert score == 0.0

    def test_multiple_expected_any_match(self):
        score = _exact_match_score("yes", ["no", "yes", "maybe"])
        assert score == 1.0

    def test_empty_expected_returns_one(self):
        score = _exact_match_score("any", [])
        assert score == 1.0


# ---------------------------------------------------------------------------
# LLMJudge integration tests
# ---------------------------------------------------------------------------

class TestLLMJudgeOffline:
    def setup_method(self):
        self.judge = LLMJudge(pass_threshold=0.60)

    def test_evaluate_case_returns_verdict(self):
        verdict = self.judge.evaluate_case(
            case_id="tc-001",
            actual_output="The invoice total is $1,250.",
            question="What is the invoice total?",
            context="Invoice shows $1,250.00 for services rendered.",
            expected_outputs=["$1,250"],
            oracle_rules=["1250"],
        )
        assert isinstance(verdict, JudgeVerdict)
        assert verdict.case_id == "tc-001"

    def test_verdict_overall_score_in_range(self):
        verdict = self.judge.evaluate_case(
            case_id="tc-002",
            actual_output="Payment processed.",
            oracle_rules=["payment"],
        )
        assert 0.0 <= verdict.overall_score <= 1.0

    def test_verdict_has_all_metrics(self):
        verdict = self.judge.evaluate_case(
            case_id="tc-003",
            actual_output="The vendor is approved for payment.",
            question="Is the vendor approved?",
            context="Vendor approval status: approved.",
            expected_outputs=["approved"],
            oracle_rules=["approved"],
        )
        metric_names = {m.name for m in verdict.metrics}
        assert "faithfulness" in metric_names
        assert "answer_relevance" in metric_names
        assert "context_recall" in metric_names
        assert "oracle_pass" in metric_names

    def test_verdict_metric_map(self):
        verdict = self.judge.evaluate_case(
            case_id="tc-004",
            actual_output="Invoice payment confirmed.",
            oracle_rules=["invoice"],
        )
        mm = verdict.metric_map
        assert "faithfulness" in mm
        assert "oracle_pass" in mm
        for v in mm.values():
            assert 0.0 <= v <= 1.0

    def test_high_quality_answer_passes(self):
        verdict = self.judge.evaluate_case(
            case_id="tc-005",
            actual_output="invoice payment vendor accounting approved",
            question="invoice payment",
            context="invoice payment vendor accounting ledger",
            expected_outputs=["invoice payment vendor accounting approved"],
            oracle_rules=["invoice", "payment"],
        )
        # With all metrics satisfied, should have high score
        assert verdict.overall_score >= 0.5

    def test_no_optional_params_still_works(self):
        verdict = self.judge.evaluate_case(
            case_id="tc-006",
            actual_output="some output",
        )
        assert isinstance(verdict, JudgeVerdict)
        assert verdict.overall_score >= 0.0

    def test_explanation_set(self):
        verdict = self.judge.evaluate_case(
            case_id="tc-007",
            actual_output="test",
        )
        assert verdict.explanation is not None


class TestLLMJudgeBatchEval:
    def setup_method(self):
        self.judge = LLMJudge()

    def test_evaluate_pack_returns_summary(self):
        cases = [
            {
                "case_id": "tc-001",
                "actual_output": "Invoice processed.",
                "oracle_rules": ["invoice"],
            },
            {
                "case_id": "tc-002",
                "actual_output": "Vendor approved.",
                "oracle_rules": ["vendor"],
            },
        ]
        summary = self.judge.evaluate_pack(cases)
        assert isinstance(summary, EvalRunSummary)
        assert summary.total_cases == 2

    def test_summary_pass_rate_range(self):
        cases = [
            {"case_id": "t1", "actual_output": "ok", "oracle_rules": ["ok"]},
            {"case_id": "t2", "actual_output": "bad", "oracle_rules": ["missing"]},
        ]
        summary = self.judge.evaluate_pack(cases)
        assert 0.0 <= summary.pass_rate <= 1.0

    def test_summary_means_in_range(self):
        cases = [
            {
                "case_id": f"tc-{i}",
                "actual_output": "invoice payment processed",
                "oracle_rules": ["invoice"],
            }
            for i in range(5)
        ]
        summary = self.judge.evaluate_pack(cases)
        assert 0.0 <= summary.mean_faithfulness <= 1.0
        assert 0.0 <= summary.mean_oracle <= 1.0

    def test_empty_cases_returns_zero_summary(self):
        summary = self.judge.evaluate_pack([])
        assert summary.total_cases == 0
        assert summary.pass_rate == 0.0

    def test_summary_verdicts_match_cases(self):
        cases = [
            {"case_id": f"tc-{i}", "actual_output": "output", "oracle_rules": []}
            for i in range(4)
        ]
        summary = self.judge.evaluate_pack(cases)
        assert len(summary.verdicts) == 4

    def test_overall_quality_in_range(self):
        cases = [
            {"case_id": "tc-1", "actual_output": "invoice total 1250"},
        ]
        summary = self.judge.evaluate_pack(cases)
        assert 0.0 <= summary.overall_quality <= 1.0

    def test_passed_failed_sum_to_total(self):
        cases = [
            {"case_id": f"tc-{i}", "actual_output": "test"} for i in range(6)
        ]
        summary = self.judge.evaluate_pack(cases)
        assert summary.passed_cases + summary.failed_cases == summary.total_cases


class TestMetricScore:
    def test_passed_threshold_check(self):
        m = MetricScore(name="test", score=0.8, passed=False, threshold=0.7)
        # __post_init__ sets passed based on score >= threshold
        assert m.passed is True

    def test_failed_threshold_check(self):
        m = MetricScore(name="test", score=0.4, passed=True, threshold=0.5)
        assert m.passed is False
