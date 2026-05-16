"""
Unit tests for ExtractionEvaluator and EvalResult.
"""
from __future__ import annotations

import pytest
from harvest_distill.eval import ExtractionEvaluator, EvalResult


@pytest.fixture
def ev():
    return ExtractionEvaluator()


# ---------------------------------------------------------------------------
# evaluate_tokens
# ---------------------------------------------------------------------------

class TestEvaluateTokens:
    def test_identical_strings_give_perfect_f1(self, ev):
        result = ev.evaluate_tokens("the quick brown fox", "the quick brown fox")
        assert result.f1 == pytest.approx(1.0)
        assert result.precision == pytest.approx(1.0)
        assert result.recall == pytest.approx(1.0)

    def test_identical_strings_exact_match(self, ev):
        result = ev.evaluate_tokens("hello world", "hello world")
        assert result.exact_match is True

    def test_disjoint_strings_give_zero_f1(self, ev):
        result = ev.evaluate_tokens("alpha beta gamma", "delta epsilon zeta")
        assert result.f1 == pytest.approx(0.0)
        assert result.precision == pytest.approx(0.0)
        assert result.recall == pytest.approx(0.0)

    def test_disjoint_no_exact_match(self, ev):
        result = ev.evaluate_tokens("foo bar", "baz qux")
        assert result.exact_match is False

    def test_partial_overlap_f1(self, ev):
        # pred = {a, b}, truth = {a, c}  → overlap=1 → P=0.5, R=0.5, F1=0.5
        result = ev.evaluate_tokens("a b", "a c")
        assert 0.0 < result.f1 < 1.0
        assert result.precision == pytest.approx(0.5)
        assert result.recall == pytest.approx(0.5)

    def test_empty_both_perfect(self, ev):
        result = ev.evaluate_tokens("", "")
        assert result.f1 == pytest.approx(1.0)
        assert result.exact_match is True

    def test_empty_pred_zero_f1(self, ev):
        result = ev.evaluate_tokens("", "some content here")
        assert result.f1 == pytest.approx(0.0)

    def test_returns_eval_result(self, ev):
        result = ev.evaluate_tokens("hello", "hello")
        assert isinstance(result, EvalResult)

    def test_case_insensitive_tokenisation(self, ev):
        result = ev.evaluate_tokens("Hello World", "hello world")
        assert result.f1 == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# _rouge_l
# ---------------------------------------------------------------------------

class TestRougeL:
    def test_identical_gives_one(self, ev):
        score = ev._rouge_l("the cat sat on the mat", "the cat sat on the mat")
        assert score == pytest.approx(1.0)

    def test_disjoint_gives_zero(self, ev):
        score = ev._rouge_l("apple orange pear", "cat dog fish")
        assert score == pytest.approx(0.0)

    def test_partial_sequence(self, ev):
        # "a b c" vs "a x c" — LCS = [a, c] = 2
        score = ev._rouge_l("a b c", "a x c")
        assert 0.0 < score < 1.0

    def test_empty_both_one(self, ev):
        assert ev._rouge_l("", "") == pytest.approx(1.0)

    def test_empty_one_side_zero(self, ev):
        assert ev._rouge_l("something here", "") == pytest.approx(0.0)
        assert ev._rouge_l("", "something here") == pytest.approx(0.0)

    def test_long_input_capped(self, ev):
        # 600-word strings should not raise; capped at 500
        long = " ".join([f"word{i}" for i in range(600)])
        score = ev._rouge_l(long, long)
        assert score == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# _lcs_length
# ---------------------------------------------------------------------------

class TestLcsLength:
    def test_empty_sequences(self, ev):
        assert ev._lcs_length([], []) == 0

    def test_no_common(self, ev):
        assert ev._lcs_length(["a", "b"], ["c", "d"]) == 0

    def test_full_match(self, ev):
        assert ev._lcs_length(["a", "b", "c"], ["a", "b", "c"]) == 3

    def test_subsequence(self, ev):
        # "a b d" in "a c b d e"
        assert ev._lcs_length(["a", "b", "d"], ["a", "c", "b", "d", "e"]) == 3


# ---------------------------------------------------------------------------
# evaluate_structured
# ---------------------------------------------------------------------------

class TestEvaluateStructured:
    def test_identical_dicts_perfect_f1(self, ev):
        pred = {"title": "hello world", "body": "some text here"}
        truth = {"title": "hello world", "body": "some text here"}
        result = ev.evaluate_structured(pred, truth)
        assert result.f1 == pytest.approx(1.0)

    def test_missing_key_lowers_score(self, ev):
        pred = {"title": "hello world"}
        truth = {"title": "hello world", "body": "some text"}
        result = ev.evaluate_structured(pred, truth)
        assert result.f1 < 1.0
        assert result.field_scores["body"] == pytest.approx(0.0)

    def test_field_scores_populated(self, ev):
        pred = {"a": "cat dog", "b": "foo bar"}
        truth = {"a": "cat dog", "b": "baz qux"}
        result = ev.evaluate_structured(pred, truth)
        assert "a" in result.field_scores
        assert "b" in result.field_scores
        assert result.field_scores["a"] == pytest.approx(1.0)
        assert result.field_scores["b"] == pytest.approx(0.0)

    def test_per_field_partial_match(self, ev):
        pred = {"name": "john doe", "city": "new york boston"}
        truth = {"name": "john doe", "city": "new york"}
        result = ev.evaluate_structured(pred, truth)
        assert result.field_scores["name"] == pytest.approx(1.0)
        # "city" has partial overlap (recall=1.0, precision=0.5 → F1≈0.667)
        assert 0.0 < result.field_scores["city"] < 1.0

    def test_empty_ground_truth_perfect(self, ev):
        result = ev.evaluate_structured({}, {})
        assert result.f1 == pytest.approx(1.0)

    def test_returns_eval_result(self, ev):
        result = ev.evaluate_structured({"k": "v"}, {"k": "v"})
        assert isinstance(result, EvalResult)


# ---------------------------------------------------------------------------
# evaluate_batch
# ---------------------------------------------------------------------------

class TestEvaluateBatch:
    def test_all_identical_avg_f1_one(self, ev):
        preds = ["hello world", "foo bar"]
        truths = ["hello world", "foo bar"]
        out = ev.evaluate_batch(preds, truths)
        assert out["avg_f1"] == pytest.approx(1.0)
        assert out["exact_match_rate"] == pytest.approx(1.0)

    def test_all_disjoint_avg_f1_zero(self, ev):
        preds = ["alpha", "beta"]
        truths = ["gamma", "delta"]
        out = ev.evaluate_batch(preds, truths)
        assert out["avg_f1"] == pytest.approx(0.0)
        assert out["exact_match_rate"] == pytest.approx(0.0)

    def test_mixed_batch(self, ev):
        preds = ["hello world", "alpha beta"]
        truths = ["hello world", "gamma delta"]
        out = ev.evaluate_batch(preds, truths)
        # first pair = 1.0, second pair = 0.0 → avg = 0.5
        assert out["avg_f1"] == pytest.approx(0.5)
        assert out["exact_match_rate"] == pytest.approx(0.5)

    def test_count_correct(self, ev):
        out = ev.evaluate_batch(["a", "b", "c"], ["a", "b", "c"])
        assert out["count"] == 3

    def test_results_list_populated(self, ev):
        out = ev.evaluate_batch(["hello"], ["hello"])
        assert len(out["results"]) == 1
        assert isinstance(out["results"][0], EvalResult)

    def test_mismatched_lengths_raises(self, ev):
        with pytest.raises(ValueError):
            ev.evaluate_batch(["a", "b"], ["a"])

    def test_empty_batch(self, ev):
        out = ev.evaluate_batch([], [])
        assert out["count"] == 0
        assert out["avg_f1"] == pytest.approx(0.0)
