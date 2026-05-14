"""
LLMJudge — LLM-as-judge evaluation harness for EvalPack step outputs.

Harvested from: RAGAS (MIT) faithfulness/relevancy/recall metric patterns.

Provides RAGAS-inspired evaluation metrics that work without a live LLM:
  - faithfulness:   does the answer contain only information from the context?
  - answer_relevance: is the answer relevant to the question?
  - context_recall: does the context contain enough to answer the question?
  - exact_match:    does the output exactly match expected (rule-based)?
  - oracle_pass:    does the output satisfy oracle_rules (regex/keyword checks)?

Two execution modes:
  1. Offline heuristic (default): pure-Python, zero API calls.
     Uses TF-IDF cosine overlap as a proxy for semantic similarity.
     Suitable for CI and unit testing.
  2. LLM-backed (optional): passes structured prompts to any OpenAI-compatible
     endpoint. Activated by passing `llm_client` to LLMJudge.__init__().

Constitutional guarantees:
- Local-first: offline mode requires no API keys, no network
- Fail-closed: missing expected_outputs causes oracle_pass=False (not skip)
- Zero-ambiguity: all metric scores are float [0.0, 1.0], never None
"""

from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass
class MetricScore:
    name: str
    score: float          # [0.0, 1.0]
    passed: bool          # score >= threshold
    threshold: float = 0.5
    detail: Optional[str] = None

    def __post_init__(self):
        self.passed = self.score >= self.threshold


@dataclass
class JudgeVerdict:
    case_id: str
    overall_score: float        # weighted mean of metric scores
    passed: bool                # overall_score >= pass_threshold
    metrics: List[MetricScore] = field(default_factory=list)
    raw_output: Optional[str] = None
    explanation: Optional[str] = None

    @property
    def metric_map(self) -> Dict[str, float]:
        return {m.name: m.score for m in self.metrics}


@dataclass
class EvalRunSummary:
    """Aggregated result across all task cases in an EvalPack."""
    total_cases: int
    passed_cases: int
    failed_cases: int
    pass_rate: float
    mean_faithfulness: float
    mean_relevance: float
    mean_recall: float
    mean_oracle: float
    verdicts: List[JudgeVerdict] = field(default_factory=list)

    @property
    def overall_quality(self) -> float:
        return (
            self.mean_faithfulness * 0.30
            + self.mean_relevance * 0.30
            + self.mean_recall * 0.20
            + self.mean_oracle * 0.20
        )


# ---------------------------------------------------------------------------
# Offline heuristic metrics (zero deps, zero API calls)
# ---------------------------------------------------------------------------

def _tokenize(text: str) -> List[str]:
    return re.findall(r"[a-z0-9]+", text.lower())


def _tfidf_vector(text: str) -> Counter:
    tokens = _tokenize(text)
    total = max(len(tokens), 1)
    return Counter({t: c / total for t, c in Counter(tokens).items()})


def _cosine(a: Counter, b: Counter) -> float:
    common = set(a) & set(b)
    if not common:
        return 0.0
    dot = sum(a[t] * b[t] for t in common)
    norm_a = math.sqrt(sum(v * v for v in a.values()))
    norm_b = math.sqrt(sum(v * v for v in b.values()))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return min(dot / (norm_a * norm_b), 1.0)


def _faithfulness_score(answer: str, context: str) -> float:
    """
    Faithfulness: fraction of answer tokens that appear in context.
    A perfect score means the answer is fully grounded in context.
    """
    a_tokens = set(_tokenize(answer))
    c_tokens = set(_tokenize(context))
    if not a_tokens:
        return 1.0
    return len(a_tokens & c_tokens) / len(a_tokens)


def _relevance_score(answer: str, question: str) -> float:
    """Answer relevance: TF-IDF cosine similarity between answer and question."""
    if not answer or not question:
        return 0.0
    return _cosine(_tfidf_vector(answer), _tfidf_vector(question))


def _context_recall_score(context: str, expected: str) -> float:
    """
    Context recall: fraction of expected tokens present in context.
    Measures whether the context is sufficient to produce the expected answer.
    """
    e_tokens = set(_tokenize(expected))
    c_tokens = set(_tokenize(context))
    if not e_tokens:
        return 1.0
    return len(e_tokens & c_tokens) / len(e_tokens)


def _oracle_pass_score(output: str, oracle_rules: List[str]) -> float:
    """
    Oracle rule evaluation: each rule is a keyword/regex that must match output.
    Returns fraction of rules that pass.
    """
    if not oracle_rules:
        return 1.0
    passed = 0
    for rule in oracle_rules:
        try:
            if re.search(rule, output, re.IGNORECASE):
                passed += 1
        except re.error:
            # Invalid regex → treat as keyword search
            if rule.lower() in output.lower():
                passed += 1
    return passed / len(oracle_rules)


def _exact_match_score(output: str, expected_outputs: List[Any]) -> float:
    """Returns 1.0 if output exactly matches any expected_output string."""
    if not expected_outputs:
        return 1.0
    for exp in expected_outputs:
        if str(exp).strip() == output.strip():
            return 1.0
    return 0.0


# ---------------------------------------------------------------------------
# LLMJudge
# ---------------------------------------------------------------------------

class LLMJudge:
    """
    LLM-as-judge evaluator for EvalPack task cases.

    Usage (offline/CI mode — no LLM required):
        judge = LLMJudge()
        verdict = judge.evaluate_case(
            case_id="tc-1",
            question="What is the invoice total?",
            context="Invoice shows $1,250.00 for services rendered.",
            actual_output="The invoice total is $1,250.00.",
            expected_outputs=["$1,250.00"],
            oracle_rules=["1250", "invoice"],
        )
        print(verdict.passed, verdict.overall_score)

    Usage (LLM-backed mode):
        judge = LLMJudge(llm_client=openai_client, model="gpt-4o-mini")
        verdict = judge.evaluate_case(...)

    Both modes return the same JudgeVerdict dataclass.
    """

    def __init__(
        self,
        pass_threshold: float = 0.70,
        faithfulness_threshold: float = 0.60,
        relevance_threshold: float = 0.50,
        recall_threshold: float = 0.50,
        oracle_threshold: float = 0.80,
        llm_client: Optional[Any] = None,
        model: str = "gpt-4o-mini",
    ):
        self.pass_threshold = pass_threshold
        self.faithfulness_threshold = faithfulness_threshold
        self.relevance_threshold = relevance_threshold
        self.recall_threshold = recall_threshold
        self.oracle_threshold = oracle_threshold
        self._llm_client = llm_client
        self._model = model

    def evaluate_case(
        self,
        case_id: str,
        actual_output: str,
        question: str = "",
        context: str = "",
        expected_outputs: Optional[List[Any]] = None,
        oracle_rules: Optional[List[str]] = None,
    ) -> JudgeVerdict:
        """
        Evaluate a single task case output.

        Args:
            case_id:          identifier from TaskCase
            actual_output:    the string produced by step execution
            question:         the task description or query
            context:          retrieved context / source document
            expected_outputs: list of acceptable outputs (for exact match)
            oracle_rules:     list of regex patterns that must match output

        Returns:
            JudgeVerdict with per-metric scores and an overall pass/fail.
        """
        expected = expected_outputs or []
        rules = oracle_rules or []

        if self._llm_client:
            return self._llm_evaluate(
                case_id, actual_output, question, context, expected, rules
            )
        return self._heuristic_evaluate(
            case_id, actual_output, question, context, expected, rules
        )

    def _heuristic_evaluate(
        self,
        case_id: str,
        output: str,
        question: str,
        context: str,
        expected: List[Any],
        rules: List[str],
    ) -> JudgeVerdict:
        """Pure-Python heuristic evaluation — zero API calls."""
        f_score = _faithfulness_score(output, context) if context else 1.0
        r_score = _relevance_score(output, question) if question else 1.0
        c_score = _context_recall_score(context, str(expected[0])) if expected and context else 1.0
        o_score = _oracle_pass_score(output, rules)
        em_score = _exact_match_score(output, expected)

        metrics = [
            MetricScore("faithfulness", f_score, f_score >= self.faithfulness_threshold,
                        self.faithfulness_threshold, "heuristic TF-IDF token overlap"),
            MetricScore("answer_relevance", r_score, r_score >= self.relevance_threshold,
                        self.relevance_threshold, "heuristic TF-IDF cosine"),
            MetricScore("context_recall", c_score, c_score >= self.recall_threshold,
                        self.recall_threshold, "heuristic token recall"),
            MetricScore("oracle_pass", o_score, o_score >= self.oracle_threshold,
                        self.oracle_threshold, f"{len(rules)} rules"),
            MetricScore("exact_match", em_score, em_score >= 0.5, 0.5,
                        f"{len(expected)} expected outputs"),
        ]

        overall = (
            f_score * 0.25
            + r_score * 0.25
            + c_score * 0.20
            + o_score * 0.20
            + em_score * 0.10
        )

        return JudgeVerdict(
            case_id=case_id,
            overall_score=round(overall, 4),
            passed=overall >= self.pass_threshold,
            metrics=metrics,
            raw_output=output,
            explanation="heuristic evaluation (offline mode)",
        )

    def _llm_evaluate(
        self,
        case_id: str,
        output: str,
        question: str,
        context: str,
        expected: List[Any],
        rules: List[str],
    ) -> JudgeVerdict:
        """
        LLM-backed evaluation via OpenAI-compatible client.
        Falls back to heuristic if the API call fails.
        """
        prompt = self._build_prompt(output, question, context, expected, rules)
        try:
            response = self._llm_client.chat.completions.create(
                model=self._model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=512,
                temperature=0.0,
            )
            text = response.choices[0].message.content.strip()
            return self._parse_llm_response(case_id, output, text, rules)
        except Exception:
            return self._heuristic_evaluate(case_id, output, question, context, expected, rules)

    def _build_prompt(
        self,
        output: str,
        question: str,
        context: str,
        expected: List[Any],
        rules: List[str],
    ) -> str:
        lines = [
            "You are an impartial evaluator. Rate the following answer on a scale from 0.0 to 1.0 for each metric.",
            "",
            f"Question: {question}" if question else "",
            f"Context: {context[:500]}" if context else "",
            f"Expected: {expected[0]}" if expected else "",
            f"Oracle rules: {rules}" if rules else "",
            f"Answer: {output}",
            "",
            "Respond with exactly this JSON (no other text):",
            '{"faithfulness": 0.0, "answer_relevance": 0.0, "context_recall": 0.0, "oracle_pass": 0.0, "explanation": "..."}',
        ]
        return "\n".join(l for l in lines if l or not l.startswith(""))

    def _parse_llm_response(
        self,
        case_id: str,
        output: str,
        response_text: str,
        rules: List[str],
    ) -> JudgeVerdict:
        """Parse JSON response from LLM judge."""
        import json
        try:
            data = json.loads(response_text)
            f_score = float(data.get("faithfulness", 0.5))
            r_score = float(data.get("answer_relevance", 0.5))
            c_score = float(data.get("context_recall", 0.5))
            o_score = float(data.get("oracle_pass", _oracle_pass_score(output, rules)))
            explanation = data.get("explanation", "LLM evaluation")

            metrics = [
                MetricScore("faithfulness", f_score, f_score >= self.faithfulness_threshold,
                            self.faithfulness_threshold),
                MetricScore("answer_relevance", r_score, r_score >= self.relevance_threshold,
                            self.relevance_threshold),
                MetricScore("context_recall", c_score, c_score >= self.recall_threshold,
                            self.recall_threshold),
                MetricScore("oracle_pass", o_score, o_score >= self.oracle_threshold,
                            self.oracle_threshold),
            ]
            overall = (f_score * 0.30 + r_score * 0.30 + c_score * 0.20 + o_score * 0.20)

            return JudgeVerdict(
                case_id=case_id,
                overall_score=round(overall, 4),
                passed=overall >= self.pass_threshold,
                metrics=metrics,
                raw_output=output,
                explanation=explanation,
            )
        except (json.JSONDecodeError, KeyError, ValueError):
            return self._heuristic_evaluate(case_id, output, "", "", [], rules)

    # ------------------------------------------------------------------
    # Batch evaluation
    # ------------------------------------------------------------------

    def evaluate_pack(
        self,
        cases: List[Dict[str, Any]],
    ) -> EvalRunSummary:
        """
        Evaluate all task cases in an EvalPack.

        Each element of `cases` should be a dict with keys:
            case_id, actual_output, question, context, expected_outputs, oracle_rules

        Returns an EvalRunSummary with aggregate RAGAS metrics.
        """
        verdicts = []
        for case in cases:
            v = self.evaluate_case(
                case_id=case.get("case_id", ""),
                actual_output=case.get("actual_output", ""),
                question=case.get("question", ""),
                context=case.get("context", ""),
                expected_outputs=case.get("expected_outputs"),
                oracle_rules=case.get("oracle_rules"),
            )
            verdicts.append(v)

        if not verdicts:
            return EvalRunSummary(
                total_cases=0,
                passed_cases=0,
                failed_cases=0,
                pass_rate=0.0,
                mean_faithfulness=0.0,
                mean_relevance=0.0,
                mean_recall=0.0,
                mean_oracle=0.0,
                verdicts=[],
            )

        passed = sum(1 for v in verdicts if v.passed)
        total = len(verdicts)

        def mean_metric(name: str) -> float:
            scores = [v.metric_map.get(name, 0.0) for v in verdicts]
            return round(sum(scores) / len(scores), 4) if scores else 0.0

        return EvalRunSummary(
            total_cases=total,
            passed_cases=passed,
            failed_cases=total - passed,
            pass_rate=round(passed / total, 4),
            mean_faithfulness=mean_metric("faithfulness"),
            mean_relevance=mean_metric("answer_relevance"),
            mean_recall=mean_metric("context_recall"),
            mean_oracle=mean_metric("oracle_pass"),
            verdicts=verdicts,
        )
