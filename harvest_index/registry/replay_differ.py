"""
ReplayDiffer — side-by-side diff of two ReplayReport runs.

Wave 5e: session_replay_debuggability — side-by-side replay diff (8→9).

Compares two replay runs (ReplayReport JSON files or dicts) step-by-step:
1. Aligns steps by step_id (stable across re-runs, unlike position)
2. Highlights divergence: steps that passed in run A but failed in run B
3. Shows timing deltas: which steps got slower/faster
4. Computes a similarity score (0.0–1.0) between the two runs
5. Renders as table text or JSON for pipeline consumption

Usage:
    differ = ReplayDiffer()
    diff = differ.diff_files(Path("run_a.json"), Path("run_b.json"))
    print(diff.to_text())
    # or
    diff = differ.diff_reports(report_a_dict, report_b_dict)
    print(json.dumps(diff.to_dict(), indent=2))

Constitutional guarantees:
- Local-first: reads only from local JSON files
- Zero-ambiguity: every step has an explicit outcome (pass/fail/missing)
- Fail-open: missing steps are flagged as 'missing', not silently skipped
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Step comparison
# ---------------------------------------------------------------------------

@dataclass
class StepComparison:
    step_id: str
    action: str
    passed_a: Optional[bool]        # None = step missing from run A
    passed_b: Optional[bool]        # None = step missing from run B
    duration_ms_a: Optional[float]
    duration_ms_b: Optional[float]
    error_a: Optional[str]
    error_b: Optional[str]

    @property
    def outcome(self) -> str:
        if self.passed_a is None:
            return "only_in_b"
        if self.passed_b is None:
            return "only_in_a"
        if self.passed_a == self.passed_b:
            return "match"
        return "diverged"

    @property
    def duration_delta_ms(self) -> Optional[float]:
        if self.duration_ms_a is not None and self.duration_ms_b is not None:
            return self.duration_ms_b - self.duration_ms_a
        return None

    @property
    def is_regression(self) -> bool:
        return self.passed_a is True and self.passed_b is False

    @property
    def is_fix(self) -> bool:
        return self.passed_a is False and self.passed_b is True


# ---------------------------------------------------------------------------
# ReplayDiff
# ---------------------------------------------------------------------------

@dataclass
class ReplayDiff:
    diff_id: str
    pack_id_a: str
    pack_id_b: str
    replay_id_a: str
    replay_id_b: str
    step_comparisons: List[StepComparison] = field(default_factory=list)
    pass_rate_a: float = 0.0
    pass_rate_b: float = 0.0
    mean_score_a: float = 0.0
    mean_score_b: float = 0.0

    @property
    def similarity_score(self) -> float:
        """Fraction of steps with matching outcomes (0.0–1.0)."""
        if not self.step_comparisons:
            return 1.0
        matched = sum(1 for s in self.step_comparisons if s.outcome == "match")
        return matched / len(self.step_comparisons)

    @property
    def regressions(self) -> List[StepComparison]:
        return [s for s in self.step_comparisons if s.is_regression]

    @property
    def fixes(self) -> List[StepComparison]:
        return [s for s in self.step_comparisons if s.is_fix]

    @property
    def only_in_a(self) -> List[StepComparison]:
        return [s for s in self.step_comparisons if s.outcome == "only_in_a"]

    @property
    def only_in_b(self) -> List[StepComparison]:
        return [s for s in self.step_comparisons if s.outcome == "only_in_b"]

    def to_dict(self) -> dict:
        return {
            "diff_id": self.diff_id,
            "replay_id_a": self.replay_id_a,
            "replay_id_b": self.replay_id_b,
            "pack_id_a": self.pack_id_a,
            "pack_id_b": self.pack_id_b,
            "similarity_score": round(self.similarity_score, 4),
            "pass_rate_a": self.pass_rate_a,
            "pass_rate_b": self.pass_rate_b,
            "regressions": len(self.regressions),
            "fixes": len(self.fixes),
            "steps_only_in_a": len(self.only_in_a),
            "steps_only_in_b": len(self.only_in_b),
            "step_comparisons": [
                {
                    "step_id": s.step_id,
                    "action": s.action,
                    "outcome": s.outcome,
                    "passed_a": s.passed_a,
                    "passed_b": s.passed_b,
                    "duration_delta_ms": round(s.duration_delta_ms, 2) if s.duration_delta_ms else None,
                    "error_a": s.error_a,
                    "error_b": s.error_b,
                }
                for s in self.step_comparisons
            ],
        }

    def to_text(self) -> str:
        col = 42
        lines = [
            f"\n{'═'*90}",
            f"  Replay Diff",
            f"  A: {self.replay_id_a}  (pass_rate={self.pass_rate_a:.1%})",
            f"  B: {self.replay_id_b}  (pass_rate={self.pass_rate_b:.1%})",
            f"  Similarity: {self.similarity_score:.1%}  |  "
            f"Regressions: {len(self.regressions)}  |  Fixes: {len(self.fixes)}",
            f"{'─'*90}",
            f"  {'Step':<20} {'Action':<{col}} {'A':>6}  {'B':>6}  {'Δms':>8}  Outcome",
            f"{'─'*90}",
        ]

        for s in self.step_comparisons:
            a_sym = "✓" if s.passed_a else ("✗" if s.passed_a is False else "?")
            b_sym = "✓" if s.passed_b else ("✗" if s.passed_b is False else "?")
            delta = f"{s.duration_delta_ms:+.0f}" if s.duration_delta_ms is not None else "  —"
            marker = " !!" if s.is_regression else " ++" if s.is_fix else "   "
            action_trunc = (s.action or "")[:col]
            lines.append(
                f"  {s.step_id[:18]:<20} {action_trunc:<{col}} {a_sym:>6}  {b_sym:>6}  {delta:>8}{marker}"
            )

        lines += [
            f"{'═'*90}\n",
        ]
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# ReplayDiffer
# ---------------------------------------------------------------------------

class ReplayDiffer:
    """
    Compare two ReplayReport runs step-by-step.

    Usage:
        differ = ReplayDiffer()
        diff = differ.diff_files(Path("report_a.json"), Path("report_b.json"))
        print(diff.to_text())
    """

    def diff_files(self, path_a: Path, path_b: Path) -> ReplayDiff:
        """Load two ReplayReport JSON files and diff them."""
        report_a = json.loads(Path(path_a).read_text(encoding="utf-8"))
        report_b = json.loads(Path(path_b).read_text(encoding="utf-8"))
        return self.diff_reports(report_a, report_b)

    def diff_reports(
        self,
        report_a: Dict[str, Any],
        report_b: Dict[str, Any],
    ) -> ReplayDiff:
        """Diff two ReplayReport dicts (as produced by ReplayReport.to_dict())."""
        from uuid import uuid4
        steps_a = {s["step_id"]: s for s in report_a.get("steps", [])}
        steps_b = {s["step_id"]: s for s in report_b.get("steps", [])}

        all_step_ids = list(steps_a.keys()) + [k for k in steps_b if k not in steps_a]

        comparisons = []
        for sid in all_step_ids:
            sa = steps_a.get(sid)
            sb = steps_b.get(sid)
            comparisons.append(StepComparison(
                step_id=sid,
                action=(sa or sb or {}).get("action", ""),
                passed_a=sa["passed"] if sa else None,
                passed_b=sb["passed"] if sb else None,
                duration_ms_a=sa.get("duration_ms") if sa else None,
                duration_ms_b=sb.get("duration_ms") if sb else None,
                error_a=sa.get("error") if sa else None,
                error_b=sb.get("error") if sb else None,
            ))

        return ReplayDiff(
            diff_id=str(uuid4()),
            pack_id_a=report_a.get("pack_id", "?"),
            pack_id_b=report_b.get("pack_id", "?"),
            replay_id_a=report_a.get("replay_id", "?"),
            replay_id_b=report_b.get("replay_id", "?"),
            step_comparisons=comparisons,
            pass_rate_a=report_a.get("pass_rate", 0.0),
            pass_rate_b=report_b.get("pass_rate", 0.0),
            mean_score_a=report_a.get("mean_score", 0.0),
            mean_score_b=report_b.get("mean_score", 0.0),
        )
