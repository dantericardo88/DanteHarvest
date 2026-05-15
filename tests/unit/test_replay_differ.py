"""Tests for harvest_index.registry.replay_differ."""
import json
import pytest
from pathlib import Path


def _make_report(replay_id="r1", pack_id="p1", steps=None, pass_rate=1.0):
    return {
        "replay_id": replay_id,
        "pack_id": pack_id,
        "pass_rate": pass_rate,
        "mean_score": 0.9,
        "steps": steps or [
            {"step_id": "s1", "action": "navigate", "passed": True, "duration_ms": 100.0},
            {"step_id": "s2", "action": "click #btn", "passed": True, "duration_ms": 50.0},
        ],
    }


def test_diff_identical_reports():
    from harvest_index.registry.replay_differ import ReplayDiffer
    differ = ReplayDiffer()
    report = _make_report()
    diff = differ.diff_reports(report, report)
    assert diff.similarity_score == pytest.approx(1.0)
    assert len(diff.regressions) == 0


def test_diff_regression_detected():
    from harvest_index.registry.replay_differ import ReplayDiffer
    differ = ReplayDiffer()
    report_a = _make_report(steps=[
        {"step_id": "s1", "action": "click", "passed": True, "duration_ms": 100.0},
    ])
    report_b = _make_report(steps=[
        {"step_id": "s1", "action": "click", "passed": False, "duration_ms": 200.0, "error": "timeout"},
    ])
    diff = differ.diff_reports(report_a, report_b)
    assert len(diff.regressions) == 1
    assert diff.regressions[0].step_id == "s1"


def test_diff_fix_detected():
    from harvest_index.registry.replay_differ import ReplayDiffer
    differ = ReplayDiffer()
    report_a = _make_report(steps=[
        {"step_id": "s1", "action": "click", "passed": False, "duration_ms": 100.0},
    ])
    report_b = _make_report(steps=[
        {"step_id": "s1", "action": "click", "passed": True, "duration_ms": 80.0},
    ])
    diff = differ.diff_reports(report_a, report_b)
    assert len(diff.fixes) == 1
    assert diff.fixes[0].step_id == "s1"


def test_diff_step_only_in_a():
    from harvest_index.registry.replay_differ import ReplayDiffer
    differ = ReplayDiffer()
    report_a = _make_report(steps=[
        {"step_id": "s1", "action": "click", "passed": True, "duration_ms": 50.0},
        {"step_id": "s2", "action": "fill", "passed": True, "duration_ms": 30.0},
    ])
    report_b = _make_report(steps=[
        {"step_id": "s1", "action": "click", "passed": True, "duration_ms": 50.0},
    ])
    diff = differ.diff_reports(report_a, report_b)
    assert any(s.step_id == "s2" and s.outcome == "only_in_a" for s in diff.step_comparisons)


def test_diff_step_only_in_b():
    from harvest_index.registry.replay_differ import ReplayDiffer
    differ = ReplayDiffer()
    report_a = _make_report(steps=[
        {"step_id": "s1", "action": "click", "passed": True, "duration_ms": 50.0},
    ])
    report_b = _make_report(steps=[
        {"step_id": "s1", "action": "click", "passed": True, "duration_ms": 50.0},
        {"step_id": "s3", "action": "submit", "passed": True, "duration_ms": 20.0},
    ])
    diff = differ.diff_reports(report_a, report_b)
    assert any(s.step_id == "s3" and s.outcome == "only_in_b" for s in diff.step_comparisons)


def test_diff_duration_delta():
    from harvest_index.registry.replay_differ import ReplayDiffer
    differ = ReplayDiffer()
    report_a = _make_report(steps=[
        {"step_id": "s1", "action": "nav", "passed": True, "duration_ms": 100.0},
    ])
    report_b = _make_report(steps=[
        {"step_id": "s1", "action": "nav", "passed": True, "duration_ms": 150.0},
    ])
    diff = differ.diff_reports(report_a, report_b)
    assert diff.step_comparisons[0].duration_delta_ms == pytest.approx(50.0)


def test_diff_similarity_partial():
    from harvest_index.registry.replay_differ import ReplayDiffer
    differ = ReplayDiffer()
    report_a = _make_report(steps=[
        {"step_id": "s1", "action": "a", "passed": True, "duration_ms": 10.0},
        {"step_id": "s2", "action": "b", "passed": True, "duration_ms": 10.0},
    ])
    report_b = _make_report(steps=[
        {"step_id": "s1", "action": "a", "passed": True, "duration_ms": 10.0},
        {"step_id": "s2", "action": "b", "passed": False, "duration_ms": 10.0},
    ])
    diff = differ.diff_reports(report_a, report_b)
    assert 0.0 < diff.similarity_score < 1.0


def test_diff_to_text_contains_regression_marker():
    from harvest_index.registry.replay_differ import ReplayDiffer
    differ = ReplayDiffer()
    report_a = _make_report(steps=[
        {"step_id": "s1", "action": "click", "passed": True, "duration_ms": 100.0},
    ])
    report_b = _make_report(steps=[
        {"step_id": "s1", "action": "click", "passed": False, "duration_ms": 100.0},
    ])
    diff = differ.diff_reports(report_a, report_b)
    text = diff.to_text()
    assert "!!" in text
    assert "s1" in text


def test_diff_to_text_contains_fix_marker():
    from harvest_index.registry.replay_differ import ReplayDiffer
    differ = ReplayDiffer()
    report_a = _make_report(steps=[
        {"step_id": "s1", "action": "click", "passed": False, "duration_ms": 100.0},
    ])
    report_b = _make_report(steps=[
        {"step_id": "s1", "action": "click", "passed": True, "duration_ms": 100.0},
    ])
    diff = differ.diff_reports(report_a, report_b)
    text = diff.to_text()
    assert "++" in text


def test_diff_to_dict_structure():
    from harvest_index.registry.replay_differ import ReplayDiffer
    differ = ReplayDiffer()
    diff = differ.diff_reports(_make_report(replay_id="a"), _make_report(replay_id="b"))
    d = diff.to_dict()
    assert "diff_id" in d
    assert "similarity_score" in d
    assert "regressions" in d
    assert "step_comparisons" in d


def test_diff_files(tmp_path):
    from harvest_index.registry.replay_differ import ReplayDiffer
    differ = ReplayDiffer()
    report_a = _make_report(replay_id="file-a")
    report_b = _make_report(replay_id="file-b")
    path_a = tmp_path / "a.json"
    path_b = tmp_path / "b.json"
    path_a.write_text(json.dumps(report_a))
    path_b.write_text(json.dumps(report_b))
    diff = differ.diff_files(path_a, path_b)
    assert diff.replay_id_a == "file-a"
    assert diff.replay_id_b == "file-b"


def test_step_comparison_outcome_match():
    from harvest_index.registry.replay_differ import StepComparison
    sc = StepComparison(
        step_id="s1", action="click",
        passed_a=True, passed_b=True,
        duration_ms_a=100.0, duration_ms_b=110.0,
        error_a=None, error_b=None,
    )
    assert sc.outcome == "match"
    assert not sc.is_regression
    assert not sc.is_fix


def test_step_comparison_outcome_diverged():
    from harvest_index.registry.replay_differ import StepComparison
    sc = StepComparison(
        step_id="s1", action="click",
        passed_a=True, passed_b=False,
        duration_ms_a=100.0, duration_ms_b=200.0,
        error_a=None, error_b="timeout",
    )
    assert sc.outcome == "diverged"
    assert sc.is_regression
    assert not sc.is_fix
