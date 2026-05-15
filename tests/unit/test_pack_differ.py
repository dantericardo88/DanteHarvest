"""Tests for harvest_distill.packs.pack_differ."""
import pytest
from pathlib import Path


def _make_pack(pack_id="wf-001", title="Test Pack", steps=None):
    return {
        "pack_id": pack_id,
        "pack_type": "workflowPack",
        "title": title,
        "steps": steps or [
            {"id": "step-1", "action": "navigate https://example.com"},
            {"id": "step-2", "action": "click #btn"},
        ],
    }


def test_diff_identical_packs(tmp_path):
    from harvest_distill.packs.pack_differ import PackDiffer
    differ = PackDiffer(changelog_dir=tmp_path / "changelogs")
    pack = _make_pack()
    diff = differ.diff(pack, pack)
    assert not diff.has_changes
    assert diff.summary == "no changes"


def test_diff_title_changed(tmp_path):
    from harvest_distill.packs.pack_differ import PackDiffer
    differ = PackDiffer(changelog_dir=tmp_path / "changelogs")
    old = _make_pack(title="Old Title")
    new = _make_pack(title="New Title")
    diff = differ.diff(old, new)
    assert diff.has_changes
    assert any(fc.field_path == "title" for fc in diff.field_changes)


def test_diff_step_added(tmp_path):
    from harvest_distill.packs.pack_differ import PackDiffer
    differ = PackDiffer(changelog_dir=tmp_path / "changelogs")
    old = _make_pack(steps=[{"id": "s1", "action": "click #a"}])
    new = _make_pack(steps=[
        {"id": "s1", "action": "click #a"},
        {"id": "s2", "action": "fill #form"},
    ])
    diff = differ.diff(old, new)
    assert any(sc.step_id == "s2" and sc.change_type == "added" for sc in diff.step_changes)


def test_diff_step_removed(tmp_path):
    from harvest_distill.packs.pack_differ import PackDiffer
    differ = PackDiffer(changelog_dir=tmp_path / "changelogs")
    old = _make_pack(steps=[
        {"id": "s1", "action": "click #a"},
        {"id": "s2", "action": "fill #form"},
    ])
    new = _make_pack(steps=[{"id": "s1", "action": "click #a"}])
    diff = differ.diff(old, new)
    assert any(sc.step_id == "s2" and sc.change_type == "removed" for sc in diff.step_changes)


def test_diff_step_modified_action(tmp_path):
    from harvest_distill.packs.pack_differ import PackDiffer
    differ = PackDiffer(changelog_dir=tmp_path / "changelogs")
    old = _make_pack(steps=[{"id": "s1", "action": "click #old"}])
    new = _make_pack(steps=[{"id": "s1", "action": "click #new"}])
    diff = differ.diff(old, new)
    assert any(sc.step_id == "s1" and sc.change_type == "modified" for sc in diff.step_changes)


def test_diff_summary_text(tmp_path):
    from harvest_distill.packs.pack_differ import PackDiffer
    differ = PackDiffer(changelog_dir=tmp_path / "changelogs")
    old = _make_pack(title="A", steps=[{"id": "s1", "action": "click"}])
    new = _make_pack(title="B", steps=[
        {"id": "s1", "action": "click"},
        {"id": "s2", "action": "fill"},
    ])
    diff = differ.diff(old, new)
    assert "field" in diff.summary or "step" in diff.summary


def test_diff_to_text(tmp_path):
    from harvest_distill.packs.pack_differ import PackDiffer
    differ = PackDiffer(changelog_dir=tmp_path / "changelogs")
    old = _make_pack(title="X")
    new = _make_pack(title="Y")
    diff = differ.diff(old, new, old_label="v1", new_label="v2")
    text = diff.to_text()
    assert "v1" in text
    assert "v2" in text
    assert "title" in text


def test_diff_to_dict(tmp_path):
    from harvest_distill.packs.pack_differ import PackDiffer
    differ = PackDiffer(changelog_dir=tmp_path / "changelogs")
    diff = differ.diff(_make_pack(), _make_pack(title="New"))
    d = diff.to_dict()
    assert "diff_id" in d
    assert "field_changes" in d


def test_record_changelog(tmp_path):
    from harvest_distill.packs.pack_differ import PackDiffer
    differ = PackDiffer(changelog_dir=tmp_path / "changelogs")
    diff = differ.diff(_make_pack(), _make_pack(title="Changed"))
    entry = differ.record_changelog(diff)
    assert entry.pack_id == "wf-001"
    history = differ.changelog_for("wf-001")
    assert len(history) == 1
    assert history[0].changelog_id == entry.changelog_id


def test_changelog_append_only(tmp_path):
    from harvest_distill.packs.pack_differ import PackDiffer
    differ = PackDiffer(changelog_dir=tmp_path / "changelogs")
    old = _make_pack()
    differ.record_changelog(differ.diff(old, _make_pack(title="v2")))
    differ.record_changelog(differ.diff(old, _make_pack(title="v3")))
    history = differ.changelog_for("wf-001")
    assert len(history) == 2
