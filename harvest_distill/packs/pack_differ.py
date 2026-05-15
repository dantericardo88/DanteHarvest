"""
PackDiffer — structural diff and changelog for WorkflowPack versions.

Wave 4e: workflow_diffing — `harvest diff` CLI + changelog history (7→9).

Compares two versions of any AnyPack and produces:
1. PackDiff: structured diff with added/removed/changed fields and steps
2. ChangelogEntry: human-readable summary saved to a JSONL changelog
3. `harvest diff` CLI integration via cmd_pack_diff()

Design:
- Steps are compared by step.id (stable across edits), not by position.
- Field-level diff is recursive for nested dicts.
- Changelog is append-only JSONL at registry/changelogs/{pack_id}.jsonl.
- Output formats: table (default), json, unified-diff-style text.

Constitutional guarantees:
- Local-first: reads only from local pack storage, no network calls
- Append-only: changelog entries are never modified, only appended
- Zero-ambiguity: every field change is explicit (old_value, new_value)
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4


# ---------------------------------------------------------------------------
# Diff primitives
# ---------------------------------------------------------------------------

@dataclass
class FieldChange:
    field_path: str         # dotted path, e.g. "title" or "eval_summary.replay_pass_rate"
    old_value: Any
    new_value: Any
    change_type: str        # "added" | "removed" | "modified"


@dataclass
class StepChange:
    step_id: str
    change_type: str        # "added" | "removed" | "modified"
    old_action: Optional[str] = None
    new_action: Optional[str] = None
    field_changes: List[FieldChange] = field(default_factory=list)


@dataclass
class PackDiff:
    diff_id: str
    pack_id: str
    pack_type: str
    old_version_label: str
    new_version_label: str
    field_changes: List[FieldChange] = field(default_factory=list)
    step_changes: List[StepChange] = field(default_factory=list)
    diffed_at: float = field(default_factory=time.time)

    @property
    def has_changes(self) -> bool:
        return bool(self.field_changes or self.step_changes)

    @property
    def summary(self) -> str:
        parts = []
        if self.field_changes:
            parts.append(f"{len(self.field_changes)} field(s) changed")
        added = sum(1 for s in self.step_changes if s.change_type == "added")
        removed = sum(1 for s in self.step_changes if s.change_type == "removed")
        modified = sum(1 for s in self.step_changes if s.change_type == "modified")
        if added:
            parts.append(f"{added} step(s) added")
        if removed:
            parts.append(f"{removed} step(s) removed")
        if modified:
            parts.append(f"{modified} step(s) modified")
        return "; ".join(parts) if parts else "no changes"

    def to_dict(self) -> dict:
        return {
            "diff_id": self.diff_id,
            "pack_id": self.pack_id,
            "pack_type": self.pack_type,
            "old_version_label": self.old_version_label,
            "new_version_label": self.new_version_label,
            "summary": self.summary,
            "diffed_at": self.diffed_at,
            "field_changes": [asdict(fc) for fc in self.field_changes],
            "step_changes": [asdict(sc) for sc in self.step_changes],
        }

    def to_text(self) -> str:
        """Render as human-readable unified-diff-style text."""
        lines = [
            f"--- {self.old_version_label}",
            f"+++ {self.new_version_label}",
            f"@@ pack_id={self.pack_id} ({self.pack_type}) @@",
            "",
        ]
        for fc in self.field_changes:
            prefix = "+" if fc.change_type == "added" else "-" if fc.change_type == "removed" else "~"
            lines.append(f"  {prefix} {fc.field_path}: {json.dumps(fc.old_value)} → {json.dumps(fc.new_value)}")

        if self.step_changes:
            lines.append("")
            lines.append("  Steps:")
            for sc in self.step_changes:
                if sc.change_type == "added":
                    lines.append(f"  + [{sc.step_id}] {sc.new_action}")
                elif sc.change_type == "removed":
                    lines.append(f"  - [{sc.step_id}] {sc.old_action}")
                else:
                    lines.append(f"  ~ [{sc.step_id}] {sc.old_action} → {sc.new_action}")
                    for fc in sc.field_changes:
                        lines.append(f"      ~ {fc.field_path}: {json.dumps(fc.old_value)} → {json.dumps(fc.new_value)}")

        lines.append("")
        lines.append(f"  Summary: {self.summary}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# ChangelogEntry
# ---------------------------------------------------------------------------

@dataclass
class ChangelogEntry:
    changelog_id: str
    pack_id: str
    recorded_at: float
    old_version_label: str
    new_version_label: str
    summary: str
    diff_id: str
    author: str = "harvest"

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# PackDiffer
# ---------------------------------------------------------------------------

class PackDiffer:
    """
    Compute structural diffs between two pack JSON dicts.

    Usage:
        differ = PackDiffer(changelog_dir=Path("registry/changelogs"))
        diff = differ.diff(old_pack_dict, new_pack_dict, pack_id="wf-abc")
        differ.record_changelog(diff)
        print(diff.to_text())
    """

    # Fields treated as step lists for step-level diffing
    _STEP_FIELDS = ("steps",)
    # Fields excluded from field-level diff (handled separately or internal)
    _EXCLUDE_FIELDS = frozenset({"steps", "pack_id", "pack_type"})

    def __init__(self, changelog_dir: Optional[Path] = None):
        self._changelog_dir = Path(changelog_dir) if changelog_dir else Path("registry/changelogs")
        self._changelog_dir.mkdir(parents=True, exist_ok=True)

    def diff(
        self,
        old_pack: Dict[str, Any],
        new_pack: Dict[str, Any],
        pack_id: Optional[str] = None,
        old_label: str = "old",
        new_label: str = "new",
    ) -> PackDiff:
        pid = pack_id or old_pack.get("pack_id", new_pack.get("pack_id", "unknown"))
        pack_type = old_pack.get("pack_type", new_pack.get("pack_type", "unknown"))

        field_changes = self._diff_fields(old_pack, new_pack, prefix="")
        step_changes = self._diff_steps(
            old_pack.get("steps", []),
            new_pack.get("steps", []),
        )

        return PackDiff(
            diff_id=str(uuid4()),
            pack_id=pid,
            pack_type=pack_type,
            old_version_label=old_label,
            new_version_label=new_label,
            field_changes=field_changes,
            step_changes=step_changes,
        )

    def record_changelog(
        self,
        diff: PackDiff,
        author: str = "harvest",
    ) -> ChangelogEntry:
        """Append a ChangelogEntry to the pack's changelog JSONL."""
        entry = ChangelogEntry(
            changelog_id=str(uuid4()),
            pack_id=diff.pack_id,
            recorded_at=time.time(),
            old_version_label=diff.old_version_label,
            new_version_label=diff.new_version_label,
            summary=diff.summary,
            diff_id=diff.diff_id,
            author=author,
        )
        log_path = self._changelog_dir / f"{diff.pack_id}.jsonl"
        try:
            with log_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(entry.to_dict()) + "\n")
        except Exception:
            pass
        return entry

    def changelog_for(self, pack_id: str) -> List[ChangelogEntry]:
        """Return all changelog entries for a pack, oldest first."""
        log_path = self._changelog_dir / f"{pack_id}.jsonl"
        if not log_path.exists():
            return []
        entries = []
        for line in log_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                try:
                    entries.append(ChangelogEntry(**json.loads(line)))
                except Exception:
                    pass
        return entries

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _diff_fields(
        self,
        old: Dict[str, Any],
        new: Dict[str, Any],
        prefix: str,
    ) -> List[FieldChange]:
        changes = []
        all_keys = set(old) | set(new)
        for k in sorted(all_keys):
            if k in self._EXCLUDE_FIELDS and not prefix:
                continue
            path = f"{prefix}.{k}" if prefix else k
            in_old, in_new = k in old, k in new
            if not in_old:
                changes.append(FieldChange(path, None, new[k], "added"))
            elif not in_new:
                changes.append(FieldChange(path, old[k], None, "removed"))
            elif isinstance(old[k], dict) and isinstance(new[k], dict):
                changes.extend(self._diff_fields(old[k], new[k], path))
            elif old[k] != new[k]:
                changes.append(FieldChange(path, old[k], new[k], "modified"))
        return changes

    def _diff_steps(
        self,
        old_steps: List[Dict[str, Any]],
        new_steps: List[Dict[str, Any]],
    ) -> List[StepChange]:
        old_by_id = {s.get("id", str(i)): s for i, s in enumerate(old_steps)}
        new_by_id = {s.get("id", str(i)): s for i, s in enumerate(new_steps)}
        all_ids = list(old_by_id) + [k for k in new_by_id if k not in old_by_id]

        changes = []
        for sid in all_ids:
            in_old, in_new = sid in old_by_id, sid in new_by_id
            if not in_old:
                s = new_by_id[sid]
                changes.append(StepChange(sid, "added", new_action=s.get("action")))
            elif not in_new:
                s = old_by_id[sid]
                changes.append(StepChange(sid, "removed", old_action=s.get("action")))
            else:
                os_, ns = old_by_id[sid], new_by_id[sid]
                field_changes = self._diff_fields(os_, ns, prefix="")
                if field_changes:
                    changes.append(StepChange(
                        sid, "modified",
                        old_action=os_.get("action"),
                        new_action=ns.get("action"),
                        field_changes=field_changes,
                    ))
        return changes
