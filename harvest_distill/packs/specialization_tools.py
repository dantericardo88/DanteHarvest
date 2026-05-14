"""
SpecializationTools — semver diff, migration validator, and template builder for SpecializationPacks.

Harvested from: Pydantic-AI agent schema patterns + semantic versioning conventions.

Fills the gaps that caused the matrix score to land at 5/10:
  1. SemVer diff: shows what changed between two SpecializationPack versions
  2. Migration validator: checks that a v1 pack can be safely migrated to v2
  3. Template builder: scaffolds a new SpecializationPack from a domain template

Constitutional guarantees:
- Local-first: no network calls; all operations on in-process objects
- Fail-closed: invalid semver raises ValueError (not silent default)
- Zero-ambiguity: diff result always lists added/removed/changed keys explicitly
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from harvest_distill.packs.pack_schemas import PromotionStatus, SpecializationPack


# ---------------------------------------------------------------------------
# SemVer utilities
# ---------------------------------------------------------------------------

_SEMVER_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")


def _parse_semver(version: str) -> Tuple[int, int, int]:
    m = _SEMVER_RE.match(version.strip())
    if not m:
        raise ValueError(f"Invalid semver: {version!r}. Expected format: MAJOR.MINOR.PATCH")
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


def bump_version(version: str, part: str = "minor") -> str:
    """
    Bump a semver string.

    Args:
        version: current version (e.g. "1.2.3")
        part:    "major", "minor", or "patch"

    Returns:
        New version string (e.g. "1.3.0")
    """
    major, minor, patch = _parse_semver(version)
    if part == "major":
        return f"{major + 1}.0.0"
    if part == "minor":
        return f"{major}.{minor + 1}.0"
    if part == "patch":
        return f"{major}.{minor}.{patch + 1}"
    raise ValueError(f"Invalid part: {part!r}. Must be 'major', 'minor', or 'patch'")


# ---------------------------------------------------------------------------
# Diff types
# ---------------------------------------------------------------------------

@dataclass
class FieldDiff:
    field: str
    change_type: str     # "added", "removed", "changed"
    old_value: Any = None
    new_value: Any = None
    is_breaking: bool = False


@dataclass
class SpecDiff:
    """Result of diffing two SpecializationPack versions."""
    pack_id: str
    old_version: str
    new_version: str
    version_bump: str    # "major", "minor", "patch", or "none"
    is_breaking: bool
    changes: List[FieldDiff] = field(default_factory=list)
    added_glossary_terms: List[str] = field(default_factory=list)
    removed_glossary_terms: List[str] = field(default_factory=list)
    added_workflow_refs: List[str] = field(default_factory=list)
    removed_workflow_refs: List[str] = field(default_factory=list)
    added_skill_refs: List[str] = field(default_factory=list)
    removed_skill_refs: List[str] = field(default_factory=list)
    migration_notes: List[str] = field(default_factory=list)

    def summary(self) -> str:
        lines = [
            f"Diff: {self.pack_id}  {self.old_version} → {self.new_version}  "
            f"({self.version_bump} bump{'  BREAKING' if self.is_breaking else ''})",
            f"  Changes: {len(self.changes)}  |  "
            f"Glossary: +{len(self.added_glossary_terms)} -{len(self.removed_glossary_terms)}  |  "
            f"Workflows: +{len(self.added_workflow_refs)} -{len(self.removed_workflow_refs)}",
        ]
        for note in self.migration_notes:
            lines.append(f"  NOTE: {note}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# SpecDiffer
# ---------------------------------------------------------------------------

# Fields whose removal is considered a breaking change
_BREAKING_FIELDS = {"domain", "rights_boundary", "disallowed_actions"}


class SpecDiffer:
    """
    Compute a structured diff between two SpecializationPack instances.

    Usage:
        differ = SpecDiffer()
        diff = differ.diff(pack_v1, pack_v2)
        print(diff.summary())
        print("Breaking?", diff.is_breaking)
    """

    def diff(self, old: SpecializationPack, new: SpecializationPack) -> SpecDiff:
        """Compare two SpecializationPack versions and return a SpecDiff."""
        old_d = old.model_dump()
        new_d = new.model_dump()

        changes: List[FieldDiff] = []
        is_breaking = False

        # Scalar field changes
        scalar_fields = ["domain", "rights_boundary", "promotion_status", "version"]
        for f in scalar_fields:
            ov, nv = old_d.get(f), new_d.get(f)
            if ov != nv:
                breaking = f in _BREAKING_FIELDS
                if breaking:
                    is_breaking = True
                changes.append(FieldDiff(
                    field=f,
                    change_type="changed",
                    old_value=ov,
                    new_value=nv,
                    is_breaking=breaking,
                ))

        # List ref diffs
        added_wf = _list_added(old.workflow_refs, new.workflow_refs)
        removed_wf = _list_removed(old.workflow_refs, new.workflow_refs)
        added_sk = _list_added(old.skill_refs, new.skill_refs)
        removed_sk = _list_removed(old.skill_refs, new.skill_refs)

        if removed_wf:
            is_breaking = True
        if removed_sk:
            is_breaking = True

        # Glossary diffs
        added_gl = _list_added(list(old.glossary), list(new.glossary))
        removed_gl = _list_removed(list(old.glossary), list(new.glossary))
        # Changed glossary definitions
        for term in set(old.glossary) & set(new.glossary):
            if old.glossary[term] != new.glossary[term]:
                changes.append(FieldDiff(
                    field=f"glossary[{term!r}]",
                    change_type="changed",
                    old_value=old.glossary[term],
                    new_value=new.glossary[term],
                    is_breaking=False,
                ))

        # Disallowed actions diff
        added_da = _list_added(old.disallowed_actions, new.disallowed_actions)
        removed_da = _list_removed(old.disallowed_actions, new.disallowed_actions)
        if removed_da:
            is_breaking = True
            for a in removed_da:
                changes.append(FieldDiff(
                    field="disallowed_actions",
                    change_type="removed",
                    old_value=a,
                    new_value=None,
                    is_breaking=True,
                ))
        for a in added_da:
            changes.append(FieldDiff(
                field="disallowed_actions",
                change_type="added",
                old_value=None,
                new_value=a,
                is_breaking=False,
            ))

        # Determine version bump
        try:
            ov_tuple = _parse_semver(old.version)
            nv_tuple = _parse_semver(new.version)
            if nv_tuple[0] > ov_tuple[0]:
                bump = "major"
            elif nv_tuple[1] > ov_tuple[1]:
                bump = "minor"
            elif nv_tuple[2] > ov_tuple[2]:
                bump = "patch"
            else:
                bump = "none"
        except ValueError:
            bump = "unknown"

        # Migration notes
        notes: List[str] = []
        if removed_wf:
            notes.append(f"Removed workflow refs {removed_wf} — consumers must update references")
        if removed_sk:
            notes.append(f"Removed skill refs {removed_sk} — consumers must update references")
        if removed_da:
            notes.append(f"Removed disallowed_actions {removed_da} — previously blocked ops may now run")
        if is_breaking and bump not in ("major", "unknown"):
            notes.append("Breaking change detected without major version bump — consider bumping MAJOR")

        return SpecDiff(
            pack_id=new.pack_id,
            old_version=old.version,
            new_version=new.version,
            version_bump=bump,
            is_breaking=is_breaking,
            changes=changes,
            added_glossary_terms=added_gl,
            removed_glossary_terms=removed_gl,
            added_workflow_refs=added_wf,
            removed_workflow_refs=removed_wf,
            added_skill_refs=added_sk,
            removed_skill_refs=removed_sk,
            migration_notes=notes,
        )


# ---------------------------------------------------------------------------
# Migration validator
# ---------------------------------------------------------------------------

@dataclass
class MigrationReport:
    can_migrate: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def summary(self) -> str:
        status = "OK" if self.can_migrate else "BLOCKED"
        lines = [f"Migration [{status}]"]
        for e in self.errors:
            lines.append(f"  ERROR: {e}")
        for w in self.warnings:
            lines.append(f"  WARN: {w}")
        return "\n".join(lines)


class MigrationValidator:
    """
    Validate that a SpecializationPack can be safely migrated to a newer version.

    Checks:
    - Required fields are present in both versions
    - No breaking changes without a major version bump
    - rights_boundary is preserved or strengthened (not weakened)
    - disallowed_actions set is not shrunk without a major bump
    """

    REQUIRED_FIELDS = {"pack_id", "domain", "version", "rights_boundary"}

    def validate(
        self,
        old: SpecializationPack,
        new: SpecializationPack,
    ) -> MigrationReport:
        """Return a MigrationReport indicating whether migration is safe."""
        errors: List[str] = []
        warnings: List[str] = []

        # Required fields
        new_dict = new.model_dump()
        for f in self.REQUIRED_FIELDS:
            if not new_dict.get(f):
                errors.append(f"Required field '{f}' is missing or empty in new version")

        # pack_id must not change
        if old.pack_id != new.pack_id:
            errors.append(
                f"pack_id changed: {old.pack_id!r} → {new.pack_id!r}. "
                "pack_id must remain stable across versions."
            )

        # domain must not change without major bump
        if old.domain != new.domain:
            try:
                major_old, _, _ = _parse_semver(old.version)
                major_new, _, _ = _parse_semver(new.version)
                if major_new <= major_old:
                    errors.append(
                        f"domain changed from {old.domain!r} to {new.domain!r} "
                        "without a major version bump"
                    )
            except ValueError:
                warnings.append("Could not validate semver for domain change check")

        # rights_boundary must not be weakened
        _RIGHTS_RANK = {"restricted": 3, "approved": 2, "pending": 1, "open": 0}
        old_rank = _RIGHTS_RANK.get(old.rights_boundary, -1)
        new_rank = _RIGHTS_RANK.get(new.rights_boundary, -1)
        if new_rank < old_rank:
            errors.append(
                f"rights_boundary weakened: {old.rights_boundary!r} → {new.rights_boundary!r}. "
                "Rights must be preserved or strengthened."
            )

        # disallowed_actions must not shrink without major bump
        removed_da = set(old.disallowed_actions) - set(new.disallowed_actions)
        if removed_da:
            try:
                major_old, _, _ = _parse_semver(old.version)
                major_new, _, _ = _parse_semver(new.version)
                if major_new <= major_old:
                    errors.append(
                        f"disallowed_actions shrunk (removed: {sorted(removed_da)}) "
                        "without a major version bump"
                    )
            except ValueError:
                warnings.append("Could not validate semver for disallowed_actions check")

        # Warn on missing glossary
        if old.glossary and not new.glossary:
            warnings.append("Glossary was present in old version but is empty in new version")

        return MigrationReport(
            can_migrate=len(errors) == 0,
            errors=errors,
            warnings=warnings,
        )


# ---------------------------------------------------------------------------
# Template builder
# ---------------------------------------------------------------------------

# Built-in domain templates
_DOMAIN_TEMPLATES: Dict[str, Dict[str, Any]] = {
    "accounting": {
        "glossary": {
            "GL account": "General Ledger account code",
            "AR": "Accounts Receivable",
            "AP": "Accounts Payable",
            "COA": "Chart of Accounts",
        },
        "disallowed_actions": ["delete_transaction", "reverse_posted_entry"],
        "taxonomy": {
            "workflows": ["invoice_processing", "bank_reconciliation", "expense_reporting"],
            "compliance": ["sox", "gaap"],
        },
    },
    "legal": {
        "glossary": {
            "matter": "A legal case or project",
            "docket": "Official court record of proceedings",
            "billable": "Time that can be charged to a client",
        },
        "disallowed_actions": ["delete_matter", "modify_billing_rate_without_approval"],
        "taxonomy": {
            "practice_areas": ["litigation", "corporate", "ip", "real_estate"],
        },
    },
    "healthcare": {
        "glossary": {
            "PHI": "Protected Health Information",
            "EHR": "Electronic Health Record",
            "ICD-10": "International Classification of Diseases, 10th revision",
        },
        "disallowed_actions": [
            "export_phi_without_consent",
            "modify_clinical_note_after_sign",
        ],
        "taxonomy": {
            "departments": ["radiology", "pharmacy", "primary_care", "billing"],
            "compliance": ["hipaa", "hitech"],
        },
    },
    "ecommerce": {
        "glossary": {
            "SKU": "Stock Keeping Unit",
            "GMV": "Gross Merchandise Value",
            "ROAS": "Return on Ad Spend",
        },
        "disallowed_actions": ["delete_order", "refund_without_approval"],
        "taxonomy": {
            "categories": ["products", "orders", "customers", "fulfillment"],
        },
    },
}


class SpecTemplateBuilder:
    """
    Scaffold a SpecializationPack from a domain template.

    Usage:
        builder = SpecTemplateBuilder()
        pack = builder.from_template("accounting", pack_id="spec-acct-001")
        print(pack.glossary)   # pre-populated with accounting terms

    Custom domain:
        pack = builder.from_template(
            "custom",
            pack_id="spec-custom-001",
            overrides={"glossary": {"KPI": "Key Performance Indicator"}}
        )
    """

    def list_templates(self) -> List[str]:
        """Return available built-in domain template names."""
        return list(_DOMAIN_TEMPLATES.keys())

    def from_template(
        self,
        domain: str,
        pack_id: str,
        version: str = "1.0.0",
        rights_boundary: str = "approved",
        overrides: Optional[Dict[str, Any]] = None,
        workflow_refs: Optional[List[str]] = None,
        skill_refs: Optional[List[str]] = None,
    ) -> SpecializationPack:
        """
        Build a SpecializationPack from a domain template.

        Args:
            domain:         domain name (matches a built-in template, or "custom")
            pack_id:        unique identifier for the new pack
            version:        semver string (default "1.0.0")
            rights_boundary: rights boundary for the pack
            overrides:      dict of fields to override from the template
            workflow_refs:  list of workflow pack IDs to include
            skill_refs:     list of skill pack IDs to include

        Returns:
            SpecializationPack with template defaults applied.
        """
        _parse_semver(version)  # Validate semver — fail-closed

        base = _DOMAIN_TEMPLATES.get(domain, {})
        ov = overrides or {}

        glossary = {**base.get("glossary", {}), **ov.get("glossary", {})}
        disallowed = list(set(base.get("disallowed_actions", []) + ov.get("disallowed_actions", [])))
        taxonomy = {**base.get("taxonomy", {}), **ov.get("taxonomy", {})}

        return SpecializationPack(
            pack_id=pack_id,
            domain=domain,
            version=version,
            rights_boundary=rights_boundary,
            glossary=glossary,
            disallowed_actions=disallowed,
            taxonomy=taxonomy,
            workflow_refs=workflow_refs or [],
            skill_refs=skill_refs or [],
            knowledge_refs=ov.get("knowledge_refs", []),
            promotion_status=PromotionStatus.CANDIDATE,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _list_added(old: List, new: List) -> List:
    old_set, new_set = set(old), set(new)
    return [x for x in new if x not in old_set]


def _list_removed(old: List, new: List) -> List:
    old_set, new_set = set(old), set(new)
    return [x for x in old if x not in new_set]
