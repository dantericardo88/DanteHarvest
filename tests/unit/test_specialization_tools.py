"""Tests for SpecializationTools — semver diff, migration validator, template builder."""

import pytest

from harvest_distill.packs.specialization_tools import (
    SpecDiffer,
    SpecDiff,
    MigrationValidator,
    MigrationReport,
    SpecTemplateBuilder,
    bump_version,
    _parse_semver,
)
from harvest_distill.packs.pack_schemas import PromotionStatus, SpecializationPack


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def make_spec(**kwargs) -> SpecializationPack:
    defaults = {
        "pack_id": "spec-001",
        "domain": "accounting",
        "version": "1.0.0",
        "rights_boundary": "approved",
        "glossary": {"GL": "General Ledger", "AR": "Accounts Receivable"},
        "workflow_refs": ["wf-001", "wf-002"],
        "skill_refs": ["sk-001"],
        "disallowed_actions": ["delete_transaction"],
        "promotion_status": PromotionStatus.CANDIDATE,
    }
    defaults.update(kwargs)
    return SpecializationPack(**defaults)


# ---------------------------------------------------------------------------
# SemVer utilities
# ---------------------------------------------------------------------------

class TestSemVer:
    def test_parse_valid(self):
        assert _parse_semver("1.2.3") == (1, 2, 3)
        assert _parse_semver("0.0.0") == (0, 0, 0)
        assert _parse_semver("10.20.30") == (10, 20, 30)

    def test_parse_invalid_raises(self):
        with pytest.raises(ValueError):
            _parse_semver("1.2")
        with pytest.raises(ValueError):
            _parse_semver("v1.2.3")
        with pytest.raises(ValueError):
            _parse_semver("1.2.3.4")

    def test_bump_patch(self):
        assert bump_version("1.2.3", "patch") == "1.2.4"

    def test_bump_minor(self):
        assert bump_version("1.2.3", "minor") == "1.3.0"

    def test_bump_major(self):
        assert bump_version("1.2.3", "major") == "2.0.0"

    def test_bump_invalid_part(self):
        with pytest.raises(ValueError):
            bump_version("1.0.0", "build")

    def test_bump_resets_lower_parts(self):
        result = bump_version("1.9.9", "minor")
        assert result == "1.10.0"


# ---------------------------------------------------------------------------
# SpecDiffer
# ---------------------------------------------------------------------------

class TestSpecDiffer:
    def setup_method(self):
        self.differ = SpecDiffer()

    def test_diff_no_changes(self):
        v1 = make_spec()
        v2 = make_spec(version="1.0.0")
        diff = self.differ.diff(v1, v2)
        assert diff.version_bump == "none"
        assert not diff.is_breaking

    def test_diff_minor_version_bump(self):
        v1 = make_spec()
        v2 = make_spec(version="1.1.0")
        diff = self.differ.diff(v1, v2)
        assert diff.version_bump == "minor"

    def test_diff_major_version_bump(self):
        v1 = make_spec()
        v2 = make_spec(version="2.0.0")
        diff = self.differ.diff(v1, v2)
        assert diff.version_bump == "major"

    def test_diff_patch_version_bump(self):
        v1 = make_spec()
        v2 = make_spec(version="1.0.1")
        diff = self.differ.diff(v1, v2)
        assert diff.version_bump == "patch"

    def test_diff_added_workflow_ref(self):
        v1 = make_spec(workflow_refs=["wf-001"])
        v2 = make_spec(workflow_refs=["wf-001", "wf-002"], version="1.1.0")
        diff = self.differ.diff(v1, v2)
        assert "wf-002" in diff.added_workflow_refs
        assert not diff.is_breaking

    def test_diff_removed_workflow_ref_is_breaking(self):
        v1 = make_spec(workflow_refs=["wf-001", "wf-002"])
        v2 = make_spec(workflow_refs=["wf-001"], version="1.1.0")
        diff = self.differ.diff(v1, v2)
        assert "wf-002" in diff.removed_workflow_refs
        assert diff.is_breaking

    def test_diff_removed_skill_ref_is_breaking(self):
        v1 = make_spec(skill_refs=["sk-001", "sk-002"])
        v2 = make_spec(skill_refs=["sk-001"], version="1.1.0")
        diff = self.differ.diff(v1, v2)
        assert diff.is_breaking

    def test_diff_added_glossary_term(self):
        v1 = make_spec(glossary={"GL": "General Ledger"})
        v2 = make_spec(glossary={"GL": "General Ledger", "AR": "Accounts Receivable"}, version="1.1.0")
        diff = self.differ.diff(v1, v2)
        assert "AR" in diff.added_glossary_terms

    def test_diff_removed_glossary_term(self):
        v1 = make_spec(glossary={"GL": "General Ledger", "AR": "Accounts Receivable"})
        v2 = make_spec(glossary={"GL": "General Ledger"}, version="1.1.0")
        diff = self.differ.diff(v1, v2)
        assert "AR" in diff.removed_glossary_terms

    def test_diff_changed_glossary_definition(self):
        v1 = make_spec(glossary={"GL": "Old definition"})
        v2 = make_spec(glossary={"GL": "Updated definition"}, version="1.1.0")
        diff = self.differ.diff(v1, v2)
        field_names = [c.field for c in diff.changes]
        assert any("glossary" in f for f in field_names)

    def test_diff_removed_disallowed_action_is_breaking(self):
        v1 = make_spec(disallowed_actions=["delete_transaction", "reverse_entry"])
        v2 = make_spec(disallowed_actions=["delete_transaction"], version="1.1.0")
        diff = self.differ.diff(v1, v2)
        assert diff.is_breaking

    def test_diff_migration_notes_for_breaking(self):
        v1 = make_spec(workflow_refs=["wf-001", "wf-002"])
        v2 = make_spec(workflow_refs=["wf-001"], version="1.1.0")
        diff = self.differ.diff(v1, v2)
        assert len(diff.migration_notes) > 0

    def test_diff_summary_no_error(self):
        v1 = make_spec()
        v2 = make_spec(version="1.1.0", glossary={"GL": "General Ledger"})
        diff = self.differ.diff(v1, v2)
        summary = diff.summary()
        assert "spec-001" in summary

    def test_diff_pack_id_in_result(self):
        v1 = make_spec()
        v2 = make_spec(version="1.1.0")
        diff = self.differ.diff(v1, v2)
        assert diff.pack_id == "spec-001"


# ---------------------------------------------------------------------------
# MigrationValidator
# ---------------------------------------------------------------------------

class TestMigrationValidator:
    def setup_method(self):
        self.validator = MigrationValidator()

    def test_valid_migration_passes(self):
        v1 = make_spec()
        v2 = make_spec(version="1.1.0", glossary={"GL": "GL", "AR": "AR", "COA": "Chart of Accounts"})
        report = self.validator.validate(v1, v2)
        assert isinstance(report, MigrationReport)
        assert report.can_migrate

    def test_pack_id_change_fails(self):
        v1 = make_spec(pack_id="spec-001")
        v2 = make_spec(pack_id="spec-999", version="2.0.0")
        report = self.validator.validate(v1, v2)
        assert not report.can_migrate
        assert any("pack_id" in e for e in report.errors)

    def test_rights_weakening_fails(self):
        v1 = make_spec(rights_boundary="restricted")
        v2 = make_spec(rights_boundary="pending", version="2.0.0")
        report = self.validator.validate(v1, v2)
        assert not report.can_migrate
        assert any("rights" in e.lower() for e in report.errors)

    def test_domain_change_without_major_bump_fails(self):
        v1 = make_spec(domain="accounting")
        v2 = make_spec(domain="legal", version="1.1.0")
        report = self.validator.validate(v1, v2)
        assert not report.can_migrate

    def test_domain_change_with_major_bump_passes(self):
        v1 = make_spec(domain="accounting")
        v2 = make_spec(domain="legal", version="2.0.0")
        report = self.validator.validate(v1, v2)
        assert report.can_migrate

    def test_disallowed_actions_shrink_without_major_fails(self):
        v1 = make_spec(disallowed_actions=["delete_transaction", "reverse_entry"])
        v2 = make_spec(disallowed_actions=["delete_transaction"], version="1.1.0")
        report = self.validator.validate(v1, v2)
        assert not report.can_migrate

    def test_disallowed_actions_shrink_with_major_passes(self):
        v1 = make_spec(disallowed_actions=["delete_transaction", "reverse_entry"])
        v2 = make_spec(disallowed_actions=["delete_transaction"], version="2.0.0")
        report = self.validator.validate(v1, v2)
        assert report.can_migrate

    def test_missing_glossary_is_warning_not_error(self):
        v1 = make_spec(glossary={"GL": "General Ledger"})
        v2 = make_spec(glossary={}, version="1.1.0")
        report = self.validator.validate(v1, v2)
        # Empty glossary is a warning, not a blocker
        assert len(report.warnings) > 0

    def test_summary_no_error(self):
        v1 = make_spec()
        v2 = make_spec(version="1.1.0")
        report = self.validator.validate(v1, v2)
        summary = report.summary()
        assert "Migration" in summary


# ---------------------------------------------------------------------------
# SpecTemplateBuilder
# ---------------------------------------------------------------------------

class TestSpecTemplateBuilder:
    def setup_method(self):
        self.builder = SpecTemplateBuilder()

    def test_list_templates(self):
        templates = self.builder.list_templates()
        assert "accounting" in templates
        assert "legal" in templates
        assert "healthcare" in templates
        assert "ecommerce" in templates

    def test_from_accounting_template(self):
        pack = self.builder.from_template("accounting", pack_id="spec-acct-001")
        assert isinstance(pack, SpecializationPack)
        assert pack.domain == "accounting"
        assert pack.pack_id == "spec-acct-001"

    def test_accounting_glossary_prepopulated(self):
        pack = self.builder.from_template("accounting", pack_id="spec-acct-001")
        assert "GL account" in pack.glossary or "AR" in pack.glossary

    def test_accounting_disallowed_actions(self):
        pack = self.builder.from_template("accounting", pack_id="spec-acct-001")
        assert len(pack.disallowed_actions) > 0

    def test_healthcare_template(self):
        pack = self.builder.from_template("healthcare", pack_id="spec-hc-001")
        assert "PHI" in pack.glossary

    def test_legal_template(self):
        pack = self.builder.from_template("legal", pack_id="spec-legal-001")
        assert "matter" in pack.glossary

    def test_custom_domain_empty_defaults(self):
        pack = self.builder.from_template("custom", pack_id="spec-custom-001")
        assert pack.domain == "custom"
        # Custom domain has no template defaults
        assert isinstance(pack.glossary, dict)

    def test_overrides_merge_with_template(self):
        pack = self.builder.from_template(
            "accounting",
            pack_id="spec-acct-002",
            overrides={"glossary": {"KPI": "Key Performance Indicator"}},
        )
        # Should have both template glossary and override
        assert "KPI" in pack.glossary
        assert len(pack.glossary) > 1

    def test_invalid_semver_raises(self):
        with pytest.raises(ValueError):
            self.builder.from_template("accounting", pack_id="spec-001", version="bad")

    def test_version_default(self):
        pack = self.builder.from_template("accounting", pack_id="spec-001")
        assert pack.version == "1.0.0"

    def test_workflow_refs_passed_through(self):
        pack = self.builder.from_template(
            "accounting",
            pack_id="spec-001",
            workflow_refs=["wf-001", "wf-002"],
        )
        assert "wf-001" in pack.workflow_refs

    def test_promotion_status_candidate(self):
        pack = self.builder.from_template("accounting", pack_id="spec-001")
        assert pack.promotion_status == PromotionStatus.CANDIDATE

    def test_rights_boundary_default(self):
        pack = self.builder.from_template("accounting", pack_id="spec-001")
        assert pack.rights_boundary == "approved"

    def test_ecommerce_template(self):
        pack = self.builder.from_template("ecommerce", pack_id="spec-ec-001")
        assert "SKU" in pack.glossary
