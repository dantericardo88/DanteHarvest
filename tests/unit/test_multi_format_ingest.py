"""
Unit tests for multi_format_ingest improvements:
- SpreadsheetLoader.get_supported_formats() always includes 'csv'
- SpreadsheetLoader.load(csv_path) works without openpyxl
- SpreadsheetLoader graceful degradation when openpyxl missing + CSV fallback
- IngestCapabilities.get_available() returns dict with csv=True
- IngestCapabilities.get_missing_deps() returns list
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path
from unittest.mock import patch

import pytest

from harvest_acquire.loaders.spreadsheet_loader import (
    IngestCapabilities,
    SpreadsheetLoader,
)
from harvest_core.control.exceptions import NormalizationError


# ---------------------------------------------------------------------------
# SpreadsheetLoader.get_supported_formats
# ---------------------------------------------------------------------------

class TestGetSupportedFormats:
    def test_csv_always_present(self):
        loader = SpreadsheetLoader()
        assert "csv" in loader.get_supported_formats()

    def test_csv_present_when_openpyxl_missing(self):
        with patch.dict(sys.modules, {"openpyxl": None}):
            # Reload the module so the flag re-evaluates
            import importlib
            import harvest_acquire.loaders.spreadsheet_loader as mod
            importlib.reload(mod)
            loader = mod.SpreadsheetLoader()
            assert "csv" in loader.get_supported_formats()
            # Reload again to restore
            importlib.reload(mod)

    def test_returns_list(self):
        loader = SpreadsheetLoader()
        result = loader.get_supported_formats()
        assert isinstance(result, list)

    def test_xlsx_present_when_openpyxl_available(self):
        # openpyxl IS available in test env
        try:
            import openpyxl  # noqa: F401
            loader = SpreadsheetLoader()
            assert "xlsx" in loader.get_supported_formats()
        except ImportError:
            pytest.skip("openpyxl not installed")

    def test_formats_are_strings(self):
        loader = SpreadsheetLoader()
        for fmt in loader.get_supported_formats():
            assert isinstance(fmt, str)


# ---------------------------------------------------------------------------
# SpreadsheetLoader.load — CSV path works without openpyxl
# ---------------------------------------------------------------------------

class TestLoadCsvWithoutOpenpyxl:
    def test_load_csv_works_without_openpyxl(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("col1,col2\nfoo,bar\n", encoding="utf-8")
        # Simulate openpyxl absent by patching sys.modules
        with patch.dict(sys.modules, {"openpyxl": None}):
            loader = SpreadsheetLoader()
            docs = loader.load(f)
        assert len(docs) == 1
        assert docs[0].format == "csv"
        assert "col1" in docs[0].markdown

    def test_load_csv_data_correct_without_openpyxl(self, tmp_path):
        f = tmp_path / "report.csv"
        f.write_text("name,value\nalpha,1\nbeta,2\n", encoding="utf-8")
        with patch.dict(sys.modules, {"openpyxl": None}):
            loader = SpreadsheetLoader()
            docs = loader.load(f)
        md = docs[0].markdown
        assert "| name | value |" in md
        assert "| alpha | 1 |" in md


# ---------------------------------------------------------------------------
# SpreadsheetLoader graceful degradation — xlsx without openpyxl
# ---------------------------------------------------------------------------

class TestGracefulDegradationXlsx:
    def test_xlsx_without_openpyxl_no_csv_raises_normalization_error(self, tmp_path):
        f = tmp_path / "report.xlsx"
        f.write_bytes(b"fake xlsx content")
        with patch.dict(sys.modules, {"openpyxl": None}):
            import importlib
            import harvest_acquire.loaders.spreadsheet_loader as mod
            importlib.reload(mod)
            loader = mod.SpreadsheetLoader()
            with pytest.raises(mod.NormalizationError, match="openpyxl"):
                loader.load(f)
            importlib.reload(mod)

    def test_xlsx_without_openpyxl_csv_fallback_used(self, tmp_path):
        xlsx_path = tmp_path / "report.xlsx"
        xlsx_path.write_bytes(b"fake xlsx content")
        csv_path = tmp_path / "report.csv"
        csv_path.write_text("a,b\n1,2\n", encoding="utf-8")
        with patch.dict(sys.modules, {"openpyxl": None}):
            import importlib
            import harvest_acquire.loaders.spreadsheet_loader as mod
            importlib.reload(mod)
            loader = mod.SpreadsheetLoader()
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                docs = loader.load(xlsx_path)
            assert len(docs) == 1
            assert "a" in docs[0].markdown
            # A warning should have been emitted
            assert any("openpyxl" in str(warning.message).lower() for warning in w)
            importlib.reload(mod)


# ---------------------------------------------------------------------------
# IngestCapabilities.get_available
# ---------------------------------------------------------------------------

class TestIngestCapabilitiesGetAvailable:
    def test_returns_dict(self):
        caps = IngestCapabilities.get_available()
        assert isinstance(caps, dict)

    def test_csv_always_true(self):
        caps = IngestCapabilities.get_available()
        assert caps.get("csv") is True

    def test_json_always_true(self):
        caps = IngestCapabilities.get_available()
        assert caps.get("json") is True

    def test_txt_always_true(self):
        caps = IngestCapabilities.get_available()
        assert caps.get("txt") is True

    def test_html_always_true(self):
        caps = IngestCapabilities.get_available()
        assert caps.get("html") is True

    def test_xlsx_key_present(self):
        caps = IngestCapabilities.get_available()
        assert "xlsx" in caps

    def test_xlsx_value_is_bool(self):
        caps = IngestCapabilities.get_available()
        assert isinstance(caps["xlsx"], bool)

    def test_pdf_key_present(self):
        caps = IngestCapabilities.get_available()
        assert "pdf" in caps

    def test_all_values_are_bools(self):
        caps = IngestCapabilities.get_available()
        for key, val in caps.items():
            assert isinstance(val, bool), f"Expected bool for key {key!r}, got {type(val)}"


# ---------------------------------------------------------------------------
# IngestCapabilities.get_missing_deps
# ---------------------------------------------------------------------------

class TestIngestCapabilitiesGetMissingDeps:
    def test_returns_list(self):
        missing = IngestCapabilities.get_missing_deps()
        assert isinstance(missing, list)

    def test_each_entry_has_required_keys(self):
        missing = IngestCapabilities.get_missing_deps()
        for entry in missing:
            assert "package" in entry
            assert "formats" in entry
            assert "install" in entry

    def test_formats_is_list(self):
        missing = IngestCapabilities.get_missing_deps()
        for entry in missing:
            assert isinstance(entry["formats"], list)

    def test_empty_when_openpyxl_available(self):
        try:
            import openpyxl  # noqa: F401
            missing = IngestCapabilities.get_missing_deps()
            packages = [m["package"] for m in missing]
            assert "openpyxl" not in packages
        except ImportError:
            pytest.skip("openpyxl not installed")

    def test_openpyxl_reported_when_missing(self):
        with patch.dict(sys.modules, {"openpyxl": None}):
            import importlib
            import harvest_acquire.loaders.spreadsheet_loader as mod
            importlib.reload(mod)
            missing = mod.IngestCapabilities.get_missing_deps()
            packages = [m["package"] for m in missing]
            assert "openpyxl" in packages
            importlib.reload(mod)

    def test_install_hint_is_string(self):
        missing = IngestCapabilities.get_missing_deps()
        for entry in missing:
            assert isinstance(entry["install"], str)
            assert "pip install" in entry["install"]
