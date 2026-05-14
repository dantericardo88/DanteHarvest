"""Unit tests for SpreadsheetLoader — CI-safe, no real openpyxl workbooks needed."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from harvest_acquire.loaders.spreadsheet_loader import (
    SpreadsheetLoader,
    SpreadsheetDocument,
    SheetDocument,
    _rows_to_markdown,
)
from harvest_core.control.exceptions import NormalizationError


# ---------------------------------------------------------------------------
# _rows_to_markdown
# ---------------------------------------------------------------------------

class TestRowsToMarkdown:
    def test_basic_table(self):
        md = _rows_to_markdown(["Name", "Age"], [["Alice", "30"], ["Bob", "25"]])
        assert "| Name | Age |" in md
        assert "| --- | --- |" in md
        assert "| Alice | 30 |" in md
        assert "| Bob | 25 |" in md

    def test_empty_headers_returns_empty(self):
        assert _rows_to_markdown([], [["a", "b"]]) == ""

    def test_no_data_rows_still_renders_header(self):
        md = _rows_to_markdown(["Col1", "Col2"], [])
        assert "| Col1 | Col2 |" in md
        assert "| --- | --- |" in md

    def test_truncates_excess_columns(self):
        md = _rows_to_markdown(["A", "B"], [["x", "y", "overflow"]])
        assert "overflow" not in md

    def test_single_column(self):
        md = _rows_to_markdown(["Value"], [["42"], ["99"]])
        assert "| Value |" in md
        assert "| 42 |" in md
        assert "| 99 |" in md


# ---------------------------------------------------------------------------
# CSV loading (stdlib only — no mocks needed)
# ---------------------------------------------------------------------------

class TestSpreadsheetLoaderCSV:
    def test_load_csv_basic(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("name,score\nAlice,95\nBob,87\n", encoding="utf-8")
        loader = SpreadsheetLoader()
        docs = loader.load(f)
        assert len(docs) == 1
        doc = docs[0]
        assert doc.format == "csv"
        assert len(doc.sheets) == 1
        assert "| name | score |" in doc.markdown
        assert "| Alice | 95 |" in doc.markdown

    def test_load_csv_empty_file(self, tmp_path):
        f = tmp_path / "empty.csv"
        f.write_text("", encoding="utf-8")
        loader = SpreadsheetLoader()
        docs = loader.load(f)
        assert len(docs) == 1
        assert docs[0].markdown == ""

    def test_load_csv_headers_only(self, tmp_path):
        f = tmp_path / "headers.csv"
        f.write_text("col1,col2\n", encoding="utf-8")
        loader = SpreadsheetLoader()
        docs = loader.load(f)
        assert "| col1 | col2 |" in docs[0].markdown

    def test_load_csv_stem_used_as_section_heading(self, tmp_path):
        f = tmp_path / "my_report.csv"
        f.write_text("a,b\n1,2\n", encoding="utf-8")
        loader = SpreadsheetLoader()
        docs = loader.load(f)
        assert "## my_report" in docs[0].markdown

    def test_load_csv_record_count(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("x,y\n1,2\n3,4\n5,6\n", encoding="utf-8")
        loader = SpreadsheetLoader()
        docs = loader.load(f)
        assert docs[0].sheets[0].row_count == 3

    def test_sheet_names_property(self, tmp_path):
        f = tmp_path / "report.csv"
        f.write_text("a,b\n1,2\n", encoding="utf-8")
        loader = SpreadsheetLoader()
        docs = loader.load(f)
        assert docs[0].sheet_names == ["report"]


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

class TestSpreadsheetLoaderErrors:
    def test_missing_file_raises(self):
        loader = SpreadsheetLoader()
        with pytest.raises(NormalizationError, match="not found"):
            loader.load(Path("/nonexistent/file.csv"))

    def test_unsupported_suffix_raises(self, tmp_path):
        f = tmp_path / "file.docx"
        f.write_text("content")
        loader = SpreadsheetLoader()
        with pytest.raises(NormalizationError, match="Unsupported"):
            loader.load(f)

    def test_openpyxl_missing_raises_normalization_error(self, tmp_path):
        f = tmp_path / "report.xlsx"
        f.write_bytes(b"fake xlsx")
        with patch.dict(sys.modules, {"openpyxl": None}):
            loader = SpreadsheetLoader()
            with pytest.raises(NormalizationError, match="openpyxl"):
                loader.load(f)


# ---------------------------------------------------------------------------
# XLSX loading (openpyxl mocked)
# ---------------------------------------------------------------------------

class TestSpreadsheetLoaderXLSX:
    def _make_mock_workbook(self, sheets: dict) -> MagicMock:
        def make_ws(rows):
            ws = MagicMock()
            ws.iter_rows.return_value = rows
            return ws

        mock_wb = MagicMock()
        mock_wb.sheetnames = list(sheets.keys())
        sheet_mocks = {k: make_ws(v) for k, v in sheets.items()}
        mock_wb.__getitem__ = lambda self, key: sheet_mocks[key]
        mock_wb.close = MagicMock()
        return mock_wb

    def test_load_xlsx_single_sheet(self, tmp_path):
        f = tmp_path / "report.xlsx"
        f.write_bytes(b"fake")
        mock_wb = self._make_mock_workbook({
            "Sales": [("Product", "Revenue"), ("Widget", 1000)],
        })
        with patch("openpyxl.load_workbook", return_value=mock_wb):
            docs = SpreadsheetLoader().load(f)
        assert len(docs) == 1
        assert "## Sales" in docs[0].markdown
        assert "| Product | Revenue |" in docs[0].markdown
        assert "| Widget | 1000 |" in docs[0].markdown

    def test_load_xlsx_multiple_sheets(self, tmp_path):
        f = tmp_path / "multi.xlsx"
        f.write_bytes(b"fake")
        mock_wb = self._make_mock_workbook({
            "Q1": [("Month", "Sales"), ("Jan", 100)],
            "Q2": [("Month", "Sales"), ("Apr", 200)],
        })
        with patch("openpyxl.load_workbook", return_value=mock_wb):
            docs = SpreadsheetLoader().load(f)
        assert "## Q1" in docs[0].markdown
        assert "## Q2" in docs[0].markdown
        assert docs[0].sheet_names == ["Q1", "Q2"]

    def test_load_xlsx_empty_sheet_skipped(self, tmp_path):
        f = tmp_path / "empty.xlsx"
        f.write_bytes(b"fake")
        mock_wb = self._make_mock_workbook({"Empty": []})
        with patch("openpyxl.load_workbook", return_value=mock_wb):
            docs = SpreadsheetLoader().load(f)
        assert docs[0].markdown == ""
        assert len(docs[0].sheets) == 0

    def test_load_xlsx_none_cells_become_empty_string(self, tmp_path):
        f = tmp_path / "nulls.xlsx"
        f.write_bytes(b"fake")
        mock_wb = self._make_mock_workbook({
            "Sheet1": [("A", None, "C"), (1, None, 3)],
        })
        with patch("openpyxl.load_workbook", return_value=mock_wb):
            docs = SpreadsheetLoader().load(f)
        md = docs[0].markdown
        assert "| A |" in md
        assert "|  |" in md  # None → ""

    def test_load_xlsx_workbook_close_called_on_success(self, tmp_path):
        f = tmp_path / "report.xlsx"
        f.write_bytes(b"fake")
        mock_wb = self._make_mock_workbook({
            "Sheet1": [("X",), ("1",)],
        })
        with patch("openpyxl.load_workbook", return_value=mock_wb):
            SpreadsheetLoader().load(f)
        mock_wb.close.assert_called_once()

    def test_load_xlsx_workbook_close_called_on_error(self, tmp_path):
        f = tmp_path / "report.xlsx"
        f.write_bytes(b"fake")
        mock_wb = self._make_mock_workbook({})
        mock_wb.sheetnames = ["Bad"]
        bad_ws = MagicMock()
        bad_ws.iter_rows.side_effect = RuntimeError("corrupt sheet")
        mock_wb.__getitem__ = lambda self, key: bad_ws
        with patch("openpyxl.load_workbook", return_value=mock_wb):
            with pytest.raises(RuntimeError, match="corrupt"):
                SpreadsheetLoader().load(f)
        mock_wb.close.assert_called_once()

    def test_spreadsheet_document_markdown_property(self, tmp_path):
        f = tmp_path / "report.xlsx"
        f.write_bytes(b"fake")
        mock_wb = self._make_mock_workbook({
            "Sheet1": [("A", "B"), ("1", "2")],
        })
        with patch("openpyxl.load_workbook", return_value=mock_wb):
            docs = SpreadsheetLoader().load(f)
        # markdown property should concatenate all sheets
        assert "## Sheet1" in docs[0].markdown
