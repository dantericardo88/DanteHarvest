"""Tests for XLSXAdapter and EPUBAdapter — spreadsheet/ebook to markdown."""

import csv
import io
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from harvest_normalize.markdown.xlsx_adapter import XLSXAdapter, _rows_to_markdown
from harvest_core.control.exceptions import NormalizationError


# ---------------------------------------------------------------------------
# _rows_to_markdown unit tests
# ---------------------------------------------------------------------------

def test_rows_to_markdown_basic():
    md = _rows_to_markdown(["Name", "Age"], [["Alice", "30"], ["Bob", "25"]])
    assert "| Name | Age |" in md
    assert "| --- | --- |" in md
    assert "| Alice | 30 |" in md
    assert "| Bob | 25 |" in md


def test_rows_to_markdown_empty_headers():
    md = _rows_to_markdown([], [["a", "b"]])
    assert md == ""


def test_rows_to_markdown_no_data_rows():
    md = _rows_to_markdown(["Col1", "Col2"], [])
    assert "| Col1 | Col2 |" in md
    assert "| --- | --- |" in md


def test_rows_to_markdown_truncates_long_rows():
    # row has more cols than headers — should be truncated
    md = _rows_to_markdown(["A", "B"], [["x", "y", "z_should_be_cut"]])
    assert "z_should_be_cut" not in md


# ---------------------------------------------------------------------------
# XLSXAdapter CSV conversion
# ---------------------------------------------------------------------------

def test_convert_csv(tmp_path):
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("name,score\nAlice,95\nBob,87\n", encoding="utf-8")
    adapter = XLSXAdapter()
    result = adapter.convert(csv_file)
    assert "## data" in result
    assert "| name | score |" in result
    assert "| Alice | 95 |" in result


def test_convert_csv_empty(tmp_path):
    csv_file = tmp_path / "empty.csv"
    csv_file.write_text("", encoding="utf-8")
    adapter = XLSXAdapter()
    result = adapter.convert(csv_file)
    assert result == ""


def test_convert_missing_file_raises():
    adapter = XLSXAdapter()
    with pytest.raises(NormalizationError, match="not found"):
        adapter.convert(Path("/nonexistent/file.xlsx"))


def test_convert_unsupported_format_raises(tmp_path):
    f = tmp_path / "file.docx"
    f.write_text("content")
    adapter = XLSXAdapter()
    with pytest.raises(NormalizationError, match="Unsupported"):
        adapter.convert(f)


# ---------------------------------------------------------------------------
# XLSXAdapter XLSX conversion (mocked openpyxl)
# ---------------------------------------------------------------------------

def test_convert_xlsx_mocked(tmp_path):
    xlsx_file = tmp_path / "report.xlsx"
    xlsx_file.write_bytes(b"fake xlsx content")

    mock_ws = MagicMock()
    mock_ws.iter_rows.return_value = [
        ("Product", "Revenue"),
        ("Widget A", 1000),
        ("Widget B", 2500),
    ]
    mock_wb = MagicMock()
    mock_wb.sheetnames = ["Sales"]
    mock_wb.__getitem__ = lambda self, key: mock_ws
    mock_wb.close = MagicMock()

    with patch("openpyxl.load_workbook", return_value=mock_wb):
        adapter = XLSXAdapter()
        result = adapter.convert(xlsx_file)

    assert "## Sales" in result
    assert "| Product | Revenue |" in result
    assert "| Widget A | 1000 |" in result


def test_convert_xlsx_empty_sheet_skipped(tmp_path):
    xlsx_file = tmp_path / "empty.xlsx"
    xlsx_file.write_bytes(b"fake")

    mock_ws = MagicMock()
    mock_ws.iter_rows.return_value = []
    mock_wb = MagicMock()
    mock_wb.sheetnames = ["Empty"]
    mock_wb.__getitem__ = lambda self, key: mock_ws
    mock_wb.close = MagicMock()

    with patch("openpyxl.load_workbook", return_value=mock_wb):
        adapter = XLSXAdapter()
        result = adapter.convert(xlsx_file)

    assert result == ""


def test_convert_xlsx_none_cells_become_empty_string(tmp_path):
    xlsx_file = tmp_path / "nulls.xlsx"
    xlsx_file.write_bytes(b"fake")

    mock_ws = MagicMock()
    mock_ws.iter_rows.return_value = [
        ("A", None, "C"),
        (1, None, 3),
    ]
    mock_wb = MagicMock()
    mock_wb.sheetnames = ["Sheet1"]
    mock_wb.__getitem__ = lambda self, key: mock_ws
    mock_wb.close = MagicMock()

    with patch("openpyxl.load_workbook", return_value=mock_wb):
        adapter = XLSXAdapter()
        result = adapter.convert(xlsx_file)

    assert "|  |" in result  # None → ""
    assert "| A |" in result


def test_convert_xlsx_multiple_sheets(tmp_path):
    xlsx_file = tmp_path / "multi.xlsx"
    xlsx_file.write_bytes(b"fake")

    def make_ws(rows):
        ws = MagicMock()
        ws.iter_rows.return_value = rows
        return ws

    sheets = {
        "Q1": make_ws([("Month", "Revenue"), ("Jan", 100)]),
        "Q2": make_ws([("Month", "Revenue"), ("Apr", 200)]),
    }
    mock_wb = MagicMock()
    mock_wb.sheetnames = ["Q1", "Q2"]
    mock_wb.__getitem__ = lambda self, key: sheets[key]
    mock_wb.close = MagicMock()

    with patch("openpyxl.load_workbook", return_value=mock_wb):
        adapter = XLSXAdapter()
        result = adapter.convert(xlsx_file)

    assert "## Q1" in result
    assert "## Q2" in result
    assert "| Jan | 100 |" in result
    assert "| Apr | 200 |" in result


def test_convert_xlsx_openpyxl_not_installed_raises(tmp_path):
    xlsx_file = tmp_path / "report.xlsx"
    xlsx_file.write_bytes(b"fake")
    import sys
    with patch.dict(sys.modules, {"openpyxl": None}):
        adapter = XLSXAdapter()
        with pytest.raises(NormalizationError, match="openpyxl"):
            adapter._convert_xlsx(xlsx_file)
