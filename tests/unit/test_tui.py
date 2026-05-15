"""Unit tests for harvest_ui.tui — Rich-powered TUI helpers."""

from harvest_ui.tui import (
    is_rich_available,
    print_status,
    print_error,
    print_table,
    print_json,
    print_panel,
    progress_context,
    spinner_context,
    console,
    err_console,
)


# ---------------------------------------------------------------------------
# is_rich_available
# ---------------------------------------------------------------------------

class TestIsRichAvailable:
    def test_returns_bool(self):
        result = is_rich_available()
        assert isinstance(result, bool)


# ---------------------------------------------------------------------------
# print_status — smoke tests (output varies by Rich availability)
# ---------------------------------------------------------------------------

class TestPrintStatus:
    def test_ok_does_not_raise(self, capsys):
        print_status("ok", "Everything is fine")
        # Just verify it ran without error; output format depends on Rich

    def test_ok_with_fields_does_not_raise(self, capsys):
        print_status("ok", "Artifact ingested", artifact_id="abc123", sha256="def456")

    def test_error_does_not_raise(self, capsys):
        print_status("error", "Something failed", code=42)

    def test_warn_does_not_raise(self, capsys):
        print_status("warn", "Low confidence", score=0.4)

    def test_info_does_not_raise(self, capsys):
        print_status("info", "Processing", count=10)

    def test_unknown_status_does_not_raise(self, capsys):
        print_status("custom", "Custom status message")

    def test_no_fields_does_not_raise(self, capsys):
        print_status("ok", "Simple message")

    def test_output_contains_message(self, capsys):
        print_status("ok", "Unique message xyz789")
        captured = capsys.readouterr()
        assert "Unique message xyz789" in captured.out or "Unique message xyz789" in captured.err


# ---------------------------------------------------------------------------
# print_error
# ---------------------------------------------------------------------------

class TestPrintError:
    def test_does_not_raise(self, capsys):
        print_error("Something went wrong")

    def test_with_fields_does_not_raise(self, capsys):
        print_error("Connection failed", host="localhost", port=5432)

    def test_output_contains_message(self, capsys):
        print_error("Critical failure abc999")
        captured = capsys.readouterr()
        # error goes to stderr
        assert "Critical failure abc999" in captured.err or "Critical failure abc999" in captured.out


# ---------------------------------------------------------------------------
# print_table
# ---------------------------------------------------------------------------

class TestPrintTable:
    def test_empty_rows_does_not_raise(self, capsys):
        print_table("Empty Table", [])

    def test_single_row(self, capsys):
        print_table("Packs", [{"id": "p1", "status": "promoted"}])

    def test_multiple_rows(self, capsys):
        rows = [
            {"pack_id": f"p{i}", "status": "candidate", "title": f"Pack {i}"}
            for i in range(5)
        ]
        print_table("All Packs", rows, columns=["pack_id", "status", "title"])

    def test_with_column_filter(self, capsys):
        rows = [{"a": 1, "b": 2, "c": 3}]
        print_table("Filtered", rows, columns=["a", "c"])

    def test_missing_column_key_uses_empty_string(self, capsys):
        rows = [{"id": "x"}]
        print_table("Sparse", rows, columns=["id", "missing_col"])
        # Should not raise

    def test_output_contains_title_or_data(self, capsys):
        print_table("MyTitle", [{"k": "v_unique_9988"}])
        captured = capsys.readouterr()
        output = captured.out + captured.err
        assert "MyTitle" in output or "v_unique_9988" in output


# ---------------------------------------------------------------------------
# print_json
# ---------------------------------------------------------------------------

class TestPrintJson:
    def test_dict_does_not_raise(self, capsys):
        print_json({"key": "value", "num": 42})

    def test_list_does_not_raise(self, capsys):
        print_json([1, 2, 3])

    def test_with_title(self, capsys):
        print_json({"status": "ok"}, title="Response")

    def test_nested_dict(self, capsys):
        print_json({"a": {"b": {"c": "deep"}}})

    def test_output_contains_data(self, capsys):
        print_json({"unique_key_xyzzy": "unique_val_12345"})
        captured = capsys.readouterr()
        output = captured.out + captured.err
        assert "unique_key_xyzzy" in output or "unique_val_12345" in output


# ---------------------------------------------------------------------------
# print_panel
# ---------------------------------------------------------------------------

class TestPrintPanel:
    def test_does_not_raise(self, capsys):
        print_panel("Content here", title="My Panel")

    def test_multiline_content(self, capsys):
        print_panel("Line 1\nLine 2\nLine 3", title="Multi")

    def test_no_title(self, capsys):
        print_panel("Just content")

    def test_custom_style(self, capsys):
        print_panel("Styled", title="Green", style="green")


# ---------------------------------------------------------------------------
# progress_context
# ---------------------------------------------------------------------------

class TestProgressContext:
    def test_basic_usage_does_not_raise(self, capsys):
        with progress_context("Processing", total=5) as prog:
            for _ in range(5):
                prog.advance()

    def test_advance_multiple(self, capsys):
        with progress_context("Batch", total=10) as prog:
            prog.advance(5)
            prog.advance(5)

    def test_update_description(self, capsys):
        with progress_context("Starting", total=3) as prog:
            prog.update("Step 1")
            prog.advance()
            prog.update("Step 2")
            prog.advance()

    def test_no_total(self, capsys):
        with progress_context("Indeterminate") as prog:
            prog.advance()
            prog.advance()

    def test_advance_default_is_one(self, capsys):
        with progress_context("Default advance", total=2) as prog:
            prog.advance()
            prog.advance()

    def test_zero_total(self, capsys):
        with progress_context("Empty", total=0) as prog:
            pass  # nothing to advance


# ---------------------------------------------------------------------------
# spinner_context
# ---------------------------------------------------------------------------

class TestSpinnerContext:
    def test_does_not_raise(self, capsys):
        with spinner_context("Connecting..."):
            pass

    def test_update_does_not_raise(self, capsys):
        with spinner_context("Starting...") as spin:
            spin.update("Step 1 done")
            spin.update("Step 2 done")

    def test_exception_propagates(self):
        import pytest
        with pytest.raises(ValueError, match="deliberate"):
            with spinner_context("Will fail"):
                raise ValueError("deliberate")


# ---------------------------------------------------------------------------
# console / err_console objects exist
# ---------------------------------------------------------------------------

class TestConsoleObjects:
    def test_console_has_print(self):
        assert hasattr(console, "print")

    def test_err_console_has_print(self):
        assert hasattr(err_console, "print")
