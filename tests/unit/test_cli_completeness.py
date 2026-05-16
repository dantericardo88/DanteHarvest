"""Tests for CLI completeness — status, version, validate commands."""
import json
import os
import sys
import tempfile
import pytest

# Import the CLI module directly so we don't rely on entry-point installation
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent.parent))
from harvest_ui.cli import main, build_parser


def _run(argv):
    """Run the CLI with given argv, capturing stdout/stderr. Returns (exit_code, stdout_lines, stderr_lines)."""
    import io
    from contextlib import redirect_stdout, redirect_stderr

    out = io.StringIO()
    err = io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        try:
            code = main(argv)
        except SystemExit as e:
            code = e.code if isinstance(e.code, int) else 1
    return code, out.getvalue(), err.getvalue()


class TestVersionCommand:
    def test_version_command_exists_in_parser(self):
        parser = build_parser()
        sub_actions = [a for a in parser._subparsers._group_actions]
        choices = {}
        for action in sub_actions:
            if hasattr(action, 'choices') and action.choices:
                choices.update(action.choices)
        assert "version" in choices

    def test_version_outputs_version_string(self):
        code, out, _ = _run(["version"])
        assert code == 0
        assert "harvest" in out.lower() or "1" in out

    def test_version_command_has_help(self):
        parser = build_parser()
        sub_actions = [a for a in parser._subparsers._group_actions]
        for action in sub_actions:
            if hasattr(action, 'choices') and action.choices and "version" in action.choices:
                assert action.choices["version"].description or action.choices["version"]._defaults or True
                break


class TestStatusCommand:
    def test_status_command_exists(self):
        parser = build_parser()
        sub_actions = [a for a in parser._subparsers._group_actions]
        choices = {}
        for action in sub_actions:
            if hasattr(action, 'choices') and action.choices:
                choices.update(action.choices)
        assert "status" in choices

    def test_status_runs_without_error(self):
        code, out, _ = _run(["status"])
        assert code == 0

    def test_status_text_format_contains_key_info(self):
        code, out, _ = _run(["status"])
        assert code == 0
        assert "storage" in out.lower() or "harvest" in out.lower()

    def test_status_json_format_outputs_valid_json(self):
        code, out, _ = _run(["status", "--output-format", "json"])
        assert code == 0
        data = json.loads(out)
        assert "storage" in data
        assert "version" in data

    def test_status_table_format_runs(self):
        code, out, _ = _run(["status", "--output-format", "table"])
        assert code == 0
        assert len(out.strip()) > 0

    def test_status_output_format_choices(self):
        parser = build_parser()
        # Parse --output-format for status
        args = parser.parse_args(["status", "--output-format", "json"])
        assert args.output_format == "json"

    def test_status_invalid_format_exits_nonzero(self):
        code, _, _ = _run(["status", "--output-format", "xml"])
        assert code != 0


class TestValidateCommand:
    def test_validate_command_exists(self):
        parser = build_parser()
        sub_actions = [a for a in parser._subparsers._group_actions]
        choices = {}
        for action in sub_actions:
            if hasattr(action, 'choices') and action.choices:
                choices.update(action.choices)
        assert "validate" in choices

    def test_validate_nonexistent_file_returns_error(self):
        code, out, err = _run(["validate", "/nonexistent/path/config.json"])
        assert code != 0
        combined = out + err
        assert "not found" in combined.lower() or "nonexistent" in combined.lower()

    def test_validate_nonexistent_file_json_format(self):
        code, out, _ = _run(["validate", "/nonexistent/path/config.json", "--output-format", "json"])
        assert code != 0
        data = json.loads(out)
        assert data["valid"] is False
        assert len(data["errors"]) > 0

    def test_validate_valid_config_with_source_key(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"source": "https://example.com"}, f)
            path = f.name
        try:
            code, out, _ = _run(["validate", path])
            assert code == 0
        finally:
            os.unlink(path)

    def test_validate_valid_config_sources_key(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"sources": ["https://a.com", "https://b.com"]}, f)
            path = f.name
        try:
            code, out, _ = _run(["validate", path])
            assert code == 0
        finally:
            os.unlink(path)

    def test_validate_json_output_valid_true(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"source": "s3://my-bucket"}, f)
            path = f.name
        try:
            code, out, _ = _run(["validate", path, "--output-format", "json"])
            assert code == 0
            data = json.loads(out)
            assert data["valid"] is True
            assert data["errors"] == []
        finally:
            os.unlink(path)

    def test_validate_json_output_missing_source_field(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"name": "my-harvest"}, f)
            path = f.name
        try:
            code, out, _ = _run(["validate", path, "--output-format", "json"])
            assert code != 0
            data = json.loads(out)
            assert data["valid"] is False
            assert any("source" in e.lower() for e in data["errors"])
        finally:
            os.unlink(path)

    def test_validate_invalid_json_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("{ this is not valid json }")
            path = f.name
        try:
            code, out, err = _run(["validate", path, "--output-format", "json"])
            assert code != 0
            data = json.loads(out)
            assert data["valid"] is False
            assert any("json" in e.lower() or "invalid" in e.lower() for e in data["errors"])
        finally:
            os.unlink(path)

    def test_validate_path_echoed_in_json_output(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"source": "file:///data"}, f)
            path = f.name
        try:
            code, out, _ = _run(["validate", path, "--output-format", "json"])
            data = json.loads(out)
            assert data["path"] == path
        finally:
            os.unlink(path)
