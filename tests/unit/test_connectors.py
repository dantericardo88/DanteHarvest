"""
Phase 7 — Connector breadth tests (GitHub, Notion, S3).

All network/SDK calls are mocked — no real credentials needed.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock
import pytest

from harvest_acquire.connectors.base_connector import BaseConnector, ConnectorError
from harvest_acquire.connectors.github_connector import GitHubConnector
from harvest_acquire.connectors.notion_connector import NotionConnector
from harvest_acquire.connectors.s3_connector import S3Connector


# ---------------------------------------------------------------------------
# BaseConnector
# ---------------------------------------------------------------------------

def test_base_connector_creates_artifact_dir(tmp_path):
    class DummyConnector(BaseConnector):
        connector_name = "dummy"
        def ingest(self, **kwargs):
            return []

    c = DummyConnector(storage_root=str(tmp_path))
    assert (tmp_path / "connectors" / "dummy").exists()


def test_base_connector_write_artifact(tmp_path):
    class DummyConnector(BaseConnector):
        connector_name = "dummy"
        def ingest(self, **kwargs):
            return []

    c = DummyConnector(storage_root=str(tmp_path))
    aid = c._write_artifact("art-001", "Hello world", meta={"source": "test"})
    assert aid == "art-001"
    assert (tmp_path / "connectors" / "dummy" / "art-001" / "content.md").read_text() == "Hello world"
    meta = json.loads((tmp_path / "connectors" / "dummy" / "art-001" / "meta.json").read_text())
    assert meta["source"] == "test"


# ---------------------------------------------------------------------------
# GitHubConnector
# ---------------------------------------------------------------------------

def test_github_connector_importable():
    from harvest_acquire.connectors.github_connector import GitHubConnector
    assert GitHubConnector


def test_github_connector_invalid_repo_raises(tmp_path):
    c = GitHubConnector(storage_root=str(tmp_path))
    with pytest.raises(ConnectorError, match="owner/repo"):
        c.ingest(repo="notavalidrepo")


def test_github_connector_via_rest_api(tmp_path):
    c = GitHubConnector(token="fake", storage_root=str(tmp_path))

    dir_response = json.dumps([
        {"type": "file", "name": "README.md", "path": "README.md",
         "url": "https://api.github.com/repos/owner/repo/contents/README.md",
         "html_url": "https://github.com/owner/repo/blob/main/README.md",
         "sha": "abc123"}
    ]).encode()

    file_response = json.dumps({
        "content": __import__("base64").b64encode(b"# Hello").decode() + "\n",
        "sha": "abc123",
    }).encode()

    call_count = [0]
    def fake_urlopen(req, timeout=None):
        call_count[0] += 1
        resp = MagicMock()
        resp.__enter__ = MagicMock(return_value=resp)
        resp.__exit__ = MagicMock(return_value=False)
        if "contents/README.md" in req.full_url:
            resp.read.return_value = file_response
        else:
            resp.read.return_value = dir_response
        return resp

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        with patch("harvest_acquire.connectors.github_connector.GitHubConnector._ingest_via_pygithub",
                   side_effect=ImportError("no pygithub")):
            aids = c.ingest(repo="owner/repo", extensions=[".md"])

    assert len(aids) == 1
    content = (tmp_path / "connectors" / "github" / aids[0] / "content.md").read_text()
    assert "Hello" in content


def test_github_connector_cli_parser():
    from harvest_ui.cli import build_parser
    parser = build_parser()
    args = parser.parse_args(["ingest", "github", "--repo", "owner/repo", "--token", "ghp_abc"])
    assert args.repo == "owner/repo"
    assert args.token == "ghp_abc"


# ---------------------------------------------------------------------------
# NotionConnector
# ---------------------------------------------------------------------------

def test_notion_connector_requires_token(tmp_path):
    with pytest.raises(ConnectorError, match="token"):
        NotionConnector(token="", storage_root=str(tmp_path))


def test_notion_connector_no_target_raises(tmp_path):
    c = NotionConnector(token="secret_abc", storage_root=str(tmp_path))
    with pytest.raises(ConnectorError, match="page_id or database_id"):
        c.ingest()


def test_notion_connector_ingest_page(tmp_path):
    c = NotionConnector(token="secret_abc", storage_root=str(tmp_path))

    page_meta = {"id": "page-001", "url": "https://notion.so/page-001",
                 "properties": {"title": {"title": [{"plain_text": "My Page"}]}}}
    blocks_resp = {"results": [
        {"type": "paragraph", "paragraph": {"rich_text": [{"plain_text": "Hello Notion"}]}, "has_children": False}
    ], "has_more": False}

    responses = [page_meta, blocks_resp]
    call_idx = [0]

    def fake_get(endpoint):
        r = responses[call_idx[0] % len(responses)]
        call_idx[0] += 1
        return r

    with patch.object(c, "_get", side_effect=fake_get):
        aids = c.ingest(page_id="page-001")

    assert len(aids) == 1
    content = (tmp_path / "connectors" / "notion" / aids[0] / "content.md").read_text()
    assert "Hello Notion" in content


def test_notion_connector_blocks_to_markdown(tmp_path):
    c = NotionConnector(token="secret_abc", storage_root=str(tmp_path))
    blocks = [
        {"type": "heading_1", "heading_1": {"rich_text": [{"plain_text": "Title"}]}, "has_children": False},
        {"type": "paragraph", "paragraph": {"rich_text": [{"plain_text": "Body text"}]}, "has_children": False},
        {"type": "bulleted_list_item", "bulleted_list_item": {"rich_text": [{"plain_text": "Item 1"}]}, "has_children": False},
    ]
    md = c._blocks_to_markdown(blocks)
    assert "# Title" in md
    assert "Body text" in md
    assert "- Item 1" in md


def test_notion_connector_cli_parser():
    from harvest_ui.cli import build_parser
    parser = build_parser()
    args = parser.parse_args(["ingest", "notion", "--token", "secret_x", "--page-id", "abc123"])
    assert args.page_id == "abc123"
    assert args.token == "secret_x"


# ---------------------------------------------------------------------------
# S3Connector
# ---------------------------------------------------------------------------

def test_s3_connector_importable():
    from harvest_acquire.connectors.s3_connector import S3Connector
    assert S3Connector


def test_s3_connector_no_boto3_raises(tmp_path):
    c = S3Connector(bucket="my-bucket", storage_root=str(tmp_path))
    with patch.dict("sys.modules", {"boto3": None}):
        with pytest.raises(ConnectorError, match="boto3"):
            c.ingest()


def test_s3_connector_ingest(tmp_path):
    c = S3Connector(bucket="my-bucket", storage_root=str(tmp_path))

    mock_boto3 = MagicMock()
    mock_client = MagicMock()
    mock_boto3.client.return_value = mock_client

    # Mock paginator
    mock_paginator = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [
        {"Contents": [
            {"Key": "docs/README.md", "Size": 100, "LastModified": "2024-01-01"},
            {"Key": "docs/config.yaml", "Size": 50, "LastModified": "2024-01-01"},  # not in extensions
        ]}
    ]

    mock_client.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=b"# Hello S3"))
    }

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
        aids = c.ingest(prefix="docs/", extensions=[".md"])

    assert len(aids) == 1
    content = (tmp_path / "connectors" / "s3" / aids[0] / "content.md").read_text()
    assert "Hello S3" in content


def test_s3_connector_cli_parser():
    from harvest_ui.cli import build_parser
    parser = build_parser()
    args = parser.parse_args(["ingest", "s3", "--bucket", "my-bucket", "--prefix", "docs/"])
    assert args.bucket == "my-bucket"
    assert args.prefix == "docs/"
