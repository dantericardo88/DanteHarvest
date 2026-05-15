"""
GitHubConnector — ingest a GitHub repository into the artifact store.

Requires: pip install PyGitHub (MIT license)
Falls back to GitHub REST API via urllib if PyGitHub is not installed.

Usage:
    connector = GitHubConnector(token="ghp_...", storage_root="storage")
    artifact_ids = connector.ingest(repo="owner/repo", path="src/", extensions=[".py", ".md"])

CLI:
    harvest ingest github --repo owner/repo --token $GITHUB_TOKEN --path src/
"""

from __future__ import annotations

import base64
import json
import urllib.request
from typing import Any, List, Optional
from uuid import uuid4

from harvest_acquire.connectors.base_connector import BaseConnector, ConnectorError


class GitHubConnector(BaseConnector):
    connector_name = "github"
    required_env_vars = ["GITHUB_TOKEN", "GH_TOKEN"]

    def __init__(self, token: Optional[str] = None, storage_root: str = "storage"):
        super().__init__(storage_root=storage_root)
        self._token = token

    def ingest(
        self,
        repo: str,
        path: str = "",
        extensions: Optional[List[str]] = None,
        branch: str = "HEAD",
        **kwargs: Any,
    ) -> List[str]:
        """
        Ingest files from a GitHub repo path.
        Returns list of artifact IDs written.
        """
        if "/" not in repo:
            raise ConnectorError(f"repo must be 'owner/repo', got: {repo!r}")
        extensions = extensions or [".py", ".md", ".txt", ".ts", ".js"]
        try:
            return self._ingest_via_pygithub(repo, path, extensions, branch)
        except ImportError:
            return self._ingest_via_rest_api(repo, path, extensions, branch)

    def _ingest_via_pygithub(
        self, repo: str, path: str, extensions: List[str], branch: str
    ) -> List[str]:
        from github import Github, GithubException  # type: ignore[import]
        g = Github(self._token) if self._token else Github()
        try:
            gh_repo = g.get_repo(repo)
        except GithubException as e:
            raise ConnectorError(f"GitHub repo not found: {repo} — {e}") from e
        contents = gh_repo.get_contents(path, ref=branch)
        artifact_ids = []
        queue = list(contents) if isinstance(contents, list) else [contents]
        while queue:
            item = queue.pop(0)
            if item.type == "dir":
                queue.extend(gh_repo.get_contents(item.path, ref=branch))
            elif item.type == "file" and any(item.name.endswith(ext) for ext in extensions):
                content = item.decoded_content.decode("utf-8", errors="replace")
                aid = str(uuid4())
                self._write_artifact(aid, content, meta={
                    "source": "github",
                    "repo": repo,
                    "path": item.path,
                    "sha": item.sha,
                    "url": item.html_url,
                })
                artifact_ids.append(aid)
        return artifact_ids

    def _ingest_via_rest_api(
        self, repo: str, path: str, extensions: List[str], branch: str
    ) -> List[str]:
        headers = {"Accept": "application/vnd.github+json"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        artifact_ids = []
        queue = [path]
        while queue:
            current = queue.pop(0)
            url = f"https://api.github.com/repos/{repo}/contents/{current}"
            if branch and branch != "HEAD":
                url += f"?ref={branch}"
            req = urllib.request.Request(url, headers=headers)
            try:
                with urllib.request.urlopen(req, timeout=15) as resp:
                    items = json.loads(resp.read().decode())
            except Exception as e:
                raise ConnectorError(f"GitHub API error for {url}: {e}") from e
            if isinstance(items, dict):
                items = [items]
            for item in items:
                if item["type"] == "dir":
                    queue.append(item["path"])
                elif item["type"] == "file" and any(item["name"].endswith(ext) for ext in extensions):
                    # Fetch file content
                    file_req = urllib.request.Request(item["url"], headers=headers)
                    try:
                        with urllib.request.urlopen(file_req, timeout=15) as fr:
                            file_data = json.loads(fr.read().decode())
                        raw = base64.b64decode(file_data.get("content", "").replace("\n", "")).decode("utf-8", errors="replace")
                    except Exception:
                        raw = ""
                    aid = str(uuid4())
                    self._write_artifact(aid, raw, meta={
                        "source": "github",
                        "repo": repo,
                        "path": item["path"],
                        "sha": item.get("sha", ""),
                        "url": item.get("html_url", ""),
                    })
                    artifact_ids.append(aid)
        return artifact_ids
