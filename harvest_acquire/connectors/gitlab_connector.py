"""
GitLabConnector — ingest file content from GitLab repositories via REST API.

Uses the GitLab REST API v4 (no SDK dependency).
Falls back gracefully with ConnectorError if token is missing or API is unreachable.

Usage:
    conn = GitLabConnector(token="glpat-xxxx", base_url="https://gitlab.com")
    artifact_ids = conn.ingest(project="mygroup/myrepo", path="src/", extensions=[".py"])

Constitutional guarantees:
- Local-first: writes artifacts to local storage; network call only to GitLab API
- Fail-closed: ConnectorError on auth failure, project not found, or rate limit
- Zero-ambiguity: always returns List[str] of artifact IDs
"""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional

from harvest_acquire.connectors.base_connector import BaseConnector, ConnectorError


class GitLabConnector(BaseConnector):
    """
    Ingests file content from a GitLab repository.

    Each file within the target path becomes one artifact containing
    the file's raw text content as markdown (fenced code block).
    """

    connector_name = "gitlab"

    def __init__(
        self,
        token: str,
        base_url: str = "https://gitlab.com",
        storage_root: str = "storage/connectors",
        timeout: int = 30,
    ):
        super().__init__(storage_root=storage_root)
        if not token:
            raise ConnectorError("GitLabConnector: token is required")
        self._token = token
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ingest(
        self,
        project: str,
        path: str = "",
        ref: str = "main",
        extensions: Optional[List[str]] = None,
        max_files: int = 500,
    ) -> List[str]:
        """
        Ingest files from a GitLab project.

        Args:
            project:    Project path with namespace, e.g. "mygroup/myrepo"
            path:       Sub-directory to restrict ingestion (default: repo root)
            ref:        Branch / tag / commit SHA (default "main")
            extensions: File extensions to include, e.g. [".py", ".md"]
            max_files:  Maximum number of files to ingest

        Returns:
            List of artifact IDs.
        """
        project_id = urllib.parse.quote(project, safe="")
        tree = self._list_tree(project_id, path, ref, recursive=True)
        files = [
            item for item in tree
            if item.get("type") == "blob"
            and (not extensions or any(item["name"].endswith(ext) for ext in extensions))
        ][:max_files]

        artifact_ids = []
        for item in files:
            file_path = item["path"]
            try:
                content = self._get_file_content(project_id, file_path, ref)
            except ConnectorError:
                continue
            ext = file_path.rsplit(".", 1)[-1] if "." in file_path else "txt"
            md = f"# {file_path}\n\n```{ext}\n{content}\n```\n"
            meta = {
                "source": "gitlab",
                "project": project,
                "file_path": file_path,
                "ref": ref,
                "base_url": self._base_url,
            }
            safe_name = file_path.replace("/", "_").replace(".", "_")
            aid = self._write_artifact(f"gl_{safe_name}", md, meta)
            artifact_ids.append(aid)

        return artifact_ids

    def list_projects(self, search: Optional[str] = None, max_results: int = 20) -> List[Dict[str, Any]]:
        """List accessible GitLab projects (useful for discovery)."""
        params: Dict[str, Any] = {"per_page": min(max_results, 100), "membership": "true"}
        if search:
            params["search"] = search
        return self._get("/api/v4/projects", params)  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _list_tree(
        self, project_id: str, path: str, ref: str, recursive: bool = False
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "ref": ref,
            "per_page": 100,
            "recursive": str(recursive).lower(),
        }
        if path:
            params["path"] = path
        endpoint = f"/api/v4/projects/{project_id}/repository/tree"
        items: List[Dict[str, Any]] = []
        page = 1
        while True:
            params["page"] = page
            batch = self._get(endpoint, params)
            if not batch:
                break
            items.extend(batch)
            if len(batch) < 100:
                break
            page += 1
        return items

    def _get_file_content(self, project_id: str, file_path: str, ref: str) -> str:
        encoded_path = urllib.parse.quote(file_path, safe="")
        endpoint = f"/api/v4/projects/{project_id}/repository/files/{encoded_path}/raw"
        return self._get_raw(endpoint, {"ref": ref})

    def _get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = self._build_url(endpoint, params)
        req = urllib.request.Request(url, headers={"PRIVATE-TOKEN": self._token})
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            raise ConnectorError(f"GitLabConnector: {endpoint} returned {e.code}: {e.reason}") from e
        except Exception as e:
            raise ConnectorError(f"GitLabConnector: request failed: {e}") from e

    def _get_raw(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> str:
        url = self._build_url(endpoint, params)
        req = urllib.request.Request(url, headers={"PRIVATE-TOKEN": self._token})
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            raise ConnectorError(f"GitLabConnector: file fetch {endpoint} returned {e.code}") from e
        except Exception as e:
            raise ConnectorError(f"GitLabConnector: file fetch failed: {e}") from e

    def _build_url(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> str:
        url = self._base_url + endpoint
        if params:
            url += "?" + urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
        return url
