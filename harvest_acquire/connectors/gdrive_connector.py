"""
GDriveConnector — ingest Google Drive documents via Drive API v3.

Constitutional guarantees:
- Fail-closed: ConnectorError on auth failure or missing credentials
- Local-first: documents exported as markdown artifacts
- Zero-ambiguity: returns list of artifact IDs, never None
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional
from uuid import uuid4

from harvest_acquire.connectors.base_connector import BaseConnector, ConnectorError


_EXPORTABLE_MIME_TYPES = {
    "application/vnd.google-apps.document": "text/plain",
    "application/vnd.google-apps.spreadsheet": "text/csv",
    "application/vnd.google-apps.presentation": "text/plain",
}


class GDriveConnector(BaseConnector):
    """
    Ingest files from Google Drive using service account credentials.

    Credentials can be provided as:
    - JSON string (service_account_json)
    - Path to credentials file (credentials_path env var GOOGLE_APPLICATION_CREDENTIALS)

    Usage:
        connector = GDriveConnector(service_account_json='{"type": "service_account", ...}')
        artifact_ids = connector.ingest(folder_id="1ABCxyz", max_files=50)
    """

    connector_name = "gdrive"

    def __init__(
        self,
        service_account_json: Optional[str] = None,
        storage_root: str = "storage",
    ):
        super().__init__(storage_root)
        import os
        self._sa_json = service_account_json or os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
        self._credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
        if not self._sa_json and not self._credentials_path:
            raise ConnectorError(
                "Google credentials required. Set GOOGLE_SERVICE_ACCOUNT_JSON or "
                "GOOGLE_APPLICATION_CREDENTIALS env var."
            )
        self._access_token: Optional[str] = None
        self._token_expiry: float = 0.0

    def ingest(
        self,
        folder_id: Optional[str] = None,
        file_ids: Optional[List[str]] = None,
        max_files: int = 100,
        mime_type_filter: Optional[str] = None,
        **kwargs: Any,
    ) -> List[str]:
        """
        Ingest documents from a Google Drive folder or specific file IDs.

        Args:
            folder_id: Drive folder ID to list files from
            file_ids: Explicit list of file IDs (overrides folder_id)
            max_files: Maximum number of files to ingest
            mime_type_filter: Filter by MIME type (e.g. "application/vnd.google-apps.document")

        Returns:
            List of artifact IDs.
        """
        if not folder_id and not file_ids:
            raise ConnectorError("Either folder_id or file_ids is required.")

        self._ensure_token()
        target_ids = file_ids or self._list_folder(folder_id or "", max_files, mime_type_filter)

        artifact_ids = []
        for fid in target_ids[:max_files]:
            try:
                meta = self._get_file_meta(fid)
                content = self._export_file(fid, meta.get("mimeType", ""))
                if content is None:
                    continue
                artifact_id = str(uuid4())
                md = self._to_markdown(content, meta)
                artifact_meta = {
                    "connector": "gdrive",
                    "file_id": fid,
                    "name": meta.get("name"),
                    "mime_type": meta.get("mimeType"),
                    "fetched_at": time.time(),
                }
                self._write_artifact(artifact_id, md, artifact_meta)
                artifact_ids.append(artifact_id)
            except Exception:
                continue  # Per-file errors never abort the batch

        return artifact_ids

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _ensure_token(self) -> None:
        if self._access_token and time.time() < self._token_expiry - 60:
            return
        token, expiry = self._fetch_token()
        self._access_token = token
        self._token_expiry = expiry

    def _fetch_token(self) -> tuple[str, float]:
        import urllib.request, urllib.parse
        try:
            import json as _json
            sa = _json.loads(self._sa_json) if self._sa_json else {}
            if not sa:
                import pathlib
                sa = _json.loads(pathlib.Path(self._credentials_path).read_text())
            # JWT assertion for service account OAuth2
            import base64, hashlib, time as _time
            header = base64.urlsafe_b64encode(
                _json.dumps({"alg": "RS256", "typ": "JWT"}).encode()
            ).rstrip(b"=").decode()
            now = int(_time.time())
            payload = base64.urlsafe_b64encode(_json.dumps({
                "iss": sa.get("client_email", ""),
                "scope": "https://www.googleapis.com/auth/drive.readonly",
                "aud": "https://oauth2.googleapis.com/token",
                "exp": now + 3600,
                "iat": now,
            }).encode()).rstrip(b"=").decode()
            # Sign with RSA-SHA256 — requires cryptography or PyJWT
            try:
                from cryptography.hazmat.primitives import hashes, serialization
                from cryptography.hazmat.primitives.asymmetric import padding
                pk = serialization.load_pem_private_key(
                    sa.get("private_key", "").encode(), password=None
                )
                sig_bytes = pk.sign(f"{header}.{payload}".encode(), padding.PKCS1v15(), hashes.SHA256())
                sig = base64.urlsafe_b64encode(sig_bytes).rstrip(b"=").decode()
            except ImportError:
                raise ConnectorError(
                    "Google Drive service account auth requires 'cryptography'. "
                    "Install with: pip install cryptography"
                )
            jwt = f"{header}.{payload}.{sig}"
            data = urllib.parse.urlencode({
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": jwt,
            }).encode()
            req = urllib.request.Request(
                "https://oauth2.googleapis.com/token",
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                result = _json.loads(resp.read())
            return result["access_token"], _time.time() + result.get("expires_in", 3600)
        except ConnectorError:
            raise
        except Exception as e:
            raise ConnectorError(f"Failed to obtain Google auth token: {e}") from e

    def _list_folder(self, folder_id: str, max_files: int, mime_filter: Optional[str]) -> List[str]:
        import urllib.request, urllib.parse
        q = f"'{folder_id}' in parents and trashed=false"
        if mime_filter:
            q += f" and mimeType='{mime_filter}'"
        params = {"q": q, "pageSize": min(max_files, 1000), "fields": "files(id)"}
        url = "https://www.googleapis.com/drive/v3/files?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {self._access_token}"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        return [f["id"] for f in data.get("files", [])]

    def _get_file_meta(self, file_id: str) -> Dict[str, Any]:
        import urllib.request
        url = f"https://www.googleapis.com/drive/v3/files/{file_id}?fields=id,name,mimeType"
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {self._access_token}"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())

    def _export_file(self, file_id: str, mime_type: str) -> Optional[str]:
        import urllib.request
        export_mime = _EXPORTABLE_MIME_TYPES.get(mime_type)
        if export_mime:
            url = f"https://www.googleapis.com/drive/v3/files/{file_id}/export?mimeType={export_mime}"
        else:
            url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {self._access_token}"})
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except Exception:
            return None

    def _to_markdown(self, content: str, meta: Dict[str, Any]) -> str:
        name = meta.get("name", "Untitled")
        mime = meta.get("mimeType", "")
        return f"# {name}\n\n**Source:** Google Drive ({mime})\n\n{content}\n"
