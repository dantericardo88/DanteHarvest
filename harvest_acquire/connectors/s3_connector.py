"""
S3Connector — ingest files from S3-compatible object storage.

Supports AWS S3, GCS (via S3-compatible endpoint), and MinIO.
Requires: pip install boto3 (Apache-2.0)

Usage:
    connector = S3Connector(bucket="my-bucket", storage_root="storage")
    artifact_ids = connector.ingest(prefix="docs/", extensions=[".md", ".txt"])

CLI:
    harvest ingest s3 --bucket my-bucket --prefix docs/ [--endpoint-url http://minio:9000]
"""

from __future__ import annotations

import io
from typing import Any, List, Optional
from uuid import uuid4

from harvest_acquire.connectors.base_connector import BaseConnector, ConnectorError


class S3Connector(BaseConnector):
    connector_name = "s3"

    def __init__(
        self,
        bucket: str,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        storage_root: str = "storage",
    ):
        super().__init__(storage_root=storage_root)
        self._bucket = bucket
        self._client_kwargs = {
            k: v for k, v in {
                "aws_access_key_id": aws_access_key_id,
                "aws_secret_access_key": aws_secret_access_key,
                "region_name": region_name,
                "endpoint_url": endpoint_url,
            }.items() if v is not None
        }

    def ingest(
        self,
        prefix: str = "",
        extensions: Optional[List[str]] = None,
        max_keys: int = 1000,
        **kwargs: Any,
    ) -> List[str]:
        """
        Download and ingest objects from S3 that match prefix and extensions.
        Returns list of artifact IDs.
        """
        try:
            import boto3
        except ImportError as e:
            raise ConnectorError(
                "boto3 not installed. Run: pip install boto3"
            ) from e

        extensions = extensions or [".md", ".txt", ".pdf", ".docx", ".csv"]
        client = boto3.client("s3", **self._client_kwargs)

        artifact_ids = []
        paginator = client.get_paginator("list_objects_v2")
        page_iter = paginator.paginate(Bucket=self._bucket, Prefix=prefix, PaginationConfig={"MaxItems": max_keys})

        for page in page_iter:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not any(key.endswith(ext) for ext in extensions):
                    continue
                try:
                    resp = client.get_object(Bucket=self._bucket, Key=key)
                    raw = resp["Body"].read()
                    content = raw.decode("utf-8", errors="replace")
                    aid = str(uuid4())
                    self._write_artifact(aid, content, meta={
                        "source": "s3",
                        "bucket": self._bucket,
                        "key": key,
                        "size": obj.get("Size", 0),
                        "last_modified": str(obj.get("LastModified", "")),
                    })
                    artifact_ids.append(aid)
                except Exception as e:
                    import logging
                    logging.getLogger(__name__).warning("S3Connector: failed to fetch %s: %s", key, e)

        return artifact_ids
