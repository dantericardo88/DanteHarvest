"""
PostgresConnector — sample rows from PostgreSQL tables and emit markdown artifacts.

Requires: psycopg2 (optional dep) — falls back to ConnectorError if not installed.

Usage:
    conn = PostgresConnector(dsn="postgresql://user:pass@host:5432/dbname")
    artifact_ids = conn.ingest(tables=["orders", "customers"], sample_rows=100)

Constitutional guarantees:
- Local-first: reads from caller-supplied DSN; no network calls beyond the DB
- Fail-closed: ConnectorError on missing dep or connection failure
- Zero-ambiguity: always returns List[str] of artifact IDs
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from harvest_acquire.connectors.base_connector import BaseConnector, ConnectorError


class PostgresConnector(BaseConnector):
    """
    Ingests table samples from a PostgreSQL database.

    Each table becomes one artifact containing a markdown table of sample rows.
    Schema introspection emits column names as the header row.
    """

    connector_name = "postgres"

    def __init__(
        self,
        dsn: str,
        storage_root: str = "storage/connectors",
        sample_rows: int = 200,
        excluded_columns: Optional[List[str]] = None,
    ):
        super().__init__(storage_root=storage_root)
        self._dsn = dsn
        self._default_sample = sample_rows
        self._excluded = set(excluded_columns or [])

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ingest(
        self,
        tables: Optional[List[str]] = None,
        sample_rows: Optional[int] = None,
        schema: str = "public",
    ) -> List[str]:
        """
        Ingest tables from the connected PostgreSQL database.

        Args:
            tables:      list of table names; None = discover all tables in schema
            sample_rows: rows per table (overrides constructor default)
            schema:      PostgreSQL schema name (default "public")

        Returns:
            List of artifact IDs, one per table.
        """
        conn = self._connect()
        try:
            target_tables = tables or self._list_tables(conn, schema)
            artifact_ids = []
            for table in target_tables:
                rows_limit = sample_rows or self._default_sample
                md = self._table_to_markdown(conn, schema, table, rows_limit)
                meta = {
                    "source": "postgres",
                    "table": f"{schema}.{table}",
                    "dsn_host": self._dsn_host(),
                    "sample_rows": rows_limit,
                }
                aid = self._write_artifact(f"pg_{schema}_{table}", md, meta)
                artifact_ids.append(aid)
            return artifact_ids
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _connect(self) -> Any:
        try:
            import psycopg2  # type: ignore[import]
        except ImportError as exc:
            raise ConnectorError(
                "psycopg2 not installed. Run: pip install psycopg2-binary"
            ) from exc
        try:
            return psycopg2.connect(self._dsn)
        except Exception as exc:
            raise ConnectorError(f"PostgresConnector: connection failed: {exc}") from exc

    def _list_tables(self, conn: Any, schema: str) -> List[str]:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """,
                (schema,),
            )
            return [row[0] for row in cur.fetchall()]

    def _table_to_markdown(
        self, conn: Any, schema: str, table: str, limit: int
    ) -> str:
        with conn.cursor() as cur:
            # Introspect columns
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (schema, table),
            )
            all_cols = [row[0] for row in cur.fetchall()]
            cols = [c for c in all_cols if c not in self._excluded]
            if not cols:
                return f"# {schema}.{table}\n\n_All columns excluded._\n"

            col_list = ", ".join(f'"{c}"' for c in cols)
            cur.execute(
                f'SELECT {col_list} FROM "{schema}"."{table}" LIMIT %s',
                (limit,),
            )
            rows = cur.fetchall()

        lines = [f"# {schema}.{table}", ""]
        # Header
        lines.append("| " + " | ".join(cols) + " |")
        lines.append("| " + " | ".join("---" for _ in cols) + " |")
        # Data
        for row in rows:
            cells = [str(v).replace("|", "\\|").replace("\n", " ") if v is not None else "" for v in row]
            lines.append("| " + " | ".join(cells) + " |")

        lines.append("")
        lines.append(f"_Showing {len(rows)} of up to {limit} rows._")
        return "\n".join(lines)

    def _dsn_host(self) -> str:
        """Extract host from DSN for metadata (omits credentials)."""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(self._dsn)
            return parsed.hostname or "unknown"
        except Exception:
            return "unknown"
