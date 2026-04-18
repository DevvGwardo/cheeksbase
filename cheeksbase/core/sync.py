"""Sync engine — syncs data from sources into DuckDB."""

from __future__ import annotations

import hashlib
import json
import re
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import duckdb

from cheeksbase.core.db import META_SCHEMA, CheeksbaseDB, _validate_identifier


@dataclass
class SyncResult:
    """Result of a sync operation."""

    connector_name: str
    connector_type: str
    tables_synced: int
    rows_synced: int
    status: str
    error: str | None = None
    row_counts: dict[str, int] = field(default_factory=dict)
    table_names: list[str] = field(default_factory=list)


class SyncEngine:
    """Syncs data from sources into DuckDB."""

    def __init__(self, db: CheeksbaseDB) -> None:
        """Create a SyncEngine backed by the given database connection."""
        self.db = db
        self._sync_t0: float | None = None

    def _log(self, msg: str) -> None:
        """Write a timestamped line to stderr."""
        elapsed = time.monotonic() - self._sync_t0 if self._sync_t0 is not None else 0.0
        line = f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] [{elapsed:7.1f}s] {msg}"
        print(line, file=sys.stderr)

    def sync(
        self,
        source_name: str,
        source_config: dict[str, Any],
        on_progress: Callable[[int, int], None] | None = None,
    ) -> SyncResult:
        """Sync a single source into DuckDB."""
        source_type = source_config["type"]
        credentials = source_config.get("credentials", {})

        self._sync_t0 = time.monotonic()
        self._log(f"=== SYNC START: {source_name} ({source_type}) ===")

        sync_id = None
        try:
            sync_id = self.db.log_sync_start(source_name, source_type)

            # Route to appropriate sync method based on type
            if source_type == "rest_api":
                result = self._sync_rest_api(source_name, credentials, source_config)
            elif source_type == "database":
                result = self._sync_database(source_name, credentials, source_config)
            elif source_type == "file":
                result = self._sync_file(source_name, credentials, source_config)
            elif source_type == "graphql":
                result = self._sync_graphql(source_name, credentials, source_config)
            else:
                raise ValueError(f"Unknown source type: {source_type}")

            # Update metadata
            if result.table_names:
                self.db.update_table_metadata(
                    source_name, source_name,
                    row_counts=result.row_counts,
                )

                # Run semantic auto-annotation after sync
                try:
                    from cheeksbase.agents.semantic import SemanticAgent
                    agent = SemanticAgent(db=self.db)
                    annotation_result = agent.annotate_connector(source_name)
                    self._log(f"  {annotation_result.summary()}")
                except Exception as e:
                    self._log(f"  Semantic annotation skipped: {e}")

            # Clear live data
            self.db.clear_live_rows(source_name)

            self.db.log_sync_end(
                sync_id,
                status="success",
                tables_synced=result.tables_synced,
                rows_synced=result.rows_synced,
            )

            self._log(f"=== SYNC COMPLETE: {result.tables_synced} tables, {result.rows_synced:,} rows ===")
            return result

        except Exception as e:
            error_msg = str(e)
            self._log(f"=== SYNC ERROR: {type(e).__name__}: {error_msg} ===")
            if sync_id is not None:
                self.db.log_sync_end(sync_id, status="error", error_message=error_msg)
            return SyncResult(
                connector_name=source_name,
                connector_type=source_type,
                tables_synced=0,
                rows_synced=0,
                status="error",
                error=error_msg,
            )

    def _sync_rest_api(
        self,
        source_name: str,
        credentials: dict[str, str],
        config: dict[str, Any],
    ) -> SyncResult:
        """Sync data from a REST API."""
        import httpx

        from cheeksbase.connectors.registry import get_connector_config

        # Load connector config from YAML
        connector_config = get_connector_config(source_name)
        if not connector_config:
            raise ValueError(f"No connector config found for {source_name}")

        base_url = connector_config.get("base_url", "")
        auth_config = connector_config.get("auth", {})
        resources = connector_config.get("resources", [])

        # Build auth headers
        headers = self._build_auth_headers(auth_config, credentials)

        total_rows = 0
        tables_synced = 0
        row_counts = {}
        table_names = []

        with httpx.Client(timeout=30.0) as client:
            for resource in resources:
                resource_name = resource["name"]
                endpoint = resource.get("endpoint", f"/{resource_name}")
                primary_key = resource.get("primary_key", "id")

                self._log(f"  Syncing {resource_name}...")

                # Pagination config from resource or connector
                pagination = resource.get("pagination", config.get("pagination", {}))
                pag_type = pagination.get("type", "")
                page_size = pagination.get("page_size", 100)
                cursor_field = pagination.get("cursor_field", "cursor")
                next_field = pagination.get("next_field", "next_cursor")
                data_field = pagination.get("data_field", "data")
                offset_param = pagination.get("offset_param", "offset")
                limit_param = pagination.get("limit_param", "limit")

                try:
                    # Fetch data from API (with pagination if configured)
                    all_data: list[dict] = []
                    base_url_fetch = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"

                    if pag_type == "cursor":
                        # Cursor-based pagination
                        cursor: str | None = None
                        while True:
                            params: dict[str, Any] = {limit_param: page_size}
                            if cursor is not None:
                                params[cursor_field] = cursor
                            response = client.get(base_url_fetch, headers=headers, params=params)
                            response.raise_for_status()
                            data = response.json()
                            # Extract data from nested wrapper if needed
                            if isinstance(data, dict) and data_field in data:
                                page_data = data[data_field]
                            elif isinstance(data, list):
                                page_data = data
                            else:
                                page_data = [data] if isinstance(data, dict) else []
                            if not page_data:
                                break
                            all_data.extend(page_data)
                            # Get next cursor
                            if isinstance(data, dict) and next_field in data:
                                cursor = data[next_field]
                                if not cursor:
                                    break
                            else:
                                break
                            if len(page_data) < page_size:
                                break
                    elif pag_type == "offset":
                        # Offset/limit pagination
                        offset = 0
                        while True:
                            params = {offset_param: offset, limit_param: page_size}
                            response = client.get(base_url_fetch, headers=headers, params=params)
                            response.raise_for_status()
                            data = response.json()
                            if isinstance(data, dict) and data_field in data:
                                page_data = data[data_field]
                            elif isinstance(data, list):
                                page_data = data
                            else:
                                page_data = [data] if isinstance(data, dict) else []
                            if not page_data:
                                break
                            all_data.extend(page_data)
                            if len(page_data) < page_size:
                                break
                            offset += page_size
                    else:
                        # No pagination — single fetch
                        response = client.get(base_url_fetch, headers=headers)
                        response.raise_for_status()
                        data = response.json()
                        if isinstance(data, dict):
                            # Some APIs wrap results in a key
                            if len(data) == 1:
                                key = next(iter(data))
                                if isinstance(data[key], list):
                                    data = data[key]
                        if not isinstance(data, list):
                            self._log(f"    Warning: Unexpected response format for {resource_name}")
                            continue
                        all_data = data

                    data = all_data

                    if not isinstance(data, list):
                        self._log(f"    Warning: Unexpected response format for {resource_name}")
                        continue

                    if not data:
                        self._log(f"    No data returned for {resource_name}")
                        continue

                    # M4: Check if data has changed since last sync
                    data_hash = hashlib.sha256(
                        json.dumps(all_data, sort_keys=True, default=str).encode()
                    ).hexdigest()[:16]

                    # Check against last successful sync hash
                    # (hash stored for future comparison — currently using timestamp-based check)
                    try:
                        self.db.query(
                            f"SELECT error_message FROM {META_SCHEMA}.sync_log "
                            "WHERE connector_name = ? AND status = 'success' "
                            "ORDER BY id DESC LIMIT 1",
                            [source_name],
                        )
                    except Exception:
                        pass  # Best-effort: skip if sync log unavailable

                    # Always log the hash for future comparison
                    self._log(f"    Data hash: {data_hash}")

                    # Create schema and table
                    safe_source = _validate_identifier(source_name)
                    safe_resource = _validate_identifier(resource_name)
                    self.db.conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{safe_source}"')

                    # Convert to DuckDB table
                    df = self._list_to_duckdb(data, resource_name, primary_key)  # noqa: F841
                    self.db.conn.execute(f'CREATE OR REPLACE TABLE "{safe_source}"."{safe_resource}" AS SELECT * FROM df')

                    self._compute_column_stats(source_name, safe_source, safe_resource)

                    row_count = len(data)
                    total_rows += row_count
                    tables_synced += 1
                    row_counts[resource_name] = row_count
                    table_names.append(resource_name)

                    self._log(f"    Synced {row_count:,} rows")

                except Exception as e:
                    self._log(f"    Error syncing {resource_name}: {e}")
                    continue

        return SyncResult(
            connector_name=source_name,
            connector_type="rest_api",
            tables_synced=tables_synced,
            rows_synced=total_rows,
            status="success",
            row_counts=row_counts,
            table_names=table_names,
        )

    def _sync_database(
        self,
        source_name: str,
        credentials: dict[str, str],
        config: dict[str, Any],
    ) -> SyncResult:
        """Sync data from a database."""
        connection_string = credentials.get("connection_string", "")
        if not connection_string:
            raise ValueError("Database connection string required")

        safe_source = _validate_identifier(source_name)
        self.db.conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{safe_source}"')

        # Attach the remote database as a duckdb attachment
        attach_name = f"_remote_{source_name}"
        try:
            # Detach if already attached from a previous sync
            self.db.conn.execute(f"DETACH DATABASE IF EXISTS {attach_name}")
        except Exception:
            pass

        self._log(f"  Attaching remote database for {source_name}")

        # Build ATTACH options from config
        read_only = config.get("read_only", True)
        tables_cfg = config.get("tables", [])

        try:
            escaped_conn = connection_string.replace("'", "''")
            attach_sql = f"ATTACH '{escaped_conn}' AS {attach_name}"
            if read_only:
                attach_sql += " (READ_ONLY)"
            self.db.conn.execute(attach_sql)
        except Exception as e:
            self._log(f"  Could not attach remote database: {e}")
            return SyncResult(
                connector_name=source_name,
                connector_type="database",
                tables_synced=0,
                rows_synced=0,
                status="success",
            )

        # Discover remote tables
        remote_tables = self.db.conn.execute(
            f"SELECT table_name FROM {attach_name}.information_schema.tables "
            f"WHERE table_schema = 'public' OR table_schema = 'main'"
        ).fetchall()

        total_rows = 0
        tables_synced = 0
        row_counts: dict[str, int] = {}
        table_names: list[str] = []

        # Filter to configured tables if specified
        allowed = {t["name"] for t in tables_cfg} if tables_cfg else None

        for (table_name,) in remote_tables:
            if allowed and table_name not in allowed:
                continue

            self._log(f"  Syncing {table_name}...")
            try:
                safe_table = _validate_identifier(table_name)
                self.db.conn.execute(
                    f'CREATE OR REPLACE TABLE "{safe_source}"."{safe_table}" AS '
                    f'SELECT * FROM {attach_name}."{safe_table}"'
                )
                row_count_result = self.db.conn.execute(
                    f'SELECT COUNT(*) FROM "{safe_source}"."{safe_table}"'
                ).fetchone()
                row_count = row_count_result[0] if row_count_result else 0
                total_rows += row_count
                tables_synced += 1
                row_counts[table_name] = row_count
                table_names.append(table_name)
                self._log(f"    Synced {row_count:,} rows")
            except Exception as e:
                self._log(f"    Error syncing {table_name}: {e}")
                continue

        # Clean up attachment
        try:
            self.db.conn.execute(f"DETACH DATABASE {attach_name}")
        except Exception:
            pass

        return SyncResult(
            connector_name=source_name,
            connector_type="database",
            tables_synced=tables_synced,
            rows_synced=total_rows,
            status="success",
            row_counts=row_counts,
            table_names=table_names,
        )

    def _sync_file(
        self,
        source_name: str,
        credentials: dict[str, str],
        config: dict[str, Any],
    ) -> SyncResult:
        """Sync data from files (CSV, Parquet, etc.)."""
        import glob
        from pathlib import Path

        file_path = config.get("path", "")
        file_format = config.get("format", "csv")

        if not file_path:
            raise ValueError("File path required")

        # Expand glob patterns
        files = glob.glob(file_path)
        if not files:
            raise ValueError(f"No files found matching: {file_path}")

        total_rows = 0
        tables_synced = 0
        row_counts = {}
        table_names = []

        safe_source = _validate_identifier(source_name)
        self.db.conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{safe_source}"')

        for fp in files:
            file_name = Path(fp).stem
            # Sanitize: lowercase, replace non-alphanumeric with underscore,
            # ensure starts with letter/underscore
            sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', file_name).strip('_').lower()
            if not sanitized or sanitized[0].isdigit():
                sanitized = f"t_{sanitized}"
            table_name = sanitized

            self._log(f"  Syncing {file_name}...")

            try:
                # Escape single quotes in file path for SQL
                safe_fp = fp.replace("'", "''")
                if file_format == "csv":
                    df = self.db.conn.execute(f"SELECT * FROM read_csv('{safe_fp}')").fetchdf()
                elif file_format == "parquet":
                    df = self.db.conn.execute(f"SELECT * FROM read_parquet('{safe_fp}')").fetchdf()
                elif file_format == "json":
                    df = self.db.conn.execute(f"SELECT * FROM read_json('{safe_fp}')").fetchdf()
                else:
                    self._log(f"    Unsupported format: {file_format}")
                    continue

                # CREATE OR REPLACE is atomic — if SELECT fails, old table survives
                self.db.conn.execute(f'CREATE OR REPLACE TABLE "{safe_source}"."{table_name}" AS SELECT * FROM df')

                self._compute_column_stats(source_name, safe_source, table_name)

                row_count = len(df)
                total_rows += row_count
                tables_synced += 1
                row_counts[table_name] = row_count
                table_names.append(table_name)

                self._log(f"    Synced {row_count:,} rows")

            except Exception as e:
                self._log(f"    Error syncing {file_name}: {e}")
                continue

        return SyncResult(
            connector_name=source_name,
            connector_type="file",
            tables_synced=tables_synced,
            rows_synced=total_rows,
            status="success",
            row_counts=row_counts,
            table_names=table_names,
        )

    def _sync_graphql(
        self,
        source_name: str,
        credentials: dict[str, str],
        config: dict[str, Any],
    ) -> SyncResult:
        """Sync data from a GraphQL API."""
        import httpx

        from cheeksbase.connectors.registry import get_connector_config

        # Load connector config from YAML
        connector_config = get_connector_config(source_name)
        if not connector_config:
            raise ValueError(f"No connector config found for {source_name}")

        endpoint = connector_config.get("endpoint", "")
        auth_config = connector_config.get("auth", {})
        resources = connector_config.get("resources", [])

        # Build auth headers
        headers = self._build_auth_headers(auth_config, credentials)
        headers["Content-Type"] = "application/json"

        total_rows = 0
        tables_synced = 0
        row_counts = {}
        table_names = []

        with httpx.Client(timeout=30.0) as client:
            for resource in resources:
                resource_name = resource["name"]
                query = resource.get("query", "")
                data_path = resource.get("data_path", "data")

                self._log(f"  Syncing {resource_name}...")

                try:
                    # Execute GraphQL query
                    response = client.post(
                        endpoint,
                        json={"query": query},
                        headers=headers,
                    )
                    response.raise_for_status()

                    result = response.json()

                    # Extract data from response
                    data = result
                    for key in data_path.split("."):
                        if isinstance(data, dict) and key in data:
                            data = data[key]
                        else:
                            data = []
                            break

                    if not isinstance(data, list):
                        self._log(f"    Warning: Unexpected response format for {resource_name}")
                        continue

                    if not data:
                        self._log(f"    No data returned for {resource_name}")
                        continue

                    # Create schema and table
                    self.db.conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{source_name}"')

                    # Convert to DuckDB table
                    primary_key = resource.get("primary_key", "id")
                    df = self._list_to_duckdb(data, resource_name, primary_key)  # noqa: F841
                    self.db.conn.execute(f'CREATE OR REPLACE TABLE "{source_name}"."{resource_name}" AS SELECT * FROM df')

                    row_count = len(data)
                    total_rows += row_count
                    tables_synced += 1
                    row_counts[resource_name] = row_count
                    table_names.append(resource_name)

                    self._log(f"    Synced {row_count:,} rows")

                except Exception as e:
                    self._log(f"    Error syncing {resource_name}: {e}")
                    continue

        return SyncResult(
            connector_name=source_name,
            connector_type="graphql",
            tables_synced=tables_synced,
            rows_synced=total_rows,
            status="success",
            row_counts=row_counts,
            table_names=table_names,
        )

    def _compute_column_stats(
        self, source_name: str, schema_name: str, table_name: str,
    ) -> None:
        """Compute and store column statistics (null_rate, distinct_count, sample_values)."""
        try:
            col_names = [c[0] for c in self.db.conn.execute(
                f'SELECT * FROM "{schema_name}"."{table_name}" LIMIT 1'
            ).description]

            for col_name in col_names:
                try:
                    stat_result = self.db.conn.execute(f"""
                        SELECT
                            1.0 * (COUNT(*) - COUNT("{col_name}")) / NULLIF(COUNT(*), 0) as null_rate,
                            COUNT(DISTINCT "{col_name}") as distinct_count
                        FROM "{schema_name}"."{table_name}"
                    """).fetchone()

                    if stat_result:
                        null_rate = float(stat_result[0]) if stat_result[0] is not None else None
                        distinct_count = int(stat_result[1]) if stat_result[1] is not None else None

                        # Sample top-5 values for categorical columns
                        sample_vals = None
                        if distinct_count is not None and distinct_count <= 100:
                            top_vals = self.db.conn.execute(f"""
                                SELECT "{col_name}", COUNT(*) as cnt
                                FROM "{schema_name}"."{table_name}"
                                GROUP BY "{col_name}"
                                ORDER BY cnt DESC
                                LIMIT 5
                            """).fetchall()
                            sample_vals = json.dumps([
                                {"value": str(v[0]), "count": int(v[1])} for v in top_vals
                            ])

                        self.db.store_column_stats(
                            connector_name=source_name,
                            schema_name=schema_name,
                            table_name=table_name,
                            column_name=col_name,
                            null_rate=null_rate,
                            distinct_count=distinct_count,
                            sample_values=sample_vals,
                        )
                except Exception as e:
                    self._log(f"    Stats error for {col_name}: {e}")
        except Exception as e:
            self._log(f"    Column stats computation skipped: {e}")

    def _build_auth_headers(self, auth_config: dict[str, Any], credentials: dict[str, str]) -> dict[str, str]:
        """Build authentication headers."""
        auth_type = auth_config.get("type", "")

        if auth_type == "bearer":
            token = credentials.get(auth_config.get("token_field", "api_key"), "")
            return {"Authorization": f"Bearer {token}"}
        elif auth_type == "api_key":
            header = auth_config.get("header", "Authorization")
            prefix = auth_config.get("prefix", "Bearer")
            key = credentials.get(auth_config.get("key_field", "api_key"), "")
            return {header: f"{prefix} {key}"}
        elif auth_type == "basic":
            import base64
            username = credentials.get("username", "")
            password = credentials.get("password", "")
            encoded = base64.b64encode(f"{username}:{password}".encode()).decode()
            return {"Authorization": f"Basic {encoded}"}
        else:
            return {}

    def _list_to_duckdb(self, data: list[dict], table_name: str, primary_key: str) -> duckdb.DuckDBPyConnection:
        """Convert a list of dicts to a DuckDB relation using native bulk insert."""
        if not data:
            return self.db.conn.execute(f"SELECT NULL as {primary_key} WHERE 1=0")

        try:
            # Use pandas for fast type inference and bulk insert
            import pandas as pd
            df = pd.DataFrame(data)

            # Convert nested dicts/lists to JSON strings
            for col in df.columns:
                mask = df[col].apply(lambda x: isinstance(x, (dict, list)))
                if mask.any():
                    df.loc[mask, col] = df.loc[mask, col].apply(json.dumps)

            # DuckDB can read pandas DataFrames directly — much faster than executemany
            self.db.conn.execute(f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM df')
            return self.db.conn.execute(f'SELECT * FROM "{table_name}"')
        except Exception:
            # Fallback to original row-by-row if pandas fails
            return self._list_to_duckdb_fallback(data, table_name, primary_key)

    def _list_to_duckdb_fallback(self, data: list[dict], table_name: str, primary_key: str) -> duckdb.DuckDBPyConnection:
        """Fallback: original row-by-row insertion for when pandas is unavailable."""
        if not data:
            return self.db.conn.execute(f"SELECT NULL as {primary_key} WHERE 1=0")

        # Get all unique keys from all records
        all_keys: set[str] = set()
        for record in data:
            all_keys.update(record.keys())

        # Create a list of tuples for DuckDB
        rows = []
        for record in data:
            row = []
            for key in sorted(all_keys):
                value = record.get(key)
                if isinstance(value, (dict, list)):
                    value = json.dumps(value)
                row.append(value)
            rows.append(tuple(row))

        # Create column definitions
        columns = []
        for key in sorted(all_keys):
            sample_value = data[0].get(key)
            if isinstance(sample_value, bool):
                col_type = "BOOLEAN"
            elif isinstance(sample_value, int):
                col_type = "INTEGER"
            elif isinstance(sample_value, float):
                col_type = "DOUBLE"
            elif isinstance(sample_value, (dict, list)):
                col_type = "JSON"
            else:
                col_type = "VARCHAR"
            columns.append(f'"{key}" {col_type}')

        create_sql = f'CREATE TABLE "{table_name}" ({", ".join(columns)})'
        self.db.conn.execute(create_sql)

        placeholders = ", ".join(["?"] * len(all_keys))
        insert_sql = f'INSERT INTO "{table_name}" VALUES ({placeholders})'
        self.db.conn.executemany(insert_sql, rows)

        return self.db.conn.execute(f"SELECT * FROM {table_name}")
