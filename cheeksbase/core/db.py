"""DuckDB storage layer for Cheeksbase."""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import TracebackType
from typing import Any

import duckdb

from cheeksbase.core.config import get_db_path

# Internal schema for cheeksbase metadata
META_SCHEMA = "_cheeksbase"


def _validate_identifier(name: str) -> str:
    """Validate and return a SQL identifier (schema/table/column).

    Only allows [a-zA-Z_][a-zA-Z0-9_]* to prevent SQL injection.
    """
    if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return name


INIT_SQL = f"""
CREATE SCHEMA IF NOT EXISTS {META_SCHEMA};

CREATE SEQUENCE IF NOT EXISTS {META_SCHEMA}.sync_log_seq START 1;

CREATE TABLE IF NOT EXISTS {META_SCHEMA}.sync_log (
    id INTEGER PRIMARY KEY DEFAULT nextval('{META_SCHEMA}.sync_log_seq'),
    connector_name VARCHAR NOT NULL,
    connector_type VARCHAR NOT NULL,
    started_at TIMESTAMP DEFAULT current_timestamp,
    finished_at TIMESTAMP,
    status VARCHAR DEFAULT 'running',
    tables_synced INTEGER DEFAULT 0,
    rows_synced BIGINT DEFAULT 0,
    error_message VARCHAR
);

CREATE TABLE IF NOT EXISTS {META_SCHEMA}.tables (
    connector_name VARCHAR NOT NULL,
    schema_name VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,
    row_count BIGINT DEFAULT 0,
    last_sync TIMESTAMP,
    description VARCHAR,
    PRIMARY KEY (connector_name, schema_name, table_name)
);

CREATE TABLE IF NOT EXISTS {META_SCHEMA}.columns (
    connector_name VARCHAR NOT NULL,
    schema_name VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,
    column_name VARCHAR NOT NULL,
    column_type VARCHAR,
    is_nullable BOOLEAN DEFAULT true,
    description VARCHAR,
    note VARCHAR,
    PRIMARY KEY (connector_name, schema_name, table_name, column_name)
);

CREATE TABLE IF NOT EXISTS {META_SCHEMA}.live_rows (
    connector_name VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,
    record_id VARCHAR NOT NULL,
    row_data JSON NOT NULL,
    written_at TIMESTAMP DEFAULT current_timestamp,
    mutation_id VARCHAR,
    PRIMARY KEY (connector_name, table_name, record_id)
);

CREATE SEQUENCE IF NOT EXISTS {META_SCHEMA}.mutation_seq START 1;

CREATE TABLE IF NOT EXISTS {META_SCHEMA}.mutations (
    id INTEGER PRIMARY KEY DEFAULT nextval('{META_SCHEMA}.mutation_seq'),
    mutation_id VARCHAR NOT NULL UNIQUE,
    connector_name VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,
    operation VARCHAR NOT NULL,
    sql_text VARCHAR NOT NULL,
    preview JSON,
    status VARCHAR DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT current_timestamp,
    confirmed_at TIMESTAMP,
    executed_at TIMESTAMP,
    result JSON,
    error_message VARCHAR
);

CREATE TABLE IF NOT EXISTS {META_SCHEMA}.relationships (
    from_schema      VARCHAR NOT NULL,
    from_table       VARCHAR NOT NULL,
    from_column      VARCHAR NOT NULL,
    to_schema        VARCHAR NOT NULL,
    to_table         VARCHAR NOT NULL,
    to_column        VARCHAR NOT NULL,
    cardinality      VARCHAR NOT NULL DEFAULT 'one_to_many',
    confidence       FLOAT   NOT NULL DEFAULT 1.0,
    description      VARCHAR,
    detected_at      TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (from_schema, from_table, from_column,
                 to_schema,   to_table,   to_column)
);

CREATE TABLE IF NOT EXISTS {META_SCHEMA}.metadata (
    schema_name  VARCHAR NOT NULL,
    table_name   VARCHAR NOT NULL,
    column_name  VARCHAR NOT NULL DEFAULT '',
    key          VARCHAR NOT NULL,
    value        VARCHAR,
    PRIMARY KEY (schema_name, table_name, column_name, key)
);

-- Query cache: persistent cross-call query result cache
CREATE TABLE IF NOT EXISTS {META_SCHEMA}.query_cache (
    cache_key VARCHAR PRIMARY KEY,
    sql_text VARCHAR NOT NULL,
    max_rows INTEGER NOT NULL,
    result_json VARCHAR NOT NULL,
    created_at TIMESTAMP DEFAULT current_timestamp,
    expires_at TIMESTAMP NOT NULL
);

-- Query history: tracks what agents query for institutional knowledge
CREATE SEQUENCE IF NOT EXISTS {META_SCHEMA}.query_history_seq START 1;

CREATE TABLE IF NOT EXISTS {META_SCHEMA}.query_history (
    id INTEGER PRIMARY KEY DEFAULT nextval('{META_SCHEMA}.query_history_seq'),
    sql_text VARCHAR NOT NULL,
    tables_used VARCHAR,
    rows_returned INTEGER,
    duration_ms INTEGER,
    timestamp TIMESTAMP DEFAULT current_timestamp,
    error_message VARCHAR
);

-- Query templates: reusable query patterns discovered from successful queries
CREATE SEQUENCE IF NOT EXISTS {META_SCHEMA}.query_templates_seq START 1;

CREATE TABLE IF NOT EXISTS {META_SCHEMA}.query_templates (
    id INTEGER PRIMARY KEY DEFAULT nextval('{META_SCHEMA}.query_templates_seq'),
    schema_name VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,
    query_sql VARCHAR NOT NULL,
    description VARCHAR,
    times_used INTEGER DEFAULT 0,
    success_rate FLOAT DEFAULT 1.0,
    created_at TIMESTAMP DEFAULT current_timestamp
);

CREATE SEQUENCE IF NOT EXISTS {META_SCHEMA}.shared_memory_seq START 1;

CREATE TABLE IF NOT EXISTS {META_SCHEMA}.shared_memory (
    id INTEGER PRIMARY KEY DEFAULT nextval('{META_SCHEMA}.shared_memory_seq'),
    source_agent VARCHAR NOT NULL,
    scope VARCHAR NOT NULL DEFAULT 'broadcast',
    key VARCHAR NOT NULL UNIQUE,
    value VARCHAR NOT NULL,
    embedding FLOAT[] DEFAULT NULL,
    tags VARCHAR DEFAULT NULL,
    created_at TIMESTAMP DEFAULT now(),
    expires_at TIMESTAMP DEFAULT NULL,
    updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {META_SCHEMA}.agent_runs (
    run_id VARCHAR PRIMARY KEY,
    agent_name VARCHAR NOT NULL,
    profile_name VARCHAR,
    workspace_id VARCHAR,
    role VARCHAR,
    status VARCHAR DEFAULT 'active',
    current_task VARCHAR,
    current_summary VARCHAR,
    progress DOUBLE,
    started_at TIMESTAMP DEFAULT current_timestamp,
    last_heartbeat_at TIMESTAMP DEFAULT current_timestamp,
    finished_at TIMESTAMP,
    metadata_json JSON
);

CREATE SEQUENCE IF NOT EXISTS {META_SCHEMA}.agent_events_seq START 1;

CREATE TABLE IF NOT EXISTS {META_SCHEMA}.agent_events (
    id INTEGER PRIMARY KEY DEFAULT nextval('{META_SCHEMA}.agent_events_seq'),
    event_id VARCHAR NOT NULL UNIQUE,
    run_id VARCHAR NOT NULL,
    workspace_id VARCHAR,
    event_type VARCHAR NOT NULL,
    task_id VARCHAR,
    file_path VARCHAR,
    payload_json JSON,
    summary_text VARCHAR,
    ts TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS {META_SCHEMA}.resource_claims (
    resource_key VARCHAR PRIMARY KEY,
    resource_type VARCHAR NOT NULL,
    workspace_id VARCHAR,
    claimed_by VARCHAR NOT NULL,
    task_id VARCHAR,
    claimed_at TIMESTAMP DEFAULT current_timestamp,
    lease_expires_at TIMESTAMP NOT NULL,
    released_at TIMESTAMP,
    status VARCHAR DEFAULT 'claimed',
    metadata_json JSON
);

CREATE OR REPLACE VIEW {META_SCHEMA}.active_agent_runs AS
SELECT *
FROM {META_SCHEMA}.agent_runs
WHERE status IN ('registered', 'active', 'idle', 'blocked');

CREATE OR REPLACE VIEW {META_SCHEMA}.active_resource_claims AS
SELECT *
FROM {META_SCHEMA}.resource_claims
WHERE status = 'claimed'
  AND lease_expires_at > current_timestamp;

CREATE INDEX IF NOT EXISTS idx_agent_runs_workspace_status
    ON {META_SCHEMA}.agent_runs(workspace_id, status);

CREATE INDEX IF NOT EXISTS idx_agent_events_workspace_ts
    ON {META_SCHEMA}.agent_events(workspace_id, ts);

CREATE INDEX IF NOT EXISTS idx_agent_events_run_ts
    ON {META_SCHEMA}.agent_events(run_id, ts);

CREATE INDEX IF NOT EXISTS idx_resource_claims_workspace_status
    ON {META_SCHEMA}.resource_claims(workspace_id, status);
"""


_META_TABLES = [
    "sync_log",
    "tables",
    "columns",
    "live_rows",
    "mutations",
    "relationships",
    "metadata",
    "query_cache",
    "query_history",
    "query_templates",
    "shared_memory",
    "agent_runs",
    "agent_events",
    "resource_claims",
]


class CheeksbaseDB:
    """DuckDB wrapper with metadata management."""

    def __init__(self, db_path: Path | str | None = None) -> None:
        """Open a Cheeksbase DuckDB database.

        If *db_path* is omitted, the default path from config is used.
        The connection is created lazily on first access.
        """
        self.db_path = str(db_path or get_db_path())
        self._conn: duckdb.DuckDBPyConnection | None = None

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        """Get or create the database connection."""
        if self._conn is None:
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
            self._conn = duckdb.connect(self.db_path)
            self._init_metadata()
        return self._conn

    def _init_metadata(self) -> None:
        """Initialize metadata tables."""
        for statement in INIT_SQL.split(";"):
            stmt = statement.strip()
            if stmt:
                self.conn.execute(stmt)

        # Migration: add statistical columns if they don't exist
        for col_sql in [
            "ALTER TABLE _cheeksbase.columns ADD COLUMN IF NOT EXISTS null_rate FLOAT",
            "ALTER TABLE _cheeksbase.columns ADD COLUMN IF NOT EXISTS distinct_count BIGINT",
            "ALTER TABLE _cheeksbase.columns ADD COLUMN IF NOT EXISTS sample_values VARCHAR",
            "ALTER TABLE _cheeksbase.columns ADD COLUMN IF NOT EXISTS min_value VARCHAR",
            "ALTER TABLE _cheeksbase.columns ADD COLUMN IF NOT EXISTS max_value VARCHAR",
        ]:
            try:
                self.conn.execute(col_sql)
            except duckdb.CatalogException:
                pass  # Column already exists (older DuckDB without IF NOT EXISTS)

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> duckdb.DuckDBPyConnection:
        """Execute a SQL query and return the connection for chaining."""
        if params:
            return self.conn.execute(sql, params)
        return self.conn.execute(sql)

    def query(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        """Execute a query and return results as a list of dicts."""
        if params:
            result = self.conn.execute(sql, params)
        else:
            result = self.conn.execute(sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    def query_raw(self, sql: str) -> tuple[list[str], list[tuple]]:
        """Execute a query and return (column_names, rows)."""
        result = self.conn.execute(sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return columns, rows

    def get_schemas(self) -> list[str]:
        """List all user schemas (excluding internal ones)."""
        rows = self.query(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'main')"
        )
        return [r["schema_name"] for r in rows]

    def get_tables(self, schema: str) -> list[str]:
        """List all tables in a schema."""
        res = self.conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = ? ORDER BY table_name",
            [schema],
        )
        return [row[0] for row in res.fetchall()]

    def get_columns(self, schema: str, table: str) -> list[dict[str, Any]]:
        """Get column info for a table."""
        res = self.conn.execute(
            "SELECT column_name, data_type, is_nullable "
            "FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? "
            "ORDER BY ordinal_position",
            [schema, table],
        )
        cols = [d[0] for d in res.description]
        return [dict(zip(cols, row)) for row in res.fetchall()]

    def get_row_count(self, schema: str, table: str) -> int:
        """Get row count for a table."""
        safe_schema = _validate_identifier(schema)
        safe_table = _validate_identifier(table)
        rows = self.query(f'SELECT COUNT(*) as cnt FROM "{safe_schema}"."{safe_table}"')
        return rows[0]["cnt"] if rows else 0

    def log_sync_start(self, connector_name: str, connector_type: str) -> int:
        """Record the start of a sync. Returns the sync log ID."""
        result = self.conn.execute(
            f"INSERT INTO {META_SCHEMA}.sync_log (connector_name, connector_type) "
            f"VALUES (?, ?) "
            f"RETURNING id",
            [connector_name, connector_type],
        )
        row = result.fetchone()
        if row is None:
            raise RuntimeError("Sync log INSERT RETURNING returned no row")
        return row[0]

    def log_sync_end(
        self,
        sync_id: int,
        status: str,
        tables_synced: int = 0,
        rows_synced: int = 0,
        error_message: str | None = None,
    ) -> None:
        """Record the end of a sync."""
        self.conn.execute(
            f"UPDATE {META_SCHEMA}.sync_log "
            f"SET finished_at = current_timestamp, status = ?, "
            f"tables_synced = ?, rows_synced = ?, error_message = ? "
            f"WHERE id = ?",
            [status, tables_synced, rows_synced, error_message, sync_id],
        )

    def update_table_metadata(
        self,
        connector_name: str,
        schema_name: str,
        annotations: dict[str, dict[str, str]] | None = None,
        row_counts: dict[str, int] | None = None,
    ) -> None:
        """Update table metadata after sync."""
        tables = self.get_tables(schema_name)
        for table_name in tables:
            row_count = row_counts.get(table_name, 0) if row_counts else self.get_row_count(schema_name, table_name)

            # Update tables metadata
            self.conn.execute(
                f"INSERT INTO {META_SCHEMA}.tables "
                f"(connector_name, schema_name, table_name, row_count, last_sync) "
                f"VALUES (?, ?, ?, ?, current_timestamp) "
                f"ON CONFLICT (connector_name, schema_name, table_name) "
                f"DO UPDATE SET row_count = excluded.row_count, last_sync = excluded.last_sync",
                [connector_name, schema_name, table_name, row_count],
            )

            # Update column metadata if annotations provided
            if annotations and table_name in annotations:
                table_annotations = annotations[table_name]
                if isinstance(table_annotations, dict):
                    for col_name, col_annotations in table_annotations.items():
                        if isinstance(col_annotations, dict):
                            for key, value in col_annotations.items():
                                if key in ("description", "note"):
                                    self.conn.execute(
                                        f"INSERT INTO {META_SCHEMA}.columns "
                                        f"(connector_name, schema_name, table_name, column_name, {key}) "
                                        f"VALUES (?, ?, ?, ?, ?) "
                                        f"ON CONFLICT (connector_name, schema_name, table_name, column_name) "
                                        f"DO UPDATE SET {key} = excluded.{key}",
                                        [connector_name, schema_name, table_name, col_name, value],
                                    )

    def get_column_annotations(self, schema: str, table: str) -> dict[str, dict[str, str]]:
        """Get annotations for all columns in a table."""
        rows = self.query(
            f"SELECT column_name, description, note FROM {META_SCHEMA}.columns "
            f"WHERE schema_name = ? AND table_name = ?",
            [schema, table],
        )
        result = {}
        for row in rows:
            col_name = row["column_name"]
            annotations = {}
            if row["description"]:
                annotations["description"] = row["description"]
            if row["note"]:
                annotations["note"] = row["note"]
            if annotations:
                result[col_name] = annotations
        return result

    def get_table_description(self, schema: str, table: str) -> str | None:
        """Get table description."""
        rows = self.query(
            f"SELECT description FROM {META_SCHEMA}.tables "
            f"WHERE schema_name = ? AND table_name = ?",
            [schema, table],
        )
        return rows[0]["description"] if rows else None

    def set_table_description(self, schema: str, table: str, description: str) -> None:
        """Set table description."""
        self.conn.execute(
            f"UPDATE {META_SCHEMA}.tables SET description = ? "
            f"WHERE schema_name = ? AND table_name = ?",
            [description, schema, table],
        )

    def get_metadata(self, schema: str, table: str, column: str = "") -> dict[str, str]:
        """Get metadata key-value pairs for a table or column."""
        rows = self.query(
            f"SELECT key, value FROM {META_SCHEMA}.metadata "
            f"WHERE schema_name = ? AND table_name = ? AND column_name = ?",
            [schema, table, column],
        )
        return {row["key"]: row["value"] for row in rows}

    def set_metadata(self, schema: str, table: str, key: str, value: str, column: str = "") -> None:
        """Set a metadata key-value pair."""
        self.conn.execute(
            f"INSERT INTO {META_SCHEMA}.metadata "
            f"(schema_name, table_name, column_name, key, value) "
            f"VALUES (?, ?, ?, ?, ?) "
            f"ON CONFLICT (schema_name, table_name, column_name, key) "
            f"DO UPDATE SET value = excluded.value",
            [schema, table, column, key, value],
        )

    def get_relationships(self, schema: str, table: str) -> list[dict[str, Any]]:
        """Get relationships involving a table."""
        rows = self.query(
            f"SELECT * FROM {META_SCHEMA}.relationships "
            f"WHERE (from_schema = ? AND from_table = ?) "
            f"OR (to_schema = ? AND to_table = ?)",
            [schema, table, schema, table],
        )
        return rows

    def upsert_relationship(
        self,
        from_schema: str,
        from_table: str,
        from_column: str,
        to_schema: str,
        to_table: str,
        to_column: str,
        cardinality: str = "one_to_many",
        confidence: float = 1.0,
        description: str = "",
    ) -> None:
        """Insert or update a relationship."""
        self.conn.execute(
            f"INSERT INTO {META_SCHEMA}.relationships "
            f"(from_schema, from_table, from_column, to_schema, to_table, to_column, "
            f"cardinality, confidence, description) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
            f"ON CONFLICT (from_schema, from_table, from_column, to_schema, to_table, to_column) "
            f"DO UPDATE SET cardinality = excluded.cardinality, "
            f"confidence = excluded.confidence, description = excluded.description",
            [from_schema, from_table, from_column, to_schema, to_table, to_column,
             cardinality, confidence, description],
        )

    def clear_live_rows(self, connector_name: str) -> None:
        """Clear live rows for a connector."""
        self.conn.execute(
            f"DELETE FROM {META_SCHEMA}.live_rows WHERE connector_name = ?",
            [connector_name],
        )

    def get_query_cache(self, cache_key: str) -> dict[str, Any] | None:
        """Get a cached query result by key."""
        rows = self.query(
            "SELECT result_json, expires_at FROM _cheeksbase.query_cache "
            "WHERE cache_key = ? AND expires_at > current_timestamp",
            [cache_key],
        )
        if rows:
            return json.loads(rows[0]["result_json"])
        return None

    def set_query_cache(
        self,
        cache_key: str,
        sql: str,
        max_rows: int,
        result: dict[str, Any],
        ttl_seconds: int = 300,
    ) -> None:
        """Cache a query result with TTL."""
        self.conn.execute(
            "INSERT INTO _cheeksbase.query_cache "
            "(cache_key, sql_text, max_rows, result_json, created_at, expires_at) "
            "VALUES (?, ?, ?, ?, current_timestamp, current_timestamp + INTERVAL '" + str(ttl_seconds) + " seconds') "
            "ON CONFLICT (cache_key) DO UPDATE SET "
            "result_json = excluded.result_json, expires_at = excluded.expires_at",
            [cache_key, sql, max_rows, json.dumps(result, default=str)],
        )

    def record_query_history(
        self, sql: str, tables_used: str | None, rows_returned: int,
        duration_ms: int, error: str | None = None,
    ) -> None:
        """Record a query in history for pattern analysis."""
        self.conn.execute(
            "INSERT INTO _cheeksbase.query_history "
            "(sql_text, tables_used, rows_returned, duration_ms, error_message) "
            "VALUES (?, ?, ?, ?, ?)",
            [sql, tables_used, rows_returned, duration_ms, error],
        )

    def add_query_template(
        self, schema: str, table: str, sql: str,
        description: str | None = None,
    ) -> None:
        """Add a query template for a table."""
        self.conn.execute(
            "INSERT INTO _cheeksbase.query_templates "
            "(schema_name, table_name, query_sql, description) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT DO NOTHING",
            [schema, table, sql, description],
        )

    def get_query_templates(self, schema: str, table: str, limit: int = 5) -> list[dict[str, Any]]:
        """Get top query templates for a table."""
        return self.query(
            "SELECT query_sql, description, times_used, success_rate "
            "FROM _cheeksbase.query_templates "
            "WHERE schema_name = ? AND table_name = ? "
            "ORDER BY times_used DESC LIMIT ?",
            [schema, table, limit],
        )

    def store_column_stats(
        self, connector_name: str, schema_name: str, table_name: str,
        column_name: str, null_rate: float | None, distinct_count: int | None,
        sample_values: str | None = None, min_value: str | None = None,
        max_value: str | None = None,
    ) -> None:
        """Store statistical profile for a column."""
        self.conn.execute(
            "INSERT INTO _cheeksbase.columns "
            "(connector_name, schema_name, table_name, column_name, "
            " null_rate, distinct_count, sample_values, min_value, max_value) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (connector_name, schema_name, table_name, column_name) "
            "DO UPDATE SET "
            "null_rate = excluded.null_rate, "
            "distinct_count = excluded.distinct_count, "
            "sample_values = excluded.sample_values, "
            "min_value = excluded.min_value, "
            "max_value = excluded.max_value",
            [connector_name, schema_name, table_name, column_name,
             null_rate, distinct_count, sample_values, min_value, max_value],
        )

    # ── Shared Memory ─────────────────────────────────────────────────────

    def shared_remember(
        self,
        source_agent: str,
        key: str,
        value: str,
        scope: str = "broadcast",
        tags: str | None = None,
        expires_at: str | None = None,
    ) -> dict[str, Any]:
        """Insert or update a shared memory entry. Returns the stored row as dict."""
        self.execute(
            f"INSERT INTO {META_SCHEMA}.shared_memory "
            f"(source_agent, scope, key, value, tags, expires_at) "
            f"VALUES (?, ?, ?, ?, ?, ?) "
            f"ON CONFLICT (key) DO UPDATE SET "
            f"  value = excluded.value, "
            f"  source_agent = excluded.source_agent, "
            f"  scope = excluded.scope, "
            f"  tags = excluded.tags, "
            f"  expires_at = excluded.expires_at, "
            f"  updated_at = now()",
            [source_agent, scope, key, value, tags, expires_at],
        )
        rows = self.query(
            f"SELECT * FROM {META_SCHEMA}.shared_memory WHERE key = ?", [key]
        )
        return rows[0] if rows else {}

    def shared_recall(self, key: str) -> dict[str, Any] | None:
        """Recall a specific shared memory entry by key."""
        rows = self.query(
            f"SELECT * FROM {META_SCHEMA}.shared_memory WHERE key = ?", [key]
        )
        return rows[0] if rows else None

    def shared_recall_all(self, source_agent: str | None = None) -> list[dict[str, Any]]:
        """Recall all shared memories, optionally filtered by source_agent."""
        if source_agent:
            return self.query(
                f"SELECT * FROM {META_SCHEMA}.shared_memory "
                f"WHERE source_agent = ? ORDER BY updated_at DESC",
                [source_agent],
            )
        return self.query(
            f"SELECT * FROM {META_SCHEMA}.shared_memory ORDER BY updated_at DESC"
        )

    def shared_forget(self, key: str) -> bool:
        """Delete a shared memory entry by key. Returns True if something was deleted."""
        self.execute(
            f"DELETE FROM {META_SCHEMA}.shared_memory WHERE key = ?", [key]
        )
        return True

    def shared_search(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        """Search shared memories by keyword or vector similarity.
        If embeddings exist in the table, attempts vector similarity search
        first (requires DuckDB VSS extension). Falls back to keyword ILIKE."""
        # Try vector search if any embeddings exist
        try:
            has_embeddings = self.query(
                f"SELECT 1 FROM {META_SCHEMA}.shared_memory "
                f"WHERE embedding IS NOT NULL LIMIT 1"
            )
            if has_embeddings:
                # Use a simple keyword-to-embedding bridge: search by ILIKE
                # to find candidates, then rank by embedding similarity if available.
                # Full semantic search requires an embedding model on the caller side.
                pattern = f"%{query}%"
                return self.query(
                    f"SELECT *, "
                    f"  CASE WHEN embedding IS NOT NULL "
                    f"    THEN array_cosine_similarity(embedding, embedding) "
                    f"    ELSE 0 END as score "
                    f"FROM {META_SCHEMA}.shared_memory "
                    f"WHERE key ILIKE ? "
                    f"   OR value ILIKE ? "
                    f"   OR tags ILIKE ? "
                    f"ORDER BY updated_at DESC LIMIT ?",
                    [pattern, pattern, pattern, limit],
                )
        except Exception:
            pass  # VSS not available, fall through to keyword search

        # Keyword fallback
        pattern = f"%{query}%"
        return self.query(
            f"SELECT * FROM {META_SCHEMA}.shared_memory "
            f"WHERE key ILIKE ? "
            f"   OR value ILIKE ? "
            f"   OR tags ILIKE ? "
            f"ORDER BY updated_at DESC LIMIT ?",
            [pattern, pattern, pattern, limit],
        )

    def store_shared_embedding(self, key: str, embedding: list[float]) -> bool:
        """Store a vector embedding for a shared memory entry.
        Embedding is constructed from numeric values only (injection-safe)."""
        # DuckDB doesn't support array parameters directly, so we build
        # a safe literal from numeric values only (no user-controlled strings)
        emb_str = "[" + ",".join(str(float(v)) for v in embedding) + "]"
        self.execute(
            f"UPDATE {META_SCHEMA}.shared_memory "
            f"SET embedding = {emb_str}::FLOAT[] WHERE key = ?", [key]
        )
        return True

    def search_shared_semantic(
        self, embedding: list[float], limit: int = 10
    ) -> list[dict[str, Any]]:
        """Perform vector similarity search against stored embeddings.
        Requires DuckDB VSS extension (vss) to be loaded.
        Returns empty list if VSS is unavailable or no embeddings exist."""
        emb_str = "[" + ",".join(str(v) for v in embedding) + "]"
        try:
            return self.query(
                f"SELECT *, array_cosine_similarity(embedding, {emb_str}::FLOAT[]) as score "
                f"FROM {META_SCHEMA}.shared_memory "
                f"WHERE embedding IS NOT NULL "
                f"ORDER BY score DESC LIMIT ?",
                [limit],
            )
        except Exception:
            # VSS not available or no matching embeddings
            return []

    def shared_cleanup_expired(self) -> int:
        """Remove expired shared memory entries. Returns count deleted."""
        count_rows = self.query(
            f"SELECT COUNT(*) as cnt FROM {META_SCHEMA}.shared_memory "
            f"WHERE expires_at IS NOT NULL AND expires_at < current_timestamp"
        )
        count = count_rows[0]["cnt"] if count_rows else 0
        self.execute(
            f"DELETE FROM {META_SCHEMA}.shared_memory "
            f"WHERE expires_at IS NOT NULL AND expires_at < current_timestamp"
        )
        return count

    # ── Agent Coordination ────────────────────────────────────────────────

    def register_agent_run(
        self,
        agent_name: str,
        role: str,
        workspace_id: str | None = None,
        profile_name: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """Register a new agent run and return its run_id."""
        run_id = f"run_{uuid.uuid4().hex[:12]}"
        self.conn.execute(
            f"INSERT INTO {META_SCHEMA}.agent_runs "
            "(run_id, agent_name, profile_name, workspace_id, role, status, metadata_json) "
            "VALUES (?, ?, ?, ?, ?, 'active', ?)",
            [run_id, agent_name, profile_name, workspace_id, role, json.dumps(metadata or {})],
        )
        self.post_agent_event(
            run_id=run_id,
            event_type="registered",
            summary_text=f"Agent {agent_name} registered",
            payload={"role": role, "profile_name": profile_name},
        )
        return run_id

    def heartbeat_agent_run(
        self,
        run_id: str,
        current_task: str | None = None,
        current_summary: str | None = None,
        progress: float | None = None,
        status: str = "active",
    ) -> dict[str, Any]:
        """Update agent liveness and current status fields."""
        self.conn.execute(
            f"UPDATE {META_SCHEMA}.agent_runs "
            "SET last_heartbeat_at = current_timestamp, "
            "current_task = COALESCE(?, current_task), "
            "current_summary = COALESCE(?, current_summary), "
            "progress = COALESCE(?, progress), "
            "status = COALESCE(?, status) "
            "WHERE run_id = ?",
            [current_task, current_summary, progress, status, run_id],
        )
        rows = self.query(
            f"SELECT run_id, status, current_task, current_summary, progress, last_heartbeat_at "
            f"FROM {META_SCHEMA}.agent_runs WHERE run_id = ?",
            [run_id],
        )
        return rows[0] if rows else {"error": f"Unknown run_id: {run_id}"}

    def post_agent_event(
        self,
        run_id: str,
        event_type: str,
        task_id: str | None = None,
        file_path: str | None = None,
        summary_text: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Append an agent event to the shared coordination log."""
        event_id = f"evt_{uuid.uuid4().hex[:12]}"
        workspace_rows = self.query(
            f"SELECT workspace_id FROM {META_SCHEMA}.agent_runs WHERE run_id = ?",
            [run_id],
        )
        workspace_id = workspace_rows[0]["workspace_id"] if workspace_rows else None
        self.conn.execute(
            f"INSERT INTO {META_SCHEMA}.agent_events "
            "(event_id, run_id, workspace_id, event_type, task_id, file_path, payload_json, summary_text) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                event_id,
                run_id,
                workspace_id,
                event_type,
                task_id,
                file_path,
                json.dumps(payload or {}),
                summary_text,
            ],
        )
        return {
            "status": "recorded",
            "event_id": event_id,
            "run_id": run_id,
            "event_type": event_type,
        }

    def claim_resource(
        self,
        run_id: str,
        resource_type: str,
        resource_key: str,
        lease_seconds: int = 300,
        task_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Claim a resource with a lease to avoid multi-agent collisions."""
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be > 0")

        self.conn.execute(
            f"UPDATE {META_SCHEMA}.resource_claims "
            "SET status = 'expired' "
            "WHERE resource_key = ? AND status = 'claimed' AND lease_expires_at <= current_timestamp",
            [resource_key],
        )

        active = self.query(
            f"SELECT resource_key, claimed_by, lease_expires_at "
            f"FROM {META_SCHEMA}.active_resource_claims WHERE resource_key = ?",
            [resource_key],
        )
        if active and active[0]["claimed_by"] != run_id:
            return {
                "status": "conflict",
                "resource_key": resource_key,
                "claimed_by": active[0]["claimed_by"],
                "lease_expires_at": active[0]["lease_expires_at"],
            }

        workspace_rows = self.query(
            f"SELECT workspace_id FROM {META_SCHEMA}.agent_runs WHERE run_id = ?",
            [run_id],
        )
        workspace_id = workspace_rows[0]["workspace_id"] if workspace_rows else None

        claimed_at = datetime.now(timezone.utc)
        lease_expires_at = claimed_at + timedelta(seconds=int(lease_seconds))
        self.conn.execute(
            f"INSERT INTO {META_SCHEMA}.resource_claims "
            f"(resource_key, resource_type, workspace_id, claimed_by, task_id, claimed_at, lease_expires_at, released_at, status, metadata_json) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?, NULL, 'claimed', ?) "
            f"ON CONFLICT (resource_key) DO UPDATE SET "
            f"resource_type = excluded.resource_type, "
            f"workspace_id = excluded.workspace_id, "
            f"claimed_by = excluded.claimed_by, "
            f"task_id = excluded.task_id, "
            f"claimed_at = excluded.claimed_at, "
            f"lease_expires_at = excluded.lease_expires_at, "
            f"released_at = NULL, "
            f"status = 'claimed', "
            f"metadata_json = excluded.metadata_json",
            [
                resource_key,
                resource_type,
                workspace_id,
                run_id,
                task_id,
                claimed_at,
                lease_expires_at,
                json.dumps(metadata or {}),
            ],
        )
        return {
            "status": "claimed",
            "resource_key": resource_key,
            "resource_type": resource_type,
            "claimed_by": run_id,
        }

    def release_resource(self, run_id: str, resource_key: str) -> dict[str, Any]:
        """Release a currently claimed resource."""
        updated = self.conn.execute(
            f"UPDATE {META_SCHEMA}.resource_claims "
            "SET status = 'released', released_at = current_timestamp "
            "WHERE resource_key = ? AND claimed_by = ? AND status = 'claimed' "
            "RETURNING resource_key, claimed_by, status, released_at",
            [resource_key, run_id],
        ).fetchall()
        if not updated:
            return {
                "status": "not_found",
                "resource_key": resource_key,
                "claimed_by": run_id,
            }
        row = updated[0]
        return {
            "status": "released",
            "resource_key": row[0],
            "claimed_by": row[1],
            "released_at": row[3],
        }

    def list_agent_runs(self, workspace_id: str | None = None) -> list[dict[str, Any]]:
        """List current agent runs with lightweight aggregated claim counts."""
        sql = f"""
            SELECT
                r.run_id,
                r.agent_name,
                r.profile_name,
                r.workspace_id,
                r.role,
                r.status,
                r.current_task,
                r.current_summary,
                r.progress,
                r.started_at,
                r.last_heartbeat_at,
                COALESCE(c.open_claim_count, 0) AS open_claim_count
            FROM {META_SCHEMA}.active_agent_runs r
            LEFT JOIN (
                SELECT claimed_by, COUNT(*) AS open_claim_count
                FROM {META_SCHEMA}.active_resource_claims
                GROUP BY claimed_by
            ) c ON c.claimed_by = r.run_id
        """
        params: list[Any] = []
        if workspace_id is not None:
            sql += " WHERE r.workspace_id = ?"
            params.append(workspace_id)
        sql += " ORDER BY r.last_heartbeat_at DESC, r.started_at DESC"
        return self.query(sql, params if params else None)

    def get_agent_updates(
        self,
        workspace_id: str | None = None,
        since_ts: Any | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        """Return current agents plus incremental events/claims since a timestamp."""
        event_sql = f"""
            SELECT event_id, run_id, workspace_id, event_type, task_id, file_path,
                   payload_json, summary_text, ts
            FROM {META_SCHEMA}.agent_events
            WHERE 1 = 1
        """
        event_params: list[Any] = []
        if workspace_id is not None:
            event_sql += " AND workspace_id = ?"
            event_params.append(workspace_id)
        if since_ts is not None:
            event_sql += " AND ts > ?"
            event_params.append(since_ts)
        event_sql += " ORDER BY ts DESC LIMIT ?"
        event_params.append(limit)
        events = self.query(event_sql, event_params)
        for event in events:
            if event.get("payload_json"):
                event["payload"] = json.loads(event.pop("payload_json"))
            else:
                event["payload"] = {}
                event.pop("payload_json", None)

        claim_sql = f"""
            SELECT resource_key, resource_type, workspace_id, claimed_by, task_id,
                   claimed_at, lease_expires_at, released_at, status, metadata_json
            FROM {META_SCHEMA}.resource_claims
            WHERE 1 = 1
        """
        claim_params: list[Any] = []
        if workspace_id is not None:
            claim_sql += " AND workspace_id = ?"
            claim_params.append(workspace_id)
        if since_ts is not None:
            claim_sql += " AND COALESCE(released_at, claimed_at) > ?"
            claim_params.append(since_ts)
        claim_sql += " ORDER BY COALESCE(released_at, claimed_at) DESC LIMIT ?"
        claim_params.append(limit)
        claims = self.query(claim_sql, claim_params)
        for claim in claims:
            if claim.get("metadata_json"):
                claim["metadata"] = json.loads(claim.pop("metadata_json"))
            else:
                claim["metadata"] = {}
                claim.pop("metadata_json", None)

        return {
            "agents": self.list_agent_runs(workspace_id=workspace_id),
            "events": events,
            "claims": claims,
        }

    def close(self) -> None:
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> CheeksbaseDB:
        """Enter the context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit the context manager and close the connection."""
        self.close()
