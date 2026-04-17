"""DuckDB storage layer for Cheeksbase."""

from __future__ import annotations

import re
from pathlib import Path
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
"""


_META_TABLES = ["sync_log", "tables", "columns", "live_rows", "mutations", "relationships", "metadata", "query_cache", "query_history", "query_templates"]


class CheeksbaseDB:
    """DuckDB wrapper with metadata management."""

    def __init__(self, db_path: Path | str | None = None):
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
            except Exception:
                pass  # Already exists

    def execute(self, sql: str, params: list | None = None) -> duckdb.DuckDBPyRelation:
        """Execute a SQL query."""
        if params:
            return self.conn.execute(sql, params)
        return self.conn.execute(sql)

    def query(self, sql: str, params: list | None = None) -> list[dict[str, Any]]:
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
        return result.fetchone()[0]

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
                for col_name, col_annotations in table_annotations.items():
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

    def get_query_cache(self, cache_key: str) -> dict | None:
        """Get a cached query result by key."""
        rows = self.query(
            "SELECT result_json, expires_at FROM _cheeksbase.query_cache "
            "WHERE cache_key = ? AND expires_at > current_timestamp",
            [cache_key],
        )
        if rows:
            import json
            return json.loads(rows[0]["result_json"])
        return None

    def set_query_cache(self, cache_key: str, sql: str, max_rows: int, result: dict, ttl_seconds: int = 300) -> None:
        """Cache a query result with TTL."""
        import json
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

    def get_query_templates(self, schema: str, table: str, limit: int = 5) -> list[dict]:
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

    def close(self) -> None:
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
