"""Query engine — executes SQL and formats results for agents."""

from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from typing import Any

from cheeksbase.core.db import META_SCHEMA, CheeksbaseDB


class QueryEngine:
    """Execute SQL queries with caching and freshness checks."""

    def __init__(self, db: CheeksbaseDB):
        self.db = db
        self._query_cache: dict[str, Any] = {}
        self._cache_ttl = 300  # 5 minutes default TTL

    def execute(self, sql: str, max_rows: int = 200, use_cache: bool = True) -> dict[str, Any]:
        """Execute a SQL query and return formatted results.

        Args:
            sql: SQL query to execute
            max_rows: Maximum rows to return
            use_cache: Whether to use query result caching

        Returns:
            Dict with columns, rows, data_types, row_count, etc.
        """
        start_time = time.monotonic()

        # Check cache first
        cache_key = f"{sql}:{max_rows}"
        if use_cache and cache_key in self._query_cache:
            cached = self._query_cache[cache_key]
            if time.time() - cached["timestamp"] < self._cache_ttl:
                cached["result"]["_cached"] = True
                return cached["result"]

        # Route mutations to the mutation engine
        first_word = sql.strip().split()[0].upper() if sql.strip() else ""
        if first_word in ("UPDATE", "INSERT", "DELETE", "DROP", "ALTER", "TRUNCATE", "CREATE", "GRANT", "REVOKE"):
            from cheeksbase.mutations.engine import MutationEngine
            mutation_engine = MutationEngine(self.db)
            return mutation_engine.handle_sql(sql)

        # Check for stale connectors and refresh if needed
        self._refresh_stale_connectors(sql)

        # Execute query
        try:
            conn_result = self.db.conn.execute(sql)
            columns = [desc[0] for desc in conn_result.description]
            data_types = {desc[0]: str(desc[1]) for desc in conn_result.description}
            rows = conn_result.fetchall()
        except Exception as e:
            return {"error": str(e)}

        total_rows = len(rows)
        truncated = total_rows > max_rows
        if truncated:
            rows = rows[:max_rows]

        # Convert rows to list of dicts, handling non-serializable types
        result_rows = []
        for row in rows:
            result_rows.append(
                {col: self._serialize(val) for col, val in zip(columns, row)}
            )

        result: dict[str, Any] = {
            "columns": columns,
            "rows": result_rows,
            "data_types": data_types,
            "row_count": len(result_rows),
            "total_rows": total_rows,
            "duration_ms": round((time.monotonic() - start_time) * 1000),
        }

        if truncated:
            result["truncated"] = True
            result["message"] = (
                f"Showing {max_rows} of {total_rows} rows. "
                f"Use LIMIT N OFFSET M to paginate "
                f"(e.g., LIMIT {max_rows} OFFSET {max_rows} for page 2), "
                f"or add a WHERE clause to narrow results."
            )

        # Cache the result
        if use_cache:
            self._query_cache[cache_key] = {
                "result": result,
                "timestamp": time.time(),
            }

        return result

    def _serialize(self, val: Any) -> Any:
        """Serialize a value for JSON output."""
        if val is None:
            return None
        if isinstance(val, (datetime,)):
            return val.isoformat()
        if isinstance(val, (bytes, bytearray)):
            return val.hex()
        # DuckDB returns Decimal for numeric types
        if hasattr(val, 'as_tuple'):  # Decimal
            return float(val)
        return val

    def _refresh_stale_connectors(self, sql: str) -> None:
        """Refresh stale connectors referenced in the query."""
        # Extract schema.table references from SQL
        refs = re.findall(
            r'\"?(\w+)\"?\s*\.\s*\"?(\w+)\"?', sql, re.IGNORECASE
        )
        for schema, _table in refs:
            # Skip metadata tables
            if schema == META_SCHEMA:
                continue
            # Check if connector is stale
            freshness = self.get_freshness(schema)
            if freshness.get("is_stale"):
                # Try to refresh the connector
                from cheeksbase.core.config import get_connectors
                connectors = get_connectors()
                if schema in connectors:
                    try:
                        from cheeksbase.core.sync import SyncEngine
                        sync_engine = SyncEngine(self.db)
                        sync_engine.sync(schema, connectors[schema])
                    except Exception:
                        pass  # Best-effort refresh

    def list_connectors(self) -> dict[str, Any]:
        """List all connected data connectors with their tables and stats."""
        schemas = self.db.get_schemas()
        connectors: list[dict[str, Any]] = []

        for schema in schemas:
            if schema == META_SCHEMA:
                continue

            tables = self.db.get_tables(schema)
            user_tables = [t for t in tables if not t.startswith("_dlt_")]
            if not user_tables:
                continue

            total_rows = 0
            table_info = []
            for table in user_tables:
                count = self.db.get_row_count(schema, table)
                total_rows += count
                table_info.append({"name": table, "rows": count})

            freshness = self.get_freshness(schema)

            connector_entry: dict[str, Any] = {
                "name": schema,
                "tables": table_info,
                "table_count": len(user_tables),
                "total_rows": total_rows,
                "last_sync": freshness["last_sync"],
            }

            if freshness["threshold"] is not None:
                connector_entry["age"] = freshness["age_human"]
                connector_entry["freshness_threshold"] = freshness["threshold_human"]
                connector_entry["is_stale"] = freshness["is_stale"]

            connectors.append(connector_entry)

        return {"connectors": connectors}

    def describe_table(self, table_ref: str) -> dict[str, Any]:
        """Describe a table's columns. table_ref can be 'schema.table' or just 'table'."""
        parts = table_ref.split(".")
        if len(parts) == 2:
            schema, table = parts
        elif len(parts) == 1:
            # Try to find the table in any schema
            table = parts[0]
            schema = self._find_schema_for_table(table)
            if schema is None:
                return {"error": f"Table '{table}' not found in any schema"}
        else:
            return {"error": f"Invalid table reference: '{table_ref}'. Use 'schema.table' format."}

        columns = self.db.get_columns(schema, table)
        if not columns:
            # Try to suggest similar tables
            suggestion = self._suggest_table(table_ref)
            msg = f"Table '{table_ref}' not found."
            if suggestion:
                msg += f" Did you mean '{suggestion}'?"
            return {"error": msg}

        row_count = self.db.get_row_count(schema, table)

        # Get annotations from _cheeksbase.columns
        annotations = self.db.get_column_annotations(schema, table)

        col_info = []
        for c in columns:
            col_name = c["column_name"]
            entry: dict[str, Any] = {
                "name": col_name,
                "type": c["data_type"],
                "nullable": c["is_nullable"] == "YES",
            }
            ann = annotations.get(col_name, {})
            if ann.get("description"):
                entry["description"] = ann["description"]
            if ann.get("note"):
                entry["note"] = ann["note"]
            col_info.append(entry)

        # Per-column KV metadata
        for entry in col_info:
            col_meta = self.db.get_metadata(schema, table, entry["name"])
            if col_meta:
                entry["metadata"] = col_meta

        # Include source freshness
        freshness = self.get_freshness(schema)
        result: dict[str, Any] = {
            "schema": schema,
            "table": table,
            "row_count": row_count,
            "columns": col_info,
            "last_sync": freshness["last_sync"],
        }

        # Table-level description and KV metadata
        table_desc = self.db.get_table_description(schema, table)
        if table_desc:
            result["description"] = table_desc
        table_meta = self.db.get_metadata(schema, table)
        if table_meta:
            result["metadata"] = table_meta
        if freshness["threshold"] is not None:
            result["age"] = freshness["age_human"]
            result["is_stale"] = freshness["is_stale"]

        # Sample rows
        try:
            sample = self.db.query(f'SELECT * FROM "{schema}"."{table}" LIMIT 3')
            result["sample_rows"] = sample
        except Exception:
            result["sample_rows"] = []

        # Enrich with pre-built relationship graph
        related = self.db.get_relationships(schema, table)
        if related:
            related_tables = []
            for r in related:
                if r["from_schema"] == schema and r["from_table"] == table:
                    other = f"{r['to_schema']}.{r['to_table']}"
                    join = f"ON {r['from_schema']}.{r['from_table']}.{r['from_column']} = {r['to_schema']}.{r['to_table']}.{r['to_column']}"
                else:
                    other = f"{r['from_schema']}.{r['from_table']}"
                    join = f"ON {r['from_schema']}.{r['from_table']}.{r['from_column']} = {r['to_schema']}.{r['to_table']}.{r['to_column']}"
                related_tables.append({
                    "table": other,
                    "join": join,
                    "cardinality": r["cardinality"],
                    "description": r["description"],
                })
            result["related_tables"] = related_tables

        return result

    def get_freshness(self, connector_name: str) -> dict[str, Any]:
        """Return freshness info for a connector."""
        result = self.db.conn.execute(
            f"SELECT MAX(finished_at) as last_sync FROM {META_SCHEMA}.sync_log "
            "WHERE connector_name = ? AND status = 'success'",
            [connector_name],
        )
        row = result.fetchone()
        last_sync = row[0] if row and row[0] else None

        # Default threshold: 1 hour
        threshold = 3600

        # Compute age
        age_seconds = None
        is_stale = True  # No sync = stale
        if last_sync:
            if isinstance(last_sync, str):
                last_sync_dt = datetime.fromisoformat(last_sync)
            else:
                last_sync_dt = last_sync
            if last_sync_dt.tzinfo is None:
                last_sync_dt = last_sync_dt.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            age_seconds = int((now - last_sync_dt).total_seconds())
            is_stale = age_seconds > threshold

        return {
            "last_sync": str(last_sync) if last_sync else None,
            "age_seconds": age_seconds,
            "age_human": self._human_duration(age_seconds) if age_seconds is not None else None,
            "threshold": threshold,
            "threshold_human": self._human_duration(threshold),
            "is_stale": is_stale,
        }

    def _human_duration(self, seconds: int | None) -> str:
        """Convert seconds to human-readable duration."""
        if seconds is None:
            return "unknown"
        if seconds < 60:
            return f"{seconds}s"
        elif seconds < 3600:
            return f"{seconds // 60}m"
        elif seconds < 86400:
            return f"{seconds // 3600}h"
        else:
            return f"{seconds // 86400}d"

    def _find_schema_for_table(self, table: str) -> str | None:
        """Find which schema contains a table."""
        schemas = self.db.get_schemas()
        for schema in schemas:
            if schema == META_SCHEMA:
                continue
            tables = self.db.get_tables(schema)
            if table in tables:
                return schema
        return None

    def _suggest_table(self, table_ref: str) -> str | None:
        """Suggest a similar table name."""
        parts = table_ref.split(".")
        target = parts[-1] if parts else table_ref

        all_tables = []
        schemas = self.db.get_schemas()
        for schema in schemas:
            if schema == META_SCHEMA:
                continue
            tables = self.db.get_tables(schema)
            for table in tables:
                all_tables.append(f"{schema}.{table}")

        # Simple similarity: find tables containing the target string
        for table in all_tables:
            if target.lower() in table.lower():
                return table

        return None

    def clear_cache(self) -> None:
        """Clear the query cache."""
        self._query_cache.clear()

    def set_cache_ttl(self, ttl_seconds: int) -> None:
        """Set the cache TTL in seconds."""
        self._cache_ttl = ttl_seconds
