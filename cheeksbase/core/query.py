"""Query engine — executes SQL and formats results for agents."""

from __future__ import annotations

import re
import threading
import time
from datetime import datetime, timezone
from typing import Any

from cheeksbase.core.db import META_SCHEMA, CheeksbaseDB

# Module-level singleton
_query_engine_singleton: QueryEngine | None = None


def get_query_engine(db: CheeksbaseDB | None = None) -> QueryEngine:
    """Get or create a shared QueryEngine singleton."""
    global _query_engine_singleton
    if _query_engine_singleton is None:
        _db = db or CheeksbaseDB()
        _query_engine_singleton = QueryEngine(_db)
    return _query_engine_singleton


def reset_query_engine() -> None:
    """Reset the singleton (useful for testing or after schema changes)."""
    global _query_engine_singleton
    if _query_engine_singleton:
        _query_engine_singleton.db.close()
    _query_engine_singleton = None

DEFAULT_QUERY_TIMEOUT_MS = 30_000
DEFAULT_FRESHNESS_THRESHOLD = 3600  # 1 hour (in seconds)


def _parse_duration(s: str) -> int:
    """Parse a human-readable duration string into seconds.

    Supports: '30s', '5m', '2h', '24h', '7d'.
    Returns the raw number if just digits (assumed seconds).
    """
    s = s.strip().lower()
    match = re.fullmatch(r"(\d+)\s*([smhd])?", s)
    if not match:
        raise ValueError(f"Invalid duration: {s!r}. Use e.g. '30s', '5m', '2h', '24h'.")
    value = int(match.group(1))
    unit = match.group(2) or "s"
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    return value * multipliers[unit]


class QueryEngine:
    """Execute SQL queries with caching and freshness checks."""

    def __init__(self, db: CheeksbaseDB):
        self.db = db
        self._query_cache: dict[str, Any] = {}
        self._cache_ttl = 300  # 5 minutes default TTL

    def execute(self, sql: str, max_rows: int = 200, use_cache: bool = True, timeout_ms: int | None = None) -> dict[str, Any]:
        """Execute a SQL query and return formatted results.

        Args:
            sql: SQL query to execute
            max_rows: Maximum rows to return
            use_cache: Whether to use query result caching
            timeout_ms: Query timeout in milliseconds (default: 30000)

        Returns:
            Dict with columns, rows, data_types, row_count, etc.
        """
        start_time = time.monotonic()

        # Check persistent DB cache first
        cache_key = f"{hash(sql)}:{max_rows}"
        try:
            cached = self.db.get_query_cache(cache_key)
            if cached:
                result = {**cached, "_cached": True}
                if "rows" in result:
                    result["rows"] = [dict(r) for r in result["rows"]]
                return result
        except Exception:
            pass  # Fall through to query if cache unavailable

        # Check local in-memory cache as fallback
        if use_cache and cache_key in self._query_cache:
            cached = self._query_cache[cache_key]
            if time.time() - cached["timestamp"] < self._cache_ttl:
                result = {**cached["result"], "_cached": True}
                if "rows" in result:
                    result["rows"] = [dict(r) for r in result["rows"]]
                return result

        # Route mutations to the mutation engine
        first_word = self._get_first_sql_keyword(sql)
        if first_word in ("UPDATE", "INSERT", "DELETE", "DROP", "ALTER", "TRUNCATE",
                          "CREATE", "GRANT", "REVOKE", "COPY", "ATTACH", "LOAD", "INSTALL"):
            from cheeksbase.mutations.engine import MutationEngine
            mutation_engine = MutationEngine(self.db)
            return mutation_engine.handle_sql(sql)

        # Check for stale connectors and refresh if needed
        self._refresh_stale_connectors(sql)

        # Execute query with timeout
        effective_timeout_s = (timeout_ms or DEFAULT_QUERY_TIMEOUT_MS) / 1000.0
        result_container: dict[str, Any] = {}
        error_container: list[Exception] = []

        def _run_query() -> None:
            try:
                conn_result = self.db.conn.execute(sql)
                result_container["columns"] = [desc[0] for desc in conn_result.description]
                result_container["data_types"] = {desc[0]: str(desc[1]) for desc in conn_result.description}
                result_container["rows"] = conn_result.fetchall()
            except Exception as e:
                error_container.append(e)

        query_thread = threading.Thread(target=_run_query, daemon=True)
        query_thread.start()
        query_thread.join(timeout=effective_timeout_s)

        if query_thread.is_alive():
            # Query exceeded timeout — interrupt and return error
            try:
                self.db.conn.interrupt()
            except Exception:
                pass
            return {"error": f"Query exceeded timeout of {int(effective_timeout_s * 1000)}ms"}

        if error_container:
            return {"error": str(error_container[0])}

        columns = result_container["columns"]
        data_types = result_container["data_types"]
        rows = result_container["rows"]

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

        # Record query history for pattern analysis
        try:
            tables_used = self._extract_tables_from_sql(sql)
            self.db.record_query_history(
                sql=sql,
                tables_used=tables_used,
                rows_returned=len(result_rows),
                duration_ms=round((time.monotonic() - start_time) * 1000),
            )
        except Exception:
            pass  # Best-effort history

        # Auto-discover query templates from successful queries
        try:
            if not error_container and result_rows:
                first_word = self._get_first_sql_keyword(sql)
                if first_word == "SELECT":
                    tables = re.findall(
                        r'["\']?(\w+)["\']?\s*\.\s*["\']?(\w+)["\']?', sql, re.IGNORECASE
                    )
                    for schema, table in tables:
                        if schema != "_cheeksbase" and schema != "information_schema":
                            self.db.add_query_template(
                                schema=schema,
                                table=table,
                                sql=sql,
                                description=f"Auto-discovered from successful query",
                            )
        except Exception:
            pass

        # Cache the result in persistent DB
        if use_cache:
            try:
                self.db.set_query_cache(cache_key, sql, max_rows, result, ttl_seconds=self._cache_ttl)
            except Exception:
                pass  # Best-effort caching
            # Also cache locally for fast access
            cached_result = dict(result)
            if "rows" in cached_result:
                cached_result["rows"] = [dict(r) for r in cached_result["rows"]]
            self._query_cache[cache_key] = {
                "result": cached_result,
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
        """List all connected data connectors with their tables and stats.

        Returns every connector declared in config.yaml (whether synced or
        not), plus any DB schemas that contain data but aren't in config
        (e.g. created directly). For each, includes table counts, row
        counts, and freshness info when available.
        """
        from cheeksbase.core.config import get_connectors

        configured = get_connectors()
        all_schemas = [s for s in self.db.get_schemas() if s != META_SCHEMA]

        # Ordered: configured connectors first, then orphan DB schemas
        names: list[str] = list(configured.keys())
        names.extend(s for s in all_schemas if s not in configured)

        connectors: list[dict[str, Any]] = []
        for name in names:
            entry = configured.get(name)

            table_info: list[dict[str, Any]] = []
            total_rows = 0
            if name in all_schemas:
                tables = self.db.get_tables(name)
                user_tables = [t for t in tables if not t.startswith("_dlt_")]
                for table in user_tables:
                    count = self.db.get_row_count(name, table)
                    total_rows += count
                    table_info.append({"name": table, "rows": count})

            freshness = self.get_freshness(name, threshold_override=entry.get("freshness_threshold") if entry else None)

            connector_entry: dict[str, Any] = {
                "name": name,
                "tables": table_info,
                "table_count": len(table_info),
                "total_rows": total_rows,
                "last_sync": freshness["last_sync"],
                "synced": len(table_info) > 0,
            }
            if entry:
                connector_entry["source"] = entry.get("source")
                connector_entry["configured"] = True
            else:
                connector_entry["configured"] = False

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

    def get_freshness(self, connector_name: str, threshold_override: str | None = None) -> dict[str, Any]:
        """Return freshness info for a connector.

        Args:
            connector_name: Name of the connector.
            threshold_override: Human-readable duration like "24h", "30m".
                                Falls back to DEFAULT_FRESHNESS_THRESHOLD if not provided.
        """
        result = self.db.conn.execute(
            f"SELECT MAX(finished_at) as last_sync FROM {META_SCHEMA}.sync_log "
            "WHERE connector_name = ? AND status = 'success'",
            [connector_name],
        )
        row = result.fetchone()
        last_sync = row[0] if row and row[0] else None

        # Resolve threshold
        threshold = _parse_duration(threshold_override) if threshold_override else DEFAULT_FRESHNESS_THRESHOLD

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

    @staticmethod
    def _get_first_sql_keyword(sql: str) -> str:
        """Extract the first real SQL keyword, stripping comments and handling WITH."""
        stripped = sql.strip()
        if not stripped:
            return ""
        # Strip leading single-line comments (-- ...) and block comments (/* ... */)
        cleaned = re.sub(r'--[^\n]*', '', stripped)
        cleaned = re.sub(r'/\*.*?\*/', '', cleaned, flags=re.DOTALL)
        cleaned = cleaned.strip()
        if not cleaned:
            return ""
        # Handle WITH ... AS (...) <actual_op> by extracting the operation after CTEs
        upper = cleaned.upper()
        if upper.startswith("WITH "):
            # Scan through the CTE definitions to find where they end.
            # CTEs are: name AS (...) [, name AS (...)]*
            # The operation keyword is the first keyword after all CTE definitions
            # at depth 0 that isn't a comma.
            depth = 0
            i = 0
            n = len(cleaned)
            while i < n:
                ch = cleaned[i]
                if ch == '(':
                    depth += 1
                elif ch == ')':
                    depth -= 1
                    if depth == 0:
                        # End of a CTE definition. Check what follows.
                        after = cleaned[i + 1:].lstrip()
                        if after and after[0] == ',':
                            # More CTEs — skip the comma and continue
                            i += 1  # will be incremented again by the loop
                        elif after:
                            # This is the operation keyword
                            return after.split()[0].upper()
                i += 1
            # Fallback: route to mutation engine for further validation
            return "WITH"
        return cleaned.split()[0].upper()

    def _extract_tables_from_sql(self, sql: str) -> str | None:
        """Extract schema.table references from SQL for query history tracking."""
        refs = re.findall(
            r'["\']?(\w+)["\']?\s*\.\s*["\']?(\w+)["\']?', sql, re.IGNORECASE
        )
        if refs:
            return ", ".join(f"{s}.{t}" for s, t in refs)
        return None
