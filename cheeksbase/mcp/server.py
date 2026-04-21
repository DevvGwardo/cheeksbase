"""MCP server for Cheeksbase — exposes query, describe, sync, and shared-memory tools. — exposes query, describe, and sync tools."""

from __future__ import annotations

import atexit
import json
import logging
import re
from typing import Annotated, Any

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from cheeksbase.core.db import CheeksbaseDB
from cheeksbase.core.query import QueryEngine, get_query_engine

logger = logging.getLogger(__name__)

# Module-level singleton DuckDB connection — shared across all tool calls.
_db: CheeksbaseDB | None = None


def _get_db() -> CheeksbaseDB:
    """Return a lazily-initialised, process-wide CheeksbaseDB connection."""
    global _db
    if _db is None:
        from cheeksbase.core.db import CheeksbaseDB as _DB

        _db = _DB()
        atexit.register(_close_db)
    return _db


def _close_db() -> None:
    """Clean up the singleton connection at interpreter exit."""
    global _db
    if _db:
        _db.close()
        _db = None


def _refresh_instructions() -> str:
    """Regenerate server instructions from current database state."""
    with CheeksbaseDB() as db:
        engine = QueryEngine(db)
        return _build_instructions(engine)


def _build_instructions(engine: QueryEngine) -> str:
    """Build MCP instructions from the current database state."""
    connectors_info = engine.list_connectors()
    connectors = connectors_info.get("connectors", [])

    if not connectors:
        return (
            "This is a Cheeksbase instance with no data loaded yet. "
            "The user needs to run `cheeksbase add <connector>` and `cheeksbase sync` first."
        )

    lines = [
        "You have access to a Cheeksbase database — business data synced from multiple connectors "
        "into a single SQL database (DuckDB dialect).",
        "",
    ]

    lines.append("Connected connectors:")
    for connector in connectors:
        table_names = [t["name"] for t in connector["tables"]]
        line = (
            f"  {connector['name']}: {', '.join(table_names)} "
            f"({connector['total_rows']:,} rows total)"
        )
        if connector.get("is_stale"):
            line += f" — STALE (last sync: {connector.get('age', '?')} ago)"
        elif connector.get("age"):
            line += f" — fresh ({connector['age']} ago)"
        lines.append(line)
    lines.append("")

    lines.append("How to work with this database:")
    lines.append("1. Use `list_connectors` to see what data is available (includes freshness)")
    lines.append("2. Use `describe` on a table to see its columns, types, annotations, and sample data")
    lines.append("3. Use `query` to run SQL (DuckDB dialect, reference tables as schema.table)")
    lines.append("4. Use `sync` to re-sync stale connectors before querying")

    return "\n".join(lines)


def _connector_not_found_response(connector: str, available: list[str]) -> str:
    """Build a consistent error for missing connectors."""
    if available:
        available_text = ", ".join(sorted(available))
        error = (
            f"Connector '{connector}' not found. "
            f"Available connectors: {available_text}."
        )
    else:
        error = (
            f"Connector '{connector}' not found. "
            "No connectors are configured yet."
        )
    return json.dumps({"error": error}, indent=2)


def _dispatch_chain_call(tool_name: str, args: dict[str, Any]) -> dict[str, Any]:
    """Execute a supported chain tool call and return the structured result."""
    if tool_name == "query":
        sql = args.get("sql", "")
        max_rows = args.get("max_rows", 200)
        eng = get_query_engine()
        return {"tool": tool_name, "result": eng.execute(sql, max_rows=max_rows)}

    if tool_name == "describe":
        table = args.get("table", "")
        eng = get_query_engine()
        return {"tool": tool_name, "result": eng.describe_table(table)}

    if tool_name == "sync":
        connector = args.get("connector", "")
        from cheeksbase.core.config import get_connectors
        from cheeksbase.core.sync import SyncEngine

        connectors = get_connectors()
        if connector not in connectors:
            return json.loads(_connector_not_found_response(connector, list(connectors.keys()))) | {
                "tool": tool_name
            }

        db = _get_db()
        sync_engine = SyncEngine(db)
        sync_result = sync_engine.sync(connector, connectors[connector])
        return {
            "tool": tool_name,
            "result": {
                "status": sync_result.status,
                "tables_synced": sync_result.tables_synced,
                "rows_synced": sync_result.rows_synced,
                "error": sync_result.error,
            },
        }

    return {"tool": tool_name, "error": f"Unknown tool: {tool_name}"}


# Module-level tool functions (defined before create_server to allow imports)

def query(
    sql: Annotated[str, Field(description="SQL query to execute (DuckDB dialect). Reference tables as schema.table, e.g. stripe.customers.")],
    max_rows: Annotated[int, Field(description="Maximum rows to return", ge=1, le=10000)] = 200,
) -> str:
    """Execute a SQL query against the database. Use `describe` first to understand table columns and data types."""
    eng = get_query_engine()
    result = eng.execute(sql, max_rows=max_rows)
    return json.dumps(result, indent=2, default=str)


def list_connectors() -> str:
    """List all connected data connectors with their tables, row counts, and last sync time."""
    eng = get_query_engine()
    result = eng.list_connectors()
    return json.dumps(result, indent=2, default=str)


def describe(
    table: Annotated[str, Field(description="Table to describe, e.g. 'stripe.customers' or 'hubspot.contacts'")],
) -> str:
    """Describe a table's columns, types, annotations, and sample rows."""
    eng = get_query_engine()
    result = eng.describe_table(table)
    return json.dumps(result, indent=2, default=str)


def find_data(
    search: Annotated[str, Field(description="Search term to find in table/column names or descriptions")],
) -> str:
    """Find tables and columns matching a search term across all connectors. Useful when you know what kind of data you want but not the exact schema.table names."""
    try:
        db = _get_db()
        results = db.query("""
            SELECT schema_name, table_name, column_name, description, null_rate, distinct_count
            FROM _cheeksbase.columns
            WHERE column_name ILIKE '%' || ? || '%'
               OR description ILIKE '%' || ? || '%'
            UNION ALL
            SELECT schema_name, table_name, '' as column_name, description, NULL, NULL
            FROM _cheeksbase.tables
            WHERE table_name ILIKE '%' || ? || '%'
               OR description ILIKE '%' || ? || '%'
        """, [search, search, search, search])
        return json.dumps({"results": results, "count": len(results)}, indent=2, default=str)
    except Exception as e:
        return json.dumps({"error": str(e), "results": [], "count": 0}, indent=2)


def explain_query(
    sql: Annotated[str, Field(description="SQL query to explain and validate")],
) -> str:
    """Analyze a SQL query for correctness, explain the execution plan, and suggest improvements. Use this before running expensive queries or when a query fails."""
    try:
        db = _get_db()

        # Try EXPLAIN to get the query plan
        try:
            explain_result = db.query(f"EXPLAIN {sql}")
            plan = explain_result
        except Exception as e:
            return json.dumps({
                "status": "error",
                "error": str(e),
                "message": "Query could not be explained. Check syntax and table references."
            }, indent=2)

        # Validate table references exist
        refs = re.findall(r'["\']?(\w+)["\']?\s*\.\s*["\']?(\w+)["\']?', sql, re.IGNORECASE)
        missing: list[str] = []
        for schema, table in refs:
            if schema == "_cheeksbase" or schema == "information_schema":
                continue
            try:
                tables = db.get_tables(schema)
                if table not in tables:
                    missing.append(f"{schema}.{table}")
            except Exception:
                logger.debug("Failed to check table %s.%s", schema, table, exc_info=True)
                missing.append(f"{schema}.{table}")

        # Build response
        response: dict[str, Any] = {
            "status": "ok",
            "execution_plan": plan,
            "tables_referenced": [f"{s}.{t}" for s, t in refs],
            "missing_tables": missing if missing else None,
            "is_valid": len(missing) == 0,
        }

        if missing:
            response["message"] = f"Query references missing tables: {', '.join(missing)}. Use list_connectors() to see available data."

        # Suggest index-like optimizations
        first_word = sql.strip().upper().split()[0] if sql.strip() else ""
        if first_word == "SELECT" and "WHERE" in sql.upper():
            response["tips"] = [
                "Consider using LIMIT to avoid fetching too many rows",
                "Check if WHERE clause columns have low cardinality (use describe to check distinct_count)",
            ]

        return json.dumps(response, indent=2, default=str)
    except Exception as e:
        return json.dumps({"status": "error", "error": str(e)}, indent=2)


def sync(
    connector: Annotated[str, Field(description="Name of the connector to re-sync (e.g. 'stripe', 'hubspot')")],
) -> str:
    """Re-sync a connector to get fresh data. Use when data is stale or you need up-to-date results before querying."""
    from cheeksbase.core.config import get_connectors
    from cheeksbase.core.sync import SyncEngine

    connectors = get_connectors()
    if connector not in connectors:
        return _connector_not_found_response(connector, list(connectors.keys()))

    connector_config = connectors[connector]

    db = _get_db()
    sync_engine = SyncEngine(db)
    result = sync_engine.sync(connector, connector_config)

    # Refresh MCP instructions so agent sees new tables
    _refresh_instructions()

    return json.dumps({
        "status": result.status,
        "tables_synced": result.tables_synced,
        "rows_synced": result.rows_synced,
        "error": result.error,
    }, indent=2, default=str)


def annotate(
    target: Annotated[str, Field(description="Target to annotate: 'schema.table' or 'schema.table.column'")],
    key: Annotated[str, Field(description="Annotation key: 'description', 'note', 'pii', 'deprecated', 'owner', etc.")],
    value: Annotated[str, Field(description="Annotation value")],
) -> str:
    """Annotate a table or column with metadata like descriptions, PII flags, etc."""
    parts = target.split(".")

    db = _get_db()
    if len(parts) == 2:
        schema, table = parts
        if key == "description":
            db.set_table_description(schema, table, value)
        else:
            db.set_metadata(schema, table, key, value, column="")
        result = {"annotated": target, "key": key, "value": value}
    elif len(parts) == 3:
        schema, table, column = parts
        if key in ("description", "note"):
            db.conn.execute(
                f"INSERT INTO _cheeksbase.columns "
                f"(connector_name, schema_name, table_name, column_name, {key}) "
                f"VALUES (?, ?, ?, ?, ?) "
                f"ON CONFLICT (connector_name, schema_name, table_name, column_name) "
                f"DO UPDATE SET {key} = excluded.{key}",
                [schema, schema, table, column, value],
            )
        else:
            db.set_metadata(schema, table, key, value, column=column)
        result = {"annotated": target, "key": key, "value": value}
    else:
        result = {"error": f"Invalid target '{target}'. Use 'schema.table' or 'schema.table.column'"}

    return json.dumps(result, indent=2)


def chain(
    calls: Annotated[list[dict[str, Any]], Field(description="List of tool calls to execute in sequence. Each call should have 'tool' and 'args' keys.")],
) -> str:
    """Chain multiple tool calls together. Execute them in sequence and return all results."""
    results: list[dict[str, Any]] = []

    for call in calls:
        tool_name = str(call.get("tool", ""))
        raw_args = call.get("args", {})
        args = raw_args if isinstance(raw_args, dict) else {}
        results.append(_dispatch_chain_call(tool_name, args))

    return json.dumps(results, indent=2, default=str)




# ── Shared Memory Tools ────────────────────────────────────────────────────


def remember_shared(
    source_agent: Annotated[str, Field(description="Name/ID of the agent writing this memory")],
    key: Annotated[str, Field(description="Unique key for this memory entry")],
    value: Annotated[str, Field(description="The memory content to store")],
    scope: Annotated[str, Field(description="Visibility scope: broadcast (all agents), topic, or targeted", default="broadcast")] = "broadcast",
    tags: Annotated[str | None, Field(description="Comma-separated tags for categorization")] = None,
    expires_at: Annotated[str | None, Field(description="ISO 8601 timestamp when this memory should expire")] = None,
) -> str:
    """Store a memory that other Hermes agents can read.
    Use this to share discoveries, decisions, or context across agents."""
    db = _get_db()
    # Clean up expired entries first
    db.shared_cleanup_expired()
    result = db.shared_remember(
        source_agent=source_agent,
        key=key,
        value=value,
        scope=scope,
        tags=tags,
        expires_at=expires_at,
    )
    return json.dumps(result, default=str)


def recall_shared(
    key: Annotated[str, Field(description="Key of the memory to recall")],
) -> str:
    """Recall a specific shared memory by key.
    Returns the memory value and metadata."""
    db = _get_db()
    db.shared_cleanup_expired()
    result = db.shared_recall(key)
    if result is None:
        return json.dumps({"error": f"No shared memory found for key: {key}"})
    return json.dumps(result, default=str)


def recall_all_shared(
    source_agent: Annotated[str | None, Field(description="Filter by source agent name")] = None,
) -> str:
    """Recall all shared memories, optionally filtered by source agent.
    Returns a list of all stored shared memories."""
    db = _get_db()
    db.shared_cleanup_expired()
    results = db.shared_recall_all(source_agent=source_agent)
    if not results:
        return json.dumps({"message": "No shared memories stored yet."})
    return json.dumps(results, default=str)


def forget_shared(
    key: Annotated[str, Field(description="Key of the memory to forget")],
) -> str:
    """Delete a shared memory entry by key."""
    db = _get_db()
    result = db.shared_recall(key)
    if result is None:
        return json.dumps({"error": f"No shared memory found for key: {key}"})
    db.shared_forget(key)
    return json.dumps({"status": "forgotten", "key": key})


def search_shared(
    query: Annotated[str, Field(description="Search term to find in shared memories")],
    limit: Annotated[int, Field(description="Maximum results to return", default=10)] = 10,
) -> str:
    """Search shared memories by keyword.
    Searches across keys, values, and tags."""
    db = _get_db()
    db.shared_cleanup_expired()
    results = db.shared_search(query=query, limit=limit)
    if not results:
        return json.dumps({"message": f"No shared memories matching '{query}'."})
    return json.dumps(results, default=str)


def search_shared_semantic(
    query: Annotated[str, Field(description="Natural language query to search semantically")],
    limit: Annotated[int, Field(description="Maximum results to return", default=5)] = 5,
) -> str:
    """Search shared memories by meaning (semantic/keyword hybrid).
    Currently uses keyword search as a fallback. Full vector similarity
    search requires pre-computed embeddings (stored via shared_store_embedding)
    and an embedding model on the caller side. TODO: add embedding generation."""
    db = _get_db()
    db.shared_cleanup_expired()
    results = db.shared_search(query=query, limit=limit)
    if not results:
        return json.dumps({"message": f"No shared memories matching '{query}'."})
    return json.dumps(results, default=str)


def embed_shared(
    key: Annotated[str, Field(description="Key of the memory to attach an embedding to")],
    embedding: Annotated[list[float], Field(description="Vector embedding as a list of floats")],
) -> str:
    """Attach a vector embedding to a shared memory entry.
    Use this after storing a memory with remember_shared to enable
    semantic search via search_shared when embeddings are present.
    The embedding should be generated using the same model for all entries."""
    db = _get_db()
    result = db.shared_recall(key)
    if result is None:
        return json.dumps({"error": f"No shared memory found for key: {key}"})
    db.store_shared_embedding(key, embedding)
    return json.dumps({"status": "embedded", "key": key, "dimensions": len(embedding)})

def create_server() -> FastMCP:
    """Create the FastMCP server."""
    # Build initial instructions
    instructions = _refresh_instructions()

    with CheeksbaseDB() as db:
        engine = QueryEngine(db)
        connectors = engine.list_connectors().get("connectors", [])

    print("Cheeksbase MCP server ready.", flush=True)
    for s in connectors:
        print(f"  {s['name']}: {s['table_count']} tables, {s['total_rows']:,} rows", flush=True)

    server = FastMCP("cheeksbase", instructions=instructions)

    # Register tools
    server.add_tool(query, name="query")
    server.add_tool(list_connectors, name="list_connectors")
    server.add_tool(describe, name="describe")
    server.add_tool(find_data, name="find_data")
    server.add_tool(explain_query, name="explain_query")
    server.add_tool(sync, name="sync")
    server.add_tool(annotate, name="annotate")
    server.add_tool(chain, name="chain")

    # Shared memory tools
    server.add_tool(remember_shared, name="remember_shared")
    server.add_tool(recall_shared, name="recall_shared")
    server.add_tool(recall_all_shared, name="recall_all_shared")
    server.add_tool(forget_shared, name="forget_shared")
    server.add_tool(search_shared, name="search_shared")
    server.add_tool(embed_shared, name="embed_shared")


    return server


def run_server(host: str = "localhost", port: int = 8000) -> None:
    """Run the MCP server."""
    import uvicorn

    server = create_server()
    uvicorn.run(server.streamable_http_app, host=host, port=port)


if __name__ == "__main__":
    import sys
    if "--http" in sys.argv:
        run_server()
    else:
        # Default: stdio transport (for hermes MCP server integration)
        server = create_server()
        server.run()
