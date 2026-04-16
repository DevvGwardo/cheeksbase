"""MCP server for Cheeksbase — exposes query, describe, and sync tools."""

from __future__ import annotations

import json
from typing import Annotated, Any

from pydantic import Field
from mcp.server.fastmcp import FastMCP

from cheeksbase.core.db import CheeksbaseDB
from cheeksbase.core.query import QueryEngine


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
    has_stale = False
    for connector in connectors:
        table_names = [t["name"] for t in connector["tables"]]
        line = (
            f"  {connector['name']}: {', '.join(table_names)} "
            f"({connector['total_rows']:,} rows total)"
        )
        if connector.get("is_stale"):
            line += f" — STALE (last sync: {connector.get('age', '?')} ago)"
            has_stale = True
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


def create_server() -> FastMCP:
    """Create the FastMCP server."""
    with CheeksbaseDB() as db:
        engine = QueryEngine(db)
        instructions = _build_instructions(engine)
        connectors = engine.list_connectors().get("connectors", [])

    print("Cheeksbase MCP server ready.", flush=True)
    for s in connectors:
        print(f"  {s['name']}: {s['table_count']} tables, {s['total_rows']:,} rows", flush=True)

    server = FastMCP("cheeksbase", instructions=instructions)

    @server.tool()
    def query(
        sql: Annotated[str, Field(description="SQL query to execute (DuckDB dialect). Reference tables as schema.table, e.g. stripe.customers.")],
        max_rows: Annotated[int, Field(description="Maximum rows to return", ge=1, le=10000)] = 200,
    ) -> str:
        """Execute a SQL query against the database. Use `describe` first to understand table columns and data types."""
        with CheeksbaseDB() as db:
            eng = QueryEngine(db)
            result = eng.execute(sql, max_rows=max_rows)
        return json.dumps(result, indent=2, default=str)

    @server.tool()
    def list_connectors() -> str:
        """List all connected data connectors with their tables, row counts, and last sync time."""
        with CheeksbaseDB() as db:
            eng = QueryEngine(db)
            result = eng.list_connectors()
        return json.dumps(result, indent=2, default=str)

    @server.tool()
    def describe(
        table: Annotated[str, Field(description="Table to describe, e.g. 'stripe.customers' or 'hubspot.contacts'")],
    ) -> str:
        """Describe a table's columns, types, annotations, and sample rows."""
        with CheeksbaseDB() as db:
            eng = QueryEngine(db)
            result = eng.describe_table(table)
        return json.dumps(result, indent=2, default=str)

    @server.tool()
    def sync(
        connector: Annotated[str, Field(description="Name of the connector to re-sync (e.g. 'stripe', 'hubspot')")],
    ) -> str:
        """Re-sync a connector to get fresh data. Use when data is stale or you need up-to-date results before querying."""
        from cheeksbase.core.config import get_connectors
        from cheeksbase.core.sync import SyncEngine
        
        connectors = get_connectors()
        if connector not in connectors:
            return json.dumps({"error": f"Connector '{connector}' not found"})
        
        connector_config = connectors[connector]
        
        with CheeksbaseDB() as db:
            sync_engine = SyncEngine(db)
            result = sync_engine.sync(connector, connector_config)
        
        return json.dumps({
            "status": result.status,
            "tables_synced": result.tables_synced,
            "rows_synced": result.rows_synced,
            "error": result.error,
        }, indent=2, default=str)

    @server.tool()
    def annotate(
        target: Annotated[str, Field(description="Target to annotate: 'schema.table' or 'schema.table.column'")],
        key: Annotated[str, Field(description="Annotation key: 'description', 'note', 'pii', 'deprecated', 'owner', etc.")],
        value: Annotated[str, Field(description="Annotation value")],
    ) -> str:
        """Annotate a table or column with metadata like descriptions, PII flags, etc."""
        parts = target.split(".")
        
        with CheeksbaseDB() as db:
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

    @server.tool()
    def chain(
        calls: Annotated[list[dict[str, Any]], Field(description="List of tool calls to execute in sequence. Each call should have 'tool' and 'args' keys.")],
    ) -> str:
        """Chain multiple tool calls together. Execute them in sequence and return all results."""
        results = []
        
        for call in calls:
            tool_name = call.get("tool", "")
            args = call.get("args", {})
            
            if tool_name == "query":
                sql = args.get("sql", "")
                max_rows = args.get("max_rows", 200)
                with CheeksbaseDB() as db:
                    eng = QueryEngine(db)
                    result = eng.execute(sql, max_rows=max_rows)
                results.append({"tool": tool_name, "result": result})
            
            elif tool_name == "describe":
                table = args.get("table", "")
                with CheeksbaseDB() as db:
                    eng = QueryEngine(db)
                    result = eng.describe_table(table)
                results.append({"tool": tool_name, "result": result})
            
            elif tool_name == "sync":
                connector = args.get("connector", "")
                from cheeksbase.core.config import get_connectors
                from cheeksbase.core.sync import SyncEngine
                
                connectors = get_connectors()
                if connector in connectors:
                    with CheeksbaseDB() as db:
                        sync_engine = SyncEngine(db)
                        result = sync_engine.sync(connector, connectors[connector])
                    results.append({"tool": tool_name, "result": {
                        "status": result.status,
                        "tables_synced": result.tables_synced,
                        "rows_synced": result.rows_synced,
                    }})
                else:
                    results.append({"tool": tool_name, "error": f"Connector '{connector}' not found"})
            
            else:
                results.append({"tool": tool_name, "error": f"Unknown tool: {tool_name}"})
        
        return json.dumps(results, indent=2, default=str)

    return server


def run_server(host: str = "localhost", port: int = 8000):
    """Run the MCP server."""
    server = create_server()
    server.run(host=host, port=port)


if __name__ == "__main__":
    run_server()