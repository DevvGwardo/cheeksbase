"""CLI for DataForge."""

from __future__ import annotations

import json
import sys

import click

from dataforge import __version__
from dataforge.core.config import (
    init_dataforge,
    get_db_path,
    get_connectors_dir,
    add_connector,
    remove_connector,
    get_connectors,
)


@click.group()
@click.version_option(version=__version__)
def cli():
    """🔨 DataForge — agent-first data platform."""
    pass


@cli.command()
def init():
    """Initialize DataForge (create config directory and database)."""
    ddir = init_dataforge()
    
    # Touch the database to create it
    from dataforge.core.db import DataForgeDB
    db = DataForgeDB()
    _ = db.conn  # triggers initialization
    db.close()
    
    click.echo(f"DataForge initialized at {ddir}")
    click.echo(f"Database: {get_db_path()}")


@cli.command()
@click.argument("source_type")
@click.option("--name", help="Custom name for the connector (defaults to source type)")
@click.option("--api-key", help="API key for authentication")
@click.option("--token", help="Token for authentication")
@click.option("--username", help="Username for authentication")
@click.option("--password", help="Password for authentication")
@click.option("--connection-string", help="Database connection string")
@click.option("--path", help="Path to files (for file connectors)")
@click.option("--format", "file_format", help="File format (csv, parquet, json)")
@click.option("--sync-interval", help="Sync interval (e.g., '1h', '30m')")
def add(
    source_type: str,
    name: str | None,
    api_key: str | None,
    token: str | None,
    username: str | None,
    password: str | None,
    connection_string: str | None,
    path: str | None,
    file_format: str | None,
    sync_interval: str | None,
):
    """Add a new data connector.
    
    Examples:
      dataforge add stripe --api-key sk_test_...
      dataforge add postgres --connection-string postgresql://...
      dataforge add csv_data --path ./data/*.csv --format csv
    """
    # Import here to avoid circular imports
    from dataforge.connectors.registry import get_connector_config, get_available_connectors
    
    # Check if connector type exists
    available = get_available_connectors()
    if source_type not in available:
        click.echo(f"Unknown connector type: {source_type}", err=True)
        click.echo(f"Available types: {', '.join(available)}", err=True)
        sys.exit(1)
    
    # Use source_type as name if not provided
    if not name:
        name = source_type
    
    # Build credentials dict
    credentials = {}
    if api_key:
        credentials["api_key"] = api_key
    if token:
        credentials["token"] = token
    if username:
        credentials["username"] = username
    if password:
        credentials["password"] = password
    if connection_string:
        credentials["connection_string"] = connection_string
    
    # Add connector
    add_connector(name, source_type, credentials, sync_interval)
    
    click.echo(f"Added connector: {name} ({source_type})")
    
    # Copy connector config if it doesn't exist
    import shutil
    from pathlib import Path
    
    source_config_path = get_connectors_dir() / f"{source_type}.yaml"
    if not source_config_path.exists():
        # Try to copy from package
        package_config_path = Path(__file__).parent / "connectors" / "configs" / f"{source_type}.yaml"
        if package_config_path.exists():
            shutil.copy(package_config_path, source_config_path)
            click.echo(f"Copied connector config to {source_config_path}")
    
    # Show next steps
    click.echo("\nNext steps:")
    click.echo(f"  dataforge sync {name}       # sync data from this connector")
    click.echo(f"  dataforge query \"SELECT * FROM {name}.<table> LIMIT 10\"  # query the data")


@cli.command()
@click.argument("name")
def remove(name: str):
    """Remove a data connector."""
    connectors = get_connectors()
    if name not in connectors:
        click.echo(f"Connector '{name}' not found", err=True)
        sys.exit(1)
    
    remove_connector(name)
    click.echo(f"Removed connector: {name}")


@cli.command()
@click.argument("name", required=False)
@click.option("--all", "sync_all", is_flag=True, help="Sync all connectors")
@click.option("--force", is_flag=True, help="Force sync even if not stale")
def sync(name: str | None, sync_all: bool, force: bool):
    """Sync data from connectors.
    
    Examples:
      dataforge sync stripe          # sync stripe connector
      dataforge sync --all           # sync all connectors
      dataforge sync stripe --force  # force sync even if fresh
    """
    from dataforge.core.db import DataForgeDB
    from dataforge.core.sync import SyncEngine
    
    connectors = get_connectors()
    
    if not connectors:
        click.echo("No connectors configured. Add one with: dataforge add <type>", err=True)
        sys.exit(1)
    
    if sync_all:
        sync_list = list(connectors.keys())
    elif name:
        if name not in connectors:
            click.echo(f"Connector '{name}' not found", err=True)
            sys.exit(1)
        sync_list = [name]
    else:
        click.echo("Please specify a connector name or use --all", err=True)
        sys.exit(1)
    
    with DataForgeDB() as db:
        sync_engine = SyncEngine(db)
        
        for connector_name in sync_list:
            connector_config = connectors[connector_name]
            click.echo(f"Syncing {connector_name}...")
            
            result = sync_engine.sync(connector_name, connector_config)
            
            if result.status == "success":
                click.echo(f"  ✓ Synced {result.tables_synced} tables, {result.rows_synced:,} rows")
            else:
                click.echo(f"  ✗ Error: {result.error}", err=True)


@cli.command()
@click.argument("sql")
@click.option("--max-rows", default=200, help="Maximum rows to return")
@click.option("--pretty", is_flag=True, help="Pretty print results")
@click.option("--no-cache", is_flag=True, help="Disable query caching")
def query(sql: str, max_rows: int, pretty: bool, no_cache: bool):
    """Execute a SQL query.
    
    Examples:
      dataforge query "SELECT * FROM stripe.customers LIMIT 10"
      dataforge query "SELECT COUNT(*) FROM hubspot.contacts" --pretty
    """
    from dataforge.core.db import DataForgeDB
    from dataforge.core.query import QueryEngine
    
    with DataForgeDB() as db:
        engine = QueryEngine(db)
        result = engine.execute(sql, max_rows=max_rows, use_cache=not no_cache)
        
        if "error" in result:
            click.echo(f"Error: {result['error']}", err=True)
            sys.exit(1)
        
        if pretty:
            _print_pretty(result)
        else:
            click.echo(json.dumps(result, indent=2, default=str))


@cli.command()
@click.argument("table")
@click.option("--pretty", is_flag=True, help="Pretty print results")
def describe(table: str, pretty: bool):
    """Describe a table's schema and metadata.
    
    Examples:
      dataforge describe stripe.customers
      dataforge describe customers --pretty
    """
    from dataforge.core.db import DataForgeDB
    from dataforge.core.query import QueryEngine
    
    with DataForgeDB() as db:
        engine = QueryEngine(db)
        result = engine.describe_table(table)
        
        if "error" in result:
            click.echo(f"Error: {result['error']}", err=True)
            sys.exit(1)
        
        if pretty:
            _print_table_description(result)
        else:
            click.echo(json.dumps(result, indent=2, default=str))


@cli.command()
@click.option("--pretty", is_flag=True, help="Pretty print results")
def connectors(pretty: bool):
    """List all configured connectors."""
    from dataforge.core.db import DataForgeDB
    from dataforge.core.query import QueryEngine
    
    with DataForgeDB() as db:
        engine = QueryEngine(db)
        result = engine.list_connectors()
        
        if pretty:
            _print_connectors(result)
        else:
            click.echo(json.dumps(result, indent=2, default=str))


@cli.command()
@click.option("--port", default=8000, help="Port to run MCP server on")
@click.option("--host", default="localhost", help="Host to bind to")
def serve(port: int, host: str):
    """Start the MCP server for AI agents."""
    from dataforge.mcp.server import run_server
    
    click.echo(f"Starting DataForge MCP server on {host}:{port}")
    run_server(host=host, port=port)


@cli.command()
@click.option("--available", is_flag=True, help="Show available connector types")
def sources(available: bool):
    """List data sources."""
    from dataforge.connectors.registry import get_available_connectors, get_connector_info
    
    if available:
        connector_types = get_available_connectors()
        click.echo("Available connector types:")
        for connector_type in sorted(connector_types):
            info = get_connector_info(connector_type)
            description = info.get("description", "") if info else ""
            click.echo(f"  {connector_type:20} {description}")
    else:
        # Show configured connectors
        connectors = get_connectors()
        if not connectors:
            click.echo("No connectors configured. Add one with: dataforge add <type>")
            return
        
        click.echo("Configured connectors:")
        for name, config in connectors.items():
            connector_type = config.get("type", "unknown")
            click.echo(f"  {name:20} ({connector_type})")


def _print_pretty(result: dict) -> None:
    """Pretty print query results as a table."""
    columns = result.get("columns", [])
    rows = result.get("rows", [])
    
    if not columns or not rows:
        click.echo("No results")
        return
    
    # Calculate column widths
    col_widths = {col: len(col) for col in columns}
    for row in rows:
        for col in columns:
            val = str(row.get(col, ""))
            col_widths[col] = max(col_widths[col], len(val))
    
    # Print header
    header = " | ".join(col.ljust(col_widths[col]) for col in columns)
    click.echo(header)
    click.echo("-" * len(header))
    
    # Print rows
    for row in rows:
        line = " | ".join(
            str(row.get(col, "")).ljust(col_widths[col]) 
            for col in columns
        )
        click.echo(line)
    
    # Print summary
    total_rows = result.get("total_rows", len(rows))
    row_count = result.get("row_count", len(rows))
    if total_rows > row_count:
        click.echo(f"\nShowing {row_count} of {total_rows} rows")
    else:
        click.echo(f"\n{row_count} rows")


def _print_table_description(result: dict) -> None:
    """Pretty print table description."""
    schema = result.get("schema", "")
    table = result.get("table", "")
    row_count = result.get("row_count", 0)
    description = result.get("description", "")
    columns = result.get("columns", [])
    related_tables = result.get("related_tables", [])
    
    click.echo(f"{schema}.{table} ({row_count:,} rows)")
    if description:
        click.echo(f"Description: {description}")
    click.echo()
    
    if columns:
        click.echo("Columns:")
        for col in columns:
            col_name = col.get("name", "")
            col_type = col.get("type", "")
            nullable = "NULL" if col.get("nullable", True) else "NOT NULL"
            col_desc = col.get("description", "")
            
            line = f"  {col_name:20} {col_type:15} {nullable:8}"
            if col_desc:
                line += f"  -- {col_desc}"
            click.echo(line)
    
    if related_tables:
        click.echo("\nRelated tables:")
        for related in related_tables:
            other_table = related.get("table", "")
            join = related.get("join", "")
            cardinality = related.get("cardinality", "")
            desc = related.get("description", "")
            
            click.echo(f"  {other_table} ({cardinality})")
            if join:
                click.echo(f"    {join}")
            if desc:
                click.echo(f"    {desc}")


def _print_connectors(result: dict) -> None:
    """Pretty print connectors list."""
    connectors = result.get("connectors", [])
    
    if not connectors:
        click.echo("No connectors configured")
        return
    
    click.echo("Connectors:")
    for connector in connectors:
        name = connector.get("name", "")
        table_count = connector.get("table_count", 0)
        total_rows = connector.get("total_rows", 0)
        last_sync = connector.get("last_sync", "never")
        is_stale = connector.get("is_stale", False)
        
        status = "STALE" if is_stale else "fresh"
        click.echo(f"  {name:20} {table_count} tables, {total_rows:,} rows")
        click.echo(f"    Last sync: {last_sync} ({status})")
        
        tables = connector.get("tables", [])
        if tables:
            click.echo("    Tables:")
            for table in tables:
                table_name = table.get("name", "")
                table_rows = table.get("rows", 0)
                click.echo(f"      {table_name:20} {table_rows:,} rows")


if __name__ == "__main__":
    cli()