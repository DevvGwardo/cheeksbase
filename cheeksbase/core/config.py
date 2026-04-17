"""Configuration management for Cheeksbase."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

DEFAULT_DIR = Path.home() / ".cheeksbase"
CONFIG_FILE = "config.yaml"
DB_FILE = "cheeksbase.duckdb"


def get_cheeksbase_dir() -> Path:
    return Path(os.environ.get("CHEEKSBASE_DIR", DEFAULT_DIR))


def get_config_path() -> Path:
    return get_cheeksbase_dir() / CONFIG_FILE


def get_db_path() -> Path | str:
    return get_cheeksbase_dir() / DB_FILE


def get_connectors_dir() -> Path:
    """Directory for connector YAML configs."""
    return get_cheeksbase_dir() / "connectors"


def get_cache_dir() -> Path:
    """Directory for cached data."""
    return get_cheeksbase_dir() / "cache"


def load_config() -> dict[str, Any]:
    """Load configuration from disk."""
    path = get_config_path()
    if not path.exists():
        return {"connectors": {}}
    try:
        with open(path) as f:
            config = yaml.safe_load(f) or {"connectors": {}}
    except yaml.YAMLError as e:
        import click
        click.echo(f"Error: broken config file at {path}", err=True)
        click.echo(f"YAML parse error: {e}", err=True)
        click.echo("Fix the file manually or delete it and run `cheeksbase init`.", err=True)
        raise SystemExit(1) from None
    return config


def save_config(config: dict[str, Any]) -> None:
    """Save configuration to disk."""
    path = get_config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)
    # Restrict config file to owner only (contains API keys)
    path.chmod(0o600)


def init_cheeksbase() -> Path:
    """Create the cheeksbase directory and default config. Returns the directory path."""
    ddir = get_cheeksbase_dir()
    ddir.mkdir(parents=True, exist_ok=True)
    (ddir / "connectors").mkdir(exist_ok=True)
    (ddir / "cache").mkdir(exist_ok=True)
    config_path = get_config_path()
    if not config_path.exists():
        config: dict[str, Any] = {"connectors": {}}
        save_config(config)
    return ddir


def get_connectors() -> dict[str, Any]:
    """Get all configured connectors."""
    return load_config().get("connectors", {})


def add_connector(
    name: str,
    source: str,
    credentials: dict[str, str],
    overrides: dict[str, Any] | None = None,
    sync_interval: str | None = None,
) -> None:
    """Add a connector to the configuration.

    `source` is the connector registry name (e.g. "csv", "stripe") used to
    look up the template at sync time. `overrides` are merged onto the
    template (e.g. `{"path": "/tmp/*.csv", "format": "csv"}`).
    """
    config = load_config()
    connector_config: dict[str, Any] = {
        "source": source,
        "credentials": credentials,
    }
    if overrides:
        connector_config["overrides"] = overrides
    if sync_interval:
        connector_config["sync_interval"] = sync_interval
    config["connectors"][name] = connector_config
    save_config(config)


def remove_connector(name: str) -> None:
    """Remove a connector from the configuration."""
    config = load_config()
    config["connectors"].pop(name, None)
    save_config(config)
