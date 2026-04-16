"""Configuration management for DataForge."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import yaml


DEFAULT_DIR = Path.home() / ".dataforge"
CONFIG_FILE = "config.yaml"
DB_FILE = "dataforge.duckdb"


def get_dataforge_dir() -> Path:
    return Path(os.environ.get("DATAFORGE_DIR", DEFAULT_DIR))


def get_config_path() -> Path:
    return get_dataforge_dir() / CONFIG_FILE


def get_db_path() -> Path | str:
    return get_dataforge_dir() / DB_FILE


def get_connectors_dir() -> Path:
    """Directory for connector YAML configs."""
    return get_dataforge_dir() / "connectors"


def get_cache_dir() -> Path:
    """Directory for cached data."""
    return get_dataforge_dir() / "cache"


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
        click.echo("Fix the file manually or delete it and run `dataforge init`.", err=True)
        raise SystemExit(1)
    return config


def save_config(config: dict[str, Any]) -> None:
    """Save configuration to disk."""
    path = get_config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)
    # Restrict config file to owner only (contains API keys)
    path.chmod(0o600)


def init_dataforge() -> Path:
    """Create the dataforge directory and default config. Returns the directory path."""
    ddir = get_dataforge_dir()
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
    connector_type: str,
    credentials: dict[str, str],
    sync_interval: str | None = None,
) -> None:
    """Add a connector to the configuration."""
    config = load_config()
    connector_config: dict[str, Any] = {
        "type": connector_type,
        "credentials": credentials,
    }
    if sync_interval:
        connector_config["sync_interval"] = sync_interval
    config["connectors"][name] = connector_config
    save_config(config)


def remove_connector(name: str) -> None:
    """Remove a connector from the configuration."""
    config = load_config()
    config["connectors"].pop(name, None)
    save_config(config)