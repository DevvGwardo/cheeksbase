"""Connector registry — manages YAML-based connector configurations."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from cheeksbase.core.config import get_connectors_dir

# Built-in connector configs directory
BUILTIN_CONFIGS_DIR = Path(__file__).parent / "configs"


def get_connector_config(connector_name: str) -> dict[str, Any] | None:
    """Load a connector config by name.

    Checks user's connectors dir first, then built-in configs.
    Returns None if not found in either location.
    """
    # User connectors take priority
    user_dir = get_connectors_dir()
    if user_dir.exists():
        user_path = user_dir / f"{connector_name}.yaml"
        if user_path.exists():
            with open(user_path) as f:
                return yaml.safe_load(f)

    # Fall back to built-in configs
    builtin_path = BUILTIN_CONFIGS_DIR / f"{connector_name}.yaml"
    if builtin_path.exists():
        with open(builtin_path) as f:
            return yaml.safe_load(f)

    return None


def get_available_connectors() -> list[str]:
    """List all available connector types (built-in + user)."""
    names: set[str] = set()

    # Built-in connectors
    if BUILTIN_CONFIGS_DIR.exists():
        names.update(
            p.stem for p in BUILTIN_CONFIGS_DIR.glob("*.yaml")
            if not p.name.startswith("_")
        )

    # User connectors
    user_dir = get_connectors_dir()
    if user_dir.exists():
        names.update(
            p.stem for p in user_dir.glob("*.yaml")
            if not p.name.startswith("_")
        )

    return sorted(names)


def get_connector_info(connector_name: str) -> dict[str, Any] | None:
    """Get connector information."""
    config = get_connector_config(connector_name)
    if not config:
        return None

    return {
        "name": config.get("name", connector_name),
        "type": config.get("type", "unknown"),
        "description": config.get("description", ""),
        "resources": [
            {
                "name": r.get("name", ""),
                "description": r.get("description", ""),
            }
            for r in config.get("resources", [])
        ],
    }


def list_connector_resources(connector_name: str) -> list[dict[str, Any]]:
    """List all resources for a connector."""
    config = get_connector_config(connector_name)
    if not config:
        return []

    return [
        {
            "name": r.get("name", ""),
            "description": r.get("description", ""),
            "endpoint": r.get("endpoint", ""),
            "primary_key": r.get("primary_key", "id"),
        }
        for r in config.get("resources", [])
    ]


def validate_connector_config(config: dict[str, Any]) -> list[str]:
    """Validate a connector configuration.

    Returns a list of validation errors, empty if valid.
    """
    errors = []

    # Required fields
    if "name" not in config:
        errors.append("Missing required field: name")

    if "type" not in config:
        errors.append("Missing required field: type")

    connector_type = config.get("type", "")

    if connector_type == "rest_api":
        if "base_url" not in config:
            errors.append("REST API connector requires 'base_url'")
        if "resources" not in config:
            errors.append("REST API connector requires 'resources'")

    elif connector_type == "database":
        # Database connectors need connection string in credentials
        pass

    elif connector_type == "file":
        if "path" not in config:
            errors.append("File connector requires 'path'")

    elif connector_type == "graphql":
        if "endpoint" not in config:
            errors.append("GraphQL connector requires 'endpoint'")
        if "resources" not in config:
            errors.append("GraphQL connector requires 'resources'")

    # Validate resources
    for i, resource in enumerate(config.get("resources", [])):
        if "name" not in resource:
            errors.append(f"Resource {i} missing required field: name")

        if connector_type in ("rest_api", "graphql"):
            if "endpoint" not in resource and "query" not in resource:
                errors.append(f"Resource {i} missing 'endpoint' or 'query'")

    return errors


def create_connector_template(connector_name: str, connector_type: str) -> dict[str, Any]:
    """Create a template connector configuration."""
    templates = {
        "rest_api": {
            "name": connector_name,
            "type": "rest_api",
            "description": f"{connector_name} API connector",
            "base_url": f"https://api.{connector_name}.com/v1",
            "auth": {
                "type": "bearer",
                "token_field": "api_key",
            },
            "resources": [
                {
                    "name": "items",
                    "endpoint": "/items",
                    "primary_key": "id",
                    "description": "List of items",
                },
            ],
        },
        "database": {
            "name": connector_name,
            "type": "database",
            "description": f"{connector_name} database connector",
            "connection_string": f"postgresql://user:password@localhost:5432/{connector_name}",
            "tables": [
                {
                    "name": "users",
                    "primary_key": "id",
                    "description": "Users table",
                },
            ],
        },
        "file": {
            "name": connector_name,
            "type": "file",
            "description": f"{connector_name} file connector",
            "path": f"./data/{connector_name}/*.csv",
            "format": "csv",
        },
        "graphql": {
            "name": connector_name,
            "type": "graphql",
            "description": f"{connector_name} GraphQL connector",
            "endpoint": f"https://api.{connector_name}.com/graphql",
            "auth": {
                "type": "bearer",
                "token_field": "api_key",
            },
            "resources": [
                {
                    "name": "items",
                    "query": "query { items { id name } }",
                    "data_path": "data.items",
                    "description": "List of items",
                },
            ],
        },
    }

    return templates.get(connector_type, templates["rest_api"])
