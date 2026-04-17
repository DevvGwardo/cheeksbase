"""Tests for Cheeksbase connector YAML configurations."""


import pytest
import yaml

from cheeksbase.connectors.registry import (
    BUILTIN_CONFIGS_DIR,
    get_available_connectors,
    get_connector_config,
    validate_connector_config,
)

NEW_CONNECTORS = ["github", "hubspot", "postgres", "csv", "slack"]


@pytest.mark.parametrize("name", NEW_CONNECTORS)
def test_connector_yaml_is_valid(name):
    """Each new connector YAML parses and passes schema validation."""
    path = BUILTIN_CONFIGS_DIR / f"{name}.yaml"
    assert path.exists(), f"{name}.yaml not found"

    with open(path) as f:
        config = yaml.safe_load(f)

    assert config["name"] == name
    assert "type" in config
    assert "description" in config

    errors = validate_connector_config(config)
    assert errors == [], f"{name} validation errors: {errors}"


@pytest.mark.parametrize("name", NEW_CONNECTORS)
def test_connector_is_registered(name):
    """Each new connector is discoverable through the registry."""
    assert name in get_available_connectors()
    assert get_connector_config(name) is not None


def test_rest_api_connectors_have_expected_resources():
    """REST API connectors declare all resources from the task spec."""
    expected = {
        "github": {"repos", "issues", "pull_requests", "commits"},
        "hubspot": {"contacts", "companies", "deals", "tickets"},
        "slack": {"channels", "messages", "users"},
    }
    for name, want in expected.items():
        config = get_connector_config(name)
        assert config["type"] == "rest_api"
        assert config["base_url"].startswith("https://")
        assert config["auth"]["type"] == "bearer"
        got = {r["name"] for r in config["resources"]}
        assert want.issubset(got), f"{name} missing resources: {want - got}"


def test_postgres_connector_is_database_type():
    config = get_connector_config("postgres")
    assert config["type"] == "database"
    assert config.get("discover_tables") is True


def test_csv_connector_is_file_type():
    config = get_connector_config("csv")
    assert config["type"] == "file"
    assert "path" in config
    assert config.get("format") == "csv"


if __name__ == "__main__":
    pytest.main([__file__])
