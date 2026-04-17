"""Tests for the Cheeksbase CLI commands."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from cheeksbase.cli import cli


@pytest.fixture()
def cli_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Set up an isolated Cheeksbase environment for CLI tests.

    Sets CHEEKSBASE_DIR to a temp directory so no real user config is touched.
    """
    monkeypatch.setenv("CHEEKSBASE_DIR", str(tmp_path))
    return tmp_path


@pytest.fixture()
def initialized_env(cli_env: Path) -> Path:
    """Initialize Cheeksbase in the temp directory."""
    runner = CliRunner()
    result = runner.invoke(cli, ["init"])
    assert result.exit_code == 0, result.output
    return cli_env


def _init_and_add_connector(cli_env: Path, connector_type: str = "stripe") -> CliRunner:
    """Helper: init and add a connector, return the runner."""
    runner = CliRunner()
    runner.invoke(cli, ["init"])
    runner.invoke(cli, ["add", connector_type, "--api-key", "test123"])
    return runner


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------

class TestInit:
    def test_init_creates_directory_structure(self, cli_env: Path):
        runner = CliRunner()
        result = runner.invoke(cli, ["init"])

        assert result.exit_code == 0
        assert "Cheeksbase initialized" in result.output

        # Verify directories and config were created
        assert (cli_env / "config.yaml").exists()
        assert (cli_env / "connectors").is_dir()
        assert (cli_env / "cache").is_dir()

    def test_init_creates_database(self, cli_env: Path):
        runner = CliRunner()
        result = runner.invoke(cli, ["init"])

        assert result.exit_code == 0
        db_path = cli_env / "cheeksbase.duckdb"
        assert db_path.exists()
        assert f"Database: {db_path}" in result.output

    def test_init_is_idempotent(self, cli_env: Path):
        runner = CliRunner()
        r1 = runner.invoke(cli, ["init"])
        r2 = runner.invoke(cli, ["init"])

        assert r1.exit_code == 0
        assert r2.exit_code == 0
        assert "Cheeksbase initialized" in r2.output


# ---------------------------------------------------------------------------
# add
# ---------------------------------------------------------------------------

class TestAdd:
    def test_add_connector(self, initialized_env: Path):
        runner = CliRunner()
        result = runner.invoke(cli, ["add", "stripe", "--api-key", "test123"])

        assert result.exit_code == 0
        assert "Added connector: stripe (stripe)" in result.output

    def test_add_connector_creates_config_entry(self, initialized_env: Path):
        runner = CliRunner()
        runner.invoke(cli, ["add", "stripe", "--api-key", "sk_live_abc"])

        config_path = initialized_env / "config.yaml"
        with open(config_path) as f:
            config = yaml.safe_load(f)

        assert "stripe" in config["connectors"]
        assert config["connectors"]["stripe"]["type"] == "stripe"
        assert config["connectors"]["stripe"]["credentials"]["api_key"] == "sk_live_abc"

    def test_add_connector_copies_yaml_config(self, initialized_env: Path):
        runner = CliRunner()
        runner.invoke(cli, ["add", "stripe", "--api-key", "test123"])

        connector_yaml = initialized_env / "connectors" / "stripe.yaml"
        assert connector_yaml.exists()

    def test_add_unknown_connector_type(self, initialized_env: Path):
        runner = CliRunner()
        result = runner.invoke(cli, ["add", "nonexistent_type", "--api-key", "x"])

        assert result.exit_code != 0
        assert "Unknown connector type" in result.output

    def test_add_with_custom_name(self, initialized_env: Path):
        runner = CliRunner()
        result = runner.invoke(
            cli, ["add", "stripe", "--name", "my_stripe", "--api-key", "test123"]
        )

        assert result.exit_code == 0
        assert "Added connector: my_stripe (stripe)" in result.output

    def test_add_shows_next_steps(self, initialized_env: Path):
        runner = CliRunner()
        result = runner.invoke(cli, ["add", "stripe", "--api-key", "test123"])

        assert "Next steps:" in result.output
        assert "cheeksbase sync" in result.output
        assert "cheeksbase query" in result.output


# ---------------------------------------------------------------------------
# remove
# ---------------------------------------------------------------------------

class TestRemove:
    def test_remove_connector(self, initialized_env: Path):
        runner = CliRunner()
        runner.invoke(cli, ["add", "stripe", "--api-key", "test123"])

        result = runner.invoke(cli, ["remove", "stripe"])
        assert result.exit_code == 0
        assert "Removed connector: stripe" in result.output

    def test_remove_connector_removes_config_entry(self, initialized_env: Path):
        runner = CliRunner()
        runner.invoke(cli, ["add", "stripe", "--api-key", "test123"])
        runner.invoke(cli, ["remove", "stripe"])

        config_path = initialized_env / "config.yaml"
        with open(config_path) as f:
            config = yaml.safe_load(f)

        assert "stripe" not in config.get("connectors", {})

    def test_remove_nonexistent_connector(self, initialized_env: Path):
        runner = CliRunner()
        result = runner.invoke(cli, ["remove", "doesnotexist"])

        assert result.exit_code != 0
        assert "not found" in result.output


# ---------------------------------------------------------------------------
# connectors
# ---------------------------------------------------------------------------

class TestConnectors:
    def test_connectors_empty(self, initialized_env: Path):
        runner = CliRunner()
        result = runner.invoke(cli, ["connectors"])

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["connectors"] == []

    def test_connectors_with_data(self, initialized_env: Path):
        """Create a table directly in the DB so the connectors command finds it."""
        from cheeksbase.core.db import CheeksbaseDB

        runner = CliRunner()
        with CheeksbaseDB() as db:
            db.conn.execute('CREATE SCHEMA IF NOT EXISTS "mysource"')
            db.conn.execute(
                'CREATE TABLE "mysource"."items" AS '
                "SELECT 1 AS id, 'hello' AS name"
            )

        result = runner.invoke(cli, ["connectors"])
        assert result.exit_code == 0

        data = json.loads(result.output)
        connector_names = [c["name"] for c in data["connectors"]]
        assert "mysource" in connector_names

    def test_connectors_pretty_output(self, initialized_env: Path):
        from cheeksbase.core.db import CheeksbaseDB

        runner = CliRunner()
        with CheeksbaseDB() as db:
            db.conn.execute('CREATE SCHEMA IF NOT EXISTS "mysource"')
            db.conn.execute(
                'CREATE TABLE "mysource"."items" AS '
                "SELECT 1 AS id, 'hello' AS name"
            )

        result = runner.invoke(cli, ["connectors", "--pretty"])
        assert result.exit_code == 0
        assert "Connectors:" in result.output


# ---------------------------------------------------------------------------
# query
# ---------------------------------------------------------------------------

class TestQuery:
    def test_query_select(self, initialized_env: Path):
        """Insert data directly, then query it via the CLI."""
        from cheeksbase.core.db import CheeksbaseDB

        runner = CliRunner()
        with CheeksbaseDB() as db:
            db.conn.execute('CREATE SCHEMA IF NOT EXISTS "testsrc"')
            db.conn.execute(
                'CREATE TABLE "testsrc"."people" AS '
                "SELECT 'Alice' AS name, 30 AS age "
                "UNION ALL SELECT 'Bob', 25"
            )

        result = runner.invoke(cli, ["query", 'SELECT * FROM "testsrc"."people" ORDER BY age'])
        assert result.exit_code == 0

        data = json.loads(result.output)
        assert data["row_count"] == 2
        assert "columns" in data
        assert "rows" in data

    def test_query_error(self, initialized_env: Path):
        runner = CliRunner()
        result = runner.invoke(cli, ["query", "SELECT * FROM nonexistent.table"])

        assert result.exit_code != 0
        assert "Error" in result.output

    def test_query_pretty(self, initialized_env: Path):
        from cheeksbase.core.db import CheeksbaseDB

        runner = CliRunner()
        with CheeksbaseDB() as db:
            db.conn.execute('CREATE SCHEMA IF NOT EXISTS "testsrc"')
            db.conn.execute(
                'CREATE TABLE "testsrc"."people" AS '
                "SELECT 'Alice' AS name, 30 AS age"
            )

        result = runner.invoke(
            cli, ["query", 'SELECT * FROM "testsrc"."people"', "--pretty"]
        )
        assert result.exit_code == 0
        assert "name" in result.output
        assert "Alice" in result.output


# ---------------------------------------------------------------------------
# describe
# ---------------------------------------------------------------------------

class TestDescribe:
    def test_describe_table(self, initialized_env: Path):
        from cheeksbase.core.db import CheeksbaseDB

        runner = CliRunner()
        with CheeksbaseDB() as db:
            db.conn.execute('CREATE SCHEMA IF NOT EXISTS "mysrc"')
            db.conn.execute(
                'CREATE TABLE "mysrc"."orders" AS '
                "SELECT 1 AS id, 'widget' AS product, 9.99 AS price"
            )

        result = runner.invoke(cli, ["describe", "mysrc.orders"])
        assert result.exit_code == 0

        data = json.loads(result.output)
        assert data["schema"] == "mysrc"
        assert data["table"] == "orders"
        assert len(data["columns"]) == 3

    def test_describe_nonexistent_table(self, initialized_env: Path):
        runner = CliRunner()
        result = runner.invoke(cli, ["describe", "nope.nope"])

        assert result.exit_code != 0
        assert "Error" in result.output

    def test_describe_pretty(self, initialized_env: Path):
        from cheeksbase.core.db import CheeksbaseDB

        runner = CliRunner()
        with CheeksbaseDB() as db:
            db.conn.execute('CREATE SCHEMA IF NOT EXISTS "mysrc"')
            db.conn.execute(
                'CREATE TABLE "mysrc"."orders" AS '
                "SELECT 1 AS id, 'widget' AS product"
            )

        result = runner.invoke(cli, ["describe", "mysrc.orders", "--pretty"])
        assert result.exit_code == 0
        assert "mysrc.orders" in result.output
        assert "Columns:" in result.output


# ---------------------------------------------------------------------------
# mutations
# ---------------------------------------------------------------------------

class TestMutations:
    def test_mutations_empty(self, initialized_env: Path):
        runner = CliRunner()
        result = runner.invoke(cli, ["mutations"])

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data == []

    def test_mutations_with_data(self, initialized_env: Path):
        """Insert a fake mutation row and verify it shows up."""
        from cheeksbase.core.db import CheeksbaseDB, META_SCHEMA

        runner = CliRunner()
        with CheeksbaseDB() as db:
            db.conn.execute(
                f"INSERT INTO {META_SCHEMA}.mutations "
                "(mutation_id, connector_name, table_name, operation, sql_text, status) "
                "VALUES ('mut-001', 'stripe', 'charges', 'INSERT', "
                "'INSERT INTO stripe.charges VALUES (1)', 'pending')"
            )

        result = runner.invoke(cli, ["mutations"])
        assert result.exit_code == 0

        data = json.loads(result.output)
        assert len(data) == 1
        assert data[0]["mutation_id"] == "mut-001"
        assert data[0]["status"] == "pending"

    def test_mutations_filter_by_status(self, initialized_env: Path):
        from cheeksbase.core.db import CheeksbaseDB, META_SCHEMA

        runner = CliRunner()
        with CheeksbaseDB() as db:
            db.conn.execute(
                f"INSERT INTO {META_SCHEMA}.mutations "
                "(mutation_id, connector_name, table_name, operation, sql_text, status) "
                "VALUES ('mut-p', 'stripe', 'charges', 'INSERT', 'SELECT 1', 'pending')"
            )
            db.conn.execute(
                f"INSERT INTO {META_SCHEMA}.mutations "
                "(mutation_id, connector_name, table_name, operation, sql_text, status) "
                "VALUES ('mut-e', 'stripe', 'charges', 'UPDATE', 'SELECT 1', 'executed')"
            )

        result = runner.invoke(cli, ["mutations", "--status", "pending"])
        assert result.exit_code == 0

        data = json.loads(result.output)
        assert len(data) == 1
        assert data[0]["mutation_id"] == "mut-p"

    def test_mutations_pretty(self, initialized_env: Path):
        from cheeksbase.core.db import CheeksbaseDB, META_SCHEMA

        runner = CliRunner()
        with CheeksbaseDB() as db:
            db.conn.execute(
                f"INSERT INTO {META_SCHEMA}.mutations "
                "(mutation_id, connector_name, table_name, operation, sql_text, status) "
                "VALUES ('mut-p', 'stripe', 'charges', 'INSERT', 'SELECT 1', 'pending')"
            )

        result = runner.invoke(cli, ["mutations", "--pretty"])
        assert result.exit_code == 0
        assert "mut-p" in result.output
        assert "pending" in result.output


# ---------------------------------------------------------------------------
# sources
# ---------------------------------------------------------------------------

class TestSources:
    def test_sources_no_connectors(self, initialized_env: Path):
        runner = CliRunner()
        result = runner.invoke(cli, ["sources"])

        assert result.exit_code == 0
        assert "No connectors configured" in result.output

    def test_sources_with_configured_connectors(self, initialized_env: Path):
        runner = CliRunner()
        runner.invoke(cli, ["add", "stripe", "--api-key", "test123"])

        result = runner.invoke(cli, ["sources"])
        assert result.exit_code == 0
        assert "Configured connectors:" in result.output
        assert "stripe" in result.output

    def test_sources_available_flag(self, initialized_env: Path):
        runner = CliRunner()
        result = runner.invoke(cli, ["sources", "--available"])

        assert result.exit_code == 0
        assert "Available connector types:" in result.output
        # Should list at least the built-in connectors
        assert "stripe" in result.output
        assert "github" in result.output
