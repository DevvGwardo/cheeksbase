"""End-to-end integration tests: add → sync → query.

These tests exercise the full CLI → config → sync → query pipeline against
real fixtures, not mocks. They guard against the class of wiring bugs
(missed transport-type resolution, dropped CLI flags, config/DB drift)
that pure-unit tests cannot catch.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from cheeksbase.cli import cli


@pytest.fixture()
def cli_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("CHEEKSBASE_DIR", str(tmp_path))
    return tmp_path


def test_csv_add_sync_query_end_to_end(cli_env: Path, tmp_path: Path):
    """The README's CSV happy path must work end-to-end.

    Regression guard for:
      - #1 sync dispatch used registry name instead of template transport type
      - #2 list-connectors ignored config.yaml
      - #3 add --path / --format were silently discarded
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "sales.csv").write_text("id,name,amount\n1,alice,100\n2,bob,250\n")

    runner = CliRunner()

    # 1. init
    r = runner.invoke(cli, ["init"])
    assert r.exit_code == 0, r.output

    # 2. add csv connector with --path and --format
    r = runner.invoke(
        cli,
        [
            "add",
            "csv",
            "--name",
            "csv_data",
            "--path",
            str(data_dir / "*.csv"),
            "--format",
            "csv",
        ],
    )
    assert r.exit_code == 0, r.output
    assert "Added connector: csv_data (csv)" in r.output

    # 2a. config.yaml must record path/format as overrides (guards #3)
    with open(cli_env / "config.yaml") as f:
        config = yaml.safe_load(f)
    entry = config["connectors"]["csv_data"]
    assert entry["source"] == "csv"
    assert entry["overrides"]["path"] == str(data_dir / "*.csv")
    assert entry["overrides"]["format"] == "csv"

    # 3. sync must dispatch to file transport via template lookup (guards #1)
    r = runner.invoke(cli, ["sync", "csv_data"])
    assert r.exit_code == 0, r.output
    assert "Error" not in r.output
    assert "Synced 1 tables" in r.output

    # 4. connectors list must show the synced connector (guards #2)
    r = runner.invoke(cli, ["connectors"])
    assert r.exit_code == 0, r.output
    data = json.loads(r.output)
    names = [c["name"] for c in data["connectors"]]
    assert "csv_data" in names
    csv_entry = next(c for c in data["connectors"] if c["name"] == "csv_data")
    assert csv_entry["table_count"] == 1
    assert csv_entry["total_rows"] == 2

    # 5. query must return the synced rows
    r = runner.invoke(cli, ["query", "SELECT id, name, amount FROM csv_data.sales ORDER BY id"])
    assert r.exit_code == 0, r.output
    data = json.loads(r.output)
    assert data["row_count"] == 2
    assert data["columns"] == ["id", "name", "amount"]


def test_connectors_lists_configured_before_sync(cli_env: Path):
    """A connector added but never synced must still be visible (guards #2)."""
    runner = CliRunner()
    runner.invoke(cli, ["init"])
    runner.invoke(cli, ["add", "stripe", "--api-key", "sk_test_x"])

    r = runner.invoke(cli, ["connectors"])
    assert r.exit_code == 0, r.output
    data = json.loads(r.output)

    names = [c["name"] for c in data["connectors"]]
    assert "stripe" in names
    stripe_entry = next(c for c in data["connectors"] if c["name"] == "stripe")
    assert stripe_entry["configured"] is True
    assert stripe_entry["synced"] is False
    assert stripe_entry["table_count"] == 0
    assert stripe_entry["source"] == "stripe"
