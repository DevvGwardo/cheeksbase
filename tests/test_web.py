"""Tests for the web UI (slice 1: read-only browser).

These rely on `cheeksbase[web]` being installed; the module is skipped
if fastapi / starlette aren't importable.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("starlette.testclient", reason="needs fastapi/starlette")

from click.testing import CliRunner
from starlette.testclient import TestClient

from cheeksbase.cli import cli
from cheeksbase.web import create_app


@pytest.fixture()
def web_env_with_data(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Initialize cheeksbase + sync a CSV so the web UI has something to show."""
    monkeypatch.setenv("CHEEKSBASE_DIR", str(tmp_path))

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "sales.csv").write_text("id,name,amount\n1,alice,100\n2,bob,250\n")

    runner = CliRunner()
    runner.invoke(cli, ["init"])
    runner.invoke(
        cli,
        [
            "add", "csv",
            "--name", "csv_data",
            "--path", str(data_dir / "*.csv"),
            "--format", "csv",
        ],
    )
    runner.invoke(cli, ["sync", "csv_data"])
    return tmp_path


@pytest.fixture()
def client(web_env_with_data: Path) -> TestClient:
    return TestClient(create_app())


class TestIndex:
    def test_index_lists_connector(self, client: TestClient):
        r = client.get("/")
        assert r.status_code == 200
        assert "csv_data" in r.text
        assert "Data sources" in r.text or "data sources" in r.text.lower()

    def test_index_empty_state_when_no_connectors(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setenv("CHEEKSBASE_DIR", str(tmp_path))
        CliRunner().invoke(cli, ["init"])
        r = TestClient(create_app()).get("/")
        assert r.status_code == 200
        assert "No data sources yet" in r.text


class TestConnectorDetail:
    def test_connector_detail_lists_tables(self, client: TestClient):
        r = client.get("/connectors/csv_data")
        assert r.status_code == 200
        assert "sales" in r.text
        # table count + row count surface
        assert "1 table" in r.text
        assert "2 rows" in r.text  # fixture has alice + bob

    def test_unknown_connector_404(self, client: TestClient):
        r = client.get("/connectors/nope")
        assert r.status_code == 404


class TestTableDetail:
    def test_table_renders_rows(self, client: TestClient):
        r = client.get("/connectors/csv_data/tables/sales")
        assert r.status_code == 200
        assert "alice" in r.text
        assert "bob" in r.text
        # actual numeric values
        assert "100" in r.text
        assert "250" in r.text
        # column headers with type
        assert "id" in r.text
        assert "amount" in r.text

    def test_table_pagination_respects_page_size(self, client: TestClient):
        r = client.get("/connectors/csv_data/tables/sales?page=1&page_size=1")
        assert r.status_code == 200
        # page_size=1 → first row only → "alice" present, "bob" absent
        assert "alice" in r.text
        assert "bob" not in r.text
        assert "Page 1 of 2" in r.text

    def test_table_pagination_next_page(self, client: TestClient):
        r = client.get("/connectors/csv_data/tables/sales?page=2&page_size=1")
        assert r.status_code == 200
        assert "alice" not in r.text
        assert "bob" in r.text
        assert "Page 2 of 2" in r.text

    def test_invalid_page_clamps_to_one(self, client: TestClient):
        r = client.get("/connectors/csv_data/tables/sales?page=0")
        assert r.status_code == 200
        assert "Page 1 of" in r.text

    def test_unknown_table_404(self, client: TestClient):
        r = client.get("/connectors/csv_data/tables/nope")
        assert r.status_code == 404
