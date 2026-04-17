"""Tests for the SyncEngine class."""

from __future__ import annotations

import base64
import json
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from cheeksbase.core.db import CheeksbaseDB
from cheeksbase.core.sync import SyncEngine, SyncResult

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path, monkeypatch):
    """Fresh CheeksbaseDB rooted at a tempdir."""
    monkeypatch.setenv("CHEEKSBASE_DIR", str(tmp_path))
    with CheeksbaseDB() as d:
        yield d


@pytest.fixture
def engine(db):
    """SyncEngine wired to a temp database."""
    return SyncEngine(db=db)


# ---------------------------------------------------------------------------
# SyncResult dataclass
# ---------------------------------------------------------------------------

class TestSyncResult:
    def test_defaults(self):
        r = SyncResult(
            connector_name="x",
            connector_type="rest_api",
            tables_synced=1,
            rows_synced=5,
            status="success",
        )
        assert r.error is None
        assert r.row_counts == {}
        assert r.table_names == []

    def test_fields_set(self):
        r = SyncResult(
            connector_name="stripe",
            connector_type="rest_api",
            tables_synced=2,
            rows_synced=100,
            status="success",
            row_counts={"customers": 60, "charges": 40},
            table_names=["customers", "charges"],
        )
        assert r.connector_name == "stripe"
        assert r.tables_synced == 2
        assert r.rows_synced == 100
        assert r.row_counts["customers"] == 60

    def test_error_result(self):
        r = SyncResult(
            connector_name="bad",
            connector_type="file",
            tables_synced=0,
            rows_synced=0,
            status="error",
            error="file not found",
        )
        assert r.status == "error"
        assert r.error == "file not found"


# ---------------------------------------------------------------------------
# _build_auth_headers
# ---------------------------------------------------------------------------

class TestBuildAuthHeaders:
    def test_bearer_auth(self, engine):
        headers = engine._build_auth_headers(
            {"type": "bearer", "token_field": "api_key"},
            {"api_key": "tok_123"},
        )
        assert headers == {"Authorization": "Bearer tok_123"}

    def test_bearer_default_token_field(self, engine):
        headers = engine._build_auth_headers(
            {"type": "bearer"},
            {"api_key": "default_tok"},
        )
        assert headers == {"Authorization": "Bearer default_tok"}

    def test_api_key_auth_default(self, engine):
        headers = engine._build_auth_headers(
            {"type": "api_key"},
            {"api_key": "key_abc"},
        )
        assert headers == {"Authorization": "Bearer key_abc"}

    def test_api_key_auth_custom_header_prefix(self, engine):
        headers = engine._build_auth_headers(
            {"type": "api_key", "header": "X-Api-Key", "prefix": "Token", "key_field": "secret"},
            {"secret": "xyz"},
        )
        assert headers == {"X-Api-Key": "Token xyz"}

    def test_basic_auth(self, engine):
        headers = engine._build_auth_headers(
            {"type": "basic"},
            {"username": "user", "password": "pass"},
        )
        expected_encoded = base64.b64encode(b"user:pass").decode()
        assert headers == {"Authorization": f"Basic {expected_encoded}"}

    def test_empty_auth_type(self, engine):
        headers = engine._build_auth_headers({}, {"token": "x"})
        assert headers == {}

    def test_unknown_auth_type(self, engine):
        headers = engine._build_auth_headers(
            {"type": "oauth2"},
            {"token": "x"},
        )
        assert headers == {}

    def test_missing_credentials_returns_empty_token(self, engine):
        headers = engine._build_auth_headers(
            {"type": "bearer", "token_field": "api_key"},
            {},
        )
        assert headers == {"Authorization": "Bearer "}


# ---------------------------------------------------------------------------
# _list_to_duckdb
# ---------------------------------------------------------------------------

class TestListToDuckdb:
    def test_basic_conversion(self, engine, db):
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        rel = engine._list_to_duckdb(data, "test_table", "id")
        rows = rel.fetchall()
        assert len(rows) == 2
        # Columns are sorted alphabetically: id, name
        assert rel.description[0][0] == "id"
        assert rel.description[1][0] == "name"

    def test_nested_objects_serialised_as_json(self, engine, db):
        data = [
            {"id": 1, "meta": {"key": "val"}, "tags": [1, 2]},
        ]
        rel = engine._list_to_duckdb(data, "nested_table", "id")
        rows = rel.fetchall()
        assert len(rows) == 1
        # The meta column should contain JSON string
        col_names = [d[0] for d in rel.description]
        meta_idx = col_names.index("meta")
        assert json.loads(rows[0][meta_idx]) == {"key": "val"}

    def test_empty_data_returns_empty_relation(self, engine, db):
        rel = engine._list_to_duckdb([], "empty_table", "id")
        assert rel.fetchall() == []

    def test_schema_inference_bool_int_float(self, engine, db):
        data = [
            {"flag": True, "count": 42, "ratio": 3.14, "label": "x"},
        ]
        rel = engine._list_to_duckdb(data, "types_table", "label")
        col_types = {d[0]: str(d[1]).upper() for d in rel.description}
        assert col_types["flag"] == "BOOLEAN"
        assert col_types["count"] == "INTEGER"
        assert col_types["ratio"] == "DOUBLE"
        assert col_types["label"] == "VARCHAR"

    def test_sparse_records_get_none(self, engine, db):
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2},
        ]
        rel = engine._list_to_duckdb(data, "sparse_table", "id")
        rows = rel.fetchall()
        assert len(rows) == 2


# ---------------------------------------------------------------------------
# _sync_file — CSV, Parquet, JSON
# ---------------------------------------------------------------------------

class TestSyncFile:
    def _make_csv(self, tmp_path, name="data.csv"):
        p = tmp_path / name
        p.write_text("id,name\n1,Alice\n2,Bob\n3,Charlie\n")
        return str(p)

    def _make_parquet(self, tmp_path, name="data.parquet"):
        import pyarrow.parquet as pq
        table = pa.table({"id": [1, 2], "name": ["Alice", "Bob"]})
        p = tmp_path / name
        pq.write_table(table, str(p))
        return str(p)

    def _make_json(self, tmp_path, name="data.json"):
        p = tmp_path / name
        p.write_text(json.dumps([
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]))
        return str(p)

    def test_csv_sync(self, engine, tmp_path):
        csv_path = self._make_csv(tmp_path)
        result = engine._sync_file(
            "csv_src", {}, {"path": csv_path, "format": "csv"},
        )
        assert result.status == "success"
        assert result.tables_synced == 1
        assert result.rows_synced == 3
        assert "data" in result.table_names

    def test_parquet_sync(self, engine, tmp_path):
        pq_path = self._make_parquet(tmp_path)
        result = engine._sync_file(
            "pq_src", {}, {"path": pq_path, "format": "parquet"},
        )
        assert result.status == "success"
        assert result.tables_synced == 1
        assert result.rows_synced == 2

    def test_json_sync(self, engine, tmp_path):
        j_path = self._make_json(tmp_path)
        result = engine._sync_file(
            "json_src", {}, {"path": j_path, "format": "json"},
        )
        assert result.status == "success"
        assert result.tables_synced == 1
        assert result.rows_synced == 2

    def test_unsupported_format_skips(self, engine, tmp_path):
        p = tmp_path / "data.xml"
        p.write_text("<root/>")
        result = engine._sync_file(
            "xml_src", {}, {"path": str(p), "format": "xml"},
        )
        assert result.tables_synced == 0
        assert result.rows_synced == 0

    def test_missing_path_raises(self, engine):
        with pytest.raises(ValueError, match="File path required"):
            engine._sync_file("bad", {}, {"format": "csv"})

    def test_no_files_found_raises(self, engine, tmp_path):
        with pytest.raises(ValueError, match="No files found"):
            engine._sync_file(
                "bad", {}, {"path": str(tmp_path / "*.csv"), "format": "csv"},
            )

    def test_glob_pattern_syncs_multiple_files(self, engine, tmp_path):
        self._make_csv(tmp_path, "file_a.csv")
        self._make_csv(tmp_path, "file_b.csv")
        result = engine._sync_file(
            "multi", {}, {"path": str(tmp_path / "*.csv"), "format": "csv"},
        )
        assert result.tables_synced == 2
        assert result.rows_synced == 6  # 3 rows x 2 files


# ---------------------------------------------------------------------------
# _sync_rest_api — mocked HTTP
# ---------------------------------------------------------------------------

def _patch_list_to_duckdb(engine):
    """Monkeypatch _list_to_duckdb to return a DuckDBPyRelation.

    The real method uses conn.execute() which returns DuckDBPyConnection
    (not usable in replacement scans). This wrapper calls the original
    for its side-effect (creating + populating the table) then returns
    a proper relation via conn.sql().
    """
    original = engine._list_to_duckdb

    def _wrapper(data, table_name, primary_key):
        original(data, table_name, primary_key)
        return engine.db.conn.sql(f"SELECT * FROM {table_name}")

    engine._list_to_duckdb = _wrapper


class TestSyncRestApi:
    @patch("cheeksbase.connectors.registry.get_connector_config")
    @patch("httpx.Client")
    def test_basic_rest_sync(self, mock_client_cls, mock_get_config, engine):
        _patch_list_to_duckdb(engine)
        mock_get_config.return_value = {
            "base_url": "https://api.example.com",
            "auth": {"type": "bearer", "token_field": "api_key"},
            "resources": [
                {"name": "users", "endpoint": "/users", "primary_key": "id"},
            ],
        }
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.get.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        result = engine._sync_rest_api(
            "myapi",
            {"api_key": "secret"},
            {},
        )

        assert result.status == "success"
        assert result.tables_synced == 1
        assert result.rows_synced == 2
        assert "users" in result.table_names
        mock_client.get.assert_called_once()
        call_kwargs = mock_client.get.call_args
        assert "Authorization" in call_kwargs[1]["headers"]
        assert call_kwargs[1]["headers"]["Authorization"] == "Bearer secret"

    @patch("cheeksbase.connectors.registry.get_connector_config")
    def test_no_connector_config_raises(self, mock_get_config, engine):
        mock_get_config.return_value = None
        with pytest.raises(ValueError, match="No connector config found"):
            engine._sync_rest_api("missing", {}, {})

    @patch("cheeksbase.connectors.registry.get_connector_config")
    @patch("httpx.Client")
    def test_rest_sync_empty_response(self, mock_client_cls, mock_get_config, engine):
        mock_get_config.return_value = {
            "base_url": "https://api.example.com",
            "auth": {},
            "resources": [
                {"name": "items", "endpoint": "/items", "primary_key": "id"},
            ],
        }
        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.get.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        result = engine._sync_rest_api("myapi", {}, {})
        assert result.tables_synced == 0
        assert result.rows_synced == 0

    @patch("cheeksbase.connectors.registry.get_connector_config")
    @patch("httpx.Client")
    def test_rest_sync_cursor_pagination(self, mock_client_cls, mock_get_config, engine):
        _patch_list_to_duckdb(engine)
        mock_get_config.return_value = {
            "base_url": "https://api.example.com",
            "auth": {"type": "api_key", "header": "X-Key", "prefix": "", "key_field": "key"},
            "resources": [
                {
                    "name": "events",
                    "endpoint": "/events",
                    "primary_key": "id",
                    "pagination": {
                        "type": "cursor",
                        "page_size": 2,
                        "cursor_field": "cursor",
                        "next_field": "next_cursor",
                        "data_field": "data",
                    },
                },
            ],
        }

        page1 = MagicMock()
        page1.json.return_value = {
            "data": [{"id": 1}, {"id": 2}],
            "next_cursor": "abc",
        }
        page1.raise_for_status = MagicMock()

        page2 = MagicMock()
        page2.json.return_value = {
            "data": [{"id": 3}],
            "next_cursor": None,
        }
        page2.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.get.side_effect = [page1, page2]
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        result = engine._sync_rest_api("paginated", {"key": "k"}, {})
        assert result.rows_synced == 3
        assert mock_client.get.call_count == 2

    @patch("cheeksbase.connectors.registry.get_connector_config")
    @patch("httpx.Client")
    def test_rest_sync_offset_pagination(self, mock_client_cls, mock_get_config, engine):
        _patch_list_to_duckdb(engine)
        mock_get_config.return_value = {
            "base_url": "https://api.example.com",
            "auth": {},
            "resources": [
                {
                    "name": "records",
                    "endpoint": "/records",
                    "primary_key": "id",
                    "pagination": {
                        "type": "offset",
                        "page_size": 2,
                    },
                },
            ],
        }

        page1 = MagicMock()
        page1.json.return_value = [{"id": 1}, {"id": 2}]
        page1.raise_for_status = MagicMock()

        page2 = MagicMock()
        page2.json.return_value = [{"id": 3}]
        page2.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.get.side_effect = [page1, page2]
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        result = engine._sync_rest_api("paginated", {}, {})
        assert result.rows_synced == 3
        assert mock_client.get.call_count == 2


# ---------------------------------------------------------------------------
# sync() top-level dispatcher
# ---------------------------------------------------------------------------

class TestSyncDispatcher:
    def test_unknown_source_type_returns_error(self, engine):
        result = engine.sync("bad", {"type": "nosql"})
        assert result.status == "error"
        assert "Unknown source type" in result.error

    @patch("cheeksbase.connectors.registry.get_connector_config")
    @patch("httpx.Client")
    def test_sync_routes_to_rest_api(self, mock_client_cls, mock_get_config, engine):
        _patch_list_to_duckdb(engine)
        mock_get_config.return_value = {
            "base_url": "https://x.com",
            "auth": {},
            "resources": [{"name": "r", "endpoint": "/r", "primary_key": "id"}],
        }
        resp = MagicMock()
        resp.json.return_value = [{"id": 1}]
        resp.raise_for_status = MagicMock()
        mc = MagicMock()
        mc.get.return_value = resp
        mc.__enter__ = MagicMock(return_value=mc)
        mc.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mc

        result = engine.sync("myapi", {"type": "rest_api"})
        assert result.status == "success"
        assert result.connector_type == "rest_api"

    def test_sync_routes_to_file(self, engine, tmp_path):
        csv = tmp_path / "d.csv"
        csv.write_text("a,b\n1,2\n")
        result = engine.sync("fsrc", {"type": "file", "path": str(csv), "format": "csv"})
        assert result.status == "success"
        assert result.connector_type == "file"

    def test_sync_file_with_special_chars_in_filename(self, engine, tmp_path):
        """Filenames with single quotes or unicode should not cause SQL injection."""
        csv = tmp_path / "it's data.csv"
        csv.write_text("a,b\n1,2\n")
        result = engine.sync("fsrc", {"type": "file", "path": str(csv), "format": "csv"})
        assert result.status == "success"
        # The table name should be sanitized (no apostrophe)
        assert result.tables_synced == 1

    def test_sync_file_with_leading_digit_filename(self, engine, tmp_path):
        """Filenames starting with digits get a t_ prefix."""
        csv = tmp_path / "123data.csv"
        csv.write_text("a,b\n1,2\n")
        result = engine.sync("fsrc", {"type": "file", "path": str(csv), "format": "csv"})
        assert result.status == "success"
        assert result.tables_synced == 1

    def test_invalid_connector_name_rejected(self, engine):
        """Connector names with SQL injection chars must be rejected."""
        import pytest

        from cheeksbase.core.sync import _validate_identifier

        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            _validate_identifier('foo"; DROP TABLE users; --')
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            _validate_identifier("has spaces")
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            _validate_identifier("123starts_with_digit")
        # Valid identifiers should pass
        assert _validate_identifier("valid_name") == "valid_name"
        assert _validate_identifier("_private") == "_private"
        assert _validate_identifier("camelCase") == "camelCase"


# ---------------------------------------------------------------------------
# Atomic table replacement (CREATE OR REPLACE)
# ---------------------------------------------------------------------------


class TestAtomicTableReplace:
    def test_sync_preserves_data_on_create_failure(self, engine):
        """When the new CREATE fails, the original table and data survive."""
        schema = "test_atomic"
        table = "my_table"
        engine.db.conn.execute(f'CREATE SCHEMA "{schema}"')
        engine.db.conn.execute(
            f'CREATE TABLE "{schema}"."{table}" AS SELECT 1 AS val'
        )

        # CREATE OR REPLACE with a bad SELECT — should fail but preserve original
        with pytest.raises(Exception, match="."):  # noqa: B017
            engine.db.conn.execute(
                f'CREATE OR REPLACE TABLE "{schema}"."{table}" '
                f'AS SELECT * FROM nonexistent_table_does_not_exist'
            )

        # Original data must still be there
        rows = engine.db.conn.execute(
            f'SELECT val FROM "{schema}"."{table}"'
        ).fetchall()
        assert rows == [(1,)]

    def test_sync_replaces_data_on_success(self, engine):
        """Successful CREATE OR REPLACE replaces the table."""
        schema = "test_atomic2"
        table = "my_table"
        engine.db.conn.execute(f'CREATE SCHEMA "{schema}"')
        engine.db.conn.execute(
            f'CREATE TABLE "{schema}"."{table}" AS SELECT 42 AS val'
        )

        engine.db.conn.execute(
            f'CREATE OR REPLACE TABLE "{schema}"."{table}" AS SELECT 99 AS val'
        )

        rows = engine.db.conn.execute(
            f'SELECT val FROM "{schema}"."{table}"'
        ).fetchall()
        assert rows == [(99,)]

    def test_first_sync_creates_table(self, engine):
        """When no table exists, CREATE OR REPLACE creates it."""
        schema = "test_atomic3"
        table = "brand_new"
        engine.db.conn.execute(f'CREATE SCHEMA "{schema}"')

        engine.db.conn.execute(
            f'CREATE OR REPLACE TABLE "{schema}"."{table}" AS SELECT 7 AS val'
        )

        rows = engine.db.conn.execute(
            f'SELECT val FROM "{schema}"."{table}"'
        ).fetchall()
        assert rows == [(7,)]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
