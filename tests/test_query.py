"""Comprehensive tests for the QueryEngine class."""

from __future__ import annotations

import shutil
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

import pytest

import cheeksbase.core.config as cfg
from cheeksbase.core.config import init_cheeksbase
from cheeksbase.core.db import CheeksbaseDB
from cheeksbase.core.query import QueryEngine


@pytest.fixture
def temp_cheeksbase_dir():
    """Fresh Cheeksbase dir per test."""
    temp_dir = tempfile.mkdtemp()
    original_default = cfg.DEFAULT_DIR
    cfg.DEFAULT_DIR = Path(temp_dir)
    yield Path(temp_dir)
    cfg.DEFAULT_DIR = original_default
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def db(temp_cheeksbase_dir):
    """DB with a test schema and a small 'users' table."""
    init_cheeksbase()
    db = CheeksbaseDB()
    db.conn.execute("CREATE SCHEMA test_connector")
    db.conn.execute(
        "CREATE TABLE test_connector.users ("
        "id INTEGER PRIMARY KEY, name VARCHAR, email VARCHAR)"
    )
    db.conn.execute(
        "INSERT INTO test_connector.users VALUES "
        "(1, 'Alice', 'alice@example.com'),"
        "(2, 'Bob',   'bob@example.com'),"
        "(3, 'Carol', 'carol@example.com'),"
        "(4, 'Dave',  'dave@example.com'),"
        "(5, 'Eve',   'eve@example.com')"
    )
    db.conn.execute(
        "CREATE TABLE test_connector.orders ("
        "id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)"
    )
    db.conn.execute(
        "INSERT INTO test_connector.orders VALUES "
        "(101, 1, 50),(102, 2, 75),(103, 1, 25)"
    )
    yield db
    db.close()


@pytest.fixture
def engine(db):
    """QueryEngine backed by the seeded db."""
    return QueryEngine(db)


# ---------------------------------------------------------------------------
# execute() — basic SELECT
# ---------------------------------------------------------------------------

class TestExecuteSelect:

    def test_returns_columns_and_rows(self, engine):
        result = engine.execute("SELECT id, name, email FROM test_connector.users ORDER BY id")
        assert "error" not in result
        assert result["columns"] == ["id", "name", "email"]
        assert result["row_count"] == 5
        assert result["total_rows"] == 5
        assert result["rows"][0]["name"] == "Alice"
        assert result["rows"][4]["name"] == "Eve"

    def test_returns_data_types(self, engine):
        result = engine.execute("SELECT id, name FROM test_connector.users")
        assert "id" in result["data_types"]
        assert "name" in result["data_types"]

    def test_max_rows_limits_returned_rows(self, engine):
        result = engine.execute("SELECT * FROM test_connector.users ORDER BY id", max_rows=2)
        assert result["row_count"] == 2
        assert result["total_rows"] == 5
        assert result["truncated"] is True
        assert "message" in result
        # Only first 2 by id
        assert [r["name"] for r in result["rows"]] == ["Alice", "Bob"]

    def test_max_rows_exact_match_no_truncation(self, engine):
        result = engine.execute("SELECT * FROM test_connector.users", max_rows=5)
        assert result["row_count"] == 5
        assert result.get("truncated") is not True

    def test_max_rows_larger_than_result(self, engine):
        result = engine.execute("SELECT * FROM test_connector.users", max_rows=999)
        assert result["row_count"] == 5
        assert result.get("truncated") is not True

    def test_invalid_sql_returns_error(self, engine):
        result = engine.execute("SELECT * FROM nonexistent.table_xyz")
        assert "error" in result

    def test_duration_ms_present(self, engine):
        result = engine.execute("SELECT 1")
        assert "duration_ms" in result
        assert isinstance(result["duration_ms"], int)

    def test_count_aggregate(self, engine):
        result = engine.execute("SELECT COUNT(*) as cnt FROM test_connector.users")
        assert result["rows"][0]["cnt"] == 5

    def test_filter_with_where(self, engine):
        result = engine.execute(
            "SELECT name FROM test_connector.users WHERE id = 3"
        )
        assert result["row_count"] == 1
        assert result["rows"][0]["name"] == "Carol"


# ---------------------------------------------------------------------------
# execute() — caching
# ---------------------------------------------------------------------------

class TestCaching:

    def test_second_call_returns_cached_result(self, engine):
        result1 = engine.execute("SELECT * FROM test_connector.users ORDER BY id", max_rows=2)
        result2 = engine.execute("SELECT * FROM test_connector.users ORDER BY id", max_rows=2)
        assert result2.get("_cached") is True
        # Same data
        assert result2["rows"] == result1["rows"]

    def test_cache_bypassed_when_disabled(self, engine):
        engine.execute("SELECT * FROM test_connector.users", use_cache=True)
        result = engine.execute("SELECT * FROM test_connector.users", use_cache=False)
        assert result.get("_cached") is not True

    def test_different_max_rows_different_cache_key(self, engine):
        r1 = engine.execute("SELECT * FROM test_connector.users", max_rows=2)
        r2 = engine.execute("SELECT * FROM test_connector.users", max_rows=5)
        assert r1["row_count"] == 2
        assert r2["row_count"] == 5

    def test_clear_cache_evicts(self, engine):
        engine.execute("SELECT * FROM test_connector.users")
        engine.clear_cache()
        result = engine.execute("SELECT * FROM test_connector.users")
        assert result.get("_cached") is not True

    def test_cache_ttl_expiration(self, engine):
        engine.set_cache_ttl(1)  # 1-second TTL
        engine.execute("SELECT * FROM test_connector.users", max_rows=2)
        time.sleep(1.1)
        result = engine.execute("SELECT * FROM test_connector.users", max_rows=2)
        assert result.get("_cached") is not True

    def test_set_cache_ttl_override(self, engine):
        engine.set_cache_ttl(0)  # Immediately stale
        engine.execute("SELECT 1")
        result = engine.execute("SELECT 1")
        assert result.get("_cached") is not True


# ---------------------------------------------------------------------------
# execute() — mutation routing
# ---------------------------------------------------------------------------

class TestMutationRouting:

    def test_update_routes_through_mutation_engine(self, engine, db):
        result = engine.execute(
            "UPDATE test_connector.users SET name = 'Alicia' WHERE id = 1"
        )
        assert result["status"] == "pending"
        assert "mutation_id" in result
        assert result["mutation_id"].startswith("mut_")

    def test_insert_routes_through_mutation_engine(self, engine, db):
        result = engine.execute(
            "INSERT INTO test_connector.users (id, name, email) "
            "VALUES (6, 'Frank', 'frank@example.com')"
        )
        assert result["status"] == "pending"

    def test_delete_routes_through_mutation_engine(self, engine, db):
        result = engine.execute(
            "DELETE FROM test_connector.users WHERE id = 1"
        )
        assert result["status"] == "pending"

    def test_drop_is_rejected(self, engine, db):
        result = engine.execute("DROP TABLE test_connector.users")
        assert result["status"] == "rejected"
        assert result.get("errors")

    def test_alter_is_rejected(self, engine, db):
        result = engine.execute(
            "ALTER TABLE test_connector.users ADD COLUMN foo VARCHAR"
        )
        assert result["status"] == "rejected"

    def test_truncate_is_rejected(self, engine, db):
        result = engine.execute("TRUNCATE test_connector.users")
        assert result["status"] == "rejected"

    def test_select_does_not_route_to_mutation(self, engine):
        result = engine.execute("SELECT * FROM test_connector.users")
        assert "columns" in result
        assert "status" not in result


# ---------------------------------------------------------------------------
# describe_table()
# ---------------------------------------------------------------------------

class TestDescribeTable:

    def test_schema_dot_table(self, engine):
        result = engine.describe_table("test_connector.users")
        assert "error" not in result
        assert result["schema"] == "test_connector"
        assert result["table"] == "users"
        assert result["row_count"] == 5
        col_names = [c["name"] for c in result["columns"]]
        assert "id" in col_names
        assert "name" in col_names
        assert "email" in col_names

    def test_column_types_present(self, engine):
        result = engine.describe_table("test_connector.users")
        for col in result["columns"]:
            assert "type" in col
            assert "nullable" in col

    def test_sample_rows_returned(self, engine):
        result = engine.describe_table("test_connector.users")
        assert "sample_rows" in result
        assert len(result["sample_rows"]) <= 3

    def test_missing_table_returns_error(self, engine):
        result = engine.describe_table("test_connector.nonexistent")
        assert "error" in result
        assert "not found" in result["error"].lower()

    def test_missing_schema_returns_error(self, engine):
        result = engine.describe_table("no_such_schema.users")
        assert "error" in result

    def test_invalid_table_ref_too_many_dots(self, engine):
        result = engine.describe_table("a.b.c")
        assert "error" in result
        assert "Invalid" in result["error"]

    def test_table_only_ref_finds_schema(self, engine):
        result = engine.describe_table("users")
        assert "error" not in result
        assert result["table"] == "users"

    def test_table_only_ref_missing(self, engine):
        result = engine.describe_table("nonexistent_tbl")
        assert "error" in result

    def test_last_sync_in_result(self, engine):
        result = engine.describe_table("test_connector.users")
        assert "last_sync" in result


# ---------------------------------------------------------------------------
# list_connectors()
# ---------------------------------------------------------------------------

class TestListConnectors:

    def test_returns_seeded_schema(self, engine):
        result = engine.list_connectors()
        assert "connectors" in result
        names = [c["name"] for c in result["connectors"]]
        assert "test_connector" in names

    def test_connector_has_table_info(self, engine):
        result = engine.list_connectors()
        tc = next(c for c in result["connectors"] if c["name"] == "test_connector")
        assert tc["table_count"] >= 1
        table_names = [t["name"] for t in tc["tables"]]
        assert "users" in table_names
        assert "orders" in table_names

    def test_connector_has_row_counts(self, engine):
        result = engine.list_connectors()
        tc = next(c for c in result["connectors"] if c["name"] == "test_connector")
        users_info = next(t for t in tc["tables"] if t["name"] == "users")
        assert users_info["rows"] == 5

    def test_meta_schema_excluded(self, engine):
        result = engine.list_connectors()
        names = [c["name"] for c in result["connectors"]]
        assert "_cheeksbase" not in names

    def test_empty_db_returns_no_connectors(self, temp_cheeksbase_dir):
        init_cheeksbase()
        with CheeksbaseDB() as db2:
            qe = QueryEngine(db2)
            result = qe.list_connectors()
            assert result["connectors"] == []


# ---------------------------------------------------------------------------
# get_freshness()
# ---------------------------------------------------------------------------

class TestGetFreshness:

    def test_no_sync_record_is_stale(self, engine):
        freshness = engine.get_freshness("test_connector")
        assert freshness["is_stale"] is True
        assert freshness["last_sync"] is None

    def test_recent_sync_is_fresh(self, engine, db):
        # Insert a sync record with an explicit UTC timestamp to avoid
        # DuckDB local-time vs UTC comparison issues.
        now_utc = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        db.conn.execute(
            "INSERT INTO _cheeksbase.sync_log "
            "(connector_name, connector_type, status, finished_at) "
            "VALUES ('test_connector', 'rest_api', 'success', ?::TIMESTAMP)",
            [now_utc],
        )

        freshness = engine.get_freshness("test_connector")
        assert freshness["is_stale"] is False
        assert freshness["last_sync"] is not None
        assert freshness["age_seconds"] is not None
        assert freshness["age_seconds"] < 5  # just happened

    def test_failed_sync_not_counted(self, engine, db):
        # A failed sync should not count as a successful sync
        sync_id = db.log_sync_start("test_connector", "rest_api")
        db.log_sync_end(sync_id, "error", error_message="timeout")

        freshness = engine.get_freshness("test_connector")
        assert freshness["is_stale"] is True
        assert freshness["last_sync"] is None

    def test_human_duration_present(self, engine, db):
        now_utc = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        db.conn.execute(
            "INSERT INTO _cheeksbase.sync_log "
            "(connector_name, connector_type, status, finished_at) "
            "VALUES ('test_connector', 'rest_api', 'success', ?::TIMESTAMP)",
            [now_utc],
        )

        freshness = engine.get_freshness("test_connector")
        assert freshness["age_human"] is not None
        assert freshness["threshold_human"] is not None

    def test_threshold_is_one_hour(self, engine):
        freshness = engine.get_freshness("any_connector")
        assert freshness["threshold"] == 3600

    def test_unknown_connector_returns_stale(self, engine):
        freshness = engine.get_freshness("totally_unknown")
        assert freshness["is_stale"] is True


# ---------------------------------------------------------------------------
# _serialize edge cases
# ---------------------------------------------------------------------------

class TestSerialize:

    def test_none_value(self, engine):
        result = engine.execute("SELECT NULL as val")
        assert result["rows"][0]["val"] is None

    def test_bytes_serialized_to_hex(self, engine, db):
        db.conn.execute("CREATE TABLE test_connector.blob_test (id INTEGER, data BLOB)")
        db.conn.execute("INSERT INTO test_connector.blob_test VALUES (1, '\\x48656c6c6f'::BLOB)")
        result = engine.execute("SELECT data FROM test_connector.blob_test")
        # Should be a hex string, not crash
        assert isinstance(result["rows"][0]["data"], str)
