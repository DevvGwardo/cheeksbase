"""Tests for the Cheeksbase mutation engine."""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import pytest

import cheeksbase.core.config as cfg
from cheeksbase.core.config import init_cheeksbase
from cheeksbase.core.db import CheeksbaseDB
from cheeksbase.core.query import QueryEngine
from cheeksbase.mutations import (
    MutationEngine,
    execute_mutation,
    generate_preview,
    validate_mutation,
)


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
def db_with_customers(temp_cheeksbase_dir):
    """DB with a `stripe.customers` table populated with two rows."""
    init_cheeksbase()
    db = CheeksbaseDB()
    db.conn.execute("CREATE SCHEMA stripe")
    db.conn.execute(
        "CREATE TABLE stripe.customers ("
        "id VARCHAR PRIMARY KEY, email VARCHAR, name VARCHAR)"
    )
    db.conn.execute(
        "INSERT INTO stripe.customers VALUES "
        "('cus_123', 'old@example.com', 'Alice'),"
        "('cus_456', 'bob@example.com', 'Bob')"
    )
    yield db
    db.close()


# --- validate_mutation -----------------------------------------------------

def test_validate_blocks_drop():
    errors = validate_mutation("DROP TABLE stripe.customers")
    assert errors
    assert "DROP" in errors[0]


def test_validate_blocks_alter():
    errors = validate_mutation("ALTER TABLE stripe.customers ADD COLUMN foo VARCHAR")
    assert errors
    assert "ALTER" in errors[0]


def test_validate_blocks_truncate():
    errors = validate_mutation("TRUNCATE stripe.customers")
    assert errors
    assert "TRUNCATE" in errors[0]


def test_validate_delete_requires_where():
    errors = validate_mutation("DELETE FROM stripe.customers")
    assert errors
    assert "WHERE" in errors[0]


def test_validate_delete_with_where_ok():
    errors = validate_mutation("DELETE FROM stripe.customers WHERE id = 'cus_123'")
    assert errors == []


def test_validate_update_ok():
    errors = validate_mutation(
        "UPDATE stripe.customers SET email = 'x@y.com' WHERE id = 'cus_123'"
    )
    assert errors == []


def test_validate_insert_ok():
    errors = validate_mutation(
        "INSERT INTO stripe.customers (id, email, name) VALUES ('cus_789', 'c@d.com', 'Carol')"
    )
    assert errors == []


def test_validate_rejects_unknown_op():
    errors = validate_mutation("CREATE TABLE foo (id INT)")
    assert errors
    assert "Unsupported" in errors[0]


def test_validate_empty():
    errors = validate_mutation("   ")
    assert errors


# --- generate_preview ------------------------------------------------------

def test_preview_update_shows_affected_rows(db_with_customers):
    preview = generate_preview(
        "UPDATE stripe.customers SET email = 'new@example.com' WHERE id = 'cus_123'",
        db_with_customers,
    )
    assert preview["operation"] == "UPDATE"
    assert preview["schema"] == "stripe"
    assert preview["table"] == "customers"
    assert preview["affected_rows"] == 1
    assert len(preview["sample_rows"]) == 1
    assert preview["sample_rows"][0]["email"] == "old@example.com"


def test_preview_delete_shows_rows_to_delete(db_with_customers):
    preview = generate_preview(
        "DELETE FROM stripe.customers WHERE id = 'cus_456'",
        db_with_customers,
    )
    assert preview["operation"] == "DELETE"
    assert preview["affected_rows"] == 1
    assert preview["sample_rows"][0]["id"] == "cus_456"


def test_preview_insert_reports_table(db_with_customers):
    preview = generate_preview(
        "INSERT INTO stripe.customers (id, email) VALUES ('cus_789', 'c@d.com')",
        db_with_customers,
    )
    assert preview["operation"] == "INSERT"
    assert preview["table"] == "customers"


# --- MutationEngine.handle_sql + confirm ----------------------------------

def test_handle_sql_returns_pending_with_mutation_id(db_with_customers):
    engine = MutationEngine(db_with_customers)
    result = engine.handle_sql(
        "UPDATE stripe.customers SET email = 'new@example.com' WHERE id = 'cus_123'"
    )
    assert result["status"] == "pending"
    assert result["mutation_id"].startswith("mut_")
    assert result["preview"]["affected_rows"] == 1


def test_handle_sql_rejects_guardrail_violation(db_with_customers):
    engine = MutationEngine(db_with_customers)
    result = engine.handle_sql("DROP TABLE stripe.customers")
    assert result["status"] == "rejected"
    assert result["errors"]
    # Nothing was recorded.
    assert engine.list_pending() == []


def test_handle_sql_rejects_delete_without_where(db_with_customers):
    engine = MutationEngine(db_with_customers)
    result = engine.handle_sql("DELETE FROM stripe.customers")
    assert result["status"] == "rejected"
    assert any("WHERE" in e for e in result["errors"])


def test_confirm_executes_update(db_with_customers):
    engine = MutationEngine(db_with_customers)
    pending = engine.handle_sql(
        "UPDATE stripe.customers SET email = 'new@example.com' WHERE id = 'cus_123'"
    )
    confirmed = engine.confirm(pending["mutation_id"])
    assert confirmed["status"] == "executed"
    assert confirmed["result"]["local_applied"] is True

    # The local row was actually updated.
    rows = db_with_customers.query(
        "SELECT email FROM stripe.customers WHERE id = 'cus_123'"
    )
    assert rows[0]["email"] == "new@example.com"


def test_confirm_executes_delete(db_with_customers):
    engine = MutationEngine(db_with_customers)
    pending = engine.handle_sql("DELETE FROM stripe.customers WHERE id = 'cus_456'")
    result = engine.confirm(pending["mutation_id"])
    assert result["status"] == "executed"
    rows = db_with_customers.query("SELECT id FROM stripe.customers")
    assert {r["id"] for r in rows} == {"cus_123"}


def test_confirm_executes_insert(db_with_customers):
    engine = MutationEngine(db_with_customers)
    pending = engine.handle_sql(
        "INSERT INTO stripe.customers (id, email, name) "
        "VALUES ('cus_789', 'c@d.com', 'Carol')"
    )
    result = engine.confirm(pending["mutation_id"])
    assert result["status"] == "executed"
    rows = db_with_customers.query(
        "SELECT id FROM stripe.customers WHERE id = 'cus_789'"
    )
    assert len(rows) == 1


def test_confirm_unknown_id_returns_error(db_with_customers):
    engine = MutationEngine(db_with_customers)
    result = engine.confirm("mut_doesnotexist")
    assert result["status"] == "error"
    assert "Unknown" in result["error"]


def test_confirm_twice_fails(db_with_customers):
    engine = MutationEngine(db_with_customers)
    pending = engine.handle_sql(
        "UPDATE stripe.customers SET email = 'z@z.com' WHERE id = 'cus_123'"
    )
    first = engine.confirm(pending["mutation_id"])
    assert first["status"] == "executed"
    second = engine.confirm(pending["mutation_id"])
    assert second["status"] == "error"
    assert "not pending" in second["error"]


def test_list_pending_reflects_status(db_with_customers):
    engine = MutationEngine(db_with_customers)
    p1 = engine.handle_sql(
        "UPDATE stripe.customers SET email = 'a@a.com' WHERE id = 'cus_123'"
    )
    engine.handle_sql(
        "UPDATE stripe.customers SET email = 'b@b.com' WHERE id = 'cus_456'"
    )
    assert len(engine.list_pending()) == 2
    engine.confirm(p1["mutation_id"])
    assert len(engine.list_pending()) == 1


# --- execute_mutation (direct) --------------------------------------------

def test_execute_mutation_local_only(db_with_customers):
    result = execute_mutation(
        "UPDATE stripe.customers SET name = 'Alice II' WHERE id = 'cus_123'",
        connector=None,
        db=db_with_customers,
    )
    assert result["local_applied"] is True
    assert result["source_applied"] is False
    rows = db_with_customers.query(
        "SELECT name FROM stripe.customers WHERE id = 'cus_123'"
    )
    assert rows[0]["name"] == "Alice II"


def test_execute_mutation_rolls_back_on_source_failure(db_with_customers):
    """If source write-back fails, the local change must be rolled back."""
    # Use a connector with a bogus base_url that will fail to resolve quickly.
    connector = {
        "type": "rest_api",
        "base_url": "http://127.0.0.1:1",  # closed port
        "resources": [{"name": "customers", "endpoint": "/customers"}],
        "credentials": {},
    }
    result = execute_mutation(
        "UPDATE stripe.customers SET name = 'SHOULD NOT STICK' WHERE id = 'cus_123'",
        connector=connector,
        db=db_with_customers,
    )
    assert result["source_applied"] is False
    assert result.get("rolled_back") is True

    rows = db_with_customers.query(
        "SELECT name FROM stripe.customers WHERE id = 'cus_123'"
    )
    assert rows[0]["name"] == "Alice"  # unchanged


# --- Integration with QueryEngine routing ---------------------------------

def test_query_engine_routes_mutations(db_with_customers):
    qe = QueryEngine(db_with_customers)
    result = qe.execute(
        "UPDATE stripe.customers SET email = 'routed@example.com' WHERE id = 'cus_123'"
    )
    assert result["status"] == "pending"
    assert result["mutation_id"].startswith("mut_")


def test_query_engine_blocks_drop(db_with_customers):
    qe = QueryEngine(db_with_customers)
    result = qe.execute("DROP TABLE stripe.customers")
    assert result["status"] == "rejected"


def test_update_without_where_rejected(db_with_customers):
    """UPDATE without WHERE must be rejected — catastrophic data change."""
    engine = MutationEngine(db_with_customers)
    result = engine.handle_sql("UPDATE stripe.customers SET email = 'all@example.com'")
    assert result["status"] == "rejected"
    assert "WHERE" in result["errors"][0]


def test_delete_without_where_rejected(db_with_customers):
    """DELETE without WHERE must be rejected."""
    engine = MutationEngine(db_with_customers)
    result = engine.handle_sql("DELETE FROM stripe.customers")
    assert result["status"] == "rejected"
    assert "WHERE" in result["errors"][0]


def test_copy_blocked(db_with_customers):
    """DuckDB COPY command must be rejected."""
    engine = MutationEngine(db_with_customers)
    result = engine.handle_sql("COPY (SELECT * FROM stripe.customers) TO '/tmp/out.csv'")
    assert result["status"] == "rejected"


def test_attach_blocked(db_with_customers):
    """DuckDB ATTACH command must be rejected."""
    engine = MutationEngine(db_with_customers)
    result = engine.handle_sql("ATTACH ':memory:' AS evil")
    assert result["status"] == "rejected"


def test_with_cte_delete_parsed_correctly(db_with_customers):
    """WITH CTE wrapping DELETE must be detected and blocked if no WHERE on the DELETE."""
    engine = MutationEngine(db_with_customers)
    result = engine.handle_sql(
        "WITH target AS (SELECT id FROM stripe.customers) "
        "DELETE FROM stripe.customers"
    )
    # The DELETE has no WHERE — should be rejected
    assert result["status"] == "rejected"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
