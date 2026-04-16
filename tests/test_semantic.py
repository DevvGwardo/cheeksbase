"""Tests for the semantic annotation agent."""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path

import pytest

from cheeksbase.agents.detectors import (
    detect_pii,
    detect_relationships,
    generate_column_description,
    generate_description,
)
from cheeksbase.agents.semantic import SemanticAgent
from cheeksbase.core.db import CheeksbaseDB


# ---------------------------------------------------------------------------
# Detector unit tests — pure functions, no DB needed
# ---------------------------------------------------------------------------


class TestDetectRelationships:
    def test_simple_fk_to_plural_table(self):
        rels = detect_relationships({
            "orders": ["id", "user_id", "total"],
            "users": ["id", "email"],
        })
        assert len(rels) == 1
        assert rels[0].from_table == "orders"
        assert rels[0].from_column == "user_id"
        assert rels[0].to_table == "users"
        assert rels[0].to_column == "id"
        assert rels[0].confidence >= 0.9

    def test_fk_to_singular_table(self):
        rels = detect_relationships({
            "events": ["id", "user_id"],
            "user": ["id", "name"],
        })
        assert len(rels) == 1
        assert rels[0].to_table == "user"

    def test_uuid_suffix(self):
        rels = detect_relationships({
            "memberships": ["id", "org_uuid"],
            "orgs": ["uuid", "name"],
        })
        assert len(rels) == 1
        assert rels[0].to_column == "uuid"

    def test_no_target_table(self):
        rels = detect_relationships({"orders": ["id", "user_id"]})
        assert rels == []

    def test_no_target_pk(self):
        rels = detect_relationships({
            "orders": ["id", "user_id"],
            "users": ["username", "email"],  # no id/uuid/pk
        })
        assert rels == []

    def test_ignores_self_reference(self):
        rels = detect_relationships({
            "users": ["id", "user_id"],
        })
        assert rels == []

    def test_multiple_fks(self):
        rels = detect_relationships({
            "orders": ["id", "user_id", "product_id"],
            "users": ["id"],
            "products": ["id"],
        })
        assert len(rels) == 2
        pairs = {(r.from_column, r.to_table) for r in rels}
        assert ("user_id", "users") in pairs
        assert ("product_id", "products") in pairs


class TestDetectPII:
    def test_email(self):
        assert detect_pii(["id", "email", "name"])["email"] == "email"

    def test_phone(self):
        result = detect_pii(["phone", "mobile_number", "fax"])
        assert result["phone"] == "phone"
        assert result["mobile_number"] == "phone"
        assert result["fax"] == "phone"

    def test_address_fields(self):
        result = detect_pii(["street", "city", "zip_code", "country"])
        for col in ("street", "city", "zip_code", "country"):
            assert result[col] == "address"

    def test_credentials(self):
        result = detect_pii(["password", "api_key", "access_token"])
        for col in ("password", "api_key", "access_token"):
            assert result[col] == "password"

    def test_name_false_positives_excluded(self):
        result = detect_pii(["id", "table_name", "column_name", "product_name"])
        assert result == {}

    def test_first_name_last_name_flagged(self):
        result = detect_pii(["first_name", "last_name"])
        assert result["first_name"] == "name"
        assert result["last_name"] == "name"

    def test_non_pii_columns(self):
        result = detect_pii(["id", "status", "created_at", "amount"])
        assert result == {}


class TestGenerateDescription:
    def test_table_description_includes_name(self):
        desc = generate_description("user_payments", ["id", "amount"])
        assert "user payments" in desc.lower()
        assert "2 columns" in desc

    def test_table_description_role_hints(self):
        desc = generate_description("events", ["id", "user_id", "email", "created_at"])
        assert "per-user" in desc

    def test_column_description_pk(self):
        assert "Primary key" in generate_column_description("id")

    def test_column_description_fk(self):
        desc = generate_column_description("user_id")
        assert "Foreign key" in desc
        assert "user" in desc

    def test_column_description_pii(self):
        desc = generate_column_description("email", pii_type="email")
        assert "PII" in desc

    def test_column_description_timestamp(self):
        desc = generate_column_description("created_at")
        assert "Timestamp" in desc


# ---------------------------------------------------------------------------
# Integration — SemanticAgent against a real DuckDB
# ---------------------------------------------------------------------------


@pytest.fixture
def temp_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Fresh CheeksbaseDB rooted at a tempdir."""
    monkeypatch.setenv("CHEEKSBASE_DIR", str(tmp_path))
    db = CheeksbaseDB()
    # Touch `.conn` to run INIT_SQL.
    _ = db.conn
    yield db
    db.close()


# NOTE: Several `CheeksbaseDB` reader helpers (get_column_annotations,
# get_relationships, get_table_description, get_metadata) currently pass
# params to `self.query()`, which doesn't accept any — so they raise
# TypeError. These tests query via `db.conn.execute` directly to stay
# independent of that bug.


def _fetch_table_description(db: CheeksbaseDB, schema: str, table: str) -> str | None:
    row = db.conn.execute(
        "SELECT description FROM _cheeksbase.tables "
        "WHERE schema_name = ? AND table_name = ?",
        [schema, table],
    ).fetchone()
    return row[0] if row else None


def _fetch_column_annotations(
    db: CheeksbaseDB, schema: str, table: str,
) -> dict[str, dict[str, str | None]]:
    rows = db.conn.execute(
        "SELECT column_name, description, note FROM _cheeksbase.columns "
        "WHERE schema_name = ? AND table_name = ?",
        [schema, table],
    ).fetchall()
    return {r[0]: {"description": r[1], "note": r[2]} for r in rows}


def _fetch_metadata(
    db: CheeksbaseDB, schema: str, table: str, column: str,
) -> dict[str, str]:
    rows = db.conn.execute(
        "SELECT key, value FROM _cheeksbase.metadata "
        "WHERE schema_name = ? AND table_name = ? AND column_name = ?",
        [schema, table, column],
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def _fetch_relationships(
    db: CheeksbaseDB, schema: str, table: str,
) -> list[dict[str, object]]:
    res = db.conn.execute(
        "SELECT from_schema, from_table, from_column, to_schema, to_table, to_column "
        "FROM _cheeksbase.relationships "
        "WHERE (from_schema = ? AND from_table = ?) OR (to_schema = ? AND to_table = ?)",
        [schema, table, schema, table],
    )
    cols = [d[0] for d in res.description]
    return [dict(zip(cols, row)) for row in res.fetchall()]


def _seed_stripe(db: CheeksbaseDB) -> None:
    """Create a tiny 'stripe' schema with two related tables."""
    db.conn.execute('CREATE SCHEMA stripe')
    db.conn.execute('''
        CREATE TABLE stripe.customers (
            id VARCHAR PRIMARY KEY,
            email VARCHAR,
            phone VARCHAR,
            name VARCHAR,
            created_at TIMESTAMP
        )
    ''')
    db.conn.execute('''
        CREATE TABLE stripe.charges (
            id VARCHAR PRIMARY KEY,
            customer_id VARCHAR,
            amount INTEGER,
            currency VARCHAR,
            created_at TIMESTAMP
        )
    ''')


class TestSemanticAgent:
    def test_annotate_connector_writes_table_descriptions(self, temp_db):
        _seed_stripe(temp_db)
        agent = SemanticAgent(db=temp_db)

        result = agent.annotate_connector("stripe")

        assert result.tables_annotated == 2
        customers_desc = _fetch_table_description(temp_db, "stripe", "customers")
        charges_desc = _fetch_table_description(temp_db, "stripe", "charges")
        assert customers_desc and "customers" in customers_desc.lower()
        assert charges_desc and "charges" in charges_desc.lower()

    def test_annotate_connector_flags_pii(self, temp_db):
        _seed_stripe(temp_db)
        agent = SemanticAgent(db=temp_db)

        result = agent.annotate_connector("stripe")

        assert "customers" in result.pii_columns
        pii = result.pii_columns["customers"]
        assert pii["email"] == "email"
        assert pii["phone"] == "phone"
        assert pii["name"] == "name"

        # PII recorded in both the `columns` note and the generic metadata table.
        annotations = _fetch_column_annotations(temp_db, "stripe", "customers")
        assert annotations["email"]["note"] == "pii:email"
        meta = _fetch_metadata(temp_db, "stripe", "customers", column="email")
        assert meta["pii_type"] == "email"

    def test_annotate_connector_detects_relationships(self, temp_db):
        _seed_stripe(temp_db)
        agent = SemanticAgent(db=temp_db)

        result = agent.annotate_connector("stripe")

        assert len(result.relationships) == 1
        rel = result.relationships[0]
        assert rel.from_table == "charges"
        assert rel.from_column == "customer_id"
        assert rel.to_table == "customers"

        stored = _fetch_relationships(temp_db, "stripe", "charges")
        assert any(
            r["from_column"] == "customer_id" and r["to_table"] == "customers"
            for r in stored
        )

    def test_annotate_connector_writes_column_descriptions(self, temp_db):
        _seed_stripe(temp_db)
        agent = SemanticAgent(db=temp_db)

        agent.annotate_connector("stripe")

        charges = _fetch_column_annotations(temp_db, "stripe", "charges")
        assert "Primary key" in charges["id"]["description"]
        assert "Foreign key" in charges["customer_id"]["description"]
        assert "Timestamp" in charges["created_at"]["description"]

    def test_annotate_empty_schema_is_noop(self, temp_db):
        # No schema created → get_tables returns [] → no writes.
        agent = SemanticAgent(db=temp_db)
        result = agent.annotate_connector("nonexistent")
        assert result.tables_annotated == 0
        assert result.columns_annotated == 0
        assert result.relationships == []

    def test_annotate_is_idempotent(self, temp_db):
        _seed_stripe(temp_db)
        agent = SemanticAgent(db=temp_db)

        first = agent.annotate_connector("stripe")
        second = agent.annotate_connector("stripe")

        assert first.tables_annotated == second.tables_annotated
        assert len(first.relationships) == len(second.relationships)

        # Relationship row count should not have doubled.
        stored = _fetch_relationships(temp_db, "stripe", "charges")
        assert len(stored) == 1

    def test_result_summary_is_human_readable(self, temp_db):
        _seed_stripe(temp_db)
        agent = SemanticAgent(db=temp_db)
        result = agent.annotate_connector("stripe")
        summary = result.summary()
        assert "2 tables" in summary
        assert "relationships" in summary


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
