"""Semantic annotation agent.

Automatically annotates a connector's tables after sync:
    - table descriptions (heuristic)
    - column descriptions (heuristic)
    - PII flags (regex-based)
    - relationship edges (foreign-key inference from column names)

The agent writes to the existing metadata tables in ``_cheeksbase`` — see
``cheeksbase.core.db.INIT_SQL`` for the schema. It does **not** read or
write user data.

Designed to be called after a successful sync:

    agent = SemanticAgent()
    agent.annotate_connector("stripe")
"""

from __future__ import annotations

from dataclasses import dataclass, field

from cheeksbase.agents.detectors import (
    Relationship,
    detect_pii,
    detect_relationships,
    generate_column_description,
    generate_description,
)
from cheeksbase.core.db import CheeksbaseDB


@dataclass
class AnnotationResult:
    """Summary of what a single ``annotate_connector`` call produced."""
    connector_name: str
    tables_annotated: int = 0
    columns_annotated: int = 0
    pii_columns: dict[str, dict[str, str]] = field(default_factory=dict)
    relationships: list[Relationship] = field(default_factory=list)

    def summary(self) -> str:
        return (
            f"Annotated {self.tables_annotated} tables, "
            f"{self.columns_annotated} columns, "
            f"flagged {sum(len(v) for v in self.pii_columns.values())} PII columns, "
            f"detected {len(self.relationships)} relationships."
        )


class SemanticAgent:
    """Heuristic annotator for freshly synced connector data."""

    def __init__(self, db: CheeksbaseDB | None = None):
        self._owns_db = db is None
        self.db = db or CheeksbaseDB()

    # ---- public API -----------------------------------------------------

    def annotate_connector(self, connector_name: str) -> AnnotationResult:
        """Annotate every table in the connector's schema.

        Assumes ``connector_name`` is also the schema name — this matches the
        convention used by ``SyncEngine``.
        """
        schema = connector_name
        result = AnnotationResult(connector_name=connector_name)

        tables = self.db.get_tables(schema)
        if not tables:
            return result

        # Build {table: [columns]} once; relationship detection needs all of
        # them together, and per-table annotation reuses the same view.
        columns_by_table: dict[str, list[str]] = {
            table: [c["column_name"] for c in self.db.get_columns(schema, table)]
            for table in tables
        }

        for table, cols in columns_by_table.items():
            self._annotate_table(connector_name, schema, table, cols, result)

        # Relationships — run once across the whole connector.
        for rel in detect_relationships(columns_by_table):
            self.db.upsert_relationship(
                from_schema=schema,
                from_table=rel.from_table,
                from_column=rel.from_column,
                to_schema=schema,
                to_table=rel.to_table,
                to_column=rel.to_column,
                cardinality="one_to_many",
                confidence=rel.confidence,
                description=rel.reason,
            )
            result.relationships.append(rel)

        return result

    def close(self) -> None:
        """Close the DB connection if we opened it."""
        if self._owns_db:
            self.db.close()

    def __enter__(self) -> "SemanticAgent":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    # ---- internals ------------------------------------------------------

    def _annotate_table(
        self,
        connector_name: str,
        schema: str,
        table: str,
        columns: list[str],
        result: AnnotationResult,
    ) -> None:
        """Write table description, column descriptions, and PII flags."""
        # Table description.
        table_desc = generate_description(table, columns)
        self.db.conn.execute(
            "INSERT INTO _cheeksbase.tables "
            "(connector_name, schema_name, table_name, description) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT (connector_name, schema_name, table_name) "
            "DO UPDATE SET description = excluded.description",
            [connector_name, schema, table, table_desc],
        )
        result.tables_annotated += 1

        # PII classification for this table's columns.
        pii = detect_pii(columns)
        if pii:
            result.pii_columns[table] = pii

        # Column descriptions + PII notes/metadata.
        for col in columns:
            pii_type = pii.get(col)
            description = generate_column_description(col, pii_type)
            note = f"pii:{pii_type}" if pii_type else None

            if note is not None:
                self.db.conn.execute(
                    "INSERT INTO _cheeksbase.columns "
                    "(connector_name, schema_name, table_name, column_name, description, note) "
                    "VALUES (?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT (connector_name, schema_name, table_name, column_name) "
                    "DO UPDATE SET description = excluded.description, note = excluded.note",
                    [connector_name, schema, table, col, description, note],
                )
                # Also expose PII through the generic metadata table so agents
                # querying by key can find it without knowing the `note` format.
                self.db.set_metadata(schema, table, "pii_type", pii_type, column=col)
            else:
                self.db.conn.execute(
                    "INSERT INTO _cheeksbase.columns "
                    "(connector_name, schema_name, table_name, column_name, description) "
                    "VALUES (?, ?, ?, ?, ?) "
                    "ON CONFLICT (connector_name, schema_name, table_name, column_name) "
                    "DO UPDATE SET description = excluded.description",
                    [connector_name, schema, table, col, description],
                )
            result.columns_annotated += 1
