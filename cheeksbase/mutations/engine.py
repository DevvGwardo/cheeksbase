"""Mutation engine — preview + confirm flow for agent-driven writes."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from cheeksbase.core.db import META_SCHEMA, CheeksbaseDB
from cheeksbase.mutations.executor import execute_mutation
from cheeksbase.mutations.preview import (
    generate_preview,
    parse_target,
    validate_mutation,
)


class MutationEngine:
    """Two-step mutation flow: handle_sql() previews, confirm() executes.

    Agents call handle_sql() with their SQL, receive a preview + mutation_id,
    then call confirm(mutation_id) to actually run it. Pending mutations are
    persisted in `_cheeksbase.mutations` so agents can inspect or confirm
    across sessions.
    """

    def __init__(self, db: CheeksbaseDB) -> None:
        """Create a MutationEngine backed by the given database connection."""
        self.db = db

    # --- Public API --------------------------------------------------------

    def handle_sql(self, sql: str) -> dict[str, Any]:
        """Validate, preview, and record a pending mutation.

        Returns a dict with status='pending' and a mutation_id the agent can
        pass to confirm(). If guardrails fail, returns status='rejected' and
        an errors list without persisting anything.
        """
        errors = validate_mutation(sql)
        if errors:
            return {
                "status": "rejected",
                "errors": errors,
                "sql": sql,
            }

        preview = generate_preview(sql, self.db)
        parsed = parse_target(sql)
        mutation_id = f"mut_{uuid.uuid4().hex[:12]}"

        self._record_pending(
            mutation_id=mutation_id,
            sql=sql,
            operation=parsed["operation"],
            schema=parsed["schema"] or "",
            table=parsed["table"] or "",
            preview=preview,
        )

        return {
            "status": "pending",
            "mutation_id": mutation_id,
            "sql": sql,
            "preview": preview,
            "message": (
                f"Preview only — call confirm('{mutation_id}') to execute. "
                "Review affected_rows and sample_rows before confirming."
            ),
        }

    # Convenience alias so the documented `engine.execute(sql)` flow works.
    execute = handle_sql

    def confirm(self, mutation_id: str) -> dict[str, Any]:
        """Execute a previously-previewed mutation."""
        record = self._load_pending(mutation_id)
        if record is None:
            return {
                "status": "error",
                "error": f"Unknown mutation_id: {mutation_id}",
            }
        if record["status"] != "pending":
            return {
                "status": "error",
                "error": (
                    f"Mutation {mutation_id} is not pending "
                    f"(current status: {record['status']})."
                ),
            }

        sql = record["sql_text"]
        schema = record["connector_name"]  # stored schema name == connector name

        connector_config = self._load_connector_config(schema)

        self._mark_confirmed(mutation_id)
        exec_result = execute_mutation(sql, connector_config, self.db)

        if exec_result.get("error") or exec_result.get("source_error"):
            self._mark_failed(
                mutation_id,
                exec_result.get("error") or exec_result.get("source_error"),
                exec_result,
            )
            return {
                "status": "failed",
                "mutation_id": mutation_id,
                "result": exec_result,
            }

        self._mark_executed(mutation_id, exec_result)
        return {
            "status": "executed",
            "mutation_id": mutation_id,
            "result": exec_result,
        }

    def list_pending(self) -> list[dict[str, Any]]:
        """Return all pending mutations."""
        rows = self.db.query(
            f"SELECT mutation_id, connector_name, table_name, operation, "
            f"sql_text, preview, created_at "
            f"FROM {META_SCHEMA}.mutations "
            f"WHERE status = 'pending' "
            f"ORDER BY created_at DESC"
        )
        for r in rows:
            if isinstance(r.get("preview"), str):
                try:
                    r["preview"] = json.loads(r["preview"])
                except json.JSONDecodeError:
                    pass
        return rows

    # --- Persistence helpers ----------------------------------------------

    def _record_pending(
        self,
        mutation_id: str,
        sql: str,
        operation: str,
        schema: str,
        table: str,
        preview: dict[str, Any],
    ) -> None:
        self.db.conn.execute(
            f"INSERT INTO {META_SCHEMA}.mutations "
            f"(mutation_id, connector_name, table_name, operation, sql_text, preview, status) "
            f"VALUES (?, ?, ?, ?, ?, ?, 'pending')",
            [mutation_id, schema, table, operation, sql, json.dumps(preview, default=str)],
        )

    def _load_pending(self, mutation_id: str) -> dict[str, Any] | None:
        result = self.db.conn.execute(
            f"SELECT mutation_id, connector_name, table_name, operation, "
            f"sql_text, preview, status "
            f"FROM {META_SCHEMA}.mutations "
            f"WHERE mutation_id = ?",
            [mutation_id],
        )
        cols = [d[0] for d in result.description]
        row = result.fetchone()
        return dict(zip(cols, row)) if row else None

    def _mark_confirmed(self, mutation_id: str) -> None:
        self.db.conn.execute(
            f"UPDATE {META_SCHEMA}.mutations "
            f"SET status = 'confirmed', confirmed_at = ? "
            f"WHERE mutation_id = ?",
            [datetime.now(timezone.utc), mutation_id],
        )

    def _mark_executed(self, mutation_id: str, result: dict[str, Any]) -> None:
        self.db.conn.execute(
            f"UPDATE {META_SCHEMA}.mutations "
            f"SET status = 'executed', executed_at = ?, result = ? "
            f"WHERE mutation_id = ?",
            [
                datetime.now(timezone.utc),
                json.dumps(result, default=str),
                mutation_id,
            ],
        )

    def _mark_failed(
        self,
        mutation_id: str,
        error: str | None,
        result: dict[str, Any],
    ) -> None:
        self.db.conn.execute(
            f"UPDATE {META_SCHEMA}.mutations "
            f"SET status = 'failed', executed_at = ?, result = ?, error_message = ? "
            f"WHERE mutation_id = ?",
            [
                datetime.now(timezone.utc),
                json.dumps(result, default=str),
                error,
                mutation_id,
            ],
        )

    def _load_connector_config(self, schema: str) -> dict[str, Any] | None:
        """Look up the connector config for the schema targeted by this mutation."""
        if not schema:
            return None
        try:
            from cheeksbase.core.config import get_connectors
        except ImportError:
            return None
        connectors = get_connectors()
        return connectors.get(schema)
