"""Preview and guardrail checks for mutation SQL."""

from __future__ import annotations

import re
from typing import Any

from cheeksbase.core.db import CheeksbaseDB

# Operations that are never allowed through the mutation engine.
BLOCKED_OPERATIONS = ("DROP", "ALTER", "TRUNCATE", "COPY", "ATTACH", "LOAD", "INSTALL")

# Operations the engine knows how to preview + execute.
SUPPORTED_OPERATIONS = ("UPDATE", "INSERT", "DELETE")


# --- SQL parsing helpers ---------------------------------------------------

_UPDATE_RE = re.compile(
    r'^\s*UPDATE\s+(?:"?(?P<schema>\w+)"?\s*\.\s*)?"?(?P<table>\w+)"?'
    r'\s+SET\s+(?P<set_clause>.+?)'
    r'(?:\s+WHERE\s+(?P<where>.+?))?'
    r'\s*;?\s*$',
    re.IGNORECASE | re.DOTALL,
)

_INSERT_RE = re.compile(
    r'^\s*INSERT\s+INTO\s+(?:"?(?P<schema>\w+)"?\s*\.\s*)?"?(?P<table>\w+)"?'
    r'\s*(?P<rest>.*)$',
    re.IGNORECASE | re.DOTALL,
)

_DELETE_RE = re.compile(
    r'^\s*DELETE\s+FROM\s+(?:"?(?P<schema>\w+)"?\s*\.\s*)?"?(?P<table>\w+)"?'
    r'(?:\s+WHERE\s+(?P<where>.+?))?'
    r'\s*;?\s*$',
    re.IGNORECASE | re.DOTALL,
)


def _first_word(sql: str) -> str:
    stripped = sql.strip()
    if not stripped:
        return ""
    # Strip comments
    cleaned = re.sub(r'--[^\n]*', '', stripped)
    cleaned = re.sub(r'/\*.*?\*/', '', cleaned, flags=re.DOTALL)
    cleaned = cleaned.strip()
    if not cleaned:
        return ""
    upper = cleaned.upper()
    if upper.startswith("WITH "):
        # Scan through CTE definitions to find where they end
        depth = 0
        i = 0
        n = len(cleaned)
        while i < n:
            ch = cleaned[i]
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    after = cleaned[i + 1:].lstrip()
                    if after and after[0] == ',':
                        i += 1  # more CTEs
                    elif after:
                        return after.split()[0].upper()
            i += 1
    return cleaned.split()[0].upper()


def parse_target(sql: str) -> dict[str, Any]:
    """Extract operation, schema, table, and clause data from mutation SQL.

    Returns a dict with keys: operation, schema, table, where, set_clause (for UPDATE),
    rest (for INSERT). Unknown fields are None.
    """
    op = _first_word(sql)
    result: dict[str, Any] = {
        "operation": op,
        "schema": None,
        "table": None,
        "where": None,
        "set_clause": None,
        "rest": None,
    }

    # Strip WITH CTE prefix so regexes can match the actual operation
    parse_sql = sql
    upper = sql.strip().upper()
    if upper.startswith("WITH "):
        depth = 0
        i = 0
        n = len(sql)
        while i < n:
            ch = sql[i]
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    after = sql[i + 1:].lstrip()
                    if after and after[0] == ',':
                        i += 1  # more CTEs
                    elif after:
                        parse_sql = after
                        break
            i += 1

    if op == "UPDATE":
        m = _UPDATE_RE.match(parse_sql)
        if m:
            result["schema"] = m.group("schema")
            result["table"] = m.group("table")
            result["set_clause"] = m.group("set_clause").strip() if m.group("set_clause") else None
            result["where"] = m.group("where").strip() if m.group("where") else None
    elif op == "INSERT":
        m = _INSERT_RE.match(parse_sql)
        if m:
            result["schema"] = m.group("schema")
            result["table"] = m.group("table")
            result["rest"] = m.group("rest").strip() if m.group("rest") else None
    elif op == "DELETE":
        m = _DELETE_RE.match(parse_sql)
        if m:
            result["schema"] = m.group("schema")
            result["table"] = m.group("table")
            result["where"] = m.group("where").strip() if m.group("where") else None

    return result


# --- Guardrails ------------------------------------------------------------

def validate_mutation(sql: str) -> list[str]:
    """Check guardrails for a mutation SQL statement.

    Returns a list of error messages. Empty list means the SQL is allowed.
    """
    errors: list[str] = []
    op = _first_word(sql)

    if not op:
        errors.append("Empty SQL statement.")
        return errors

    if op in BLOCKED_OPERATIONS:
        errors.append(
            f"{op} is not permitted through the mutation engine "
            f"(blocked operations: {', '.join(BLOCKED_OPERATIONS)})."
        )
        return errors

    if op not in SUPPORTED_OPERATIONS:
        errors.append(
            f"Unsupported mutation type: {op}. "
            f"Supported: {', '.join(SUPPORTED_OPERATIONS)}."
        )
        return errors

    parsed = parse_target(sql)
    if parsed["table"] is None:
        errors.append(f"Could not parse target table from {op} statement.")
        return errors

    # DELETE and UPDATE must have a WHERE clause — prevents catastrophic full-table mutations.
    if op in ("DELETE", "UPDATE") and not parsed["where"]:
        errors.append(f"{op} requires a WHERE clause.")

    return errors


# --- Preview generation ----------------------------------------------------

def generate_preview(sql: str, db: CheeksbaseDB, sample_limit: int = 10) -> dict[str, Any]:
    """Build a preview of what a mutation would do.

    For UPDATE/DELETE, runs a SELECT on the WHERE clause to show affected rows.
    For INSERT, reports the parsed target table.
    """
    parsed = parse_target(sql)
    op = parsed["operation"]
    schema = parsed["schema"]
    table = parsed["table"]

    preview: dict[str, Any] = {
        "operation": op,
        "schema": schema,
        "table": table,
    }

    if not table:
        preview["error"] = f"Could not parse target table from {op} statement."
        return preview

    qualified = f'"{schema}"."{table}"' if schema else f'"{table}"'

    if op in ("UPDATE", "DELETE"):
        where = parsed["where"]
        preview["where"] = where
        if op == "UPDATE":
            preview["set_clause"] = parsed["set_clause"]
        try:
            select_sql = f"SELECT * FROM {qualified}"
            if where:
                select_sql += f" WHERE {where}"
            select_sql += f" LIMIT {sample_limit}"
            rows = db.query(select_sql)

            count_sql = f"SELECT COUNT(*) AS cnt FROM {qualified}"
            if where:
                count_sql += f" WHERE {where}"
            count_rows = db.query(count_sql)
            affected = int(count_rows[0]["cnt"]) if count_rows else 0

            preview["affected_rows"] = affected
            preview["sample_rows"] = rows
            if affected > sample_limit:
                preview["truncated"] = True
        except Exception as e:
            preview["error"] = f"Could not preview affected rows: {e}"

    elif op == "INSERT":
        preview["payload"] = parsed["rest"]
        # Best-effort affected count — 1 unless the user is doing INSERT...SELECT.
        preview["affected_rows"] = None

    return preview
