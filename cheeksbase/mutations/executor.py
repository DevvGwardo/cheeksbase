"""Execute a confirmed mutation locally and optionally write back to the source."""

from __future__ import annotations

from typing import Any

from cheeksbase.core.db import CheeksbaseDB
from cheeksbase.mutations.preview import parse_target


def execute_mutation(
    sql: str,
    connector: dict[str, Any] | None,
    db: CheeksbaseDB,
) -> dict[str, Any]:
    """Execute a mutation against the local DuckDB copy and optional source API.

    Wrapped in a transaction so a failed source write-back rolls back the local
    change, keeping Cheeksbase in sync with the source of truth.

    Args:
        sql: The UPDATE/INSERT/DELETE statement.
        connector: Connector config dict (from `get_connectors()`), or None to
            skip source write-back.
        db: Cheeksbase DB handle.

    Returns:
        Dict with keys: rows_affected, local_applied, source_applied,
        source_error (optional).

    """
    result: dict[str, Any] = {
        "rows_affected": 0,
        "local_applied": False,
        "source_applied": False,
    }

    # Open a transaction so we can roll back if source write-back fails.
    db.conn.execute("BEGIN TRANSACTION")
    try:
        exec_result = db.conn.execute(sql)
        # DuckDB exposes affected rows via the connection after a DML statement.
        rows_affected: int | None = None
        try:
            # fetchone() on a DML execute returns (count,) in DuckDB.
            fetched = exec_result.fetchone()
            if fetched and isinstance(fetched[0], int):
                rows_affected = fetched[0]
        except Exception:
            rows_affected = None
        result["rows_affected"] = rows_affected if rows_affected is not None else 0
        result["local_applied"] = True
    except Exception as e:
        db.conn.execute("ROLLBACK")
        result["error"] = f"Local execution failed: {e}"
        return result

    # Attempt source write-back. If it fails, roll back the local change too so
    # Cheeksbase doesn't drift from the source of truth.
    if connector is not None:
        source_result = _write_back_to_source(sql, connector)
        if source_result.get("ok"):
            result["source_applied"] = True
            if "response" in source_result:
                result["source_response"] = source_result["response"]
        else:
            db.conn.execute("ROLLBACK")
            result["local_applied"] = False
            result["source_applied"] = False
            result["source_error"] = source_result.get("error", "source write-back failed")
            result["rolled_back"] = True
            return result

    db.conn.execute("COMMIT")
    return result


def _write_back_to_source(
    sql: str,
    connector: dict[str, Any],
) -> dict[str, Any]:
    """Call the source system's write API for this mutation.

    Currently supports rest_api connectors. Returns {"ok": True} on success,
    {"ok": False, "error": ...} on failure. Unsupported connector types are
    treated as a no-op success (mutation lives only in Cheeksbase).
    """
    connector_type = connector.get("type", "")

    if connector_type != "rest_api":
        # Unknown types can't be written back to — treat as local-only success.
        return {"ok": True, "response": {"skipped": f"no write-back for type '{connector_type}'"}}

    try:
        import httpx
    except ImportError:
        return {"ok": False, "error": "httpx not available for REST write-back"}

    parsed = parse_target(sql)
    op = parsed["operation"]
    base_url = connector.get("base_url") or connector.get("config", {}).get("base_url")
    if not base_url:
        return {"ok": False, "error": "connector is missing base_url"}

    # Resource lookup: match connector resource by table name.
    resources = connector.get("resources") or connector.get("config", {}).get("resources", [])
    resource = next((r for r in resources if r.get("name") == parsed["table"]), None)
    if resource is None:
        return {
            "ok": False,
            "error": (
                f"No resource definition found for table '{parsed['table']}' "
                f"in connector config. Write-back requires a matching resource entry."
            ),
        }
    endpoint = resource.get("endpoint", "")
    url = base_url.rstrip("/") + endpoint

    # Build auth headers.
    headers: dict[str, str] = {}
    auth = connector.get("auth") or connector.get("config", {}).get("auth") or {}
    credentials = connector.get("credentials", {})
    if auth.get("type") == "bearer":
        token_field = auth.get("token_field", "api_key")
        token = credentials.get(token_field)
        if token:
            headers["Authorization"] = f"Bearer {token}"

    method_map = {"UPDATE": "PATCH", "INSERT": "POST", "DELETE": "DELETE"}
    method = method_map.get(op)
    if method is None:
        return {"ok": False, "error": f"no HTTP mapping for operation {op}"}

    # Serialize mutation into JSON request body for the HTTP call
    body: dict[str, Any] | None = None
    if op == "UPDATE":
        # Parse SET clause into a dict of column -> value
        set_clause = parsed.get("set_clause", "") or ""
        set_fields: dict[str, Any] = {}
        for part in set_clause.split(","):
            part = part.strip()
            if "=" in part:
                key, val = part.split("=", 1)
                key = key.strip().strip('"')
                val = val.strip().strip("'")
                set_fields[key] = val
        body = {"table": parsed.get("table", ""), "set": set_fields}
        where = parsed.get("where")
        if where:
            body["where"] = where
    elif op == "INSERT":
        body = {"table": parsed.get("table", ""), "values": parsed.get("rest", "")}
    elif op == "DELETE":
        body = {"table": parsed.get("table", "")}
        where = parsed.get("where")
        if where:
            body["where"] = where

    try:
        with httpx.Client(timeout=15.0) as client:
            kwargs: dict[str, Any] = {"headers": headers}
            if body is not None:
                kwargs["json"] = body
            response = client.request(method, url, **kwargs)
        if response.status_code >= 400:
            return {"ok": False, "error": f"source returned {response.status_code}: {response.text[:200]}"}
        return {"ok": True, "response": {"status": response.status_code, "url": url}}
    except Exception as e:
        return {"ok": False, "error": f"HTTP call failed: {e}"}
