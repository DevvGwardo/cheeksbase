"""Read-only access to a Hermes kanban.db SQLite store.

Hermes maintains its own task board (tasks, runs, events, links) per profile.
Cheeksbase reads it directly rather than syncing — single source of truth.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any


def kanban_db_path() -> Path:
    """Resolve the Hermes kanban DB. Override with HERMES_KANBAN_DB."""
    override = os.environ.get("HERMES_KANBAN_DB")
    if override:
        return Path(override).expanduser()

    home = Path.home() / ".hermes"
    candidates = [home / "kanban.db"]
    profiles_dir = home / "profiles"
    if profiles_dir.exists():
        candidates.extend(sorted(profiles_dir.glob("*/kanban.db")))

    best: Path | None = None
    best_count = -1
    for path in candidates:
        if not path.exists():
            continue
        try:
            with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as con:
                count = con.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
            if count > best_count:
                best_count = count
                best = path
        except sqlite3.Error:
            continue

    return best or (home / "kanban.db")


def _connect() -> sqlite3.Connection:
    path = kanban_db_path()
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    return con


def list_tasks() -> list[dict[str, Any]]:
    with _connect() as con:
        rows = con.execute(
            "SELECT id, title, body, assignee, status, priority, created_by, "
            "created_at, started_at, completed_at, last_heartbeat_at, "
            "current_run_id, tenant "
            "FROM tasks ORDER BY created_at DESC"
        ).fetchall()
    return [dict(row) for row in rows]


def list_runs(limit: int = 500) -> list[dict[str, Any]]:
    with _connect() as con:
        rows = con.execute(
            "SELECT id, task_id, profile, status, started_at, ended_at, "
            "outcome, summary, last_heartbeat_at "
            "FROM task_runs ORDER BY id DESC LIMIT ?",
            [limit],
        ).fetchall()
    return [dict(row) for row in rows]


def list_links() -> list[dict[str, Any]]:
    with _connect() as con:
        rows = con.execute(
            "SELECT parent_id, child_id FROM task_links"
        ).fetchall()
    return [dict(row) for row in rows]


def latest_event_id() -> int:
    with _connect() as con:
        row = con.execute("SELECT COALESCE(MAX(id), 0) AS id FROM task_events").fetchone()
    return int(row["id"]) if row else 0


def events_since(
    since_id: int = 0,
    task_id: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    sql = (
        "SELECT id, task_id, run_id, kind, payload, created_at "
        "FROM task_events WHERE id > ?"
    )
    params: list[Any] = [since_id]
    if task_id:
        sql += " AND task_id = ?"
        params.append(task_id)
    sql += " ORDER BY id ASC LIMIT ?"
    params.append(limit)
    with _connect() as con:
        rows = con.execute(sql, params).fetchall()
    return [dict(row) for row in rows]


def task_events(task_id: str, limit: int = 200) -> list[dict[str, Any]]:
    """All events for a single task, oldest first."""
    with _connect() as con:
        rows = con.execute(
            "SELECT id, task_id, run_id, kind, payload, created_at "
            "FROM task_events WHERE task_id = ? "
            "ORDER BY id ASC LIMIT ?",
            [task_id, limit],
        ).fetchall()
    return [dict(row) for row in rows]
