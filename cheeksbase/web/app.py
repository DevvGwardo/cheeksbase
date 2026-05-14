"""FastAPI app for the Cheeksbase web browser (slice 1: read-only)."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, AsyncGenerator

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from cheeksbase.core.db import CheeksbaseDB
from cheeksbase.core.query import QueryEngine
from cheeksbase.web import hermes as hermes_kanban

TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"
DEFAULT_PAGE_SIZE = 50


def create_app() -> FastAPI:
    """Build the FastAPI app."""
    app = FastAPI(title="Cheeksbase", docs_url=None, redoc_url=None)
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request) -> Any:
        with CheeksbaseDB() as db:
            engine = QueryEngine(db)
            result = engine.list_connectors()
        return templates.TemplateResponse(
            request,
            "index.html",
            {"connectors": result["connectors"]},
        )

    @app.get("/connectors/{name}", response_class=HTMLResponse)
    def connector_detail(request: Request, name: str) -> Any:
        with CheeksbaseDB() as db:
            engine = QueryEngine(db)
            listing = engine.list_connectors()
        entry = next((c for c in listing["connectors"] if c["name"] == name), None)
        if entry is None:
            raise HTTPException(status_code=404, detail=f"Connector {name!r} not found")
        return templates.TemplateResponse(
            request,
            "connector.html",
            {"connector": entry},
        )

    @app.get("/connectors/{name}/tables/{table}", response_class=HTMLResponse)
    def table_detail(
        request: Request,
        name: str,
        table: str,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> Any:
        if page < 1:
            page = 1
        if page_size < 1 or page_size > 500:
            page_size = DEFAULT_PAGE_SIZE

        offset = (page - 1) * page_size

        with CheeksbaseDB() as db:
            engine = QueryEngine(db)

            # Schema lookup — validates name/table pair exists, returns columns
            describe = engine.describe_table(f"{name}.{table}")
            if "error" in describe:
                raise HTTPException(status_code=404, detail=describe["error"])

            total_rows = db.get_row_count(name, table)

            # Use the validated identifiers from describe to build the query
            # (QueryEngine.execute handles SQL safety via its own validation)
            schema = describe["schema"]
            table_name = describe["table"]
            sql = (
                f'SELECT * FROM "{schema}"."{table_name}" '
                f"LIMIT {page_size} OFFSET {offset}"
            )
            result = engine.execute(sql, max_rows=page_size, use_cache=False)

        if "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])

        total_pages = max(1, (total_rows + page_size - 1) // page_size)

        return templates.TemplateResponse(
            request,
            "table.html",
            {
                "connector_name": name,
                "table_name": table,
                "schema": describe,
                "columns": result["columns"],
                "rows": result["rows"],
                "page": page,
                "page_size": page_size,
                "total_rows": total_rows,
                "total_pages": total_pages,
            },
        )

    @app.get("/tasks", response_class=HTMLResponse)
    def tasks_index(
        request: Request,
        team_id: str | None = None,
        status: str | None = None,
        target_agent: str | None = None,
    ) -> Any:
        with CheeksbaseDB() as db:
            tasks = db.list_cross_agent_tasks(
                team_id=team_id or None,
                status=status or None,
                target_agent=target_agent or None,
                limit=500,
            )
        counts = {"pending": 0, "running": 0, "done": 0, "failed": 0}
        for t in tasks:
            counts[t["status"]] = counts.get(t["status"], 0) + 1
        return templates.TemplateResponse(
            request,
            "tasks.html",
            {
                "tasks": tasks,
                "counts": counts,
                "filter_team_id": team_id or "",
                "filter_status": status or "",
                "filter_target_agent": target_agent or "",
            },
        )

    @app.post("/tasks/create")
    def tasks_create(
        title: str = Form(...),
        source_agent: str = Form(""),
        target_agent: str = Form(""),
        team_id: str = Form(""),
        description: str = Form(""),
        depends_on: str = Form(""),
        acceptance_criteria: str = Form(""),
    ) -> Any:
        if not title.strip():
            raise HTTPException(status_code=400, detail="title is required")
        deps = [d.strip() for d in depends_on.split(",") if d.strip()] or None
        with CheeksbaseDB() as db:
            db.create_cross_agent_task(
                title=title.strip(),
                source_agent=source_agent.strip() or None,
                target_agent=target_agent.strip() or None,
                team_id=team_id.strip() or None,
                description=description.strip() or None,
                depends_on=deps,
                acceptance_criteria=acceptance_criteria.strip() or None,
            )
        return RedirectResponse(url="/tasks", status_code=303)

    @app.get("/tasks/dag", response_class=HTMLResponse)
    def tasks_dag(request: Request) -> Any:
        with CheeksbaseDB() as db:
            tasks = db.list_cross_agent_tasks(limit=500)

        known = {t["id"] for t in tasks}
        lines = ["flowchart TD"]
        for t in tasks:
            label = t["title"]
            for bad, good in (('"', "'"), ("<", "("), (">", ")"), ("&", "+")):
                label = label.replace(bad, good)
            if len(label) > 50:
                label = label[:47] + "..."
            lines.append(f'    {t["id"]}["{label}"]:::{t["status"]}')
        for t in tasks:
            for dep in t["depends_on"]:
                if dep in known:
                    lines.append(f"    {dep} --> {t['id']}")
        lines += [
            "    classDef pending fill:#f5f5f5,stroke:#888,color:#222;",
            "    classDef running fill:#fef3c7,stroke:#d97706,color:#222;",
            "    classDef done fill:#dcfce7,stroke:#16a34a,color:#222;",
            "    classDef failed fill:#fee2e2,stroke:#dc2626,color:#222;",
        ]
        mermaid_src = "\n".join(lines)

        counts = {"pending": 0, "running": 0, "done": 0, "failed": 0}
        for t in tasks:
            counts[t["status"]] = counts.get(t["status"], 0) + 1

        return templates.TemplateResponse(
            request,
            "tasks_dag.html",
            {"mermaid_src": mermaid_src, "counts": counts, "task_count": len(tasks)},
        )

    @app.get("/graph", response_class=HTMLResponse)
    def graph_view(request: Request) -> Any:
        return templates.TemplateResponse(
            request,
            "graph.html",
            {"hermes_db": str(hermes_kanban.kanban_db_path())},
        )

    @app.get("/graph/data")
    def graph_data() -> dict[str, Any]:
        status_color = {
            "todo":     "#9ca3af",
            "triage":   "#a78bfa",
            "ready":    "#60a5fa",
            "running":  "#f59e0b",
            "blocked":  "#fb7185",
            "done":     "#22c55e",
            "archived": "#6b7280",
            "failed":   "#ef4444",
        }
        tasks = hermes_kanban.list_tasks()
        runs = hermes_kanban.list_runs(limit=200)
        links_raw = hermes_kanban.list_links()

        nodes: list[dict[str, Any]] = []
        links: list[dict[str, Any]] = []
        agent_seen: set[str] = set()
        task_ids: set[str] = set()

        def ensure_agent(name: str | None) -> str | None:
            if not name:
                return None
            node_id = f"agent:{name}"
            if node_id not in agent_seen:
                agent_seen.add(node_id)
                nodes.append({
                    "id": node_id,
                    "name": name,
                    "type": "agent",
                    "role": "profile",
                    "color": "#06b6d4",
                    "val": 7,
                })
            return node_id

        for t in tasks:
            task_ids.add(t["id"])
            nodes.append({
                "id": t["id"],
                "name": t["title"],
                "type": "task",
                "status": t["status"],
                "tenant": t.get("tenant"),
                "color": status_color.get(t["status"], "#9ca3af"),
                "val": 5 if t["status"] not in ("running", "ready") else 8,
            })
            target_id = ensure_agent(t.get("assignee"))
            if target_id:
                links.append({
                    "source": target_id,
                    "target": t["id"],
                    "type": "assigned_to",
                    "color": "#3b82f6",
                })

        for r in runs:
            agent_id = ensure_agent(r.get("profile"))
            if agent_id and r["task_id"] in task_ids and r["status"] == "running":
                links.append({
                    "source": agent_id,
                    "target": r["task_id"],
                    "type": "running",
                    "color": "#f59e0b",
                })

        for link in links_raw:
            if link["parent_id"] in task_ids and link["child_id"] in task_ids:
                links.append({
                    "source": link["parent_id"],
                    "target": link["child_id"],
                    "type": "depends_on",
                    "color": "#cbd5e1",
                })

        return {
            "nodes": nodes,
            "links": links,
            "cursor": hermes_kanban.latest_event_id(),
        }

    @app.get("/graph/task/{task_id}")
    def graph_task_detail(task_id: str) -> dict[str, Any]:
        tasks = [t for t in hermes_kanban.list_tasks() if t["id"] == task_id]
        if not tasks:
            raise HTTPException(status_code=404, detail=f"Task {task_id!r} not found")
        events = hermes_kanban.task_events(task_id, limit=200)
        return {"task": tasks[0], "events": events}

    async def _event_stream(
        since_id: int, task_id: str | None
    ) -> AsyncGenerator[str, None]:
        cursor = since_id
        loop = asyncio.get_event_loop()
        # send a hello so the client knows the stream is open
        yield f"event: open\ndata: {json.dumps({'cursor': cursor})}\n\n"
        while True:
            try:
                events = await loop.run_in_executor(
                    None,
                    lambda: hermes_kanban.events_since(cursor, task_id=task_id),
                )
                for evt in events:
                    payload = json.dumps(evt, default=str)
                    yield f"event: task_event\ndata: {payload}\n\n"
                    cursor = max(cursor, int(evt["id"]))
                # heartbeat keeps the connection alive past proxies
                yield f": ping {cursor}\n\n"
            except Exception as e:  # noqa: BLE001
                yield f"event: error\ndata: {json.dumps({'error': str(e)})}\n\n"
            await asyncio.sleep(1)

    @app.get("/graph/stream")
    async def graph_stream(since_id: int = 0, task_id: str | None = None) -> Any:
        return StreamingResponse(
            _event_stream(since_id, task_id),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.post("/tasks/{task_id}/status")
    def tasks_set_status(task_id: str, status: str = Form(...)) -> Any:
        if status not in ("pending", "running", "done", "failed"):
            raise HTTPException(status_code=400, detail=f"invalid status: {status}")
        with CheeksbaseDB() as db:
            result = db.update_cross_agent_task(task_id, status=status)
        if "error" in result:
            raise HTTPException(status_code=404, detail=result["error"])
        return RedirectResponse(url="/tasks", status_code=303)

    return app
