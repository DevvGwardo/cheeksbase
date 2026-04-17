"""FastAPI app for the Cheeksbase web browser (slice 1: read-only)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from cheeksbase.core.db import CheeksbaseDB
from cheeksbase.core.query import QueryEngine

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

    return app
