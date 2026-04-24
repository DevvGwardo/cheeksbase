"""Tests for multi-agent coordination storage and MCP tools."""

from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

import pytest

import cheeksbase.core.config as cfg
from cheeksbase.core.config import init_cheeksbase
from cheeksbase.core.db import CheeksbaseDB
from cheeksbase.mcp import server as mcp_server


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
    init_cheeksbase()
    db = CheeksbaseDB()
    yield db
    db.close()


@pytest.fixture
def mcp_db(db, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(mcp_server, "_db", db)
    yield db
    monkeypatch.setattr(mcp_server, "_db", None)


class TestCoordinationMetadata:
    def test_init_creates_coordination_tables_and_views(self, db: CheeksbaseDB):
        rows = db.query(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '_cheeksbase'
              AND table_name IN ('agent_runs', 'agent_events', 'resource_claims')
            ORDER BY table_name
            """
        )
        assert [row["table_name"] for row in rows] == [
            "agent_events",
            "agent_runs",
            "resource_claims",
        ]

        view_rows = db.query(
            """
            SELECT table_name
            FROM information_schema.views
            WHERE table_schema = '_cheeksbase'
              AND table_name IN ('active_agent_runs', 'active_resource_claims')
            ORDER BY table_name
            """
        )
        assert [row["table_name"] for row in view_rows] == [
            "active_agent_runs",
            "active_resource_claims",
        ]


class TestCoordinationStore:
    def test_register_heartbeat_and_list_agents(self, db: CheeksbaseDB):
        run_id = db.register_agent_run(
            agent_name="builder-1",
            role="builder",
            workspace_id="repo-a",
            profile_name="builder",
        )
        db.heartbeat_agent_run(
            run_id,
            current_task="Implement coordination",
            current_summary="Wiring MCP tools",
            progress=0.4,
        )

        agents = db.list_agent_runs(workspace_id="repo-a")
        assert len(agents) == 1
        assert agents[0]["run_id"] == run_id
        assert agents[0]["agent_name"] == "builder-1"
        assert agents[0]["current_task"] == "Implement coordination"
        assert agents[0]["progress"] == pytest.approx(0.4)
        assert agents[0]["open_claim_count"] == 0

    def test_claim_and_release_resource_with_lease(self, db: CheeksbaseDB):
        run_id = db.register_agent_run(
            agent_name="builder-1",
            role="builder",
            workspace_id="repo-a",
        )

        claim = db.claim_resource(
            run_id=run_id,
            resource_type="file",
            resource_key="repo-a:src/app.py",
            lease_seconds=120,
        )
        assert claim["status"] == "claimed"
        assert claim["resource_key"] == "repo-a:src/app.py"

        second_run = db.register_agent_run(
            agent_name="reviewer-1",
            role="reviewer",
            workspace_id="repo-a",
        )
        conflict = db.claim_resource(
            run_id=second_run,
            resource_type="file",
            resource_key="repo-a:src/app.py",
            lease_seconds=120,
        )
        assert conflict["status"] == "conflict"
        assert conflict["claimed_by"] == run_id

        released = db.release_resource(run_id=run_id, resource_key="repo-a:src/app.py")
        assert released["status"] == "released"

        reclaimed = db.claim_resource(
            run_id=second_run,
            resource_type="file",
            resource_key="repo-a:src/app.py",
            lease_seconds=120,
        )
        assert reclaimed["status"] == "claimed"
        assert reclaimed["claimed_by"] == second_run

    def test_post_event_and_get_updates_since_timestamp(self, db: CheeksbaseDB):
        run_id = db.register_agent_run(
            agent_name="builder-1",
            role="builder",
            workspace_id="repo-a",
        )
        db.post_agent_event(
            run_id=run_id,
            event_type="task_started",
            task_id="task-1",
            summary_text="Started task 1",
            payload={"step": 1},
        )
        first_ts = db.query(
            "SELECT MAX(ts) AS ts FROM _cheeksbase.agent_events WHERE run_id = ?",
            [run_id],
        )[0]["ts"]

        db.post_agent_event(
            run_id=run_id,
            event_type="task_progress",
            task_id="task-1",
            summary_text="Halfway done",
            payload={"step": 2},
        )
        db.claim_resource(
            run_id=run_id,
            resource_type="file",
            resource_key="repo-a:src/app.py",
            lease_seconds=120,
        )

        updates = db.get_agent_updates(workspace_id="repo-a", since_ts=first_ts)
        assert len(updates["events"]) == 1
        assert updates["events"][0]["event_type"] == "task_progress"
        assert updates["events"][0]["payload"]["step"] == 2
        assert len(updates["claims"]) == 1
        assert updates["claims"][0]["resource_key"] == "repo-a:src/app.py"
        assert len(updates["agents"]) == 1
        assert updates["agents"][0]["run_id"] == run_id


class TestCoordinationMCPTools:
    def test_register_agent_tool_returns_run_id(self, mcp_db: CheeksbaseDB):
        result = json.loads(
            mcp_server.register_agent(
                agent_name="builder-1",
                role="builder",
                workspace_id="repo-a",
                profile_name="builder",
            )
        )
        assert result["status"] == "registered"
        assert result["run_id"].startswith("run_")

    def test_claim_release_and_list_agents_tools(self, mcp_db: CheeksbaseDB):
        run_id = json.loads(
            mcp_server.register_agent(
                agent_name="builder-1",
                role="builder",
                workspace_id="repo-a",
            )
        )["run_id"]

        claim = json.loads(
            mcp_server.claim_resource(
                run_id=run_id,
                resource_key="repo-a:src/app.py",
                resource_type="file",
                lease_seconds=120,
            )
        )
        assert claim["status"] == "claimed"

        agents = json.loads(mcp_server.list_agents(workspace_id="repo-a"))
        assert agents["agents"][0]["open_claim_count"] == 1

        released = json.loads(
            mcp_server.release_resource(run_id=run_id, resource_key="repo-a:src/app.py")
        )
        assert released["status"] == "released"

    def test_heartbeat_post_event_and_get_updates_tools(self, mcp_db: CheeksbaseDB):
        run_id = json.loads(
            mcp_server.register_agent(
                agent_name="builder-1",
                role="builder",
                workspace_id="repo-a",
            )
        )["run_id"]

        heartbeat = json.loads(
            mcp_server.heartbeat(
                run_id=run_id,
                current_task="task-1",
                current_summary="working",
                progress=0.25,
            )
        )
        assert heartbeat["status"] == "ok"

        event = json.loads(
            mcp_server.post_event(
                run_id=run_id,
                event_type="task_progress",
                task_id="task-1",
                summary_text="did some work",
                payload_json='{"delta": 1}',
            )
        )
        assert event["status"] == "recorded"

        updates = json.loads(mcp_server.get_updates(workspace_id="repo-a"))
        assert updates["events"][0]["event_type"] == "task_progress"
        assert updates["agents"][0]["current_task"] == "task-1"
