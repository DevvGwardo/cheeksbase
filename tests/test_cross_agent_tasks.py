"""Tests for cross-agent task storage and MCP tools."""

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


class TestCrossAgentTasksSchema:
    def test_init_creates_cross_agent_tasks_table(self, db: CheeksbaseDB):
        rows = db.query(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '_cheeksbase'
              AND table_name = 'cross_agent_tasks'
            """
        )
        assert len(rows) == 1


class TestCrossAgentTasksStore:
    def test_create_get_and_default_status(self, db: CheeksbaseDB):
        task_id = db.create_cross_agent_task(
            title="Implement /api/teams",
            source_agent="planner-1",
            target_agent="builder-1",
            team_id="team-ai-os",
            description="Add CRUD endpoints",
            acceptance_criteria="Endpoints respond 200 + tests pass",
        )
        assert task_id.startswith("task_")

        task = db.get_cross_agent_task(task_id)
        assert task is not None
        assert task["title"] == "Implement /api/teams"
        assert task["status"] == "pending"
        assert task["source_agent"] == "planner-1"
        assert task["target_agent"] == "builder-1"
        assert task["team_id"] == "team-ai-os"
        assert task["depends_on"] == []
        assert task["result"] is None
        assert task["completed_at"] is None

    def test_depends_on_round_trips_as_list(self, db: CheeksbaseDB):
        upstream = db.create_cross_agent_task(title="Schema", target_agent="builder-1")
        task_id = db.create_cross_agent_task(
            title="Implement",
            target_agent="builder-2",
            depends_on=[upstream, "task_other"],
        )
        task = db.get_cross_agent_task(task_id)
        assert task is not None
        assert task["depends_on"] == [upstream, "task_other"]

    def test_update_status_to_done_sets_completed_at_and_result(self, db: CheeksbaseDB):
        task_id = db.create_cross_agent_task(title="Build", target_agent="builder-1")
        updated = db.update_cross_agent_task(
            task_id,
            status="done",
            result={"rows_changed": 12, "notes": "ok"},
        )
        assert updated["status"] == "done"
        assert updated["completed_at"] is not None
        assert updated["result"] == {"rows_changed": 12, "notes": "ok"}

    def test_update_running_does_not_set_completed_at(self, db: CheeksbaseDB):
        task_id = db.create_cross_agent_task(title="Build", target_agent="builder-1")
        updated = db.update_cross_agent_task(task_id, status="running")
        assert updated["status"] == "running"
        assert updated["completed_at"] is None

    def test_update_unknown_task_returns_error(self, db: CheeksbaseDB):
        result = db.update_cross_agent_task("task_does_not_exist", status="done")
        assert "error" in result

    def test_list_filters_by_team_status_and_target(self, db: CheeksbaseDB):
        db.create_cross_agent_task(title="A", team_id="team-1", target_agent="b1")
        running_id = db.create_cross_agent_task(title="B", team_id="team-1", target_agent="b1")
        db.update_cross_agent_task(running_id, status="running")
        db.create_cross_agent_task(title="C", team_id="team-2", target_agent="b1")
        db.create_cross_agent_task(title="D", team_id="team-1", target_agent="b2")

        team_1 = db.list_cross_agent_tasks(team_id="team-1")
        assert {t["title"] for t in team_1} == {"A", "B", "D"}

        running = db.list_cross_agent_tasks(team_id="team-1", status="running")
        assert [t["title"] for t in running] == ["B"]

        b1 = db.list_cross_agent_tasks(team_id="team-1", target_agent="b1")
        assert {t["title"] for t in b1} == {"A", "B"}


class TestCrossAgentTasksMCPTools:
    def test_create_and_get_task_round_trip(self, mcp_db: CheeksbaseDB):
        created = json.loads(
            mcp_server.create_task(
                title="Wire dashboard",
                source_agent="planner-1",
                target_agent="builder-1",
                team_id="team-ai-os",
                depends_on="task_a, task_b",
                acceptance_criteria="Dashboard renders",
            )
        )
        assert created["status"] == "created"
        task_id = created["task_id"]

        fetched = json.loads(mcp_server.get_task(task_id=task_id))
        assert fetched["title"] == "Wire dashboard"
        assert fetched["status"] == "pending"
        assert fetched["depends_on"] == ["task_a", "task_b"]

    def test_update_task_records_status_and_result(self, mcp_db: CheeksbaseDB):
        task_id = json.loads(
            mcp_server.create_task(title="Build", target_agent="builder-1")
        )["task_id"]

        updated = json.loads(
            mcp_server.update_task(
                task_id=task_id,
                status="done",
                result_json='{"ok": true}',
            )
        )
        assert updated["status"] == "done"
        assert updated["result"] == {"ok": True}
        assert updated["completed_at"] is not None

    def test_list_tasks_filters(self, mcp_db: CheeksbaseDB):
        mcp_server.create_task(title="A", team_id="team-1", target_agent="b1")
        running_id = json.loads(
            mcp_server.create_task(title="B", team_id="team-1", target_agent="b1")
        )["task_id"]
        mcp_server.update_task(task_id=running_id, status="running")
        mcp_server.create_task(title="C", team_id="team-2", target_agent="b1")

        listed = json.loads(mcp_server.list_tasks(team_id="team-1", status="running"))
        assert [t["title"] for t in listed["tasks"]] == ["B"]

    def test_get_unknown_task_returns_error(self, mcp_db: CheeksbaseDB):
        result = json.loads(mcp_server.get_task(task_id="task_missing"))
        assert "error" in result
