# AI OS Integration — Cheeksbase ↔ Hermes-Deploy

Cheeksbase is the **brain** half of the AI OS architecture described in
`~/hermes-deploy/docs/ai-os-architecture.md`. Hermes-Deploy is the **face**
(Next.js dashboard, VM provisioning, billing). This document explains how the
spec's terminology maps to existing Cheeksbase tables and tools so the
hermes-deploy side can integrate without duplicating storage.

## Spec → Existing Schema

| Spec concept | Existing `_cheeksbase` table | Notes |
|---|---|---|
| Shared memory bus | `shared_memory` | Already supports scope/tags/embeddings/expiry |
| Agent registry & heartbeat | `agent_runs` | Use `workspace_id` as the team scope |
| War-room event log | `agent_events` | `event_type` + `payload_json` per spec; team scoping via `workspace_id` |
| Resource locks | `resource_claims` | Lease-based, conflict-aware |
| Cross-agent task DAG | `cross_agent_tasks` | **NEW** — added in this integration |

`workspace_id` plays the role of the spec's `team_id`. There is no separate
`teams` table on the cheeksbase side — team metadata lives in hermes-deploy's
Prisma schema. Cheeksbase only needs the team identifier as a scoping string.

## What `cross_agent_tasks` Adds

Spec phase 4 requires a queue with DAG dependencies. None of the existing
tables represent this, so a new one was added:

```sql
CREATE TABLE _cheeksbase.cross_agent_tasks (
    id VARCHAR PRIMARY KEY,
    team_id VARCHAR,
    title VARCHAR NOT NULL,
    description VARCHAR,
    source_agent VARCHAR,
    target_agent VARCHAR,
    depends_on VARCHAR,           -- comma-separated upstream task ids
    status VARCHAR DEFAULT 'pending',  -- pending | running | done | failed
    acceptance_criteria VARCHAR,
    result_json JSON,
    created_at TIMESTAMP DEFAULT current_timestamp,
    updated_at TIMESTAMP DEFAULT current_timestamp,
    completed_at TIMESTAMP
);
```

`completed_at` is set automatically when status transitions to `done` or
`failed`. `depends_on` is stored as a comma-separated string and hydrated into
a list by the read methods.

## MCP Tools

The cheeksbase MCP server exposes these tools for hermes-deploy to proxy:

### Shared memory (existing)
`remember_shared`, `recall_shared`, `recall_all_shared`, `forget_shared`,
`search_shared`, `search_shared_semantic`, `embed_shared`

### Agent coordination (existing)
`register_agent`, `heartbeat`, `post_event`, `claim_resource`,
`release_resource`, `list_agents`, `get_updates`

### Cross-agent tasks (added in this integration)
`create_task`, `get_task`, `update_task`, `list_tasks`

## Hermes-Deploy Side

Per the spec, hermes-deploy is responsible for:

- Prisma models for `Team`, `TeamMember`, `TeamAgent` (deployment metadata)
- `src/lib/cheeksbase.ts` — typed client wrapping the MCP tools above
- `/api/teams/*` Next.js routes that proxy to cheeksbase MCP at
  `http://localhost:8000/mcp/`
- War room UI that calls `get_updates` filtered by team
- Task DAG UI that calls `list_tasks` / `create_task` / `update_task`
- 3D hive mind, voice activation, etc. (spec phases 5–6)

## Why No Separate `warroom_events` Table

The spec sketches a dedicated `warroom_events` table, but
`agent_events` already provides the same shape (event_type, source via
run_id → agent_name, payload, timestamp) plus a per-event id. Using the
existing table avoids duplicating storage and lets the war room reuse the
indexed `(workspace_id, ts)` query path. Hermes-deploy queries
`get_updates(workspace_id=team_id, since_ts=cursor)` for war room feeds.

## Why No `team_id` Column on Existing Tables

Adding a `team_id` column to `agent_runs` / `agent_events` /
`resource_claims` would be redundant with `workspace_id`. The spec's
`team_id` and the existing `workspace_id` describe the same concept (a
shared scope for a group of agents). Hermes-deploy passes the team id as
`workspace_id` when calling cheeksbase tools.

## Dev Wiring

```bash
# Terminal 1: Cheeksbase MCP server
cd ~/cheeksbase
cheeksbase serve            # exposes MCP at http://localhost:8000/mcp/

# Terminal 2: Hermes-Deploy
cd ~/hermes-deploy
npm run dev
```

Hermes-deploy's `src/lib/cheeksbase.ts` should target
`http://localhost:8000/mcp/` and call the tools listed above.
