# Cheeksbase Bases — Obsidian-Style Views for DuckDB

> Spec: Mapping Obsidian Bases concepts onto Cheeksbase + DuckDB
> Status: Draft for implementation
> Date: 2026-05-19

## Overview

Obsidian Bases lets users create structured database views over markdown
notes — define a query, apply filters, add formulas, configure display.
This spec ports that concept onto Cheeksbase, which already runs on DuckDB.

The result: **named, queryable, configurable views** over any synced data
source, with zero additional infrastructure.

## Core Mapping

| Obsidian Bases Concept | Cheeksbase / DuckDB Equivalent |
|---|---|
| `.base` file (YAML) | `_cheeksbase.bases` row or `.base.yaml` file |
| Query (dataview SQL) | DuckDB `SELECT` — stored as text |
| Filters UI state | `display_config` JSON column (columns, sort, where) |
| Formulas | `CREATE MACRO` for reusable expressions |
| Virtual base (live query) | `CREATE VIEW` — zero storage, always fresh |
| Materialized base (snapshot) | `CREATE TABLE ... AS` — fast reads, needs refresh |
| Base linking | `_cheeksbase.base_links` — base A feeds into base B |
| Embedded fields | DuckDB JSON extraction via `->>` syntax + `json_group_array` |

## Database Schema

### `_cheeksbase.bases` — Base registry

```sql
CREATE TABLE _cheeksbase.bases (
    name            VARCHAR PRIMARY KEY,          -- unique base name
    label           VARCHAR,                       -- human-readable title
    query_sql       VARCHAR NOT NULL,              -- the SELECT statement
    query_type      VARCHAR NOT NULL DEFAULT 'virtual',  -- 'virtual' | 'materialized'
    materialized_table VARCHAR,                    -- table name if materialized
    display_config  JSON,                          -- {columns, sort, filters, group_by, pivot}
    formulas        JSON,                          -- {name: sql_expr, ...} macros
    source_tables   VARCHAR[],                      -- tables this base depends on
    description     VARCHAR,
    owner           VARCHAR DEFAULT 'agent',       -- 'user' | 'agent' | system
    row_limit       INTEGER DEFAULT 5000,
    auto_refresh    BOOLEAN DEFAULT false,          -- auto-refresh materialized on sync
    created_at      TIMESTAMP DEFAULT current_timestamp,
    updated_at      TIMESTAMP DEFAULT current_timestamp
);
```

### `_cheeksbase.base_links` — Base dependency graph

```sql
CREATE TABLE _cheeksbase.base_links (
    source_base     VARCHAR NOT NULL REFERENCES _cheeksbase.bases(name),
    target_base     VARCHAR NOT NULL REFERENCES _cheeksbase.bases(name),
    join_condition  VARCHAR,                      -- optional ON clause
    PRIMARY KEY (source_base, target_base)
);
```

### `_cheeksbase.base_formulas` — Reusable macros

```sql
CREATE TABLE _cheeksbase.base_formulas (
    name            VARCHAR PRIMARY KEY,
    macro_sql       VARCHAR NOT NULL,              -- e.g. 'ROI(profit, cost) AS (profit - cost) / cost * 100'
    description     VARCHAR,
    return_type     VARCHAR,                       -- hint: 'number', 'text', 'boolean'
    created_at      TIMESTAMP DEFAULT current_timestamp
);
```

## Query Types

### Virtual Base (default)

A `CREATE VIEW` that stays live against source data. Zero storage cost,
always current. Best for:
- Live dashboards
- Ad-hoc aggregations
- Data that changes frequently

```
CREATE VIEW bases.my_live_base AS
SELECT name, value, created_at
FROM twitter.posts
WHERE created_at > current_date - INTERVAL '7 days'
ORDER BY created_at DESC;
```

### Materialized Base

A `CREATE TABLE ... AS` snapshot. Consumes storage, needs refresh.
Best for:
- Expensive computations (joins, aggregations)
- Data that changes on sync schedule
- Serving data to downstream consumers

```
-- Create
CREATE TABLE bases.my_materialized_base AS
SELECT source, COUNT(*) as post_count, AVG(sentiment) as avg_sentiment
FROM twitter.posts
GROUP BY source;

-- Refresh
DELETE FROM bases.my_materialized_base;
INSERT INTO bases.my_materialized_base
SELECT source, COUNT(*) as post_count, AVG(sentiment) as avg_sentiment
FROM twitter.posts
GROUP BY source;
```

### Snapshot Base

A point-in-time copy — never refreshed. Useful for:
- Comparisons between periods
- Audit trails
- Freezing reference data

```
CREATE TABLE bases.q1_2026_posts AS
SELECT * FROM instagram.posts
WHERE created_at >= '2026-01-01' AND created_at < '2026-04-01';
```

## Formulas / Macros

DuckDB `CREATE MACRO` gives us Obsidian-formula-level power without
needing a custom expression parser.

### Built-in macros on init

```sql
CREATE MACRO _cheeksbase.pct(a, b) AS CASE WHEN b = 0 THEN 0 ELSE a / b * 100 END;
CREATE MACRO _cheeksbase.avg_sentiment(tbl) AS (
    SELECT AVG(sentiment) FROM query(tbl)
);
CREATE MACRO _cheeksbase.rank_over(col) AS rank() OVER (ORDER BY col DESC);
```

### User-defined via `CREATE MACRO`

Stored in `_cheeksbase.base_formulas` and auto-created on DB init.

### Usage in base queries

```sql
-- Using a stored macro
SELECT name, _cheeksbase.pct(likes, impressions) as engagement_rate
FROM twitter.posts;

-- Using an inline macro
CREATE MACRO engagement(impressions) AS impressions * 0.05;
SELECT name, engagement(impressions) FROM twitter.posts;
```

## Base File Format (YAML)

Bases can be defined as files (like Obsidian `.base`) or stored in the
metadata table. Files are simpler for git tracking and portability.

### `bases/top-posts.base.yaml`

```yaml
name: top-posts
label: Top Posts This Week
query: >
  SELECT p.*, u.name as author_name
  FROM twitter.posts p
  JOIN twitter.users u ON p.author_id = u.id
  WHERE p.created_at > current_date - INTERVAL '7 days'
  ORDER BY p.likes DESC
type: virtual
display:
  columns:
    - author_name
    - text
    - likes
    - created_at
  sort:
    - column: likes
      direction: desc
  limit: 25
  group_by: null
formulas:
  engagement_rate: "likes / impressions * 100"
  reading_time: "LENGTH(text) / 200.0"
auto_refresh: false
```

### `bases/dashboard.base.yaml`

```yaml
name: social-dashboard
label: Social Media Dashboard
query: >
  SELECT
    source,
    COUNT(*) as posts,
    AVG(likes) as avg_likes,
    AVG(impressions) as avg_impressions
  FROM twitter.posts
  GROUP BY source
type: materialized
refresh_interval: 3600   # seconds — auto-refresh via cron
display:
  columns:
    - source
    - posts
    - avg_likes
    - avg_impressions
  chart: bar              # hint for UI rendering
  limit: 50
formulas: {}
auto_refresh: true
```

## CLI Commands

```bash
# Create a base from a file
cheeksbase base create ./bases/top-posts.base.yaml

# Create a base inline
cheeksbase base create top-posts \
  --query "SELECT * FROM twitter.posts ORDER BY likes DESC" \
  --type virtual \
  --label "Top Posts"

# List all bases
cheeksbase base list
# → top-posts (virtual)  | Top Posts This Week    | twitter.posts
# → social-dashboard (mat)| Social Media Dashboard | twitter.posts

# Show base definition + preview
cheeksbase base show top-posts

# Refresh a materialized base
cheeksbase base refresh top-posts

# Refresh all stale materialized bases
cheeksbase base refresh --stale

# Update a base definition
cheeksbase base update top-posts --query "..." --label "..."
```

### Base directory scanning

A `bases/` directory under `~/.cheeksbase/` is auto-scanned on startup.
Files matching `*.base.yaml` are registered in `_cheeksbase.bases` if
not already present (upsert by name).

```
~/.cheeksbase/
├── config.yaml
├── cheeksbase.duckdb
├── connectors/
├── cache/
└── bases/               ← new
    ├── top-posts.base.yaml
    └── dashboard.base.yaml
```

## MCP Tools (Agent Interface)

For AI agents (Hermes, Claude Code, etc.) that use Cheeksbase's MCP server:

### `base_create(name, query_sql, type, label, display_config, formulas)`
Register a new base.

### `base_list()`
Return all registered bases with metadata.

### `base_query(name, limit, filters)`
Run the base's query and return results. For virtual bases, executes live.
For materialized bases, queries the cached table (or prompts refresh if stale).

### `base_refresh(name)`
Drop and recreate a materialized base.

### `base_formula_create(name, macro_sql, description)`
Register a reusable macro.

## Implementation Phases

### Phase 1 — Core (2-3 hours)
- Add `bases` table to `INIT_SQL` in `db.py`
- Add `base_formulas` + `base_links` tables
- Implement `create_base()`, `drop_base()`, `refresh_base()` methods on `CheeksbaseDB`
- Implement base directory scanning in `_init_metadata()`
- Add `base create / list / show / refresh / update` CLI commands
- Implement `CREATE MACRO` lifecycle (init builtins, register user macros)

### Phase 2 — Materialized Base Management (1-2 hours)
- Auto-refresh on connector sync (when `auto_refresh=true`)
- Staleness detection: `SELECT * FROM base WHERE last_refresh < threshold`
- Concurrent refresh protection (lock per base name)
- Base dependency graph: refresh dependent bases in topological order

### Phase 3 — MCP + Agent Interface (1 hour)
- Expose `base_create`, `base_list`, `base_query`, `base_refresh` as MCP tools
- Add `base_formula_create` tool
- Let agents discover and compose bases automatically

### Phase 4 — Display & Web (2-3 hours)
- `display_config` drives the web UI column picker, sort, filter controls
- Auto-chart type inference from query result shapes
- Base editor in the web UI (if web module is enabled)

## Potential Pitfalls

1. **Macro scope**: DuckDB `CREATE MACRO` is connection-scoped. If the
   CheeksbaseDB connection is recreated (close + reopen), macros must be
   re-registered. Solution: store macro definitions in `base_formulas` table
   and replay on every `_init_metadata()` call.

2. **Materialized base staleness**: A materialized base is a snapshot. If
   the source table gets updated (via sync), the base is stale until
   explicitly refreshed. `auto_refresh=true` triggers refresh on connector
   sync, but for manual syncs the user must call `base refresh`.

3. **DROP vs TRUNCATE for refresh**: `DELETE FROM` + `INSERT INTO` vs
   `DROP` + `CREATE TABLE ... AS`. The latter is faster but drops indexes
   and schemas. Use `DELETE` + `INSERT` within a transaction for safety,
   or `CREATE OR REPLACE TABLE` (DuckDB 1.0+).

4. **Circular base links**: `base_links` could form cycles. Detect and
   reject cycles in `create_base()` by traversing the dependency graph.

5. **Query injection from base files**: Base queries are arbitrary SQL.
   Loading a `.base.yaml` file from an untrusted source could execute
   malicious SQL. Trust model: same as DuckDB itself (filesystem + data
   access). Document this.

6. **Formula name collisions**: User-defined macros could shadow built-in
   DuckDB functions. Use `_cheeksbase.` prefix for built-ins, and either
   require user macros to be prefixed or document the collision risk.

## Open Questions

1. Should bases be versioned? (immutable base snapshots with timestamps?)
2. Should base files support `!import` / `!include` for shared query fragments?
3. Should we support base-to-base joins as a first-class concept (like
   Obsidian "linked views")?
4. Export base query results as CSV/Parquet/JSON?

## References

- Obsidian Bases plugin: https://obsidian.md/plugins?id=obsidian-bases
- DuckDB CREATE VIEW: https://duckdb.org/docs/sql/statements/create_view
- DuckDB CREATE MACRO: https://duckdb.org/docs/sql/statements/create_macro
- DuckDB CREATE TABLE AS SELECT: https://duckdb.org/docs/sql/statements/create_table_as
- Cheeksbase DB layer: `cheeksbase/core/db.py` — existing `_cheeksbase` schema
