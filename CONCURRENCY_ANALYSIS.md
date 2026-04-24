# Cheeksbase Concurrency & Multi-Agent Storage Analysis

## 1. Current State

### Connection Patterns
- `cheeksbase/core/db.py:249`: `duckdb.connect(self.db_path)` with **no access-mode flags**.
- CLI: creates a new `CheeksbaseDB()` per command (`with CheeksbaseDB() as db:`).
- Web app: creates a new `CheeksbaseDB()` per HTTP request.
- MCP server: **module-level singleton** `_db` opened once and held for the process lifetime.
- QueryEngine: **module-level singleton** backed by a `CheeksbaseDB` instance.
- SyncEngine / MutationEngine: receive an injected `CheeksbaseDB` and write through it.

### Multi-Agent Tables
Three tables form the coordination bus:
- `_cheeksbase.agent_runs` — agent identity & liveness
- `_cheeksbase.agent_events` — append-only event log
- `_cheeksbase.resource_claims` — lease-based resource locking

**Critical observation**: `claim_resource()` uses a read-then-write pattern across **three separate SQL statements** without an explicit transaction:
1. `UPDATE ... SET status='expired'`
2. `SELECT ... FROM active_resource_claims` (read)
3. `INSERT ... ON CONFLICT DO UPDATE` (write)

Under the current single-process usage this is fine (DuckDB serializes statements on one connection), but it is not robust if multiple writers ever exist.

### Docker / Local Split
There is no Docker configuration in the repo yet, but the codebase is clearly intended to be used by:
- Local CLI (`cheeksbase` command)
- Local web browser (`cheeksbase web`)
- MCP server (`cheeksbase mcp` or `python -m cheeksbase.mcp.server`)
- Future Docker-based agents that will need to read/write the same DB file

---

## 2. DuckDB Concurrency Model (v1.0+)

| Scenario | Supported? | Notes |
|----------|-----------|-------|
| **Single process, multiple threads on one connection** | NO | `DuckDBPyConnection` is **not thread-safe**. |
| **Single process, one connection per thread** | YES | Use a connection pool or `queue` pattern. |
| **Multiple processes reading** | YES (since 0.10) | Readers must open with `access_mode='READ_ONLY'`. |
| **Multiple processes writing** | **NO** | Only **one process** may hold the write lock. Second writer gets `IO Error: Could not set lock` or hangs. |
| **One writer + multiple readers** | YES | Readers use `READ_ONLY`; writer uses default `READ_WRITE`. File-level locking coordinates this. |
| **WAL / journal_mode like SQLite** | NO | DuckDB does not have SQLite-style WAL. It uses deterministic file locking. |

**Bottom line**: DuckDB is fundamentally a **single-writer, multi-reader** embedded database. You cannot have two Docker containers and a local CLI all writing to the same `.duckdb` file simultaneously.

---

## 3. Option Analysis

### Option A: Single MCP Server Owns All Writes

Architecture:
- One MCP server process holds the **sole write connection**.
- Local CLI, web app, and Docker agents are **read-only** clients.
- All writes (sync, agent registration, heartbeats, claims, mutations) go through MCP tools over HTTP.

**Pros**
- Respects DuckDB's single-writer constraint perfectly.
- No file-locking complexity; coordination happens at the application layer.
- Docker agents only need a network endpoint, not volume mounts.
- The MCP server already exposes `register_agent`, `heartbeat`, `claim_resource`, `sync`, `chain`, etc.

**Cons**
- The CLI can no longer do writes directly (e.g. `cheeksbase sync <connector>` would fail if the MCP server holds the lock).
- Requires the MCP server to be running for any write operation.
- Adds a network hop for every write.
- If the MCP server crashes with the connection open, the lock may persist until the OS releases it (usually on process death).

**Code changes needed**
1. `cheeksbase/core/db.py`: add `read_only: bool = False` parameter and pass `config={"access_mode": "READ_ONLY"}` to `duckdb.connect()` when true.
2. `cheeksbase/cli.py`: detect running MCP server for write commands; fall back to direct write only when server is not running.
3. `cheeksbase/web/app.py`: open all DB connections in `READ_ONLY` mode.
4. `cheeksbase/mcp/server.py`: keep write mode; document that it **must run with a single uvicorn worker**.

---

### Option B: DuckDB `access_mode='READ_ONLY'` for Direct File Access

Architecture:
- Local processes and Docker containers volume-mount the `.cheeksbase` directory.
- Exactly **one** process opens the DB read-write (e.g. a dedicated sync daemon).
- Everyone else opens `READ_ONLY`.

**Pros**
- Direct SQL performance for heavy analytical queries; no serialization overhead.
- Simple to reason about: file is the source of truth.
- Works well for read-heavy workloads (querying, browsing).

**Cons**
- Still requires a single writer process somewhere. If that process dies, no writes happen.
- Docker containers need `--volume` mounts to the host path, which is platform-specific and brittle.
- Read-only clients cannot do heartbeats, claims, or mutations locally — they must talk to the writer.
- The writer process itself must be carefully managed (systemd, supervisor, etc.).

**Code changes needed**
1. `CheeksbaseDB.__init__(..., read_only: bool = False)`.
2. `duckdb.connect(self.db_path, config={"access_mode": "READ_ONLY"})` when `read_only=True`.
3. Environment variable `CHEEKSBASE_READONLY=1` to switch mode globally.
4. All write code paths need clear error messages when opened read-only.

---

### Option C: PostgreSQL as Backend

Architecture:
- Replace DuckDB with PostgreSQL (or add a PostgreSQL backend option).
- All processes connect via TCP; true multi-writer concurrency.

**Pros**
- True multi-writer, multi-reader; solves the problem completely.
- Works naturally across Docker network boundaries.
- Mature transaction support; `claim_resource` can use a real `SELECT ... FOR UPDATE`.

**Cons**
- Loses DuckDB's key value proposition: **zero-ops embedded analytics**.
- Users now need to run and manage a Postgres instance.
- Migration of existing `.duckdb` files is non-trivial (schema + data export).
- Query dialect differences (DuckDB's `QUALIFY`, `PIVOT`, nested types, etc.).
- Dependency injection becomes a much larger refactor.

**Code changes needed**
- Abstract `CheeksbaseDB` into a backend interface.
- Implement `PostgresBackend` with `psycopg` or `asyncpg`.
- Migrate all DuckDB-specific SQL (e.g. `read_csv`, `read_parquet`, `ATTACH`) to equivalent Postgres patterns or keep DuckDB for sync and replicate to Postgres.
- This is a **large architectural migration**, not a tactical fix.

---

### Option D: File Locking or WAL Mode Strategies

Architecture:
- Implement application-level file locking (e.g. `fcntl`, `portalocker`) around `duckdb.connect()`.
- Or hope that DuckDB gains SQLite-style WAL mode.

**Pros**
- Keeps the embedded, single-file model.

**Cons**
- **DuckDB does not support WAL mode for multi-writer.** This is a fundamental storage-engine limitation, not a configuration toggle.
- Application-level locking around the *entire* database session serializes all operations and is extremely brittle across processes (stale locks on crash, NFS issues, Windows vs. POSIX differences).
- Would require a lock manager daemon anyway, which is just re-inventing Option A poorly.

**Verdict**: Not viable.

---

## 4. Recommended Approach: Hybrid A + B

**Best for this use case**: a **single writer process** (the MCP server) with **read-only direct access** for analytics clients.

### Why this fits Cheeksbase
- The project is already MCP-centric. Agents are expected to talk to the MCP server.
- The web app and CLI are primarily read-oriented (query, describe, list).
- Writes are infrequent relative to reads (syncs, heartbeats, claims).
- DuckDB's analytical performance is preserved for read-only connections.

### Architecture Diagram

```
+---------------------------------------------------+
|  Host Machine                                     |
|  +---------------------------------------------+  |
|  |  MCP Server (single uvicorn worker)         |  |
|  |  - holds sole WRITE connection to .duckdb   |  |
|  |  - exposes HTTP / MCP tools                 |  |
|  +---------------------------------------------+  |
|            ^                     ^                |
|            | HTTP                | HTTP            |
+------------|---------------------|----------------+
             |                     |
    +--------+--------+   +--------+--------+
    |  Local CLI      |   |  Docker Agents  |
    |  (read-only DB) |   |  (HTTP to MCP)  |
    +-----------------+   +-----------------+
             |
    +--------+--------+
    |  Web Browser    |
    |  (read-only DB) |
    +-----------------+
```

### Specific Code Changes Required

#### 4.1 Add read-only mode to `CheeksbaseDB`

File: `cheeksbase/core/db.py`

```python
def __init__(self, db_path: Path | str | None = None, read_only: bool = False) -> None:
    self.db_path = str(db_path or get_db_path())
    self.read_only = read_only
    self._conn: duckdb.DuckDBPyConnection | None = None

@property
def conn(self) -> duckdb.DuckDBPyConnection:
    if self._conn is None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        if self.read_only:
            self._conn = duckdb.connect(self.db_path, config={"access_mode": "READ_ONLY"})
        else:
            self._conn = duckdb.connect(self.db_path)
        self._init_metadata()
    return self._conn
```

**Add read-only guards on write methods:**

```python
def _assert_writable(self) -> None:
    if self.read_only:
        raise RuntimeError("Database opened in read-only mode. Writes must go through the MCP server.")
```

Call `_assert_writable()` at the top of:
- `log_sync_start`, `log_sync_end`
- `update_table_metadata`
- `set_table_description`, `set_metadata`
- `upsert_relationship`
- `clear_live_rows`
- `set_query_cache`, `record_query_history`, `add_query_template`
- `store_column_stats`
- `register_agent_run`, `heartbeat_agent_run`, `post_agent_event`
- `claim_resource`, `release_resource`
- Any mutation engine calls

#### 4.2 CLI write-mode detection

File: `cheeksbase/cli.py`

For commands that write (`sync`, `mutate`, agent commands), add a helper:

```python
def _get_db_for_writes() -> CheeksbaseDB:
    """Return a writable DB, or raise if the MCP server likely holds the lock."""
    db = CheeksbaseDB()
    # Optional: try a lightweight probe. If it fails, suggest using the MCP server.
    try:
        db.conn.execute("SELECT 1")
    except duckdb.IOException as e:
        if "lock" in str(e).lower():
            raise RuntimeError(
                "Database is locked by another process (likely the MCP server). "
                "Use the MCP tools for writes, or stop the MCP server first."
            ) from e
        raise
    return db
```

For read commands (`query`, `describe`, `web`), default to `read_only=True`:

```python
with CheeksbaseDB(read_only=True) as db:
    ...
```

#### 4.3 Web app uses read-only

File: `cheeksbase/web/app.py`

Change every `with CheeksbaseDB() as db:` to `with CheeksbaseDB(read_only=True) as db:`.

#### 4.4 MCP server enforces single-worker

File: `cheeksbase/mcp/server.py`

Document and enforce that the MCP server is the writer. Update `run_server`:

```python
def run_server(host: str = "localhost", port: int = 8000, workers: int = 1) -> None:
    if workers != 1:
        raise ValueError(
            "Cheeksbase MCP server must run with a single worker "
            "because DuckDB allows only one writer process per database file."
        )
    import uvicorn
    server = create_server()
    uvicorn.run(server.streamable_http_app, host=host, port=port, workers=workers)
```

Also fix a latent thread-safety issue: `_get_db()` returns a singleton connection. If uvicorn is run with threading (not the default, but possible), this is unsafe. Add a note:

```python
# NOTE: This connection is shared across all requests in a single worker.
# DuckDB connections are NOT thread-safe. Do not run with threaded workers.
```

#### 4.5 Harden `claim_resource` with a transaction

Even with a single writer, concurrent requests on the same connection in an async context could interleave. Wrap `claim_resource` in an explicit transaction:

```python
def claim_resource(self, ...) -> dict[str, Any]:
    self._assert_writable()
    self.conn.execute("BEGIN TRANSACTION")
    try:
        self.conn.execute("UPDATE ... SET status='expired' ...")
        active = self.query("SELECT ... FROM active_resource_claims WHERE resource_key = ?", [resource_key])
        if active and active[0]["claimed_by"] != run_id:
            self.conn.execute("ROLLBACK")
            return {"status": "conflict", ...}
        # ... INSERT / UPDATE ...
        self.conn.execute("COMMIT")
        return {"status": "claimed", ...}
    except Exception:
        self.conn.execute("ROLLBACK")
        raise
```

#### 4.6 Environment variable for default mode

File: `cheeksbase/core/config.py`

```python
def get_db_path() -> Path:
    return get_cheeksbase_dir() / DB_FILE

def is_db_read_only() -> bool:
    return os.environ.get("CHEEKSBASE_READONLY", "").lower() in ("1", "true", "yes")
```

Then `CheeksbaseDB()` can default `read_only=is_db_read_only()`.

#### 4.7 Docker agent guidance

Add documentation for Docker agents:

```dockerfile
# For read-only analytics (direct file access):
# docker run -v ~/.cheeksbase:/data:ro cheeksbase-agent
# env CHEEKSBASE_READONLY=1

# For read-write coordination (recommended):
# docker run -e CHEEKSBASE_MCP_URL=http://host.docker.internal:8000 cheeksbase-agent
```

---

## 5. Risk Summary

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| MCP server crashes, lock persists | Low | Medium | OS releases file locks on process death; add health-check |
| User runs CLI write while MCP is up | Medium | High | Clear error message; probe on connect |
| Uvicorn started with `workers=4` | Medium | High | Explicit `ValueError` in `run_server()` |
| Docker agent can't reach host MCP | Medium | Medium | Document `host.docker.internal`; fallback to read-only mount |
| Read-only client tries to write | Low | Low | `_assert_writable()` raises with helpful message |
| `claim_resource` race (single writer) | Low | Medium | Wrap in explicit transaction |

---

## 6. Conclusion

DuckDB's single-writer model is a hard constraint. The **cheapest, most aligned** path for Cheeksbase is to make the existing MCP server the sole writer and teach all other clients to open the database in `READ_ONLY` mode. This preserves DuckDB's analytical strengths, requires minimal code changes, and fits the project's agent-first architecture.

If true multi-writer becomes a hard requirement in the future, a migration to PostgreSQL (Option C) should be evaluated, but that is a **strategic platform decision**, not a tactical concurrency fix.
