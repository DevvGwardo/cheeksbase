# Hermes memory provider

Canonical source for the Hermes [`MemoryProvider`](https://github.com/) that backs
Hermes's persistent recall with cheeksbase's `_cheeksbase.shared_memory` table.

- **This file is the source of truth.** It is deployed by copying (or symlinking)
  to `~/.hermes/plugins/cheeksbase/__init__.py`. Keep the two in sync — a bare
  `hermes update` can clobber `~/.hermes`, so the repo copy is the backup.
- It imports `agent.*`, `hermes_cli.*`, `tools.*`, `hermes_constants` from the
  Hermes runtime, so it is **not** importable or tested inside this repo (and is
  excluded from CI: `ruff`/`mypy`/`pytest` only target `cheeksbase/` and `tests/`).

## Deploy

```bash
cp integrations/hermes/cheeksbase_provider.py ~/.hermes/plugins/cheeksbase/__init__.py
# or symlink so future edits stay in sync:
# ln -sf "$PWD/integrations/hermes/cheeksbase_provider.py" ~/.hermes/plugins/cheeksbase/__init__.py
```

Then in `~/.hermes/config.yaml`:

```yaml
memory:
  provider: cheeksbase
plugins:
  cheeksbase:
    db_path: $HERMES_HOME/cheeksbase.duckdb
    prefetch_limit: 5      # max durable memories injected before each turn
    turn_ttl_days: 14      # session-turn fragments expire (and GC) after N days; 0 disables
```

## Memory `kind` partition

Entries in `shared_memory` are classified by `kind` so auto-recall stays signal,
not transcript noise:

| kind      | written by             | injected by `prefetch`? |
|-----------|------------------------|-------------------------|
| `durable` | `cheeksbase_remember`  | ✅ yes                  |
| `mirror`  | `on_memory_write` (MEMORY.md / USER.md) | ✅ yes |
| `turn`    | `sync_turn` (per-turn USER/ASSISTANT pair) | ❌ no — TTL'd; reachable only via the explicit `cheeksbase_search` tool |

`prefetch` calls `db.shared_search(..., kinds=("durable", "mirror"))`, so per-turn
session fragments can never crowd out durable facts in recall. They remain
searchable on demand (cross-agent visibility) and expire via `turn_ttl_days`.

Requires `db.shared_search`/`shared_remember` to support the `kind`/`kinds`
parameters (cheeksbase ≥ the commit that added the `kind` column).
