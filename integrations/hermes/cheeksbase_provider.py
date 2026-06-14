"""Cheeksbase memory provider for Hermes.

Wraps cheeksbase's `_cheeksbase.shared_memory` table as a Hermes
MemoryProvider. Auto-prefetches relevant past entries before each turn,
writes turn pairs after each turn, and exposes the cheeksbase shared-memory
tools to the agent.

Storage: $HERMES_HOME/cheeksbase.duckdb (DuckDB). Override via plugins.cheeksbase.db_path
in $HERMES_HOME/config.yaml.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

from agent.memory_provider import MemoryProvider
from hermes_cli.config import cfg_get
from tools.registry import tool_error

logger = logging.getLogger(__name__)


# ── Tool schemas (OpenAI function-calling format) ───────────────────────────

CHEEKSBASE_REMEMBER_SCHEMA = {
    "name": "cheeksbase_remember",
    "description": (
        "Store a durable memory in cheeksbase. Use for facts the user expects "
        "you to remember across sessions: preferences, project context, decisions."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "key": {"type": "string", "description": "Stable, descriptive key."},
            "value": {"type": "string", "description": "The memory content."},
            "scope": {
                "type": "string",
                "enum": ["broadcast", "topic", "targeted"],
                "default": "broadcast",
                "description": "Visibility scope for shared memory.",
            },
            "tags": {"type": "string", "description": "Comma-separated tags."},
        },
        "required": ["key", "value"],
    },
}

CHEEKSBASE_RECALL_SCHEMA = {
    "name": "cheeksbase_recall",
    "description": "Recall a specific cheeksbase memory by exact key.",
    "parameters": {
        "type": "object",
        "properties": {"key": {"type": "string"}},
        "required": ["key"],
    },
}

CHEEKSBASE_SEARCH_SCHEMA = {
    "name": "cheeksbase_search",
    "description": (
        "Search cheeksbase memories by keyword (matches keys, values, tags). "
        "Use when you need to recall something but don't know the exact key."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "query": {"type": "string"},
            "limit": {"type": "integer", "default": 10},
        },
        "required": ["query"],
    },
}

CHEEKSBASE_FORGET_SCHEMA = {
    "name": "cheeksbase_forget",
    "description": "Delete a cheeksbase memory by key.",
    "parameters": {
        "type": "object",
        "properties": {"key": {"type": "string"}},
        "required": ["key"],
    },
}


# ── Config helpers ──────────────────────────────────────────────────────────


def _load_plugin_config() -> dict:
    try:
        from hermes_constants import get_hermes_home
        config_path = get_hermes_home() / "config.yaml"
        if not config_path.exists():
            return {}
        import yaml
        with open(config_path) as f:
            all_config = yaml.safe_load(f) or {}
        return cfg_get(all_config, "plugins", "cheeksbase", default={}) or {}
    except Exception:
        return {}


def _resolve_db_path(config: dict) -> str:
    """Resolve cheeksbase DuckDB path. Defaults to $HERMES_HOME/cheeksbase.duckdb."""
    from hermes_constants import get_hermes_home
    hermes_home = str(get_hermes_home())
    raw = config.get("db_path") or os.path.join(hermes_home, "cheeksbase.duckdb")
    if isinstance(raw, str):
        raw = raw.replace("$HERMES_HOME", hermes_home).replace("${HERMES_HOME}", hermes_home)
        raw = os.path.expanduser(raw)
    return raw


# ── Provider ────────────────────────────────────────────────────────────────


class CheeksbaseMemoryProvider(MemoryProvider):
    """DuckDB-backed shared memory with embeddings, scopes, tags, expiry."""

    def __init__(self, config: dict | None = None) -> None:
        self._config = config or _load_plugin_config()
        self._db_path: str | None = None
        self._session_id: str = ""
        self._agent_identity: str = "hermes"
        self._turn_counter: int = 0
        self._prefetch_limit = int(self._config.get("prefetch_limit", 5))
        # Last-reconciled mtime per built-in target; gates on_turn_start work.
        self._mirror_mtimes: dict[str, float] = {}

    @property
    def name(self) -> str:
        return "cheeksbase"

    def is_available(self) -> bool:
        try:
            import cheeksbase  # noqa: F401
            return True
        except Exception:
            return False

    def initialize(self, session_id: str, **kwargs: Any) -> None:
        self._db_path = _resolve_db_path(self._config)
        self._session_id = session_id
        self._agent_identity = kwargs.get("agent_identity") or "hermes"
        # Ensure schema exists and reconcile the built-in memory mirror so that
        # facts removed or edited while we weren't running don't linger in
        # recall (the built-in tool never notifies us on 'remove').
        try:
            with self._open() as db:
                for target in ("memory", "user"):
                    self._reconcile_builtin_mirror(db, target)
        except Exception as e:
            logger.debug("cheeksbase initial mirror reconcile failed: %s", e)
        logger.info("cheeksbase memory ready at %s (agent=%s)", self._db_path, self._agent_identity)

    def shutdown(self) -> None:
        # Connections are opened per-call; nothing persistent to close.
        return None

    # ── Lifecycle hooks ─────────────────────────────────────────────────

    def system_prompt_block(self) -> str:
        try:
            with self._open() as db:
                row = db.query(
                    "SELECT COUNT(*) AS c FROM _cheeksbase.shared_memory"
                )[0]
            count = int(row["c"])
        except Exception:
            count = 0
        if count == 0:
            return (
                "# Cheeksbase Memory\n"
                "Active. Empty store — proactively call `cheeksbase_remember` "
                "for facts the user expects you to recall across sessions."
            )
        return (
            f"# Cheeksbase Memory\n"
            f"Active. {count} entries with semantic-aware keyword search. "
            f"Call `cheeksbase_search` for fuzzy lookup or `cheeksbase_recall` for exact key."
        )

    def prefetch(self, query: str, *, session_id: str = "") -> str:
        if not query or not self._db_path:
            return ""
        try:
            with self._open() as db:
                results = self._tokenized_search(db, query, self._prefetch_limit)
            if not results:
                return ""
            lines = ["## Cheeksbase Memory"]
            for r in results:
                kind = r.get("kind") or "durable"
                key = r.get("key") or ""
                value = r.get("value") or ""
                if isinstance(value, str) and len(value) > 220:
                    value = value[:217] + "..."
                # Mirror rows carry a synthetic hermes:builtin:* key (an internal
                # content hash) with no recall value — surface just the fact.
                # Durable rows have a human-chosen key worth showing as a label.
                if kind == "mirror" or not key:
                    lines.append(f"- {value}")
                else:
                    lines.append(f"- **{key}**: {value}")
            return "\n".join(lines)
        except Exception as e:
            logger.debug("cheeksbase prefetch failed: %s", e)
            return ""

    _STOP_WORDS = frozenset({
        "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
        "have", "has", "had", "do", "does", "did", "will", "would", "could",
        "should", "may", "might", "must", "shall", "can", "i", "you", "he",
        "she", "it", "we", "they", "me", "him", "her", "us", "them", "my",
        "your", "his", "its", "our", "their", "this", "that", "these", "those",
        "and", "or", "but", "if", "when", "where", "how", "what", "who",
        "which", "why", "of", "in", "on", "at", "to", "for", "with", "as",
        "from", "by", "about", "user", "agent", "hermes",
    })

    @classmethod
    def _tokenize(cls, query: str) -> list[str]:
        import re
        tokens = re.findall(r"[a-zA-Z][a-zA-Z0-9_-]{2,}", query.lower())
        return [t for t in tokens if t not in cls._STOP_WORDS]

    # Auto-recall pulls only durable facts (and durable facts mirrored from the
    # built-in MEMORY.md / USER.md store). Per-turn session transcript fragments
    # (kind='turn') are deliberately excluded — they are recency-dense and would
    # otherwise crowd out durable memory in prefetch. They remain reachable via
    # the explicit `cheeksbase_search` tool.
    _RECALL_KINDS = ("durable", "mirror")

    def _tokenized_search(self, db, query: str, limit: int) -> list[dict[str, Any]]:
        # Try the full-string match first — exact phrase wins.
        seen: dict[str, dict[str, Any]] = {}
        for r in db.shared_search(query=query, limit=limit, kinds=self._RECALL_KINDS):
            seen.setdefault(str(r.get("id") or r.get("key")), r)
        # Then per-token searches to catch reworded queries.
        for token in self._tokenize(query):
            if len(seen) >= limit:
                break
            for r in db.shared_search(query=token, limit=limit, kinds=self._RECALL_KINDS):
                seen.setdefault(str(r.get("id") or r.get("key")), r)
                if len(seen) >= limit:
                    break
        return list(seen.values())[:limit]

    def sync_turn(
        self,
        user_content: str,
        assistant_content: str,
        *,
        session_id: str = "",
    ) -> None:
        if not user_content or not self._db_path:
            return
        sid = session_id or self._session_id or "session"
        self._turn_counter += 1
        # Compact the turn into one entry; keep the user message as the seed.
        snippet_user = self._truncate(user_content, 800)
        snippet_assistant = self._truncate(assistant_content, 600)
        body = f"USER: {snippet_user}\n\nASSISTANT: {snippet_assistant}"
        key = f"hermes:{sid}:turn:{self._turn_counter}"
        try:
            with self._open() as db:
                db.shared_remember(
                    source_agent=self._agent_identity,
                    key=key,
                    value=body,
                    scope="broadcast",
                    kind="turn",
                    tags=f"hermes,turn,session:{sid}",
                    expires_at=self._turn_expiry(),
                )
        except Exception as e:
            logger.debug("cheeksbase sync_turn failed: %s", e)

    def _turn_expiry(self) -> str | None:
        """TTL for per-turn session fragments so they auto-GC.

        Returns an SQL TIMESTAMP literal ``turn_ttl_days`` in the future, or
        None to disable expiry (set ``plugins.cheeksbase.turn_ttl_days: 0``).
        """
        try:
            days = int(self._config.get("turn_ttl_days", 14))
        except (TypeError, ValueError):
            days = 14
        if days <= 0:
            return None
        from datetime import datetime, timedelta
        return (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")

    def get_tool_schemas(self) -> list[dict[str, Any]]:
        return [
            CHEEKSBASE_REMEMBER_SCHEMA,
            CHEEKSBASE_RECALL_SCHEMA,
            CHEEKSBASE_SEARCH_SCHEMA,
            CHEEKSBASE_FORGET_SCHEMA,
        ]

    def handle_tool_call(self, tool_name: str, args: dict[str, Any], **kwargs: Any) -> str:
        try:
            with self._open() as db:
                if tool_name == "cheeksbase_remember":
                    key = args["key"]
                    value = args["value"]
                    res = db.shared_remember(
                        source_agent=self._agent_identity,
                        key=key,
                        value=value,
                        scope=args.get("scope", "broadcast"),
                        tags=args.get("tags"),
                    )
                    return json.dumps({"ok": True, "result": res}, default=str)
                if tool_name == "cheeksbase_recall":
                    res = db.shared_recall(args["key"])
                    if res is None:
                        return json.dumps({"error": f"No entry for key: {args['key']}"})
                    return json.dumps(res, default=str)
                if tool_name == "cheeksbase_search":
                    res = db.shared_search(
                        query=args["query"], limit=int(args.get("limit", 10))
                    )
                    return json.dumps({"results": res, "count": len(res)}, default=str)
                if tool_name == "cheeksbase_forget":
                    existing = db.shared_recall(args["key"])
                    if existing is None:
                        return json.dumps({"error": f"No entry for key: {args['key']}"})
                    db.shared_forget(args["key"])
                    return json.dumps({"ok": True, "key": args["key"]})
        except KeyError as exc:
            return tool_error(f"Missing required argument: {exc}")
        except Exception as exc:
            return tool_error(str(exc))
        return tool_error(f"Unknown tool: {tool_name}")

    def on_memory_write(
        self,
        action: str,
        target: str,
        content: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Reconcile mirrored built-in memory after a MEMORY.md / USER.md write.

        The built-in memory tool only notifies us on ``add``/``replace`` (never
        ``remove``) and does not pass the prior text on ``replace`` — so a
        targeted single-row update is impossible. Instead we rebuild the mirror
        set for *target* from the current on-disk built-in store: drop the old
        mirror rows and re-insert the current entries. This keeps mirror rows in
        sync (no stale or duplicate entries); ``remove`` is caught by the same
        reconcile run once at session start (see ``initialize``).
        """
        if not self._db_path or target not in ("memory", "user"):
            return
        if action not in ("add", "replace"):
            return
        try:
            with self._open() as db:
                self._reconcile_builtin_mirror(db, target)
        except Exception as e:
            logger.debug("cheeksbase mirror reconcile failed: %s", e)

    def on_turn_start(self, turn_number: int, message: str, **kwargs: Any) -> None:
        """Catch built-in memory edits we aren't notified about (notably 'remove').

        run_agent only bridges add/replace to ``on_memory_write`` and never
        passes the prior text, so a removed or externally-edited MEMORY.md /
        USER.md entry would otherwise linger in recall. This runs each turn
        BEFORE prefetch, but only reconciles a target whose source-file mtime
        changed since the last reconcile — so the steady-state cost is two
        stat() calls and no DB open.
        """
        if not self._db_path:
            return
        changed = [
            t for t in ("memory", "user")
            if self._mirror_source_mtime(t) != self._mirror_mtimes.get(t)
        ]
        if not changed:
            return
        try:
            with self._open() as db:
                for target in changed:
                    self._reconcile_builtin_mirror(db, target)
        except Exception as e:
            logger.debug("cheeksbase turn-start mirror reconcile failed: %s", e)

    def _reconcile_builtin_mirror(self, db: Any, target: str) -> None:
        """Make the ``mirror`` rows for *target* equal the current built-in store.

        Reads the live MEMORY.md / USER.md entries via the built-in MemoryStore
        (reusing its parser), clears the existing mirror rows for *target*, and
        re-inserts the current entries with stable content-hashed keys. Records
        the source-file mtime so ``on_turn_start`` can skip unchanged targets.
        """
        import hashlib
        try:
            from tools.memory_tool import MemoryStore
        except Exception:
            return  # built-in memory not available; nothing to mirror
        # Capture mtime up front so a write that races this reconcile is
        # re-detected next turn rather than missed.
        mtime = self._mirror_source_mtime(target)
        store = MemoryStore()
        store.load_from_disk()
        entries = store.user_entries if target == "user" else store.memory_entries
        scope = "targeted" if target == "user" else "broadcast"
        prefix = f"hermes:builtin:{target}:"
        db.shared_forget_prefix(prefix, kind="mirror")
        for entry in entries:
            if not entry:
                continue
            digest = hashlib.sha1(entry.encode("utf-8")).hexdigest()[:12]
            db.shared_remember(
                source_agent=self._agent_identity,
                key=f"{prefix}{digest}",
                value=self._truncate(entry, 1500),
                scope=scope,
                kind="mirror",
                tags=f"hermes,builtin,{target}",
            )
        self._mirror_mtimes[target] = mtime

    @staticmethod
    def _mirror_source_filename(target: str) -> str:
        return "USER.md" if target == "user" else "MEMORY.md"

    def _mirror_source_mtime(self, target: str) -> float:
        """mtime of the built-in source file for *target*, or 0.0 if absent."""
        try:
            from tools.memory_tool import get_memory_dir
            path = get_memory_dir() / self._mirror_source_filename(target)
            return path.stat().st_mtime if path.exists() else 0.0
        except Exception:
            return 0.0

    # ── Setup wizard config schema ──────────────────────────────────────

    def get_config_schema(self) -> list[dict[str, Any]]:
        from hermes_constants import display_hermes_home
        return [
            {
                "key": "db_path",
                "description": "Cheeksbase DuckDB path",
                "default": f"{display_hermes_home()}/cheeksbase.duckdb",
            },
            {
                "key": "prefetch_limit",
                "description": "Max memories to prefetch before each turn",
                "default": "5",
            },
        ]

    def save_config(self, values: dict[str, Any], hermes_home: str) -> None:
        config_path = Path(hermes_home) / "config.yaml"
        try:
            import yaml
            existing: dict[str, Any] = {}
            if config_path.exists():
                with open(config_path) as f:
                    existing = yaml.safe_load(f) or {}
            existing.setdefault("plugins", {})
            existing["plugins"]["cheeksbase"] = values
            with open(config_path, "w") as f:
                yaml.dump(existing, f, default_flow_style=False)
        except Exception as e:
            logger.debug("cheeksbase save_config failed: %s", e)

    # ── Internals ───────────────────────────────────────────────────────

    def _open(self):
        try:
            from cheeksbase.core.db import CheeksbaseDB
        except ImportError:
            # Fallback: add the cheeksbase source directory to sys.path
            # so the plugin works even if cheeksbase isn't in site-packages.
            import sys
            _cb_candidates = [
                Path.home() / "cheeksbase",
                Path(__file__).parent / ".." / ".." / ".." / "cheeksbase",
            ]
            for _p in _cb_candidates:
                _rp = _p.resolve()
                if (_rp / "cheeksbase" / "core" / "db.py").exists():
                    sys.path.insert(0, str(_rp))
                    break
            from cheeksbase.core.db import CheeksbaseDB
        return CheeksbaseDB(self._db_path)

    @staticmethod
    def _truncate(s: str, limit: int) -> str:
        if not isinstance(s, str):
            return ""
        if len(s) <= limit:
            return s
        return s[: limit - 3] + "..."



def register(ctx) -> None:
    """Plugin entry point."""
    config = _load_plugin_config()
    ctx.register_memory_provider(CheeksbaseMemoryProvider(config=config))