"""Heuristic detectors for relationships, PII, and descriptions.

Core detection functions are pure — no database access, no side effects.
Some validation helpers (``validate_relationship``, ``detect_pii_in_values``)
accept a ``CheeksbaseDB`` handle for data-level checks. The semantic agent
orchestrates these and writes results to metadata tables.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

from cheeksbase.core.db import CheeksbaseDB

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Relationships
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Relationship:
    """A detected foreign-key relationship between two columns."""

    from_table: str
    from_column: str
    to_table: str
    to_column: str
    confidence: float
    reason: str


# Columns named exactly one of these are treated as the referenced table's
# primary key when resolving `<entity>_id` references.
_PK_NAMES = ("id", "uuid", "pk")

# Suffixes that hint at a foreign-key column. Order matters: longest first so
# `_uuid` matches before `_id` for columns like `user_uuid`.
_FK_SUFFIXES = ("_uuid", "_id")


def _singularize(name: str) -> str:
    """Very small English singularizer — enough for table-name matching."""
    if name.endswith("ies") and len(name) > 3:
        return name[:-3] + "y"
    if name.endswith("sses") or name.endswith("shes") or name.endswith("ches"):
        return name[:-2]
    if name.endswith("s") and not name.endswith("ss"):
        return name[:-1]
    return name


def _pluralize(name: str) -> str:
    """Very small English pluralizer — enough for table-name matching."""
    if name.endswith("y") and len(name) > 1 and name[-2] not in "aeiou":
        return name[:-1] + "ies"
    if name.endswith(("s", "x", "z")) or name.endswith(("sh", "ch")):
        return name + "es"
    return name + "s"


def _candidate_table_names(entity: str) -> list[str]:
    """Return plausible table names for a referenced entity."""
    seen: list[str] = []
    for name in (entity, _pluralize(entity), _singularize(entity)):
        if name and name not in seen:
            seen.append(name)
    return seen


def _extract_fk_entity(column: str) -> str | None:
    """Strip known FK suffixes. `user_id` → `user`, `owner_uuid` → `owner`."""
    lower = column.lower()
    for suffix in _FK_SUFFIXES:
        if lower.endswith(suffix) and len(lower) > len(suffix):
            return lower[: -len(suffix)]
    return None


def detect_relationships(
    columns: dict[str, list[str]],
) -> list[Relationship]:
    """Detect foreign-key relationships from column names.

    Args:
        columns: Mapping of table name to its list of column names.

    Returns:
        A list of `Relationship` records. Self-references (a table's FK
        pointing back at itself) are included only when the column name is
        clearly distinct from the table's own primary key.

    """
    tables = set(columns.keys())
    relationships: list[Relationship] = []

    for table, cols in columns.items():
        for col in cols:
            entity = _extract_fk_entity(col)
            if entity is None:
                continue

            # Find a target table matching the entity (try plural/singular).
            target: str | None = None
            for candidate in _candidate_table_names(entity):
                if candidate in tables and candidate != table:
                    target = candidate
                    break
                # Case-insensitive fallback.
                for t in tables:
                    if t.lower() == candidate and t != table:
                        target = t
                        break
                if target is not None:
                    break

            if target is None:
                continue

            # Find a primary-key-ish column in the target table.
            target_cols_lower = {c.lower(): c for c in columns[target]}
            target_pk: str | None = None
            for pk_name in _PK_NAMES:
                if pk_name in target_cols_lower:
                    target_pk = target_cols_lower[pk_name]
                    break
            if target_pk is None:
                continue

            # Confidence: exact-plural match is strongest.
            if target == _pluralize(entity):
                confidence = 0.95
            elif target.lower() == entity:
                confidence = 0.9
            else:
                confidence = 0.8

            relationships.append(
                Relationship(
                    from_table=table,
                    from_column=col,
                    to_table=target,
                    to_column=target_pk,
                    confidence=confidence,
                    reason=f"{col} looks like a foreign key into {target}.{target_pk}",
                )
            )

    return relationships


# ---------------------------------------------------------------------------
# PII detection
# ---------------------------------------------------------------------------


# Ordered list of (pii_type, compiled_regex) — first match wins.
_PII_RULES: list[tuple[str, re.Pattern[str]]] = [
    ("email", re.compile(r"(^|_)(email|e_mail|email_address)($|_)", re.IGNORECASE)),
    ("phone", re.compile(r"(^|_)(phone|telephone|mobile|cell|fax)($|_|_number)", re.IGNORECASE)),
    ("ssn", re.compile(r"(^|_)(ssn|social_security(_number)?|tax_id|tin)($|_)", re.IGNORECASE)),
    ("credit_card", re.compile(r"(^|_)(credit_card|card_number|cc_number|pan)($|_)", re.IGNORECASE)),
    ("ip_address", re.compile(r"(^|_)(ip|ip_address|client_ip|remote_ip)($|_)", re.IGNORECASE)),
    ("date_of_birth", re.compile(r"(^|_)(dob|date_of_birth|birth_date|birthdate|birthday)($|_)", re.IGNORECASE)),
    ("address", re.compile(r"(^|_)(address|street|city|zip(code)?|postal_code|postcode|country)($|_)", re.IGNORECASE)),
    ("name", re.compile(r"(^|_)(first_name|last_name|full_name|given_name|family_name|middle_name|name)($|_)", re.IGNORECASE)),
    ("password", re.compile(r"(^|_)(password|passwd|pwd|secret|api_key|access_token|refresh_token)($|_)", re.IGNORECASE)),
    ("gender", re.compile(r"(^|_)(gender|sex)($|_)", re.IGNORECASE)),
]

# Very common non-PII names that would false-match "name" rule.
_NAME_FALSE_POSITIVES = {
    "table_name", "schema_name", "column_name", "connector_name",
    "field_name", "file_name", "key_name", "type_name", "event_name",
    "product_name", "company_name",
}


def detect_pii(columns: list[str]) -> dict[str, str]:
    """Classify columns as PII by name pattern.

    Returns a dict mapping column_name → pii_type (e.g. ``"email"``). Columns
    with no PII signal are omitted.
    """
    result: dict[str, str] = {}
    for col in columns:
        if col.lower() in _NAME_FALSE_POSITIVES:
            continue
        for pii_type, pattern in _PII_RULES:
            if pattern.search(col):
                result[col] = pii_type
                break
    return result


# ---------------------------------------------------------------------------
# Description generation
# ---------------------------------------------------------------------------


# Columns that universally signal what a table is about — used to refine the
# generated description.
_ROLE_HINTS: list[tuple[tuple[str, ...], str]] = [
    (("user_id", "email"), "per-user"),
    (("customer_id",), "per-customer"),
    (("order_id", "amount"), "per-order"),
    (("created_at", "updated_at"), "with audit timestamps"),
]


def _humanize(name: str) -> str:
    """`user_payments` → `user payments`."""
    return name.replace("_", " ").strip()


def generate_description(table_name: str, columns: list[str]) -> str:
    """Build a short, human-readable table description.

    Pure heuristic — it reads column names but never touches the database.
    """
    human_name = _humanize(table_name)
    col_set = {c.lower() for c in columns}

    hints: list[str] = []
    for required, label in _ROLE_HINTS:
        if all(r in col_set for r in required):
            hints.append(label)

    if hints:
        return f"{human_name.capitalize()} ({', '.join(hints)}) — {len(columns)} columns."
    return f"{human_name.capitalize()} — {len(columns)} columns."


def generate_column_description(column_name: str, pii_type: str | None = None) -> str:
    """Build a short description for a single column."""
    human = _humanize(column_name)
    if pii_type:
        return f"{human.capitalize()} (PII: {pii_type})."
    if column_name.lower() in _PK_NAMES:
        return "Primary key."
    entity = _extract_fk_entity(column_name)
    if entity:
        return f"Foreign key reference to {entity}."
    if column_name.lower().endswith("_at"):
        return f"Timestamp: {human}."
    return f"{human.capitalize()}."


# ---------------------------------------------------------------------------
# Data-level validation and PII detection
# ---------------------------------------------------------------------------


def validate_relationship(
    db: CheeksbaseDB,
    schema: str,
    rel: Relationship,
) -> dict[str, float]:
    """Validate a heuristic relationship against actual data.

    Returns dict with orphan_rate (fraction of FK values not in target)
    and fk_coverage (fraction of target PK values that are referenced).
    """
    try:
        # Orphan rate: % of FK values that don't exist in target
        orphan_result = db.query(f"""
            SELECT
                1.0 * COUNT(*) FILTER (WHERE t."{rel.from_column}" IS NOT NULL
                    AND t."{rel.from_column}" NOT IN (
                        SELECT "{rel.to_column}" FROM "{schema}"."{rel.to_table}"
                    ))
                / NULLIF(COUNT(*) FILTER (WHERE t."{rel.from_column}" IS NOT NULL), 0)
                as orphan_rate
            FROM "{schema}"."{rel.from_table}" t
        """)
        orphan_rate = orphan_result[0]["orphan_rate"] if orphan_result else 1.0

        # FK coverage: % of target PK values that are actually referenced
        coverage_result = db.query(f"""
            SELECT
                1.0 * COUNT(DISTINCT t."{rel.from_column}")
                / NULLIF((SELECT COUNT(DISTINCT "{rel.to_column}") FROM "{schema}"."{rel.to_table}"), 0)
                as fk_coverage
            FROM "{schema}"."{rel.from_table}" t
            WHERE t."{rel.from_column}" IS NOT NULL
        """)
        fk_coverage = coverage_result[0]["fk_coverage"] if coverage_result else 0.0

        return {"orphan_rate": orphan_rate, "fk_coverage": fk_coverage}
    except Exception:
        logger.debug("Failed to validate relationship %s", rel, exc_info=True)
        return {"orphan_rate": float("nan"), "fk_coverage": float("nan")}


# Patterns for value-level PII detection
_PII_VALUE_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("email", re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')),
    ("phone", re.compile(r'^[\+]?[\d\s\-\(\)]{7,15}$')),
    ("ssn", re.compile(r'^\d{3}-\d{2}-\d{4}$')),
    ("credit_card", re.compile(r'^\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}$')),
    ("ip_address", re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$')),
]


def detect_pii_in_values(
    db: CheeksbaseDB,
    schema: str,
    table: str,
    columns: list[str],
    sample_size: int = 100,
) -> dict[str, str]:
    """Sample actual data values to detect PII that column-name detection missed.

    Returns dict mapping column_name -> pii_type for columns with value-level PII.
    """
    result: dict[str, str] = {}
    try:
        # Get sample of non-null values
        sample_result = db.query(f"""
            SELECT * FROM "{schema}"."{table}"
            WHERE {" OR ".join(f'"{c}" IS NOT NULL' for c in columns)}
            LIMIT {sample_size}
        """)

        if not sample_result:
            return result

        for col in columns:
            if col.lower() in _NAME_FALSE_POSITIVES:
                continue
            # Collect non-null string values
            values = [str(row.get(col, "")) for row in sample_result if row.get(col)]
            if not values:
                continue

            # Test each value against patterns
            for pii_type, pattern in _PII_VALUE_PATTERNS:
                matches = sum(1 for v in values if pattern.match(v))
                if matches >= max(3, len(values) * 0.3):  # 30%+ match or at least 3
                    result[col] = pii_type
                    break
    except Exception:
        logger.debug("Failed to detect PII in values", exc_info=True)

    return result
