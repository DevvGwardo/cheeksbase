"""Prompts and few-shot examples for LLM-based annotation.

These are reusable building blocks for an optional LLM-powered enrichment
pass on top of the heuristic detectors. They are exported as plain strings
so a caller can assemble them into a chat/completion payload in whatever
shape their provider requires.
"""

from __future__ import annotations

SYSTEM_PROMPT_TABLE_DESCRIPTION = """\
You are a database semantic-annotation assistant. Given a table name and its
column list, produce a concise one- or two-sentence description of what the
table represents. Be literal and factual — describe the data, not the
business domain. Do not speculate about use cases. Output only the
description, with no preamble or quotes.
"""


SYSTEM_PROMPT_COLUMN_DESCRIPTION = """\
You are a database semantic-annotation assistant. Given a column name and
the table it belongs to (plus peer column names for context), produce a
short description of what the column holds. One sentence. No preamble.
"""


SYSTEM_PROMPT_RELATIONSHIP = """\
You are a database semantic-annotation assistant. Given two tables and a
candidate foreign-key link between them, decide whether the link is a real
relationship or a false positive. Answer with a JSON object:
{"is_relationship": bool, "cardinality": "one_to_one"|"one_to_many"|"many_to_many", "confidence": 0.0-1.0}.
"""


# Few-shot examples — each tuple is (user_message, assistant_response).
TABLE_DESCRIPTION_EXAMPLES: list[tuple[str, str]] = [
    (
        "Table: users\nColumns: id, email, created_at, updated_at",
        "Registered user accounts, keyed by id, with contact email and audit timestamps.",
    ),
    (
        "Table: orders\nColumns: id, user_id, total_cents, currency, status, placed_at",
        "Customer orders with total amount, currency, lifecycle status, and placement time.",
    ),
    (
        "Table: stripe_charges\nColumns: id, amount, customer_id, card_last4, succeeded, created",
        "Stripe charge records linked to a customer, with amount, card suffix, success flag, and creation timestamp.",
    ),
]


COLUMN_DESCRIPTION_EXAMPLES: list[tuple[str, str]] = [
    (
        "Table: users, Column: email, Peers: id, name, created_at",
        "User's primary email address.",
    ),
    (
        "Table: orders, Column: user_id, Peers: id, total_cents, status",
        "Foreign key referencing the user who placed the order.",
    ),
    (
        "Table: events, Column: payload, Peers: id, event_type, created_at",
        "JSON payload describing the event's details.",
    ),
]


RELATIONSHIP_EXAMPLES: list[tuple[str, str]] = [
    (
        "From: orders.user_id → To: users.id",
        '{"is_relationship": true, "cardinality": "one_to_many", "confidence": 0.98}',
    ),
    (
        "From: events.tenant_id → To: tenants.id",
        '{"is_relationship": true, "cardinality": "one_to_many", "confidence": 0.95}',
    ),
    (
        "From: logs.level_id → To: users.id",
        '{"is_relationship": false, "cardinality": "one_to_many", "confidence": 0.1}',
    ),
]


def format_table_prompt(table_name: str, columns: list[str]) -> str:
    """Format the user message for table description annotation."""
    return f"Table: {table_name}\nColumns: {', '.join(columns)}"


def format_column_prompt(table_name: str, column: str, peers: list[str]) -> str:
    """Format the user message for column description annotation."""
    peer_str = ", ".join(c for c in peers if c != column)
    return f"Table: {table_name}, Column: {column}, Peers: {peer_str}"


def format_relationship_prompt(
    from_table: str, from_column: str, to_table: str, to_column: str,
) -> str:
    """Format the user message for relationship verification."""
    return f"From: {from_table}.{from_column} → To: {to_table}.{to_column}"
