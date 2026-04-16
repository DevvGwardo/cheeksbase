"""Mutation engine — agent-driven write-backs with preview + confirm flow."""

from cheeksbase.mutations.engine import MutationEngine
from cheeksbase.mutations.preview import generate_preview, validate_mutation
from cheeksbase.mutations.executor import execute_mutation

__all__ = [
    "MutationEngine",
    "generate_preview",
    "validate_mutation",
    "execute_mutation",
]
