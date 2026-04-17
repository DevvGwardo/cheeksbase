"""Web UI for Cheeksbase.

A minimal read-only browser for non-technical users:
  - List connectors
  - Drill into tables
  - Preview rows with pagination

Install the optional deps with `pip install "cheeksbase[web]"` and launch
with `cheeksbase serve-web`.
"""

from cheeksbase.web.app import create_app

__all__ = ["create_app"]
