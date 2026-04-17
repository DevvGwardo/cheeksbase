# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-04-16

### Added
- Initial release of Cheeksbase
- DuckDB-backed storage with metadata management
- YAML-based connector configuration (Stripe, HubSpot, GitHub, Slack, Postgres, CSV)
- CLI tool with init, add, remove, sync, query, describe commands
- Semantic annotation agent for auto-detecting PII, relationships, and descriptions
- Mutation engine with preview/confirm flow and guardrails
- REST API sync with cursor and offset pagination support
- Database sync using DuckDB ATTACH
- Mutation write-back to source APIs with JSON serialization
- MCP server for AI agent integration
- Mutation CLI commands: `mutations`, `confirm`, `reject`
- Parquet and JSON file connector configs
- CI workflow with GitHub Actions
- Documentation and examples placeholders
