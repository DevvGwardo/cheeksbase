# AGENTS.md

This document defines how AI agents collaborate on the Cheeksbase project.

## Overview

Cheeksbase is an agent-first data platform. AI agents are first-class contributors to this project — they write code, review PRs, fix bugs, and improve documentation.

## Agent Identity

When contributing as an agent:
- Use a consistent identity (name + model) in commits and PRs
- Sign commits with `Agent: <name> (<model>)` in the commit body
- Reference your session/task ID when relevant

## Repository Structure

```
cheeksbase/
├── cheeksbase/           # Main Python package
│   ├── core/            # Core engine (DB, query, sync, config)
│   ├── connectors/      # YAML connector registry + configs
│   ├── mcp/             # MCP server for agent integration
│   ├── mutations/       # Write-back mutation engine
│   └── agents/          # Semantic annotation agents
├── tests/               # Test suite
├── docs/                # Documentation
└── examples/            # Example configurations
```

## Development Workflow

### 1. Understand the Task

Before writing code:
- Read relevant existing code
- Check `CONTRIBUTING.md` for conventions
- Review open issues or PR comments
- Ask for clarification if the task is ambiguous

### 2. Create a Branch

```bash
git checkout -b agent/<task-description>
# Example: agent/add-github-connector
```

### 3. Write Code

Follow these principles:
- **YAML-first connectors**: No Python code for new data sources
- **Type hints**: All functions must have type annotations
- **Docstrings**: All public functions need docstrings
- **Small functions**: Keep functions focused and testable
- **Error handling**: Graceful degradation, informative errors

### 4. Write Tests

Every change needs tests:
```bash
pytest tests/ -v
```

Test categories:
- `test_core.py` — Core engine functionality
- `test_connectors.py` — Connector loading and validation
- `test_query.py` — Query engine edge cases
- `test_sync.py` — Sync engine behavior

### 5. Commit

Use conventional commits:
```
feat: add GitHub connector
fix: handle empty API responses
docs: update connector guide
test: add sync engine tests
refactor: simplify query caching
```

Include agent attribution:
```
feat: add GitHub connector

Syncs repos, issues, pull requests from GitHub API.

Agent: Claude (claude-sonnet-4-20250514)
Task: #42
```

### 6. Create PR

PR title: `[Agent] <description>`

PR body must include:
- What changed and why
- Test results
- Any risks or open questions
- Agent identity and model

## Adding Connectors

Connectors are YAML-only. No Python code needed.

### Create a connector config:

```yaml
# cheeksbase/connectors/configs/myapi.yaml
name: myapi
type: rest_api
description: My API description
base_url: https://api.example.com/v1
auth:
  type: bearer
  token_field: api_key
resources:
  - name: items
    endpoint: /items
    primary_key: id
    description: List of items
```

### Test the connector:

```bash
cheeksbase add myapi --api-key test123
cheeksbase sync myapi
cheeksbase query "SELECT * FROM myapi.items LIMIT 10"
```

### Submit:

1. Add the YAML file
2. Add tests in `tests/test_connector_myapi.py`
3. Update README if it's a notable connector
4. Create PR with `[Agent] Add myapi connector`

## Code Review

### As a reviewer agent:

1. Check correctness
2. Verify tests pass
3. Look for edge cases
4. Ensure type hints are complete
5. Validate YAML connector schema
6. Check error handling
7. Review security (no hardcoded secrets)

### Review checklist:

- [ ] Code follows project style
- [ ] Tests cover new functionality
- [ ] Documentation updated if needed
- [ ] No breaking changes (or properly documented)
- [ ] Error messages are helpful
- [ ] Performance considered for large datasets

## Agent Collaboration Patterns

### Parallel Work

For large features, split work across agents:
- Agent 1: Core engine changes
- Agent 2: Connector configs
- Agent 3: Tests
- Agent 4: Documentation

Coordinate via PR comments or shared task list.

### Handoffs

When handing off work:
1. Summarize what's done
2. List what's left
3. Note any blockers
4. Update the task/issue

### Conflict Resolution

If agents disagree on approach:
1. Document both approaches
2. List trade-offs
3. Tag a human maintainer for decision
4. Implement the chosen approach

## Tooling

### Available Tools

- `cheeksbase` CLI — Run queries, sync data, manage connectors
- `pytest` — Run test suite
- `ruff` — Linting (if configured)
- `mypy` — Type checking (if configured)

### Local Development

```bash
# Install in dev mode
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Run specific test
pytest tests/test_core.py::test_query_engine -v

# Type check
mypy cheeksbase/

# Lint
ruff check cheeksbase/
```

## MCP Integration

Cheeksbase exposes an MCP server for agents:

```python
# Tools available to agents:
- query(sql, max_rows)      # Execute SQL
- describe(table)           # Get table schema
- sync(connector)           # Refresh data
- annotate(target, key, value)  # Add metadata
- chain(calls)              # Chain multiple calls
```

Agents can use these tools to interact with user data.

## Escalation

When stuck:
1. Check existing issues/PRs for similar problems
2. Search codebase for patterns
3. Ask in PR comments
4. Tag maintainer if truly blocked

## Security

- Never commit secrets
- Use environment variables for credentials
- Validate all user input
- Sanitize SQL queries
- Report security issues privately

## Questions?

Open an issue with the `[Agent Question]` tag.