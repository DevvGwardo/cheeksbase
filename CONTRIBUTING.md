# Contributing to Cheeksbase

Thank you for your interest in contributing to Cheeksbase! This document provides guidelines for both human and AI agent contributors.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Project Structure](#project-structure)
- [Coding Standards](#coding-standards)
- [Adding Connectors](#adding-connectors)
- [Testing](#testing)
- [Pull Request Process](#pull-request-process)
- [Agent Contributions](#agent-contributions)
- [Security](#security)

## Code of Conduct

Be respectful, constructive, and inclusive. We welcome contributions from everyone — humans and AI agents alike.

## Getting Started

1. Fork the repository
2. Clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/cheeksbase.git
   cd cheeksbase
   ```
3. Create a branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```

## Development Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode
pip install -e ".[dev]"

# Run tests to verify setup
pytest tests/ -v
```

## Project Structure

```
cheeksbase/
├── cheeksbase/
│   ├── __init__.py          # Package version
│   ├── cli.py               # CLI interface (click)
│   ├── core/
│   │   ├── config.py        # Configuration management
│   │   ├── db.py            # DuckDB wrapper + metadata
│   │   ├── query.py         # Query engine with caching
│   │   └── sync.py          # Data sync engine
│   ├── connectors/
│   │   ├── registry.py      # YAML connector loader
│   │   └── configs/         # Built-in connector YAMLs
│   ├── mcp/
│   │   └── server.py        # MCP server for agents
│   ├── mutations/           # Write-back mutation engine
│   └── agents/              # Semantic annotation agents
├── tests/
│   ├── test_core.py
│   ├── test_connectors.py
│   └── ...
├── docs/                    # Documentation
└── examples/                # Example configs
```

## Coding Standards

### Python Style

- **Python 3.10+** required
- **Type hints** on all function signatures
- **Docstrings** on all public functions (Google style)
- **Line length**: 100 characters max
- **Imports**: Grouped (stdlib, third-party, local), alphabetized

Example:
```python
"""Module docstring."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb

from cheeksbase.core.config import get_db_path


def my_function(param1: str, param2: int) -> dict[str, Any]:
    """Short description.
    
    Longer description if needed.
    
    Args:
        param1: Description of param1.
        param2: Description of param2.
        
    Returns:
        Description of return value.
        
    Raises:
        ValueError: When something is wrong.
    """
    # Implementation
    return {"result": param1}
```

### YAML Style (Connectors)

```yaml
# Use 2-space indentation
# Always include description
# Use snake_case for names

name: myapi
type: rest_api
description: Brief description of the API
base_url: https://api.example.com/v1

auth:
  type: bearer
  token_field: api_key  # Field name in credentials dict

resources:
  - name: items
    endpoint: /items
    primary_key: id
    description: What this resource represents
    
  - name: users
    endpoint: /users
    primary_key: id
    description: Users in the system
```

### Commit Messages

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `style`: Code style (formatting, semicolons, etc.)
- `refactor`: Code change that neither fixes a bug nor adds a feature
- `test`: Adding missing tests
- `chore`: Changes to build process or auxiliary tools

Examples:
```
feat(connectors): add GitHub connector
fix(query): handle NULL values in aggregation
docs(readme): update quickstart guide
test(sync): add tests for REST API sync
```

For AI agents, add attribution:
```
feat(connectors): add GitHub connector

Syncs repos, issues, PRs, and commits.

Agent: Claude (claude-sonnet-4-20250514)
Task: #42
```

## Adding Connectors

Cheeksbase uses YAML-only connectors. No Python code needed!

### Step 1: Create the YAML config

Create `cheeksbase/connectors/configs/<connector_name>.yaml`:

```yaml
name: myapi
type: rest_api
description: My API
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

### Step 2: Test locally

```bash
# Add the connector
cheeksbase add myapi --api-key YOUR_KEY

# Sync data
cheeksbase sync myapi

# Query
cheeksbase query "SELECT * FROM myapi.items LIMIT 10"
```

### Step 3: Add tests

Create `tests/test_connector_myapi.py`:

```python
import pytest
from cheeksbase.connectors.registry import get_connector_config


def test_myapi_config_exists():
    config = get_connector_config("myapi")
    assert config is not None
    assert config["name"] == "myapi"
    assert config["type"] == "rest_api"


def test_myapi_resources():
    config = get_connector_config("myapi")
    resources = config.get("resources", [])
    assert len(resources) > 0
    assert any(r["name"] == "items" for r in resources)
```

### Step 4: Submit PR

- Title: `feat(connectors): add myapi connector`
- Include test results
- Document any special auth requirements

## Testing

### Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_core.py -v

# Run with coverage
pytest tests/ --cov=cheeksbase --cov-report=html

# Run specific test
pytest tests/test_core.py::test_query_engine -v
```

### Writing Tests

- Use `pytest` fixtures for setup/teardown
- Test both success and error cases
- Use descriptive test names
- Mock external API calls

Example:
```python
import pytest
from cheeksbase.core.db import CheeksbaseDB


@pytest.fixture
def db():
    """Create a test database."""
    with CheeksbaseDB(":memory:") as database:
        yield database


def test_create_table(db):
    """Test creating a table."""
    db.conn.execute('CREATE SCHEMA test')
    db.conn.execute('CREATE TABLE test.users (id INTEGER, name VARCHAR)')
    
    tables = db.get_tables("test")
    assert "users" in tables


def test_query_empty_result(db):
    """Test querying with no results."""
    db.conn.execute('CREATE SCHEMA test')
    db.conn.execute('CREATE TABLE test.empty (id INTEGER)')
    
    result = db.query("SELECT * FROM test.empty")
    assert result == []
```

## Pull Request Process

### Before Submitting

1. Ensure all tests pass
2. Update documentation if needed
3. Add your changes to CHANGELOG (if exists)
4. Rebase on latest main

### PR Template

```markdown
## Description

Brief description of changes.

## Type of Change

- [ ] Bug fix
- [ ] New feature
- [ ] Documentation update
- [ ] Refactor
- [ ] New connector

## Testing

- [ ] Tests pass locally
- [ ] Added new tests for changes
- [ ] Manual testing performed

## Checklist

- [ ] Code follows project style
- [ ] Self-review completed
- [ ] Comments added for complex logic
- [ ] Documentation updated
- [ ] No breaking changes (or documented)

## Screenshots/Logs

If applicable.

## Agent Attribution

If contributed by AI:
- Agent: [name] ([model])
- Task: [#issue]
```

### Review Process

1. Automated checks run (tests, linting)
2. At least one maintainer review
3. Address feedback
4. Squash and merge

## Agent Contributions

We welcome AI agent contributions! See [AGENTS.md](AGENTS.md) for detailed guidelines.

Quick summary:
- Agents can submit PRs like humans
- Must include agent attribution in commits
- Follow all coding standards
- Tests are mandatory
- YAML-only for new connectors

## Security

### Reporting Vulnerabilities

**Do NOT open public issues for security vulnerabilities.**

Email: security@cheeksbase.dev (or open a private security advisory on GitHub)

### Security Guidelines

1. **Never commit secrets**
   - Use environment variables
   - Use `.env` files (gitignored)
   - Use secret managers in production

2. **Validate input**
   - Sanitize SQL queries
   - Validate YAML configs
   - Check API response formats

3. **Handle credentials securely**
   - Never log credentials
   - Use secure storage
   - Implement token refresh

4. **Dependencies**
   - Keep dependencies updated
   - Review dependency licenses
   - Audit for vulnerabilities

## Questions?

- Open an issue with the `[Question]` tag
- Join our community chat (if exists)
- Check existing issues and PRs

## License

By contributing, you agree that your contributions will be licensed under the MIT License.