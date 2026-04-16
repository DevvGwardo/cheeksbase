# 🔨 Cheeksbase

**Agent-first data platform with YAML-only connectors.**

Cheeksbase syncs data from APIs, databases, and files into a unified SQL database (DuckDB), making it easy for AI agents to query and write back to your data sources.

---

## 🎯 Best Use Cases

| Use Case | What You Get |
|----------|-------------|
| **"Which customers churned last quarter with declining usage AND open support tickets?"** | Cross-connector SQL joins across Stripe, Zendesk, and your analytics DB — one query, one result set |
| **"Update all HubSpot deals over $10k to priority status"** | SQL mutation with preview/confirm flow — see exactly what changes before pushing to HubSpot API |
| **"Show me a unified view of revenue, support tickets, and product usage"** | Sync Stripe + Zendesk + Postgres into DuckDB, query across all three with standard SQL |
| **"Let my AI agent access all my business data safely"** | MCP server exposes `query`, `describe`, `sync`, `annotate` tools — agents get SQL access without raw API keys |
| **"I need to join GitHub issues with Linear tasks and Jira tickets"** | YAML-only connectors for each — no Python code, just config files |
| **"Analyze CSV exports alongside live API data"** | File connectors read at query time (no sync), join with synced API data seamlessly |
| **"Auto-annotate my data with PII flags and descriptions"** | Semantic agent runs after each sync — detects relationships, flags sensitive columns, generates docs |

### Why Not Just Use API Tool Calls?

| Problem with Tool Calls | Cheeksbase Solution |
|------------------------|---------------------|
| Can't JOIN across APIs | Unified SQL database |
| Paginated JSON fills context windows | Single result set with LIMIT/OFFSET |
| No semantic context for field values | Auto-annotations with descriptions |
| 35% accuracy on cross-source questions | 91% accuracy (per benchmarks) |
| 16-22x more expensive per correct answer | Single DuckDB query |

---

## 📋 Quick Start

```bash
# Install
pip install cheeksbase

# Initialize
cheeksbase init

# Add a connector
cheeksbase connector add stripe --api-key sk_test_...

# Sync data
cheeksbase sync stripe

# Query
cheeksbase query "SELECT * FROM stripe.customers LIMIT 10"

# Start MCP server
cheeksbase serve
```

---

## 🚀 Copy-Paste For Your Agent

Copy this prompt and paste it to your AI agent (Claude, GPT, Cursor, etc.) to get started:

```
Look at this repo: https://github.com/DevvGwardo/cheeksbase

Cheeksbase is an agent-first data platform that syncs data from APIs, databases, and files into a unified SQL database. It's designed for AI agents to query and write back to data sources.

I want you to:
1. Clone and install it: git clone https://github.com/DevvGwardo/cheeksbase && cd cheeksbase && pip install -e ".[dev]"
2. Run the tests to verify it works: pytest tests/ -v
3. Read the README and AGENTS.md to understand the architecture
4. Help me set up a connector for [YOUR_DATA_SOURCE] (e.g., Stripe, GitHub, Postgres, CSV files)
5. Show me how to query my data with SQL once it's synced

The key insight: instead of giving you 10 different API tool calls that can't JOIN data, Cheeksbase syncs everything into DuckDB so you can write one SQL query across all my business data.

Available connectors: stripe, github, hubspot, slack, postgres, csv (and 100+ more via YAML configs)

Start with: cheeksbase init && cheeksbase add stripe --api-key sk_test_... && cheeksbase sync stripe && cheeksbase query "SELECT * FROM stripe.customers LIMIT 10"
```

---

## Features

- **YAML-only connectors** - No Python code needed to add new data sources
- **Unified SQL interface** - Query all your data with DuckDB
- **Agent-first design** - Built for AI agents with MCP integration
- **Write-back mutations** - Update source systems via SQL
- **Smart caching** - Multi-layer cache for performance
- **Tool chaining** - Chain MCP tools for complex workflows

---

## How It Works

```mermaid
flowchart TB
    subgraph Sources["🌐 Data Sources"]
        direction TB
        API["☁️ REST APIs\nStripe, GitHub, HubSpot"]
        DB["🗄️ Databases\nPostgres, MySQL"]
        Files["📁 Files\nCSV, Parquet"]
        MCP_EXT["🔌 MCP Servers\nExternal tools"]
    end

    subgraph Config["⚙️ YAML Connectors"]
        direction TB
        YAML["📝 Connector Configs\nname, auth, resources"]
    end

    subgraph Engine["⚡ Cheeksbase Engine"]
        direction TB
        Sync["🔄 Sync Engine\nIncremental updates"]
        DuckDB[("🦆 DuckDB\nUnified SQL")]
        Cache["💾 Smart Cache\nL1: Memory, L2: Disk"]
        Query["🔍 Query Engine\nCross-connector joins"]
        Mutations["✏️ Mutation Engine\nPreview → Confirm"]
    end

    subgraph Agents["🤖 AI Agents"]
        direction TB
        MCP["📡 MCP Server\nquery, describe, sync"]
        Semantic["🧠 Semantic Agent\nAuto-annotations"]
        Agent["🤖 Your Agent\nClaude, GPT, etc."]
    end

    Sources -->|"Add connector"| Config
    Config -->|"Configure"| Sync
    Sync -->|"Load data"| DuckDB
    DuckDB <-->|"Cache hot data"| Cache
    Query -->|"Read"| DuckDB
    Mutations -->|"Write locally"| DuckDB
    Mutations -->|"Push changes"| Sources
    DuckDB -->|"Expose"| MCP
    MCP -->|"Tools"| Agent
    Semantic -->|"Annotate"| DuckDB

    style Sources fill:#e8f4fd,stroke:#3498db,stroke-width:2px,color:#2c3e50
    style Config fill:#fef9e7,stroke:#f39c12,stroke-width:2px,color:#2c3e50
    style Engine fill:#eafaf1,stroke:#27ae60,stroke-width:2px,color:#2c3e50
    style Agents fill:#fdedec,stroke:#e74c3c,stroke-width:2px,color:#2c3e50

    style API fill:#d4e6f1,stroke:#2980b9,color:#2c3e50
    style DB fill:#d4e6f1,stroke:#2980b9,color:#2c3e50
    style Files fill:#d4e6f1,stroke:#2980b9,color:#2c3e50
    style MCP_EXT fill:#d4e6f1,stroke:#2980b9,color:#2c3e50

    style YAML fill:#fdebd0,stroke:#e67e22,color:#2c3e50

    style Sync fill:#d5f5e3,stroke:#27ae60,color:#2c3e50
    style DuckDB fill:#abebc6,stroke:#1e8449,color:#2c3e50
    style Cache fill:#d5f5e3,stroke:#27ae60,color:#2c3e50
    style Query fill:#d5f5e3,stroke:#27ae60,color:#2c3e50
    style Mutations fill:#d5f5e3,stroke:#27ae60,color:#2c3e50

    style MCP fill:#fadbd8,stroke:#c0392b,color:#2c3e50
    style Semantic fill:#fadbd8,stroke:#c0392b,color:#2c3e50
    style Agent fill:#f5b7b1,stroke:#c0392b,color:#2c3e50
```

### Data Flow

1. **Connect** — Add data sources with YAML configs (no code needed)
2. **Sync** — Incrementally load data into DuckDB
3. **Query** — SQL across all your data with cross-connector joins
4. **Annotate** — Semantic agent adds descriptions and PII flags
5. **Mutate** — Write back to source systems via SQL with preview/confirm
6. **Integrate** — AI agents access everything via MCP tools

---

## Connectors

Cheeksbase supports multiple connector types:

### REST APIs
```yaml
# connectors/stripe.yaml
name: stripe
type: rest_api
base_url: https://api.stripe.com/v1
auth:
  type: bearer
  token_field: stripe_secret_key
resources:
  - name: customers
    endpoint: /customers
    primary_key: id
  - name: charges
    endpoint: /charges
    primary_key: id
```

### Databases
```yaml
# connectors/postgres.yaml
name: postgres
type: database
connection_string: "{{postgres_url}}"
tables:
  - name: users
    primary_key: id
  - name: orders
    primary_key: id
```

### Files
```yaml
# connectors/csv_data.yaml
name: csv_data
type: file
path: ./data/*.csv
format: csv
```

---

## MCP Integration

Cheeksbase exposes an MCP server for AI agents:

```python
# Agent can use these tools:
# - query: Execute SQL queries
# - describe: Get table schema and metadata
# - sync: Refresh data from sources
# - annotate: Add semantic annotations
# - chain: Chain multiple tool calls
```

---

## Development

```bash
git clone https://github.com/DevvGwardo/cheeksbase
cd cheeksbase
pip install -e ".[dev]"
pytest
```

---

## License

MIT