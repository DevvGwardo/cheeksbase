"""Tests for Cheeksbase core functionality."""

import shutil
import tempfile
from pathlib import Path

import pytest

from cheeksbase.core.config import add_connector, get_connectors, init_cheeksbase
from cheeksbase.core.db import CheeksbaseDB
from cheeksbase.core.query import QueryEngine


@pytest.fixture
def temp_cheeksbase_dir():
    """Create a temporary Cheeksbase directory for testing."""
    temp_dir = tempfile.mkdtemp()
    Path.home() / ".cheeksbase"

    # Monkey patch the default directory
    import os

    import cheeksbase.core.config
    original_default = cheeksbase.core.config.DEFAULT_DIR
    original_env = os.environ.pop("CHEEKSBASE_DIR", None)
    cheeksbase.core.config.DEFAULT_DIR = Path(temp_dir)

    yield Path(temp_dir)

    # Restore original
    cheeksbase.core.config.DEFAULT_DIR = original_default
    if original_env is not None:
        os.environ["CHEEKSBASE_DIR"] = original_env
    shutil.rmtree(temp_dir, ignore_errors=True)


def test_init_cheeksbase(temp_cheeksbase_dir):
    """Test Cheeksbase initialization."""
    ddir = init_cheeksbase()
    assert ddir.exists()
    assert (ddir / "config.yaml").exists()
    assert (ddir / "connectors").exists()
    assert (ddir / "cache").exists()


def test_database_creation(temp_cheeksbase_dir):
    """Test database creation and metadata tables."""
    init_cheeksbase()

    with CheeksbaseDB() as db:
        # Check that metadata schema exists
        schemas = db.get_schemas()
        assert "_cheeksbase" in schemas

        # Check that metadata tables exist
        tables = db.get_tables("_cheeksbase")
        expected_tables = ["sync_log", "tables", "columns", "live_rows", "mutations", "relationships", "metadata"]
        for table in expected_tables:
            assert table in tables


def test_query_engine(temp_cheeksbase_dir):
    """Test query engine functionality."""
    init_cheeksbase()

    with CheeksbaseDB() as db:
        # Create a test table
        db.conn.execute('CREATE SCHEMA test_schema')
        db.conn.execute('''
            CREATE TABLE test_schema.users (
                id INTEGER PRIMARY KEY,
                name VARCHAR,
                email VARCHAR
            )
        ''')
        db.conn.execute('''
            INSERT INTO test_schema.users VALUES
            (1, 'Alice', 'alice@example.com'),
            (2, 'Bob', 'bob@example.com')
        ''')

        # Test query engine
        engine = QueryEngine(db)
        result = engine.execute("SELECT * FROM test_schema.users")

        assert "error" not in result
        assert result["row_count"] == 2
        assert len(result["columns"]) == 3
        assert result["columns"] == ["id", "name", "email"]


def test_connectors_config(temp_cheeksbase_dir):
    """Test connector configuration."""
    init_cheeksbase()

    # Add a connector
    add_connector("test_connector", "stripe", {"api_key": "***"})

    # Get connectors
    connectors = get_connectors()
    assert "test_connector" in connectors
    assert connectors["test_connector"]["source"] == "stripe"
    assert connectors["test_connector"]["credentials"]["api_key"] == "***"


if __name__ == "__main__":
    pytest.main([__file__])
