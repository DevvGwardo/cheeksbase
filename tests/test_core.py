"""Tests for DataForge core functionality."""

import pytest
from pathlib import Path
import tempfile
import shutil

from dataforge.core.config import init_dataforge, get_db_path, add_connector, get_connectors
from dataforge.core.db import DataForgeDB
from dataforge.core.query import QueryEngine


@pytest.fixture
def temp_dataforge_dir():
    """Create a temporary DataForge directory for testing."""
    temp_dir = tempfile.mkdtemp()
    original_dir = Path.home() / ".dataforge"
    
    # Monkey patch the default directory
    import dataforge.core.config
    original_default = dataforge.core.config.DEFAULT_DIR
    dataforge.core.config.DEFAULT_DIR = Path(temp_dir)
    
    yield Path(temp_dir)
    
    # Restore original
    dataforge.core.config.DEFAULT_DIR = original_default
    shutil.rmtree(temp_dir, ignore_errors=True)


def test_init_dataforge(temp_dataforge_dir):
    """Test DataForge initialization."""
    ddir = init_dataforge()
    assert ddir.exists()
    assert (ddir / "config.yaml").exists()
    assert (ddir / "connectors").exists()
    assert (ddir / "cache").exists()


def test_database_creation(temp_dataforge_dir):
    """Test database creation and metadata tables."""
    init_dataforge()
    
    with DataForgeDB() as db:
        # Check that metadata schema exists
        schemas = db.get_schemas()
        assert "_dataforge" in schemas
        
        # Check that metadata tables exist
        tables = db.get_tables("_dataforge")
        expected_tables = ["sync_log", "tables", "columns", "live_rows", "mutations", "relationships", "metadata"]
        for table in expected_tables:
            assert table in tables


def test_query_engine(temp_dataforge_dir):
    """Test query engine functionality."""
    init_dataforge()
    
    with DataForgeDB() as db:
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


def test_connectors_config(temp_dataforge_dir):
    """Test connector configuration."""
    init_dataforge()
    
    # Add a connector
    add_connector("test_connector", "rest_api", {"api_key": "test123"})
    
    # Get connectors
    connectors = get_connectors()
    assert "test_connector" in connectors
    assert connectors["test_connector"]["type"] == "rest_api"
    assert connectors["test_connector"]["credentials"]["api_key"] == "test123"


if __name__ == "__main__":
    pytest.main([__file__])