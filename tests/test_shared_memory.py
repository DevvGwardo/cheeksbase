"""Tests for the shared memory feature in CheeksbaseDB."""

import os
import tempfile

import pytest

from cheeksbase.core.db import CheeksbaseDB


@pytest.fixture
def temp_db():
    """Create a temporary CheeksbaseDB with shared_memory table."""
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "test.duckdb")
    db = CheeksbaseDB(db_path=db_path)
    yield db
    db.close()
    import shutil
    shutil.rmtree(tmpdir, ignore_errors=True)


class TestSharedMemoryRemember:
    """Tests for shared_remember (insert and upsert)."""

    def test_remember_basic(self, temp_db):
        result = temp_db.shared_remember("agent1", "test_key", "hello world")
        assert result["key"] == "test_key"
        assert result["value"] == "hello world"
        assert result["source_agent"] == "agent1"
        assert result["scope"] == "broadcast"

    def test_remember_with_scope_and_tags(self, temp_db):
        result = temp_db.shared_remember(
            "agent1", "tagged_key", "some value",
            scope="broadcast", tags="deploy,production"
        )
        assert result["tags"] == "deploy,production"

    def test_remember_upsert(self, temp_db):
        temp_db.shared_remember("agent1", "dup_key", "original")
        result = temp_db.shared_remember("agent2", "dup_key", "updated")
        assert result["value"] == "updated"
        assert result["source_agent"] == "agent2"

    def test_remember_colon_key(self, temp_db):
        result = temp_db.shared_remember("hermes", "hermes:deploy_info", "v2.3")
        assert result["key"] == "hermes:deploy_info"
        recalled = temp_db.shared_recall("hermes:deploy_info")
        assert recalled["value"] == "v2.3"

    def test_remember_with_expiry(self, temp_db):
        result = temp_db.shared_remember(
            "agent1", "expiring", "will expire",
            expires_at="2099-12-31T23:59:59"
        )
        assert result["expires_at"] is not None


class TestSharedMemoryRecall:
    """Tests for shared_recall and shared_recall_all."""

    def test_recall_existing(self, temp_db):
        temp_db.shared_remember("agent1", "key1", "value1")
        result = temp_db.shared_recall("key1")
        assert result is not None
        assert result["value"] == "value1"

    def test_recall_missing(self, temp_db):
        result = temp_db.shared_recall("nonexistent")
        assert result is None

    def test_recall_all(self, temp_db):
        temp_db.shared_remember("agent1", "k1", "v1")
        temp_db.shared_remember("agent2", "k2", "v2")
        results = temp_db.shared_recall_all()
        assert len(results) == 2

    def test_recall_all_filtered(self, temp_db):
        temp_db.shared_remember("agent1", "k1", "v1")
        temp_db.shared_remember("agent2", "k2", "v2")
        results = temp_db.shared_recall_all(source_agent="agent1")
        assert len(results) == 1
        assert results[0]["source_agent"] == "agent1"

    def test_recall_all_no_match(self, temp_db):
        temp_db.shared_remember("agent1", "k1", "v1")
        results = temp_db.shared_recall_all(source_agent="nonexistent")
        assert len(results) == 0


class TestSharedMemoryForget:
    """Tests for shared_forget."""

    def test_forget_existing(self, temp_db):
        temp_db.shared_remember("agent1", "to_delete", "gone soon")
        result = temp_db.shared_forget("to_delete")
        assert result is True
        assert temp_db.shared_recall("to_delete") is None

    def test_forget_missing(self, temp_db):
        # shared_forget always returns True (no error on missing)
        result = temp_db.shared_forget("nonexistent")
        assert result is True


class TestSharedMemorySearch:
    """Tests for shared_search (keyword matching)."""

    def test_search_by_key(self, temp_db):
        temp_db.shared_remember("agent1", "deploy_prod", "deployment info")
        results = temp_db.shared_search("deploy")
        assert len(results) >= 1

    def test_search_by_value(self, temp_db):
        temp_db.shared_remember("agent1", "some_key", "production deployment")
        results = temp_db.shared_search("production")
        assert len(results) >= 1

    def test_search_by_tags(self, temp_db):
        temp_db.shared_remember(
            "agent1", "tagged", "value", tags="deploy,production"
        )
        results = temp_db.shared_search("production")
        assert len(results) >= 1

    def test_search_no_match(self, temp_db):
        temp_db.shared_remember("agent1", "key1", "value1")
        results = temp_db.shared_search("zzz_nonexistent")
        assert len(results) == 0


class TestSharedMemoryEmbedding:
    """Tests for store_shared_embedding."""

    def test_store_embedding(self, temp_db):
        temp_db.shared_remember("agent1", "emb_key", "embedded value")
        result = temp_db.store_shared_embedding("emb_key", [0.1, 0.2, 0.3])
        assert result is True
        recalled = temp_db.shared_recall("emb_key")
        assert recalled["embedding"] is not None


class TestSharedMemoryExpiry:
    """Tests for shared_cleanup_expired."""

    def test_cleanup_expired(self, temp_db):
        temp_db.shared_remember("agent1", "keep", "stays")
        temp_db.shared_remember(
            "agent1", "gone", "expires",
            expires_at="2000-01-01T00:00:00"
        )
        count = temp_db.shared_cleanup_expired()
        assert count >= 1
        assert temp_db.shared_recall("keep") is not None
        assert temp_db.shared_recall("gone") is None

    def test_cleanup_no_expired(self, temp_db):
        temp_db.shared_remember("agent1", "permanent", "stays")
        count = temp_db.shared_cleanup_expired()
        assert count == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
