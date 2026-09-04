# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Unit tests for tenant SQL schema (DDL, migrations, optimistic locking),
memory scoping and the vector backend."""

from __future__ import annotations

import pytest

from trpc_agent_sdk.abc import SearchMemoryResponse
from trpc_agent_sdk.tenants import (
    InMemoryVectorBackend,
    MIGRATIONS,
    OptimisticLockError,
    SchemaMigrator,
    SqlTenantStore,
    TENANT_TABLE_NAMES,
    TENANT_TABLES_DDL_MYSQL,
    TENANT_TABLES_DDL_SQLITE,
    TenantConfig,
    TenantScopedMemoryService,
    VectorRecord,
)


def _sqlite_url(tmp_path) -> str:
    """Build a SQLAlchemy SQLite URL from a pytest tmp_path."""
    path = str(tmp_path / "tenants.db").replace("\\", "/")
    return f"sqlite:///{path}"


def _table_statement(statements, table: str) -> str:
    """Return the single CREATE TABLE statement for a table."""
    return next(s for s in statements if f"CREATE TABLE IF NOT EXISTS {table} " in s)


class TestDdl:

    def test_eight_tables_declared(self):
        assert TENANT_TABLE_NAMES == ("tenants", "sessions", "events", "messages", "memory", "summaries",
                                      "channel_bindings", "audit_logs")

    def test_sqlite_ddl_covers_all_tables(self):
        for table in TENANT_TABLE_NAMES:
            statement = _table_statement(TENANT_TABLES_DDL_SQLITE, table)
            assert statement
        # Optimistic-lock version columns on read-modify-write tables
        for table in ("tenants", "sessions", "summaries"):
            assert "version" in _table_statement(TENANT_TABLES_DDL_SQLITE, table)

    def test_mysql_ddl_covers_all_tables(self):
        for table in TENANT_TABLE_NAMES:
            assert _table_statement(TENANT_TABLES_DDL_MYSQL, table)
        assert "\n".join(TENANT_TABLES_DDL_MYSQL).count("ENGINE=InnoDB") == len(TENANT_TABLE_NAMES)

    def test_audit_logs_has_eleven_governance_fields(self):
        statement = _table_statement(TENANT_TABLES_DDL_SQLITE, "audit_logs")
        for column in ("tenant_id", "channel", "user_id", "session_id", "agent_name", "tool_name", "decision",
                       "latency_ms", "error_type", "cost_usd", "trace_id"):
            assert column in statement


class TestSchemaMigrator:

    def test_apply_creates_all_tables_and_is_idempotent(self, tmp_path):
        import sqlite3

        migrator = SchemaMigrator(_sqlite_url(tmp_path))
        applied = migrator.apply_migrations()
        assert applied == [m.version for m in MIGRATIONS]

        conn = sqlite3.connect(tmp_path / "tenants.db")
        try:
            names = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        finally:
            conn.close()
        for table in TENANT_TABLE_NAMES:
            assert table in names

        # Second run is a no-op
        assert migrator.apply_migrations() == []
        assert migrator.applied_versions() == [m.version for m in MIGRATIONS]

    def test_dry_run_applies_nothing(self, tmp_path):
        migrator = SchemaMigrator(_sqlite_url(tmp_path))
        assert migrator.apply_migrations(dry_run=True) == [m.version for m in MIGRATIONS]
        assert migrator.applied_versions() == []

    def test_checksum_drift_is_detected(self, tmp_path):
        import sqlite3

        migrator = SchemaMigrator(_sqlite_url(tmp_path))
        migrator.apply_migrations()
        assert migrator.verify_checksums() == []

        # Tamper with the recorded checksum -> drift detected
        conn = sqlite3.connect(tmp_path / "tenants.db")
        conn.execute("UPDATE schema_migrations SET checksum = 'deadbeef'")
        conn.commit()
        conn.close()
        assert migrator.verify_checksums() != []

    def test_cli_main_reports_success(self, tmp_path):
        from trpc_agent_sdk.tenants._sql_migrations import main

        assert main([_sqlite_url(tmp_path)]) == 0
        # Idempotent re-run also succeeds
        assert main([_sqlite_url(tmp_path)]) == 0


class TestSqlTenantStoreOptimisticLock:

    async def _make_store(self, tmp_path) -> SqlTenantStore:
        pytest.importorskip("aiosqlite")
        return SqlTenantStore(_sqlite_url(tmp_path))

    async def test_update_increments_version(self, tmp_path):
        store = await self._make_store(tmp_path)
        tenant = TenantConfig(tenant_id="acme", name="ACME").to_tenant()
        await store.create_tenant(tenant)

        current = await store.get_tenant("acme")
        assert current.version == 0

        current.name = "ACME Corp"
        updated = await store.update_tenant(current)
        assert updated.version == 1

        reread = await store.get_tenant("acme")
        assert reread.name == "ACME Corp"
        assert reread.version == 1

    async def test_stale_update_raises_optimistic_lock_error(self, tmp_path):
        store = await self._make_store(tmp_path)
        await store.create_tenant(TenantConfig(tenant_id="acme", name="ACME").to_tenant())

        first = await store.get_tenant("acme")
        second = await store.get_tenant("acme")

        first.name = "Writer A"
        await store.update_tenant(first)  # version 0 -> 1

        second.name = "Writer B"  # still holds version 0
        with pytest.raises(OptimisticLockError):
            await store.update_tenant(second)

        # Writer B re-reads and retries successfully
        fresh = await store.get_tenant("acme")
        assert fresh.version == 1
        fresh.name = "Writer B"
        await store.update_tenant(fresh)
        assert (await store.get_tenant("acme")).name == "Writer B"

    async def test_update_missing_tenant_raises_value_error(self, tmp_path):
        store = await self._make_store(tmp_path)
        with pytest.raises(ValueError):
            await store.update_tenant(TenantConfig(tenant_id="ghost", name="Ghost").to_tenant())


class _FakeSession:
    """Minimal session stand-in with the same copy API as the SDK Session."""

    def __init__(self, session_id: str):
        self.id = session_id

    def model_copy(self, update: dict):
        clone = _FakeSession(update.get("id", self.id))
        return clone


class TestTenantScopedMemoryService:

    class _RecordingMemoryService:
        """Records the keys it receives to verify tenant scoping."""

        def __init__(self):
            self.stored_ids = []
            self.searched_keys = []
            self.closed = False

        async def store_session(self, session, agent_context=None) -> None:
            self.stored_ids.append(session.id)

        async def search_memory(self, key, query, limit=10, agent_context=None) -> SearchMemoryResponse:
            self.searched_keys.append(key)
            return SearchMemoryResponse()

        async def close(self) -> None:
            self.closed = True

    async def test_store_and_search_are_tenant_scoped(self):
        base = self._RecordingMemoryService()
        service = TenantScopedMemoryService("acme", base)

        await service.store_session(_FakeSession("s1"))
        assert base.stored_ids == ["acme:s1"]

        await service.search_memory("s1", "hello")
        assert base.searched_keys == ["acme:s1"]

    async def test_close_delegates(self):
        base = self._RecordingMemoryService()
        service = TenantScopedMemoryService("acme", base)
        await service.close()
        assert base.closed is True

    def test_scope_key_prefixes_tenant(self):
        service = TenantScopedMemoryService("globex", self._RecordingMemoryService())
        assert service.scope_key("chat:1") == "globex:chat:1"


class TestInMemoryVectorBackend:

    def _record(self, embedding, text="", metadata=None, id_=""):
        return VectorRecord(embedding=embedding, text=text, metadata=metadata or {}, id=id_)

    def test_search_orders_by_cosine_similarity(self):
        backend = InMemoryVectorBackend()
        backend.upsert("acme", [
            self._record([1.0, 0.0], text="x-axis", id_="x"),
            self._record([0.0, 1.0], text="y-axis", id_="y"),
        ])

        results = backend.search("acme", [0.9, 0.1], top_k=2)
        assert [r.id for r in results] == ["x", "y"]
        assert results[0].score > results[1].score

    def test_top_k_limits_results(self):
        backend = InMemoryVectorBackend()
        backend.upsert("acme", [self._record([float(i), 1.0], id_=str(i)) for i in range(5)])
        assert len(backend.search("acme", [1.0, 0.0], top_k=3)) == 3

    def test_tenants_are_isolated(self):
        backend = InMemoryVectorBackend()
        backend.upsert("acme", [self._record([1.0, 0.0], id_="secret")])
        assert backend.count("acme") == 1
        assert backend.count("globex") == 0
        assert backend.search("globex", [1.0, 0.0]) == []

    def test_metadata_filter(self):
        backend = InMemoryVectorBackend()
        backend.upsert("acme", [
            self._record([1.0, 0.0], metadata={"kind": "faq"}, id_="faq1"),
            self._record([0.9, 0.1], metadata={"kind": "doc"}, id_="doc1"),
        ])
        results = backend.search("acme", [1.0, 0.0], top_k=5, metadata_filter={"kind": "doc"})
        assert [r.id for r in results] == ["doc1"]

    def test_upsert_replaces_same_id(self):
        backend = InMemoryVectorBackend()
        backend.upsert("acme", [self._record([1.0, 0.0], text="v1", id_="r1")])
        backend.upsert("acme", [self._record([0.0, 1.0], text="v2", id_="r1")])
        assert backend.count("acme") == 1
        results = backend.search("acme", [0.0, 1.0])
        assert results[0].text == "v2"

    def test_delete(self):
        backend = InMemoryVectorBackend()
        backend.upsert("acme", [self._record([1.0], id_="a"), self._record([0.0], id_="b")])
        assert backend.delete("acme", ["a", "missing"]) == 1
        assert backend.count("acme") == 1

    def test_ids_generated_when_missing(self):
        record = self._record([1.0])
        assert record.id
        backend = InMemoryVectorBackend()
        ids = backend.upsert("acme", [record])
        assert ids == [record.id]
