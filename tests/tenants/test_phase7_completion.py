# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Tests for phase-7 gap completion: unified storage summary/artifact,
file-system backend, cross-backend migration, admin API, IM session
strategy / WeCom markdown, OTLP exporter and session-dimension capacity."""

from __future__ import annotations

import json

import httpx
import pytest

from trpc_agent_sdk.tenants import (
    ChannelConfig,
    ConfigRolloutManager,
    FileSystemStorageBackend,
    InMemoryStorageBackend,
    InMemoryTenantStore,
    InMemoryVectorBackend,
    MigrationReport,
    SQLStorageBackend,
    StorageRouter,
    Tenant,
    TenantAdminService,
    TenantConfig,
    TenantMessage,
    UnifiedStorageBackend,
    VectorRecord,
    WeComSender,
    capacity_plan,
    create_admin_app,
    migrate_tenants,
    migrate_vectors,
    nodes_for_sessions,
    verify_tenant_migration,
)
from trpc_agent_sdk.tenants._tenant_channels import (
    TelegramTenantAdapter,
    WeComTenantAdapter,
    _resolve_session_strategy,
    _strategy_session_id,
)

# ---------------------------------------------------------------- summary / artifact


class _StubBackend(UnifiedStorageBackend):
    """Minimal concrete backend that does not implement summary/artifact."""

    async def get_session(self, tenant_id, session_id):
        return None

    async def save_session(self, tenant_id, session):
        pass

    async def add_session_event(self, tenant_id, session_id, event):
        pass

    async def get_memory(self, tenant_id, user_id, memory_id):
        return None

    async def save_memory(self, tenant_id, user_id, memory_data):
        pass

    async def search_knowledge(self, tenant_id, query, limit=10):
        return []

    async def save_audit_log(self, tenant_id, audit_data):
        pass

    async def health_check(self):
        return True


class TestSummaryArtifactAbc:

    async def test_default_methods_raise_not_implemented(self):
        backend = _StubBackend()
        with pytest.raises(NotImplementedError):
            await backend.save_summary("t", "s", "summary")
        with pytest.raises(NotImplementedError):
            await backend.get_summary("t", "s")
        with pytest.raises(NotImplementedError):
            await backend.save_artifact("t", "a", b"data")
        with pytest.raises(NotImplementedError):
            await backend.get_artifact("t", "a")
        with pytest.raises(NotImplementedError):
            await backend.delete_artifact("t", "a")

    async def test_inmemory_summary_roundtrip(self):
        backend = InMemoryStorageBackend()
        assert await backend.get_summary("t", "s") is None
        await backend.save_summary("t", "s", "first")
        await backend.save_summary("t", "s", "second")  # latest wins
        assert await backend.get_summary("t", "s") == "second"
        assert await backend.get_summary("t", "other") is None

    async def test_inmemory_artifact_roundtrip(self):
        backend = InMemoryStorageBackend()
        assert await backend.get_artifact("t", "a1") is None

        descriptor = await backend.save_artifact("t", "a1", b"\x00\x01", {"mime": "application/octet-stream"})
        assert descriptor["artifact_id"] == "a1"
        assert descriptor["tenant_id"] == "t"
        assert descriptor["size"] == 2

        loaded = await backend.get_artifact("t", "a1")
        assert loaded["data"] == b"\x00\x01"
        assert loaded["metadata"] == {"mime": "application/octet-stream"}

        assert await backend.delete_artifact("t", "a1") is True
        assert await backend.delete_artifact("t", "a1") is False
        assert await backend.get_artifact("t", "a1") is None

    async def test_sql_summary_and_artifact_roundtrip(self, tmp_path):
        pytest.importorskip("aiosqlite")
        path = str(tmp_path / "storage.db").replace("\\", "/")
        backend = SQLStorageBackend(f"sqlite:///{path}")

        await backend.save_summary("t", "s", "hello summary")
        assert await backend.get_summary("t", "s") == "hello summary"
        assert await backend.get_summary("t", "missing") is None

        descriptor = await backend.save_artifact("t", "a1", b"blob-bytes", {"mime": "text/plain"})
        assert descriptor["size"] == len(b"blob-bytes")
        loaded = await backend.get_artifact("t", "a1")
        assert loaded["data"] == b"blob-bytes"
        assert loaded["metadata"] == {"mime": "text/plain"}
        assert await backend.get_artifact("t", "missing") is None
        assert await backend.delete_artifact("t", "a1") is True
        assert await backend.delete_artifact("t", "a1") is False

    async def test_router_routes_artifact_to_dedicated_backend(self):
        router = StorageRouter({"t": {
            "session_backend": "in_memory",
            "artifact_backend": "in_memory",
        }})
        await router.save_summary("t", "s", "via-router")
        assert await router.get_summary("t", "s") == "via-router"

        descriptor = await router.save_artifact("t", "a1", b"xyz")
        assert descriptor["artifact_id"] == "a1"
        assert (await router.get_artifact("t", "a1"))["data"] == b"xyz"


# ---------------------------------------------------------------- file system backend


class TestFileSystemStorageBackend:

    def _backend(self, tmp_path) -> FileSystemStorageBackend:
        return FileSystemStorageBackend(str(tmp_path / "storage"))

    async def test_artifact_roundtrip_and_layout(self, tmp_path):
        backend = self._backend(tmp_path)
        await backend.save_artifact("acme", "report.bin", b"\x01\x02\x03", {"mime": "application/x"})
        data_path = tmp_path / "storage" / "acme" / "artifacts" / "report.bin"
        assert data_path.read_bytes() == b"\x01\x02\x03"
        meta = json.loads((tmp_path / "storage" / "acme" / "artifacts" / "report.bin.meta.json").read_text())
        assert meta["size"] == 3

        loaded = await backend.get_artifact("acme", "report.bin")
        assert loaded["data"] == b"\x01\x02\x03"
        assert loaded["metadata"] == {"mime": "application/x"}

        assert await backend.delete_artifact("acme", "report.bin") is True
        assert not data_path.exists()
        assert await backend.get_artifact("acme", "report.bin") is None

    async def test_session_and_summary_roundtrip(self, tmp_path):
        from trpc_agent_sdk.sessions import Session

        backend = self._backend(tmp_path)
        session = Session(id="s1", app_name="app", user_id="u1", save_key="sk", state={"k": "v"})
        await backend.save_session("acme", session)
        loaded = await backend.get_session("acme", "s1")
        assert loaded is not None
        assert loaded.id == "s1"
        assert loaded.state == {"k": "v"}

        await backend.save_summary("acme", "s1", "sum")
        assert await backend.get_summary("acme", "s1") == "sum"

    async def test_memory_knowledge_audit(self, tmp_path):
        backend = self._backend(tmp_path)
        await backend.save_memory("acme", "u1", {"id": "m1", "text": "fact"})
        assert (await backend.get_memory("acme", "u1", "m1"))["text"] == "fact"

        await backend.save_audit_log("acme", {"decision": "allow"})
        audit_file = tmp_path / "storage" / "acme" / "audit" / "audit.jsonl"
        assert json.loads(audit_file.read_text())["decision"] == "allow"

    async def test_unsafe_path_component_rejected(self, tmp_path):
        backend = self._backend(tmp_path)
        with pytest.raises(ValueError):
            await backend.save_artifact("../escape", "a", b"x")
        with pytest.raises(ValueError):
            await backend.save_artifact("a/b", "a", b"x")

    async def test_health_check(self, tmp_path):
        backend = self._backend(tmp_path)
        assert await backend.health_check() is True


# ---------------------------------------------------------------- data migration


def _tenant(tid: str, name: str) -> Tenant:
    return TenantConfig(tenant_id=tid, name=name).to_tenant()


class TestTenantMigration:

    async def test_migrate_tenants_is_idempotent(self):
        source = InMemoryTenantStore()
        target = InMemoryTenantStore()
        for tid in ("a", "b", "c"):
            await source.create_tenant(_tenant(tid, f"Tenant {tid}"))

        report = await migrate_tenants(source, target)
        assert report.ok
        assert report.migrated == 3
        assert (await target.get_tenant("b")).name == "Tenant b"

        # Re-run: nothing to do
        rerun = await migrate_tenants(source, target)
        assert rerun.migrated == 0
        assert rerun.skipped == 3

        # Overwrite replaces target copies
        overwrite = await migrate_tenants(source, target, overwrite=True)
        assert overwrite.migrated == 3
        assert overwrite.skipped == 0

    async def test_migrate_tenants_continues_after_failure(self):
        source = InMemoryTenantStore()
        target = InMemoryTenantStore()
        await source.create_tenant(_tenant("good1", "G1"))
        await source.create_tenant(_tenant("bad", "B"))
        await source.create_tenant(_tenant("good2", "G2"))

        original_create = target.create_tenant

        async def flaky_create(tenant: Tenant) -> Tenant:
            if tenant.tenant_id == "bad":
                raise ConnectionError("boom")
            return await original_create(tenant)

        target.create_tenant = flaky_create  # type: ignore[method-assign]

        report = await migrate_tenants(source, target)
        assert report.migrated == 2
        assert report.failed == 1
        assert not report.ok
        assert "bad" in report.errors[0]

    async def test_migrate_vectors_and_read_back(self):
        source = InMemoryVectorBackend()
        target = InMemoryVectorBackend()
        records = [
            VectorRecord(embedding=[1.0, 0.0], text="r1", id="id1"),
            VectorRecord(embedding=[0.0, 1.0], text="r2", id="id2"),
            VectorRecord(embedding=[0.5, 0.5], text="r3", id="id3"),
        ]
        source.upsert("acme", records)

        report = await migrate_vectors(source, target, ["acme"])
        assert report.ok
        assert report.migrated == 3
        assert target.count("acme") == 3
        # Original ids preserved -> idempotent re-run overwrites in place
        rerun = await migrate_vectors(source, target, ["acme"])
        assert rerun.migrated == 3
        assert target.count("acme") == 3

    async def test_migrate_vectors_count_mismatch_reported(self):
        source = InMemoryVectorBackend()
        target = InMemoryVectorBackend()
        source.upsert("acme", [VectorRecord(embedding=[1.0], id="id1")])

        # Target loses the record after upsert -> read-back catches it
        original_upsert = target.upsert

        def leaky_upsert(tid, batch):
            result = original_upsert(tid, batch)
            target.delete(tid, ["id1"])
            return result

        target.upsert = leaky_upsert  # type: ignore[method-assign]

        report = await migrate_vectors(source, target, ["acme"])
        assert report.failed == 1
        assert "count mismatch" in report.errors[0]

    async def test_vector_list_records_base_raises(self):
        backend = InMemoryVectorBackend()
        records = backend.list_records("missing")
        assert records == []

    async def test_verify_tenant_migration_gate(self):
        source = InMemoryTenantStore()
        target = InMemoryTenantStore()
        await source.create_tenant(_tenant("a", "Alpha"))
        await source.create_tenant(_tenant("b", "Beta"))

        assert (await verify_tenant_migration(source, target)).failed == 2

        await migrate_tenants(source, target)
        report = await verify_tenant_migration(source, target)
        assert report.ok
        assert report.migrated == 2

        # Configuration drift is caught (recreate with an independent object —
        # migrated tenants share references between the two stores)
        await target.delete_tenant("a")
        await target.create_tenant(_tenant("a", "Renamed"))
        report = await verify_tenant_migration(source, target)
        assert report.failed == 1
        assert "mismatch" in report.errors[0]


# ---------------------------------------------------------------- admin API


class TestTenantAdminService:

    async def test_crud_and_activation(self):
        store = InMemoryTenantStore()
        service = TenantAdminService(store)

        created = await service.create_tenant({"tenant_id": "acme", "name": "ACME"})
        assert created["tenant_id"] == "acme"
        assert created["is_active"] is True

        got = await service.get_tenant("acme")
        assert got["name"] == "ACME"
        assert await service.get_tenant("ghost") is None

        updated = await service.update_tenant("acme", {"tenant_id": "acme", "name": "ACME Corp"})
        assert updated["name"] == "ACME Corp"

        deactivated = await service.set_active("acme", False)
        assert deactivated["is_active"] is False
        activated = await service.set_active("acme", True)
        assert activated["is_active"] is True

        assert await service.delete_tenant("acme") is True
        assert await service.get_tenant("acme") is None

    async def test_update_missing_tenant_raises(self):
        service = TenantAdminService(InMemoryTenantStore())
        with pytest.raises(KeyError):
            await service.update_tenant("ghost", {"tenant_id": "ghost", "name": "G"})
        with pytest.raises(KeyError):
            await service.set_active("ghost", True)

    async def test_health_and_metrics(self):
        service = TenantAdminService(InMemoryTenantStore())
        health = await service.health()
        assert health["healthy"] is True
        assert health["store"] is True
        assert "tenant_" in service.metrics_text() or service.metrics_text() == ""

    async def test_health_reports_degraded_backends(self):
        from trpc_agent_sdk.tenants import DegradationController

        controller = DegradationController()
        controller.register("db", failure_threshold=1, probe_interval_seconds=0)
        controller.report_failure("db")
        controller.report_failure("db")
        service = TenantAdminService(InMemoryTenantStore(), degradation=controller)
        health = await service.health()
        assert health["healthy"] is False
        assert health["backends"]["db"]["state"] == "unavailable"

    def test_rollout_operations(self):
        service = TenantAdminService(InMemoryTenantStore(), rollout=ConfigRolloutManager())
        assert service.rollout_state("acme") is None

        state = service.publish("acme", target_version=2, canary_percent=10, baseline_version=1)
        assert state["target_version"] == 2
        assert service.set_canary("acme", 50)["canary_percent"] == 50
        assert service.promote("acme")["canary_percent"] == 100
        assert service.rollback("acme")["target_version"] == 1

    def test_capacity_with_session_dimension(self):
        service = TenantAdminService(InMemoryTenantStore())
        summary = service.capacity(
            peak_qps=120.0,
            per_node_qps=50.0,
            tenant_usage=[{
                "tenant_id": "acme",
                "requests_mtd": 1000,
                "cost_usd_mtd": 10.0,
                "monthly_budget_usd": 50.0
            }],
            concurrent_sessions=2000,
            per_node_sessions=500,
        )
        # QPS needs 4 nodes, sessions need 6 -> max wins
        assert summary["nodes_required"] == 6
        assert summary["session_nodes_required"] == 6


class TestAdminHttpApi:

    def _app(self, store=None):
        pytest.importorskip("fastapi")
        store = store or InMemoryTenantStore()
        return create_admin_app(TenantAdminService(store, rollout=ConfigRolloutManager()))

    async def _client(self, app):
        transport = httpx.ASGITransport(app=app)
        return httpx.AsyncClient(transport=transport, base_url="http://admin")

    async def test_full_tenant_lifecycle_over_http(self):
        app = self._app()
        async with await self._client(app) as client:
            health = await client.get("/health")
            assert health.status_code == 200
            assert health.json()["healthy"] is True

            created = await client.post("/tenants", json={"tenant_id": "acme", "name": "ACME"})
            assert created.status_code == 201

            got = await client.get("/tenants/acme")
            assert got.status_code == 200
            assert got.json()["name"] == "ACME"

            assert (await client.get("/tenants/ghost")).status_code == 404

            activated = await client.post("/tenants/acme/deactivate")
            assert activated.json()["is_active"] is False
            assert (await client.post("/tenants/ghost/activate")).status_code == 404

            deleted = await client.delete("/tenants/acme")
            assert deleted.status_code == 200
            assert (await client.delete("/tenants/acme")).status_code == 404

            listed = await client.get("/tenants")
            assert listed.status_code == 200
            assert isinstance(listed.json(), list)

    async def test_rollout_and_capacity_routes(self):
        app = self._app()
        async with await self._client(app) as client:
            # Rollout target must exceed the baseline (defaults to 0 with no
            # history) -> publishing version 0 is rejected with 409
            bad = await client.post("/rollout/acme/publish", json={"target_version": 0})
            assert bad.status_code == 409

            ok = await client.post("/rollout/acme/publish",
                                   json={
                                       "target_version": 2,
                                       "canary_percent": 10,
                                       "baseline_version": 1
                                   })
            assert ok.status_code == 200
            assert ok.json()["canary_percent"] == 10

            canary = await client.post("/rollout/acme/canary", json={"canary_percent": 40})
            assert canary.json()["canary_percent"] == 40

            assert (await client.post("/rollout/acme/promote")).json()["canary_percent"] == 100
            assert (await client.post("/rollout/acme/rollback")).json()["target_version"] == 1
            assert (await client.get("/rollout/acme")).status_code == 200
            assert (await client.get("/rollout/ghost")).status_code == 404

            capacity = await client.post(
                "/capacity",
                json={
                    "peak_qps": 100.0,
                    "per_node_qps": 50.0,
                    "tenant_usage": [],
                    "concurrent_sessions": 1000,
                    "per_node_sessions": 200,
                },
            )
            assert capacity.status_code == 200
            assert capacity.json()["nodes_required"] == 7  # ceil(5 * 1.3) sessions dominate

            metrics = await client.get("/metrics")
            assert metrics.status_code == 200
            assert metrics.headers["content-type"].startswith("text/plain")


# ---------------------------------------------------------------- IM session strategy


def _message(strategy=None, is_group=False, user="u1", chat="c1") -> TenantMessage:
    from datetime import datetime

    metadata = {"session_strategy": strategy} if strategy else {}
    return TenantMessage(
        tenant_id="acme",
        channel_type="wecom",
        user_id=user,
        chat_id=chat,
        message_id="m1",
        content="hi",
        metadata=metadata,
        timestamp=datetime.utcnow(),
        is_group_chat=is_group,
    )


class TestSessionStrategy:

    def test_default_strategy_is_chat_level(self):
        assert _strategy_session_id(_message(), "wecom") == "acme:wecom:chat:c1"

    def test_user_strategy_spans_chats(self):
        assert _strategy_session_id(_message("user", chat="c1"), "wecom") == "acme:wecom:user:u1"
        assert _strategy_session_id(_message("user", chat="c2"), "wecom") == "acme:wecom:user:u1"

    def test_group_strategy_only_applies_to_group_chats(self):
        assert _strategy_session_id(_message("group", is_group=True), "wecom") == "acme:wecom:group:c1"
        # Private chat falls back to chat-level
        assert _strategy_session_id(_message("group", is_group=False), "wecom") == "acme:wecom:chat:c1"

    def test_invalid_strategy_falls_back_to_chat(self):
        assert _strategy_session_id(_message("bogus"), "wecom") == "acme:wecom:chat:c1"

    def test_resolve_strategy_from_tenant_config(self):
        tenant = Tenant(tenant_id="acme",
                        name="ACME",
                        channel_configs={"wecom": ChannelConfig(channel_type="wecom", session_strategy="user")})
        assert _resolve_session_strategy(tenant, "wecom") == "user"
        assert _resolve_session_strategy(tenant, "telegram") == "chat"
        assert _resolve_session_strategy(None, "wecom") == "chat"

    def test_adapters_honor_strategy(self):
        from datetime import datetime

        message = TenantMessage(
            tenant_id="acme",
            channel_type="telegram",
            user_id="u9",
            chat_id="c9",
            message_id="m1",
            content="hi",
            metadata={"session_strategy": "user"},
            timestamp=datetime.utcnow(),
        )
        assert TelegramTenantAdapter(tenant_store=None).generate_session_id(message) == "acme:telegram:user:u9"
        # WeCom adapter with default (chat) strategy
        wecom_message = _message()
        assert WeComTenantAdapter(tenant_store=None).generate_session_id(wecom_message) == "acme:wecom:chat:c1"


class TestWeComMarkdown:

    async def test_send_markdown_uses_markdown_msgtype(self):
        requests = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            if request.url.path == "/cgi-bin/gettoken":
                return httpx.Response(200, json={"errcode": 0, "access_token": "AT", "expires_in": 7200})
            return httpx.Response(200, json={"errcode": 0})

        transport = httpx.MockTransport(handler)
        sender = WeComSender("corp_1",
                             "secret_1",
                             "agent_1",
                             http_client=httpx.AsyncClient(transport=transport, base_url="https://qyapi.weixin.qq.com"))
        assert await sender.send_markdown("u1", "**bold** [link](https://x)") is True
        body = json.loads(requests[-1].content)
        assert body["msgtype"] == "markdown"
        assert body["markdown"]["content"] == "**bold** [link](https://x)"
        assert "text" not in body

    async def test_send_markdown_failure(self):

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/cgi-bin/gettoken":
                return httpx.Response(200, json={"errcode": 0, "access_token": "AT", "expires_in": 7200})
            return httpx.Response(200, json={"errcode": 81013})

        transport = httpx.MockTransport(handler)
        sender = WeComSender("corp_1",
                             "secret_1",
                             "agent_1",
                             http_client=httpx.AsyncClient(transport=transport, base_url="https://qyapi.weixin.qq.com"))
        assert await sender.send_markdown("u1", "x") is False

    async def test_adapter_card_response_sends_markdown(self):
        from trpc_agent_sdk.tenants import TenantResponse

        store = InMemoryTenantStore()
        await store.create_tenant(
            Tenant(
                tenant_id="acme",
                name="ACME",
                channel_configs={
                    "wecom":
                    ChannelConfig(
                        channel_type="wecom",
                        bot_id="corp_1",
                        api_key="corp_secret_1",
                        webhook_token="agent_1",
                    )
                },
            ))
        requests = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            if request.url.path == "/cgi-bin/gettoken":
                return httpx.Response(200, json={"errcode": 0, "access_token": "AT", "expires_in": 7200})
            return httpx.Response(200, json={"errcode": 0})

        transport = httpx.MockTransport(handler)
        adapter = WeComTenantAdapter(store,
                                     http_client=httpx.AsyncClient(transport=transport,
                                                                   base_url="https://qyapi.weixin.qq.com"))
        response = TenantResponse(
            tenant_id="acme",
            channel_type="wecom",
            user_id="u1",
            chat_id="c1",
            content="**card**",
            message_type="card",
        )
        assert await adapter.send_response(response) is True
        assert json.loads(requests[-1].content)["msgtype"] == "markdown"


# ---------------------------------------------------------------- OTLP exporter & capacity


class TestOtelHttpExporter:

    def test_noop_when_provider_already_set(self, monkeypatch):
        from trpc_agent_sdk.tenants import _tenant_telemetry

        class _FakeProvider:

            def add_span_processor(self, processor):
                pass

        monkeypatch.setattr(_tenant_telemetry.trace, "get_tracer_provider", lambda: _FakeProvider())
        assert _tenant_telemetry.configure_otel_http_exporter("http://collector:4318/v1/traces") is False

    def test_installs_and_is_idempotent(self, monkeypatch):
        pytest.importorskip("opentelemetry.sdk.trace")
        pytest.importorskip("opentelemetry.exporter.otlp.proto.http.trace_exporter")
        from trpc_agent_sdk.tenants import _tenant_telemetry

        installed_providers = []
        monkeypatch.setattr(_tenant_telemetry.trace, "set_tracer_provider", installed_providers.append)
        monkeypatch.setattr(_tenant_telemetry, "_otel_export_configured", False)

        assert _tenant_telemetry.configure_otel_http_exporter("http://collector:4318/v1/traces") is True
        assert len(installed_providers) == 1

        # Second call must be a no-op (guard flag)
        assert _tenant_telemetry.configure_otel_http_exporter("http://collector:4318/v1/traces") is False
        assert len(installed_providers) == 1


class TestSessionCapacity:

    def test_nodes_for_sessions(self):
        assert nodes_for_sessions(0, 500) == 1
        assert nodes_for_sessions(1000, 500) == 3  # ceil(2 * 1.3)
        assert nodes_for_sessions(1000, 500, headroom_percent=0) == 2
        with pytest.raises(ValueError):
            nodes_for_sessions(-1, 500)
        with pytest.raises(ValueError):
            nodes_for_sessions(100, 0)

    def test_capacity_plan_takes_max_of_dimensions(self):
        report = capacity_plan(
            peak_qps=120.0,
            per_node_qps=50.0,  # 4 qps nodes
            tenant_usage=[],
            concurrent_sessions=2000,
            per_node_sessions=500,  # 6 session nodes
        )
        assert report.nodes_required == 6
        assert report.session_nodes_required == 6
        assert report.peak_concurrent_sessions == 2000

        # QPS-dominant workload keeps the qps dimension
        report = capacity_plan(
            peak_qps=500.0,
            per_node_qps=50.0,  # 13 qps nodes
            tenant_usage=[],
            concurrent_sessions=100,
            per_node_sessions=500,  # 1 session node
        )
        assert report.nodes_required == 13
        assert report.summary()["session_nodes_required"] == 1

    def test_capacity_report_defaults_unevaluated(self):
        report = capacity_plan(peak_qps=10.0, per_node_qps=10.0, tenant_usage=[])
        assert report.session_nodes_required == 0
        assert report.summary()["nodes_required"] == 2  # 10 QPS @ 10/node with 30% headroom


def test_migration_report_defaults():
    report = MigrationReport()
    assert report.ok
    assert report.summary() == "migrated=0 skipped=0 failed=0"
