# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Unit tests for tenant isolation (TenantAwareSessionService, TenantContext)."""

from __future__ import annotations

import pytest

from trpc_agent_sdk.sessions import InMemorySessionService
from trpc_agent_sdk.tenants import (
    InMemoryTenantStore,
    Tenant,
    TenantAwareSessionService,
    TenantConfig,
    TenantContext,
    get_context_manager,
)

APP = "demo_app"


@pytest.fixture
async def store() -> InMemoryTenantStore:
    store = InMemoryTenantStore()
    for tid in ("acme", "globex"):
        await store.create_tenant(Tenant(tenant_id=tid, name=tid.upper()))
    return store


def make_service(store: InMemoryTenantStore, tenant_id: str) -> TenantAwareSessionService:
    return TenantAwareSessionService(
        tenant_context=TenantContext(store._tenants[tenant_id]),
        base_session_service=InMemorySessionService(),
    )


class TestSessionIsolation:

    async def test_create_scopes_storage_key_and_injects_tenant_state(self, store: InMemoryTenantStore):
        service = make_service(store, "acme")
        session = await service.create_session(app_name=APP, user_id="u1", session_id="s1")
        # Storage key is scoped, and state carries tenant ownership.
        assert session.id == "acme:s1"
        assert session.state["tenant_id"] == "acme"

    async def test_cross_tenant_session_ids_do_not_collide(self, store: InMemoryTenantStore):
        """Same session id for two tenants maps to two distinct storage keys."""
        acme = make_service(store, "acme")
        globex = make_service(store, "globex")

        await acme.create_session(app_name=APP, user_id="u1", session_id="s1")
        await globex.create_session(app_name=APP, user_id="u1", session_id="s1")

        got_acme = await acme.get_session(app_name=APP, user_id="u1", session_id="s1")
        got_globex = await globex.get_session(app_name=APP, user_id="u1", session_id="s1")
        assert got_acme is not None
        assert got_globex is not None
        assert got_acme.id != got_globex.id

    async def test_get_session_rejects_foreign_tenant_key(self, store: InMemoryTenantStore):
        acme = make_service(store, "acme")
        globex = make_service(store, "globex")

        # Create directly in the underlying storage under globex's scope.
        await globex.create_session(app_name=APP, user_id="u1", session_id="s1")

        # acme asks for the same client-visible id -> its own scoped key
        # "acme:s1", which does not exist -> None.
        assert (await acme.get_session(app_name=APP, user_id="u1", session_id="s1") is None)

    async def test_list_sessions_filters_other_tenants(self, store: InMemoryTenantStore):
        acme = make_service(store, "acme")
        globex = make_service(store, "globex")

        await acme.create_session(app_name=APP, user_id="u1", session_id="s_acme")
        await globex.create_session(app_name=APP, user_id="u1", session_id="s_globex")

        acme_list = await acme.list_sessions(app_name=APP, user_id="u1")
        globex_list = await globex.list_sessions(app_name=APP, user_id="u1")

        assert [s.id for s in acme_list.sessions] == ["acme:s_acme"]
        assert [s.id for s in globex_list.sessions] == ["globex:s_globex"]

    async def test_delete_session_scoped(self, store: InMemoryTenantStore):
        acme = make_service(store, "acme")
        await acme.create_session(app_name=APP, user_id="u1", session_id="s1")
        await acme.delete_session(app_name=APP, user_id="u1", session_id="s1")
        assert await acme.get_session(app_name=APP, user_id="u1", session_id="s1") is None


class TestTenantContextPermissions:

    def _tenant(self, **permissions) -> Tenant:
        config = TenantConfig(tenant_id="acme", name="ACME", tool_permissions=permissions)
        return config.to_tenant()

    def test_allowed_and_blocked_tools(self):
        tenant = self._tenant(allowed_tools=["search", "weather"], blocked_tools=["shell"])
        context = TenantContext(tenant)
        assert context.is_tool_allowed("search") is True
        assert context.is_tool_allowed("shell") is False
        assert context.is_tool_allowed("unknown") is False  # not in whitelist

    def test_empty_whitelist_allows_all(self):
        tenant = self._tenant(allowed_tools=[])
        context = TenantContext(tenant)
        assert context.is_tool_allowed("anything") is True

    def test_dangerous_tools(self):
        tenant = self._tenant(dangerous_tools=["shell"])
        context = TenantContext(tenant)
        assert context.is_tool_dangerous("shell") is True
        assert context.is_tool_dangerous("search") is False

    def test_context_manager_scoping(self, store: InMemoryTenantStore):
        tenant = store._tenants["acme"]
        manager = get_context_manager()
        with manager.with_tenant(tenant) as context:
            assert context.tenant_id == "acme"
            assert manager.get_current_context("acme") is context
        assert manager.get_current_context("acme") is None
