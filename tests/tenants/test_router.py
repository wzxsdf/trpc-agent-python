# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Unit tests for trpc_agent_sdk.tenants._router (tenant routing)."""

from __future__ import annotations

import hashlib

import pytest

from trpc_agent_sdk.tenants import (
    ApiKeyIdentifier,
    CustomIdentifier,
    HttpHeaderIdentifier,
    ImWebhookTokenIdentifier,
    InMemoryTenantStore,
    PathPrefixIdentifier,
    SubdomainIdentifier,
    Tenant,
    TenantRouter,
)


@pytest.fixture
def tenant() -> Tenant:
    return Tenant(tenant_id="acme", name="ACME")


@pytest.fixture
async def store(tenant: Tenant) -> InMemoryTenantStore:
    store = InMemoryTenantStore()
    await store.create_tenant(tenant)
    return store


class TestIdentifiers:

    async def test_http_header(self):
        identifier = HttpHeaderIdentifier("X-Tenant-ID")
        request = {"headers": {"X-Tenant-ID": "acme"}}
        assert await identifier.extract_tenant_id(request) == "acme"

    async def test_http_header_missing(self):
        identifier = HttpHeaderIdentifier("X-Tenant-ID")
        assert await identifier.extract_tenant_id({"headers": {}}) is None

    async def test_api_key(self):
        identifier = ApiKeyIdentifier({"sk_1": "acme"})
        assert await identifier.extract_tenant_id({"api_key": "sk_1"}) == "acme"
        assert await identifier.extract_tenant_id({"api_key": "bad"}) is None

    async def test_subdomain(self):
        identifier = SubdomainIdentifier("example.com")
        request = {"headers": {"Host": "acme.example.com:8080"}}
        assert await identifier.extract_tenant_id(request) == "acme"

    async def test_path_prefix(self):
        identifier = PathPrefixIdentifier("/t/")
        assert await identifier.extract_tenant_id({"path": "/t/acme/chat"}) == "acme"

    async def test_im_webhook_token(self):
        identifier = ImWebhookTokenIdentifier({"tok": "acme"})
        assert await identifier.extract_tenant_id({"token": "tok"}) == "acme"

    async def test_custom(self):
        identifier = CustomIdentifier(lambda req, meta: "acme")
        assert await identifier.extract_tenant_id({}) == "acme"


class TestTenantRouter:

    async def test_route_by_header(self, store: InMemoryTenantStore):
        router = TenantRouter(store).add_identifier(HttpHeaderIdentifier("X-Tenant-ID"))
        tenant = await router.route_request({"headers": {"X-Tenant-ID": "acme"}})
        assert tenant is not None
        assert tenant.tenant_id == "acme"

    async def test_route_fallback_strategy(self, store: InMemoryTenantStore):
        """First strategy misses, second one matches."""
        router = TenantRouter(store)
        router.add_identifier(SubdomainIdentifier("example.com"))
        router.add_identifier(ApiKeyIdentifier({"sk_1": "acme"}))
        tenant = await router.route_request({"api_key": "sk_1"})
        assert tenant is not None
        assert tenant.tenant_id == "acme"

    async def test_route_unknown_tenant_returns_none(self, store: InMemoryTenantStore):
        router = TenantRouter(store).add_identifier(HttpHeaderIdentifier("X-Tenant-ID"))
        assert await router.route_request({"headers": {"X-Tenant-ID": "ghost"}}) is None

    async def test_route_fallback_tenant(self, store: InMemoryTenantStore):
        router = TenantRouter(store).add_identifier(HttpHeaderIdentifier("X-Tenant-ID"))
        router.set_fallback_tenant("acme")
        tenant = await router.route_request({"headers": {}})
        assert tenant is not None
        assert tenant.tenant_id == "acme"

    async def test_route_no_match_returns_none(self, store: InMemoryTenantStore):
        router = TenantRouter(store)
        assert await router.route_request({"headers": {}}) is None


class TestWebhookSignature:

    async def test_valid_wecom_style_signature(self, store: InMemoryTenantStore, tenant: Tenant):
        secret, ts, nonce = "sec", "123", "abc"
        signature = hashlib.sha1("".join(sorted([secret, ts, nonce])).encode()).hexdigest()
        router = TenantRouter(store)
        assert (await router.verify_webhook_signature(tenant,
                                                      b"payload",
                                                      signature,
                                                      secret=secret,
                                                      timestamp=ts,
                                                      nonce=nonce) is True)

    async def test_valid_hmac_signature(self, store: InMemoryTenantStore, tenant: Tenant):
        import hmac as hmac_mod

        secret = "sec"
        signature = hmac_mod.new(secret.encode(), b"payload", hashlib.sha256).hexdigest()
        router = TenantRouter(store)
        assert (await router.verify_webhook_signature(tenant, b"payload", signature, secret=secret) is True)

    async def test_invalid_signature(self, store: InMemoryTenantStore, tenant: Tenant):
        router = TenantRouter(store)
        assert (await router.verify_webhook_signature(tenant, b"payload", "deadbeef", secret="sec") is False)

    async def test_empty_signature(self, store: InMemoryTenantStore, tenant: Tenant):
        router = TenantRouter(store)
        assert await router.verify_webhook_signature(tenant, b"payload", "") is False
