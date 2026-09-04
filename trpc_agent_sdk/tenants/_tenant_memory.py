# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Tenant-scoped memory service.

Wraps any SDK memory service (``InMemoryMemoryService``,
``RedisMemoryService``, ``SqlMemoryService``, ...) and namespaces every
memory key by tenant. The important property for multi-node deployments:
when the underlying service is backed by a shared store (Redis / SQL), all
nodes of the tenant see the same memories — the tenant prefix is a pure
key-naming convention, so no sticky session or node-local cache is
involved.
"""

from typing import Optional

from trpc_agent_sdk.abc import MemoryServiceABC, SearchMemoryResponse


class TenantScopedMemoryService(MemoryServiceABC):
    """Memory service that scopes all keys to a single tenant.

    Args:
        tenant_id: Tenant the service is scoped to.
        base_memory_service: Shared memory backend (Redis / SQL for
            cross-node visibility; in-memory only for tests).
    """

    def __init__(self, tenant_id: str, base_memory_service: MemoryServiceABC):
        self._tenant_id = tenant_id
        self._base_service = base_memory_service

    def scope_key(self, key: str) -> str:
        """Convert a client-visible memory key to its tenant-scoped form."""
        return f"{self._tenant_id}:{key}"

    @property
    def base_service(self) -> MemoryServiceABC:
        """Return the wrapped shared backend."""
        return self._base_service

    async def store_session(self, session, agent_context: Optional[object] = None) -> None:
        """Store session content under the tenant-scoped key."""
        scoped_session = session.model_copy(update={"id": self.scope_key(session.id)})
        await self._base_service.store_session(scoped_session, agent_context)

    async def search_memory(self,
                            key: str,
                            query: str,
                            limit: int = 10,
                            agent_context: Optional[object] = None) -> SearchMemoryResponse:
        """Search memory within this tenant's namespace only."""
        return await self._base_service.search_memory(self.scope_key(key), query, limit, agent_context)

    async def close(self) -> None:
        """Close the underlying shared backend."""
        await self._base_service.close()
