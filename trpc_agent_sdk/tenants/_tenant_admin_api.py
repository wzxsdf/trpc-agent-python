# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Admin API for tenant management (control-plane HTTP interface).

Two layers:

- :class:`TenantAdminService` — framework-agnostic operations (tenant CRUD,
  activation, health, metrics, rollout ops, capacity planning). Usable
  directly from Python or CLI tooling.
- :func:`create_admin_app` — a FastAPI application exposing the service over
  HTTP (``fastapi`` is an optional dependency; install with
  ``pip install fastapi``).

Endpoints (when mounted, e.g. behind the Gateway with admin auth):

    GET    /health                      liveness + storage degradation snapshot
    GET    /metrics                     Prometheus text exposition
    GET    /tenants                     list tenants (skip/limit/active_only)
    POST   /tenants                     create tenant
    GET    /tenants/{tenant_id}         read tenant
    PUT    /tenants/{tenant_id}         replace tenant config
    DELETE /tenants/{tenant_id}         delete tenant
    POST   /tenants/{tenant_id}/activate
    POST   /tenants/{tenant_id}/deactivate
    GET    /rollout/{tenant_id}         current rollout state
    POST   /rollout/{tenant_id}/publish    {target_version, canary_percent, baseline_version?}
    POST   /rollout/{tenant_id}/canary     {canary_percent}
    POST   /rollout/{tenant_id}/promote
    POST   /rollout/{tenant_id}/rollback
    POST   /capacity                   {peak_qps, per_node_qps, tenant_usage[], headroom_percent?}

This is the *management* plane — tenant data-plane requests keep going through
the Gateway/Worker topology; it does not serve end-user traffic.
"""

from typing import Any, Dict, List, Optional

from trpc_agent_sdk.log import logger
from trpc_agent_sdk.tenants._tenant_capacity import capacity_plan
from trpc_agent_sdk.tenants._tenant_degradation import DegradationController
from trpc_agent_sdk.tenants._tenant_model import Tenant, TenantConfig
from trpc_agent_sdk.tenants._tenant_rollout import ConfigRolloutManager
from trpc_agent_sdk.tenants._tenant_store import TenantStore
from trpc_agent_sdk.tenants._tenant_telemetry import get_tenant_metrics


class TenantAdminService:
    """Framework-agnostic tenant administration operations."""

    def __init__(self,
                 store: TenantStore,
                 rollout: Optional[ConfigRolloutManager] = None,
                 degradation: Optional[DegradationController] = None):
        """Initialize the service.

        Args:
            store: Tenant store backing CRUD operations.
            rollout: Config rollout manager; a fresh in-memory one is created
                when omitted (rollout state is process-local — production
                deployments should share it behind a single admin instance).
            degradation: Degradation controller whose snapshot feeds /health.
        """
        self._store = store
        self._rollout = rollout or ConfigRolloutManager()
        self._degradation = degradation

    # -- tenant CRUD -------------------------------------------------------

    async def list_tenants(self, skip: int = 0, limit: int = 100, active_only: bool = False) -> List[Dict[str, Any]]:
        tenants = await self._store.list_tenants(skip=skip, limit=limit, active_only=active_only)
        return [self._dump(t) for t in tenants]

    async def get_tenant(self, tenant_id: str) -> Optional[Dict[str, Any]]:
        tenant = await self._store.get_tenant(tenant_id)
        return self._dump(tenant) if tenant else None

    async def create_tenant(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        tenant = TenantConfig(**payload).to_tenant()
        created = await self._store.create_tenant(tenant)
        logger.info(f"Admin API: created tenant {created.tenant_id}")
        return self._dump(created)

    async def update_tenant(self, tenant_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        existing = await self._store.get_tenant(tenant_id)
        if existing is None:
            raise KeyError(f"Tenant '{tenant_id}' does not exist")
        payload = dict(payload)
        payload["tenant_id"] = tenant_id
        updated = TenantConfig(**payload).to_tenant()
        # Preserve server-owned fields TenantConfig does not carry.
        updated.created_at = existing.created_at
        result = await self._store.update_tenant(updated)
        logger.info(f"Admin API: updated tenant {tenant_id}")
        return self._dump(result)

    async def delete_tenant(self, tenant_id: str) -> bool:
        deleted = await self._store.delete_tenant(tenant_id)
        logger.info(f"Admin API: delete tenant {tenant_id} -> {deleted}")
        return deleted

    async def set_active(self, tenant_id: str, active: bool) -> Dict[str, Any]:
        tenant = await self._store.get_tenant(tenant_id)
        if tenant is None:
            raise KeyError(f"Tenant '{tenant_id}' does not exist")
        tenant.is_active = active
        updated = await self._store.update_tenant(tenant)
        action = "activated" if active else "deactivated"
        logger.info(f"Admin API: {action} tenant {tenant_id}")
        return self._dump(updated)

    # -- health / metrics --------------------------------------------------

    async def health(self) -> Dict[str, Any]:
        """Liveness probe payload: store reachability + degradation snapshot."""
        store_healthy = True
        try:
            await self._store.list_tenants(skip=0, limit=1, active_only=False)
        except Exception as e:  # noqa: BLE001 — health must never raise
            store_healthy = False
            logger.error(f"Admin health probe failed on tenant store: {e}")

        payload: Dict[str, Any] = {
            "healthy": store_healthy,
            "store": store_healthy,
        }
        if self._degradation is not None:
            payload["backends"] = self._degradation.snapshot()
            if not all(b["state"] == "closed" for b in payload["backends"].values()):
                payload["healthy"] = False
        return payload

    def metrics_text(self) -> str:
        """Prometheus text exposition of tenant runtime metrics."""
        return get_tenant_metrics().render_prometheus()

    # -- rollout ops -------------------------------------------------------

    def rollout_state(self, tenant_id: str) -> Optional[Dict[str, Any]]:
        state = self._rollout.rollout_state(tenant_id)
        return self._dump_rollout(state) if state else None

    def publish(self,
                tenant_id: str,
                target_version: int,
                canary_percent: int = 0,
                baseline_version: Optional[int] = None) -> Dict[str, Any]:
        state = self._rollout.publish(tenant_id, target_version, canary_percent, baseline_version)
        return self._dump_rollout(state)

    def set_canary(self, tenant_id: str, canary_percent: int) -> Dict[str, Any]:
        state = self._rollout.set_canary_percent(tenant_id, canary_percent)
        return self._dump_rollout(state)

    def promote(self, tenant_id: str) -> Dict[str, Any]:
        state = self._rollout.promote(tenant_id)
        return self._dump_rollout(state)

    def rollback(self, tenant_id: str) -> Optional[Dict[str, Any]]:
        state = self._rollout.rollback(tenant_id)
        return self._dump_rollout(state) if state else None

    # -- capacity ----------------------------------------------------------

    def capacity(self,
                 peak_qps: float,
                 per_node_qps: float,
                 tenant_usage: List[Dict[str, float]],
                 headroom_percent: float = 30.0,
                 concurrent_sessions: float = 0.0,
                 per_node_sessions: float = 0.0) -> Dict[str, Any]:
        report = capacity_plan(peak_qps,
                               per_node_qps,
                               tenant_usage,
                               headroom_percent,
                               concurrent_sessions=concurrent_sessions,
                               per_node_sessions=per_node_sessions)
        return report.summary()

    # -- helpers -----------------------------------------------------------

    @staticmethod
    def _dump(tenant: Tenant) -> Dict[str, Any]:
        return TenantConfig.from_tenant(tenant).model_dump()

    @staticmethod
    def _dump_rollout(state) -> Dict[str, Any]:
        return {
            "tenant_id": state.tenant_id,
            "target_version": state.target_version,
            "baseline_version": state.baseline_version,
            "canary_percent": state.canary_percent,
            "published": state.published,
        }


def create_admin_app(service: TenantAdminService):
    """Create a FastAPI application exposing :class:`TenantAdminService`.

    Requires the optional ``fastapi`` dependency. Route handlers translate
    HTTP errors (404 for unknown tenants, 400/409 for rollout violations).

    Args:
        service: The admin service backing the routes.

    Returns:
        A configured ``fastapi.FastAPI`` instance.
    """
    try:
        from fastapi import FastAPI, HTTPException
        from pydantic import BaseModel
    except ImportError as e:
        raise ImportError("Admin API requires 'fastapi'. Install with: pip install fastapi") from e

    class TenantPayload(BaseModel):
        """Tenant configuration as JSON (TenantConfig schema)."""

        model_config = {"extra": "allow"}

    class PublishPayload(BaseModel):
        target_version: int
        canary_percent: int = 0
        baseline_version: Optional[int] = None

    class CanaryPayload(BaseModel):
        canary_percent: int

    class CapacityPayload(BaseModel):
        peak_qps: float
        per_node_qps: float
        tenant_usage: List[Dict[str, float]]
        headroom_percent: float = 30.0
        concurrent_sessions: float = 0.0
        per_node_sessions: float = 0.0

    app = FastAPI(title="tRPC-Agent-Python Tenant Admin API", version="1.0")

    @app.get("/health")
    async def health():
        return await service.health()

    @app.get("/metrics")
    async def metrics():
        from fastapi import Response

        return Response(content=service.metrics_text(), media_type="text/plain; version=0.0.4")

    @app.get("/tenants")
    async def list_tenants(skip: int = 0, limit: int = 100, active_only: bool = False):
        return await service.list_tenants(skip=skip, limit=limit, active_only=active_only)

    @app.post("/tenants", status_code=201)
    async def create_tenant(payload: TenantPayload):
        return await service.create_tenant(payload.model_dump(exclude_none=True))

    @app.get("/tenants/{tenant_id}")
    async def get_tenant(tenant_id: str):
        tenant = await service.get_tenant(tenant_id)
        if tenant is None:
            raise HTTPException(status_code=404, detail=f"Tenant '{tenant_id}' not found")
        return tenant

    @app.put("/tenants/{tenant_id}")
    async def update_tenant(tenant_id: str, payload: TenantPayload):
        try:
            return await service.update_tenant(tenant_id, payload.model_dump(exclude_none=True))
        except KeyError:
            raise HTTPException(status_code=404, detail=f"Tenant '{tenant_id}' not found")

    @app.delete("/tenants/{tenant_id}")
    async def delete_tenant(tenant_id: str):
        deleted = await service.delete_tenant(tenant_id)
        if not deleted:
            raise HTTPException(status_code=404, detail=f"Tenant '{tenant_id}' not found")
        return {"deleted": True, "tenant_id": tenant_id}

    @app.post("/tenants/{tenant_id}/activate")
    async def activate_tenant(tenant_id: str):
        try:
            return await service.set_active(tenant_id, True)
        except KeyError:
            raise HTTPException(status_code=404, detail=f"Tenant '{tenant_id}' not found")

    @app.post("/tenants/{tenant_id}/deactivate")
    async def deactivate_tenant(tenant_id: str):
        try:
            return await service.set_active(tenant_id, False)
        except KeyError:
            raise HTTPException(status_code=404, detail=f"Tenant '{tenant_id}' not found")

    @app.get("/rollout/{tenant_id}")
    async def rollout_state(tenant_id: str):
        state = service.rollout_state(tenant_id)
        if state is None:
            raise HTTPException(status_code=404, detail=f"No rollout for tenant '{tenant_id}'")
        return state

    @app.post("/rollout/{tenant_id}/publish")
    async def publish(tenant_id: str, payload: PublishPayload):
        try:
            return service.publish(tenant_id, payload.target_version, payload.canary_percent, payload.baseline_version)
        except ValueError as e:
            raise HTTPException(status_code=409, detail=str(e))

    @app.post("/rollout/{tenant_id}/canary")
    async def set_canary(tenant_id: str, payload: CanaryPayload):
        try:
            return service.set_canary(tenant_id, payload.canary_percent)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/rollout/{tenant_id}/promote")
    async def promote(tenant_id: str):
        try:
            return service.promote(tenant_id)
        except ValueError as e:
            raise HTTPException(status_code=409, detail=str(e))

    @app.post("/rollout/{tenant_id}/rollback")
    async def rollback(tenant_id: str):
        state = service.rollback(tenant_id)
        if state is None:
            raise HTTPException(status_code=409, detail="Nothing to roll back")
        return state

    @app.post("/capacity")
    async def capacity(payload: CapacityPayload):
        return service.capacity(payload.peak_qps, payload.per_node_qps, payload.tenant_usage, payload.headroom_percent,
                                payload.concurrent_sessions, payload.per_node_sessions)

    return app
