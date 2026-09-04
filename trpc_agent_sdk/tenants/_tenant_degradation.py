# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Unified storage degradation: primary/fallback with health-driven recovery.

Multi-tenant deployments must survive the loss of a shared backend (Redis
down, SQL failover) without taking every tenant offline. This module provides
the common policy once, instead of ad-hoc try/except in every store:

- :class:`BackendHealth` tracks per-backend availability with a failure
  threshold and a recovery probe interval (closed → open → probe, the same
  state machine shape as the tool circuit breaker but as a *switch*, not a
  denial source).
- :class:`DegradationController` is the process-wide switchboard: callers ask
  ``allow(backend)`` before touching the primary and report the outcome; the
  controller flips back to the primary automatically once the probe window
  elapses.
- :class:`TenantStoreWithFallback` wires a primary ``TenantStore`` (SQL /
  Redis) to a fallback (in-memory): reads/writes transparently degrade while
  the primary is marked unavailable.

Consistency trade-off (documented in MULTI_TENANT_OPERATIONS.md): while
degraded, data written to the fallback does not replicate to the primary —
the choice is availability over durability, and the controller exposes
``snapshot()`` so monitoring can alert on degradation instead of hiding it.
"""

import threading
import time
from typing import Callable, Dict, Optional

from trpc_agent_sdk.log import logger

from ._tenant_store import TenantStore

HEALTH_CLOSED = "available"
HEALTH_OPEN = "unavailable"
HEALTH_PROBING = "probing"


class BackendHealth:
    """Availability state for one backend with health-driven recovery.

    Args:
        name: Backend identifier (used in logs and snapshots).
        failure_threshold: Consecutive failures before marking unavailable.
        probe_interval_seconds: How long to wait before probing the backend
            again after it has been marked unavailable.
        clock: Monotonic time function (injectable for tests).
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 3,
        probe_interval_seconds: int = 30,
        clock: Callable[[], float] = time.monotonic,
    ):
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if probe_interval_seconds < 0:
            raise ValueError("probe_interval_seconds must be >= 0")
        self.name = name
        self._failure_threshold = failure_threshold
        self._probe_interval_seconds = probe_interval_seconds
        self._clock = clock
        self._lock = threading.Lock()
        self._state = HEALTH_CLOSED
        self._failures = 0
        self._opened_at = 0.0

    def is_available(self) -> bool:
        """Return whether calls to the backend should go to the primary.

        When the probe interval elapsed, the backend flips to ``probing`` and
        the next call is allowed through (its outcome decides recovery).
        """
        with self._lock:
            if self._state == HEALTH_CLOSED:
                return True
            if self._clock() - self._opened_at >= self._probe_interval_seconds:
                self._state = HEALTH_PROBING
                return True
            return False

    def record_success(self) -> None:
        """Record a successful call; marks the backend available again."""
        with self._lock:
            self._failures = 0
            self._state = HEALTH_CLOSED

    def record_failure(self) -> None:
        """Record a failed call; may mark the backend unavailable."""
        with self._lock:
            if self._state == HEALTH_PROBING:
                self._state = HEALTH_OPEN
                self._opened_at = self._clock()
                return
            self._failures += 1
            if self._failures >= self._failure_threshold:
                self._state = HEALTH_OPEN
                self._opened_at = self._clock()
                logger.warning(f"Backend '{self.name}' marked unavailable after "
                               f"{self._failures} consecutive failures")

    def snapshot(self) -> Dict[str, object]:
        """Return a JSON-able summary for monitoring endpoints."""
        with self._lock:
            return {
                "backend": self.name,
                "state": self._state,
                "consecutive_failures": self._failures,
            }


class DegradationController:
    """Registry of :class:`BackendHealth` switches, keyed by backend name."""

    def __init__(self):
        self._backends: Dict[str, BackendHealth] = {}
        self._lock = threading.Lock()

    def register(
        self,
        name: str,
        failure_threshold: int = 3,
        probe_interval_seconds: int = 30,
    ) -> BackendHealth:
        """Register (or replace) a backend health switch."""
        health = BackendHealth(
            name=name,
            failure_threshold=failure_threshold,
            probe_interval_seconds=probe_interval_seconds,
        )
        with self._lock:
            self._backends[name] = health
        return health

    def get(self, name: str) -> Optional[BackendHealth]:
        """Return the health switch for ``name`` (None when unregistered)."""
        with self._lock:
            return self._backends.get(name)

    def allow(self, name: str) -> bool:
        """Whether the primary backend may be tried; unregistered → True."""
        health = self.get(name)
        return health.is_available() if health is not None else True

    def report_success(self, name: str) -> None:
        """Report a successful primary call (no-op when unregistered)."""
        health = self.get(name)
        if health is not None:
            health.record_success()

    def report_failure(self, name: str) -> None:
        """Report a failed primary call (no-op when unregistered)."""
        health = self.get(name)
        if health is not None:
            health.record_failure()

    def snapshot(self) -> Dict[str, Dict[str, object]]:
        """Snapshot of every registered backend, for /healthz style output."""
        with self._lock:
            backends = list(self._backends.values())
        return {health.name: health.snapshot() for health in backends}


_CONTROLLER: Optional[DegradationController] = None


def get_degradation_controller() -> DegradationController:
    """Return the process-wide degradation controller (created on first use)."""
    global _CONTROLLER
    if _CONTROLLER is None:
        _CONTROLLER = DegradationController()
    return _CONTROLLER


class TenantStoreWithFallback(TenantStore):
    """``TenantStore`` wrapper: primary store with transparent fallback.

    Every operation first consults the :class:`DegradationController`. When
    the primary is allowed, it is attempted; a failure reports to the
    controller and retries against the fallback. While the primary is marked
    unavailable, operations go straight to the fallback.

    Args:
        primary: The production store (SQL / Redis).
        fallback: The degraded store (typically :class:`InMemoryTenantStore`).
        backend_name: Controller key for the primary backend.
        controller: Explicit controller; defaults to the process-wide one.
    """

    def __init__(
        self,
        primary: TenantStore,
        fallback: TenantStore,
        backend_name: str = "tenant_store",
        controller: Optional[DegradationController] = None,
    ):
        self._primary = primary
        self._fallback = fallback
        self._backend_name = backend_name
        self._controller = controller or get_degradation_controller()

    @property
    def degraded(self) -> bool:
        """True when the primary is currently marked unavailable."""
        health = self._controller.get(self._backend_name)
        if health is None:
            return False
        snapshot = health.snapshot()
        return snapshot["state"] != HEALTH_CLOSED

    async def _call(self, operation: str, primary_coro_factory, fallback_coro_factory):
        """Run one store operation with degradation policy applied."""
        if self._controller.allow(self._backend_name):
            try:
                result = await primary_coro_factory()
                self._controller.report_success(self._backend_name)
                return result
            except Exception as e:
                self._controller.report_failure(self._backend_name)
                logger.error(f"Backend '{self._backend_name}' failed on {operation} "
                             f"({type(e).__name__}: {e}); degrading to fallback")
        return await fallback_coro_factory()

    async def get_tenant(self, tenant_id: str):
        return await self._call(
            "get_tenant",
            lambda: self._primary.get_tenant(tenant_id),
            lambda: self._fallback.get_tenant(tenant_id),
        )

    async def list_tenants(self, skip: int = 0, limit: int = 100, active_only: bool = True):
        return await self._call(
            "list_tenants",
            lambda: self._primary.list_tenants(skip=skip, limit=limit, active_only=active_only),
            lambda: self._fallback.list_tenants(skip=skip, limit=limit, active_only=active_only),
        )

    async def create_tenant(self, tenant):
        return await self._call(
            "create_tenant",
            lambda: self._primary.create_tenant(tenant),
            lambda: self._fallback.create_tenant(tenant),
        )

    async def update_tenant(self, tenant):
        return await self._call(
            "update_tenant",
            lambda: self._primary.update_tenant(tenant),
            lambda: self._fallback.update_tenant(tenant),
        )

    async def delete_tenant(self, tenant_id: str):
        return await self._call(
            "delete_tenant",
            lambda: self._primary.delete_tenant(tenant_id),
            lambda: self._fallback.delete_tenant(tenant_id),
        )

    async def tenant_exists(self, tenant_id: str):
        return await self._call(
            "tenant_exists",
            lambda: self._primary.tenant_exists(tenant_id),
            lambda: self._fallback.tenant_exists(tenant_id),
        )
