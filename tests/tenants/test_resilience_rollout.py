# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Unit tests for phase-5 operations: resilience (circuit breaker + model
fallback), storage degradation, canary rollout/rollback and capacity."""

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace

import pytest

from trpc_agent_sdk.events import Event
from trpc_agent_sdk.sessions import InMemorySessionService
from trpc_agent_sdk.tenants import (
    BackendHealth,
    CapacityReport,
    ConfigHistoryEntry,
    ConfigRolloutManager,
    DegradationController,
    InMemoryTenantStore,
    InMemoryConfigHistory,
    TenantAwareSessionService,
    TenantConfig,
    TenantContext,
    TenantRunner,
    TenantStoreWithFallback,
    TenantToolResilienceHooks,
    ToolCircuitBreaker,
    budget_headroom,
    capacity_plan,
    nodes_for_qps,
    project_month_end_usage,
    user_bucket,
)
from trpc_agent_sdk.tenants._tenant_degradation import HEALTH_OPEN, HEALTH_PROBING
from trpc_agent_sdk.tenants._tenant_resilience import (
    _is_short_circuit_response,
    CIRCUIT_CLOSED,
    CIRCUIT_HALF_OPEN,
    CIRCUIT_OPEN,
)
from trpc_agent_sdk.types import Content, Part


def make_context(model_failure_action: str = "error", tool_failure_policy: str = "closed") -> TenantContext:
    tenant = TenantConfig(tenant_id="acme", name="ACME").to_tenant()
    tenant.resilience_config.model_failure_action = model_failure_action
    tenant.resilience_config.tool_failure_policy = tool_failure_policy
    return TenantContext(tenant)


def make_text_event(text: str) -> Event:
    return Event(author="assistant", content=Content(parts=[Part.from_text(text=text)]), partial=False)


class TestToolCircuitBreaker:

    def _breaker(self, threshold=3, reset_seconds=60, clock=None):
        kwargs = {}
        if clock is not None:
            kwargs["clock"] = clock
        return ToolCircuitBreaker(failure_threshold=threshold, reset_seconds=reset_seconds, **kwargs)

    def test_starts_closed_and_allows(self):
        breaker = self._breaker()
        assert breaker.state("search") == CIRCUIT_CLOSED
        assert breaker.allow("search") is True

    def test_opens_after_consecutive_failures(self):
        breaker = self._breaker(threshold=3)
        for _ in range(2):
            breaker.record_failure("search")
        assert breaker.state("search") == CIRCUIT_CLOSED
        breaker.record_failure("search")
        assert breaker.state("search") == CIRCUIT_OPEN
        assert breaker.allow("search") is False

    def test_success_resets_failure_count(self):
        breaker = self._breaker(threshold=3)
        breaker.record_failure("search")
        breaker.record_failure("search")
        breaker.record_success("search")
        breaker.record_failure("search")
        breaker.record_failure("search")
        assert breaker.state("search") == CIRCUIT_CLOSED

    def test_probe_after_reset_window(self):
        now = 1000.0
        clock = lambda: now  # noqa: E731
        breaker = self._breaker(threshold=1, reset_seconds=60, clock=clock)
        breaker.record_failure("search")
        assert breaker.allow("search") is False

        now += 61
        assert breaker.allow("search") is True
        assert breaker.state("search") == CIRCUIT_HALF_OPEN

    def test_probe_success_closes(self):
        now = 1000.0
        clock = lambda: now  # noqa: E731
        breaker = self._breaker(threshold=1, reset_seconds=60, clock=clock)
        breaker.record_failure("search")
        now += 61
        assert breaker.allow("search") is True  # half-open probe
        breaker.record_success("search")
        assert breaker.state("search") == CIRCUIT_CLOSED

    def test_probe_failure_reopens(self):
        now = 1000.0
        clock = lambda: now  # noqa: E731
        breaker = self._breaker(threshold=1, reset_seconds=60, clock=clock)
        breaker.record_failure("search")
        now += 61
        breaker.allow("search")  # probe allowed
        breaker.record_failure("search")
        assert breaker.state("search") == CIRCUIT_OPEN
        # Restarted reset window: still denied.
        now += 10
        assert breaker.allow("search") is False

    def test_tools_are_tracked_independently(self):
        breaker = self._breaker(threshold=1)
        breaker.record_failure("search")
        assert breaker.allow("search") is False
        assert breaker.allow("calculator") is True

    def test_invalid_construction(self):
        with pytest.raises(ValueError):
            ToolCircuitBreaker(failure_threshold=0)
        with pytest.raises(ValueError):
            ToolCircuitBreaker(reset_seconds=-1)


class TestTenantToolResilienceHooks:

    def _hooks(self, breaker, policy="closed", audit=None):
        return TenantToolResilienceHooks("acme", breaker=breaker, tool_failure_policy=policy, audit_logger=audit)

    async def test_open_circuit_short_circuits_fail_closed(self):
        breaker = ToolCircuitBreaker(failure_threshold=1)
        breaker.record_failure("search")
        hooks = self._hooks(breaker)
        callback = hooks.before_tool_callback()
        response = await callback(SimpleNamespace(invocation_id="i1"), SimpleNamespace(name="search"), {}, None)
        assert response == {"error": "Tool 'search' is temporarily unavailable, please retry later"}

    async def test_closed_circuit_passes_through(self):
        hooks = self._hooks(ToolCircuitBreaker())
        callback = hooks.before_tool_callback()
        assert await callback(SimpleNamespace(), SimpleNamespace(name="search"), {}, None) is None

    async def test_fail_open_never_blocks(self):
        breaker = ToolCircuitBreaker(failure_threshold=1)
        breaker.record_failure("search")
        hooks = self._hooks(breaker, policy="open")
        callback = hooks.before_tool_callback()
        assert await callback(SimpleNamespace(), SimpleNamespace(name="search"), {}, None) is None

    async def test_after_tool_feeds_breaker(self):
        breaker = ToolCircuitBreaker(failure_threshold=2)
        hooks = self._hooks(breaker)
        after = hooks.after_tool_callback()
        tool = SimpleNamespace(name="search")
        await after(SimpleNamespace(), tool, {}, {"error": "boom"})
        assert breaker.state("search") == CIRCUIT_CLOSED
        await after(SimpleNamespace(), tool, {}, {"error": "boom again"})
        assert breaker.state("search") == CIRCUIT_OPEN
        await after(SimpleNamespace(), tool, {}, {"result": "ok"})
        assert breaker.state("search") == CIRCUIT_CLOSED

    async def test_governance_denials_do_not_open_breaker(self):
        breaker = ToolCircuitBreaker(failure_threshold=1)
        hooks = self._hooks(breaker)
        after = hooks.after_tool_callback()
        tool = SimpleNamespace(name="dangerous_tool")
        for _ in range(5):
            await after(SimpleNamespace(), tool, {}, {"error": "Tool 'dangerous_tool' is not allowed for this tenant"})
            await after(SimpleNamespace(), tool, {},
                        {"error": "Tool 'dangerous_tool' is dangerous and requires confirmation"})
        assert breaker.state("dangerous_tool") == CIRCUIT_CLOSED

    async def test_breaker_own_reply_is_not_counted(self):
        assert _is_short_circuit_response({"error": "Tool 'x' is temporarily unavailable, please retry later"})
        assert _is_short_circuit_response({"error": "Tool 'x' is not allowed for this tenant"})
        assert not _is_short_circuit_response({"error": "connection refused"})

    async def test_invalid_policy_rejected(self):
        with pytest.raises(ValueError):
            self._hooks(ToolCircuitBreaker(), policy="bubble")

    async def test_circuit_open_writes_audit(self):
        entries = []

        async def writer(data: dict) -> None:
            entries.append(data)

        from trpc_agent_sdk.tenants import TenantAuditLogger

        audit = TenantAuditLogger("acme", writer=writer)
        breaker = ToolCircuitBreaker(failure_threshold=1)
        breaker.record_failure("search")
        hooks = self._hooks(breaker, audit=audit)
        await hooks.before_tool_callback()(SimpleNamespace(), SimpleNamespace(name="search"), {}, None)
        assert entries[0]["decision"] == "tool_circuit_open"


class TestModelFailurePolicy:

    class _FailingBaseRunner:

        def run_async(self, **kwargs):

            async def gen():
                yield make_text_event("partial")
                raise RuntimeError("model exploded")

            return gen()

    async def test_fallback_action_degrades_instead_of_raising(self):
        context = make_context(model_failure_action="fallback")
        runner = TenantRunner(context, self._FailingBaseRunner(),
                              TenantAwareSessionService(context, InMemorySessionService()))
        events = [event async for event in runner.run_async(user_id="u1", session_id="s1", new_message=None)]
        assert [e.get_text()
                for e in events] == ["partial", "The assistant is temporarily unavailable. Please try again later."]
        assert events[-1].turn_complete is True

    async def test_error_action_still_raises(self):
        context = make_context(model_failure_action="error")
        runner = TenantRunner(context, self._FailingBaseRunner(),
                              TenantAwareSessionService(context, InMemorySessionService()))
        with pytest.raises(RuntimeError):
            async for _ in runner.run_async(user_id="u1", session_id="s1", new_message=None):
                pass


class _FlakyStore(InMemoryTenantStore):
    """Primary store that always fails, to exercise the degradation path."""

    async def create_tenant(self, tenant):
        raise ConnectionError("redis is down")

    async def get_tenant(self, tenant_id):
        raise ConnectionError("redis is down")


class TestBackendHealth:

    def test_threshold_marks_unavailable(self):
        health = BackendHealth("redis", failure_threshold=2, probe_interval_seconds=30)
        health.record_failure()
        assert health.is_available() is True
        health.record_failure()
        assert health.snapshot() == {
            "backend": "redis",
            "state": HEALTH_OPEN,
            "consecutive_failures": 2,
        }
        assert health.is_available() is False

    def test_probe_window_recovers(self):
        now = 0.0
        health = BackendHealth("redis", failure_threshold=1, probe_interval_seconds=30, clock=lambda: now)
        health.record_failure()
        assert health.is_available() is False
        now += 31
        assert health.is_available() is True
        assert health.snapshot()["state"] == HEALTH_PROBING
        health.record_success()
        assert health.snapshot()["state"] == "available"

    def test_failed_probe_reopens(self):
        now = 0.0
        health = BackendHealth("redis", failure_threshold=1, probe_interval_seconds=30, clock=lambda: now)
        health.record_failure()
        now += 31
        health.is_available()  # probe
        health.record_failure()
        assert health.snapshot()["state"] == HEALTH_OPEN


class TestDegradationController:

    def test_unregistered_backend_defaults_to_allow(self):
        controller = DegradationController()
        assert controller.allow("unknown") is True
        controller.report_failure("unknown")  # no-op, must not raise

    def test_register_and_snapshot(self):
        controller = DegradationController()
        controller.register("redis", failure_threshold=1, probe_interval_seconds=10)
        controller.report_failure("redis")
        snapshot = controller.snapshot()
        assert snapshot["redis"]["state"] == HEALTH_OPEN
        assert controller.allow("redis") is False


class TestTenantStoreWithFallback:

    async def test_primary_used_when_healthy(self):
        primary = InMemoryTenantStore()
        fallback = InMemoryTenantStore()
        controller = DegradationController()
        controller.register("store", failure_threshold=1)
        store = TenantStoreWithFallback(primary, fallback, backend_name="store", controller=controller)

        tenant = TenantConfig(tenant_id="acme", name="ACME").to_tenant()
        await store.create_tenant(tenant)
        assert await primary.get_tenant("acme") is not None
        assert await fallback.get_tenant("acme") is None

    async def test_fails_over_to_fallback(self):
        primary = _FlakyStore()
        fallback = InMemoryTenantStore()
        controller = DegradationController()
        controller.register("store", failure_threshold=2, probe_interval_seconds=0)
        store = TenantStoreWithFallback(primary, fallback, backend_name="store", controller=controller)

        tenant = TenantConfig(tenant_id="acme", name="ACME").to_tenant()
        await store.create_tenant(tenant)
        # Writes degraded into the fallback after the primary failed.
        assert await fallback.get_tenant("acme") is not None
        # Reads are also served from the fallback while unhealthy.
        assert (await store.get_tenant("acme")).tenant_id == "acme"
        assert controller.snapshot()["store"]["state"] == HEALTH_OPEN
        assert store.degraded is True

    async def test_recovery_after_probe_window(self):

        class _RecoveringStore(InMemoryTenantStore):

            def __init__(self):
                super().__init__()
                self.fail = True

            async def get_tenant(self, tenant_id):
                if self.fail:
                    raise ConnectionError("down")
                return await super().get_tenant(tenant_id)

        primary = _RecoveringStore()
        fallback = InMemoryTenantStore()
        controller = DegradationController()
        controller.register("store", failure_threshold=1, probe_interval_seconds=0)
        store = TenantStoreWithFallback(primary, fallback, backend_name="store", controller=controller)
        tenant = TenantConfig(tenant_id="acme", name="ACME").to_tenant()
        # Both stores hold the tenant (create_tenant is not overridden).
        await primary.create_tenant(tenant)
        await fallback.create_tenant(tenant)

        assert (await store.get_tenant("acme")) is not None  # probe fails -> fallback
        assert controller.allow("store") is True  # probe window elapsed

        primary.fail = False
        assert (await store.get_tenant("acme")) is not None
        assert store.degraded is False
        assert controller.snapshot()["store"]["state"] == "available"


class TestUserBucket:

    def test_deterministic_and_in_range(self):
        for _ in range(20):
            bucket = user_bucket("acme", "u1")
            assert bucket == user_bucket("acme", "u1")
            assert 0 <= bucket < 100

    def test_differs_across_tenants_and_users(self):
        assert user_bucket("acme", "u1") != user_bucket("globex", "u1")
        assert user_bucket("acme", "u1") != user_bucket("acme", "u2")


class TestConfigRollout:

    def test_publish_and_resolve_without_rollout(self):
        manager = ConfigRolloutManager()
        assert manager.resolve_config_version("acme", "u1", default_version=7) == 7
        assert manager.rollout_state("acme") is None

    def test_canary_routes_deterministically(self):
        manager = ConfigRolloutManager()
        manager.publish("acme", target_version=2, canary_percent=0)
        assert manager.resolve_config_version("acme", "u1", default_version=1) == 1

        manager.set_canary_percent("acme", 100)
        assert manager.resolve_config_version("acme", "u1") == 2

        # Pick a user whose bucket is below a known percent boundary.
        manager2 = ConfigRolloutManager()
        manager2.publish("acme", target_version=2, canary_percent=0)
        low_user = next(u for u in ("u1", "u2", "u3", "u4", "u5") if user_bucket("acme", u) < 50)
        high_user = next(u for u in ("u1", "u2", "u3", "u4", "u5") if user_bucket("acme", u) >= 50)
        manager2.set_canary_percent("acme", 50)
        assert manager2.resolve_config_version("acme", low_user) == 2
        assert manager2.resolve_config_version("acme", high_user) == 1
        assert manager2.rollout_state("acme").canary_percent == 50

    def test_promote_publishes_to_everyone_and_records_history(self):
        history = InMemoryConfigHistory()
        manager = ConfigRolloutManager(history=history)
        manager.publish("acme", target_version=3)
        manager.promote("acme")
        assert manager.resolve_config_version("acme", "any-user") == 3
        assert history.previous_version("acme", before_version=99) == 3
        assert manager.rollout_state("acme").published is True

    def test_rollback_restores_baseline(self):
        history = InMemoryConfigHistory()
        manager = ConfigRolloutManager(history=history)
        manager.publish("acme", target_version=1)
        manager.promote("acme")  # v1 fully live
        manager.publish("acme", target_version=2, canary_percent=25)  # v2 canary
        assert manager.resolve_config_version("acme", "u1", default_version=1) in (1, 2)

        rolled = manager.rollback("acme")
        assert rolled is not None
        assert rolled.target_version == 1
        assert rolled.canary_percent == 100
        assert manager.resolve_config_version("acme", "u1", default_version=1) == 1
        # The rollback itself is recorded so a later publish sees v1 as baseline.
        assert history.previous_version("acme", before_version=99) == 1

    def test_rollback_without_baseline_returns_none(self):
        manager = ConfigRolloutManager()
        assert manager.rollback("acme") is None
        manager.publish("acme", target_version=2)  # no baseline on record
        assert manager.rollback("acme") is None

    def test_invalid_publish_inputs(self):
        manager = ConfigRolloutManager()
        with pytest.raises(ValueError):
            manager.publish("acme", target_version=2, canary_percent=150)
        with pytest.raises(ValueError):
            manager.set_canary_percent("missing", 10)

    def test_history_is_bounded(self):
        history = InMemoryConfigHistory(max_entries_per_tenant=2)
        for version in range(1, 6):
            history.record(ConfigHistoryEntry(tenant_id="acme", version=version, canary_percent=100, published=True))
        assert len(history.snapshot()["acme"]) == 2
        assert history.previous_version("acme", before_version=99) == 5


class TestCapacity:

    def test_nodes_for_qps(self):
        assert nodes_for_qps(0, 100) == 1
        assert nodes_for_qps(100, 100) == 2  # 1 * 1.3 -> ceil 2
        assert nodes_for_qps(200, 100, headroom_percent=0) == 2
        assert nodes_for_qps(201, 100, headroom_percent=0) == 3
        with pytest.raises(ValueError):
            nodes_for_qps(10, 0)
        with pytest.raises(ValueError):
            nodes_for_qps(-1, 100)

    def test_project_month_end_usage(self):
        mid_month = dt.date(2026, 9, 15)
        assert project_month_end_usage(300.0, today=mid_month) == pytest.approx(600.0)
        month_end = dt.date(2026, 9, 30)
        assert project_month_end_usage(123.0, today=month_end) == 123.0
        leap = dt.date(2024, 2, 14)
        assert project_month_end_usage(140.0, today=leap) == pytest.approx(290.0)  # 140/14 * 29

    def test_budget_headroom(self):
        mid_month = dt.date(2026, 9, 15)
        assert budget_headroom(1000.0, 300.0, today=mid_month) == pytest.approx(400.0)
        assert budget_headroom(100.0, 300.0, today=mid_month) < 0

    def test_capacity_plan_report(self):
        report = capacity_plan(
            peak_qps=250.0,
            per_node_qps=100.0,
            tenant_usage=[
                {
                    "tenant_id": "acme",
                    "requests_mtd": 10000,
                    "cost_usd_mtd": 30.0,
                    "monthly_budget_usd": 100.0,
                },
                {
                    "tenant_id": "globex",
                    "requests_mtd": 5000,
                    "cost_usd_mtd": 90.0,
                    "monthly_budget_usd": 100.0,
                },
            ],
            today=dt.date(2026, 9, 15),
        )
        assert isinstance(report, CapacityReport)
        assert report.nodes_required == 4  # 2.5 * 1.3 = 3.25 -> 4
        by_id = {row.tenant_id: row for row in report.tenants}
        assert by_id["acme"].projected_month_end_cost_usd == pytest.approx(60.0)
        assert by_id["acme"].budget_headroom_usd == pytest.approx(40.0)
        assert by_id["globex"].projected_month_end_cost_usd == pytest.approx(180.0)
        assert report.tenants_over_budget() == ["globex"]
        assert report.summary()["nodes_required"] == 4
        assert report.summary()["tenants_over_budget"] == ["globex"]
