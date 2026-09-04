# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Unit tests for tenant governance (budget, tool permissions, audit)."""

from __future__ import annotations

import pytest

from trpc_agent_sdk.abc import FilterResult
from trpc_agent_sdk.models import OpenAIModel
from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.sessions import InMemorySessionService
from trpc_agent_sdk.tenants import (
    AuditLog,
    BudgetExceededError,
    InMemoryTenantStore,
    Tenant,
    TenantAuditLogger,
    TenantConfig,
    TenantContext,
    TenantGovernanceFilter,
    TenantToolGovernor,
    TenantUsageTracker,
    create_tenant_runner,
    check_im_user_allowed,
    mask_secrets,
)
from trpc_agent_sdk.tenants._audit import DECISION_ALLOW, DECISION_COMPLETE, DECISION_ERROR

REDIS_UNREACHABLE = "redis://127.0.0.1:1/0"


def make_tenant(**permissions) -> Tenant:
    config = TenantConfig(tenant_id="acme", name="ACME", tool_permissions=permissions)
    return config.to_tenant()


@pytest.fixture
def tenant() -> Tenant:
    return make_tenant(
        allowed_tools=["search", "weather", "shell", "deploy"],
        blocked_tools=["shell"],
        dangerous_tools=["deploy"],
        tool_budget_monthly_usd=10.0,
    )


@pytest.fixture
def context(tenant: Tenant) -> TenantContext:
    return TenantContext(tenant)


@pytest.fixture
async def tracker() -> TenantUsageTracker:
    """Tracker against an unreachable Redis -> exercises the in-memory fallback."""
    return TenantUsageTracker(redis_url=REDIS_UNREACHABLE)


class TestMaskSecrets:

    def test_masks_secret_keys(self):
        data = {"api_key": "sk-1234567890abcd", "password": "hunter2hunter2", "name": "ok"}
        masked = mask_secrets(data)
        assert masked["api_key"].startswith("sk-1") and masked["api_key"].endswith("abcd")
        assert "hunter2" not in masked["password"]
        assert masked["name"] == "ok"

    def test_masks_short_values_completely(self):
        assert mask_secrets({"token": "abc"})["token"] == "***SANITIZED***"

    def test_masks_nested_and_lists(self):
        data = {"outer": {"secret": "supersecretvalue"}, "items": [{"api_key": "sk-1234567890"}]}
        masked = mask_secrets(data)
        assert "supersecretvalue" not in masked["outer"]["secret"]
        assert "sk-1234567890" not in masked["items"][0]["api_key"]

    def test_does_not_mutate_input(self):
        data = {"api_key": "sk-1234567890abcd"}
        mask_secrets(data)
        assert data["api_key"] == "sk-1234567890abcd"


class TestAuditLog:

    def test_schema_contains_required_fields(self):
        entry = AuditLog(tenant_id="acme", channel="telegram", user_id="u1", session_id="s1")
        data = entry.to_dict()
        for field_name in (
                "tenant_id",
                "channel",
                "user_id",
                "session_id",
                "agent_name",
                "tool_name",
                "decision",
                "latency_ms",
                "error_type",
                "cost_usd",
                "trace_id",
        ):
            assert field_name in data

    async def test_callable_writer_receives_masked_details(self):
        captured = []

        async def writer(data: dict) -> None:
            captured.append(data)

        audit = TenantAuditLogger("acme", writer=writer)
        await audit.log_event(decision=DECISION_ALLOW, details={"api_key": "sk-1234567890abcd"})
        assert captured[0]["tenant_id"] == "acme"
        assert "sk-1234567890abcd" not in str(captured[0]["details"])

    async def test_backend_writer_save_audit_log(self):
        calls = []

        class FakeRouter:

            async def save_audit_log(self, tenant_id: str, data: dict) -> None:
                calls.append((tenant_id, data))

        audit = TenantAuditLogger("acme", writer=FakeRouter())
        await audit.log_event(decision=DECISION_ALLOW)
        assert calls[0][0] == "acme"
        assert calls[0][1]["decision"] == DECISION_ALLOW

    async def test_writer_failure_does_not_raise(self):

        async def broken_writer(data: dict) -> None:
            raise RuntimeError("backend down")

        audit = TenantAuditLogger("acme", writer=broken_writer)
        await audit.log_event(decision=DECISION_ALLOW)  # must not raise


class TestTenantUsageTracker:

    async def test_record_and_read_usage(self, context: TenantContext, tracker: TenantUsageTracker):
        await tracker.record_usage(context, requests=2, input_tokens=100, estimated_cost_usd=0.5)
        usage = await tracker.get_usage(context)
        assert usage["requests"] == 2.0
        assert usage["input_tokens"] == 100.0
        assert usage["cost_usd"] == pytest.approx(0.5)

    async def test_budget_allowed_under_limit(self, context: TenantContext, tracker: TenantUsageTracker):
        await tracker.record_usage(context, estimated_cost_usd=5.0)
        check = await tracker.check_budget(context)
        assert check.allowed is True
        assert check.limit_usd == 10.0

    async def test_budget_denied_when_exhausted(self, context: TenantContext, tracker: TenantUsageTracker):
        await tracker.record_usage(context, estimated_cost_usd=10.0)
        check = await tracker.check_budget(context)
        assert check.allowed is False
        assert check.used_usd == pytest.approx(10.0)


class TestTenantToolGovernor:

    async def test_tool_not_in_whitelist_denied(self, context: TenantContext):
        governor = TenantToolGovernor(context)
        decision = await governor.check_tool("unknown_tool", {})
        assert decision.allowed is False
        assert "not allowed" in decision.response["error"]

    async def test_blocked_tool_denied(self, context: TenantContext):
        governor = TenantToolGovernor(context)
        decision = await governor.check_tool("shell", {"cmd": "ls"})
        assert decision.allowed is False

    async def test_allowed_tool_passes(self, context: TenantContext):
        governor = TenantToolGovernor(context)
        decision = await governor.check_tool("search", {})
        assert decision.allowed is True
        assert decision.response is None

    async def test_dangerous_tool_rejected_without_confirmation(self, context: TenantContext):
        governor = TenantToolGovernor(context)
        decision = await governor.check_tool("deploy", {})
        assert decision.allowed is False
        assert "dangerous" in decision.response["error"]

    async def test_dangerous_tool_allowed_after_confirmation(self, context: TenantContext):
        confirmed = []

        async def confirm(tool_name: str, args: dict) -> bool:
            confirmed.append(tool_name)
            return True

        governor = TenantToolGovernor(context, confirm_dangerous_tool=confirm)
        decision = await governor.check_tool("deploy", {})
        assert decision.allowed is True
        assert confirmed == ["deploy"]

    async def test_before_tool_callback_short_circuits_denied_call(self, context: TenantContext):
        governor = TenantToolGovernor(context)
        callback = governor.before_tool_callback()

        class FakeTool:
            name = "unknown_tool"

        result = await callback(None, FakeTool(), {}, None)
        assert result is not None and "not allowed" in result["error"]

        class FakeAllowedTool:
            name = "search"

        assert await callback(None, FakeAllowedTool(), {}, None) is None


class TestTenantGovernanceFilter:

    async def test_before_denies_when_budget_exhausted(self, context: TenantContext, tracker: TenantUsageTracker):
        await tracker.record_usage(context, estimated_cost_usd=10.0)
        audit_entries = []
        audit = TenantAuditLogger("acme", writer=lambda d: audit_entries.append(d))
        governance = TenantGovernanceFilter(context, usage_tracker=tracker, audit_logger=audit)

        rsp = FilterResult()
        await governance._before(None, None, rsp)
        assert isinstance(rsp.error, BudgetExceededError)
        assert rsp.is_continue is False
        assert audit_entries[0]["decision"] == "deny_budget"

    async def test_before_allows_under_budget(self, context: TenantContext, tracker: TenantUsageTracker):
        governance = TenantGovernanceFilter(context, usage_tracker=tracker)
        rsp = FilterResult()
        await governance._before(None, None, rsp)
        assert rsp.error is None
        assert rsp.is_continue is True

    async def test_after_records_error_and_latency(self, context: TenantContext):
        audit_entries = []

        async def writer(data: dict) -> None:
            audit_entries.append(data)

        governance = TenantGovernanceFilter(context, audit_logger=TenantAuditLogger("acme", writer=writer))
        await governance._before(None, None, FilterResult())
        rsp = FilterResult(error=ValueError("boom"))
        await governance._after(None, None, rsp)
        assert audit_entries[-1]["decision"] == DECISION_ERROR
        assert audit_entries[-1]["error_type"] == "ValueError"

    async def test_after_records_complete(self, context: TenantContext):
        audit_entries = []

        async def writer(data: dict) -> None:
            audit_entries.append(data)

        governance = TenantGovernanceFilter(context, audit_logger=TenantAuditLogger("acme", writer=writer))
        await governance._before(None, None, FilterResult())
        await governance._after(None, None, FilterResult())
        assert audit_entries[-1]["decision"] == DECISION_COMPLETE


class TestCheckImUserAllowed:

    def test_empty_allowlist_accepts_everyone(self, context: TenantContext):
        assert check_im_user_allowed(context, "wecom", "any_user") is False  # no wecom config
        assert check_im_user_allowed(context, "telegram", "any_user") is False

    def test_channel_allowlist(self, tenant: Tenant):
        from trpc_agent_sdk.tenants import ChannelConfig

        tenant.channel_configs["telegram"] = ChannelConfig(channel_type="telegram", allowed_users=["alice", "bob"])
        context = TenantContext(tenant)
        assert check_im_user_allowed(context, "telegram", "alice") is True
        assert check_im_user_allowed(context, "telegram", "mallory") is False

    def test_disabled_channel_rejects(self, tenant: Tenant):
        from trpc_agent_sdk.tenants import ChannelConfig

        tenant.channel_configs["telegram"] = ChannelConfig(channel_type="telegram", enabled=False)
        context = TenantContext(tenant)
        assert check_im_user_allowed(context, "telegram", "alice") is False


class TestCreateTenantRunnerWiring:

    async def test_runner_wires_governance_into_agent(self, tenant: Tenant):
        store = InMemoryTenantStore()
        await store.create_tenant(tenant)
        context = TenantContext(tenant)

        agent = LlmAgent(
            name="assistant_acme",
            model=OpenAIModel(model_name="gpt-4o-mini", api_key="sk-test"),
            tools=[],
        )
        tracker = TenantUsageTracker(redis_url=REDIS_UNREACHABLE)
        runner = await create_tenant_runner(context, agent, InMemorySessionService(), usage_tracker=tracker)

        assert runner.get_tenant_id() == "acme"
        # Governance filter appended to the agent's filter chain
        governance_filters = [f for f in agent.filters if isinstance(f, TenantGovernanceFilter)]
        assert len(governance_filters) == 1
        # Tool governance callback wired into the agent's before_tool_callback
        # (list also contains the resilience circuit-breaker callback added in
        # phase 5 and the telemetry timing callback added in phase 3)
        callbacks = agent.before_tool_callback
        assert isinstance(callbacks, list) and len(callbacks) == 3
