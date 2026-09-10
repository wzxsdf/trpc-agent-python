# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Tenant governance: budget limits, tool permission enforcement, IM user
permission checks and a pluggable SDK filter.

Components:

- :class:`TenantUsageTracker` — per-tenant monthly usage accounting
  (requests / tokens / estimated cost) backed by Redis with an in-memory
  fallback, used to enforce ``tool_budget_monthly_usd``.
- :class:`TenantToolGovernor` — runtime tool permission enforcement
  (whitelist / blocked / dangerous-tool confirmation) exposed as an
  ``LlmAgent.before_tool_callback``.
- :class:`TenantGovernanceFilter` — SDK ``AGENT``-level filter enforcing the
  budget before each run and recording usage/audit entries after it.
- :func:`check_im_user_allowed` — IM user permission check based on the
  channel's ``allowed_users`` configuration.
"""

import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional

from trpc_agent_sdk.abc import FilterResult
from trpc_agent_sdk.abc import FilterType
from trpc_agent_sdk.filter import BaseFilter
from trpc_agent_sdk.log import logger

from ._audit import (
    DECISION_ALLOW,
    DECISION_COMPLETE,
    DECISION_DANGEROUS_CONFIRMED,
    DECISION_DANGEROUS_REJECTED,
    DECISION_DENY_BUDGET,
    DECISION_DENY_TOOL,
    DECISION_ERROR,
    TenantAuditLogger,
    mask_secrets,
)
from ._im_transport import RedisFallbackMixin
from ._tenant_context import TenantContext
from ._tenant_telemetry import get_tenant_metrics

ConfirmCallback = Callable[[str, Dict[str, Any]], Awaitable[bool]]
"""Async callable(tool_name, args) -> bool deciding whether a dangerous tool
call may proceed."""


class BudgetExceededError(Exception):
    """Raised when a tenant has exhausted its configured monthly budget."""


@dataclass
class BudgetCheck:
    """Result of a budget check."""

    allowed: bool
    used_usd: float = 0.0
    limit_usd: float = 0.0
    reason: str = ""


@dataclass
class ToolDecision:
    """Result of a tool governance check."""

    allowed: bool
    response: Optional[Dict[str, Any]] = None
    """Response dict to substitute for the tool result when not allowed."""
    decision: str = DECISION_ALLOW
    """Audit decision value (see ``_audit`` constants)."""


class TenantUsageTracker(RedisFallbackMixin):
    """Per-tenant monthly usage tracker with budget enforcement.

    Uses Redis hash counters keyed by ``tenant_usage:{tenant_id}:{YYYYMM}``
    so all nodes share one view of the budget. When Redis is unavailable the
    tracker degrades to per-process in-memory counters (same pattern as
    :class:`~trpc_agent_sdk.tenants.MessageDeduplicator`) and periodically
    retries the shared backend.

    Cost is accumulated in micro-USD integers to avoid float drift.
    """

    _KEY_PATTERNS = ("requests", "input_tokens", "output_tokens", "cost_micros")

    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        """Initialize the usage tracker.

        Args:
            redis_url: Redis URL for shared usage counters.
        """
        try:
            import redis.asyncio as redis  # noqa: F401 -- availability check
        except ImportError:
            raise ImportError("TenantUsageTracker requires 'redis' package. "
                              "Install with: pip install redis")

        self._redis_url = redis_url
        self._redis = None
        self._init_fallback_state()
        # In-memory fallback: {month_key: {"requests": int, "cost_micros": int, ...}}
        self._local_usage: Dict[str, Dict[str, int]] = {}

    async def _get_redis(self):
        """Lazy initialization of the Redis connection."""
        if self._redis is None:
            import redis.asyncio as redis

            self._redis = await redis.from_url(self._redis_url, decode_responses=True)
        return self._redis

    @staticmethod
    def _month_key(tenant_id: str) -> str:
        """Build the current-month counter key for a tenant."""
        return f"tenant_usage:{tenant_id}:{time.strftime('%Y%m')}"

    async def check_budget(self, tenant_context: TenantContext) -> BudgetCheck:
        """Check whether the tenant still has budget left.

        Args:
            tenant_context: Tenant to check.

        Returns:
            BudgetCheck with ``allowed`` False when the accumulated estimated
            cost reached ``tool_budget_monthly_usd``.
        """
        limit_usd = tenant_context.tenant.tool_permissions.tool_budget_monthly_usd
        usage = await self.get_usage(tenant_context)
        used_usd = usage.get("cost_usd", 0.0)
        allowed = used_usd < limit_usd
        return BudgetCheck(
            allowed=allowed,
            used_usd=used_usd,
            limit_usd=limit_usd,
            reason="" if allowed else "monthly tool budget exhausted",
        )

    async def record_usage(
        self,
        tenant_context: TenantContext,
        *,
        requests: int = 1,
        input_tokens: int = 0,
        output_tokens: int = 0,
        estimated_cost_usd: float = 0.0,
    ) -> None:
        """Accumulate usage for the current month.

        Args:
            tenant_context: Tenant the usage belongs to.
            requests: Number of agent runs to add.
            input_tokens: Prompt tokens consumed.
            output_tokens: Completion tokens consumed.
            estimated_cost_usd: Estimated cost in USD for this call.
        """
        cost_micros = int(round(estimated_cost_usd * 1_000_000))
        key = self._month_key(tenant_context.tenant_id)
        increments = {
            "requests": requests,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cost_micros": cost_micros,
        }

        # Mirror usage into the Prometheus metrics registry (explicit token
        # counts only — event-stream tokens are counted in TenantRunner).
        metrics = get_tenant_metrics()
        if estimated_cost_usd:
            metrics.incr("tenant_cost_usd_total", {"tenant_id": tenant_context.tenant_id}, estimated_cost_usd)
        if input_tokens:
            metrics.incr("tenant_tokens_total", {"tenant_id": tenant_context.tenant_id, "type": "input"}, input_tokens)
        if output_tokens:
            metrics.incr("tenant_tokens_total", {
                "tenant_id": tenant_context.tenant_id,
                "type": "output"
            }, output_tokens)

        if not self._redis_down():
            try:
                redis_client = await self._get_redis()
                pipe = redis_client.pipeline()
                for field, value in increments.items():
                    if value:
                        pipe.hincrby(key, field, value)
                pipe.expire(key, 60 * 60 * 24 * 40)  # ~40 days covers a month
                await pipe.execute()
                return
            except Exception as e:
                logger.warning(f"Redis usage tracking unavailable, "
                               f"falling back to in-memory: {e}")
                self._mark_redis_failed()

        local = self._local_usage.setdefault(key, {})
        for field, value in increments.items():
            local[field] = local.get(field, 0) + value

    async def get_usage(self, tenant_context: TenantContext) -> Dict[str, float]:
        """Return current-month usage as ``{requests, input_tokens,
        output_tokens, cost_usd}``."""
        key = self._month_key(tenant_context.tenant_id)
        raw: Dict[str, int] = {}

        if not self._redis_down():
            try:
                redis_client = await self._get_redis()
                data = await redis_client.hgetall(key)
                raw = {k: int(v) for k, v in (data or {}).items()}
            except Exception as e:
                logger.warning(f"Redis usage read unavailable, "
                               f"falling back to in-memory: {e}")
                self._mark_redis_failed()

        if not raw:
            raw = self._local_usage.get(key, {})

        return {
            "requests": float(raw.get("requests", 0)),
            "input_tokens": float(raw.get("input_tokens", 0)),
            "output_tokens": float(raw.get("output_tokens", 0)),
            "cost_usd": raw.get("cost_micros", 0) / 1_000_000,
        }


def check_im_user_allowed(tenant_context: TenantContext, channel_type: str, user_id: str) -> bool:
    """Check whether an IM user may interact with the tenant.

    An empty ``allowed_users`` list means every user of the channel is
    accepted; otherwise the user must be listed.

    Args:
        tenant_context: Tenant context.
        channel_type: Channel the message arrived on (e.g. ``"wecom"``).
        user_id: External IM user identifier.

    Returns:
        True when the user is allowed to talk to this tenant.
    """
    channel_config = tenant_context.tenant.get_channel_config(channel_type)
    if channel_config is None:
        return False
    if not channel_config.enabled:
        return False
    allowed_users = channel_config.allowed_users
    if not allowed_users:
        return True
    return user_id in allowed_users


class TenantToolGovernor:
    """Runtime tool permission enforcement for one tenant.

    Wrap it with :meth:`before_tool_callback` and attach the result to an
    ``LlmAgent``; a non-``None`` callback return short-circuits tool
    execution and substitutes the returned dict as the tool response.
    """

    def __init__(
        self,
        tenant_context: TenantContext,
        confirm_dangerous_tool: Optional[ConfirmCallback] = None,
        audit_logger: Optional[TenantAuditLogger] = None,
    ):
        """Initialize the tool governor.

        Args:
            tenant_context: Tenant whose ``ToolPermissions`` apply.
            confirm_dangerous_tool: Optional async callback invoked for tools
                listed in ``dangerous_tools``; returning False rejects the
                call. When absent, dangerous tools are rejected by default.
            audit_logger: Optional audit logger for allow/deny decisions.
        """
        self._tenant_context = tenant_context
        self._confirm = confirm_dangerous_tool
        self._audit = audit_logger

    async def check_tool(self, tool_name: str, args: Dict[str, Any]) -> ToolDecision:
        """Check a tool call against tenant permissions.

        Args:
            tool_name: Name of the tool being invoked.
            args: Tool call arguments (only used for audit details, masked).

        Returns:
            ToolDecision; ``response`` is set (and the tool skipped) when the
            call is denied.
        """
        masked_details = {"args": mask_secrets(args)} if args else {}

        if not self._tenant_context.is_tool_allowed(tool_name):
            await self._audit_log(DECISION_DENY_TOOL, tool_name, masked_details)
            return ToolDecision(
                allowed=False,
                response={"error": f"Tool '{tool_name}' is not allowed for this tenant"},
                decision=DECISION_DENY_TOOL,
            )

        if self._tenant_context.is_tool_dangerous(tool_name):
            if self._confirm is not None and await self._confirm(tool_name, args):
                await self._audit_log(DECISION_DANGEROUS_CONFIRMED, tool_name, masked_details)
                return ToolDecision(allowed=True, decision=DECISION_DANGEROUS_CONFIRMED)
            await self._audit_log(DECISION_DANGEROUS_REJECTED, tool_name, masked_details)
            return ToolDecision(
                allowed=False,
                response={"error": f"Tool '{tool_name}' is dangerous and requires confirmation"},
                decision=DECISION_DANGEROUS_REJECTED,
            )

        return ToolDecision(allowed=True, decision=DECISION_ALLOW)

    async def _audit_log(self, decision: str, tool_name: str, details: Dict[str, Any]):
        """Write an audit entry when an audit logger is configured."""
        if self._audit is not None:
            await self._audit.log_event(decision=decision, tool_name=tool_name, details=details)

    def before_tool_callback(self):
        """Return an ``LlmAgent``-compatible ``before_tool_callback``.

        The returned callback denies the call (by returning an error dict)
        when governance rejects it, and returns ``None`` otherwise so the
        tool executes normally.
        """

        async def _callback(invocation_context, tool, args: Dict[str, Any], kwargs) -> Optional[Dict[str, Any]]:
            tool_name = getattr(tool, "name", "") or ""
            decision = await self.check_tool(tool_name, args or {})
            if decision.allowed:
                return None
            return decision.response

        return _callback


class TenantGovernanceFilter(BaseFilter):
    """SDK ``AGENT``-level filter enforcing tenant budget and audit.

    Add it to an agent's ``filters`` list (or wire it via
    :func:`create_tenant_runner`) to check the tenant budget before each run
    and record an audit entry with latency afterwards.

    Streaming note: ``_after_every_stream`` captures token usage from events
    carrying ``usage_metadata`` and records it into the usage tracker.
    """

    def __init__(
        self,
        tenant_context: TenantContext,
        usage_tracker: Optional[TenantUsageTracker] = None,
        audit_logger: Optional[TenantAuditLogger] = None,
    ):
        """Initialize the governance filter.

        Args:
            tenant_context: Tenant this filter enforces.
            usage_tracker: Optional usage tracker; when absent the budget
                check is a no-op (always allowed).
            audit_logger: Optional audit logger for run start/complete
                entries.
        """
        super().__init__()
        self._type = FilterType.AGENT
        self._name = "tenant_governance"
        self._tenant_context = tenant_context
        self._usage_tracker = usage_tracker
        self._audit = audit_logger
        self._run_started_at: Optional[float] = None

    async def _before(self, ctx, req: Any, rsp: FilterResult):
        """Enforce the tenant budget before the run starts."""
        if self._usage_tracker is None:
            return
        check = await self._usage_tracker.check_budget(self._tenant_context)
        if not check.allowed:
            if self._audit is not None:
                await self._audit.log_event(
                    decision=DECISION_DENY_BUDGET,
                    error_type="BudgetExceeded",
                    details={
                        "used_usd": check.used_usd,
                        "limit_usd": check.limit_usd
                    },
                )
            rsp.error = BudgetExceededError(check.reason)
            rsp.is_continue = False
            return
        self._run_started_at = time.monotonic()
        if self._audit is not None:
            await self._audit.log_event(decision=DECISION_ALLOW)

    async def _after(self, ctx, req: Any, rsp: FilterResult):
        """Record run completion (latency / error) in the audit log."""
        if self._audit is None:
            return
        latency_ms = 0.0
        if self._run_started_at is not None:
            latency_ms = (time.monotonic() - self._run_started_at) * 1000
            self._run_started_at = None
        if rsp.error is not None:
            await self._audit.log_event(decision=DECISION_ERROR,
                                        latency_ms=latency_ms,
                                        error_type=type(rsp.error).__name__,
                                        details={"error": str(rsp.error)})
        else:
            await self._audit.log_event(decision=DECISION_COMPLETE, latency_ms=latency_ms)

    async def _after_every_stream(self, ctx, req: Any, rsp: FilterResult) -> None:
        """Capture per-event token usage during streaming runs."""
        if self._usage_tracker is None or rsp.rsp is None:
            return
        usage = getattr(rsp.rsp, "usage_metadata", None)
        if usage is None:
            return
        input_tokens = int(getattr(usage, "prompt_token_count", 0) or 0)
        output_tokens = int(getattr(usage, "candidates_token_count", 0) or 0)
        if input_tokens or output_tokens:
            await self._usage_tracker.record_usage(
                self._tenant_context,
                requests=0,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
            )
