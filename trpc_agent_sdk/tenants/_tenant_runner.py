# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Tenant-aware runner wiring.

Tenant isolation comes from :class:`TenantAwareSessionService` (scoped session
keys + tenant state verification). :class:`TenantRunner` adds optional event
sanitization on top of a standard :class:`Runner`.
"""

import time
import uuid
from typing import AsyncGenerator, Optional

from google.genai import types as genai_types
from opentelemetry import trace
from trpc_agent_sdk.agents import BaseAgent, LlmAgent
from trpc_agent_sdk.context import AgentContext
from trpc_agent_sdk.configs import RunConfig
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.log import logger
from trpc_agent_sdk.runners import Runner
from trpc_agent_sdk.sessions import BaseSessionService
from trpc_agent_sdk.types import Content

from ._audit import TenantAuditLogger, mask_secrets
from ._tenant_context import TenantContext
from ._tenant_governance import ConfirmCallback, TenantGovernanceFilter, TenantToolGovernor, TenantUsageTracker
from ._tenant_resilience import TenantToolResilienceHooks, ToolCircuitBreaker
from ._tenant_session_service import TenantAwareSessionService
from ._tenant_telemetry import TRACER, TenantTelemetryHooks, get_tenant_metrics


def _append_callback(existing, callback):
    """Append ``callback`` to an SDK callback field (None / callable / list)."""
    if existing is None:
        return callback
    if isinstance(existing, list):
        return [*existing, callback]
    return [existing, callback]


class TenantRunner:
    """Runner wrapper that scopes execution to one tenant.

    Session isolation is enforced by the wrapped ``TenantAwareSessionService``;
    this class additionally sanitizes events when the tenant audit config
    requires it.
    """

    def __init__(
        self,
        tenant_context: TenantContext,
        base_runner: Runner,
        tenant_session_service: TenantAwareSessionService,
        usage_tracker: Optional[TenantUsageTracker] = None,
        audit_logger: Optional[TenantAuditLogger] = None,
    ):
        """Initialize tenant-aware runner.

        Args:
            tenant_context: Current tenant context.
            base_runner: Underlying standard runner.
            tenant_session_service: The tenant-scoped session service used by
                ``base_runner``.
            usage_tracker: Optional usage tracker; when present each run
                counts against the tenant's monthly budget.
            audit_logger: Optional audit logger for run-level entries.
        """
        self._tenant_context = tenant_context
        self._base_runner = base_runner
        self._tenant_session_service = tenant_session_service
        self._usage_tracker = usage_tracker
        self._audit = audit_logger
        self._tenant_id = tenant_context.tenant_id

    async def run_async(
        self,
        *,
        user_id: str,
        session_id: str,
        new_message: Content | list[Content],
        run_config: RunConfig = RunConfig(),
        agent_context: Optional[AgentContext] = None,
    ) -> AsyncGenerator[Event, None]:
        """Run the agent with tenant isolation.

        Args:
            user_id: User identifier.
            session_id: Client-visible session identifier (scoped internally).
            new_message: New message content.
            run_config: Run configuration.
            agent_context: Optional agent context.

        Yields:
            Events from agent execution, sanitized per tenant audit config.
        """
        logger.debug(f"Tenant {self._tenant_id} run: user={user_id}, session={session_id}")
        metrics = get_tenant_metrics()
        metrics.incr("tenant_requests_total", {"tenant_id": self._tenant_id})
        if self._usage_tracker is not None:
            await self._usage_tracker.record_usage(self._tenant_context, requests=1)
        started = time.monotonic()
        with TRACER.start_as_current_span("tenant.runner.run") as span:
            span.set_attribute("tenant.id", self._tenant_id)
            span.set_attribute("tenant.user_id", user_id)
            span.set_attribute("tenant.session_id", session_id)
            try:
                async for event in self._base_runner.run_async(
                        user_id=user_id,
                        session_id=session_id,
                        new_message=new_message,
                        run_config=run_config,
                        agent_context=agent_context,
                ):
                    self._record_token_usage(event)
                    if self._tenant_context.should_sanitize_tool_inputs():
                        event = self._sanitize_event(event)
                    yield event
            except Exception as e:
                metrics.incr("tenant_errors_total", {"tenant_id": self._tenant_id, "error_type": type(e).__name__})
                span.record_exception(e)
                span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                # Tenant-level model failure policy: degrade to a canned
                # fallback message instead of propagating the error.
                resilience = self._tenant_context.tenant.resilience_config
                if resilience.model_failure_action == "fallback":
                    agent_name = getattr(getattr(self._base_runner, "agent", None), "name", "") or ""
                    logger.warning(f"Tenant {self._tenant_id}: model failed ({type(e).__name__}), "
                                   f"degrading to fallback response")
                    yield Event(
                        invocation_id=uuid.uuid4().hex,
                        author=agent_name,
                        content=genai_types.Content(
                            role="model",
                            parts=[genai_types.Part(text=resilience.model_fallback_text)],
                        ),
                        turn_complete=True,
                    )
                    return
                raise
            finally:
                metrics.observe("tenant_run_latency_ms", {"tenant_id": self._tenant_id},
                                (time.monotonic() - started) * 1000)

    def _record_token_usage(self, event: Event) -> None:
        """Count model token consumption reported in event usage metadata."""
        usage = getattr(event, "usage_metadata", None)
        if usage is None:
            return
        metrics = get_tenant_metrics()
        prompt_tokens = getattr(usage, "prompt_token_count", 0) or 0
        completion_tokens = getattr(usage, "candidates_token_count", 0) or 0
        if prompt_tokens:
            metrics.incr("tenant_tokens_total", {"tenant_id": self._tenant_id, "type": "input"}, prompt_tokens)
        if completion_tokens:
            metrics.incr("tenant_tokens_total", {"tenant_id": self._tenant_id, "type": "output"}, completion_tokens)

    def _sanitize_event(self, event: Event) -> Event:
        """Mask secret-looking values in function-call args."""
        if event.content and event.content.parts:
            for part in event.content.parts:
                if part.function_call and part.function_call.args:
                    part.function_call.args = mask_secrets(part.function_call.args)
        return event

    def get_tenant_id(self) -> str:
        """Return the tenant id this runner is scoped to."""
        return self._tenant_id


async def create_tenant_runner(
    tenant_context: TenantContext,
    agent: BaseAgent,
    base_session_service: BaseSessionService,
    app_name: Optional[str] = None,
    *,
    usage_tracker: Optional[TenantUsageTracker] = None,
    confirm_dangerous_tool: Optional[ConfirmCallback] = None,
    audit_logger: Optional[TenantAuditLogger] = None,
) -> TenantRunner:
    """Create a tenant-scoped runner.

    Args:
        tenant_context: Current tenant context.
        agent: Agent to run (build it with the tenant's model/tools).
        base_session_service: Shared session service backing storage.
        app_name: App name (defaults to the tenant's configured app name).
        usage_tracker: Optional usage tracker enabling monthly budget
            enforcement (``tool_budget_monthly_usd``).
        confirm_dangerous_tool: Optional async callback approving dangerous
            tool calls; when absent, dangerous tools are rejected by default.
        audit_logger: Optional audit logger; when absent one writing to the
            application logger is created if the tenant enables audit logs.

    Returns:
        TenantRunner wrapping a standard Runner wired to a tenant-scoped
        session service, with governance (budget filter + tool permission
        callback) attached to the agent.

    Example:
        >>> runner = await create_tenant_runner(ctx, agent, RedisSessionService())
        >>> async for event in runner.run_async(user_id="u1", session_id="s1",
        ...                                     new_message=msg):
        ...     print(event)
    """
    tenant_session_service = TenantAwareSessionService(
        tenant_context=tenant_context,
        base_session_service=base_session_service,
    )
    if app_name is None:
        app_name = tenant_context.tenant.app_config.app_name

    if audit_logger is None and tenant_context.tenant.audit_config.enable_audit_log:
        audit_logger = TenantAuditLogger(tenant_context.tenant_id)

    # Tool-level governance: whitelist / dangerous-tool confirmation enforced
    # inside the agent's tool execution pipeline.
    governor = TenantToolGovernor(
        tenant_context=tenant_context,
        confirm_dangerous_tool=confirm_dangerous_tool,
        audit_logger=audit_logger,
    )
    if isinstance(agent, LlmAgent):
        existing = agent.before_tool_callback
        callbacks = existing if isinstance(existing, list) else ([existing] if existing else [])
        # Model/tool latency telemetry callbacks (never deny, return None).
        telemetry_hooks = TenantTelemetryHooks(tenant_context.tenant_id)
        # Tool failure policy: circuit breaker short-circuits a failing tool
        # (fail-closed) per the tenant's resilience config.
        resilience_config = tenant_context.tenant.resilience_config
        before_tool_extra = []
        if resilience_config.circuit_breaker_enabled and resilience_config.tool_failure_policy == "closed":
            breaker = ToolCircuitBreaker(
                failure_threshold=resilience_config.circuit_failure_threshold,
                reset_seconds=resilience_config.circuit_reset_seconds,
            )
            resilience_hooks = TenantToolResilienceHooks(
                tenant_id=tenant_context.tenant_id,
                breaker=breaker,
                tool_failure_policy=resilience_config.tool_failure_policy,
                audit_logger=audit_logger,
            )
            before_tool_extra.append(resilience_hooks.before_tool_callback())
            agent.after_tool_callback = _append_callback(agent.after_tool_callback,
                                                         resilience_hooks.after_tool_callback())
        agent.before_tool_callback = [
            *callbacks,
            governor.before_tool_callback(),
            *before_tool_extra,
            telemetry_hooks.before_tool,
        ]
        agent.after_tool_callback = _append_callback(agent.after_tool_callback, telemetry_hooks.after_tool)
        agent.before_model_callback = _append_callback(agent.before_model_callback, telemetry_hooks.before_model)
        agent.after_model_callback = _append_callback(agent.after_model_callback, telemetry_hooks.after_model)

    # Run-level governance: budget check + audit as an agent filter.
    if usage_tracker is not None:
        agent.filters.append(
            TenantGovernanceFilter(
                tenant_context=tenant_context,
                usage_tracker=usage_tracker,
                audit_logger=audit_logger,
            ))

    base_runner = Runner(
        app_name=app_name,
        agent=agent,
        session_service=tenant_session_service,
    )
    return TenantRunner(
        tenant_context=tenant_context,
        base_runner=base_runner,
        tenant_session_service=tenant_session_service,
        usage_tracker=usage_tracker,
        audit_logger=audit_logger,
    )
