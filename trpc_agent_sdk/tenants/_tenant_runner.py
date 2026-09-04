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

from typing import AsyncGenerator, Optional

from trpc_agent_sdk.agents import BaseAgent
from trpc_agent_sdk.context import AgentContext
from trpc_agent_sdk.configs import RunConfig
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.log import logger
from trpc_agent_sdk.runners import Runner
from trpc_agent_sdk.sessions import BaseSessionService
from trpc_agent_sdk.types import Content

from ._tenant_context import TenantContext
from ._tenant_session_service import TenantAwareSessionService


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
    ):
        """Initialize tenant-aware runner.

        Args:
            tenant_context: Current tenant context.
            base_runner: Underlying standard runner.
            tenant_session_service: The tenant-scoped session service used by
                ``base_runner``.
        """
        self._tenant_context = tenant_context
        self._base_runner = base_runner
        self._tenant_session_service = tenant_session_service
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
        async for event in self._base_runner.run_async(
                user_id=user_id,
                session_id=session_id,
                new_message=new_message,
                run_config=run_config,
                agent_context=agent_context,
        ):
            if self._tenant_context.should_sanitize_tool_inputs():
                event = self._sanitize_event(event)
            yield event

    def _sanitize_event(self, event: Event) -> Event:
        """Mask secret-looking values in function-call args."""
        if event.content and event.content.parts:
            for part in event.content.parts:
                if part.function_call and part.function_call.args:
                    self._sanitize_dict(part.function_call.args)
        return event

    def _sanitize_dict(self, data: dict) -> None:
        """Recursively mask values whose key looks like a secret, in place."""
        secret_keys = ("api_key", "apikey", "secret", "token", "password")
        for key, value in data.items():
            key_lower = key.lower()
            if any(pattern in key_lower for pattern in secret_keys):
                if isinstance(value, str) and len(value) > 8:
                    data[key] = f"{value[:4]}...{value[-4:]}"
                else:
                    data[key] = "***SANITIZED***"
            elif isinstance(value, dict):
                self._sanitize_dict(value)

    def get_tenant_id(self) -> str:
        """Return the tenant id this runner is scoped to."""
        return self._tenant_id


async def create_tenant_runner(
    tenant_context: TenantContext,
    agent: BaseAgent,
    base_session_service: BaseSessionService,
    app_name: Optional[str] = None,
) -> TenantRunner:
    """Create a tenant-scoped runner.

    Args:
        tenant_context: Current tenant context.
        agent: Agent to run (build it with the tenant's model/tools).
        base_session_service: Shared session service backing storage.
        app_name: App name (defaults to the tenant's configured app name).

    Returns:
        TenantRunner wrapping a standard Runner wired to a tenant-scoped
        session service.

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

    base_runner = Runner(
        app_name=app_name,
        agent=agent,
        session_service=tenant_session_service,
    )
    return TenantRunner(
        tenant_context=tenant_context,
        base_runner=base_runner,
        tenant_session_service=tenant_session_service,
    )
