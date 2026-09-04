# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Tenant-aware session service implementation.

Wraps any ``BaseSessionService`` and enforces tenant isolation by:
- prefixing session ids with ``{tenant_id}:`` (hard isolation on the storage key)
- injecting ``tenant_id`` into the session ``state`` (ownership verification)

The public interface matches :class:`trpc_agent_sdk.abc.SessionServiceABC`.
"""

from typing import Optional

from typing_extensions import override

from trpc_agent_sdk.abc import ListSessionsResponse
from trpc_agent_sdk.context import AgentContext
from trpc_agent_sdk.log import logger
from trpc_agent_sdk.sessions import BaseSessionService
from trpc_agent_sdk.sessions import Session

from ._tenant_context import TenantContext

TENANT_STATE_KEY = "tenant_id"


class TenantAwareSessionService(BaseSessionService):
    """Session service that scopes all session keys to a single tenant."""

    def __init__(
        self,
        tenant_context: TenantContext,
        base_session_service: BaseSessionService,
    ):
        """Initialize tenant-aware session service.

        Args:
            tenant_context: Current tenant context.
            base_session_service: Underlying session service to wrap.
        """
        super().__init__(
            summarizer_manager=base_session_service.summarizer_manager,
            session_config=base_session_service.session_config,
        )
        self._tenant_context = tenant_context
        self._base_service = base_session_service
        self._tenant_id = tenant_context.tenant_id

    def scope_session_id(self, session_id: str) -> str:
        """Convert a client session id to its tenant-scoped storage key."""
        return f"{self._tenant_id}:{session_id}"

    def unscope_session_id(self, scoped_session_id: str) -> str:
        """Strip the tenant prefix from a scoped storage key."""
        prefix = f"{self._tenant_id}:"
        if scoped_session_id.startswith(prefix):
            return scoped_session_id[len(prefix):]
        return scoped_session_id

    @override
    async def create_session(
        self,
        *,
        app_name: str,
        user_id: str,
        state: Optional[dict] = None,
        session_id: Optional[str] = None,
        agent_context: Optional[AgentContext] = None,
    ) -> Session:
        """Create a session with tenant id injected into state and scoped key."""
        state = dict(state or {})
        state[TENANT_STATE_KEY] = self._tenant_id

        scoped_id = self.scope_session_id(session_id) if session_id else None
        logger.debug(f"Creating session for tenant {self._tenant_id}, user {user_id}")

        return await self._base_service.create_session(
            app_name=app_name,
            user_id=user_id,
            state=state,
            session_id=scoped_id,
            agent_context=agent_context,
        )

    @override
    async def get_session(
        self,
        *,
        app_name: str,
        user_id: str,
        session_id: str,
        agent_context: Optional[AgentContext] = None,
    ) -> Optional[Session]:
        """Get a session, rejecting sessions owned by another tenant."""
        session = await self._base_service.get_session(
            app_name=app_name,
            user_id=user_id,
            session_id=self.scope_session_id(session_id),
            agent_context=agent_context,
        )
        if session is None:
            return None
        if session.state.get(TENANT_STATE_KEY) != self._tenant_id:
            logger.warning(f"Session {session_id} does not belong to tenant {self._tenant_id}")
            return None
        return session

    @override
    async def list_sessions(
        self,
        *,
        app_name: str,
        user_id: Optional[str] = None,
    ) -> ListSessionsResponse:
        """List sessions belonging to this tenant only."""
        response = await self._base_service.list_sessions(app_name=app_name, user_id=user_id)
        prefix = f"{self._tenant_id}:"
        response.sessions = [s for s in response.sessions if s.id.startswith(prefix)]
        return response

    @override
    async def delete_session(self, *, app_name: str, user_id: str, session_id: str) -> None:
        """Delete a session (tenant-scoped key)."""
        await self._base_service.delete_session(
            app_name=app_name,
            user_id=user_id,
            session_id=self.scope_session_id(session_id),
        )

    @override
    async def append_event(self, session: Session, event) -> Session:
        """Append an event; the session already carries its scoped id."""
        return await self._base_service.append_event(session, event)

    @override
    async def update_session(self, session: Session) -> None:
        """Persist session changes."""
        await self._base_service.update_session(session)

    def get_tenant_id(self) -> str:
        """Return the tenant id this service is scoped to."""
        return self._tenant_id
