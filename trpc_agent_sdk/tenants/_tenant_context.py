# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Multi-tenant context management.

This module provides tenant context injection and management capabilities,
allowing tenant-specific configuration and permissions to be accessed
throughout the agent execution pipeline.
"""

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Dict, List, Optional

from ._tenant_model import Tenant


class TenantContext:
    """Tenant execution context providing access to tenant-specific resources."""

    def __init__(self, tenant: Tenant):
        """Initialize tenant context.

        Args:
            tenant: Tenant object containing configuration and permissions
        """
        self._tenant = tenant

    @property
    def tenant_id(self) -> str:
        """Get tenant ID."""
        return self._tenant.tenant_id

    @property
    def tenant(self) -> Tenant:
        """Get underlying tenant object."""
        return self._tenant

    def get_model_config(self) -> Dict[str, Any]:
        """Get model configuration for this tenant."""
        return self._tenant.model_config.__dict__

    def get_allowed_tools(self) -> List[str]:
        """Get list of allowed tools for this tenant."""
        return self._tenant.tool_permissions.allowed_tools

    def get_blocked_tools(self) -> List[str]:
        """Get list of blocked tools for this tenant."""
        return self._tenant.tool_permissions.blocked_tools

    def get_dangerous_tools(self) -> List[str]:
        """Get list of dangerous tools requiring confirmation."""
        return self._tenant.tool_permissions.dangerous_tools

    def is_tool_allowed(self, tool_name: str) -> bool:
        """Check if a tool is allowed for this tenant."""
        return self._tenant.is_tool_allowed(tool_name)

    def is_tool_dangerous(self, tool_name: str) -> bool:
        """Check if a tool requires confirmation."""
        return self._tenant.is_tool_dangerous(tool_name)

    def get_channel_config(self, channel_type: str) -> Optional[Dict[str, Any]]:
        """Get configuration for a specific channel."""
        config = self._tenant.get_channel_config(channel_type)
        return config.__dict__ if config else None

    def get_storage_config(self) -> Dict[str, Any]:
        """Get storage configuration for this tenant."""
        return self._tenant.storage_config.__dict__

    def get_audit_config(self) -> Dict[str, Any]:
        """Get audit configuration for this tenant."""
        return self._tenant.audit_config.__dict__

    def should_sanitize_api_keys(self) -> bool:
        """Check if API keys should be sanitized in logs."""
        return self._tenant.audit_config.sanitize_api_keys

    def should_sanitize_tool_inputs(self) -> bool:
        """Check if tool inputs should be sanitized in logs."""
        return self._tenant.audit_config.sanitize_tool_inputs

    def get_custom_attribute(self, key: str, default: Any = None) -> Any:
        """Get custom attribute value."""
        return self._tenant.custom_attributes.get(key, default)

    def is_active(self) -> bool:
        """Check if tenant is active."""
        return self._tenant.is_active


class TenantContextManager:
    """Copy-on-write tenant context registry backed by ``contextvars``.

    The context map lives in a :class:`~contextvars.ContextVar` holding an
    immutable snapshot (a dict that is only ever replaced, never mutated).
    :meth:`with_tenant` copies the snapshot, adds its context, and restores
    the previous snapshot via token reset on exit. This gives:

    - per-task isolation in asyncio (concurrent requests on one thread do
      not observe or clear each other's tenant context, unlike a shared
      dict), and per-thread isolation for sync code;
    - correct nesting: an inner ``with_tenant`` for the same tenant no
      longer deletes the outer scope's context when it exits, because each
      scope restores its own snapshot.
    """

    def __init__(self):
        """Initialize the manager with an empty context snapshot."""
        self._contexts: ContextVar = ContextVar("tenant_contexts", default=None)

    def _snapshot(self) -> Dict[str, TenantContext]:
        """Return the current immutable context map (empty when unset)."""
        current = self._contexts.get()
        return current if current is not None else {}

    @contextmanager
    def with_tenant(self, tenant: Tenant):
        """Context manager for executing code with tenant context.

        Args:
            tenant: Tenant object to use for this context

        Yields:
            TenantContext: Tenant context for this scope

        Example:
            >>> tenant = await tenant_store.get_tenant("tenant123")
            >>> with context_manager.with_tenant(tenant) as context:
            ...     # Code here has access to tenant context
            ...     model_config = context.get_model_config()
        """
        context = TenantContext(tenant)
        merged = dict(self._snapshot())
        merged[tenant.tenant_id] = context
        token = self._contexts.set(merged)
        try:
            yield context
        finally:
            # Restore the previous snapshot so nested/overlapping scopes for
            # the same tenant keep working.
            self._contexts.reset(token)

    def get_current_context(self, tenant_id: str) -> Optional[TenantContext]:
        """Get current tenant context for a specific tenant.

        Args:
            tenant_id: ID of the tenant

        Returns:
            TenantContext if available, None otherwise
        """
        return self._snapshot().get(tenant_id)

    def set_current_context(self, context: TenantContext) -> None:
        """Set current tenant context.

        Args:
            context: Tenant context to set as current
        """
        merged = dict(self._snapshot())
        merged[context.tenant_id] = context
        self._contexts.set(merged)

    def clear_context(self, tenant_id: str) -> None:
        """Clear tenant context.

        Args:
            tenant_id: ID of the tenant to clear context for
        """
        merged = dict(self._snapshot())
        merged.pop(tenant_id, None)
        self._contexts.set(merged)

    def clear_all_contexts(self) -> None:
        """Clear all tenant contexts."""
        self._contexts.set({})

    def get_active_tenants(self) -> List[str]:
        """Get list of tenant IDs with active contexts.

        Returns:
            List of tenant IDs that currently have active contexts
        """
        return list(self._snapshot().keys())


# Global context manager instance
_global_context_manager = TenantContextManager()


def get_context_manager() -> TenantContextManager:
    """Get the global tenant context manager.

    Returns:
        Global TenantContextManager instance
    """
    return _global_context_manager


def get_current_tenant_context(tenant_id: str) -> Optional[TenantContext]:
    """Get current tenant context using global manager.

    Args:
        tenant_id: ID of the tenant

    Returns:
        TenantContext if available, None otherwise
    """
    return _global_context_manager.get_current_context(tenant_id)


def set_tenant_context(context: TenantContext) -> None:
    """Set tenant context using global manager.

    Args:
        context: Tenant context to set as current
    """
    _global_context_manager.set_current_context(context)


def clear_tenant_context(tenant_id: str) -> None:
    """Clear tenant context using global manager.

    Args:
        tenant_id: ID of the tenant to clear context for
    """
    _global_context_manager.clear_context(tenant_id)
