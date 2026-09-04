# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Multi-tenant management module.

This module provides tenant isolation and management capabilities including:
- Tenant data models and configuration
- Tenant storage backends (InMemory, Redis, SQL)
- Tenant context injection and routing
- Tenant-level resource isolation
"""

from ._tenant_model import (
    Tenant,
    TenantConfig,
    AppConfig,
    ModelConfig,
    ToolPermissions,
    ChannelConfig,
    StorageConfig,
    AuditConfig,
)
from ._tenant_store import (
    TenantStore,
    InMemoryTenantStore,
    RedisTenantStore,
    SqlTenantStore,
)
from ._tenant_context import (
    TenantContext,
    TenantContextManager,
    get_context_manager,
    get_current_tenant_context,
    set_tenant_context,
    clear_tenant_context,
)
from ._router import (
    TenantRouter,
    TenantIdentificationStrategy,
    TenantIdentifier,
    TenantExtractionError,
    HttpHeaderIdentifier,
    ApiKeyIdentifier,
    SubdomainIdentifier,
    PathPrefixIdentifier,
    ImWebhookTokenIdentifier,
    CustomIdentifier,
)
from ._tenant_session_service import (
    TenantAwareSessionService, )
from ._tenant_runner import (
    TenantRunner,
    create_tenant_runner,
)
from ._unified_storage import (
    UnifiedStorageBackend,
    StorageRouter,
    StorageBackendType,
    DataCategory,
    InMemoryStorageBackend,
    RedisStorageBackend,
    SQLStorageBackend,
)
from ._tenant_channels import (
    TenantChannelAdapter,
    TenantMessage,
    TenantResponse,
    WeComTenantAdapter,
    TelegramTenantAdapter,
    MessageDeduplicator,
    TenantChannelManager,
)

__all__ = [
    # Data models
    "Tenant",
    "TenantConfig",
    "AppConfig",
    "ModelConfig",
    "ToolPermissions",
    "ChannelConfig",
    "StorageConfig",
    "AuditConfig",

    # Storage backends
    "TenantStore",
    "InMemoryTenantStore",
    "RedisTenantStore",
    "SqlTenantStore",

    # Context management
    "TenantContext",
    "TenantContextManager",
    "get_context_manager",
    "get_current_tenant_context",
    "set_tenant_context",
    "clear_tenant_context",

    # Routing
    "TenantRouter",
    "TenantIdentificationStrategy",
    "TenantIdentifier",
    "TenantExtractionError",
    "HttpHeaderIdentifier",
    "ApiKeyIdentifier",
    "SubdomainIdentifier",
    "PathPrefixIdentifier",
    "ImWebhookTokenIdentifier",
    "CustomIdentifier",

    # Session services
    "TenantAwareSessionService",

    # Runner
    "TenantRunner",
    "create_tenant_runner",

    # Unified storage
    "UnifiedStorageBackend",
    "StorageRouter",
    "StorageBackendType",
    "DataCategory",
    "InMemoryStorageBackend",
    "RedisStorageBackend",
    "SQLStorageBackend",

    # IM channels
    "TenantChannelAdapter",
    "TenantMessage",
    "TenantResponse",
    "WeComTenantAdapter",
    "TelegramTenantAdapter",
    "MessageDeduplicator",
    "TenantChannelManager",
]
