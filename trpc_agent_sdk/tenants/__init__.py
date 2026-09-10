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
    ResilienceConfig,
)
from ._tenant_store import (
    OptimisticLockError,
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
    FileSystemStorageBackend,
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
from ._im_transport import (
    MessageChunker,
    RateLimiter,
    TelegramSender,
    WeComSender,
    send_with_retry,
)
from ._wecom_crypto import (
    WeComCrypto,
    WeComCryptoError,
    WeComSignatureError,
    WeComDecryptError,
)
from ._audit import (
    AuditLog,
    TenantAuditLogger,
    mask_secrets,
)
from ._tenant_governance import (
    BudgetCheck,
    BudgetExceededError,
    TenantGovernanceFilter,
    TenantToolGovernor,
    TenantUsageTracker,
    check_im_user_allowed,
)
from ._tenant_telemetry import (
    TenantMetrics,
    TenantTelemetryHooks,
    configure_otel_http_exporter,
    current_trace_id,
    extract_trace_headers,
    get_tenant_metrics,
    inject_trace_headers,
    reset_tenant_metrics,
)
from ._tenant_memory import TenantScopedMemoryService
from ._tenant_vector import (
    InMemoryVectorBackend,
    VectorBackend,
    VectorRecord,
)
from ._tenant_data_migration import (
    MigrationReport,
    migrate_tenants,
    migrate_vectors,
    verify_tenant_migration,
)
from ._sql_ddl import (
    TENANT_TABLE_NAMES,
    TENANT_TABLES_DDL_MYSQL,
    TENANT_TABLES_DDL_SQLITE,
)
from ._sql_migrations import (
    MIGRATIONS,
    Migration,
    SchemaMigrator,
)
from ._tenant_resilience import (
    TenantToolResilienceHooks,
    ToolCircuitBreaker,
)
from ._tenant_degradation import (
    BackendHealth,
    DegradationController,
    TenantStoreWithFallback,
    get_degradation_controller,
)
from ._tenant_rollout import (
    ConfigHistoryEntry,
    ConfigRolloutManager,
    InMemoryConfigHistory,
    RolloutState,
    user_bucket,
)
from ._tenant_capacity import (
    CapacityReport,
    TenantCapacityRow,
    budget_headroom,
    capacity_plan,
    nodes_for_qps,
    nodes_for_sessions,
    project_month_end_usage,
)
from ._tenant_admin_api import (
    TenantAdminService,
    create_admin_app,
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
    "OptimisticLockError",
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
    "FileSystemStorageBackend",

    # IM channels
    "TenantChannelAdapter",
    "TenantMessage",
    "TenantResponse",
    "WeComTenantAdapter",
    "TelegramTenantAdapter",
    "WeComCrypto",
    "WeComCryptoError",
    "WeComSignatureError",
    "WeComDecryptError",
    "MessageDeduplicator",
    "TenantChannelManager",

    # IM transport
    "MessageChunker",
    "RateLimiter",
    "TelegramSender",
    "WeComSender",
    "send_with_retry",

    # Audit
    "AuditLog",
    "TenantAuditLogger",
    "mask_secrets",

    # Governance
    "BudgetCheck",
    "BudgetExceededError",
    "TenantGovernanceFilter",
    "TenantToolGovernor",
    "TenantUsageTracker",
    "check_im_user_allowed",

    # Telemetry (tracing + metrics)
    "TenantMetrics",
    "TenantTelemetryHooks",
    "configure_otel_http_exporter",
    "current_trace_id",
    "extract_trace_headers",
    "get_tenant_metrics",
    "inject_trace_headers",
    "reset_tenant_metrics",

    # Memory scoping
    "TenantScopedMemoryService",

    # Vector store
    "VectorBackend",
    "VectorRecord",
    "InMemoryVectorBackend",

    # Cross-backend data migration
    "MigrationReport",
    "migrate_tenants",
    "migrate_vectors",
    "verify_tenant_migration",

    # Admin API (control plane)
    "TenantAdminService",
    "create_admin_app",

    # SQL schema (DDL + migrations)
    "TENANT_TABLE_NAMES",
    "TENANT_TABLES_DDL_SQLITE",
    "TENANT_TABLES_DDL_MYSQL",
    "Migration",
    "MIGRATIONS",
    "SchemaMigrator",

    # Resilience (circuit breaker, failure policies)
    "ResilienceConfig",
    "ToolCircuitBreaker",
    "TenantToolResilienceHooks",

    # Storage degradation (primary/fallback with health recovery)
    "BackendHealth",
    "DegradationController",
    "TenantStoreWithFallback",
    "get_degradation_controller",

    # Canary rollout & config rollback
    "ConfigRolloutManager",
    "ConfigHistoryEntry",
    "InMemoryConfigHistory",
    "RolloutState",
    "user_bucket",

    # Capacity planning
    "CapacityReport",
    "TenantCapacityRow",
    "budget_headroom",
    "capacity_plan",
    "nodes_for_qps",
    "nodes_for_sessions",
    "project_month_end_usage",
]
