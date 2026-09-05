# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Multi-tenant data models.

This module defines the core data structures for tenant management,
including tenant configuration, permissions, and isolation settings.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel


@dataclass
class AppConfig:
    """Application-level configuration for a tenant."""

    app_name: str = "default_app"
    app_description: Optional[str] = None
    max_concurrent_sessions: int = 100
    session_timeout_seconds: int = 3600
    max_history_messages: int = 50
    enable_summarization: bool = True
    summarization_threshold: int = 10


@dataclass
class ModelConfig:
    """Model configuration for a tenant."""

    model_provider: str = "openai"  # 'openai', 'anthropic', 'litellm'
    model_name: str = "gpt-4o-mini"
    api_key: Optional[str] = None  # Should be stored securely
    base_url: Optional[str] = None
    max_tokens: int = 4096
    temperature: float = 0.7
    enable_streaming: bool = True
    timeout_seconds: int = 30
    max_retries: int = 3


@dataclass
class ToolPermissions:
    """Tool access permissions for a tenant."""

    allowed_tools: List[str] = field(default_factory=list)
    blocked_tools: List[str] = field(default_factory=list)
    dangerous_tools: List[str] = field(default_factory=list)  # Require confirmation
    tool_budget_monthly_usd: float = 100.0


@dataclass
class ChannelConfig:
    """IM channel configuration for a tenant."""

    channel_type: str  # 'wecom', 'telegram', 'wechat', 'http'
    enabled: bool = True
    webhook_token: Optional[str] = None
    webhook_secret: Optional[str] = None
    api_key: Optional[str] = None
    bot_id: Optional[str] = None
    allowed_users: List[str] = field(default_factory=list)
    rate_limit_per_minute: int = 60
    enable_streaming: bool = True
    # Session id generation strategy for this channel:
    #   "chat"  — one session per chat (default, 1:1 conversations)
    #   "user"  — one session per user across all chats
    #   "group" — group chats share a group session, private chats fall
    #             back to per-user sessions
    session_strategy: str = "chat"


@dataclass
class StorageConfig:
    """Storage backend configuration for a tenant."""

    session_backend: str = "redis"  # 'in_memory', 'redis', 'sql'
    memory_backend: str = "redis"  # 'in_memory', 'redis', 'sql', 'mem0'
    knowledge_backend: str = "langchain_vectorstore"  # 'langchain_vectorstore', 'external'
    audit_backend: str = "sql"  # 'sql', 'external'

    # Backend-specific settings
    redis_url: Optional[str] = None
    redis_cluster_enabled: bool = False
    sql_url: Optional[str] = None
    vector_store_url: Optional[str] = None
    external_memory_url: Optional[str] = None


@dataclass
class AuditConfig:
    """Audit and compliance configuration for a tenant."""

    enable_audit_log: bool = True
    audit_retention_days: int = 90
    log_level: str = "INFO"  # 'DEBUG', 'INFO', 'WARNING', 'ERROR'

    # Data sanitization rules
    sanitize_api_keys: bool = True
    sanitize_user_data: bool = False
    sanitize_tool_inputs: bool = True

    # Alerting
    enable_cost_alerts: bool = True
    cost_alert_threshold_usd: float = 50.0
    enable_error_alerts: bool = True


@dataclass
class ResilienceConfig:
    """Tenant-level fault-tolerance policy (model failure, tool failure).

    - ``model_failure_action``: what a run does when the model call fails
      after retries — ``"error"`` propagates the exception, ``"fallback"``
      yields a synthetic event with ``model_fallback_text`` instead.
    - ``tool_failure_policy``: ``"closed"`` (fail-closed) propagates tool
      errors; ``"open"`` (fail-open) lets the run continue with the tool
      marked failed. Either way the per-tool circuit breaker
      (:class:`~trpc_agent_sdk.tenants.ToolCircuitBreaker`) stops repeated
      calls to a failing tool once ``circuit_failure_threshold`` consecutive
      failures are recorded.
    """

    model_timeout_seconds: int = 30
    model_max_retries: int = 3
    model_failure_action: str = "error"  # 'error' | 'fallback'
    model_fallback_text: str = "The assistant is temporarily unavailable. Please try again later."

    tool_failure_policy: str = "closed"  # 'open' | 'closed'
    circuit_breaker_enabled: bool = True
    circuit_failure_threshold: int = 5
    circuit_reset_seconds: int = 60


@dataclass
class Tenant:
    """Core tenant model representing a multi-tenant organization."""

    tenant_id: str
    name: str
    description: Optional[str] = None

    # Configuration sections
    app_config: AppConfig = field(default_factory=AppConfig)
    model_config: ModelConfig = field(default_factory=ModelConfig)
    tool_permissions: ToolPermissions = field(default_factory=ToolPermissions)
    channel_configs: Dict[str, ChannelConfig] = field(default_factory=dict)
    storage_config: StorageConfig = field(default_factory=StorageConfig)
    audit_config: AuditConfig = field(default_factory=AuditConfig)
    resilience_config: ResilienceConfig = field(default_factory=ResilienceConfig)

    # Metadata
    custom_attributes: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    is_active: bool = True
    # Optimistic-lock revision, incremented by the SQL store on each update;
    # carries over the config round-trip so concurrent writers are detected.
    version: int = 0
    # Config release version for canary rollout / rollback (see
    # trpc_agent_sdk.tenants.TenantRolloutManager).
    config_version: int = 1

    def get_channel_config(self, channel_type: str) -> Optional[ChannelConfig]:
        """Get configuration for a specific channel type."""
        return self.channel_configs.get(channel_type)

    def is_tool_allowed(self, tool_name: str) -> bool:
        """Check if a tool is allowed for this tenant."""
        if tool_name in self.tool_permissions.blocked_tools:
            return False
        if not self.tool_permissions.allowed_tools:
            # Empty allowed list means all tools are allowed
            return True
        return tool_name in self.tool_permissions.allowed_tools

    def is_tool_dangerous(self, tool_name: str) -> bool:
        """Check if a tool requires confirmation."""
        return tool_name in self.tool_permissions.dangerous_tools


class TenantConfig(BaseModel):
    """Pydantic model for tenant configuration validation and serialization."""

    tenant_id: str
    name: str
    description: Optional[str] = None

    # Configuration sections (using Pydantic for validation)
    # NOTE: field is named llm_config because pydantic v2 reserves `model_config`
    app_config: Dict[str, Any] = field(default_factory=dict)
    llm_config: Dict[str, Any] = field(default_factory=dict)
    tool_permissions: Dict[str, Any] = field(default_factory=dict)
    channel_configs: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    storage_config: Dict[str, Any] = field(default_factory=dict)
    audit_config: Dict[str, Any] = field(default_factory=dict)
    resilience_config: Dict[str, Any] = field(default_factory=dict)

    custom_attributes: Dict[str, Any] = field(default_factory=dict)
    is_active: bool = True
    version: int = 0
    config_version: int = 1

    def to_tenant(self) -> Tenant:
        """Convert Pydantic model to Tenant dataclass."""
        return Tenant(
            tenant_id=self.tenant_id,
            name=self.name,
            description=self.description,
            app_config=AppConfig(**self.app_config) if self.app_config else AppConfig(),
            model_config=ModelConfig(**self.llm_config) if self.llm_config else ModelConfig(),
            tool_permissions=ToolPermissions(**self.tool_permissions) if self.tool_permissions else ToolPermissions(),
            channel_configs={
                k: ChannelConfig(**v)
                for k, v in self.channel_configs.items()
            },
            storage_config=StorageConfig(**self.storage_config) if self.storage_config else StorageConfig(),
            audit_config=AuditConfig(**self.audit_config) if self.audit_config else AuditConfig(),
            resilience_config=(ResilienceConfig(
                **self.resilience_config) if self.resilience_config else ResilienceConfig()),
            custom_attributes=self.custom_attributes,
            is_active=self.is_active,
            version=self.version,
            config_version=self.config_version,
        )

    @classmethod
    def from_tenant(cls, tenant: Tenant) -> "TenantConfig":
        """Create TenantConfig from Tenant dataclass."""
        return cls(
            tenant_id=tenant.tenant_id,
            name=tenant.name,
            description=tenant.description,
            app_config=tenant.app_config.__dict__,
            llm_config=tenant.model_config.__dict__,
            tool_permissions=tenant.tool_permissions.__dict__,
            channel_configs={
                k: v.__dict__
                for k, v in tenant.channel_configs.items()
            },
            storage_config=tenant.storage_config.__dict__,
            audit_config=tenant.audit_config.__dict__,
            resilience_config=tenant.resilience_config.__dict__,
            custom_attributes=tenant.custom_attributes,
            is_active=tenant.is_active,
            version=tenant.version,
            config_version=tenant.config_version,
        )
