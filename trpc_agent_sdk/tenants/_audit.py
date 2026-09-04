# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Tenant audit logging.

Defines the standardized audit log schema (see ``AuditLog``) and a small
writer (:class:`TenantAuditLogger`) that routes audit entries to a configured
backend (anything exposing ``save_audit_log`` — e.g. a
:class:`~trpc_agent_sdk.tenants.StorageRouter`) while guaranteeing that:

- sensitive values are masked before persistence,
- audit failures never break the main execution path.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional, Union

from trpc_agent_sdk.log import logger

# Decision values used in ``AuditLog.decision``.
DECISION_ALLOW = "allow"
"""Request/tool usage permitted."""
DECISION_DENY_BUDGET = "deny_budget"
"""Rejected because the tenant budget was exhausted."""
DECISION_DENY_TOOL = "deny_tool"
"""Rejected because the tool is not allowed for the tenant."""
DECISION_DANGEROUS_REJECTED = "dangerous_tool_rejected"
"""Dangerous tool call rejected by the confirmation callback."""
DECISION_DANGEROUS_CONFIRMED = "dangerous_tool_confirmed"
"""Dangerous tool call approved by the confirmation callback."""
DECISION_COMPLETE = "complete"
"""Run completed successfully."""
DECISION_ERROR = "error"
"""Run failed."""
DECISION_IM_RECEIVED = "im_message_received"
"""IM webhook message accepted."""
DECISION_IM_REJECTED = "im_message_rejected"
"""IM webhook message rejected (unknown token, bad signature, ...)."""
DECISION_IM_DUPLICATE = "im_message_duplicate"
"""IM webhook message dropped as a duplicate."""
DECISION_IM_REPLIED = "im_reply_sent"
"""IM reply delivered successfully."""
DECISION_IM_REPLY_FAILED = "im_reply_failed"
"""IM reply could not be delivered after retries."""

_SECRET_KEY_PATTERNS = ("api_key", "apikey", "secret", "token", "password")


def mask_secrets(data: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of ``data`` with secret-looking values masked.

    Recursively walks dicts/lists; a value is masked when its key contains one
    of the secret patterns (api_key/apikey/secret/token/password).

    Args:
        data: Mapping that may contain sensitive values.

    Returns:
        A new mapping with secrets masked; the input is not modified.
    """

    def _mask(value: Any) -> Any:
        if isinstance(value, dict):
            masked: Dict[str, Any] = {}
            for key, item in value.items():
                key_lower = str(key).lower()
                if any(pattern in key_lower for pattern in _SECRET_KEY_PATTERNS):
                    if isinstance(item, str) and len(item) > 8:
                        masked[key] = f"{item[:4]}...{item[-4:]}"
                    else:
                        masked[key] = "***SANITIZED***"
                else:
                    masked[key] = _mask(item)
            return masked
        if isinstance(value, (list, tuple)):
            return [_mask(item) for item in value]
        return value

    return _mask(data)


@dataclass
class AuditLog:
    """Standardized tenant audit log entry.

    The fields below are the minimum required by the governance spec; ``details``
    carries any extra context (secrets masked on write).
    """

    tenant_id: str
    channel: str = "api"
    user_id: str = ""
    session_id: str = ""
    agent_name: str = ""
    tool_name: str = ""
    decision: str = DECISION_ALLOW
    latency_ms: float = 0.0
    error_type: str = ""
    cost_usd: float = 0.0
    trace_id: str = ""
    timestamp: datetime = field(default_factory=datetime.utcnow)
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a plain dict suitable for storage backends."""
        return {
            "tenant_id": self.tenant_id,
            "channel": self.channel,
            "user_id": self.user_id,
            "session_id": self.session_id,
            "agent_name": self.agent_name,
            "tool_name": self.tool_name,
            "decision": self.decision,
            "latency_ms": self.latency_ms,
            "error_type": self.error_type,
            "cost_usd": self.cost_usd,
            "trace_id": self.trace_id,
            "timestamp": self.timestamp.isoformat(),
            "details": self.details,
        }


AuditWriter = Union[None, Any]
"""Audit sink: an object exposing ``async save_audit_log(tenant_id, data)``,
an ``async`` callable receiving the serialized dict, or ``None`` (logger only)."""


class TenantAuditLogger:
    """Writes :class:`AuditLog` entries for one tenant.

    Args:
        tenant_id: Tenant the logger is scoped to.
        writer: Optional sink. Either an object with
            ``async save_audit_log(tenant_id, data)`` (e.g. ``StorageRouter``)
            or an async callable ``(data: dict) -> None``. When ``None``,
            entries are written to the application logger only.
        mask_details: Whether to mask secret-looking values in ``details``.
    """

    def __init__(self, tenant_id: str, writer: AuditWriter = None, mask_details: bool = True):
        self._tenant_id = tenant_id
        self._writer = writer
        self._mask_details = mask_details

    @property
    def tenant_id(self) -> str:
        """Return the tenant this logger is scoped to."""
        return self._tenant_id

    async def log(self, audit_log: AuditLog) -> None:
        """Write one audit entry; never raises.

        Args:
            audit_log: The entry to persist.
        """
        try:
            if not audit_log.trace_id:
                # Auto-correlate with the active OpenTelemetry trace.
                from trpc_agent_sdk.tenants._tenant_telemetry import current_trace_id

                audit_log.trace_id = current_trace_id()
            data = audit_log.to_dict()
            if self._mask_details and data.get("details"):
                data["details"] = mask_secrets(data["details"])
            if self._writer is None:
                logger.info(f"[audit] {data}")
                return
            if hasattr(self._writer, "save_audit_log"):
                await self._writer.save_audit_log(self._tenant_id, data)
            else:
                await self._writer(data)
        except Exception as e:  # pylint: disable=broad-except
            # Audit must never break the main execution path.
            logger.warning(f"Audit log write failed for tenant {self._tenant_id}: {e}")

    async def log_event(
        self,
        *,
        decision: str,
        channel: str = "api",
        user_id: str = "",
        session_id: str = "",
        agent_name: str = "",
        tool_name: str = "",
        latency_ms: float = 0.0,
        error_type: str = "",
        cost_usd: float = 0.0,
        trace_id: str = "",
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Convenience wrapper building an :class:`AuditLog` for this tenant."""
        await self.log(
            AuditLog(
                tenant_id=self._tenant_id,
                channel=channel,
                user_id=user_id,
                session_id=session_id,
                agent_name=agent_name,
                tool_name=tool_name,
                decision=decision,
                latency_ms=latency_ms,
                error_type=error_type,
                cost_usd=cost_usd,
                trace_id=trace_id,
                details=details or {},
            ))
