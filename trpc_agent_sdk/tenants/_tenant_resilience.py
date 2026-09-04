# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Tenant-level resilience: tool circuit breaker and failure policies.

Implements the "failure recovery" requirements of the multi-tenant task:

- **Tool failure policy** (:class:`TenantToolResilienceHooks`): when a tool
  fails repeatedly the circuit breaker opens and subsequent calls are
  short-circuited with a degraded error response (fail-closed) instead of
  hammering a broken dependency. ``tool_failure_policy="open"`` disables the
  breaker (fail-open: always attempt the tool) for tenants that prefer
  best-effort execution.
- **Model failure action** (:meth:`TenantRunner` wiring): tenants choose
  between re-raising model errors (``model_failure_action="error"``) and
  degrading to a canned fallback message (``"fallback"``).

The breaker is a classic closed → open → half-open state machine keyed by
tool name, thread-safe, with an injectable clock for tests.
"""

import threading
import time
from typing import Any, Callable, Dict, Optional

from trpc_agent_sdk.log import logger

from ._audit import TenantAuditLogger
from ._tenant_telemetry import get_tenant_metrics

DECISION_TOOL_CIRCUIT_OPEN = "tool_circuit_open"

CIRCUIT_CLOSED = "closed"
CIRCUIT_OPEN = "open"
CIRCUIT_HALF_OPEN = "half_open"


class ToolCircuitBreaker:
    """Per-tool circuit breaker (closed → open → half-open).

    State machine per tool name:

    - ``closed``: normal operation; consecutive failures are counted and the
      breaker opens once the count reaches ``failure_threshold``.
    - ``open``: calls are rejected; after ``reset_seconds`` elapse the next
      call is allowed as a probe.
    - ``half_open``: one probe call is allowed; success closes the breaker,
      failure re-opens it.

    Args:
        failure_threshold: Consecutive failures before opening.
        reset_seconds: How long the breaker stays open before probing.
        clock: Monotonic time function (injectable for tests).
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        reset_seconds: int = 60,
        clock: Callable[[], float] = time.monotonic,
    ):
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if reset_seconds < 0:
            raise ValueError("reset_seconds must be >= 0")
        self._failure_threshold = failure_threshold
        self._reset_seconds = reset_seconds
        self._clock = clock
        self._lock = threading.Lock()
        # Per-tool breaker state.
        self._states: Dict[str, str] = {}
        self._failures: Dict[str, int] = {}
        self._opened_at: Dict[str, float] = {}

    @property
    def failure_threshold(self) -> int:
        """Consecutive failures required to open the breaker."""
        return self._failure_threshold

    @property
    def reset_seconds(self) -> int:
        """Seconds the breaker stays open before allowing a probe."""
        return self._reset_seconds

    def state(self, tool_name: str) -> str:
        """Return the current state for ``tool_name`` (no probing side effects)."""
        with self._lock:
            return self._peek_state(tool_name)

    def allow(self, tool_name: str) -> bool:
        """Return whether a call to ``tool_name`` may proceed.

        An ``open`` breaker transitions to ``half_open`` and allows the call
        once ``reset_seconds`` have elapsed (the call becomes the probe).
        """
        with self._lock:
            state = self._peek_state(tool_name)
            if state == CIRCUIT_CLOSED or state == CIRCUIT_HALF_OPEN:
                return True
            # Open: allow a single probe after the reset window.
            if self._clock() - self._opened_at.get(tool_name, 0.0) >= self._reset_seconds:
                self._states[tool_name] = CIRCUIT_HALF_OPEN
                return True
            return False

    def record_success(self, tool_name: str) -> None:
        """Record a successful call; closes the breaker and resets the count."""
        with self._lock:
            self._failures[tool_name] = 0
            self._states[tool_name] = CIRCUIT_CLOSED

    def record_failure(self, tool_name: str) -> None:
        """Record a failed call; may open the breaker.

        A failed ``half_open`` probe re-opens the breaker immediately and
        restarts the reset window.
        """
        with self._lock:
            state = self._peek_state(tool_name)
            if state == CIRCUIT_HALF_OPEN:
                self._states[tool_name] = CIRCUIT_OPEN
                self._opened_at[tool_name] = self._clock()
                return
            failures = self._failures.get(tool_name, 0) + 1
            self._failures[tool_name] = failures
            if failures >= self._failure_threshold:
                self._states[tool_name] = CIRCUIT_OPEN
                self._opened_at[tool_name] = self._clock()
                logger.warning(f"Circuit breaker opened for tool '{tool_name}' "
                               f"after {failures} consecutive failures")

    def _peek_state(self, tool_name: str) -> str:
        """Read the stored state without transitioning (lock must be held)."""
        return self._states.get(tool_name, CIRCUIT_CLOSED)


def _response_indicates_error(response: Any) -> bool:
    """Heuristic: does a tool response represent a failure?

    Handles plain dicts (``{"error": ...}``) and response objects exposing an
    ``error`` attribute, which covers the SDK's tool result conventions.
    """
    if response is None:
        return False
    if isinstance(response, dict):
        return bool(response.get("error"))
    error = getattr(response, "error", None)
    return bool(error)


# Error markers of platform-generated short-circuit responses (governance
# denials and the circuit breaker's own degraded reply). These flow through
# ``after_tool_callback`` like real tool results but are NOT tool failures
# and must not count towards opening the breaker.
_SHORT_CIRCUIT_MARKERS = (
    "is not allowed for this tenant",
    "requires confirmation",
    "temporarily unavailable",
)


def _is_short_circuit_response(response: Any) -> bool:
    """Return True when the response is a platform denial, not a tool failure."""
    error = response.get("error") if isinstance(response, dict) else getattr(response, "error", None)
    if not isinstance(error, str):
        return False
    return any(marker in error for marker in _SHORT_CIRCUIT_MARKERS)


class TenantToolResilienceHooks:
    """Circuit-breaker hooks wired into an ``LlmAgent``.

    - ``before_tool_callback`` short-circuits with a degraded error dict when
      the breaker is open (fail-closed); with ``tool_failure_policy="open"``
      the hooks never block (fail-open).
    - ``after_tool_callback`` feeds success/failure into the breaker.

    Args:
        tenant_id: Tenant for metrics labels.
        breaker: The circuit breaker instance.
        tool_failure_policy: ``"closed"`` (default) blocks calls while the
            breaker is open; ``"open"`` always attempts the tool.
        audit_logger: Optional audit logger for short-circuit decisions.
    """

    def __init__(
        self,
        tenant_id: str,
        breaker: ToolCircuitBreaker,
        tool_failure_policy: str = "closed",
        audit_logger: Optional[TenantAuditLogger] = None,
    ):
        if tool_failure_policy not in ("closed", "open"):
            raise ValueError("tool_failure_policy must be 'closed' or 'open'")
        self._tenant_id = tenant_id
        self._breaker = breaker
        self._fail_closed = tool_failure_policy == "closed"
        self._audit = audit_logger

    @property
    def breaker(self) -> ToolCircuitBreaker:
        """The underlying circuit breaker."""
        return self._breaker

    def before_tool_callback(self):
        """Return an ``LlmAgent``-compatible ``before_tool_callback``."""

        async def _callback(invocation_context, tool, args: Dict[str, Any], kwargs) -> Optional[Dict[str, Any]]:
            if not self._fail_closed:
                return None
            tool_name = getattr(tool, "name", "") or ""
            if self._breaker.allow(tool_name):
                return None
            metrics = get_tenant_metrics()
            metrics.incr("tenant_errors_total", {
                "tenant_id": self._tenant_id,
                "error_type": "ToolCircuitOpen",
            })
            if self._audit is not None:
                await self._audit.log_event(
                    decision=DECISION_TOOL_CIRCUIT_OPEN,
                    tool_name=tool_name,
                    details={"policy": "fail_closed"},
                )
            logger.warning(f"Tenant {self._tenant_id}: tool '{tool_name}' short-circuited (circuit open)")
            return {"error": f"Tool '{tool_name}' is temporarily unavailable, please retry later"}

        return _callback

    def after_tool_callback(self):
        """Return an ``LlmAgent``-compatible ``after_tool_callback``."""

        async def _callback(invocation_context, tool, args: Dict[str, Any], response) -> None:
            tool_name = getattr(tool, "name", "") or ""
            if _is_short_circuit_response(response):
                return None  # Platform denial / breaker's own reply: not a tool failure.
            if _response_indicates_error(response):
                self._breaker.record_failure(tool_name)
            else:
                self._breaker.record_success(tool_name)
            return None

        return _callback
