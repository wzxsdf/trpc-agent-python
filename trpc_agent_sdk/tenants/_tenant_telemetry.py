# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Tenant telemetry: OpenTelemetry tracing helpers and Prometheus metrics.

Tracing
-------
The tRPC-Agent SDK already creates spans for Runner/LLM/Tool execution via
``trpc_agent_sdk.telemetry``. This module adds the missing tenant context:

- :func:`current_trace_id` — hex trace id of the current span (for audit logs).
- :func:`inject_trace_headers` / :func:`extract_trace_headers` — W3C
  ``traceparent`` propagation for gateway → worker hops.
- Spans opened around IM webhooks (``tenant.im.callback``), agent runs
  (``tenant.runner.run``) and IM replies (``tenant.im.reply``) chain the
  full path: IM callback → Runner → Model/Tool → Session/Memory → IM reply.

Metrics
-------
:class:`TenantMetrics` is a dependency-free in-process registry exposing a
Prometheus text-exposition endpoint via :meth:`TenantMetrics.render_prometheus`.
Per-node aggregation is expected to be scraped and aggregated by Prometheus
itself, so no cross-node store is required.
"""

import threading
import time
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional, Tuple

from opentelemetry import context as otel_context
from opentelemetry import propagate
from opentelemetry import trace

TRACER = trace.get_tracer("trpc.python.tenants")

_RUN_LATENCY_BUCKETS = [10, 50, 100, 250, 500, 1000, 2500, 5000, 10000]
_BACKEND_LATENCY_BUCKETS = [1, 5, 10, 25, 50, 100, 250, 500, 1000]


def current_trace_id() -> str:
    """Return the current OpenTelemetry trace id as 32-char hex (or "")."""
    span = trace.get_current_span()
    span_context = span.get_span_context()
    if span_context.is_valid:
        return format(span_context.trace_id, "032x")
    return ""


def inject_trace_headers(headers: Dict[str, str]) -> Dict[str, str]:
    """Inject the current trace context into ``headers`` (W3C traceparent).

    Use on the outgoing side of a hop (e.g. Gateway → Worker dispatch).

    Args:
        headers: Header dict to update in place.

    Returns:
        The same dict, updated.
    """
    propagate.inject(headers)
    return headers


@contextmanager
def extract_trace_headers(headers: Dict[str, str]) -> Iterator[None]:
    """Continue a trace received in ``headers`` for the enclosed block.

    Use on the incoming side of a hop (e.g. inside an IM webhook handler)
    so the new span becomes a child of the upstream trace.

    Args:
        headers: Incoming HTTP headers potentially carrying ``traceparent``.
    """
    context = propagate.extract(headers)
    token = otel_context.attach(context)
    try:
        yield
    finally:
        otel_context.detach(token)


def _escape_label_value(value: Any) -> str:
    """Escape a label value for the Prometheus text format."""
    text = str(value)
    return (text.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n"))


class TenantMetrics:
    """Dependency-free tenant metrics registry with Prometheus rendering.

    Thread-safe; aggregates counters and histograms per label set. Each
    process holds its own instance — Prometheus scrapes and aggregates
    across nodes.
    """

    _METRIC_META: Dict[str, Tuple[str, str]] = {
        # name: (type, help)
        "tenant_requests_total": ("counter", "Tenant agent requests"),
        "tenant_errors_total": ("counter", "Tenant agent run errors"),
        "tenant_run_latency_ms": ("histogram", "Tenant agent run latency in ms"),
        "tenant_model_latency_ms": ("histogram", "Tenant model call latency in ms"),
        "tenant_tool_latency_ms": ("histogram", "Tenant tool call latency in ms"),
        "tenant_im_reply_total": ("counter", "Tenant IM replies by delivery result"),
        "tenant_tokens_total": ("counter", "Tenant model token consumption"),
        "tenant_cost_usd_total": ("counter", "Tenant estimated cost in USD"),
        "tenant_session_backend_latency_ms": ("histogram", "Tenant session backend latency in ms"),
    }

    def __init__(self):
        """Initialize an empty registry."""
        self._lock = threading.Lock()
        self._counters: Dict[Tuple[str, Tuple[Tuple[str, str], ...]], float] = {}
        # {name: {labels: {"count": int, "sum": float, "buckets": [counts]}}}
        self._histograms: Dict[str, Dict[Tuple[Tuple[str, str], ...], Dict[str, Any]]] = {}

    def incr(self, name: str, labels: Optional[Dict[str, str]] = None, value: float = 1.0) -> None:
        """Increment a counter metric.

        Args:
            name: Metric name (must be declared in ``_METRIC_META``).
            labels: Label dimensions.
            value: Amount to add.
        """
        key = tuple(sorted((labels or {}).items()))
        with self._lock:
            self._counters[(name, key)] = self._counters.get((name, key), 0.0) + value

    def observe(self,
                name: str,
                labels: Optional[Dict[str, str]],
                value: float,
                buckets: Optional[List[float]] = None) -> None:
        """Record one observation of a histogram metric.

        Args:
            name: Metric name (must be declared in ``_METRIC_META``).
            labels: Label dimensions.
            value: Observed value.
            buckets: Bucket upper bounds (defaults per metric).
        """
        if buckets is None:
            buckets = (_BACKEND_LATENCY_BUCKETS
                       if name == "tenant_session_backend_latency_ms" else _RUN_LATENCY_BUCKETS)
        key = tuple(sorted((labels or {}).items()))
        with self._lock:
            series = self._histograms.setdefault(name, {})
            entry = series.setdefault(key, {"count": 0, "sum": 0.0, "buckets": [0] * len(buckets)})
            entry["count"] += 1
            entry["sum"] += value
            for index, bound in enumerate(buckets):
                if value <= bound:
                    entry["buckets"][index] += 1

    def get_counter(self, name: str, labels: Optional[Dict[str, str]] = None) -> float:
        """Return a counter value (for tests and internal assertions)."""
        key = tuple(sorted((labels or {}).items()))
        with self._lock:
            return self._counters.get((name, key), 0.0)

    def render_prometheus(self) -> str:
        """Render all recorded metrics in the Prometheus text format."""
        lines: List[str] = []
        with self._lock:
            counters = dict(self._counters)
            histograms = {name: dict(series) for name, series in self._histograms.items()}

        for (name, labels_key), value in sorted(counters.items()):
            self._emit_help_type(lines, name)
            lines.append(f"{name}{self._format_labels(labels_key)} {value}")

        for name, series in sorted(histograms.items()):
            self._emit_help_type(lines, name)
            buckets = self._buckets_for(name)
            for labels_key, entry in sorted(series.items()):
                # Bucket counts are stored already-cumulative (every bound
                # >= value is incremented), matching Prometheus semantics.
                for bound, bucket_count in zip(buckets, entry["buckets"]):
                    lines.append(f'{name}_bucket{self._format_labels(labels_key, {"le": bound})} {bucket_count}')
                lines.append(f'{name}_bucket{self._format_labels(labels_key, {"le": "+Inf"})} {entry["count"]}')
                lines.append(f'{name}_sum{self._format_labels(labels_key)} {entry["sum"]}')
                lines.append(f'{name}_count{self._format_labels(labels_key)} {entry["count"]}')

        return "\n".join(lines) + ("\n" if lines else "")

    @classmethod
    def _buckets_for(cls, name: str) -> List[float]:
        """Return the bucket bounds configured for a histogram metric."""
        return (_BACKEND_LATENCY_BUCKETS if name == "tenant_session_backend_latency_ms" else _RUN_LATENCY_BUCKETS)

    @classmethod
    def _emit_help_type(cls, lines: List[str], name: str) -> None:
        """Append HELP/TYPE header lines once per metric name."""
        meta = cls._METRIC_META.get(name)
        if meta and (not lines or f"# HELP {name} " not in lines):
            metric_type, help_text = meta
            lines.append(f"# HELP {name} {help_text}")
            lines.append(f"# TYPE {name} {metric_type}")

    @staticmethod
    def _format_labels(labels_key: Tuple[Tuple[str, str], ...], extra: Optional[Dict[str, Any]] = None) -> str:
        """Format a label key (plus extra labels) as Prometheus label text."""
        merged = dict(labels_key)
        merged.update(extra or {})
        if not merged:
            return ""
        parts = ",".join(f'{k}="{_escape_label_value(v)}"' for k, v in sorted(merged.items()))
        return f"{{{parts}}}"


_metrics_lock = threading.Lock()
_global_metrics: Optional[TenantMetrics] = None


def get_tenant_metrics() -> TenantMetrics:
    """Return the process-wide tenant metrics registry."""
    global _global_metrics
    with _metrics_lock:
        if _global_metrics is None:
            _global_metrics = TenantMetrics()
        return _global_metrics


def reset_tenant_metrics() -> None:
    """Reset the process-wide registry (mainly for tests)."""
    global _global_metrics
    with _metrics_lock:
        _global_metrics = TenantMetrics()


class TenantTelemetryHooks:
    """Per-tenant model/tool timing callbacks for ``LlmAgent``.

    The ``before_*/after_*`` callables match the SDK callback signatures and
    may be appended to an agent's ``before_model_callback`` /
    ``after_model_callback`` / ``before_tool_callback`` /
    ``after_tool_callback`` lists. They record latency histograms keyed by
    tenant (and tool name).
    """

    def __init__(self, tenant_id: str, metrics: Optional[TenantMetrics] = None):
        """Initialize telemetry hooks.

        Args:
            tenant_id: Tenant the observed calls belong to.
            metrics: Optional registry override (defaults to the global one).
        """
        self._tenant_id = tenant_id
        self._metrics = metrics or get_tenant_metrics()
        self._model_started: Dict[str, float] = {}
        self._tool_started: Dict[Tuple[str, str], float] = {}

    async def before_model(self, invocation_context, request) -> None:
        """Record model call start time."""
        invocation_id = getattr(invocation_context, "invocation_id", "")
        self._model_started[invocation_id] = time.monotonic()
        return None

    async def after_model(self, invocation_context, response) -> None:
        """Record model call latency."""
        invocation_id = getattr(invocation_context, "invocation_id", "")
        started = self._model_started.pop(invocation_id, None)
        if started is not None:
            latency_ms = (time.monotonic() - started) * 1000
            self._metrics.observe("tenant_model_latency_ms", {"tenant_id": self._tenant_id}, latency_ms)
        return None

    async def before_tool(self, invocation_context, tool, args, kwargs) -> None:
        """Record tool call start time."""
        invocation_id = getattr(invocation_context, "invocation_id", "")
        tool_name = getattr(tool, "name", "") or ""
        self._tool_started[(invocation_id, tool_name)] = time.monotonic()
        return None

    async def after_tool(self, invocation_context, tool, args, response) -> None:
        """Record tool call latency."""
        invocation_id = getattr(invocation_context, "invocation_id", "")
        tool_name = getattr(tool, "name", "") or ""
        started = self._tool_started.pop((invocation_id, tool_name), None)
        if started is not None:
            latency_ms = (time.monotonic() - started) * 1000
            self._metrics.observe("tenant_tool_latency_ms", {
                "tenant_id": self._tenant_id,
                "tool": tool_name
            }, latency_ms)
        return None
