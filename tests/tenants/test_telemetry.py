# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Unit tests for tenant telemetry (tracing, metrics, audit trace ids)."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import httpx
import pytest
from opentelemetry import trace

from trpc_agent_sdk.events import Event
from trpc_agent_sdk.sessions import InMemorySessionService
from trpc_agent_sdk.tenants import (
    AuditLog,
    ChannelConfig,
    InMemoryTenantStore,
    Tenant,
    TenantAuditLogger,
    TenantChannelManager,
    TenantConfig,
    TenantContext,
    TenantMessage,
    TenantRunner,
    TenantTelemetryHooks,
    TenantAwareSessionService,
    TelegramTenantAdapter,
    current_trace_id,
    extract_trace_headers,
    get_tenant_metrics,
    inject_trace_headers,
    reset_tenant_metrics,
)
from trpc_agent_sdk.tenants._audit import DECISION_IM_REPLY_FAILED
from trpc_agent_sdk.tenants._tenant_telemetry import TenantMetrics
from trpc_agent_sdk.types import Content, GenerateContentResponseUsageMetadata, Part

REDIS_UNREACHABLE = "redis://127.0.0.1:1/0"


@pytest.fixture(autouse=True)
def _clean_metrics():
    """Reset the global metrics registry around every test."""
    reset_tenant_metrics()
    yield
    reset_tenant_metrics()


@pytest.fixture(scope="module")
def span_exporter():
    """Install an SDK tracer provider with an in-memory exporter once."""
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    return exporter


def make_tenant() -> Tenant:
    config = TenantConfig(tenant_id="acme", name="ACME")
    return config.to_tenant()


def make_context() -> TenantContext:
    return TenantContext(make_tenant())


def make_text_event(text: str, usage: GenerateContentResponseUsageMetadata = None) -> Event:
    return Event(author="assistant",
                 content=Content(parts=[Part.from_text(text=text)]),
                 partial=False,
                 usage_metadata=usage)


def make_message() -> TenantMessage:
    return TenantMessage(
        tenant_id="acme",
        channel_type="telegram",
        user_id="u1",
        chat_id="c1",
        message_id="m1",
        content="hello",
        metadata={},
        timestamp=None,
    )


class TestCurrentTraceId:

    def test_empty_outside_span(self):
        assert current_trace_id() == ""

    def test_valid_inside_span(self, span_exporter):
        with trace.get_tracer("test").start_as_current_span("op"):
            trace_id = current_trace_id()
            assert len(trace_id) == 32
            int(trace_id, 16)


class TestTracePropagation:

    async def test_inject_and_extract_roundtrip(self, span_exporter):
        tracer = trace.get_tracer("test")
        with tracer.start_as_current_span("origin"):
            trace_id = current_trace_id()
            headers: dict = {}
            inject_trace_headers(headers)
            assert "traceparent" in headers

        # Simulate a downstream node: the detached context of a new task
        # carries no span, only the injected headers.
        async def downstream() -> str:
            with extract_trace_headers(headers):
                assert current_trace_id() == trace_id
                with tracer.start_as_current_span("child"):
                    return current_trace_id()

        child_trace_id = await asyncio.ensure_future(downstream())
        assert child_trace_id == trace_id

    def test_extract_without_traceparent_is_noop(self, span_exporter):
        with extract_trace_headers({}):
            with trace.get_tracer("test").start_as_current_span("root"):
                assert len(current_trace_id()) == 32


class TestTenantMetrics:

    def test_counter_increment_and_labels(self):
        metrics = TenantMetrics()
        metrics.incr("tenant_requests_total", {"tenant_id": "acme"})
        metrics.incr("tenant_requests_total", {"tenant_id": "acme"})
        metrics.incr("tenant_requests_total", {"tenant_id": "globex"})
        assert metrics.get_counter("tenant_requests_total", {"tenant_id": "acme"}) == 2
        assert metrics.get_counter("tenant_requests_total", {"tenant_id": "globex"}) == 1
        assert metrics.get_counter("tenant_requests_total", {"tenant_id": "other"}) == 0

    def test_render_prometheus_counter(self):
        metrics = TenantMetrics()
        metrics.incr("tenant_requests_total", {"tenant_id": "acme"}, 3)
        text = metrics.render_prometheus()
        assert "# HELP tenant_requests_total" in text
        assert "# TYPE tenant_requests_total counter" in text
        assert 'tenant_requests_total{tenant_id="acme"} 3' in text

    def test_render_prometheus_histogram(self):
        metrics = TenantMetrics()
        metrics.observe("tenant_run_latency_ms", {"tenant_id": "acme"}, 5)
        metrics.observe("tenant_run_latency_ms", {"tenant_id": "acme"}, 500)
        text = metrics.render_prometheus()
        assert "# TYPE tenant_run_latency_ms histogram" in text
        assert 'tenant_run_latency_ms_bucket{le="10",tenant_id="acme"} 1' in text
        assert 'tenant_run_latency_ms_bucket{le="500",tenant_id="acme"} 2' in text
        assert 'tenant_run_latency_ms_bucket{le="+Inf",tenant_id="acme"} 2' in text
        assert 'tenant_run_latency_ms_count{tenant_id="acme"} 2' in text
        assert "tenant_run_latency_ms_sum" in text

    def test_label_value_escaping(self):
        metrics = TenantMetrics()
        metrics.incr("tenant_errors_total", {"tenant_id": 'a"b\\c'}, 1)
        text = metrics.render_prometheus()
        assert 'tenant_id="a\\"b\\\\c"' in text

    def test_global_registry_and_reset(self):
        get_tenant_metrics().incr("tenant_requests_total", {"tenant_id": "acme"})
        assert get_tenant_metrics().get_counter("tenant_requests_total", {"tenant_id": "acme"}) == 1
        reset_tenant_metrics()
        assert get_tenant_metrics().get_counter("tenant_requests_total", {"tenant_id": "acme"}) == 0


class TestTenantTelemetryHooks:

    async def test_model_and_tool_latency_recorded(self):
        metrics = TenantMetrics()
        hooks = TenantTelemetryHooks("acme", metrics=metrics)
        ctx = SimpleNamespace(invocation_id="inv1")
        tool = SimpleNamespace(name="search")

        await hooks.before_model(ctx, None)
        await hooks.after_model(ctx, None)
        await hooks.before_tool(ctx, tool, {}, None)
        await hooks.after_tool(ctx, tool, {}, None)

        text = metrics.render_prometheus()
        assert 'tenant_model_latency_ms_count{tenant_id="acme"} 1' in text
        assert 'tenant_tool_latency_ms_count{tenant_id="acme",tool="search"} 1' in text

    async def test_after_without_before_is_noop(self):
        metrics = TenantMetrics()
        hooks = TenantTelemetryHooks("acme", metrics=metrics)
        ctx = SimpleNamespace(invocation_id="inv2")
        await hooks.after_model(ctx, None)
        await hooks.after_tool(ctx, SimpleNamespace(name="search"), {}, None)
        assert metrics.render_prometheus() == ""


class TestRunnerMetrics:

    class _FakeBaseRunner:
        """Yields two events with usage metadata, then records completion."""

        def run_async(self, **kwargs):

            async def gen():
                usage = GenerateContentResponseUsageMetadata(prompt_token_count=10, candidates_token_count=5)
                yield make_text_event("Hello", usage)
                yield make_text_event(" world", usage)

            return gen()

    class _FailingBaseRunner:

        def run_async(self, **kwargs):

            async def gen():
                yield make_text_event("partial")
                raise RuntimeError("model exploded")

            return gen()

    async def test_run_counts_requests_tokens_and_latency(self):
        metrics = get_tenant_metrics()
        runner = TenantRunner(make_context(), self._FakeBaseRunner(),
                              TenantAwareSessionService(make_context(), InMemorySessionService()))
        events = [event async for event in runner.run_async(user_id="u1", session_id="s1", new_message=None)]
        assert "".join(e.get_text() for e in events) == "Hello world"

        assert metrics.get_counter("tenant_requests_total", {"tenant_id": "acme"}) == 1
        assert metrics.get_counter("tenant_tokens_total", {"tenant_id": "acme", "type": "input"}) == 20
        assert metrics.get_counter("tenant_tokens_total", {"tenant_id": "acme", "type": "output"}) == 10
        text = metrics.render_prometheus()
        assert 'tenant_run_latency_ms_count{tenant_id="acme"} 1' in text

    async def test_run_error_counts_and_reraises(self):
        metrics = get_tenant_metrics()
        runner = TenantRunner(make_context(), self._FailingBaseRunner(),
                              TenantAwareSessionService(make_context(), InMemorySessionService()))
        with pytest.raises(RuntimeError):
            async for _ in runner.run_async(user_id="u1", session_id="s1", new_message=None):
                pass
        assert metrics.get_counter("tenant_errors_total", {"tenant_id": "acme", "error_type": "RuntimeError"}) == 1

    async def test_run_span_created(self, span_exporter):
        runner = TenantRunner(make_context(), self._FakeBaseRunner(),
                              TenantAwareSessionService(make_context(), InMemorySessionService()))
        async for _ in runner.run_async(user_id="u1", session_id="s1", new_message=None):
            pass
        names = [span.name for span in span_exporter.get_finished_spans()]
        assert "tenant.runner.run" in names


class TestAuditTraceId:

    async def test_log_event_fills_trace_id_inside_span(self, span_exporter):
        captured = []

        async def writer(data: dict) -> None:
            captured.append(data)

        audit = TenantAuditLogger("acme", writer=writer)
        with trace.get_tracer("test").start_as_current_span("op"):
            expected = current_trace_id()
            await audit.log_event(decision="allow")

        assert captured[0]["trace_id"] == expected

    async def test_log_event_keeps_explicit_trace_id(self, span_exporter):
        captured = []

        async def writer(data: dict) -> None:
            captured.append(data)

        audit = TenantAuditLogger("acme", writer=writer)
        await audit.log_event(decision="allow", trace_id="explicit")
        assert captured[0]["trace_id"] == "explicit"


class TestSessionBackendLatency:

    async def test_create_and_get_record_latency(self):
        metrics = get_tenant_metrics()
        context = make_context()
        service = TenantAwareSessionService(context, InMemorySessionService())
        await service.create_session(app_name="app", user_id="u1", session_id="s1")
        await service.get_session(app_name="app", user_id="u1", session_id="s1")
        text = metrics.render_prometheus()
        assert 'tenant_session_backend_latency_ms_count{operation="create",tenant_id="acme"} 1' in text
        assert 'tenant_session_backend_latency_ms_count{operation="get",tenant_id="acme"} 1' in text


class TestChannelTelemetry:

    async def test_webhook_and_reply_spans_and_metrics(self, span_exporter):
        store = InMemoryTenantStore()
        await store.create_tenant(
            Tenant(
                tenant_id="acme",
                name="ACME",
                channel_configs={
                    "telegram": ChannelConfig(channel_type="telegram",
                                              api_key="bot_token_abc",
                                              rate_limit_per_minute=60)
                },
            ))

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"ok": True})

        manager = TenantChannelManager(store, redis_url=REDIS_UNREACHABLE)
        manager.register_adapter(
            "telegram",
            TelegramTenantAdapter(store,
                                  http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler),
                                                                base_url="https://api.telegram.org")))

        message = await manager.handle_webhook(
            "telegram",
            {"message": {
                "message_id": "m1",
                "text": "hi",
                "chat": {
                    "id": 1,
                    "type": "private"
                },
                "from": {
                    "id": 42
                },
            }}, {"X-Telegram-Bot-Token": "bot_token_abc"})
        assert message is not None

        async def events():
            yield make_text_event("reply")

        results = await manager.dispatch_agent_events(message, events())
        assert results == [True]

        assert get_tenant_metrics().get_counter("tenant_im_reply_total", {
            "tenant_id": "acme",
            "channel": "telegram",
            "result": "success"
        }) == 1

        names = [span.name for span in span_exporter.get_finished_spans()]
        assert "tenant.im.callback" in names
        assert "tenant.im.reply" in names

    async def test_failed_reply_counts(self):
        store = InMemoryTenantStore()
        await store.create_tenant(
            Tenant(
                tenant_id="acme",
                name="ACME",
                channel_configs={
                    "telegram": ChannelConfig(channel_type="telegram",
                                              api_key="bot_token_abc",
                                              rate_limit_per_minute=60)
                },
            ))

        audit_entries = []

        class _ListAuditLogger:

            def __init__(self, tenant_id: str):
                self._tenant_id = tenant_id

            async def log_event(self, **kwargs) -> None:
                entry = AuditLog(tenant_id=self._tenant_id, **kwargs)
                audit_entries.append(entry.to_dict())

        manager = TenantChannelManager(store, redis_url=REDIS_UNREACHABLE, audit_logger_factory=_ListAuditLogger)
        manager.register_adapter(
            "telegram",
            TelegramTenantAdapter(store,
                                  http_client=httpx.AsyncClient(
                                      transport=httpx.MockTransport(lambda r: httpx.Response(500, json={"ok": False})),
                                      base_url="https://api.telegram.org")))

        async def events():
            yield make_text_event("reply")

        results = await manager.dispatch_agent_events(make_message(), events())
        assert results == [False]
        assert get_tenant_metrics().get_counter("tenant_im_reply_total", {
            "tenant_id": "acme",
            "channel": "telegram",
            "result": "failed"
        }) == 1
        assert audit_entries[-1]["decision"] == DECISION_IM_REPLY_FAILED
