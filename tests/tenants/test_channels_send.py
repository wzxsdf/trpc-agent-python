# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Unit tests for the IM reply pipeline (senders, chunking, dispatch)."""

from __future__ import annotations

import json

import httpx

from trpc_agent_sdk.events import Event
from trpc_agent_sdk.tenants import (
    ChannelConfig,
    InMemoryTenantStore,
    MessageChunker,
    RateLimiter,
    Tenant,
    TenantChannelManager,
    TenantMessage,
    TenantResponse,
    TelegramSender,
    WeComSender,
    send_with_retry,
)
from trpc_agent_sdk.tenants._audit import DECISION_IM_REPLY_FAILED, DECISION_IM_REPLIED
from trpc_agent_sdk.tenants._tenant_channels import TelegramTenantAdapter, WeComTenantAdapter
from trpc_agent_sdk.types import Content, Part

REDIS_UNREACHABLE = "redis://127.0.0.1:1/0"


def make_message(message_id: str = "m1", tenant_id: str = "acme") -> TenantMessage:
    return TenantMessage(
        tenant_id=tenant_id,
        channel_type="telegram",
        user_id="u1",
        chat_id="c1",
        message_id=message_id,
        content="hello",
        metadata={},
        timestamp=None,
    )


def make_text_event(text: str, partial: bool = False) -> Event:
    return Event(author="assistant", content=Content(parts=[Part.from_text(text=text)]), partial=partial)


async def make_telegram_store() -> InMemoryTenantStore:
    store = InMemoryTenantStore()
    await store.create_tenant(
        Tenant(
            tenant_id="acme",
            name="ACME",
            channel_configs={
                "telegram": ChannelConfig(channel_type="telegram", api_key="bot_token_abc", rate_limit_per_minute=60)
            },
        ))
    return store


async def make_wecom_store() -> InMemoryTenantStore:
    store = InMemoryTenantStore()
    await store.create_tenant(
        Tenant(
            tenant_id="acme",
            name="ACME",
            channel_configs={
                "wecom":
                ChannelConfig(
                    channel_type="wecom",
                    bot_id="corp_1",
                    api_key="corp_secret_1",
                    webhook_token="agent_1",
                )
            },
        ))
    return store


def telegram_mock(handler) -> httpx.AsyncClient:
    """Build an httpx client backed by a MockTransport for api.telegram.org."""
    transport = httpx.MockTransport(handler)
    return httpx.AsyncClient(transport=transport, base_url="https://api.telegram.org")


def wecom_mock(handler) -> httpx.AsyncClient:
    """Build an httpx client backed by a MockTransport for qyapi.weixin.qq.com."""
    transport = httpx.MockTransport(handler)
    return httpx.AsyncClient(transport=transport, base_url="https://qyapi.weixin.qq.com")


class TestMessageChunker:

    def test_short_text_single_chunk(self):
        assert MessageChunker.split_text("hello", 100) == ["hello"]

    def test_platform_limits(self):
        assert MessageChunker.limit_for("telegram") == 4096
        assert MessageChunker.limit_for("wecom") == 2048
        assert MessageChunker.limit_for("unknown") == 4096

    def test_long_text_split_respects_limit(self):
        text = "a" * 5000
        chunks = MessageChunker.split_text(text, 4096)
        assert all(len(chunk) <= 4096 for chunk in chunks)
        assert "".join(chunks).replace("\n", "") == text

    def test_prefers_newline_cut(self):
        text = ("a" * 100) + "\n" + ("b" * 100)
        chunks = MessageChunker.split_text(text, 150)
        assert len(chunks) == 2
        assert chunks[0] == "a" * 100

    def test_empty_text_yields_single_chunk(self):
        assert MessageChunker.split_text("", 100) == [""]


class TestRateLimiter:

    async def test_local_fallback_enforces_limit(self):
        limiter = RateLimiter(redis_url=REDIS_UNREACHABLE)
        assert await limiter.acquire("acme", "telegram", 2) is True
        assert await limiter.acquire("acme", "telegram", 2) is True
        assert await limiter.acquire("acme", "telegram", 2) is False

    async def test_zero_limit_always_allows(self):
        limiter = RateLimiter(redis_url=REDIS_UNREACHABLE)
        for _ in range(5):
            assert await limiter.acquire("acme", "telegram", 0) is True


class TestSendWithRetry:

    async def test_success_first_attempt(self):
        calls = []

        async def send() -> bool:
            calls.append(1)
            return True

        async def sleep(delay: float) -> None:
            pass

        assert await send_with_retry(send, max_retries=3, sleep=sleep) is True
        assert len(calls) == 1

    async def test_succeeds_after_failures(self):
        calls = []
        delays = []

        async def sleep(delay: float) -> None:
            delays.append(delay)

        async def send() -> bool:
            calls.append(1)
            return len(calls) >= 3

        assert await send_with_retry(send, max_retries=3, sleep=sleep) is True
        assert len(calls) == 3
        assert delays == [0.5, 1.0]

    async def test_exhausts_retries(self):

        async def send() -> bool:
            return False

        async def sleep(delay: float) -> None:
            pass

        assert await send_with_retry(send, max_retries=2, sleep=sleep) is False

    async def test_exception_is_retried(self):
        calls = []

        async def sleep(delay: float) -> None:
            pass

        async def send() -> bool:
            calls.append(1)
            if len(calls) < 2:
                raise RuntimeError("transient")
            return True

        assert await send_with_retry(send, max_retries=3, sleep=sleep) is True


class TestTelegramSender:

    async def test_send_text_success(self):
        requests = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(200, json={"ok": True})

        sender = TelegramSender("bot_token_abc", http_client=telegram_mock(handler))
        assert await sender.send_text("c1", "hi", reply_to_message_id="m1") is True
        body = json.loads(requests[0].content)
        assert requests[0].url.path == "/botbot_token_abc/sendMessage"
        assert body["chat_id"] == "c1"
        assert body["text"] == "hi"
        assert body["reply_to_message_id"] == "m1"

    async def test_send_text_api_failure(self):
        sender = TelegramSender("bot_token_abc",
                                http_client=telegram_mock(lambda r: httpx.Response(200, json={"ok": False})))
        assert await sender.send_text("c1", "hi") is False

    async def test_card_uses_markdown(self):
        requests = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(200, json={"ok": True})

        sender = TelegramSender("tok", http_client=telegram_mock(handler))
        await sender.send_text("c1", "**hi**", parse_mode="Markdown")
        assert json.loads(requests[0].content)["parse_mode"] == "Markdown"

    async def test_send_image_uses_send_photo(self):
        requests = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(200, json={"ok": True})

        sender = TelegramSender("tok", http_client=telegram_mock(handler))
        assert await sender.send_image("c1", "https://example.com/pic.png", caption="cap") is True
        assert requests[0].url.path.endswith("/sendPhoto")

    async def test_adapter_send_response_end_to_end(self):
        store = await make_telegram_store()
        requests = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(200, json={"ok": True})

        adapter = TelegramTenantAdapter(store, http_client=telegram_mock(handler))
        response = TenantResponse(
            tenant_id="acme",
            channel_type="telegram",
            user_id="u1",
            chat_id="c1",
            content="reply text",
            reply_to_message_id="m1",
        )
        assert await adapter.send_response(response) is True
        assert len(requests) == 1

    async def test_adapter_missing_config_fails(self):
        store = InMemoryTenantStore()
        await store.create_tenant(Tenant(tenant_id="acme", name="ACME"))
        adapter = TelegramTenantAdapter(store)
        response = TenantResponse(tenant_id="acme", channel_type="telegram", user_id="u1", chat_id="c1", content="x")
        assert await adapter.send_response(response) is False


class TestWeComSender:

    async def test_send_text_with_token_fetch_and_cache(self):
        requests = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            if request.url.path == "/cgi-bin/gettoken":
                return httpx.Response(200, json={"errcode": 0, "access_token": "AT", "expires_in": 7200})
            return httpx.Response(200, json={"errcode": 0})

        sender = WeComSender("corp_1", "secret_1", "agent_1", http_client=wecom_mock(handler))
        assert await sender.send_text("u1", "hello") is True
        assert await sender.send_text("u1", "again") is True
        paths = [r.url.path for r in requests]
        # access token fetched once, then cached
        assert paths.count("/cgi-bin/gettoken") == 1
        assert paths.count("/cgi-bin/message/send") == 2
        send_body = json.loads(requests[-1].content)
        assert send_body["touser"] == "u1"
        assert send_body["text"]["content"] == "again"

    async def test_token_fetch_failure(self):
        sender = WeComSender("corp_1",
                             "secret_1",
                             "agent_1",
                             http_client=wecom_mock(lambda r: httpx.Response(200, json={"errcode": 40013})))
        assert await sender.send_text("u1", "hello") is False

    async def test_message_send_failure(self):

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/cgi-bin/gettoken":
                return httpx.Response(200, json={"errcode": 0, "access_token": "AT", "expires_in": 7200})
            return httpx.Response(200, json={"errcode": 81013})

        sender = WeComSender("corp_1", "secret_1", "agent_1", http_client=wecom_mock(handler))
        assert await sender.send_text("u1", "hello") is False

    async def test_adapter_send_response_end_to_end(self):
        store = await make_wecom_store()
        requests = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            if request.url.path == "/cgi-bin/gettoken":
                return httpx.Response(200, json={"errcode": 0, "access_token": "AT", "expires_in": 7200})
            return httpx.Response(200, json={"errcode": 0})

        adapter = WeComTenantAdapter(store, http_client=wecom_mock(handler))
        response = TenantResponse(tenant_id="acme", channel_type="wecom", user_id="u1", chat_id="c1", content="reply")
        assert await adapter.send_response(response) is True
        send_request = requests[-1]
        assert send_request.url.params["access_token"] == "AT"
        assert send_request.url.path == "/cgi-bin/message/send"


def make_manager(store: InMemoryTenantStore, handler) -> TenantChannelManager:
    """Build a TenantChannelManager wired to a MockTransport handler."""
    manager = TenantChannelManager(store, redis_url=REDIS_UNREACHABLE)
    manager.register_adapter("telegram", TelegramTenantAdapter(store, http_client=telegram_mock(handler)))
    return manager


class TestDispatchAgentEvents:

    async def test_reply_delivered_and_audited(self):
        store = await make_telegram_store()
        requests = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(200, json={"ok": True})

        audit_entries = []
        manager = make_manager(store, handler)
        manager._audit_logger_factory = lambda tenant_id: _ListAuditLogger(tenant_id, audit_entries)

        async def events():
            yield make_text_event("Hello ")
            yield make_text_event("world!", partial=False)

        results = await manager.dispatch_agent_events(make_message(), events())
        assert results == [True]
        assert len(requests) == 1
        assert json.loads(requests[0].content)["text"] == "Hello world!"
        assert audit_entries[-1]["decision"] == DECISION_IM_REPLIED
        assert audit_entries[-1]["session_id"] == "acme:telegram:chat:c1"

    async def test_partial_events_are_skipped(self):
        store = await make_telegram_store()
        requests = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(200, json={"ok": True})

        manager = make_manager(store, handler)

        async def events():
            yield make_text_event("partial", partial=True)
            yield make_text_event("final")

        results = await manager.dispatch_agent_events(make_message(), events())
        assert results == [True]
        assert json.loads(requests[0].content)["text"] == "final"

    async def test_long_reply_is_chunked(self):
        store = await make_telegram_store()
        requests = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(200, json={"ok": True})

        manager = make_manager(store, handler)

        async def events():
            yield make_text_event("a" * 5000)

        results = await manager.dispatch_agent_events(make_message(), events())
        assert len(results) == 2
        assert all(results)
        assert len(requests) == 2

    async def test_empty_reply_sends_nothing(self):
        store = await make_telegram_store()
        requests = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(200, json={"ok": True})

        manager = make_manager(store, handler)

        async def events():
            yield make_text_event("partial-only", partial=True)
            yield Event(author="assistant", content=Content(parts=[]), partial=False)

        results = await manager.dispatch_agent_events(make_message(), events())
        assert results == []
        assert requests == []

    async def test_failed_delivery_is_audited(self):
        store = await make_telegram_store()
        audit_entries = []
        manager = make_manager(store, lambda r: httpx.Response(500, json={"ok": False}))
        manager._audit_logger_factory = lambda tenant_id: _ListAuditLogger(tenant_id, audit_entries)

        async def events():
            yield make_text_event("hello")

        results = await manager.dispatch_agent_events(make_message(), events())
        assert results == [False]
        assert audit_entries[-1]["decision"] == DECISION_IM_REPLY_FAILED
        assert audit_entries[-1]["error_type"] == "IMDeliveryError"


class TestRunAndReply:

    async def test_full_loop_routes_session_and_content(self):
        store = await make_telegram_store()
        requests = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(200, json={"ok": True})

        manager = make_manager(store, handler)
        captured = {}

        class FakeRunner:
            # Mimics TenantRunner.run_async: an async-generator *function*
            # (calling it returns the async generator without awaiting).

            def run_async(self, **kwargs):
                captured.update(kwargs)

                async def gen():
                    yield make_text_event("agent reply")

                return gen()

        results = await manager.run_and_reply(FakeRunner(), make_message("m9"))
        assert results == [True]
        assert captured["user_id"] == "u1"
        assert captured["session_id"] == "acme:telegram:chat:c1"
        assert captured["new_message"].parts[0].text == "hello"
        assert json.loads(requests[0].content)["text"] == "agent reply"
        assert json.loads(requests[0].content)["reply_to_message_id"] == "m9"


class _ListAuditLogger:
    """Minimal audit logger appending entries to a list for assertions."""

    def __init__(self, tenant_id: str, sink: list):
        self._tenant_id = tenant_id
        self._sink = sink

    async def log_event(self, **kwargs) -> None:
        from trpc_agent_sdk.tenants import AuditLog

        entry = AuditLog(tenant_id=self._tenant_id, **kwargs)
        self._sink.append(entry.to_dict())
