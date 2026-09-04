# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Unit tests for IM message deduplication and tenant channel routing."""

from __future__ import annotations

import hashlib

import pytest

from trpc_agent_sdk.tenants import (
    ChannelConfig,
    InMemoryTenantStore,
    MessageDeduplicator,
    Tenant,
    TenantChannelManager,
    TenantMessage,
)
from trpc_agent_sdk.tenants._tenant_channels import WeComTenantAdapter

from datetime import datetime


def make_message(message_id: str = "m1", tenant_id: str = "acme") -> TenantMessage:
    return TenantMessage(
        tenant_id=tenant_id,
        channel_type="telegram",
        user_id="u1",
        chat_id="c1",
        message_id=message_id,
        content="hello",
        metadata={},
        timestamp=datetime.utcnow(),
    )


@pytest.fixture
async def store() -> InMemoryTenantStore:
    store = InMemoryTenantStore()
    await store.create_tenant(
        Tenant(
            tenant_id="acme",
            name="ACME",
            channel_configs={
                "telegram": ChannelConfig(
                    channel_type="telegram",
                    api_key="tok_acme",
                ),
                "wecom": ChannelConfig(
                    channel_type="wecom",
                    webhook_token="wecom_tok",
                    webhook_secret="wecom_sec",
                ),
            },
        ))
    return store


class TestMessageDeduplicator:

    async def test_first_seen_not_duplicate(self):
        dedup = MessageDeduplicator(redis_url="redis://127.0.0.1:1/0")
        assert await dedup.is_duplicate(make_message("m1")) is False

    async def test_second_seen_is_duplicate(self):
        dedup = MessageDeduplicator(redis_url="redis://127.0.0.1:1/0")
        await dedup.is_duplicate(make_message("m2"))
        assert await dedup.is_duplicate(make_message("m2")) is True

    async def test_different_messages_not_duplicates(self):
        dedup = MessageDeduplicator(redis_url="redis://127.0.0.1:1/0")
        await dedup.is_duplicate(make_message("m3"))
        assert await dedup.is_duplicate(make_message("m4")) is False

    async def test_tenant_scoped_keys(self):
        """Same message id under different tenants is not a duplicate."""
        dedup = MessageDeduplicator(redis_url="redis://127.0.0.1:1/0")
        await dedup.is_duplicate(make_message("m5", tenant_id="acme"))
        assert await dedup.is_duplicate(make_message("m5", tenant_id="globex")) is False


class TestTenantChannelManager:

    async def test_telegram_webhook_routes_to_tenant(self, store: InMemoryTenantStore):
        manager = TenantChannelManager(store)
        payload = {
            "message": {
                "message_id": 100,
                "from": {
                    "id": 1,
                    "username": "alice"
                },
                "chat": {
                    "id": 7,
                    "type": "private"
                },
                "text": "hi",
            }
        }
        message = await manager.handle_webhook("telegram", payload, {"X-Telegram-Bot-Token": "tok_acme"})
        assert message is not None
        assert message.tenant_id == "acme"
        assert message.content == "hi"

    async def test_unknown_bot_token_rejected(self, store: InMemoryTenantStore):
        manager = TenantChannelManager(store)
        message = await manager.handle_webhook("telegram", {"message": {}}, {"X-Telegram-Bot-Token": "bad_token"})
        assert message is None

    async def test_duplicate_webhook_filtered(self, store: InMemoryTenantStore):
        manager = TenantChannelManager(store)
        payload = {
            "message": {
                "message_id": 200,
                "from": {
                    "id": 1,
                    "username": "alice"
                },
                "chat": {
                    "id": 7,
                    "type": "private"
                },
                "text": "once",
            }
        }
        headers = {"X-Telegram-Bot-Token": "tok_acme"}
        first = await manager.handle_webhook("telegram", payload, headers)
        second = await manager.handle_webhook("telegram", payload, headers)
        assert first is not None
        assert second is None

    async def test_session_id_scoped_by_tenant_and_chat(self, store: InMemoryTenantStore):
        manager = TenantChannelManager(store)
        message = make_message("m9", tenant_id="acme")
        session_id = manager.generate_session_id(message)
        assert session_id == "acme:telegram:chat:c1"


class TestWeComSignature:

    async def test_valid_signature_passes(self, store: InMemoryTenantStore):
        adapter = WeComTenantAdapter(store)
        ts, nonce = "123", "abc"
        signature = hashlib.sha1("".join(sorted(["wecom_sec", ts, nonce])).encode()).hexdigest()
        payload = {
            "token": "wecom_tok",
            "timestamp": ts,
            "nonce": nonce,
            "signature": signature,
            "msg_type": "text",
            "from_user_id": "u1",
            "content": "hello",
            "msg_id": "w1",
        }
        message = await adapter.handle_webhook("wecom", payload, {})
        assert message is not None
        assert message.tenant_id == "acme"

    async def test_invalid_signature_rejected(self, store: InMemoryTenantStore):
        adapter = WeComTenantAdapter(store)
        payload = {
            "token": "wecom_tok",
            "timestamp": "123",
            "nonce": "abc",
            "signature": "deadbeef",
            "msg_type": "text",
            "from_user_id": "u1",
            "content": "hello",
            "msg_id": "w2",
        }
        assert await adapter.handle_webhook("wecom", payload, {}) is None
