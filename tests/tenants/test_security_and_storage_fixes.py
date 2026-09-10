# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Regression tests for tenant security, consistency and wiring fixes.

Covers:

- Admin API secret masking (api_key / webhook_secret / webhook_token).
- Telegram webhook secret-token verification.
- IM user whitelist enforcement on the webhook path.
- Redis fallback recovery probes (dedup + rate limiter + usage tracker).
- TenantContextManager thread/task isolation and nesting.
- SQL audit write against the canonical ``audit_logs`` schema.
- Full-fidelity event serialization (function calls survive storage).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import threading
import time

import pytest

from trpc_agent_sdk.events import Event
from trpc_agent_sdk.tenants import (
    ChannelConfig,
    InMemoryTenantStore,
    MessageDeduplicator,
    ModelConfig,
    RateLimiter,
    Tenant,
    TenantChannelManager,
    TenantMessage,
)
from trpc_agent_sdk.tenants._im_transport import _REDIS_RETRY_INTERVAL_SECONDS
from trpc_agent_sdk.tenants._im_transport import RedisFallbackMixin
from trpc_agent_sdk.tenants._tenant_admin_api import SECRET_MASK, TenantAdminService
from trpc_agent_sdk.tenants._tenant_channels import WeComTenantAdapter
from trpc_agent_sdk.tenants._tenant_context import TenantContextManager
from trpc_agent_sdk.tenants._tenant_governance import TenantUsageTracker
from trpc_agent_sdk.types import Content, FunctionCall, Part

from datetime import datetime


def make_message(message_id: str = "m1", tenant_id: str = "acme", user_id: str = "u1") -> TenantMessage:
    return TenantMessage(
        tenant_id=tenant_id,
        channel_type="telegram",
        user_id=user_id,
        chat_id="c1",
        message_id=message_id,
        content="hello",
        metadata={},
        timestamp=datetime.utcnow(),
    )


def telegram_payload(message_id: int, user_id: int, text: str = "hi") -> dict:
    return {
        "message": {
            "message_id": message_id,
            "from": {
                "id": user_id,
                "username": f"user{user_id}"
            },
            "chat": {
                "id": 7,
                "type": "private"
            },
            "text": text,
        }
    }


@pytest.fixture
async def secret_store() -> InMemoryTenantStore:
    """Tenant with a Telegram webhook secret and a user whitelist."""
    store = InMemoryTenantStore()
    await store.create_tenant(
        Tenant(
            tenant_id="acme",
            name="ACME",
            model_config=ModelConfig(api_key="sk-super-secret"),
            channel_configs={
                "telegram":
                ChannelConfig(
                    channel_type="telegram",
                    api_key="tok_acme",
                    webhook_secret="telegram_secret",
                    webhook_token="wecom_tok_acme",
                    allowed_users=["1"],
                ),
            },
        ))
    return store


class TestTelegramSecretTokenVerification:

    async def test_missing_secret_token_rejected(self, secret_store):
        manager = TenantChannelManager(secret_store)
        message = await manager.handle_webhook("telegram", telegram_payload(1, 1), {"X-Telegram-Bot-Token": "tok_acme"})
        assert message is None

    async def test_wrong_secret_token_rejected(self, secret_store):
        manager = TenantChannelManager(secret_store)
        headers = {
            "X-Telegram-Bot-Token": "tok_acme",
            "X-Telegram-Bot-Api-Secret-Token": "forged",
        }
        message = await manager.handle_webhook("telegram", telegram_payload(1, 1), headers)
        assert message is None

    async def test_correct_secret_token_accepted(self, secret_store):
        manager = TenantChannelManager(secret_store)
        headers = {
            "X-Telegram-Bot-Token": "tok_acme",
            "X-Telegram-Bot-Api-Secret-Token": "telegram_secret",
        }
        message = await manager.handle_webhook("telegram", telegram_payload(1, 1), headers)
        assert message is not None
        assert message.tenant_id == "acme"

    async def test_header_lookup_is_case_insensitive(self, secret_store):
        manager = TenantChannelManager(secret_store)
        headers = {
            "x-telegram-bot-token": "tok_acme",
            "x-telegram-bot-api-secret-token": "telegram_secret",
        }
        message = await manager.handle_webhook("telegram", telegram_payload(1, 1), headers)
        assert message is not None


class TestImUserWhitelist:

    async def test_user_not_in_whitelist_rejected(self, secret_store):
        manager = TenantChannelManager(secret_store)
        headers = {
            "X-Telegram-Bot-Token": "tok_acme",
            "X-Telegram-Bot-Api-Secret-Token": "telegram_secret",
        }
        message = await manager.handle_webhook("telegram", telegram_payload(2, 42), headers)
        assert message is None

    async def test_whitelisted_user_accepted(self, secret_store):
        manager = TenantChannelManager(secret_store)
        headers = {
            "X-Telegram-Bot-Token": "tok_acme",
            "X-Telegram-Bot-Api-Secret-Token": "telegram_secret",
        }
        # Telegram "from.id" 1 maps to user_id "1" in the adapter; whitelist
        # entries use the platform user id.
        message = await manager.handle_webhook("telegram", telegram_payload(3, 1), headers)
        assert message is not None
        assert message.user_id == "1"


class TestRedisFallbackRecovery:

    async def test_dedup_retries_redis_after_cooldown(self):
        dedup = MessageDeduplicator(redis_url="redis://127.0.0.1:1/0")
        assert await dedup.is_duplicate(make_message("m1")) is False
        # Failure recorded and Redis skipped while inside the cooldown window.
        assert dedup._redis_failed_at is not None
        assert dedup._redis_down() is True

        # After the cooldown the shared backend is probed again (it fails
        # again here since there is no server, but the sticky flag resets).
        dedup._redis_failed_at = time.monotonic() - (_REDIS_RETRY_INTERVAL_SECONDS + 1)
        assert dedup._redis_down() is False
        assert await dedup.is_duplicate(make_message("m1")) is True
        assert dedup._redis_failed_at is not None

    async def test_rate_limiter_retries_redis_after_cooldown(self):
        limiter = RateLimiter(redis_url="redis://127.0.0.1:1/0")
        assert await limiter.acquire("t", "telegram", 10) is True
        assert limiter._redis_failed_at is not None
        assert limiter._redis_down() is True

        limiter._redis_failed_at = time.monotonic() - (_REDIS_RETRY_INTERVAL_SECONDS + 1)
        assert limiter._redis_down() is False
        # Still enforces the limit through the in-memory fallback.
        for _ in range(10):
            await limiter.acquire("t", "telegram", 10)
        assert await limiter.acquire("t", "telegram", 10) is False

    def test_retry_interval_matches_across_modules(self):
        # All Redis-backed components share one retry interval via the mixin.
        assert RedisFallbackMixin._redis_retry_interval == _REDIS_RETRY_INTERVAL_SECONDS


class TestTenantContextThreadIsolation:

    async def test_context_not_visible_across_threads(self):
        manager = TenantContextManager()
        seen_in_thread = {}

        class _FakeTenant:
            tenant_id = "acme"

        manager.set_current_context(_FakeTenant())

        def _read():
            seen_in_thread["context"] = manager.get_current_context("acme")

        thread = threading.Thread(target=_read)
        thread.start()
        thread.join()

        assert seen_in_thread["context"] is None
        assert manager.get_current_context("acme") is not None


class TestAdminSecretMasking:

    async def test_dump_masks_secrets(self, secret_store):
        service = TenantAdminService(secret_store)
        dumped = await service.get_tenant("acme")

        assert dumped["llm_config"]["api_key"] == SECRET_MASK
        channel = dumped["channel_configs"]["telegram"]
        assert channel["api_key"] == SECRET_MASK
        assert channel["webhook_secret"] == SECRET_MASK
        assert channel["webhook_token"] == SECRET_MASK
        assert dumped["channel_configs"]["telegram"]["allowed_users"] == ["1"]

    async def test_update_with_mask_keeps_stored_secret(self, secret_store):
        service = TenantAdminService(secret_store)
        payload = await service.get_tenant("acme")

        # Client echoes the masked values back together with a rename.
        payload["name"] = "ACME Renamed"
        updated = await service.update_tenant("acme", payload)
        assert updated["name"] == "ACME Renamed"

        tenant = await secret_store.get_tenant("acme")
        assert tenant.model_config.api_key == "sk-super-secret"
        assert tenant.get_channel_config("telegram").webhook_secret == "telegram_secret"
        assert tenant.get_channel_config("telegram").webhook_token == "wecom_tok_acme"

    async def test_update_with_new_secret_replaces(self, secret_store):
        service = TenantAdminService(secret_store)
        payload = await service.get_tenant("acme")
        payload["llm_config"]["api_key"] = "sk-brand-new"

        await service.update_tenant("acme", payload)
        tenant = await secret_store.get_tenant("acme")
        assert tenant.model_config.api_key == "sk-brand-new"


class TestSqlAuditAndEventFidelity:

    async def test_sql_audit_write_matches_canonical_ddl(self, tmp_path):
        pytest.importorskip("aiosqlite")
        from trpc_agent_sdk.tenants import SQLStorageBackend

        path = str(tmp_path / "audit.db").replace("\\", "/")
        backend = SQLStorageBackend(f"sqlite:///{path}")

        audit_data = {
            "tenant_id": "acme",
            "channel": "telegram",
            "user_id": "u1",
            "session_id": "s1",
            "agent_name": "assistant",
            "tool_name": "search",
            "decision": "allow",
            "latency_ms": 12.5,
            "error_type": "",
            "cost_usd": 0.01,
            "trace_id": "trace-123",
            "details": {
                "reason": "ok"
            },
        }
        await backend.save_audit_log("acme", audit_data)

        # Read back through the canonical schema with plain sqlite3.
        conn = sqlite3.connect(path)
        try:
            row = conn.execute("SELECT channel, user_id, decision, latency_ms, trace_id, details_json FROM audit_logs "
                               "WHERE tenant_id = 'acme'").fetchone()
        finally:
            conn.close()

        assert row is not None
        assert row[0] == "telegram"
        assert row[1] == "u1"
        assert row[2] == "allow"
        assert row[3] == pytest.approx(12.5)
        assert row[4] == "trace-123"
        assert json.loads(row[5]) == {"reason": "ok"}

    async def test_sql_event_serialization_keeps_function_calls(self, tmp_path):
        pytest.importorskip("aiosqlite")
        from trpc_agent_sdk.tenants import SQLStorageBackend

        path = str(tmp_path / "events.db").replace("\\", "/")
        backend = SQLStorageBackend(f"sqlite:///{path}")

        event = Event(author="assistant",
                      content=Content(parts=[Part(function_call=FunctionCall(name="my_tool", args={"k": "v"}))]))
        await backend.add_session_event("acme", "s1", event)

        conn = sqlite3.connect(path)
        try:
            raw = conn.execute(
                "SELECT event_json FROM unified_events WHERE tenant_id = 'acme' AND session_id = 's1'").fetchone()
        finally:
            conn.close()

        assert raw is not None
        stored = json.loads(raw[0])
        parts = stored["content"]["parts"]
        assert parts[0]["function_call"]["name"] == "my_tool"
        assert parts[0]["function_call"]["args"] == {"k": "v"}

    def test_serialize_event_roundtrip_full_fidelity(self):
        from trpc_agent_sdk.tenants._unified_storage import serialize_event

        event = Event(author="assistant",
                      content=Content(parts=[Part(function_call=FunctionCall(name="t", args={"a": 1}))]))
        restored = Event.model_validate(json.loads(serialize_event(event)))
        assert restored.get_function_calls()[0].name == "t"

    async def test_sql_audit_tolerates_malformed_numbers(self, tmp_path):
        pytest.importorskip("aiosqlite")
        from trpc_agent_sdk.tenants import SQLStorageBackend

        path = str(tmp_path / "audit_bad.db").replace("\\", "/")
        backend = SQLStorageBackend(f"sqlite:///{path}")

        audit_data = {
            "tenant_id": "acme",
            "decision": "allow",
            "latency_ms": "not-a-number",
            "cost_usd": None,
        }
        # A malformed numeric field must not abort the whole audit write.
        await backend.save_audit_log("acme", audit_data)

        conn = sqlite3.connect(path)
        try:
            row = conn.execute("SELECT latency_ms, cost_usd FROM audit_logs WHERE tenant_id = 'acme'").fetchone()
        finally:
            conn.close()

        assert row is not None
        assert row[0] == 0.0
        assert row[1] == 0.0


class TestWeComHeaderCaseInsensitivity:

    async def test_token_from_lowercased_header(self):
        store = InMemoryTenantStore()
        await store.create_tenant(
            Tenant(
                tenant_id="acme",
                name="ACME",
                channel_configs={
                    "wecom": ChannelConfig(
                        channel_type="wecom",
                        webhook_token="wecom_tok",
                        webhook_secret="wecom_sec",
                    ),
                },
            ))
        adapter = WeComTenantAdapter(store)
        ts, nonce = "123", "abc"
        signature = hashlib.sha1("".join(sorted(["wecom_sec", ts, nonce])).encode()).hexdigest()
        # No token in the payload — it must be picked up from the header
        # regardless of the header's letter case.
        payload = {
            "timestamp": ts,
            "nonce": nonce,
            "signature": signature,
            "msg_type": "text",
            "from_user_id": "u1",
            "content": "hello",
            "msg_id": "w1",
        }
        message = await adapter.handle_webhook("wecom", payload, {"x-wecom-token": "wecom_tok"})
        assert message is not None
        assert message.tenant_id == "acme"


class TestUpdateTenantPartialPayload:

    async def test_partial_payload_keeps_config(self, secret_store):
        service = TenantAdminService(secret_store)
        updated = await service.update_tenant("acme", {"name": "ACME Renamed"})
        assert updated["name"] == "ACME Renamed"

        # Sections absent from the payload keep their stored values instead
        # of being reset by TenantConfig defaults.
        tenant = await secret_store.get_tenant("acme")
        assert tenant.model_config.api_key == "sk-super-secret"
        channel = tenant.get_channel_config("telegram")
        assert channel is not None
        assert channel.api_key == "tok_acme"
        assert channel.allowed_users == ["1"]

    async def test_masking_limited_to_secret_sections(self, secret_store):
        service = TenantAdminService(secret_store)
        payload = await service.get_tenant("acme")
        payload["custom_attributes"] = {"api_key": "not-a-secret", "note": "x"}
        dumped = await service.update_tenant("acme", payload)

        # Only llm_config / channel_configs are masked; a field that merely
        # shares a secret field's name elsewhere stays readable.
        assert dumped["custom_attributes"]["api_key"] == "not-a-secret"
        assert dumped["llm_config"]["api_key"] == SECRET_MASK


class TestNestedTenantContext:

    async def test_nested_same_tenant_keeps_outer_context(self):
        manager = TenantContextManager()

        class _FakeTenant:

            def __init__(self, tenant_id):
                self.tenant_id = tenant_id

        with manager.with_tenant(_FakeTenant("acme")):
            assert manager.get_current_context("acme") is not None
            with manager.with_tenant(_FakeTenant("acme")):
                assert manager.get_current_context("acme") is not None
            # The inner exit must not delete the outer scope's context.
            assert manager.get_current_context("acme") is not None
            assert manager.get_active_tenants() == ["acme"]
        assert manager.get_current_context("acme") is None

    async def test_concurrent_tasks_isolated(self):
        manager = TenantContextManager()

        class _FakeTenant:

            def __init__(self, tenant_id):
                self.tenant_id = tenant_id

        seen = {}

        async def _worker(tenant_id, other_id):
            with manager.with_tenant(_FakeTenant(tenant_id)):
                await asyncio.sleep(0.01)
                seen[tenant_id] = manager.get_current_context(tenant_id) is not None
                seen[f"{tenant_id}_sees_{other_id}"] = manager.get_current_context(other_id) is not None

        await asyncio.gather(_worker("acme", "other"), _worker("other", "acme"))
        assert seen["acme"] is True
        assert seen["other"] is True
        # Concurrent asyncio tasks must not observe each other's context.
        assert seen["acme_sees_other"] is False
        assert seen["other_sees_acme"] is False


class TestUsageTrackerRecovery:

    async def test_usage_tracker_retries_redis_after_cooldown(self):

        class _FakeTenant:
            tenant_id = "acme"

        tracker = TenantUsageTracker(redis_url="redis://127.0.0.1:1/0")
        await tracker.record_usage(_FakeTenant(), requests=1, estimated_cost_usd=0.5)
        assert tracker._redis_failed_at is not None
        assert tracker._redis_down() is True

        # After the cooldown the shared backend is probed again (it fails
        # again here since there is no server, but the sticky flag resets)
        # and local counters keep accumulating.
        tracker._redis_failed_at = time.monotonic() - (_REDIS_RETRY_INTERVAL_SECONDS + 1)
        assert tracker._redis_down() is False
        await tracker.record_usage(_FakeTenant(), requests=1, estimated_cost_usd=0.5)
        assert tracker._redis_failed_at is not None

        usage = await tracker.get_usage(_FakeTenant())
        assert usage["requests"] == 2.0
        assert usage["cost_usd"] == pytest.approx(1.0)
