# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Integration tests for the real WeCom (企业微信) callback protocol path:
URL verification, encrypted-XML message callbacks, corp_id tenant routing
and the shared governance pipeline."""

import httpx
import pytest

pytest.importorskip("cryptography")

from trpc_agent_sdk.tenants import (  # noqa: E402
    ChannelConfig, InMemoryTenantStore, Tenant, TenantChannelManager,
)
from trpc_agent_sdk.tenants._tenant_channels import WeComTenantAdapter  # noqa: E402
from trpc_agent_sdk.tenants._wecom_crypto import WeComCrypto  # noqa: E402

# 43-char base64url keys (decode to 32 bytes).
AES_KEY = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ"
AES_KEY_2 = "ZYXWVUTSRQPONMLKJIHGFEDCBAzyxwvutsrqponmlkj"
TOKEN = "callback_token"
TOKEN_2 = "callback_token_two"
CORP_ACME = "ww_acme_corp_id"
CORCE_GLOBEX = "ww_globex_corp_id"
AGENT_ID = "1000002"


def wecom_config(**overrides) -> ChannelConfig:
    defaults = dict(channel_type="wecom",
                    webhook_token=TOKEN,
                    webhook_secret=AES_KEY,
                    bot_id=CORP_ACME,
                    api_key="corp_secret",
                    agent_id=AGENT_ID)
    defaults.update(overrides)
    return ChannelConfig(**defaults)


async def build_store(*tenants: Tenant) -> InMemoryTenantStore:
    store = InMemoryTenantStore()
    for tenant in tenants:
        await store.create_tenant(tenant)
    return store


def make_tenant(tenant_id: str, config: ChannelConfig) -> Tenant:
    return Tenant(tenant_id=tenant_id, name=tenant_id.upper(), channel_configs={"wecom": config})


def callback_query(crypto: WeComCrypto, encrypt: str, timestamp="1700000000", nonce="nonce1") -> dict:
    return {
        "msg_signature": crypto.signature(TOKEN, timestamp, nonce, encrypt),
        "timestamp": timestamp,
        "nonce": nonce,
    }


def encrypted_text_message(crypto: WeComCrypto,
                           corp_id: str,
                           user_id="u1",
                           msg_id="10001",
                           content="hello wecom",
                           agent_id=str(AGENT_ID)) -> str:
    inner = (f"<xml><ToUserName><![CDATA[{corp_id}]]></ToUserName>"
             f"<FromUserName><![CDATA[{user_id}]]></FromUserName>"
             f"<CreateTime>1700000000</CreateTime>"
             f"<MsgType><![CDATA[text]]></MsgType>"
             f"<Content><![CDATA[{content}]]></Content>"
             f"<MsgId>{msg_id}</MsgId>"
             f"<AgentID>{agent_id}</AgentID></xml>")
    return crypto.encrypt(inner, "1700000000", "nonce1")[0]


def encrypted_post_body(crypto: WeComCrypto, corp_id: str, encrypt: str) -> str:
    return (f"<xml><ToUserName><![CDATA[{corp_id}]]></ToUserName>"
            f"<Encrypt><![CDATA[{encrypt}]]></Encrypt>"
            f"<AgentID>{AGENT_ID}</AgentID></xml>")


@pytest.fixture
async def adapter():
    store = await build_store(make_tenant("acme", wecom_config()))
    return WeComTenantAdapter(store)


class TestUrlVerification:

    async def test_url_verification_routes_tenant(self, adapter):
        crypto = WeComCrypto(TOKEN, AES_KEY, CORP_ACME)
        echo_plain = "RANDOM_ECHO_12345"
        encrypt, msg_signature = crypto.encrypt(echo_plain, "1700000000", "n1")
        query = {
            "msg_signature": msg_signature,
            "timestamp": "1700000000",
            "nonce": "n1",
            "echostr": encrypt,
        }
        assert await adapter.verify_wecom_url(query) == echo_plain

    async def test_url_verification_wrong_tenant_rejected(self, adapter):
        # Signed with a different tenant's token.
        crypto = WeComCrypto("other_token", AES_KEY_2, "ww_other")
        encrypt, msg_signature = crypto.encrypt("echo", "1700000000", "n1")
        query = {
            "msg_signature": msg_signature,
            "timestamp": "1700000000",
            "nonce": "n1",
            "echostr": encrypt,
        }
        assert await adapter.verify_wecom_url(query) is None

    async def test_url_verification_missing_params_rejected(self, adapter):
        assert await adapter.verify_wecom_url({"msg_signature": "x"}) is None


class TestMessageCallback:

    async def test_encrypted_message_becomes_tenant_message(self, adapter):
        crypto = WeComCrypto(TOKEN, AES_KEY, CORP_ACME)
        encrypt = encrypted_text_message(crypto, CORP_ACME)
        message = await adapter.handle_wecom_callback(encrypted_post_body(crypto, CORP_ACME, encrypt),
                                                      callback_query(crypto, encrypt))

        assert message is not None
        assert message.tenant_id == "acme"
        assert message.channel_type == "wecom"
        assert message.user_id == "u1"
        assert message.chat_id == "u1"  # private message falls back to the sender id
        assert message.message_id == "10001"
        assert message.content == "hello wecom"
        assert message.is_group_chat is False
        assert message.metadata["agent_id"] == str(AGENT_ID)
        assert message.metadata["msg_type"] == "text"

    async def test_corp_id_routing_multi_tenant(self):
        store = await build_store(
            make_tenant("acme", wecom_config()),
            make_tenant(
                "globex",
                wecom_config(webhook_token=TOKEN_2, webhook_secret=AES_KEY_2, bot_id=CORCE_GLOBEX, agent_id="1000003")),
        )
        adapter = WeComTenantAdapter(store)

        crypto_globex = WeComCrypto(TOKEN_2, AES_KEY_2, CORCE_GLOBEX)
        encrypt = encrypted_text_message(crypto_globex, CORCE_GLOBEX, user_id="u2", msg_id="20001", agent_id="1000003")
        body = (f"<xml><ToUserName><![CDATA[{CORCE_GLOBEX}]]></ToUserName>"
                f"<Encrypt><![CDATA[{encrypt}]]></Encrypt></xml>")
        query = {
            "msg_signature": crypto_globex.signature(TOKEN_2, "1700000000", "nonce1", encrypt),
            "timestamp": "1700000000",
            "nonce": "nonce1",
        }
        message = await adapter.handle_wecom_callback(body, query)
        assert message is not None
        assert message.tenant_id == "globex"

    async def test_bad_signature_rejected(self, adapter):
        crypto = WeComCrypto(TOKEN, AES_KEY, CORP_ACME)
        encrypt = encrypted_text_message(crypto, CORP_ACME)
        query = callback_query(crypto, encrypt)
        query["msg_signature"] = "0" * 40
        message = await adapter.handle_wecom_callback(encrypted_post_body(crypto, CORP_ACME, encrypt), query)
        assert message is None

    async def test_unknown_corp_id_rejected(self, adapter):
        crypto = WeComCrypto(TOKEN, AES_KEY, CORP_ACME)
        encrypt = encrypted_text_message(crypto, CORP_ACME)
        body = encrypted_post_body(crypto, "ww_unknown_corp", encrypt)
        message = await adapter.handle_wecom_callback(body, callback_query(crypto, encrypt))
        assert message is None

    async def test_receiveid_tamper_rejected(self, adapter):
        # Encrypt a payload whose embedded receiveid differs from the
        # plaintext <ToUserName> — the corp_id check must reject it.
        crypto = WeComCrypto(TOKEN, AES_KEY, "ww_tampered")
        encrypt = encrypted_text_message(crypto, "ww_tampered")
        message = await adapter.handle_wecom_callback(encrypted_post_body(crypto, CORP_ACME, encrypt),
                                                      callback_query(crypto, encrypt))
        assert message is None

    async def test_same_corp_multi_app_disambiguation(self):
        # Same corp, two apps; each tenant pins its own agent_id.
        store = await build_store(
            make_tenant("app_one", wecom_config()),
            make_tenant("app_two", wecom_config(webhook_token=TOKEN_2, webhook_secret=AES_KEY_2, agent_id="1000003")),
        )
        adapter = WeComTenantAdapter(store)

        crypto_two = WeComCrypto(TOKEN_2, AES_KEY_2, CORP_ACME)
        encrypt = encrypted_text_message(crypto_two, CORP_ACME, agent_id="1000003")
        query = {
            "msg_signature": crypto_two.signature(TOKEN_2, "1700000000", "nonce1", encrypt),
            "timestamp": "1700000000",
            "nonce": "nonce1",
        }
        message = await adapter.handle_wecom_callback(encrypted_post_body(crypto_two, CORP_ACME, encrypt), query)
        assert message is not None
        assert message.tenant_id == "app_two"

    async def test_group_chat_uses_chat_id(self, adapter):
        crypto = WeComCrypto(TOKEN, AES_KEY, CORP_ACME)
        inner = ("<xml><ToUserName><![CDATA[ww_acme_corp_id]]></ToUserName>"
                 "<FromUserName><![CDATA[u1]]></FromUserName>"
                 "<CreateTime>1700000000</CreateTime>"
                 "<MsgType><![CDATA[text]]></MsgType>"
                 "<Content><![CDATA[group hello]]></Content>"
                 "<ChatId><![CDATA[wr_group_9]]></ChatId>"
                 "<MsgId>30001</MsgId></xml>")
        encrypt = crypto.encrypt(inner, "1700000000", "nonce1")[0]
        message = await adapter.handle_wecom_callback(encrypted_post_body(crypto, CORP_ACME, encrypt),
                                                      callback_query(crypto, encrypt))

        assert message is not None
        assert message.is_group_chat is True
        assert message.chat_id == "wr_group_9"

    async def test_event_message_placeholder(self, adapter):
        crypto = WeComCrypto(TOKEN, AES_KEY, CORP_ACME)
        inner = ("<xml><ToUserName><![CDATA[ww_acme_corp_id]]></ToUserName>"
                 "<FromUserName><![CDATA[u1]]></FromUserName>"
                 "<CreateTime>1700000000</CreateTime>"
                 "<MsgType><![CDATA[event]]></MsgType>"
                 "<Event><![CDATA[subscribe]]></Event>"
                 "<EventKey><![CDATA[KEY_1]]></EventKey>"
                 "<AgentID>1000002</AgentID></xml>")
        encrypt = crypto.encrypt(inner, "1700000000", "nonce1")[0]
        message = await adapter.handle_wecom_callback(encrypted_post_body(crypto, CORP_ACME, encrypt),
                                                      callback_query(crypto, encrypt))

        assert message is not None
        assert message.content == "[event:subscribe]"
        assert message.metadata["event"] == "subscribe"
        assert message.metadata["event_key"] == "KEY_1"
        # Event callbacks carry no MsgId; the derived id must be stable.
        assert message.message_id.startswith("wecom_evt_")

    async def test_invalid_xml_body_rejected(self, adapter):
        assert await adapter.handle_wecom_callback("not xml at all", {
            "msg_signature": "s",
            "timestamp": "t",
            "nonce": "n",
        }) is None

    async def test_missing_query_params_rejected(self, adapter):
        assert await adapter.handle_wecom_callback("<xml/>", {}) is None


class TestManagerPipeline:

    async def test_manager_accepts_and_dedups(self):
        store = await build_store(make_tenant("acme", wecom_config()))
        manager = TenantChannelManager(store)
        crypto = WeComCrypto(TOKEN, AES_KEY, CORP_ACME)
        encrypt = encrypted_text_message(crypto, CORP_ACME, msg_id="90001")
        body = encrypted_post_body(crypto, CORP_ACME, encrypt)

        first = await manager.handle_wecom_callback(body, callback_query(crypto, encrypt))
        second = await manager.handle_wecom_callback(body, callback_query(crypto, encrypt))
        assert first is not None
        assert second is None  # duplicate MsgId filtered by the pipeline

    async def test_manager_enforces_whitelist(self):
        store = await build_store(make_tenant("acme", wecom_config(allowed_users=["allowed_user"])))
        manager = TenantChannelManager(store)
        crypto = WeComCrypto(TOKEN, AES_KEY, CORP_ACME)
        encrypt = encrypted_text_message(crypto, CORP_ACME, user_id="stranger", msg_id="91001")
        message = await manager.handle_wecom_callback(encrypted_post_body(crypto, CORP_ACME, encrypt),
                                                      callback_query(crypto, encrypt))
        assert message is None

    async def test_manager_verify_wecom_url(self):
        store = await build_store(make_tenant("acme", wecom_config()))
        manager = TenantChannelManager(store)
        crypto = WeComCrypto(TOKEN, AES_KEY, CORP_ACME)
        encrypt, msg_signature = crypto.encrypt("echo_from_manager", "1700000000", "n9")
        echo = await manager.verify_wecom_url({
            "msg_signature": msg_signature,
            "timestamp": "1700000000",
            "nonce": "n9",
            "echostr": encrypt,
        })
        assert echo == "echo_from_manager"


class TestSendResponseAgentId:

    async def test_agent_id_field_preferred(self):
        import json

        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/cgi-bin/gettoken":
                return httpx.Response(200, json={"errcode": 0, "access_token": "AT", "expires_in": 7200})
            captured["json"] = json.loads(request.content)
            return httpx.Response(200, json={"errcode": 0})

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://qyapi.weixin.qq.com")
        store = await build_store(make_tenant("acme", wecom_config()))
        adapter = WeComTenantAdapter(store, http_client=client)
        from trpc_agent_sdk.tenants import TenantResponse

        assert await adapter.send_response(
            TenantResponse(tenant_id="acme",
                           channel_type="wecom",
                           user_id="u1",
                           chat_id="u1",
                           content="reply",
                           message_type="text")) is True
        # agent_id (1000002) wins over the legacy webhook_token fallback.
        assert captured["json"]["agentid"] == AGENT_ID

    async def test_agent_id_falls_back_to_webhook_token(self):
        import json

        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/cgi-bin/gettoken":
                return httpx.Response(200, json={"errcode": 0, "access_token": "AT", "expires_in": 7200})
            captured["json"] = json.loads(request.content)
            return httpx.Response(200, json={"errcode": 0})

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://qyapi.weixin.qq.com")
        store = await build_store(make_tenant("acme", wecom_config(agent_id=None)))
        adapter = WeComTenantAdapter(store, http_client=client)
        from trpc_agent_sdk.tenants import TenantResponse

        assert await adapter.send_response(
            TenantResponse(tenant_id="acme",
                           channel_type="wecom",
                           user_id="u1",
                           chat_id="u1",
                           content="reply",
                           message_type="text")) is True
        # Legacy behaviour preserved: webhook_token is used as the agent id.
        assert captured["json"]["agentid"] == TOKEN
