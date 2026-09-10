# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Build a tenant store from environment variables for the WeCom callback demo.

Environment variables:

    WECOM_TENANT_ID      Tenant id to register (default: ``acme``)
    WECOM_CORP_ID        CorpId of the self-built app (企业微信后台 → 我的企业 → 企业信息)
    WECOM_CORP_SECRET    Secret of the self-built app (应用的 Secret)
    WECOM_AGENT_ID       AgentId of the self-built app (应用的 AgentId)
    WECOM_CALLBACK_TOKEN Token configured in the app's "接收消息" panel
    WECOM_AES_KEY        EncodingAESKey configured in the app's "接收消息" panel
"""

import os

from trpc_agent_sdk.tenants import ChannelConfig, InMemoryTenantStore, Tenant

DEFAULT_AES_KEY = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ"  # 43 chars, demo only


async def build_tenant_store() -> InMemoryTenantStore:
    """Create a demo store with one tenant wired for the WeCom callback.

    Replace ``InMemoryTenantStore`` with ``SQLTenantStore`` /
    ``RedisTenantStore`` for production deployments.
    """
    tenant_id = os.environ.get("WECOM_TENANT_ID", "acme")
    corp_id = os.environ.get("WECOM_CORP_ID", "ww_demo_corp_id")
    corp_secret = os.environ.get("WECOM_CORP_SECRET", "demo_corp_secret")
    agent_id = os.environ.get("WECOM_AGENT_ID", "1000002")
    callback_token = os.environ.get("WECOM_CALLBACK_TOKEN", "demo_callback_token")
    aes_key = os.environ.get("WECOM_AES_KEY", DEFAULT_AES_KEY)

    store = InMemoryTenantStore()
    await store.create_tenant(
        Tenant(
            tenant_id=tenant_id,
            name="WeCom Demo Tenant",
            channel_configs={
                "wecom":
                ChannelConfig(
                    channel_type="wecom",
                    # Real callback protocol mapping:
                    bot_id=corp_id,  # corp_id (plaintext <ToUserName> routing)
                    api_key=corp_secret,  # corp secret (outgoing message/send)
                    agent_id=agent_id,  # app agent id (outgoing message/send)
                    webhook_token=callback_token,  # callback Token (msg_signature)
                    webhook_secret=aes_key,  # EncodingAESKey (43 chars)
                )
            },
        ))
    return store
