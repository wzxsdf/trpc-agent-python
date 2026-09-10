# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""WeCom (企业微信) callback demo: a FastAPI service exposing the real
self-built-app callback protocol.

Endpoints (mount path ``/wecom/callback`` must match the URL configured in
the WeCom admin console):

- ``GET  /wecom/callback`` — URL verification: decrypt the ``echostr``
  challenge and return the plain text verbatim.
- ``POST /wecom/callback`` — message callbacks: verify ``msg_signature``,
  decrypt the encrypted XML, run the tenant pipeline (whitelist / dedup /
  audit) and answer ``"success"`` immediately. The agent reply is produced
  in a background task (WeCom enforces a 5-second response deadline).

Run:

    uvicorn main:app --host 0.0.0.0 --port 8000

This demo echoes the received text back via the ``message/send`` API
instead of running a full agent. Wire ``handle_message`` to
``TenantRunner`` for real agent execution.
"""

import asyncio
import os

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse

from trpc_agent_sdk.log import logger
from trpc_agent_sdk.tenants import TenantChannelManager, TenantResponse

from setup_tenants import build_tenant_store

# Cap concurrent background agent runs so a burst of callbacks cannot
# exhaust the process; messages beyond the cap still queue as tasks.
MAX_CONCURRENT_REPLIES = int(os.environ.get("WECOM_DEMO_MAX_CONCURRENT", "8"))

app = FastAPI(title="WeCom Callback Demo", version="1.0")
_manager: TenantChannelManager = None  # type: ignore[assignment]
_reply_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REPLIES)


async def get_manager() -> TenantChannelManager:
    """Lazily build the channel manager (async store setup)."""
    global _manager
    if _manager is None:
        store = await build_tenant_store()
        _manager = TenantChannelManager(store)
    return _manager


async def handle_message(manager: TenantChannelManager, message) -> None:
    """Produce the agent reply in the background.

    The demo echoes the text back; replace the body with a TenantRunner
    invocation for real agent execution.
    """
    async with _reply_semaphore:
        try:
            reply = TenantResponse(
                tenant_id=message.tenant_id,
                channel_type=message.channel_type,
                user_id=message.user_id,
                chat_id=message.chat_id,
                content=f"Echo: {message.content}",
                message_type="text",
                reply_to_message_id=message.message_id,
            )
            await manager.send_response(reply)
        except Exception as e:  # noqa: BLE001 — a reply failure must not kill the task
            logger.error(f"Background reply failed for message {message.message_id}: {e}")


@app.get("/wecom/callback")
async def verify_url(msg_signature: str, timestamp: str, nonce: str, echostr: str):
    """WeCom callback URL verification (the admin console's "保存" click)."""
    manager = await get_manager()
    echo = await manager.verify_wecom_url({
        "msg_signature": msg_signature,
        "timestamp": timestamp,
        "nonce": nonce,
        "echostr": echostr,
    })
    if echo is None:
        raise HTTPException(status_code=403, detail="URL verification failed")
    # Must be the verbatim decrypted echo text.
    return PlainTextResponse(echo)


@app.post("/wecom/callback")
async def message_callback(request: Request, msg_signature: str, timestamp: str, nonce: str):
    """WeCom encrypted message callback."""
    manager = await get_manager()
    xml_body = (await request.body()).decode("utf-8")
    message = await manager.handle_wecom_callback(xml_body, {
        "msg_signature": msg_signature,
        "timestamp": timestamp,
        "nonce": nonce,
    })
    if message is None:
        # Signature/routing/whitelist/dedup failure. Still answer "success"
        # for duplicates (the message was already processed); WeCom retries
        # three times on non-success responses.
        logger.warning("WeCom callback rejected by the tenant pipeline")
    else:
        # 5-second deadline: acknowledge now, reply in the background.
        asyncio.create_task(handle_message(manager, message))
    return PlainTextResponse("success")
