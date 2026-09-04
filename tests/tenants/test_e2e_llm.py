# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""End-to-end tenant test against a real LLM (opt-in).

Runs only when credentials are available, either via environment variables
(TRPC_AGENT_API_KEY / TRPC_AGENT_BASE_URL / TRPC_AGENT_MODEL_NAME) or a local
``_e2e_env.txt`` file (``KEY=VALUE`` lines, git-ignored, never committed).
Otherwise the whole module is skipped.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.models import OpenAIModel
from trpc_agent_sdk.sessions import InMemorySessionService
from trpc_agent_sdk.tools import FunctionTool
from trpc_agent_sdk.tenants import (
    InMemoryTenantStore,
    TenantConfig,
    create_tenant_runner,
    get_context_manager,
)
from trpc_agent_sdk.types import Content, Part

_ENV_FILE = Path(__file__).parent / "_e2e_env.txt"


def _load_credentials() -> dict:
    """Load LLM credentials from env or the local opt-in file."""
    creds = {
        "api_key": os.environ.get("TRPC_AGENT_API_KEY", ""),
        "base_url": os.environ.get("TRPC_AGENT_BASE_URL", ""),
        "model_name": os.environ.get("TRPC_AGENT_MODEL_NAME", ""),
    }
    if creds["api_key"]:
        return creds
    if _ENV_FILE.exists():
        for line in _ENV_FILE.read_text(encoding="utf-8").splitlines():
            key, _, value = line.strip().partition("=")
            if key in ("TRPC_AGENT_API_KEY", "TRPC_AGENT_BASE_URL", "TRPC_AGENT_MODEL_NAME"):
                os.environ.setdefault(key, value)
        creds = {
            "api_key": os.environ.get("TRPC_AGENT_API_KEY", ""),
            "base_url": os.environ.get("TRPC_AGENT_BASE_URL", ""),
            "model_name": os.environ.get("TRPC_AGENT_MODEL_NAME", ""),
        }
    return creds


_CREDENTIALS = _load_credentials()

pytestmark = pytest.mark.skipif(
    not _CREDENTIALS["api_key"],
    reason="LLM credentials not configured (set TRPC_AGENT_* env vars or _e2e_env.txt)",
)


async def _get_weather(city: str) -> dict:
    """Demo tool: return fixed weather."""
    return {"city": city, "temperature": "25C", "condition": "Sunny"}


def _build_agent(ctx, creds: dict) -> LlmAgent:
    model = OpenAIModel(
        model_name=creds["model_name"],
        api_key=creds["api_key"],
        base_url=creds["base_url"] or "",
    )
    tools = [FunctionTool(_get_weather)] if ctx.is_tool_allowed("get_weather") else []
    return LlmAgent(
        name=f"assistant_{ctx.tenant_id}",
        model=model,
        instruction=(f"You are the assistant of {ctx.tenant.name}. Reply in one short sentence."),
        tools=tools,
    )


@pytest.mark.asyncio
async def test_tenant_full_chain_real_llm():
    """Routing -> agent execution -> session isolation against a real LLM."""
    store = InMemoryTenantStore()
    acme = TenantConfig(
        tenant_id="acme",
        name="ACME",
        app_config={
            "app_name": "acme_app"
        },
        llm_config={
            "model_name": _CREDENTIALS["model_name"],
            "api_key": _CREDENTIALS["api_key"],
            "base_url": _CREDENTIALS["base_url"],
        },
        tool_permissions={
            "allowed_tools": ["get_weather"]
        },
    ).to_tenant()
    globex = TenantConfig(
        tenant_id="globex",
        name="Globex",
        app_config={
            "app_name": "globex_app"
        },
        llm_config={
            "model_name": _CREDENTIALS["model_name"],
            "api_key": _CREDENTIALS["api_key"],
            "base_url": _CREDENTIALS["base_url"],
        },
        tool_permissions={
            "allowed_tools": []
        },
    ).to_tenant()
    await store.create_tenant(acme)
    await store.create_tenant(globex)

    base_session_service = InMemorySessionService()
    context_manager = get_context_manager()

    runners = {}
    for tenant in (acme, globex):
        with context_manager.with_tenant(tenant) as ctx:
            runners[tenant.tenant_id] = await create_tenant_runner(ctx, _build_agent(ctx, _CREDENTIALS),
                                                                   base_session_service)

    # Same session id for both tenants -> must not collide
    session_id = str(uuid.uuid4())
    user_id = "e2e_user"
    question = Content(parts=[Part.from_text(text="Beijing weather today? Use the tool if available.")])

    outputs = {}
    for tid in ("acme", "globex"):
        text_parts = []
        async for event in runners[tid].run_async(user_id=user_id, session_id=session_id, new_message=question):
            if event.content and event.content.parts and not event.partial:
                for part in event.content.parts:
                    if part.text:
                        text_parts.append(part.text)
        outputs[tid] = "".join(text_parts).strip()

    assert outputs["acme"], "acme produced no output"
    assert outputs["globex"], "globex produced no output"

    # Session isolation in shared storage
    acme_service = runners["acme"]._tenant_session_service
    globex_service = runners["globex"]._tenant_session_service
    acme_sessions = await acme_service.list_sessions(app_name="acme_app", user_id=user_id)
    globex_sessions = await globex_service.list_sessions(app_name="globex_app", user_id=user_id)
    assert len(acme_sessions.sessions) == 1
    assert len(globex_sessions.sessions) == 1
    assert acme_sessions.sessions[0].id != globex_sessions.sessions[0].id
    assert acme_sessions.sessions[0].state.get("tenant_id") == "acme"
    assert globex_sessions.sessions[0].state.get("tenant_id") == "globex"

    # Cross-tenant read must fail
    foreign = await globex_service._base_service.get_session(
        app_name="globex_app",
        user_id=user_id,
        session_id=acme_sessions.sessions[0].id,
    )
    assert foreign is None, "cross-tenant leak!"
