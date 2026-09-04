# Multi-Tenant Demo Example

This example demonstrates the basic multi-tenant architecture and usage patterns.

## Features Demonstrated

- Tenant creation and configuration
- Multiple storage backends (InMemory, Redis, SQL)
- Tenant identification and routing
- Tenant context injection
- Tool permission isolation
- Channel-specific configuration

## Quick Start

### 1. Basic Tenant Management

```python
import asyncio
from trpc_agent_sdk.tenants import (
    InMemoryTenantStore,
    Tenant,
    AppConfig,
    ModelConfig,
    ToolPermissions,
    ChannelConfig,
)
from trpc_agent_sdk.tenants import TenantRouter, HttpHeaderIdentifier

async def main():
    # Create tenant store
    tenant_store = InMemoryTenantStore()

    # Create a tenant
    tenant = Tenant(
        tenant_id="acme_corp",
        name="ACME Corporation",
        description="Testing and demo tenant",
        app_config=AppConfig(
            app_name="acme_assistant",
            max_concurrent_sessions=50,
        ),
        model_config=ModelConfig(
            model_provider="openai",
            model_name="gpt-4",
            api_key="sk-...",  # Should be stored securely
        ),
        tool_permissions=ToolPermissions(
            allowed_tools=["web_search", "weather"],
            dangerous_tools=["shell_exec"],
        ),
        channel_configs={
            "wecom": ChannelConfig(
                channel_type="wecom",
                webhook_token="acme_wecom_token",
                webhook_secret="acme_secret",
            )
        }
    )

    # Save tenant
    await tenant_store.create_tenant(tenant)

    # Setup routing
    router = TenantRouter(tenant_store)
    router.add_identifier(HttpHeaderIdentifier("X-Tenant-ID"))

    # Simulate request
    mock_request = {
        "headers": {"X-Tenant-ID": "acme_corp"}
    }

    # Route request to tenant
    matched_tenant = await router.route_request(mock_request)
    print(f"Routed to tenant: {matched_tenant.name}")

    # Use tenant context
    from trpc_agent_sdk.tenants import get_context_manager

    context_manager = get_context_manager()
    with context_manager.with_tenant(matched_tenant) as context:
        # Access tenant-specific configuration
        model_config = context.get_model_config()
        print(f"Model: {model_config['model_name']}")

        # Check tool permissions
        if context.is_tool_allowed("web_search"):
            print("Web search is allowed")
        if context.is_tool_dangerous("shell_exec"):
            print("Shell exec requires confirmation")

if __name__ == "__main__":
    asyncio.run(main())
```

### 2. Multi-Backend Storage

```python
from trpc_agent_sdk.tenants import RedisTenantStore, SqlTenantStore

# Redis storage (for distributed deployments)
redis_store = RedisTenantStore(redis_url="redis://localhost:6379/0")
await redis_store.create_tenant(tenant)

# SQL storage (for persistence)
sql_store = SqlTenantStore(database_url="postgresql://user:pass@localhost/tenants")
await sql_store.create_tenant(tenant)
```

### 3. Multiple Identification Strategies

```python
from trpc_agent_sdk.tenants import (
    TenantRouter,
    HttpHeaderIdentifier,
    ApiKeyIdentifier,
    SubdomainIdentifier,
    ImWebhookTokenIdentifier
)

router = TenantRouter(tenant_store)

# Try HTTP header first
router.add_identifier(HttpHeaderIdentifier("X-Tenant-ID"))

# Fallback to API key
router.add_identifier(ApiKeyIdentifier({
    "sk_acme_key": "acme_corp",
    "sk_globex_key": "globex_inc"
}))

# Fallback to subdomain
router.add_identifier(SubdomainIdentifier("example.com"))

# Fallback to IM webhook token
router.add_identifier(ImWebhookTokenIdentifier({
    "wecom_token_123": "acme_corp",
    "telegram_token_456": "globex_inc"
}))
```

## Project Integration

### Integrating with Runner

```python
from trpc_agent_sdk.runners import Runner
from trpc_agent_sdk.tenants import get_current_tenant_context

async def process_with_tenant(tenant_id: str, user_message: str):
    # Get tenant context
    context = get_current_tenant_context(tenant_id)
    if not context:
        raise ValueError(f"No context for tenant: {tenant_id}")

    # Create runner with tenant-specific configuration
    model_config = context.get_model_config()
    agent = create_tenant_agent(context)

    # Use tenant-isolated session service
    session_service = TenantAwareSessionService(context, base_session_service)

    runner = Runner(
        app_name=context.get_custom_attribute("app_name", "default"),
        agent=agent,
        session_service=session_service
    )

    # Process message
    async for event in runner.run_async(user_id=user_id, session_id=session_id, new_message=user_message):
        # Handle events with tenant context
        if context.should_sanitize_tool_inputs() and event.content:
            # Sanitize sensitive data before logging
            event = sanitize_event(event)
        yield event
```

### FastAPI Integration

```python
from fastapi import FastAPI, Request, Header
from trpc_agent_sdk.tenants import get_context_manager

app = FastAPI()

@app.middleware("http")
async def tenant_middleware(request: Request, call_next):
    # Extract and route tenant
    router = get_tenant_router()  # Pre-configured router
    tenant = await router.route_request(request)

    if not tenant:
        raise HTTPException(status_code=401, detail="Invalid tenant")

    # Set tenant context for this request
    context_manager = get_context_manager()
    with context_manager.with_tenant(tenant) as context:
        # Process request with tenant context
        response = await call_next(request)
        return response

@app.post("/chat")
async def chat_endpoint(request: Request, x_tenant_id: str = Header(None)):
    # Tenant context is already set by middleware
    context = get_current_tenant_context(x_tenant_id)

    # Process chat request
    return await process_chat(request, context)
```

## Testing Different Tenants

The example includes runnable scripts:

- `single_tenant.py` - Basic single-tenant usage
- `multi_tenant_integration.py` - End-to-end tour: routing, isolation, IM webhooks, runner setup
- `full_stack_demo.py` - Container smoke entrypoint: runs 9 assertion-backed checks covering
  storage, routing, IM, runner wiring, circuit breaker, degradation, canary rollout and capacity

## Docker Smoke Test

Run the multi-tenant stack with a shared Redis (requires Docker):

```bash
# Single node
docker compose -f examples/multi_tenant_demo/docker-compose.yml up --build

# Two demo nodes sharing Redis (no sticky session needed; the script is idempotent)
docker compose -f examples/multi_tenant_demo/docker-compose.yml up --build --scale demo=2
```

Each demo replica prints "✅ 全部 9 项冒烟检查通过" and exits 0 on success
(`docker compose up` shows `demo-1 exited with code 0`). Without Docker you can run
the same script directly — it falls back to in-memory storage when `REDIS_URL` is unset:

```bash
python examples/multi_tenant_demo/full_stack_demo.py
```

## Real LLM (Optional)

The demos verify the multi-tenant platform itself and do **not** call a model by default.
To execute the agent against a real LLM, provide credentials via environment variables
(`OPENAI_API_KEY`, optionally `OPENAI_BASE_URL`/model name) — the tenant's `ModelConfig`
is what `create_tenant_runner` wires into the agent.

## Key Concepts

### 1. Tenant Isolation
Each tenant has completely isolated:
- Configuration and settings
- Tool permissions and budgets
- User sessions and data
- IM channel bindings

### 2. Context Management
Tenant context provides thread-safe access to:
- Model configuration
- Tool permissions
- Storage backends
- Audit settings
- Custom attributes

### 3. Flexible Routing
Support for multiple identification strategies:
- HTTP headers
- API keys
- Subdomains
- Path prefixes
- IM webhook tokens
- Custom functions

### 4. Storage Flexibility
Choose backend based on requirements:
- InMemory: Development and testing
- Redis: Distributed deployments
- SQL: Production persistence

## Next Steps

1. Run the basic examples to understand the patterns
2. Integrate with your existing FastAPI/A2A setup
3. Configure production storage backends
4. Set up proper tenant identification for your use case
5. Implement tenant-specific monitoring and billing