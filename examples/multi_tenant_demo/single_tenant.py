#!/usr/bin/env python3
"""Single tenant demo showing basic multi-tenant architecture usage."""

import asyncio
from datetime import datetime

from trpc_agent_sdk.tenants import (
    InMemoryTenantStore,
    Tenant,
    AppConfig,
    ModelConfig,
    ToolPermissions,
    ChannelConfig,
    StorageConfig,
    AuditConfig,
)
from trpc_agent_sdk.tenants import (
    TenantRouter,
    HttpHeaderIdentifier,
    get_context_manager,
)


async def main():
    """Demonstrate basic single-tenant setup and usage."""
    print("=== Multi-Tenant Demo: Single Tenant ===\n")

    # Step 1: Create tenant store
    print("1. Creating tenant store...")
    tenant_store = InMemoryTenantStore()
    print("   ✓ InMemory tenant store created\n")

    # Step 2: Create a tenant
    print("2. Creating tenant 'acme_corp'...")
    tenant = Tenant(
        tenant_id="acme_corp",
        name="ACME Corporation",
        description="Demo tenant for testing",
        app_config=AppConfig(
            app_name="acme_assistant",
            app_description="ACME's AI assistant",
            max_concurrent_sessions=50,
            session_timeout_seconds=3600,
            max_history_messages=100,
            enable_summarization=True,
            summarization_threshold=15,
        ),
        model_config=ModelConfig(
            model_provider="openai",
            model_name="gpt-4",
            api_key="sk-demo-key",  # Demo key, should be stored securely
            base_url="https://api.openai.com/v1",
            max_tokens=4096,
            temperature=0.7,
            enable_streaming=True,
        ),
        tool_permissions=ToolPermissions(
            allowed_tools=["web_search", "weather", "calculator"],
            blocked_tools=["shell_exec", "file_write"],
            dangerous_tools=["web_search"],
            tool_budget_monthly_usd=100.0,
        ),
        channel_configs={
            "wecom": ChannelConfig(
                channel_type="wecom",
                enabled=True,
                webhook_token="acme_wecom_token_123",
                webhook_secret="acme_secret_key_456",
                bot_id="ww1234567890abcdef",
                allowed_users=["user1", "user2"],
                rate_limit_per_minute=60,
                enable_streaming=True,
            ),
            "telegram": ChannelConfig(
                channel_type="telegram",
                enabled=True,
                api_key="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
                allowed_users=["@telegram_user1"],
                rate_limit_per_minute=30,
            ),
        },
        storage_config=StorageConfig(
            session_backend="redis",
            memory_backend="redis",
            knowledge_backend="langchain_vectorstore",
            audit_backend="sql",
            redis_url="redis://localhost:6379/1",
            sql_url="postgresql://user:pass@localhost/acme_db",
        ),
        audit_config=AuditConfig(
            enable_audit_log=True,
            audit_retention_days=90,
            log_level="INFO",
            sanitize_api_keys=True,
            sanitize_user_data=False,
            sanitize_tool_inputs=True,
            enable_cost_alerts=True,
            cost_alert_threshold_usd=50.0,
        ),
        custom_attributes={
            "industry": "technology",
            "tier": "enterprise",
            "sales_rep": "john.doe@acme.com"
        },
        is_active=True,
    )

    await tenant_store.create_tenant(tenant)
    print(f"   ✓ Tenant created: {tenant.name}")
    print(f"   ✓ ID: {tenant.tenant_id}")
    print(f"   ✓ Allowed tools: {tenant.tool_permissions.allowed_tools}")
    print(f"   ✓ Blocked tools: {tenant.tool_permissions.blocked_tools}")
    print(f"   ✓ Channels: {list(tenant.channel_configs.keys())}\n")

    # Step 3: Set up routing
    print("3. Setting up tenant routing...")
    router = TenantRouter(tenant_store)
    router.add_identifier(HttpHeaderIdentifier("X-Tenant-ID"))
    print("   ✓ Router configured with HTTP header identification\n")

    # Step 4: Simulate incoming request
    print("4. Simulating incoming request...")
    mock_request = {
        "headers": {"X-Tenant-ID": "acme_corp"},
        "body": {"message": "Hello, how can you help me?"}
    }

    matched_tenant = await router.route_request(mock_request)
    if matched_tenant:
        print(f"   ✓ Request routed to tenant: {matched_tenant.name}")
        print(f"   ✓ Tenant is active: {matched_tenant.is_active}")
    else:
        print("   ✗ Failed to route request")
        return

    # Step 5: Use tenant context
    print("\n5. Using tenant context...")
    context_manager = get_context_manager()

    with context_manager.with_tenant(matched_tenant) as context:
        print(f"   ✓ Tenant context set for: {context.tenant_id}")

        # Access model configuration
        model_config = context.get_model_config()
        print(f"   ✓ Model provider: {model_config['model_provider']}")
        print(f"   ✓ Model name: {model_config['model_name']}")

        # Check tool permissions
        print(f"\n6. Tool permission checks:")
        test_tools = ["web_search", "weather", "shell_exec", "file_write"]
        for tool in test_tools:
            if context.is_tool_allowed(tool):
                dangerous = " (DANGEROUS)" if context.is_tool_dangerous(tool) else ""
                print(f"   ✓ {tool}: ALLOWED{dangerous}")
            else:
                print(f"   ✗ {tool}: BLOCKED")

        # Access channel configuration
        print(f"\n7. Channel configurations:")
        wecom_config = context.get_channel_config("wecom")
        if wecom_config:
            print(f"   ✓ WeCom enabled: {wecom_config['enabled']}")
            print(f"   ✓ WeCom bot ID: {wecom_config['bot_id']}")
            print(f"   ✓ WeCom streaming: {wecom_config['enable_streaming']}")

        telegram_config = context.get_channel_config("telegram")
        if telegram_config:
            print(f"   ✓ Telegram enabled: {telegram_config['enabled']}")
            print(f"   ✓ Telegram rate limit: {telegram_config['rate_limit_per_minute']}/min")

        # Access custom attributes
        print(f"\n8. Custom attributes:")
        print(f"   ✓ Industry: {context.get_custom_attribute('industry')}")
        print(f"   ✓ Tier: {context.get_custom_attribute('tier')}")
        print(f"   ✓ Sales rep: {context.get_custom_attribute('sales_rep')}")

        # Check audit settings
        print(f"\n9. Audit configuration:")
        print(f"   ✓ Audit logging: {context.get_audit_config()['enable_audit_log']}")
        print(f"   ✓ Sanitize API keys: {context.should_sanitize_api_keys()}")
        print(f"   ✓ Sanitize tool inputs: {context.should_sanitize_tool_inputs()}")

        # Get storage configuration
        storage_config = context.get_storage_config()
        print(f"\n10. Storage configuration:")
        print(f"   ✓ Session backend: {storage_config['session_backend']}")
        print(f"   ✓ Memory backend: {storage_config['memory_backend']}")
        print(f"   ✓ Knowledge backend: {storage_config['knowledge_backend']}")
        print(f"   ✓ Audit backend: {storage_config['audit_backend']}")

    print("\n=== Demo completed successfully! ===")
    print("\nKey takeaways:")
    print("• Tenants provide complete configuration isolation")
    print("• Tenant context enables seamless access to tenant-specific settings")
    print("• Flexible routing supports multiple identification strategies")
    print("• Tool permissions ensure security and compliance")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nDemo interrupted by user")
    except Exception as e:
        print(f"\n\nDemo failed with error: {e}")
        import traceback
        traceback.print_exc()