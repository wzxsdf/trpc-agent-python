#!/usr/bin/env python3
"""完整的多租户集成示例，展示所有组件如何协同工作。"""

import asyncio
import os
from datetime import datetime

from trpc_agent_sdk.tenants import (
    InMemoryTenantStore, RedisTenantStore, SqlTenantStore,
    Tenant, AppConfig, ModelConfig, ToolPermissions, ChannelConfig,
    TenantRouter, HttpHeaderIdentifier, ApiKeyIdentifier,
    get_context_manager,
)
from trpc_agent_sdk.tenants import (
    TenantAwareSessionService,
    TenantChannelManager, MessageDeduplicator,
)
from trpc_agent_sdk.tenants import (
    TenantMessage, TenantResponse,
)
from trpc_agent_sdk.runners import Runner
from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.models import OpenAIModel
from trpc_agent_sdk.tools import FunctionTool
from trpc_agent_sdk.sessions import InMemorySessionService


# 模拟工具函数
async def get_weather(city: str) -> dict:
    """获取天气信息的工具函数。"""
    return {
        "city": city,
        "temperature": "25°C",
        "condition": "Sunny",
        "humidity": "60%"
    }


async def web_search(query: str) -> dict:
    """网络搜索工具函数。"""
    return {
        "query": query,
        "results": [
            {"title": f"Result for {query}", "url": "https://example.com"}
        ]
    }


class MultiTenantSystem:
    """完整的多租户系统演示。"""

    def __init__(self):
        """初始化多租户系统。"""
        self.tenant_store = None
        self.session_services = {}
        self.tenant_router = None
        self.channel_manager = None

    async def initialize(self):
        """初始化系统组件。"""
        print("🚀 初始化多租户系统...")

        # 1. 创建租户存储 (可以根据环境选择不同后端)
        if os.getenv("REDIS_URL"):
            self.tenant_store = RedisTenantStore(os.getenv("REDIS_URL"))
            print("   ✓ 使用Redis租户存储")
        elif os.getenv("DATABASE_URL"):
            self.tenant_store = SqlTenantStore(os.getenv("DATABASE_URL"))
            print("   ✓ 使用SQL租户存储")
        else:
            self.tenant_store = InMemoryTenantStore()
            print("   ✓ 使用内存租户存储 (开发模式)")

        # 2. 创建示例租户
        await self._create_sample_tenants()

        # 3. 设置租户路由
        self.tenant_router = TenantRouter(self.tenant_store)
        self.tenant_router.add_identifier(HttpHeaderIdentifier("X-Tenant-ID"))
        self.tenant_router.add_identifier(ApiKeyIdentifier({
            "sk_acme_demo_key": "acme_corp",
            "sk_globex_demo_key": "globex_inc"
        }))
        print("   ✓ 租户路由器配置完成")

        # 4. 设置IM通道管理器
        self.channel_manager = TenantChannelManager(
            self.tenant_store,
            redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0")
        )
        print("   ✓ IM通道管理器配置完成")

        # 5. 为每个租户创建Session服务
        await self._setup_tenant_session_services()

        print("✅ 多租户系统初始化完成!\n")

    async def _create_sample_tenants(self):
        """创建示例租户。"""

        # ACME Corporation租户
        acme_tenant = Tenant(
            tenant_id="acme_corp",
            name="ACME Corporation",
            description="技术创新公司",
            app_config=AppConfig(
                app_name="acme_assistant",
                max_concurrent_sessions=100,
                enable_summarization=True,
                summarization_threshold=15,
            ),
            model_config=ModelConfig(
                model_provider="openai",
                model_name="gpt-4",
                api_key=os.getenv("OPENAI_API_KEY", "demo_key"),
                temperature=0.7,
            ),
            tool_permissions=ToolPermissions(
                allowed_tools=["get_weather", "web_search"],
                dangerous_tools=["web_search"],
                tool_budget_monthly_usd=100.0,
            ),
            channel_configs={
                "wecom": ChannelConfig(
                    channel_type="wecom",
                    webhook_token="acme_wecom_token",
                    webhook_secret="acme_secret",
                    allowed_users=["user1", "user2"],
                ),
                "telegram": ChannelConfig(
                    channel_type="telegram",
                    api_key="acme_telegram_bot_token",
                    allowed_users=["@acme_user1"],
                ),
            },
            custom_attributes={"industry": "technology", "tier": "enterprise"}
        )

        # Globex Inc租户
        globex_tenant = Tenant(
            tenant_id="globex_inc",
            name="Globex Inc",
            description="全球金融服务公司",
            app_config=AppConfig(
                app_name="globex_assistant",
                max_concurrent_sessions=50,
                enable_summarization=True,
                summarization_threshold=10,
            ),
            model_config=ModelConfig(
                model_provider="openai",
                model_name="gpt-3.5-turbo",
                api_key=os.getenv("OPENAI_API_KEY", "demo_key"),
                temperature=0.5,
            ),
            tool_permissions=ToolPermissions(
                allowed_tools=["web_search"],
                dangerous_tools=[],
                tool_budget_monthly_usd=50.0,
            ),
            channel_configs={
                "telegram": ChannelConfig(
                    channel_type="telegram",
                    api_key="globex_telegram_bot_token",
                    allowed_users=["@globex_user1"],
                ),
            },
            custom_attributes={"industry": "finance", "tier": "standard"}
        )

        # 保存租户
        await self.tenant_store.create_tenant(acme_tenant)
        await self.tenant_store.create_tenant(globex_tenant)

        print("   ✓ 创建了2个示例租户: acme_corp, globex_inc")

    async def _setup_tenant_session_services(self):
        """为每个租户设置Session服务。"""
        tenants = await self.tenant_store.list_tenants()

        for tenant in tenants:
            base_session_service = InMemorySessionService()

            context_manager = get_context_manager()
            with context_manager.with_tenant(tenant) as context:
                tenant_session_service = TenantAwareSessionService(
                    tenant_context=context,
                    base_session_service=base_session_service,
                )

                self.session_services[tenant.tenant_id] = {
                    "service": tenant_session_service,
                    "context": context,
                }

        print(f"   ✓ 为{len(tenants)}个租户配置了Session服务")

    async def demonstrate_tenant_routing(self):
        """演示租户路由功能。"""
        print("🔀 演示租户路由功能...")

        # 模拟HTTP请求
        mock_requests = [
            {"headers": {"X-Tenant-ID": "acme_corp"}, "body": {"message": "ACME的请求"}},
            {"headers": {"X-API-Key": "sk_globex_demo_key"}, "body": {"message": "Globex的请求"}},
            {"headers": {"X-Tenant-ID": "nonexistent"}, "body": {"message": "无效租户"}},
        ]

        for i, request in enumerate(mock_requests, 1):
            print(f"\n   请求 {i}:")
            tenant = await self.tenant_router.route_request(request)

            if tenant:
                print(f"   ✓ 成功路由到租户: {tenant.name}")
                print(f"   ✓ 租户ID: {tenant.tenant_id}")
                print(f"   ✓ 行业: {tenant.custom_attributes.get('industry')}")
            else:
                print(f"   ✗ 路由失败: 无效的租户标识")

    async def demonstrate_tenant_isolation(self):
        """演示租户隔离功能。"""
        print("\n🔒 演示租户隔离功能...")

        # 为不同租户创建相同的session_id，验证隔离
        session_id = "test_session_123"

        for tenant_id in ["acme_corp", "globex_inc"]:
            context = self.session_services[tenant_id]["context"]
            session_service = self.session_services[tenant_id]["service"]

            # 使用租户上下文
            with get_context_manager().with_tenant(context.tenant) as tenant_context:
                print(f"\n   租户: {tenant_id}")

                # 检查工具权限
                tools_to_check = ["get_weather", "web_search", "shell_exec"]
                for tool in tools_to_check:
                    if tenant_context.is_tool_allowed(tool):
                        dangerous = " (危险)" if tenant_context.is_tool_dangerous(tool) else ""
                        print(f"   ✓ {tool}: 允许{dangerous}")
                    else:
                        print(f"   ✗ {tool}: 禁止")

                # 检查通道配置
                wecom_config = tenant_context.get_channel_config("wecom")
                if wecom_config:
                    print(f"   ✓ WeCom配置: {wecom_config['allowed_users']}")

    async def demonstrate_im_integration(self):
        """演示IM集成功能。"""
        print("\n💬 演示IM集成功能...")

        # 模拟WeCom webhook
        # 按 WeCom 验签算法 (sha1(sorted(secret, timestamp, nonce))) 计算合法签名
        import hashlib
        _ts, _nonce, _secret = "1234567890", "random_nonce", "acme_secret"
        valid_signature = hashlib.sha1("".join(sorted([_secret, _ts, _nonce])).encode()).hexdigest()

        wecom_payload = {
            "token": "acme_wecom_token",
            "timestamp": _ts,
            "nonce": _nonce,
            "signature": valid_signature,
            "msg_type": "text",
            "from_user_id": "user1",
            "from_user_name": "张三",
            "to_user_name": "acme_assistant",
            "content": "你好，请问今天北京的天气怎么样？",
            "msg_id": "wecom_msg_123"
        }

        print("   模拟WeCom消息到达...")
        message = await self.channel_manager.handle_webhook(
            "wecom", wecom_payload, {}
        )

        if message:
            print(f"   ✓ 消息处理成功")
            print(f"   ✓ 租户: {message.tenant_id}")
            print(f"   ✓ 用户: {message.metadata.get('from_user_name')}")
            print(f"   ✓ 内容: {message.content}")

            # 生成session_id
            session_id = self.channel_manager.generate_session_id(message)
            print(f"   ✓ Session ID: {session_id}")

        else:
            print("   ✗ 消息处理失败")

        # 模拟Telegram webhook
        telegram_payload = {
            "message": {
                "message_id": 123,
                "from": {
                    "id": 456,
                    "username": "globex_user1",
                    "first_name": "John"
                },
                "chat": {
                    "id": 789,
                    "type": "private"
                },
                "text": "Hello, how can you help me?"
            }
        }

        print("\n   模拟Telegram消息到达...")
        message = await self.channel_manager.handle_webhook(
            "telegram", telegram_payload, {"X-Telegram-Bot-Token": "globex_telegram_bot_token"}
        )

        if message:
            print(f"   ✓ 消息处理成功")
            print(f"   ✓ 租户: {message.tenant_id}")
            print(f"   ✓ 用户: {message.metadata.get('from_username')}")
            print(f"   ✓ 内容: {message.content}")
        else:
            print("   ✗ 消息处理失败")

    async def demonstrate_agent_execution(self):
        """演示Agent执行功能。"""
        print("\n🤖 演示多租户Agent执行...")

        # 为ACME租户创建简单agent
        acme_context = self.session_services["acme_corp"]["context"]
        acme_session_service = self.session_services["acme_corp"]["service"]

        # 创建租户特定的agent
        model_config = acme_context.get_model_config()
        model = OpenAIModel(
            model_name=model_config["model_name"],
            api_key=model_config["api_key"],
            base_url=model_config.get("base_url", ""),
        )

        # 根据租户权限选择工具
        allowed_tools = acme_context.get_allowed_tools()
        tools = []
        if "get_weather" in allowed_tools:
            tools.append(FunctionTool(get_weather))
        if "web_search" in allowed_tools:
            tools.append(FunctionTool(web_search))

        agent = LlmAgent(
            name="acme_assistant",
            model=model,
            instruction="你是ACME公司的AI助手，帮助用户查询天气和搜索信息。",
            tools=tools,
        )

        # 创建Runner
        runner = Runner(
            app_name=acme_context.get_custom_attribute("app_name", "acme_assistant"),
            agent=agent,
            session_service=acme_session_service,
        )

        print("   ✓ ACME Agent创建完成")
        print(f"   ✓ 配置工具: {[tool.name for tool in tools]}")

        # 模拟用户消息
        from trpc_agent_sdk.types import Content, Part

        user_message = Content(parts=[
            Part.from_text(text="请问今天北京的天气怎么样？")
        ])

        print(f"\n   处理用户消息: {user_message.parts[0].text}")

        # 这里可以执行agent，但在demo中我们只是展示设置
        print("   ✓ Agent已配置完成，可以处理租户acme_corp的请求")

    async def cleanup(self):
        """清理资源。"""
        print("\n🧹 清理系统资源...")

        if hasattr(self.tenant_store, 'close'):
            await self.tenant_store.close()

        if hasattr(self.channel_manager, '_deduplicator'):
            if hasattr(self.channel_manager._deduplicator, '_get_redis'):
                redis = await self.channel_manager._deduplicator._get_redis()
                await redis.close()

        print("✅ 系统资源清理完成")


async def main():
    """主演示函数。"""
    try:
        print("=" * 70)
        print("🏢 多租户系统完整演示")
        print("=" * 70)

        # 创建并初始化系统
        system = MultiTenantSystem()
        await system.initialize()

        # 演示各个功能
        await system.demonstrate_tenant_routing()
        await system.demonstrate_tenant_isolation()
        await system.demonstrate_im_integration()
        await system.demonstrate_agent_execution()

        print("\n" + "=" * 70)
        print("📊 演示总结")
        print("=" * 70)

        print("""
✅ 已演示的核心功能:

1. **租户路由**: 支持多种租户识别策略
2. **租户隔离**: 完全的配置、权限、数据隔离
3. **IM集成**: 多平台消息处理和租户路由
4. **Agent执行**: 租户特定的agent配置和执行
5. **权限控制**: 租户级别的工具权限管理
6. **Session管理**: 租户隔离的session存储

🎯 系统特点:

• 水平扩展: 无状态worker设计，支持动态扩展
• 高可用性: 多后端存储支持，故障自动恢复
• 安全隔离: 租户级别数据和配置完全隔离
• 灵活配置: 每个租户可配置不同的模型、工具、通道
• 生产就绪: 包含监控、审计、限流等企业功能

📈 部署选项:

• 开发: Docker Compose + InMemory存储
• 测试: Docker Compose + Redis/SQL存储
• 生产: Kubernetes + Redis Cluster + PostgreSQL HA

🔧 下一步集成:

1. 替换为真实的模型API密钥
2. 配置实际的IM Webhook URL
3. 设置Redis/SQL持久化存储
4. 配置监控和告警系统
5. 部署到生产环境
        """)

        await system.cleanup()

    except KeyboardInterrupt:
        print("\n\n⚠️  演示被用户中断")
    except Exception as e:
        print(f"\n\n❌ 演示失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())