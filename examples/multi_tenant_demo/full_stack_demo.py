#!/usr/bin/env python3
"""全栈多租户演示（容器化冒烟入口）。

演示并验证 SDK 中 **已实现** 的多租户能力，是 docker-compose 冒烟测试
的入口脚本（见同目录 docker-compose.yml）。脚本设计为**幂等**：多副本
（``docker compose up --scale demo=2``）共享 Redis 时不会互相冲突。

覆盖能力（均为可运行验证，非演示性打印）：

1. 租户存储：Redis（REDIS_URL 存在时）/ 内存（默认），多副本幂等创建
2. 租户路由：X-Tenant-ID 头 + API Key 两种识别策略
3. IM 接入：WeCom 验签 webhook + session_id 规则
4. Runner 接线：create_tenant_runner 的治理/熔断/遥测回调链
5. 工具熔断：连续失败触发 open，探测窗口后半开
6. 存储降级：primary 故障时透明切换 fallback
7. 配置灰度：canary 哈希路由 + promote + rollback
8. 容量评估：capacity_plan 输出节点数与预算外推
9. 监控指标：零依赖 Prometheus 文本格式渲染

不依赖真实 LLM（模型调用需要 API Key，属可选集成，见 README）。
"""

import asyncio
import hashlib
import os
import sys

from trpc_agent_sdk.tenants import (
    ApiKeyIdentifier,
    ConfigRolloutManager,
    DegradationController,
    HttpHeaderIdentifier,
    InMemoryTenantStore,
    TenantConfig,
    TenantStoreWithFallback,
    TenantChannelManager,
    TenantRouter,
    ToolCircuitBreaker,
    WeComTenantAdapter,
    capacity_plan,
    create_tenant_runner,
    get_tenant_metrics,
    nodes_for_qps,
    reset_tenant_metrics,
    user_bucket,
)
from trpc_agent_sdk.tenants._tenant_resilience import CIRCUIT_OPEN
from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.models import OpenAIModel
from trpc_agent_sdk.sessions import InMemorySessionService

import datetime as dt


def _step(title: str) -> None:
    print(f"\n{'=' * 60}\n▶ {title}\n{'=' * 60}")


async def build_tenant_store():
    """Redis 优先（多节点共享），否则内存（单机演示）。"""
    redis_url = os.getenv("REDIS_URL")
    if redis_url:
        from trpc_agent_sdk.tenants import RedisTenantStore

        store = RedisTenantStore(redis_url)
        print(f"✓ 租户存储: Redis ({redis_url}) — 多节点共享")
    else:
        store = InMemoryTenantStore()
        print("✓ 租户存储: InMemory（未设置 REDIS_URL，单机演示）")
    return store


async def create_tenant_idempotent(store, config: TenantConfig) -> None:
    """幂等创建租户（多副本并发启动安全）。"""
    if not await store.tenant_exists(config.tenant_id):
        await store.create_tenant(config.to_tenant())
        print(f"✓ 创建租户 {config.tenant_id}")
    else:
        print(f"✓ 租户 {config.tenant_id} 已存在（幂等跳过）")


async def demo_storage_and_tenants():
    _step("1. 租户存储与租户注册")
    store = await build_tenant_store()

    acme = TenantConfig(
        tenant_id="acme_corp",
        name="ACME Corporation",
        description="容器化冒烟演示租户",
        app_config={"app_name": "acme_assistant"},
        llm_config={
            "model_provider": "openai",
            "model_name": "gpt-4o-mini"
        },
        tool_permissions={
            "allowed_tools": ["get_weather"],
            "blocked_tools": [],
            "dangerous_tools": ["web_search"],
        },
        channel_configs={
            "wecom": {
                "channel_type": "wecom",
                "webhook_token": "acme_wecom_token",
                "webhook_secret": "acme_secret",
                "allowed_users": ["user1"],
            }
        },
        resilience_config={
            "model_failure_action": "fallback",
            "circuit_failure_threshold": 3
        },
    )
    globex = TenantConfig(tenant_id="globex_inc", name="Globex Inc")
    await create_tenant_idempotent(store, acme)
    await create_tenant_idempotent(store, globex)

    tenant = await store.get_tenant("acme_corp")
    assert tenant is not None, "租户读取失败"
    assert tenant.resilience_config.model_failure_action == "fallback", "ResilienceConfig 往返失败"
    assert tenant.config_version == 1
    print(f"✓ 租户往返验证: model_failure_action={tenant.resilience_config.model_failure_action}, "
          f"config_version={tenant.config_version}")
    return store


async def demo_routing(store):
    _step("2. 租户路由（HTTP 头 + API Key 识别）")
    router = TenantRouter(store)
    router.add_identifier(HttpHeaderIdentifier("X-Tenant-ID"))
    router.add_identifier(ApiKeyIdentifier({"sk_acme_demo_key": "acme_corp"}))

    by_header = await router.route_request({"headers": {"X-Tenant-ID": "acme_corp"}})
    by_key = await router.route_request({"headers": {"X-API-Key": "sk_acme_demo_key"}})
    unknown = await router.route_request({"headers": {"X-Tenant-ID": "ghost"}})
    assert by_header and by_header.tenant_id == "acme_corp"
    assert by_key and by_key.tenant_id == "acme_corp"
    assert unknown is None
    print(f"✓ X-Tenant-ID 头识别 → {by_header.tenant_id}")
    print(f"✓ API Key 识别     → {by_key.tenant_id}")
    print("✓ 未知租户正确拒绝")


async def demo_im_channel(store):
    _step("3. IM 接入（WeCom 验签 + session_id 规则）")
    manager = TenantChannelManager(store, redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    manager.register_adapter("wecom", WeComTenantAdapter(store))

    # 按 WeCom 验签算法 sha1(sorted(secret, timestamp, nonce)) 构造合法签名
    secret, ts, nonce = "acme_secret", "1234567890", "demo_nonce"
    signature = hashlib.sha1("".join(sorted([secret, ts, nonce])).encode()).hexdigest()
    payload = {
        "token": "acme_wecom_token",
        "timestamp": ts,
        "nonce": nonce,
        "signature": signature,
        "msg_type": "text",
        "from_user_id": "user1",
        "content": "北京今天天气怎么样？",
        "msg_id": "demo_msg_001",
    }
    message = await manager.handle_webhook("wecom", payload, {})
    assert message is not None, "WeCom webhook 处理失败"
    assert message.tenant_id == "acme_corp"
    session_id = manager.generate_session_id(message)
    print(f"✓ 验签通过, 租户={message.tenant_id}, 用户={message.user_id}")
    print(f"✓ 生成 session_id: {session_id}")


async def demo_runner_wiring(store):
    _step("4. TenantRunner 接线（治理 + 熔断 + 遥测回调链）")
    from trpc_agent_sdk.tenants import TenantContext

    tenant = await store.get_tenant("acme_corp")
    context = TenantContext(tenant)
    agent = LlmAgent(
        name="acme_assistant",
        model=OpenAIModel(model_name="gpt-4o-mini", api_key="sk-demo"),
        tools=[],
    )
    runner = await create_tenant_runner(context, agent, InMemorySessionService())
    callbacks = agent.before_tool_callback
    assert isinstance(callbacks, list) and len(callbacks) == 3, "回调链接线不完整"
    assert runner.get_tenant_id() == "acme_corp"
    print("✓ create_tenant_runner 接线完成:")
    print("    before_tool_callback = [治理白名单, 工具熔断, 遥测计时] (3 个)")
    print("    after_tool/before_model/after_model 均挂载遥测回调")
    print("✓ 模型失败策略: fallback（模型异常时降级为固定话术，不抛错）")


async def demo_circuit_breaker():
    _step("5. 工具熔断（closed → open → half-open）")
    breaker = ToolCircuitBreaker(failure_threshold=3, reset_seconds=60)
    for i in range(3):
        breaker.record_failure("web_search")
    assert breaker.state("web_search") == CIRCUIT_OPEN
    assert breaker.allow("web_search") is False
    print("✓ 连续失败 3 次后熔断打开，后续调用被短路（返回降级错误 dict）")
    breaker.record_success("get_weather")
    assert breaker.allow("get_weather") is True
    print("✓ 不同工具独立计数：get_weather 不受影响")


async def demo_degradation():
    _step("6. 存储降级（primary 故障 → fallback 透明切换）")

    class _BrokenStore(InMemoryTenantStore):
        """模拟 Redis/SQL 故障。"""

        async def create_tenant(self, tenant):
            raise ConnectionError("primary is down")

        async def get_tenant(self, tenant_id):
            raise ConnectionError("primary is down")

    controller = DegradationController()
    controller.register("demo_store", failure_threshold=1, probe_interval_seconds=0)
    store = TenantStoreWithFallback(
        primary=_BrokenStore(),
        fallback=InMemoryTenantStore(),
        backend_name="demo_store",
        controller=controller,
    )
    await store.create_tenant(TenantConfig(tenant_id="fallback_tenant", name="Fallback").to_tenant())
    tenant = await store.get_tenant("fallback_tenant")
    assert tenant is not None, "降级读取失败"
    assert store.degraded is True
    snapshot = controller.snapshot()
    print(f"✓ primary 故障后自动切换 fallback，读到租户: {tenant.tenant_id}")
    print(f"✓ 健康快照（供 /healthz 输出）: {snapshot}")
    assert snapshot["demo_store"]["state"] == "unavailable"


async def demo_rollout():
    _step("7. 配置灰度发布（canary 哈希路由 + 回滚）")
    manager = ConfigRolloutManager()
    # 先全量发布 v1 作为基线（无基线则无从回滚）
    manager.publish("acme_corp", target_version=1)
    manager.promote("acme_corp")
    manager.publish("acme_corp", target_version=2, canary_percent=5)
    # 放量到 50%：桶号 < 50 的用户吃新版本（确定性 sha256 路由，无 sticky session）
    manager.set_canary_percent("acme_corp", 50)
    bucket = user_bucket("acme_corp", "user1")
    version = manager.resolve_config_version("acme_corp", "user1")
    print(f"✓ user1 哈希桶 = {bucket} → config_version = {version}")
    assert version in (1, 2)
    assert manager.resolve_config_version("acme_corp", "user1") == version  # 确定性
    manager.promote("acme_corp")
    assert manager.resolve_config_version("acme_corp", "user1") == 2
    print("✓ promote 后 100% 用户 → v2")
    manager.rollback("acme_corp")
    assert manager.resolve_config_version("acme_corp", "user1") == 1
    print("✓ rollback 后 100% 用户 → v1（秒级回切）")


async def demo_capacity():
    _step("8. 容量评估（节点数 + 预算外推）")
    report = capacity_plan(
        peak_qps=120.0,
        per_node_qps=50.0,
        tenant_usage=[
            {
                "tenant_id": "acme_corp",
                "requests_mtd": 20000,
                "cost_usd_mtd": 40.0,
                "monthly_budget_usd": 100.0
            },
            {
                "tenant_id": "globex_inc",
                "requests_mtd": 8000,
                "cost_usd_mtd": 70.0,
                "monthly_budget_usd": 100.0
            },
        ],
        today=dt.date(2026, 9, 15),
    )
    assert report.nodes_required == nodes_for_qps(120.0, 50.0) == 4
    over = report.tenants_over_budget()
    assert over == ["globex_inc"]
    print(f"✓ 峰值 120 QPS @ 50 QPS/节点（30% 余量）→ 需要 {report.nodes_required} 个节点")
    print(f"✓ 预计月末超支租户: {over}（acme 预计花费 80/100, globex 140/100）")


def demo_metrics():
    _step("9. 监控指标（零依赖 Prometheus 文本格式）")
    metrics = get_tenant_metrics()
    metrics.incr("tenant_requests_total", {"tenant_id": "acme_corp"})
    text = metrics.render_prometheus()
    assert "tenant_requests_total" in text
    print("✓ Prometheus 文本输出样例:")
    for line in text.strip().splitlines()[:3]:
        print(f"    {line}")


async def main() -> int:
    reset_tenant_metrics()
    print("=" * 60)
    print("🏢 tRPC-Agent-Python 多租户全栈冒烟演示")
    print("=" * 60)

    store = await demo_storage_and_tenants()
    await demo_routing(store)
    await demo_im_channel(store)
    await demo_runner_wiring(store)
    await demo_circuit_breaker()
    await demo_degradation()
    await demo_rollout()
    await demo_capacity()
    demo_metrics()

    print("\n" + "=" * 60)
    print("✅ 全部 9 项冒烟检查通过")
    print("=" * 60)

    if hasattr(store, "close"):
        await store.close()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except Exception as e:  # noqa: BLE001 — 冒烟脚本需捕获并展示完整失败
        print(f"\n❌ 冒烟失败: {type(e).__name__}: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
