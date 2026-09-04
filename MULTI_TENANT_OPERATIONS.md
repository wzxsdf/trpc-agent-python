# 多租户故障恢复与运维手册

> 对应交付项：故障恢复与运维（降级、灰度发布、容量评估、部署方案）。
> 本文档说明阶段五落地的四块能力：租户级弹性策略、存储降级、配置灰度发布与回滚、容量评估，
> 并给出运维 SOP。

## 1. 租户级弹性策略（模型失败 / 工具失败）

每个租户在 `ResilienceConfig`（`_tenant_model.py`）中独立配置故障行为，
通过 `TenantConfig.resilience_config` 持久化：

```python
@dataclass
class ResilienceConfig:
    model_timeout_seconds: int = 30        # 模型超时（ModelConfig.timeout_seconds 同步使用）
    model_max_retries: int = 3             # 模型重试次数
    model_failure_action: str = "error"    # 'error' 向上抛 | 'fallback' 降级为固定话术
    model_fallback_text: str = "The assistant is temporarily unavailable. Please try again later."
    tool_failure_policy: str = "closed"    # 'closed' 熔断生效 | 'open' 永远尝试（fail-open）
    circuit_breaker_enabled: bool = True
    circuit_failure_threshold: int = 5     # 连续失败 N 次后熔断
    circuit_reset_seconds: int = 60        # 熔断开启后的探测等待窗口
```

**模型失败策略**（`TenantRunner.run_async`，`_tenant_runner.py`）：

- `model_failure_action="error"`（默认）：模型异常照常抛出，计入
  `tenant_errors_total{error_type=...}`，span 记录异常 —— 适合金融等强一致场景；
- `model_failure_action="fallback"`：捕获异常后产出一条合成的
  `turn_complete` 文本事件（`model_fallback_text`），IM 侧用户收到固定降级话术
  而非报错。注意：span 状态仍标记 ERROR（失败确实发生并被降级），监控按
  `tenant_errors_total` 告警。

**工具熔断**（`_tenant_resilience.py`）：

- `ToolCircuitBreaker`：按工具名的 closed → open → half-open 状态机，
  线程安全、时钟可注入（便于测试）；
- `create_tenant_runner` 在租户配置 `tool_failure_policy="closed"` 且
  `circuit_breaker_enabled=True` 时自动接线：`before_tool_callback` 在熔断
  打开时直接返回降级错误 dict（不再调用工具），`after_tool_callback` 把
  工具成功/失败喂给熔断器；
- **平台短路响应不计入熔断**：治理层（工具白名单/危险工具确认）的拒绝响应
  与熔断器自身的降级响应会被识别并跳过，避免"权限拒绝"误触发熔断；
- 熔断打开时的短路会写审计日志（`decision="tool_circuit_open"`）并计入
  `tenant_errors_total{error_type="ToolCircuitOpen"}`。

## 2. 存储降级（primary/fallback + 健康检查驱动）

`_tenant_degradation.py` 提供统一降级策略，避免每个存储各自写 try/except：

| 组件 | 职责 |
|---|---|
| `BackendHealth` | 单后端可用性状态机：连续失败达阈值 → unavailable；探测窗口（`probe_interval_seconds`）过后放行一次探测，成功恢复、失败重新拉黑 |
| `DegradationController` | 进程级开关登记处：`allow(name)` / `report_success(name)` / `report_failure(name)`，`snapshot()` 供 `/healthz` 输出 |
| `TenantStoreWithFallback` | 包装 primary（SQL/Redis）+ fallback（InMemory）：primary 允许时先试 primary，失败自动降级；primary 被拉黑期间直接走 fallback |

典型接法：

```python
controller = get_degradation_controller()
controller.register("tenant_store", failure_threshold=3, probe_interval_seconds=30)
store = TenantStoreWithFallback(
    primary=SqlTenantStore("mysql+pymysql://..."),
    fallback=InMemoryTenantStore(),
    backend_name="tenant_store",
)
```

**一致性取舍（显式声明）**：降级期间写入 fallback 的数据**不会**回灌 primary，
恢复后以 primary 为准 —— 这是可用性优先的选择。运维动作：

1. `controller.snapshot()` 接入监控，`state != "available"` 即告警；
2. 降级窗口内产生的 fallback 数据如需保留，需人工导出（fallback 是内存态，
   节点重启即失 —— 因此降级窗口应尽量短，告警后尽快修复 primary）；
3. `tenant.degraded` 属性可直接用于网关侧返回降级标记。

## 3. 配置灰度发布与回滚

`_tenant_rollout.py` 实现按租户的 `config_version` 灰度：

- `ConfigRolloutManager.publish(tenant_id, target_version, canary_percent)`：
  发布新版本到指定百分比用户；`set_canary_percent` 分阶段放量（如 5% → 25% → 50%）；
  `promote` 全量发布；`rollback` 回到基线版本；
- **路由**：`resolve_config_version(tenant_id, user_id)` 用
  `sha256("{tenant_id}:{user_id}")` 的确定性哈希桶（0-99）与 canary 百分比比较。
  同一用户永远落同一桶 → 会话中途配置不会漂移，且**所有节点独立算出相同结果，
  无需 sticky session**；
- `InMemoryConfigHistory` 记录每个租户最近 N 次发布，`rollback` 无需外部状态
  即可找到基线版本。生产可把 `snapshot()` 持久化到 Redis 后重启恢复。

标准灰度 SOP：

```
1. 修改租户配置（TenantConfig.config_version 自增），写入 SQL store（乐观锁保护）；
2. rollout.publish(tenant_id, target_version=N, canary_percent=5)，
   观察 tenant_errors_total / tenant_run_latency_ms 分版本无异常；
3. set_canary_percent 5 → 25 → 50，每档观察 ≥ 一个业务高峰；
4. promote(tenant_id) 全量；
5. 异常时 rollback(tenant_id)：立即 100% 回到基线版本（秒级，无需重新部署）。
```

## 4. 容量评估

`_tenant_capacity.py` 提供纯函数估算（CI/cron/运维 CLI 均可运行）：

| 函数 | 用途 |
|---|---|
| `nodes_for_qps(target_qps, per_node_qps, headroom_percent=30)` | 目标 QPS → 节点数（默认 30% 余量，覆盖滚动发布与流量毛刺） |
| `project_month_end_usage(mtd, today)` | 月初至今用量线性外推到月末 |
| `budget_headroom(budget, cost_mtd, today)` | 租户月末预算余量（负值 = 预计超支） |
| `capacity_plan(...)` | 一次性生成 `CapacityReport`：节点数 + 各租户预算外推行 + `summary()` JSON |

数据来源：`get_tenant_metrics().render_prometheus()` 里的
`tenant_requests_total` / `tenant_cost_usd_total`（阶段三埋点），
按天聚合出 month-to-date 值后调用 `capacity_plan`。超支租户由
`report.tenants_over_budget()` 列出，接入告警。

## 5. 单测覆盖

`tests/tenants/test_resilience_rollout.py`（39 用例）：

- 熔断器：开闭状态机、阈值触发、探测窗口、探测成功/失败、按工具独立计数、参数校验；
- 弹性 hooks：fail-closed 短路、fail-open 放行、after_tool 喂计数、
  治理拒绝不计入熔断、审计写入；
- 模型失败策略：fallback 降级产出合成事件、error 保持抛出；
- 降级：健康状态机、控制器快照、primary→fallback 切换与探测恢复；
- 灰度：确定性哈希路由、放量/全量/回滚、历史有界、非法参数；
- 容量：节点数取整与余量、月末外推（含闰月）、预算余量、报告汇总。

## 6. 部署方案补充

结合 `MULTI_TENANT_DEPLOYMENT.md` 的节点拓扑，故障恢复相关配置项：

| 配置 | 建议值 | 说明 |
|---|---|---|
| `circuit_failure_threshold` | 5 | 连续失败 5 次熔断（IM 场景 QPS 低，避免误熔断） |
| `circuit_reset_seconds` | 60 | 探测窗口 1 分钟 |
| `model_failure_action` | 按租户 | IM 客服类租户建议 `fallback`；对准确率敏感的租户保持 `error` |
| 降级 `failure_threshold` | 3 | 存储连续失败 3 次拉黑 |
| 降级 `probe_interval_seconds` | 30 | 半分钟探测一次 primary |
| 灰度起始比例 | 5% | 新 `config_version` 首批观察用户 |
