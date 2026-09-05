# 多租户数据一致性与存储架构说明

> 对应交付项：数据同步与多后端支持（统一抽象、一致性策略、迁移、幂等、DDL 表结构）。
> 本文档说明阶段四落地的 SQL Schema（八表 DDL）、乐观锁并发一致性、版本化迁移工具、
> Memory 跨节点可见性与向量库后端的取舍。

## 1. 统一 DDL（八表）

权威 DDL 位于 `trpc_agent_sdk/tenants/_sql_ddl.py`，提供两个方言变体：

| 变体 | 用途 |
|---|---|
| `TENANT_TABLES_DDL_SQLITE` | 测试 / 单机开发（事务性 DDL，可整体回滚） |
| `TENANT_TABLES_DDL_MYSQL` | 生产 MySQL 8 / TiDB（utf8mb4、InnoDB、AUTO_INCREMENT） |

八表覆盖租户平台全部数据类别：

| 表 | 用途 | 关键约束 |
|---|---|---|
| `tenants` | 租户注册表（TenantConfig JSON + 乐观锁版本） | PK `tenant_id`；`version` 列 |
| `sessions` | 租户隔离会话 | PK `(tenant_id, session_id)`；`version` 列 |
| `events` | 会话事件流（追加写） | UNIQUE `(tenant_id, session_id, event_id)`（幂等） |
| `messages` | 归一化 IM 消息 | UNIQUE `(tenant_id, message_id)`（幂等） |
| `memory` | 长期记忆（可选 embedding BLOB） | PK `(tenant_id, user_id, memory_id)` |
| `summaries` | 会话摘要 | PK `(tenant_id, session_id)`；`version` 列 |
| `channel_bindings` | IM 身份 → 租户会话绑定 | PK `(tenant_id, channel_type, external_id)` |
| `audit_logs` | 治理审计（11 个必备字段 + trace_id） | 索引 `(tenant_id, created_at)`、`trace_id` |

**租户隔离是存储键层面的硬隔离**：所有租户数据表都带 `tenant_id` 列并纳入主键/唯一键，
配合 `TenantAwareSessionService` 的 `{tenant_id}:` 键前缀，任何 SQL 查询都不会跨租户。
审计日志的 11 个字段与任务书一一对应，`trace_id` 列支撑全链路追踪关联（阶段三）。

**幂等性**：`events` 与 `messages` 的 UNIQUE 约束使得 IM webhook 重试 / 跨节点重复写入
天然幂等（重复写入报唯一冲突即丢弃），与内存去重器 `MessageDeduplicator` 双保险。

## 2. 乐观锁并发一致性

**模型**：读-改-写表（`tenants` / `sessions` / `summaries`）带 `version INTEGER NOT NULL DEFAULT 0`。
以 `SqlTenantStore.update_tenant` 为例：

```sql
UPDATE tenants SET ..., version = version + 1
WHERE tenant_id = :tid AND version = :expected_version;
```

- `rowcount == 0` → 抛出 `trpc_agent_sdk.tenants.OptimisticLockError`，调用方**重读后重试**
  （last-writer-wins 带冲突检测，而非静默覆盖）；
- 持久化的 `config_data` JSON 携带**自增后**的版本，读路径（`get_tenant`）以行的
  `version` 列为准还原，保证读写一致。

**为什么选乐观锁而不是分布式锁 / SELECT FOR UPDATE**：

| 方案 | 取舍 |
|---|---|
| 乐观锁（采用） | 无额外基础设施、无死锁风险；冲突率低的租户配置写入场景开销≈0。代价：高冲突时重试增多 |
| `SELECT FOR UPDATE` | 需要 long-lived 事务，跨节点持有锁时间不可控；webhook 请求路径上引入尾延迟 |
| Redis 分布式锁 | 引入第三组件与锁超时/续约问题；租户配置写入频率低，不值 |

**其他数据的一致性策略**：

- `events` / `messages` / `audit_logs`：追加写（append-only），唯一约束幂等，无需锁。
- 会话事件写入：沿用 SDK `SqlSessionService` 的 ORM 事务；事件流为追加写，
  并发追加由数据库事务串行化，不读旧值所以不存在丢失更新。
- IM 限流/去重：Redis `SETNX` + 固定窗口计数（原子操作），Redis 不可用时降级为
  进程内实现（见 `_im_transport.py`）——降级期间的一致性取舍是**单节点内正确**，
  多节点窗口可能短暂超限，属于可用性优先的显式选择。

## 3. 版本化迁移工具

`trpc_agent_sdk/tenants/_sql_migrations.py` 提供离线迁移 runner：

```bash
# 测试 / 开发
python -m trpc_agent_sdk.tenants._sql_migrations sqlite:///./tenants.db

# 生产 MySQL
python -m trpc_agent_sdk.tenants._sql_migrations mysql+pymysql://user:pwd@host:3306/tenants
```

- 首先建 `schema_migrations`（version PK / name / checksum / applied_at）账本表；
- 按版本升序逐个应用待执行迁移，每个迁移与其账本行同事务提交
  （SQLite 支持事务性 DDL；MySQL DDL 隐式提交，账本行随后记录）；
- **幂等**：已应用版本跳过，可在每次部署时安全重复执行；
- **防漂移**：账本记录 DDL 校验和，同版本号内容被篡改时拒绝执行并退出码 2；
- `--dry-run` 只报告待执行版本。

**上线纪律**：迁移在离线状态由发布流水线单点执行；在线节点应先排空或保证迁移
只做向后兼容的加列/加索引，避免新旧节点混布期间的写冲突。

## 4. Memory 跨节点可见性

多节点部署下 Memory 必须落在共享后端（Redis / SQL），SDK 已提供
`RedisMemoryService` / `SqlMemoryService` / `_redis_cluster_memory_service.py`。
租户层新增 `TenantScopedMemoryService`（`_tenant_memory.py`）包装任意
`MemoryServiceABC`：

- `store_session`：session id 加 `{tenant_id}:` 前缀后透传；
- `search_memory`：key 同样加前缀，**搜索天然限定在租户命名空间内**；
- 前缀只是键命名约定，不引入任何节点本地缓存 —— 任意节点写入，任意节点可见
  （无 sticky session 依赖）。

## 5. 向量库后端

`_tenant_vector.py` 定义最小抽象 `VectorBackend`（`upsert` / `search` /
`delete` / `count`，全部按 `tenant_id` 作用域），并内置零依赖实现
`InMemoryVectorBackend`（暴力余弦相似度 + 元数据过滤 + 线程安全）。

| 后端 | 适用场景 |
|---|---|
| `InMemoryVectorBackend`（内置） | 测试、小规模单机；重启丢数据 |
| pgvector / Milvus / ES（自行实现接口） | 生产多节点；接口面小（4 个方法），接入成本低 |

与统一存储层的关系：`UnifiedStorageBackend.search_knowledge` 面向知识检索的
文本匹配；`VectorBackend` 面向 embedding 语义检索。两者按
`StorageConfig` / `DataCategory` 路由，可独立选型。

## 6. 后端间数据迁移

`_tenant_data_migration.py` 提供跨后端迁移工具，覆盖两类数据：

**租户配置迁移** `migrate_tenants(source, target, page_size=100, overwrite=False)`：

- 按 `list_tenants` 分页遍历源存储，逐个 `tenant_exists` 幂等判断后写入目标
  （已存在默认跳过，`overwrite=True` 时删除重建）；
- 单个租户失败只记录进 `MigrationReport.errors`，不中断批次——崩溃后可直接重跑。

**向量数据迁移** `migrate_vectors(source, target, tenant_ids, batch_size=500)`：

- `VectorBackend` 新增 `list_records(tenant_id)`（基类默认抛
  `NotImplementedError`，后端必须显式实现才可参与迁移，避免"静默空结果"
  被误判为空库）；按租户拉全量记录，原 id 分批 `upsert` 到目标，天然幂等；
- 迁移后逐租户回读 `count` 比对，数量不一致计入失败。

**切换前回读校验** `verify_tenant_migration(source, target, tenant_ids=None)`：
对每个租户双读比对 `name` / `config_version` / `tool_permissions`，
`report.ok` 为 True 才允许切换流量。

**四阶段切换流程**（与灰度发布 `_tenant_rollout.py` 配合）：

```
1. 双写期   写路径同时写 source 与 target（应用层或网关侧实现）
2. 追平期   跑 migrate_tenants + migrate_vectors（幂等，可多次重跑）
3. 校验期   verify_tenant_migration 全部通过 → report.ok == True
4. 切换期   灰度发布把读流量逐步切到 target（5% → 25% → 50% → 100%）；
            出问题 rollback 秒级回切 source
```

**一致性取舍声明**：迁移工具是批量离线工具，不做双写编排本身；双写期
source/target 之间存在短暂窗口不一致，最终以第 3 步回读校验 + 第 4 步灰度
观察为准。向量迁移按 `count` 回读校验，不逐条比对 embedding（浮点序列化
差异会导致假阳性）。

## 7. 测试覆盖

`tests/tenants/test_storage_sql.py`（21 用例）：

- DDL：八表齐全、双方言一致、乐观锁 `version` 列、审计 11 字段；
- 迁移：建表 + 幂等重跑、dry-run、checksum 防漂移、CLI 退出码；
- 乐观锁（aiosqlite 实库）：正常更新版本自增、过期写抛
  `OptimisticLockError`、重读重试成功、更新不存在租户抛 `ValueError`；
- Memory：store/search 均带租户前缀、close 透传；
- 向量：余弦排序、top_k、租户隔离、元数据过滤、upsert 覆盖、删除、ID 生成。
