# 多租户节点部署拓扑设计

> **文档性质：部署设计蓝图（blueprint）**。本文描述目标生产拓扑与容量规划方法，
> 其中 Gateway/Worker 分层、Redis Cluster、PostgreSQL HA、Kubernetes 编排等为
> **设计方案，仓库中不含可直接部署的 gateway/worker 服务与 k8s 清单**。
> 已实现并测试过的能力见以下文档与代码：
>
> - 租户模型/存储/路由/IM：`trpc_agent_sdk/tenants/`（200+ 单测，`tests/tenants/`）
> - 数据一致性与迁移：`MULTI_TENANT_CONSISTENCY.md`
> - 故障恢复与运维 SOP：`MULTI_TENANT_OPERATIONS.md`
> - 容器化冒烟：`examples/multi_tenant_demo/docker-compose.yml`（Redis + demo 节点）
>
> 文中代码示例为设计示意，个别 API 签名以 SDK 源码为准。

## 🏗️ 架构概览

### 组件拓扑图

```
┌─────────────────────────────────────────────────────────────────┐
│                        External Load Balancer                     │
│                    (Nginx/HAProxy/Cloudflare)                     │
└────────────────────────────┬────────────────────────────────────┘
                             │
                ┌────────────┴────────────┐
                │   TLS Termination       │
                └────────────┬────────────┘
                             │
    ┌────────────────────────┼────────────────────────┐
    │                        │                        │
┌───▼──────────┐    ┌────────▼─────────┐    ┌──────▼──────┐
│  Gateway 1   │    │   Gateway 2      │    │  Gateway N  │  ← Agent Gateway Layer
└───┬──────────┘    └────────┬─────────┘    └──────┬──────┘
    │                        │                        │
    │   ┌────────────────────┼────────────────────┐ │
    │   │   Tenant Router    │   Load Balancer     │ │
    │   └────────────────────┼────────────────────┘ │
    │                        │                        │
    └────────────────────────┼────────────────────────┘
                             │
    ┌────────────────────────┼────────────────────────┐
    │                        │                        │
┌───▼──────────┐    ┌────────▼─────────┐    ┌──────▼──────┐
│  Worker 1    │    │   Worker 2       │    │  Worker N   │  ← Agent Worker Layer
│              │    │                  │    │             │  (Stateless)
│  ┌────────┐  │    │   ┌──────────┐  │    │  ┌────────┐ │
│  │Runner  │  │    │   │  Runner  │  │    │  │ Runner │ │
│  └────────┘  │    │   └──────────┘  │    │  └────────┘ │
└───┬──────────┘    └────────┬─────────┘    └──────┬──────┘
    │                        │                        │
    └────────────────────────┼────────────────────────┘
                             │
    ┌────────────────────────┼────────────────────────┐
    │                        │                        │
┌───▼──────────┐    ┌────────▼─────────┐    ┌──────▼──────┐
│ Redis Cluster│    │  PostgreSQL HA    │    │   Vector    │  ← Storage Layer
│ (Session)    │    │  (Audit/Tenant)   │    │   Store     │
└──────────────┘    └───────────────────┘    └─────────────┘
                             │
    ┌────────────────────────┼────────────────────────┐
    │                        │                        │
┌───▼──────────┐    ┌────────▼─────────┐    ┌──────▼──────┐
│   Message    │    │  OpenTelemetry   │    │   Admin     │  ← Services Layer
│   Queue      │    │  Collector       │    │   API       │
└──────────────┘    └───────────────────┘    └─────────────┘
```

## 🔧 核心组件

### 1. Agent Gateway Layer

**职责：**
- 租户识别和路由
- 请求验证和授权
- 负载均衡到 Workers
- 结果聚合和响应

**技术栈：**
- FastAPI + Gunicorn
- 租户路由中间件
- 请求限流和熔断
- 健康检查端点

**配置示例：**
```python
# gateway_config.py
from fastapi import FastAPI
from trpc_agent_sdk.tenants import (
    TenantRouter, HttpHeaderIdentifier,
    RedisTenantStore
)

app = FastAPI()

# 租户存储
tenant_store = RedisTenantStore("redis://redis-cluster:6379/0")

# 租户路由器
tenant_router = TenantRouter(tenant_store)
tenant_router.add_identifier(HttpHeaderIdentifier("X-Tenant-ID"))
tenant_router.add_identifier(ApiKeyIdentifier({
    "sk_key1": "tenant1",
    "sk_key2": "tenant2"
}))

@app.middleware("http")
async def tenant_routing_middleware(request, call_next):
    # 租户识别和路由
    tenant = await tenant_router.route_request(request)
    if not tenant:
        raise HTTPException(status_code=401, detail="Invalid tenant")

    # 设置租户上下文
    context_manager = get_context_manager()
    with context_manager.with_tenant(tenant):
        response = await call_next(request)
        return response
```

### 2. Agent Worker Layer

**职责：**
- 无状态 Agent 执行
- Session/Memory 操作
- 工具调用和结果处理
- 流式事件生成

**特性：**
- **无状态设计**: 完全依赖共享存储，支持水平扩展
- **并发控制**: 每个Worker处理多个并发session
- **优雅关闭**: 完成进行中的任务后再退出
- **健康检查**: 向Gateway报告健康状态

**Worker 配置：**
```python
# worker_config.py
import asyncio
from trpc_agent_sdk.tenants import get_current_tenant_context
from trpc_agent_sdk.runners import Runner

class TenantWorker:
    def __init__(self, worker_id: str, max_concurrency: int = 10):
        self.worker_id = worker_id
        self.max_concurrency = max_concurrency
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.is_healthy = True

    async def process_request(
        self, tenant_id: str, user_id: str, session_id: str, message: str
    ):
        async with self.semaphore:  # 并发控制
            try:
                # 获取租户上下文
                context = get_current_tenant_context(tenant_id)

                # 创建租户特定的Runner
                runner = await create_tenant_runner(
                    tenant_context=context,
                    agent=self.get_tenant_agent(context),
                    base_session_service=self.session_service,
                )

                # 执行Agent
                events = []
                async for event in runner.run_async(
                    user_id=user_id, session_id=session_id, new_message=message
                ):
                    events.append(event)
                    yield event

                # 记录执行指标
                self.record_execution_metrics(tenant_id, events)

            except Exception as e:
                self.is_healthy = False
                logger.error(f"Worker {self.worker_id} error: {e}")
                raise

    def get_tenant_agent(self, tenant_context):
        """根据租户配置创建Agent"""
        model_config = tenant_context.get_model_config()

        # 创建租户特定的模型
        model = OpenAIModel(
            model_name=model_config["model_name"],
            api_key=model_config["api_key"],
            base_url=model_config.get("base_url", ""),
        )

        # 应用租户工具权限
        allowed_tools = tenant_context.get_allowed_tools()
        tools = self.create_tools(allowed_tools)

        return LlmAgent(
            name=tenant_context.tenant.name,
            model=model,
            tools=tools,
        )
```

### 3. Storage Layer

**职责：**
- 分布式状态存储
- 数据持久化和备份
- 跨节点数据同步
- 故障恢复和一致性保证

**Redis Cluster 配置：**
```yaml
# docker-compose.yml
redis-cluster:
  image: redis:7-alpine
  command: redis-cli --cluster create redis-node-1:6379 redis-node-2:6379 redis-node-3:6379 redis-node-4:6379 redis-node-5:6379 redis-node-6:6379 --cluster-yes
  networks:
    - tenant-network

redis-node-1:
  image: redis:7-alpine
  command: redis-server --cluster-enabled yes --cluster-config-file nodes.conf --port 6379
  networks:
    - tenant-network

# 其他节点配置类似...
```

**PostgreSQL HA 配置：**
```yaml
postgres-primary:
  image: postgres:15-alpine
  environment:
    POSTGRES_REPLICATION_MODE: master
    POSTGRES_REPLICATION_USER: replicator
    POSTGRES_REPLICATION_PASSWORD: rep_pass
  volumes:
    - postgres_data:/var/lib/postgresql/data
  networks:
    - tenant-network

postgres-replica:
  image: postgres:15-alpine
  environment:
    POSTGRES_REPLICATION_MODE: slave
    POSTGRES_MASTER_HOST: postgres-primary
    POSTGRES_REPLICATION_USER: replicator
    POSTGRES_REPLICATION_PASSWORD: rep_pass
  depends_on:
    - postgres-primary
  networks:
    - tenant-network
```

## 🚀 部署方案

### 方案一：Docker Compose (开发/测试环境)

**docker-compose.yml:**
```yaml
version: '3.8'

services:
  # 负载均衡
  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf
    depends_on:
      - gateway-1
      - gateway-2
    networks:
      - tenant-network

  # API 网关
  gateway-1:
    build: ./gateway
    environment:
      - GATEWAY_ID=gateway-1
      - REDIS_URL=redis://redis-cluster:6379/0
      - WORKER_URLS=worker-1:8080,worker-2:8080
    depends_on:
      - redis-cluster
    networks:
      - tenant-network

  gateway-2:
    build: ./gateway
    environment:
      - GATEWAY_ID=gateway-2
      - REDIS_URL=redis://redis-cluster:6379/0
      - WORKER_URLS=worker-1:8080,worker-2:8080
    depends_on:
      - redis-cluster
    networks:
      - tenant-network

  # Worker 节点
  worker-1:
    build: ./worker
    environment:
      - WORKER_ID=worker-1
      - REDIS_URL=redis://redis-cluster:6379/1
      - POSTGRES_URL=postgresql://user:pass@postgres-primary/tenants
      - MAX_CONCURRENCY=20
    depends_on:
      - redis-cluster
      - postgres-primary
    networks:
      - tenant-network

  worker-2:
    build: ./worker
    environment:
      - WORKER_ID=worker-2
      - REDIS_URL=redis://redis-cluster:6379/1
      - POSTGRES_URL=postgresql://user:pass@postgres-primary/tenants
      - MAX_CONCURRENCY=20
    depends_on:
      - redis-cluster
      - postgres-primary
    networks:
      - tenant-network

  # Redis Cluster
  redis-cluster:
    image: redis:7-alpine
    command: redis-cli --cluster create \
      redis-node-1:6379 redis-node-2:6379 redis-node-3:6379 \
      --cluster-yes
    depends_on:
      - redis-node-1
      - redis-node-2
      - redis-node-3
    networks:
      - tenant-network

  redis-node-1:
    image: redis:7-alpine
    command: redis-server --cluster-enabled yes --port 6379
    volumes:
      - redis_data_1:/data
    networks:
      - tenant-network

  redis-node-2:
    image: redis:7-alpine
    command: redis-server --cluster-enabled yes --port 6379
    volumes:
      - redis_data_2:/data
    networks:
      - tenant-network

  redis-node-3:
    image: redis:7-alpine
    command: redis-server --cluster-enabled yes --port 6379
    volumes:
      - redis_data_3:/data
    networks:
      - tenant-network

  # PostgreSQL
  postgres-primary:
    image: postgres:15-alpine
    environment:
      POSTGRES_DB: tenants
      POSTGRES_USER: tenant_user
      POSTGRES_PASSWORD: secure_password
    volumes:
      - postgres_data:/var/lib/postgresql/data
    networks:
      - tenant-network

  # 监控服务
  prometheus:
    image: prometheus:latest
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    ports:
      - "9090:9090"
    networks:
      - tenant-network

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - grafana_data:/var/lib/grafana
    networks:
      - tenant-network

networks:
  tenant-network:
    driver: bridge

volumes:
  redis_data_1:
  redis_data_2:
  redis_data_3:
  postgres_data:
  grafana_data:
```

### 方案二：Kubernetes (生产环境)

**部署架构：**
```yaml
# k8s/gateway-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tenant-gateway
spec:
  replicas: 3
  selector:
    matchLabels:
      app: tenant-gateway
  template:
    metadata:
      labels:
        app: tenant-gateway
    spec:
      containers:
      - name: gateway
        image: trpc-agent/gateway:latest
        ports:
        - containerPort: 8080
        env:
        - name: REDIS_URL
          valueFrom:
            configMapKeyRef:
              name: tenant-config
              key: redis-url
        - name: WORKER_URLS
          value: "http://tenant-worker-service:8080"
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "1Gi"
            cpu: "1000m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5

---
apiVersion: v1
kind: Service
metadata:
  name: tenant-gateway-service
spec:
  selector:
    app: tenant-gateway
  ports:
  - protocol: TCP
    port: 80
    targetPort: 8080
  type: LoadBalancer
```

**Worker部署：**
```yaml
# k8s/worker-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tenant-worker
spec:
  replicas: 10  # 可根据负载自动扩展
  selector:
    matchLabels:
      app: tenant-worker
  template:
    metadata:
      labels:
        app: tenant-worker
    spec:
      containers:
      - name: worker
        image: trpc-agent/worker:latest
        ports:
        - containerPort: 8080
        env:
        - name: WORKER_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: REDIS_URL
          valueFrom:
            configMapKeyRef:
              name: tenant-config
              key: redis-url
        - name: MAX_CONCURRENCY
          value: "20"
        resources:
          requests:
            memory: "1Gi"
            cpu: "1000m"
          limits:
            memory: "2Gi"
            cpu: "2000m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 60
          periodSeconds: 30

---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: tenant-worker-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: tenant-worker
  minReplicas: 5
  maxReplicas: 20
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
```

## 📊 水平扩展策略

### 无状态Worker扩展

**优点：**
- 支持动态增减节点
- 无需Sticky Session
- 负载均衡简单高效

**实现：**
```bash
# 手动扩展Worker数量
kubectl scale deployment tenant-worker --replicas=15

# 自动扩展 (基于CPU/内存)
kubectl autoscale deployment tenant-worker \
  --min=5 --max=20 \
  --cpu-percent=70 \
  --memory-percent=80

# 基于自定义指标 (并发session数)
kubectl autoscale deployment tenant-worker \
  --min=5 --max=20 \
  --custom-metrics=concurrent_sessions_per_worker<50
```

### Gateway扩展

**策略：**
- 基于请求吞吐量扩展
- 保持Gateway:Worker = 1:3的比例
- 独立扩展避免相互影响

**监控指标：**
```yaml
# Gateway HPA 配置
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: tenant-gateway-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: tenant-gateway
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: Pods
    pods:
      metric:
        name: requests_per_second
      target:
        type: AverageValue
        averageValue: "1000"
```

## 🔍 监控和可观测性

### Prometheus 监控指标

**业务指标：**
```python
# metrics.py
from prometheus_client import Counter, Histogram, Gauge

# 请求计数
tenant_requests_total = Counter(
    'tenant_requests_total',
    'Total requests per tenant',
    ['tenant_id', 'status']
)

# 执行延迟
agent_execution_duration = Histogram(
    'agent_execution_duration_seconds',
    'Agent execution duration',
    ['tenant_id', 'agent_name']
)

# 并发session数
concurrent_sessions = Gauge(
    'concurrent_sessions',
    'Current number of concurrent sessions',
    ['tenant_id']
)

# 工具调用
tool_calls_total = Counter(
    'tool_calls_total',
    'Total tool calls',
    ['tenant_id', 'tool_name', 'status']
)
```

### Grafana 仪表板

**关键指标：**
1. **租户级别指标：**
   - QPS per tenant
   - Latency per tenant
   - Error rate per tenant
   - Cost per tenant

2. **系统级别指标：**
   - Worker CPU/Memory 使用率
   - Redis 连接数和延迟
   - PostgreSQL QPS 和连接数
   - 网络流量

3. **业务级别指标：**
   - 活跃session数
   - 平均对话轮次
   - 工具调用成功率
   - Token消耗趋势

## 🚦 故障恢复

### 优雅关闭
```python
# worker_shutdown.py
import signal
import asyncio

class GracefulShutdown:
    def __init__(self, worker):
        self.worker = worker
        self.shutdown = False

        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

    def handle_shutdown(self, signum, frame):
        logger.info("Shutdown signal received")
        self.shutdown = True

    async def wait_for_completion(self):
        """等待进行中的任务完成"""
        timeout = 30  # 30秒超时
        start_time = time.time()

        while self.worker.active_sessions > 0:
            if time.time() - start_time > timeout:
                logger.warning("Shutdown timeout, forcing exit")
                break
            logger.info(f"Waiting for {self.worker.active_sessions} sessions to complete")
            await asyncio.sleep(1)

        logger.info("All sessions completed, shutting down")
```

### 健康检查
```python
# health_check.py
from fastapi import FastAPI

app = FastAPI()

@app.get("/health")
async def health_check():
    """基本健康检查"""
    return {"status": "healthy"}

@app.get("/ready")
async def readiness_check():
    """就绪检查 - 验证依赖服务"""
    try:
        # 检查Redis连接
        redis_client.ping()

        # 检查数据库连接
        await database.execute("SELECT 1")

        # 检查并发数是否已达上限
        if current_sessions >= MAX_CONCURRENCY:
            return {"status": "not_ready", "reason": "max_concurrency_reached"}

        return {"status": "ready"}
    except Exception as e:
        return {"status": "not_ready", "reason": str(e)}
```

这个部署设计提供了完整的水平扩展能力，支持从开发环境到生产环境的平滑升级，确保了系统的高可用性和可扩展性。