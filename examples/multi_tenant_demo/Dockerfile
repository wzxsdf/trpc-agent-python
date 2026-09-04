# 容器化多租户冒烟演示镜像。
# 构建上下文必须是仓库根目录：
#   docker build -f examples/multi_tenant_demo/Dockerfile -t trpc-tenant-demo .
# 或直接使用同目录 docker-compose.yml。
FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUTF8=1 \
    PYTHONUNBUFFERED=1

# python-magic 需要 libmagic（SDK 全量导入依赖）
RUN apt-get update \
    && apt-get install -y --no-install-recommends libmagic1 \
    && rm -rf /var/lib/apt/lists/*

# 先装依赖层（pyproject 变化少，命中缓存）
COPY pyproject.toml README.md ./
RUN pip install --no-cache-dir .

# 再拷贝源码与演示脚本
COPY trpc_agent_sdk ./trpc_agent_sdk
COPY examples/multi_tenant_demo ./examples/multi_tenant_demo

# 冒烟入口：REDIS_URL 指向 redis 服务时走共享存储（多副本安全，脚本幂等）
CMD ["python", "examples/multi_tenant_demo/full_stack_demo.py"]
