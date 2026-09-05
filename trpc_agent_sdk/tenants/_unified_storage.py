# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Unified multi-backend data access abstraction layer.

This module provides a unified interface for accessing different storage backends,
supporting tenant-specific storage configurations and automatic backend routing.
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
from enum import Enum
import asyncio
import base64
import functools
import json
import os
import uuid

from trpc_agent_sdk.sessions import Session
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.log import logger


class StorageBackendType(Enum):
    """Supported storage backend types."""

    IN_MEMORY = "in_memory"
    REDIS = "redis"
    SQL = "sql"
    FILE_SYSTEM = "file_system"
    MEM0 = "mem0"
    MEMPALACE = "mempalace"
    LANGCHAIN_VECTORSTORE = "langchain_vectorstore"
    EXTERNAL = "external"


class DataCategory(Enum):
    """Categories of data that can be stored."""

    SESSION = "session"
    MEMORY = "memory"
    KNOWLEDGE = "knowledge"
    AUDIT_LOG = "audit_log"
    ARTIFACT = "artifact"
    SUMMARY = "summary"


class UnifiedStorageBackend(ABC):
    """Abstract base class for unified storage backends.

    This interface provides a consistent way to interact with different
    storage backends while supporting tenant-specific configurations.
    """

    @abstractmethod
    async def get_session(self, tenant_id: str, session_id: str) -> Optional[Session]:
        """Get session by ID for a specific tenant.

        Args:
            tenant_id: Tenant identifier
            session_id: Session identifier

        Returns:
            Session if found, None otherwise
        """
        pass

    @abstractmethod
    async def save_session(self, tenant_id: str, session: Session) -> None:
        """Save session for a specific tenant.

        Args:
            tenant_id: Tenant identifier
            session: Session to save
        """
        pass

    @abstractmethod
    async def add_session_event(self, tenant_id: str, session_id: str, event: Event) -> None:
        """Add event to session for a specific tenant.

        Args:
            tenant_id: Tenant identifier
            session_id: Session identifier
            event: Event to add
        """
        pass

    @abstractmethod
    async def get_memory(self, tenant_id: str, user_id: str, memory_id: str) -> Optional[Dict[str, Any]]:
        """Get memory by ID for a specific tenant and user.

        Args:
            tenant_id: Tenant identifier
            user_id: User identifier
            memory_id: Memory identifier

        Returns:
            Memory data if found, None otherwise
        """
        pass

    @abstractmethod
    async def save_memory(self, tenant_id: str, user_id: str, memory_data: Dict[str, Any]) -> None:
        """Save memory for a specific tenant and user.

        Args:
            tenant_id: Tenant identifier
            user_id: User identifier
            memory_data: Memory data to save
        """
        pass

    @abstractmethod
    async def search_knowledge(self, tenant_id: str, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Search knowledge base for a specific tenant.

        Args:
            tenant_id: Tenant identifier
            query: Search query
            limit: Maximum number of results

        Returns:
            List of matching knowledge items
        """
        pass

    @abstractmethod
    async def save_audit_log(self, tenant_id: str, audit_data: Dict[str, Any]) -> None:
        """Save audit log for a specific tenant.

        Args:
            tenant_id: Tenant identifier
            audit_data: Audit data to save
        """
        pass

    @abstractmethod
    async def health_check(self) -> bool:
        """Check if the storage backend is healthy.

        Returns:
            True if healthy, False otherwise
        """
        pass

    # ---- Summary & Artifact storage -------------------------------------
    # Concrete backends override these; the base implementations raise so a
    # backend that silently drops data is impossible to mistake for one that
    # stores it.

    async def save_summary(self, tenant_id: str, session_id: str, summary: str) -> None:
        """Persist the rolling summary of a session.

        Args:
            tenant_id: Tenant identifier.
            session_id: Session the summary belongs to.
            summary: Summary text (latest version replaces the previous one).
        """
        raise NotImplementedError(f"{type(self).__name__} does not support summary storage")

    async def get_summary(self, tenant_id: str, session_id: str) -> Optional[str]:
        """Return the stored summary for a session (None when absent)."""
        raise NotImplementedError(f"{type(self).__name__} does not support summary storage")

    async def save_artifact(self,
                            tenant_id: str,
                            artifact_id: str,
                            data: bytes,
                            metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Store an opaque binary artifact for a tenant.

        Args:
            tenant_id: Tenant identifier.
            artifact_id: Caller-provided unique id within the tenant.
            data: Raw bytes.
            metadata: Optional JSON-able descriptor (filename, mime, ...).

        Returns:
            Descriptor dict with at least ``artifact_id``, ``tenant_id`` and
            ``size``.
        """
        raise NotImplementedError(f"{type(self).__name__} does not support artifact storage")

    async def get_artifact(self, tenant_id: str, artifact_id: str) -> Optional[Dict[str, Any]]:
        """Load an artifact.

        Returns:
            Dict with ``data`` (bytes) and ``metadata``, or None when absent.
        """
        raise NotImplementedError(f"{type(self).__name__} does not support artifact storage")

    async def delete_artifact(self, tenant_id: str, artifact_id: str) -> bool:
        """Delete an artifact; returns True when it existed."""
        raise NotImplementedError(f"{type(self).__name__} does not support artifact storage")


class StorageRouter:
    """Router that directs data operations to appropriate backends.

    This class manages multiple storage backends and routes operations
    based on data category and tenant configuration.
    """

    def __init__(self, tenant_configs: Dict[str, Dict[str, str]]):
        """Initialize storage router with tenant configurations.

        Args:
            tenant_configs: Dictionary mapping tenant IDs to their storage
                          configurations, specifying which backend to use for
                          each data category.

        Example:
            {
                "tenant1": {
                    "session_backend": "redis",
                    "memory_backend": "mem0",
                    "knowledge_backend": "langchain_vectorstore",
                    "audit_backend": "sql"
                }
            }
        """
        self._tenant_configs = tenant_configs
        self._backends: Dict[str, Dict[DataCategory, UnifiedStorageBackend]] = {}
        self._backend_factories: Dict[StorageBackendType, callable] = {}

        # Register default backend factories
        self._register_default_factories()

    def _register_default_factories(self):
        """Register default storage backend factories."""
        # In-Memory backend
        self._backend_factories[StorageBackendType.IN_MEMORY] = (lambda config: InMemoryStorageBackend())

        # Redis backend
        self._backend_factories[StorageBackendType.REDIS] = (
            lambda config: RedisStorageBackend(config.get("redis_url", "redis://localhost:6379/0")))

        # SQL backend
        self._backend_factories[StorageBackendType.SQL] = (
            lambda config: SQLStorageBackend(config.get("sql_url", "sqlite:///./storage.db")))

        # File system backend (local object storage)
        self._backend_factories[StorageBackendType.FILE_SYSTEM] = (
            lambda config: FileSystemStorageBackend(config.get("storage_root", "./tenant_storage")))

    def register_backend_factory(self, backend_type: StorageBackendType, factory: callable):
        """Register a custom backend factory.

        Args:
            backend_type: Type of storage backend
            factory: Factory function that takes config and returns backend instance
        """
        self._backend_factories[backend_type] = factory
        logger.info(f"Registered custom factory for {backend_type}")

    def get_backend(self, tenant_id: str, category: DataCategory) -> UnifiedStorageBackend:
        """Get appropriate storage backend for a tenant and data category.

        Args:
            tenant_id: Tenant identifier
            category: Data category

        Returns:
            Storage backend instance

        Raises:
            ValueError: If tenant configuration is invalid or backend creation fails
        """
        if tenant_id not in self._tenant_configs:
            raise ValueError(f"No storage configuration found for tenant {tenant_id}")

        tenant_config = self._tenant_configs[tenant_id]

        # Map data category to backend type. Summaries follow the session
        # backend (same consistency domain); artifacts default to a dedicated
        # "artifact_backend" key and fall back to the session backend.
        category_to_backend_key = {
            DataCategory.SESSION: "session_backend",
            DataCategory.MEMORY: "memory_backend",
            DataCategory.KNOWLEDGE: "knowledge_backend",
            DataCategory.AUDIT_LOG: "audit_backend",
            DataCategory.SUMMARY: "session_backend",
            DataCategory.ARTIFACT: "artifact_backend",
        }

        backend_key = category_to_backend_key.get(category)
        if not backend_key:
            raise ValueError(f"Unsupported data category: {category}")

        backend_type_str = tenant_config.get(backend_key, "in_memory")
        try:
            backend_type = StorageBackendType(backend_type_str)
        except ValueError:
            raise ValueError(f"Invalid backend type: {backend_type_str}")

        # Get or create backend instance
        if tenant_id not in self._backends:
            self._backends[tenant_id] = {}

        if category not in self._backends[tenant_id]:
            # Create new backend instance
            factory = self._backend_factories.get(backend_type)
            if not factory:
                raise ValueError(f"No factory registered for backend type: {backend_type}")

            backend = factory(tenant_config)
            self._backends[tenant_id][category] = backend
            logger.info(f"Created {backend_type} backend for tenant {tenant_id}, category {category}")

        return self._backends[tenant_id][category]

    async def get_session(self, tenant_id: str, session_id: str) -> Optional[Session]:
        """Get session using appropriate backend."""
        backend = self.get_backend(tenant_id, DataCategory.SESSION)
        return await backend.get_session(tenant_id, session_id)

    async def save_session(self, tenant_id: str, session: Session) -> None:
        """Save session using appropriate backend."""
        backend = self.get_backend(tenant_id, DataCategory.SESSION)
        await backend.save_session(tenant_id, session)

    async def add_session_event(self, tenant_id: str, session_id: str, event: Event) -> None:
        """Add event using appropriate backend."""
        backend = self.get_backend(tenant_id, DataCategory.SESSION)
        await backend.add_session_event(tenant_id, session_id, event)

    async def get_memory(self, tenant_id: str, user_id: str, memory_id: str) -> Optional[Dict[str, Any]]:
        """Get memory using appropriate backend."""
        backend = self.get_backend(tenant_id, DataCategory.MEMORY)
        return await backend.get_memory(tenant_id, user_id, memory_id)

    async def save_memory(self, tenant_id: str, user_id: str, memory_data: Dict[str, Any]) -> None:
        """Save memory using appropriate backend."""
        backend = self.get_backend(tenant_id, DataCategory.MEMORY)
        await backend.save_memory(tenant_id, user_id, memory_data)

    async def search_knowledge(self, tenant_id: str, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Search knowledge using appropriate backend."""
        backend = self.get_backend(tenant_id, DataCategory.KNOWLEDGE)
        return await backend.search_knowledge(tenant_id, query, limit)

    async def save_audit_log(self, tenant_id: str, audit_data: Dict[str, Any]) -> None:
        """Save audit log using appropriate backend."""
        backend = self.get_backend(tenant_id, DataCategory.AUDIT_LOG)
        await backend.save_audit_log(tenant_id, audit_data)

    async def save_summary(self, tenant_id: str, session_id: str, summary: str) -> None:
        """Save session summary using appropriate backend."""
        backend = self.get_backend(tenant_id, DataCategory.SUMMARY)
        await backend.save_summary(tenant_id, session_id, summary)

    async def get_summary(self, tenant_id: str, session_id: str) -> Optional[str]:
        """Load session summary using appropriate backend."""
        backend = self.get_backend(tenant_id, DataCategory.SUMMARY)
        return await backend.get_summary(tenant_id, session_id)

    async def save_artifact(self,
                            tenant_id: str,
                            artifact_id: str,
                            data: bytes,
                            metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Store an artifact using appropriate backend."""
        backend = self.get_backend(tenant_id, DataCategory.ARTIFACT)
        return await backend.save_artifact(tenant_id, artifact_id, data, metadata)

    async def get_artifact(self, tenant_id: str, artifact_id: str) -> Optional[Dict[str, Any]]:
        """Load an artifact using appropriate backend."""
        backend = self.get_backend(tenant_id, DataCategory.ARTIFACT)
        return await backend.get_artifact(tenant_id, artifact_id)

    async def delete_artifact(self, tenant_id: str, artifact_id: str) -> bool:
        """Delete an artifact using appropriate backend."""
        backend = self.get_backend(tenant_id, DataCategory.ARTIFACT)
        return await backend.delete_artifact(tenant_id, artifact_id)

    async def health_check(self, tenant_id: Optional[str] = None) -> Dict[str, bool]:
        """Check health of storage backends.

        Args:
            tenant_id: Optional specific tenant to check. If None, checks all tenants.

        Returns:
            Dictionary mapping backend identifiers to health status
        """
        results = {}

        tenants_to_check = [tenant_id] if tenant_id else list(self._backends.keys())

        for tid in tenants_to_check:
            if tid not in self._backends:
                continue

            for category, backend in self._backends[tid].items():
                key = f"{tid}:{category.value}"
                try:
                    is_healthy = await backend.health_check()
                    results[key] = is_healthy
                except Exception as e:
                    logger.error(f"Health check failed for {key}: {e}")
                    results[key] = False

        return results


class InMemoryStorageBackend(UnifiedStorageBackend):
    """In-memory storage backend for development and testing.

    Note: This implementation is not suitable for production as it
    doesn't persist data across restarts and doesn't support distributed
    deployments.
    """

    def __init__(self):
        """Initialize in-memory storage."""
        self._sessions: Dict[str, Dict[str, Session]] = {}
        self._events: Dict[str, Dict[str, List[Event]]] = {}
        self._memories: Dict[str, Dict[str, Dict[str, Dict[str, Any]]]] = {}
        self._knowledge: Dict[str, List[Dict[str, Any]]] = {}
        self._audit_logs: Dict[str, List[Dict[str, Any]]] = {}
        self._summaries: Dict[str, Dict[str, str]] = {}
        self._artifacts: Dict[str, Dict[str, Dict[str, Any]]] = {}

    def _tenant_session_key(self, tenant_id: str) -> str:
        return tenant_id

    async def get_session(self, tenant_id: str, session_id: str) -> Optional[Session]:
        tenant_key = self._tenant_session_key(tenant_id)
        return self._sessions.get(tenant_key, {}).get(session_id)

    async def save_session(self, tenant_id: str, session: Session) -> None:
        tenant_key = self._tenant_session_key(tenant_id)
        if tenant_key not in self._sessions:
            self._sessions[tenant_key] = {}

        self._sessions[tenant_key][session.id] = session

    async def add_session_event(self, tenant_id: str, session_id: str, event: Event) -> None:
        tenant_key = self._tenant_session_key(tenant_id)
        if tenant_key not in self._events:
            self._events[tenant_key] = {}

        if session_id not in self._events[tenant_key]:
            self._events[tenant_key][session_id] = []

        self._events[tenant_key][session_id].append(event)

    async def get_memory(self, tenant_id: str, user_id: str, memory_id: str) -> Optional[Dict[str, Any]]:
        return self._memories.get(tenant_id, {}).get(user_id, {}).get(memory_id)

    async def save_memory(self, tenant_id: str, user_id: str, memory_data: Dict[str, Any]) -> None:
        if tenant_id not in self._memories:
            self._memories[tenant_id] = {}

        if user_id not in self._memories[tenant_id]:
            self._memories[tenant_id][user_id] = {}

        memory_id = memory_data.get("id", f"mem_{len(self._memories[tenant_id][user_id])}")
        memory_data["id"] = memory_id
        self._memories[tenant_id][user_id][memory_id] = memory_data

    async def search_knowledge(self, tenant_id: str, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        # Simple text search for demo purposes
        tenant_knowledge = self._knowledge.get(tenant_id, [])
        query_lower = query.lower()

        results = [item for item in tenant_knowledge if query_lower in str(item).lower()]

        return results[:limit]

    async def save_audit_log(self, tenant_id: str, audit_data: Dict[str, Any]) -> None:
        if tenant_id not in self._audit_logs:
            self._audit_logs[tenant_id] = []

        self._audit_logs[tenant_id].append(audit_data)

    async def health_check(self) -> bool:
        return True

    async def save_summary(self, tenant_id: str, session_id: str, summary: str) -> None:
        self._summaries.setdefault(tenant_id, {})[session_id] = summary

    async def get_summary(self, tenant_id: str, session_id: str) -> Optional[str]:
        return self._summaries.get(tenant_id, {}).get(session_id)

    async def save_artifact(self,
                            tenant_id: str,
                            artifact_id: str,
                            data: bytes,
                            metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        descriptor = {
            "artifact_id": artifact_id,
            "tenant_id": tenant_id,
            "size": len(data),
            "metadata": dict(metadata or {}),
        }
        self._artifacts.setdefault(tenant_id, {})[artifact_id] = {"data": data, "descriptor": descriptor}
        return descriptor

    async def get_artifact(self, tenant_id: str, artifact_id: str) -> Optional[Dict[str, Any]]:
        record = self._artifacts.get(tenant_id, {}).get(artifact_id)
        if record is None:
            return None
        return {"data": record["data"], "metadata": record["descriptor"]["metadata"]}

    async def delete_artifact(self, tenant_id: str, artifact_id: str) -> bool:
        tenant_artifacts = self._artifacts.get(tenant_id, {})
        if artifact_id in tenant_artifacts:
            del tenant_artifacts[artifact_id]
            return True
        return False


class RedisStorageBackend(UnifiedStorageBackend):
    """Redis-based storage backend for production deployments.

    This implementation provides persistence, high performance, and
    support for distributed deployments.
    """

    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        """Initialize Redis storage backend.

        Args:
            redis_url: Redis connection URL
        """
        try:
            import redis.asyncio as redis
        except ImportError:
            raise ImportError("Redis backend requires 'redis' package. "
                              "Install with: pip install redis")

        self._redis_url = redis_url
        self._redis: Optional[redis.Redis] = None

    async def _get_redis(self):
        """Lazy initialization of Redis connection."""
        if self._redis is None:
            import redis.asyncio as redis

            self._redis = await redis.from_url(self._redis_url, decode_responses=True)
        return self._redis

    def _tenant_prefix(self, tenant_id: str) -> str:
        """Generate key prefix for tenant."""
        return f"tenant:{tenant_id}"

    def _session_key(self, tenant_id: str, session_id: str) -> str:
        """Generate Redis key for session."""
        return f"{self._tenant_prefix(tenant_id)}:session:{session_id}"

    def _events_key(self, tenant_id: str, session_id: str) -> str:
        """Generate Redis key for session events."""
        return f"{self._tenant_prefix(tenant_id)}:events:{session_id}"

    async def get_session(self, tenant_id: str, session_id: str) -> Optional[Session]:
        redis_client = await self._get_redis()
        key = self._session_key(tenant_id, session_id)

        data = await redis_client.get(key)
        if not data:
            return None

        # Deserialize session (simplified, use proper serialization in production)
        import json
        session_data = json.loads(data)
        return Session(**session_data)

    async def save_session(self, tenant_id: str, session: Session) -> None:
        redis_client = await self._get_redis()
        key = self._session_key(tenant_id, session.id)

        # Serialize session (simplified, use proper serialization in production)
        import json
        session_data = {
            "id": session.id,
            "app_name": session.app_name,
            "user_id": session.user_id,
            "metadata": session.metadata,
            "created_at": session.created_at.isoformat() if session.created_at else None,
        }

        await redis_client.set(key, json.dumps(session_data))

    async def add_session_event(self, tenant_id: str, session_id: str, event: Event) -> None:
        redis_client = await self._get_redis()
        key = self._events_key(tenant_id, session_id)

        # Serialize event (simplified, use proper serialization in production)
        import json
        event_data = {
            "author": event.author,
            "content": {
                "parts": [{
                    "text": part.text
                } for part in event.content.parts]
            } if event.content else [],
            "timestamp": event.timestamp.isoformat() if event.timestamp else None,
        }

        # Use Redis list for events (in production, consider streams)
        await redis_client.rpush(key, json.dumps(event_data))

    async def get_memory(self, tenant_id: str, user_id: str, memory_id: str) -> Optional[Dict[str, Any]]:
        redis_client = await self._get_redis()
        key = f"{self._tenant_prefix(tenant_id)}:memory:{user_id}:{memory_id}"

        data = await redis_client.get(key)
        if not data:
            return None

        import json
        return json.loads(data)

    async def save_memory(self, tenant_id: str, user_id: str, memory_data: Dict[str, Any]) -> None:
        redis_client = await self._get_redis()
        memory_id = memory_data.get("id", f"mem_{int(asyncio.get_event_loop().time())}")
        key = f"{self._tenant_prefix(tenant_id)}:memory:{user_id}:{memory_id}"

        import json
        await redis_client.set(key, json.dumps(memory_data))

    async def search_knowledge(self, tenant_id: str, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        # Redis would typically use a separate search module or RediSearch
        # This is a simplified placeholder
        redis_client = await self._get_redis()
        pattern = f"{self._tenant_prefix(tenant_id)}:knowledge:*"

        results = []
        async for key in redis_client.scan_iter(match=pattern):
            data = await redis_client.get(key)
            if data:
                import json
                item = json.loads(data)
                if query.lower() in str(item).lower():
                    results.append(item)
                    if len(results) >= limit:
                        break

        return results

    async def save_audit_log(self, tenant_id: str, audit_data: Dict[str, Any]) -> None:
        redis_client = await self._get_redis()
        key = f"{self._tenant_prefix(tenant_id)}:audit:{int(asyncio.get_event_loop().time())}"

        import json
        await redis_client.set(key, json.dumps(audit_data))

    async def save_summary(self, tenant_id: str, session_id: str, summary: str) -> None:
        redis_client = await self._get_redis()
        key = f"{self._tenant_prefix(tenant_id)}:summary:{session_id}"
        await redis_client.set(key, summary)

    async def get_summary(self, tenant_id: str, session_id: str) -> Optional[str]:
        redis_client = await self._get_redis()
        key = f"{self._tenant_prefix(tenant_id)}:summary:{session_id}"
        return await redis_client.get(key)

    async def save_artifact(self,
                            tenant_id: str,
                            artifact_id: str,
                            data: bytes,
                            metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        redis_client = await self._get_redis()
        key = f"{self._tenant_prefix(tenant_id)}:artifact:{artifact_id}"
        descriptor = {
            "artifact_id": artifact_id,
            "tenant_id": tenant_id,
            "size": len(data),
            "metadata": dict(metadata or {}),
        }
        payload = {"descriptor": descriptor, "data_base64": base64.b64encode(data).decode("ascii")}
        await redis_client.set(key, json.dumps(payload))
        return descriptor

    async def get_artifact(self, tenant_id: str, artifact_id: str) -> Optional[Dict[str, Any]]:
        redis_client = await self._get_redis()
        key = f"{self._tenant_prefix(tenant_id)}:artifact:{artifact_id}"
        raw = await redis_client.get(key)
        if not raw:
            return None
        payload = json.loads(raw)
        return {
            "data": base64.b64decode(payload["data_base64"]),
            "metadata": payload["descriptor"]["metadata"],
        }

    async def delete_artifact(self, tenant_id: str, artifact_id: str) -> bool:
        redis_client = await self._get_redis()
        key = f"{self._tenant_prefix(tenant_id)}:artifact:{artifact_id}"
        return bool(await redis_client.delete(key))

    async def health_check(self) -> bool:
        try:
            redis_client = await self._get_redis()
            await redis_client.ping()
            return True
        except Exception as e:
            logger.error(f"Redis health check failed: {e}")
            return False


class FileSystemStorageBackend(UnifiedStorageBackend):
    """File-system storage backend — local object storage for artifacts.

    Layout (all paths are namespaced by tenant, never sharing directories):

        <root>/<tenant_id>/sessions/<session_id>.json
        <root>/<tenant_id>/events/<session_id>.jsonl
        <root>/<tenant_id>/memory/<user_id>/<memory_id>.json
        <root>/<tenant_id>/knowledge/<item_id>.json
        <root>/<tenant_id>/audit/audit.jsonl
        <root>/<tenant_id>/summaries/<session_id>.txt
        <root>/<tenant_id>/artifacts/<artifact_id>        (raw bytes)
        <root>/<tenant_id>/artifacts/<artifact_id>.meta.json

    Suited for single-node deployments and as a drop-in local object store;
    for S3/OSS/MinIO, register a custom factory via
    ``StorageRouter.register_backend_factory`` implementing the same interface.
    """

    def __init__(self, root: str = "./tenant_storage"):
        self._root = root

    # -- path helpers ------------------------------------------------------

    def _component(self, *parts: str) -> str:
        """Build a safe relative path, rejecting separators/parent refs."""
        for part in parts:
            if not part or "/" in part or "\\" in part or part in (".", ".."):
                raise ValueError(f"Unsafe path component: {part!r}")
        return os.path.join(self._root, *parts)

    @staticmethod
    def _run_sync(func, *args):
        return asyncio.get_event_loop().run_in_executor(None, functools.partial(func, *args))

    def _write_text(self, path: str, text: str) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)

    def _read_text(self, path: str) -> Optional[str]:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return f.read()
        except FileNotFoundError:
            return None

    def _append_text(self, path: str, text: str) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(text)

    # -- sessions ----------------------------------------------------------

    def _session_path(self, tenant_id: str, session_id: str) -> str:
        return self._component(tenant_id, "sessions", f"{session_id}.json")

    async def get_session(self, tenant_id: str, session_id: str) -> Optional[Session]:
        raw = await self._run_sync(self._read_text, self._session_path(tenant_id, session_id))
        if raw is None:
            return None
        return Session(**json.loads(raw))

    async def save_session(self, tenant_id: str, session: Session) -> None:
        data = session.model_dump()
        await self._run_sync(self._write_text, self._session_path(tenant_id, session.id), json.dumps(data))

    def _events_path(self, tenant_id: str, session_id: str) -> str:
        return self._component(tenant_id, "events", f"{session_id}.jsonl")

    async def add_session_event(self, tenant_id: str, session_id: str, event: Event) -> None:
        event_data = {
            "author": event.author,
            "content": {
                "parts": [{
                    "text": part.text
                } for part in event.content.parts]
            } if event.content else [],
            "timestamp": event.timestamp.isoformat() if event.timestamp else None,
        }
        await self._run_sync(self._append_text, self._events_path(tenant_id, session_id), json.dumps(event_data) + "\n")

    # -- memory ------------------------------------------------------------

    def _memory_path(self, tenant_id: str, user_id: str, memory_id: str) -> str:
        return self._component(tenant_id, "memory", user_id, f"{memory_id}.json")

    async def get_memory(self, tenant_id: str, user_id: str, memory_id: str) -> Optional[Dict[str, Any]]:
        raw = await self._run_sync(self._read_text, self._memory_path(tenant_id, user_id, memory_id))
        return json.loads(raw) if raw is not None else None

    async def save_memory(self, tenant_id: str, user_id: str, memory_data: Dict[str, Any]) -> None:
        memory_id = memory_data.get("id", f"mem_{uuid.uuid4().hex[:12]}")
        memory_data["id"] = memory_id
        await self._run_sync(self._write_text, self._memory_path(tenant_id, user_id, memory_id),
                             json.dumps(memory_data))

    # -- knowledge ---------------------------------------------------------

    def _knowledge_dir(self, tenant_id: str) -> str:
        return self._component(tenant_id, "knowledge")

    async def search_knowledge(self, tenant_id: str, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        directory = self._knowledge_dir(tenant_id)

        def _scan():
            if not os.path.isdir(directory):
                return []
            hits = []
            for name in sorted(os.listdir(directory)):
                if not name.endswith(".json"):
                    continue
                with open(os.path.join(directory, name), "r", encoding="utf-8") as f:
                    item = json.load(f)
                if query.lower() in str(item).lower():
                    hits.append(item)
                    if len(hits) >= limit:
                        break
            return hits

        return await self._run_sync(_scan)

    # -- audit -------------------------------------------------------------

    async def save_audit_log(self, tenant_id: str, audit_data: Dict[str, Any]) -> None:
        path = self._component(tenant_id, "audit", "audit.jsonl")
        await self._run_sync(self._append_text, path, json.dumps(audit_data) + "\n")

    # -- summary -----------------------------------------------------------

    def _summary_path(self, tenant_id: str, session_id: str) -> str:
        return self._component(tenant_id, "summaries", f"{session_id}.txt")

    async def save_summary(self, tenant_id: str, session_id: str, summary: str) -> None:
        await self._run_sync(self._write_text, self._summary_path(tenant_id, session_id), summary)

    async def get_summary(self, tenant_id: str, session_id: str) -> Optional[str]:
        return await self._run_sync(self._read_text, self._summary_path(tenant_id, session_id))

    # -- artifacts (the primary use case: local object storage) ------------

    def _artifact_path(self, tenant_id: str, artifact_id: str) -> str:
        return self._component(tenant_id, "artifacts", artifact_id)

    async def save_artifact(self,
                            tenant_id: str,
                            artifact_id: str,
                            data: bytes,
                            metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        descriptor = {
            "artifact_id": artifact_id,
            "tenant_id": tenant_id,
            "size": len(data),
            "metadata": dict(metadata or {}),
        }

        def _write():
            data_path = self._artifact_path(tenant_id, artifact_id)
            os.makedirs(os.path.dirname(data_path), exist_ok=True)
            with open(data_path, "wb") as f:
                f.write(data)
            with open(data_path + ".meta.json", "w", encoding="utf-8") as f:
                json.dump(descriptor, f)

        await self._run_sync(_write)
        return descriptor

    async def get_artifact(self, tenant_id: str, artifact_id: str) -> Optional[Dict[str, Any]]:
        data_path = self._artifact_path(tenant_id, artifact_id)

        def _read():
            try:
                with open(data_path, "rb") as f:
                    blob = f.read()
                with open(data_path + ".meta.json", "r", encoding="utf-8") as f:
                    descriptor = json.load(f)
                return blob, descriptor
            except FileNotFoundError:
                return None, None

        blob, descriptor = await self._run_sync(_read)
        if blob is None:
            return None
        return {"data": blob, "metadata": descriptor.get("metadata", {})}

    async def delete_artifact(self, tenant_id: str, artifact_id: str) -> bool:
        data_path = self._artifact_path(tenant_id, artifact_id)

        def _delete():
            existed = os.path.exists(data_path)
            for path in (data_path, data_path + ".meta.json"):
                try:
                    os.remove(path)
                except FileNotFoundError:
                    pass
            return existed

        return await self._run_sync(_delete)

    # -- health ------------------------------------------------------------

    async def health_check(self) -> bool:
        try:
            await self._run_sync(os.makedirs, self._root, True)
            probe = os.path.join(self._root, ".health_probe")
            await self._run_sync(self._write_text, probe, "ok")
            return True
        except Exception as e:
            logger.error(f"File system health check failed: {e}")
            return False


class SQLStorageBackend(UnifiedStorageBackend):
    """SQL-based storage backend for production deployments.

    This implementation provides ACID transactions, complex querying,
    and data persistence with backup/restore capabilities.
    """

    def __init__(self, database_url: str = "sqlite:///./storage.db"):
        """Initialize SQL storage backend.

        Args:
            database_url: Database connection URL
        """
        try:
            from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession  # noqa: F401
            from sqlalchemy.orm import sessionmaker  # noqa: F401
        except ImportError:
            raise ImportError("SQL backend requires 'sqlalchemy' package. "
                              "Install with: pip install sqlalchemy[asyncio]")

        self._database_url = database_url
        self._engine = None
        self._session_factory = None

    async def _initialize(self):
        """Initialize database connection and create tables."""
        if self._engine is None:
            from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
            from sqlalchemy.orm import sessionmaker
            from sqlalchemy import Column, String, Text, DateTime, Integer
            from sqlalchemy.ext.declarative import declarative_base

            # Convert sqlite:// to sqlite+aiosqlite:// for async support
            db_url = self._database_url
            if db_url.startswith("sqlite://"):
                db_url = db_url.replace("sqlite://", "sqlite+aiosqlite://")

            self._engine = create_async_engine(db_url, echo=False)
            self._session_factory = sessionmaker(bind=self._engine, class_=AsyncSession, expire_on_commit=False)

            # Create tables (simplified, proper schema needed in production)
            Base = declarative_base()

            class SessionRecord(Base):
                __tablename__ = "sessions"
                id = Column(String(64), primary_key=True)
                tenant_id = Column(String(64), nullable=False, index=True)
                app_name = Column(String(256))
                user_id = Column(String(256))
                metadata_json = Column(Text)
                created_at = Column(DateTime)

            class EventRecord(Base):
                __tablename__ = "events"
                id = Column(Integer, primary_key=True, autoincrement=True)
                tenant_id = Column(String(64), nullable=False, index=True)
                session_id = Column(String(64), nullable=False, index=True)
                event_json = Column(Text)
                created_at = Column(DateTime)

            class SummaryRecord(Base):
                __tablename__ = "summaries"
                tenant_id = Column(String(64), primary_key=True)
                session_id = Column(String(64), primary_key=True)
                summary = Column(Text)
                updated_at = Column(DateTime)

            class ArtifactRecord(Base):
                __tablename__ = "artifacts"
                tenant_id = Column(String(64), primary_key=True)
                artifact_id = Column(String(256), primary_key=True)
                data = Column(Text)  # base64-encoded bytes (portable across drivers)
                metadata_json = Column(Text)
                updated_at = Column(DateTime)

            # AsyncEngine requires DDL via run_sync on a connection
            async with self._engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)

    async def _get_session(self):
        """Get database session."""
        await self._initialize()
        return self._session_factory()

    async def get_session(self, tenant_id: str, session_id: str) -> Optional[Session]:
        session = await self._get_session()

        try:
            from sqlalchemy import text

            result = await session.execute(text("SELECT * FROM sessions WHERE id = :sid AND tenant_id = :tid"), {
                "sid": session_id,
                "tid": tenant_id
            })
            row = result.fetchone()

            if not row:
                return None

            # Parse and return Session object
            import json
            return Session(
                id=row.id,
                app_name=row.app_name,
                user_id=row.user_id,
                metadata=json.loads(row.metadata_json) if row.metadata_json else {},
                created_at=row.created_at,
            )
        finally:
            await session.close()

    async def save_session(self, tenant_id: str, session: Session) -> None:
        session_db = await self._get_session()

        try:
            import json
            from sqlalchemy import text

            await session_db.execute(
                text("""
                    INSERT OR REPLACE INTO sessions (id, tenant_id, app_name, user_id, metadata_json, created_at)
                    VALUES (:id, :tid, :app_name, :user_id, :metadata, :created_at)
                """), {
                    "id": session.id,
                    "tid": tenant_id,
                    "app_name": session.app_name,
                    "user_id": session.user_id,
                    "metadata": json.dumps(session.metadata),
                    "created_at": session.created_at,
                })
            await session_db.commit()
        except Exception:
            await session_db.rollback()
            raise
        finally:
            await session_db.close()

    async def add_session_event(self, tenant_id: str, session_id: str, event: Event) -> None:
        session_db = await self._get_session()

        try:
            import json
            from datetime import datetime
            from sqlalchemy import text

            event_json = {
                "author": event.author,
                "content": {
                    "parts": [{
                        "text": part.text
                    } for part in event.content.parts] if event.content else []
                },
                "timestamp": event.timestamp.isoformat() if event.timestamp else datetime.utcnow().isoformat(),
            }

            await session_db.execute(
                text("""
                    INSERT INTO events (tenant_id, session_id, event_json, created_at)
                    VALUES (:tid, :sid, :event_json, :created_at)
                """), {
                    "tid": tenant_id,
                    "sid": session_id,
                    "event_json": json.dumps(event_json),
                    "created_at": datetime.utcnow(),
                })
            await session_db.commit()
        except Exception:
            await session_db.rollback()
            raise
        finally:
            await session_db.close()

    async def get_memory(self, tenant_id: str, user_id: str, memory_id: str) -> Optional[Dict[str, Any]]:
        # Simplified implementation - would need proper memory table
        return None

    async def save_memory(self, tenant_id: str, user_id: str, memory_data: Dict[str, Any]) -> None:
        # Simplified implementation - would need proper memory table
        pass

    async def search_knowledge(self, tenant_id: str, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        # Simplified implementation - would need proper knowledge table with search
        return []

    async def save_audit_log(self, tenant_id: str, audit_data: Dict[str, Any]) -> None:
        session_db = await self._get_session()

        try:
            import json
            from datetime import datetime
            from sqlalchemy import text

            await session_db.execute(
                text("""
                    INSERT INTO audit_logs (tenant_id, audit_json, created_at)
                    VALUES (:tid, :audit_json, :created_at)
                """), {
                    "tid": tenant_id,
                    "audit_json": json.dumps(audit_data),
                    "created_at": datetime.utcnow(),
                })
            await session_db.commit()
        except Exception:
            await session_db.rollback()
            raise
        finally:
            await session_db.close()

    async def health_check(self) -> bool:
        try:
            from sqlalchemy import text

            session = await self._get_session()
            await session.execute(text("SELECT 1"))
            await session.close()
            return True
        except Exception as e:
            logger.error(f"SQL health check failed: {e}")
            return False

    async def save_summary(self, tenant_id: str, session_id: str, summary: str) -> None:
        session_db = await self._get_session()

        try:
            from datetime import datetime
            from sqlalchemy import text

            await session_db.execute(
                text("""
                    INSERT OR REPLACE INTO summaries (tenant_id, session_id, summary, updated_at)
                    VALUES (:tid, :sid, :summary, :updated_at)
                """), {
                    "tid": tenant_id,
                    "sid": session_id,
                    "summary": summary,
                    "updated_at": datetime.utcnow(),
                })
            await session_db.commit()
        except Exception:
            await session_db.rollback()
            raise
        finally:
            await session_db.close()

    async def get_summary(self, tenant_id: str, session_id: str) -> Optional[str]:
        session_db = await self._get_session()

        try:
            from sqlalchemy import text

            result = await session_db.execute(
                text("SELECT summary FROM summaries WHERE tenant_id = :tid AND session_id = :sid"), {
                    "tid": tenant_id,
                    "sid": session_id,
                })
            row = result.fetchone()
            return row.summary if row else None
        finally:
            await session_db.close()

    async def save_artifact(self,
                            tenant_id: str,
                            artifact_id: str,
                            data: bytes,
                            metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        session_db = await self._get_session()

        try:
            from datetime import datetime
            from sqlalchemy import text

            descriptor = {
                "artifact_id": artifact_id,
                "tenant_id": tenant_id,
                "size": len(data),
                "metadata": dict(metadata or {}),
            }
            await session_db.execute(
                text("""
                    INSERT OR REPLACE INTO artifacts
                        (tenant_id, artifact_id, data, metadata_json, updated_at)
                    VALUES (:tid, :aid, :data, :metadata, :updated_at)
                """), {
                    "tid": tenant_id,
                    "aid": artifact_id,
                    "data": base64.b64encode(data).decode("ascii"),
                    "metadata": json.dumps(descriptor),
                    "updated_at": datetime.utcnow(),
                })
            await session_db.commit()
            return descriptor
        except Exception:
            await session_db.rollback()
            raise
        finally:
            await session_db.close()

    async def get_artifact(self, tenant_id: str, artifact_id: str) -> Optional[Dict[str, Any]]:
        session_db = await self._get_session()

        try:
            from sqlalchemy import text

            result = await session_db.execute(
                text("SELECT data, metadata_json FROM artifacts WHERE tenant_id = :tid AND artifact_id = :aid"), {
                    "tid": tenant_id,
                    "aid": artifact_id,
                })
            row = result.fetchone()
            if not row:
                return None
            metadata = json.loads(row.metadata_json) if row.metadata_json else {}
            return {
                "data": base64.b64decode(row.data.encode("ascii")),
                "metadata": metadata.get("metadata", {}),
            }
        finally:
            await session_db.close()

    async def delete_artifact(self, tenant_id: str, artifact_id: str) -> bool:
        session_db = await self._get_session()

        try:
            from sqlalchemy import text

            result = await session_db.execute(
                text("DELETE FROM artifacts WHERE tenant_id = :tid AND artifact_id = :aid"), {
                    "tid": tenant_id,
                    "aid": artifact_id,
                })
            await session_db.commit()
            return result.rowcount > 0
        except Exception:
            await session_db.rollback()
            raise
        finally:
            await session_db.close()
