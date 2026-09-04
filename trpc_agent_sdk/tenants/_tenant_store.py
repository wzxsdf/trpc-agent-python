# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Multi-tenant storage implementations.

This module provides various storage backends for tenant configuration,
including in-memory, Redis, and SQL implementations.
"""

from abc import ABC, abstractmethod
from typing import List, Optional
from datetime import datetime

from trpc_agent_sdk.log import logger

from ._tenant_model import Tenant, TenantConfig


class OptimisticLockError(Exception):
    """Raised when a concurrent writer modified the row first.

    The SQL tenant store guards ``update_tenant`` with the row's ``version``
    column: the update only applies when the version read by the caller is
    still current. On conflict the caller should re-read the tenant and retry
    (last-writer-wins with detection, not silent overwrite).
    """


def _tenant_with_version(tenant: Tenant, version: int) -> Tenant:
    """Return a shallow copy of ``tenant`` with a specific lock version."""
    import dataclasses

    return dataclasses.replace(tenant, version=version)


class TenantStore(ABC):
    """Abstract base class for tenant storage backends."""

    @abstractmethod
    async def get_tenant(self, tenant_id: str) -> Optional[Tenant]:
        """Get tenant by ID.

        Args:
            tenant_id: Unique tenant identifier

        Returns:
            Tenant object if found, None otherwise
        """
        pass

    @abstractmethod
    async def list_tenants(
        self,
        skip: int = 0,
        limit: int = 100,
        active_only: bool = True,
    ) -> List[Tenant]:
        """List tenants with pagination and filtering.

        Args:
            skip: Number of tenants to skip
            limit: Maximum number of tenants to return
            active_only: Only return active tenants

        Returns:
            List of tenant objects
        """
        pass

    @abstractmethod
    async def create_tenant(self, tenant: Tenant) -> Tenant:
        """Create a new tenant.

        Args:
            tenant: Tenant object to create

        Returns:
            Created tenant object
        """
        pass

    @abstractmethod
    async def update_tenant(self, tenant: Tenant) -> Tenant:
        """Update an existing tenant.

        Args:
            tenant: Tenant object with updated fields

        Returns:
            Updated tenant object
        """
        pass

    @abstractmethod
    async def delete_tenant(self, tenant_id: str) -> bool:
        """Delete a tenant.

        Args:
            tenant_id: ID of tenant to delete

        Returns:
            True if deleted, False if not found
        """
        pass

    @abstractmethod
    async def tenant_exists(self, tenant_id: str) -> bool:
        """Check if tenant exists.

        Args:
            tenant_id: ID of tenant to check

        Returns:
            True if tenant exists, False otherwise
        """
        pass


class InMemoryTenantStore(TenantStore):
    """In-memory tenant storage for development and testing.

    Note: This implementation is not suitable for production use as it
    doesn't persist data across restarts and doesn't support multi-instance
    deployments.
    """

    def __init__(self):
        """Initialize in-memory storage."""
        self._tenants: dict[str, Tenant] = {}

    async def get_tenant(self, tenant_id: str) -> Optional[Tenant]:
        """Get tenant from in-memory storage."""
        return self._tenants.get(tenant_id)

    async def list_tenants(
        self,
        skip: int = 0,
        limit: int = 100,
        active_only: bool = True,
    ) -> List[Tenant]:
        """List tenants from in-memory storage."""
        tenants = list(self._tenants.values())

        if active_only:
            tenants = [t for t in tenants if t.is_active]

        return tenants[skip:skip + limit]

    async def create_tenant(self, tenant: Tenant) -> Tenant:
        """Create tenant in in-memory storage."""
        if tenant.tenant_id in self._tenants:
            raise ValueError(f"Tenant {tenant.tenant_id} already exists")

        tenant.created_at = datetime.utcnow()
        tenant.updated_at = datetime.utcnow()

        self._tenants[tenant.tenant_id] = tenant
        logger.info(f"Created tenant: {tenant.tenant_id}")
        return tenant

    async def update_tenant(self, tenant: Tenant) -> Tenant:
        """Update tenant in in-memory storage."""
        if tenant.tenant_id not in self._tenants:
            raise ValueError(f"Tenant {tenant.tenant_id} not found")

        tenant.updated_at = datetime.utcnow()
        self._tenants[tenant.tenant_id] = tenant
        logger.info(f"Updated tenant: {tenant.tenant_id}")
        return tenant

    async def delete_tenant(self, tenant_id: str) -> bool:
        """Delete tenant from in-memory storage."""
        if tenant_id in self._tenants:
            del self._tenants[tenant_id]
            logger.info(f"Deleted tenant: {tenant_id}")
            return True
        return False

    async def tenant_exists(self, tenant_id: str) -> bool:
        """Check if tenant exists in in-memory storage."""
        return tenant_id in self._tenants


class RedisTenantStore(TenantStore):
    """Redis-based tenant storage for production distributed deployments.

    This implementation provides:
    - Persistence across restarts
    - Support for multi-instance deployments
    - Fast read/write operations
    - TTL support for automatic cleanup

    Note: Requires Redis connection configuration.
    """

    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        """Initialize Redis tenant storage.

        Args:
            redis_url: Redis connection URL
        """
        try:
            import redis.asyncio as redis
        except ImportError:
            raise ImportError("Redis requires 'redis' package. "
                              "Install it with: pip install redis")

        self._redis_url = redis_url
        self._redis: Optional[redis.Redis] = None

    async def _get_redis(self):
        """Lazy initialization of Redis connection."""
        if self._redis is None:
            import redis.asyncio as redis

            self._redis = await redis.from_url(self._redis_url, decode_responses=True)
        return self._redis

    def _tenant_key(self, tenant_id: str) -> str:
        """Generate Redis key for tenant storage."""
        return f"tenant:{tenant_id}"

    async def get_tenant(self, tenant_id: str) -> Optional[Tenant]:
        """Get tenant from Redis storage."""
        redis_client = await self._get_redis()

        key = self._tenant_key(tenant_id)
        data = await redis_client.get(key)

        if not data:
            return None

        try:
            config = TenantConfig.parse_raw(data)
            return config.to_tenant()
        except Exception as e:
            logger.error(f"Failed to parse tenant config: {e}")
            return None

    async def list_tenants(
        self,
        skip: int = 0,
        limit: int = 100,
        active_only: bool = True,
    ) -> List[Tenant]:
        """List tenants from Redis storage."""
        redis_client = await self._get_redis()

        pattern = "tenant:*"
        keys = []
        async for key in redis_client.scan_iter(match=pattern):
            keys.append(key)

        tenants = []
        for key in keys[skip:skip + limit]:
            data = await redis_client.get(key)
            if data:
                try:
                    config = TenantConfig.parse_raw(data)
                    tenant = config.to_tenant()
                    if not active_only or tenant.is_active:
                        tenants.append(tenant)
                except Exception as e:
                    logger.error(f"Failed to parse tenant at {key}: {e}")

        return tenants

    async def create_tenant(self, tenant: Tenant) -> Tenant:
        """Create tenant in Redis storage."""
        redis_client = await self._get_redis()

        key = self._tenant_key(tenant.tenant_id)
        if await redis_client.exists(key):
            raise ValueError(f"Tenant {tenant.tenant_id} already exists")

        tenant.created_at = datetime.utcnow()
        tenant.updated_at = datetime.utcnow()

        config = TenantConfig.from_tenant(tenant)
        await redis_client.set(key, config.json())

        logger.info(f"Created tenant in Redis: {tenant.tenant_id}")
        return tenant

    async def update_tenant(self, tenant: Tenant) -> Tenant:
        """Update tenant in Redis storage."""
        redis_client = await self._get_redis()

        key = self._tenant_key(tenant.tenant_id)
        if not await redis_client.exists(key):
            raise ValueError(f"Tenant {tenant.tenant_id} not found")

        tenant.updated_at = datetime.utcnow()
        config = TenantConfig.from_tenant(tenant)
        await redis_client.set(key, config.json())

        logger.info(f"Updated tenant in Redis: {tenant.tenant_id}")
        return tenant

    async def delete_tenant(self, tenant_id: str) -> bool:
        """Delete tenant from Redis storage."""
        redis_client = await self._get_redis()

        key = self._tenant_key(tenant_id)
        result = await redis_client.delete(key)

        if result:
            logger.info(f"Deleted tenant from Redis: {tenant_id}")
            return True
        return False

    async def tenant_exists(self, tenant_id: str) -> bool:
        """Check if tenant exists in Redis storage."""
        redis_client = await self._get_redis()

        key = self._tenant_key(tenant_id)
        return await redis_client.exists(key) > 0

    async def close(self):
        """Close Redis connection."""
        if self._redis:
            await self._redis.close()


class SqlTenantStore(TenantStore):
    """SQL-based tenant storage for production deployments with persistence.

    This implementation provides:
    - ACID transactions
    - Complex querying capabilities
    - Data persistence and backup
    - SQL database support (PostgreSQL, MySQL, SQLite)

    Note: Requires database table setup and connection configuration.
    """

    def __init__(self, database_url: str = "sqlite:///./tenants.db"):
        """Initialize SQL tenant storage.

        Args:
            database_url: Database connection URL
        """
        try:
            from sqlalchemy import (Column, Integer, String, Text, Boolean, DateTime, create_engine)  # noqa: F401
            from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession  # noqa: F401
            from sqlalchemy.orm import sessionmaker  # noqa: F401
            from sqlalchemy.ext.declarative import declarative_base
        except ImportError:
            raise ImportError("SQL storage requires 'sqlalchemy' package. "
                              "Install it with: pip install sqlalchemy[asyncio]")

        self._database_url = database_url
        self._Base = declarative_base()
        self._engine = None
        self._session_factory = None

        # Define table model
        class TenantRecord(self._Base):
            """SQL table model for tenant storage."""

            __tablename__ = "tenants"

            tenant_id = Column(String(64), primary_key=True)
            name = Column(String(256), nullable=False)
            description = Column(Text)
            config_data = Column(Text, nullable=False)  # JSON string of TenantConfig
            is_active = Column(Boolean, default=True)
            version = Column(Integer, nullable=False, default=0)  # optimistic lock
            created_at = Column(DateTime, default=datetime.utcnow)
            updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

        self._TenantRecord = TenantRecord

    async def _initialize(self):
        """Initialize database connection and create tables."""
        if self._engine is None:
            from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
            from sqlalchemy.orm import sessionmaker

            # Convert sqlite:// to sqlite+aiosqlite:// for async support
            db_url = self._database_url
            if db_url.startswith("sqlite://"):
                db_url = db_url.replace("sqlite://", "sqlite+aiosqlite://")

            self._engine = create_async_engine(db_url, echo=False)
            self._session_factory = sessionmaker(bind=self._engine, class_=AsyncSession, expire_on_commit=False)

            # Create tables
            async with self._engine.begin() as conn:
                await conn.run_sync(self._Base.metadata.create_all)

    async def _get_session(self):
        """Get database session."""
        await self._initialize()
        return self._session_factory()

    async def get_tenant(self, tenant_id: str) -> Optional[Tenant]:
        """Get tenant from SQL storage."""
        session = await self._get_session()

        try:
            from sqlalchemy import select

            result = await session.execute(select(self._TenantRecord).where(self._TenantRecord.tenant_id == tenant_id))
            record = result.scalar_one_or_none()

            if not record:
                return None

            config = TenantConfig.parse_raw(record.config_data)
            tenant = config.to_tenant()
            # The row's version column is authoritative for optimistic locking.
            tenant.version = record.version
            return tenant
        finally:
            await session.close()

    async def list_tenants(
        self,
        skip: int = 0,
        limit: int = 100,
        active_only: bool = True,
    ) -> List[Tenant]:
        """List tenants from SQL storage."""
        session = await self._get_session()

        try:
            from sqlalchemy import select

            query = select(self._TenantRecord)
            if active_only:
                query = query.where(self._TenantRecord.is_active.is_(True))

            query = query.offset(skip).limit(limit)
            query = query.order_by(self._TenantRecord.created_at.desc())

            result = await session.execute(query)
            records = result.scalars().all()

            tenants = []
            for record in records:
                try:
                    config = TenantConfig.parse_raw(record.config_data)
                    tenants.append(config.to_tenant())
                except Exception as e:
                    logger.error(f"Failed to parse tenant {record.tenant_id}: {e}")

            return tenants
        finally:
            await session.close()

    async def create_tenant(self, tenant: Tenant) -> Tenant:
        """Create tenant in SQL storage."""
        session = await self._get_session()

        try:
            from sqlalchemy import select

            # Check if tenant already exists
            existing = await session.execute(
                select(self._TenantRecord).where(self._TenantRecord.tenant_id == tenant.tenant_id))
            if existing.scalar_one_or_none():
                raise ValueError(f"Tenant {tenant.tenant_id} already exists")

            # Create new tenant record
            tenant.created_at = datetime.utcnow()
            tenant.updated_at = datetime.utcnow()
            tenant.version = 0

            config = TenantConfig.from_tenant(tenant)
            record = self._TenantRecord(
                tenant_id=tenant.tenant_id,
                name=tenant.name,
                description=tenant.description,
                config_data=config.json(),
                is_active=tenant.is_active,
                version=tenant.version,
                created_at=tenant.created_at,
                updated_at=tenant.updated_at,
            )

            session.add(record)
            await session.commit()
            await session.refresh(record)

            logger.info(f"Created tenant in SQL: {tenant.tenant_id}")
            return tenant
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

    async def update_tenant(self, tenant: Tenant) -> Tenant:
        """Update tenant in SQL storage with optimistic locking.

        The update applies only when the caller's ``tenant.version`` matches
        the stored row version; otherwise :class:`OptimisticLockError` is
        raised so the caller can re-read and retry.

        Args:
            tenant: Tenant object with updated fields (``version`` must come
                from a recent read).

        Returns:
            Updated tenant object with the new version.

        Raises:
            ValueError: Tenant does not exist.
            OptimisticLockError: A concurrent writer updated the row first.
        """
        session = await self._get_session()

        try:
            from sqlalchemy import select, update

            # Check if tenant exists
            existing = await session.execute(
                select(self._TenantRecord.version).where(self._TenantRecord.tenant_id == tenant.tenant_id))
            stored_version = existing.scalar_one_or_none()
            if stored_version is None:
                raise ValueError(f"Tenant {tenant.tenant_id} not found")

            # Optimistic concurrent update: only succeeds when the stored
            # version still matches what the caller read. The persisted
            # config JSON carries the *new* version so reads stay consistent.
            expected_version = tenant.version
            tenant.updated_at = datetime.utcnow()
            updated_snapshot = TenantConfig.from_tenant(_tenant_with_version(tenant, expected_version + 1))

            new_values = {
                "name": tenant.name,
                "description": tenant.description,
                "config_data": updated_snapshot.json(),
                "is_active": tenant.is_active,
                "version": self._TenantRecord.version + 1,
                "updated_at": tenant.updated_at,
            }
            stmt = update(self._TenantRecord).where(self._TenantRecord.tenant_id == tenant.tenant_id,
                                                    self._TenantRecord.version == expected_version).values(**new_values)
            result = await session.execute(stmt)
            if result.rowcount == 0:
                await session.rollback()
                raise OptimisticLockError(f"Tenant {tenant.tenant_id} was modified concurrently "
                                          f"(expected version {expected_version}, stored version {stored_version})")
            await session.commit()

            tenant.version = int(stored_version) + 1
            logger.info(f"Updated tenant in SQL: {tenant.tenant_id} (v{tenant.version})")
            return tenant
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

    async def delete_tenant(self, tenant_id: str) -> bool:
        """Delete tenant from SQL storage."""
        session = await self._get_session()

        try:
            from sqlalchemy import select, delete

            # Check if tenant exists
            existing = await session.execute(
                select(self._TenantRecord).where(self._TenantRecord.tenant_id == tenant_id))
            if not existing.scalar_one_or_none():
                return False

            # Delete tenant record
            await session.execute(delete(self._TenantRecord).where(self._TenantRecord.tenant_id == tenant_id))
            await session.commit()

            logger.info(f"Deleted tenant from SQL: {tenant_id}")
            return True
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

    async def tenant_exists(self, tenant_id: str) -> bool:
        """Check if tenant exists in SQL storage."""
        session = await self._get_session()

        try:
            from sqlalchemy import select

            result = await session.execute(
                select(self._TenantRecord.tenant_id).where(self._TenantRecord.tenant_id == tenant_id))
            return result.scalar_one_or_none() is not None
        finally:
            await session.close()

    async def close(self):
        """Close database connection."""
        if self._engine:
            await self._engine.dispose()
