# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Cross-backend tenant data migration tooling.

Implements the "dual-write → catch-up → cutover → read-back verify" strategy
described in MULTI_TENANT_CONSISTENCY.md:

1. :func:`migrate_tenants` copies tenant configurations from a source
   :class:`TenantStore` to a target one (pagination + idempotent upserts).
2. :func:`migrate_vectors` moves tenant-scoped :class:`VectorRecord` batches
   between :class:`VectorBackend` implementations.
3. :func:`verify_tenant_migration` performs the read-back check (cutover gate).

All functions are offline/batch tools: run them during a migration window and
stop writes to the source (or rely on the dual-write phase) before cutover.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Sequence

from trpc_agent_sdk.log import logger
from trpc_agent_sdk.tenants._tenant_store import TenantStore
from trpc_agent_sdk.tenants._tenant_vector import VectorBackend


@dataclass
class MigrationReport:
    """Outcome summary of a migration run."""

    migrated: int = 0
    """Records copied to the target backend."""
    skipped: int = 0
    """Records already present (idempotent re-run) and not overwritten."""
    failed: int = 0
    """Records that could not be migrated."""
    errors: List[str] = field(default_factory=list)
    """Human-readable error for each failed record."""

    @property
    def ok(self) -> bool:
        """True when nothing failed."""
        return self.failed == 0

    def summary(self) -> str:
        return f"migrated={self.migrated} skipped={self.skipped} failed={self.failed}"


async def migrate_tenants(source: TenantStore,
                          target: TenantStore,
                          page_size: int = 100,
                          overwrite: bool = False) -> MigrationReport:
    """Copy all tenant configurations from ``source`` to ``target``.

    Idempotent: tenants already present on the target are skipped unless
    ``overwrite`` is set, so a crashed run can simply be re-executed.

    Args:
        source: Store to read tenants from (e.g. Redis).
        target: Store to write tenants to (e.g. SQL).
        page_size: Pagination size when listing source tenants.
        overwrite: Replace existing target tenants instead of skipping.

    Returns:
        A :class:`MigrationReport`; individual failures do not abort the run.
    """
    report = MigrationReport()
    skip = 0

    while True:
        batch = await source.list_tenants(skip=skip, limit=page_size, active_only=False)
        if not batch:
            break
        skip += len(batch)

        for tenant in batch:
            try:
                exists = await target.tenant_exists(tenant.tenant_id)
                if exists and not overwrite:
                    report.skipped += 1
                    continue
                if exists and overwrite:
                    await target.delete_tenant(tenant.tenant_id)
                await target.create_tenant(tenant)
                report.migrated += 1
            except Exception as e:  # noqa: BLE001 — one bad tenant must not abort the batch
                report.failed += 1
                report.errors.append(f"tenant {tenant.tenant_id}: {type(e).__name__}: {e}")
                logger.error(f"Migration failed for tenant {tenant.tenant_id}: {e}")

    logger.info(f"Tenant migration done: {report.summary()}")
    return report


async def migrate_vectors(source: VectorBackend,
                          target: VectorBackend,
                          tenant_ids: Sequence[str],
                          batch_size: int = 500) -> MigrationReport:
    """Move tenant-scoped vector records between backends.

    ``VectorBackend`` has no "list tenants" operation, so the tenant ids must
    be supplied (typically the same list fed to :func:`migrate_tenants`).
    Records are re-upserted on the target with their original ids, making the
    operation idempotent.

    Args:
        source: Backend to read records from.
        target: Backend to write records to.
        tenant_ids: Tenants to migrate.
        batch_size: Number of records per upsert batch.

    Returns:
        A :class:`MigrationReport` covering all tenants.
    """
    report = MigrationReport()

    for tenant_id in tenant_ids:
        try:
            records = source.list_records(tenant_id)
            for start in range(0, len(records), batch_size):
                batch = records[start:start + batch_size]
                target.upsert(tenant_id, batch)
                report.migrated += len(batch)

            # Read-back check per tenant: counts must match after migration.
            source_count = source.count(tenant_id)
            target_count = target.count(tenant_id)
            if target_count < source_count:
                report.failed += 1
                report.errors.append(f"vector tenant {tenant_id}: count mismatch "
                                     f"source={source_count} target={target_count}")
        except Exception as e:  # noqa: BLE001 — one bad tenant must not abort the run
            report.failed += 1
            report.errors.append(f"vector tenant {tenant_id}: {type(e).__name__}: {e}")
            logger.error(f"Vector migration failed for tenant {tenant_id}: {e}")

    logger.info(f"Vector migration done: {report.summary()}")
    return report


async def verify_tenant_migration(source: TenantStore,
                                  target: TenantStore,
                                  tenant_ids: Optional[Sequence[str]] = None) -> MigrationReport:
    """Read-back verification gate before cutover.

    For each tenant (all source tenants when ``tenant_ids`` is None) both
    stores are read and the essential configuration fields are compared.

    Returns:
        A :class:`MigrationReport` where ``failed`` counts mismatches and
        ``migrated`` counts verified tenants.
    """
    report = MigrationReport()

    if tenant_ids is None:
        tenants = await source.list_tenants(skip=0, limit=10000, active_only=False)
        tenant_ids = [t.tenant_id for t in tenants]

    for tenant_id in tenant_ids:
        source_tenant = await source.get_tenant(tenant_id)
        target_tenant = await target.get_tenant(tenant_id)
        if source_tenant is None:
            continue
        if target_tenant is None:
            report.failed += 1
            report.errors.append(f"tenant {tenant_id}: missing on target")
            continue

        same = (source_tenant.name == target_tenant.name
                and source_tenant.config_version == target_tenant.config_version
                and source_tenant.tool_permissions == target_tenant.tool_permissions)
        if same:
            report.migrated += 1
        else:
            report.failed += 1
            report.errors.append(f"tenant {tenant_id}: configuration mismatch")

    return report
