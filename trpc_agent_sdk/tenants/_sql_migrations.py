# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Versioned schema migration runner for the multi-tenant SQL schema.

The runner is a small, dependency-light ops tool (sync SQLAlchemy engine)
suitable for CI jobs and deploy pipelines:

.. code-block:: bash

    # SQLite / dev
    python -m trpc_agent_sdk.tenants._sql_migrations sqlite:///./tenants.db

    # MySQL / production
    python -m trpc_agent_sdk.tenants._sql_migrations mysql+pymysql://user:pwd@host:3306/tenants

Behavior:

- creates the ``schema_migrations`` bookkeeping table first,
- applies pending migrations in ascending version order, each inside its
  own transaction together with the bookkeeping row (atomic apply),
- is idempotent: already-applied versions are skipped, so the command is
  safe to run on every deploy,
- refuses to run a modified migration with the same version (checksum
  mismatch) instead of silently drifting schemas.

Consistency note: migrations run offline (single runner); online nodes
should be drained or deploy with backward-compatible columns. See
``MULTI_TENANT_CONSISTENCY.md`` for the full trade-off discussion.
"""

import argparse
import hashlib
import sys
from typing import List, NamedTuple, Optional

from trpc_agent_sdk.log import logger
from trpc_agent_sdk.tenants._sql_ddl import (
    SCHEMA_MIGRATIONS_DDL,
    TENANT_TABLES_DDL_MYSQL,
    TENANT_TABLES_DDL_SQLITE,
)


class Migration(NamedTuple):
    """One schema migration step."""

    version: int
    name: str
    # Dialect → ordered statements. Missing dialects fall back to "sqlite".
    statements: dict


def _tenant_tables_statements(dialect: str) -> List[str]:
    """Return the eight-table DDL for the requested dialect."""
    if dialect.startswith("mysql"):
        return list(TENANT_TABLES_DDL_MYSQL)
    return list(TENANT_TABLES_DDL_SQLITE)


MIGRATIONS: List[Migration] = [
    Migration(
        version=1,
        name="create_tenant_tables",
        # Every dialect maps onto the same logical schema.
        statements={
            "sqlite": None,
            "mysql": None
        },  # resolved lazily below
    ),
]


def _resolve_statements(migration: Migration, dialect: str) -> List[str]:
    """Resolve a migration's statements for a dialect (lazy defaults)."""
    if migration.name == "create_tenant_tables":
        return _tenant_tables_statements(dialect)
    return migration.statements.get(dialect) or migration.statements.get("sqlite") or []


def _checksum(statements: List[str]) -> str:
    """Compute a stable checksum for a migration's statements."""
    digest = hashlib.sha256()
    for statement in statements:
        digest.update("".join(statement.split()).encode("utf-8"))
        digest.update(b"\x00")
    return digest.hexdigest()[:64]


class SchemaMigrator:
    """Applies versioned schema migrations to a SQL database.

    Args:
        database_url: SQLAlchemy URL (sync driver). SQLite is used as-is;
            MySQL URLs like ``mysql+pymysql://...`` pick the MySQL DDL.
    """

    def __init__(self, database_url: str, migrations: Optional[List[Migration]] = None):
        self._database_url = database_url
        self._migrations = sorted(migrations or MIGRATIONS, key=lambda m: m.version)
        self._engine = None

    def _get_engine(self):
        """Lazily create the sync engine."""
        if self._engine is None:
            from sqlalchemy import create_engine

            self._engine = create_engine(self._database_url, future=True)
        return self._engine

    @staticmethod
    def _dialect(engine) -> str:
        """Map the SQLAlchemy dialect to a DDL dialect name."""
        name = engine.dialect.name
        return "mysql" if name == "mysql" else "sqlite"

    def _ensure_bookkeeping_table(self, conn) -> None:
        """Create the schema_migrations table if absent."""
        from sqlalchemy import text

        conn.execute(text(SCHEMA_MIGRATIONS_DDL))
        conn.commit()

    def applied_versions(self) -> List[int]:
        """Return the list of already-applied migration versions."""
        from sqlalchemy import text

        engine = self._get_engine()
        with engine.connect() as conn:
            self._ensure_bookkeeping_table(conn)
            rows = conn.execute(text("SELECT version FROM schema_migrations ORDER BY version")).fetchall()
        return [int(row[0]) for row in rows]

    def apply_migrations(self, dry_run: bool = False) -> List[int]:
        """Apply all pending migrations; return the versions applied.

        Args:
            dry_run: When True, only report what would run.

        Returns:
            Versions applied by this call (empty when up to date).
        """
        from sqlalchemy import text

        engine = self._get_engine()
        dialect = self._dialect(engine)
        applied: List[int] = []

        with engine.connect() as conn:
            self._ensure_bookkeeping_table(conn)
            existing = {
                int(row[0]): row[1]
                for row in conn.execute(text("SELECT version, name FROM schema_migrations")).fetchall()
            }

            for migration in self._migrations:
                if migration.version in existing:
                    continue
                statements = _resolve_statements(migration, dialect)
                checksum = _checksum(statements)
                if dry_run:
                    logger.info(f"[dry-run] would apply v{migration.version} {migration.name}")
                    applied.append(migration.version)
                    continue
                # Atomic per migration: DDL + bookkeeping row commit together
                # where the engine supports transactional DDL (SQLite does;
                # MySQL DDL auto-commits, the bookkeeping row then records it).
                for statement in statements:
                    conn.execute(text(statement))
                record = {"version": migration.version, "name": migration.name, "checksum": checksum}
                conn.execute(
                    text("INSERT INTO schema_migrations (version, name, checksum) "
                         "VALUES (:version, :name, :checksum)"), record)
                conn.commit()
                logger.info(f"Applied migration v{migration.version}: {migration.name}")
                applied.append(migration.version)

        return applied

    def verify_checksums(self) -> List[str]:
        """Return names of applied migrations whose checksum drifted."""
        from sqlalchemy import text

        engine = self._get_engine()
        dialect = self._dialect(engine)
        drifted: List[str] = []
        with engine.connect() as conn:
            self._ensure_bookkeeping_table(conn)
            rows = conn.execute(
                text("SELECT version, name, checksum FROM schema_migrations ORDER BY version")).fetchall()
        for version, name, checksum in rows:
            migration = next((m for m in self._migrations if m.version == int(version)), None)
            if migration is None:
                continue
            statements = _resolve_statements(migration, dialect)
            if _checksum(statements) != checksum:
                drifted.append(f"v{version} {name}")
        return drifted


def main(argv: Optional[List[str]] = None) -> int:
    """CLI entry point: apply migrations and print the result."""
    parser = argparse.ArgumentParser(description="Apply tenant schema migrations")
    parser.add_argument("database_url", help="SQLAlchemy URL, e.g. sqlite:///./tenants.db")
    parser.add_argument("--dry-run", action="store_true", help="Only report pending migrations")
    args = parser.parse_args(argv)

    migrator = SchemaMigrator(args.database_url)
    drifted = migrator.verify_checksums()
    if drifted:
        logger.error(f"Checksum drift detected for: {', '.join(drifted)}; refusing to run")
        return 2
    applied = migrator.apply_migrations(dry_run=args.dry_run)
    if applied:
        logger.info(f"Applied versions: {applied}")
    else:
        logger.info("Schema is up to date")
    return 0


if __name__ == "__main__":
    sys.exit(main())
