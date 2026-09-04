# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Authoritative DDL for the multi-tenant deployment schema.

Eight tables cover every data category the tenant platform owns:

==============  =========================================================
Table           Purpose
==============  =========================================================
tenants         Tenant registry (config JSON + optimistic-lock version)
sessions        Tenant-scoped agent sessions (state + version)
events          Session event stream (append-only)
messages        Normalized inbound/outbound IM messages
memory          Long-term memory entries (optional embedding blob)
summaries       Session summaries produced by the summarizer
channel_bindings  IM identity → tenant session bindings (routing)
audit_logs      Governance audit trail (11 required fields + trace_id)
==============  =========================================================

Two dialect variants are provided:

- :data:`TENANT_TABLES_DDL_SQLITE` — runnable in tests / single-node dev.
- :data:`TENANT_TABLES_DDL_MYSQL` — production MySQL 8 / TiDB variant
  (utf8mb4, InnoDB, AUTO_INCREMENT). For PostgreSQL swap AUTO_INCREMENT
  columns for ``BIGSERIAL``; the rest applies as-is.

Every tenant-owned table carries a ``tenant_id`` column (hard isolation on
the storage key). Tables that are read-modify-write (``tenants``,
``sessions``, ``summaries``) carry a ``version`` column used for optimistic
concurrency control — see :class:`~trpc_agent_sdk.tenants.OptimisticLockError`.

The statements are executed by the versioned migration runner in
:mod:`trpc_agent_sdk.tenants._sql_migrations`; do not run them by hand in
production.
"""

from typing import List

# Internal bookkeeping table for the migration runner itself.
SCHEMA_MIGRATIONS_DDL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    name VARCHAR(256) NOT NULL,
    checksum VARCHAR(64) NOT NULL,
    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""

TENANT_TABLES_DDL_SQLITE: List[str] = [
    # 1. Tenant registry -------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS tenants (
        tenant_id     VARCHAR(64)  PRIMARY KEY,
        name          VARCHAR(256) NOT NULL,
        description   TEXT,
        config_data   TEXT         NOT NULL,  -- serialized TenantConfig JSON
        is_active     BOOLEAN      NOT NULL DEFAULT 1,
        version       INTEGER      NOT NULL DEFAULT 0,  -- optimistic lock
        created_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    # 2. Sessions (tenant-scoped storage keys) ---------------------------------
    """
    CREATE TABLE IF NOT EXISTS sessions (
        tenant_id     VARCHAR(64)  NOT NULL,
        session_id    VARCHAR(256) NOT NULL,
        app_name      VARCHAR(256) NOT NULL,
        user_id       VARCHAR(256) NOT NULL,
        state_json    TEXT         NOT NULL DEFAULT '{}',
        version       INTEGER      NOT NULL DEFAULT 0,  -- optimistic lock
        created_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (tenant_id, session_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_sessions_tenant_user
        ON sessions (tenant_id, user_id)
    """,
    # 3. Events (append-only stream per session) -------------------------------
    """
    CREATE TABLE IF NOT EXISTS events (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        tenant_id     VARCHAR(64)  NOT NULL,
        session_id    VARCHAR(256) NOT NULL,
        event_id      VARCHAR(128) NOT NULL,
        author        VARCHAR(256),
        event_json    TEXT         NOT NULL,
        created_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (tenant_id, session_id, event_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_events_tenant_session
        ON events (tenant_id, session_id, created_at)
    """,
    # 4. Messages (normalized IM messages) --------------------------------------
    """
    CREATE TABLE IF NOT EXISTS messages (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        tenant_id     VARCHAR(64)  NOT NULL,
        session_id    VARCHAR(256) NOT NULL,
        message_id    VARCHAR(128) NOT NULL,
        channel_type  VARCHAR(64)  NOT NULL,
        direction     VARCHAR(16)  NOT NULL DEFAULT 'inbound',
        user_id       VARCHAR(256),
        chat_id       VARCHAR(256),
        content       TEXT         NOT NULL,
        metadata_json TEXT,
        created_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (tenant_id, message_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_messages_tenant_session
        ON messages (tenant_id, session_id, created_at)
    """,
    # 5. Memory (long-term memory entries, embedding optional) ------------------
    """
    CREATE TABLE IF NOT EXISTS memory (
        tenant_id     VARCHAR(64)  NOT NULL,
        user_id       VARCHAR(256) NOT NULL,
        memory_id     VARCHAR(128) NOT NULL,
        content_json  TEXT         NOT NULL,
        embedding     BLOB,           -- float32 vector, optional
        version       INTEGER      NOT NULL DEFAULT 0,
        created_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (tenant_id, user_id, memory_id)
    )
    """,
    # 6. Summaries (per-session rolling summaries) ------------------------------
    """
    CREATE TABLE IF NOT EXISTS summaries (
        tenant_id     VARCHAR(64)  NOT NULL,
        session_id    VARCHAR(256) NOT NULL,
        summary       TEXT         NOT NULL,
        version       INTEGER      NOT NULL DEFAULT 0,  -- optimistic lock
        created_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (tenant_id, session_id)
    )
    """,
    # 7. Channel bindings (IM identity → tenant session) ------------------------
    """
    CREATE TABLE IF NOT EXISTS channel_bindings (
        tenant_id          VARCHAR(64)  NOT NULL,
        channel_type       VARCHAR(64)  NOT NULL,
        external_id        VARCHAR(256) NOT NULL,  -- platform user/chat id
        webhook_token_hash VARCHAR(64),         -- sha256 of the webhook token
        session_id         VARCHAR(256) NOT NULL,  -- bound tenant session
        attributes_json    TEXT,
        created_at         TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (tenant_id, channel_type, external_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_channel_bindings_token
        ON channel_bindings (webhook_token_hash)
    """,
    # 8. Audit logs (governance trail) ------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS audit_logs (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        tenant_id     VARCHAR(64)  NOT NULL,
        channel       VARCHAR(64)  NOT NULL DEFAULT 'api',
        user_id       VARCHAR(256) NOT NULL DEFAULT '',
        session_id    VARCHAR(256) NOT NULL DEFAULT '',
        agent_name    VARCHAR(256) NOT NULL DEFAULT '',
        tool_name     VARCHAR(256) NOT NULL DEFAULT '',
        decision      VARCHAR(64)  NOT NULL,
        latency_ms    REAL         NOT NULL DEFAULT 0,
        error_type    VARCHAR(128) NOT NULL DEFAULT '',
        cost_usd      REAL         NOT NULL DEFAULT 0,
        trace_id      VARCHAR(64)  NOT NULL DEFAULT '',
        details_json  TEXT,
        created_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_audit_logs_tenant_time
        ON audit_logs (tenant_id, created_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_audit_logs_trace
        ON audit_logs (trace_id)
    """,
]

# MySQL 8 / TiDB variant. Column definitions match the SQLite variant; only
# the engine-specific bits differ (AUTO_INCREMENT, ENGINE=InnoDB, utf8mb4).
TENANT_TABLES_DDL_MYSQL: List[str] = [
    """
    CREATE TABLE IF NOT EXISTS tenants (
        tenant_id     VARCHAR(64)  PRIMARY KEY,
        name          VARCHAR(256) NOT NULL,
        description   TEXT,
        config_data   JSON         NOT NULL,
        is_active     BOOLEAN      NOT NULL DEFAULT TRUE,
        version       INT          NOT NULL DEFAULT 0,
        created_at    TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at    TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                           ON UPDATE CURRENT_TIMESTAMP(6)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS sessions (
        tenant_id     VARCHAR(64)  NOT NULL,
        session_id    VARCHAR(256) NOT NULL,
        app_name      VARCHAR(256) NOT NULL,
        user_id       VARCHAR(256) NOT NULL,
        state_json    JSON         NOT NULL,
        version       INT          NOT NULL DEFAULT 0,
        created_at    TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at    TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                           ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (tenant_id, session_id),
        KEY idx_sessions_tenant_user (tenant_id, user_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS events (
        id            BIGINT       NOT NULL AUTO_INCREMENT,
        tenant_id     VARCHAR(64)  NOT NULL,
        session_id    VARCHAR(256) NOT NULL,
        event_id      VARCHAR(128) NOT NULL,
        author        VARCHAR(256),
        event_json    JSON         NOT NULL,
        created_at    TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        PRIMARY KEY (id),
        UNIQUE KEY uq_events_identity (tenant_id, session_id, event_id),
        KEY idx_events_tenant_session (tenant_id, session_id, created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS messages (
        id            BIGINT       NOT NULL AUTO_INCREMENT,
        tenant_id     VARCHAR(64)  NOT NULL,
        session_id    VARCHAR(256) NOT NULL,
        message_id    VARCHAR(128) NOT NULL,
        channel_type  VARCHAR(64)  NOT NULL,
        direction     VARCHAR(16)  NOT NULL DEFAULT 'inbound',
        user_id       VARCHAR(256),
        chat_id       VARCHAR(256),
        content       TEXT         NOT NULL,
        metadata_json JSON,
        created_at    TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        PRIMARY KEY (id),
        UNIQUE KEY uq_messages_identity (tenant_id, message_id),
        KEY idx_messages_tenant_session (tenant_id, session_id, created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS memory (
        tenant_id     VARCHAR(64)  NOT NULL,
        user_id       VARCHAR(256) NOT NULL,
        memory_id     VARCHAR(128) NOT NULL,
        content_json  JSON         NOT NULL,
        embedding     BLOB,
        version       INT          NOT NULL DEFAULT 0,
        created_at    TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at    TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                           ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (tenant_id, user_id, memory_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS summaries (
        tenant_id     VARCHAR(64)  NOT NULL,
        session_id    VARCHAR(256) NOT NULL,
        summary       TEXT         NOT NULL,
        version       INT          NOT NULL DEFAULT 0,
        created_at    TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at    TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                           ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (tenant_id, session_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS channel_bindings (
        tenant_id          VARCHAR(64)  NOT NULL,
        channel_type       VARCHAR(64)  NOT NULL,
        external_id        VARCHAR(256) NOT NULL,
        webhook_token_hash VARCHAR(64),
        session_id         VARCHAR(256) NOT NULL,
        attributes_json    JSON,
        created_at         TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        PRIMARY KEY (tenant_id, channel_type, external_id),
        KEY idx_channel_bindings_token (webhook_token_hash)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS audit_logs (
        id            BIGINT       NOT NULL AUTO_INCREMENT,
        tenant_id     VARCHAR(64)  NOT NULL,
        channel       VARCHAR(64)  NOT NULL DEFAULT 'api',
        user_id       VARCHAR(256) NOT NULL DEFAULT '',
        session_id    VARCHAR(256) NOT NULL DEFAULT '',
        agent_name    VARCHAR(256) NOT NULL DEFAULT '',
        tool_name     VARCHAR(256) NOT NULL DEFAULT '',
        decision      VARCHAR(64)  NOT NULL,
        latency_ms    DOUBLE       NOT NULL DEFAULT 0,
        error_type    VARCHAR(128) NOT NULL DEFAULT '',
        cost_usd      DOUBLE       NOT NULL DEFAULT 0,
        trace_id      VARCHAR(64)  NOT NULL DEFAULT '',
        details_json  JSON,
        created_at    TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        PRIMARY KEY (id),
        KEY idx_audit_logs_tenant_time (tenant_id, created_at),
        KEY idx_audit_logs_trace (trace_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
]

# The eight tenant-owned table names, in creation order.
TENANT_TABLE_NAMES = (
    "tenants",
    "sessions",
    "events",
    "messages",
    "memory",
    "summaries",
    "channel_bindings",
    "audit_logs",
)


def apply_ddl(conn, statements: List[str]) -> None:
    """Execute a list of DDL statements on an open (sync) DB-API connection.

    Args:
        conn: DB-API connection (``sqlite3`` or a SQLAlchemy sync connection).
        statements: SQL statements to execute in order.
    """
    cursor = conn.cursor()
    try:
        for statement in statements:
            cursor.execute(statement)
        conn.commit()
    finally:
        cursor.close()
