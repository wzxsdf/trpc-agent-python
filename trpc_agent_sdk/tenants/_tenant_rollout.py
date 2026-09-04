# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Tenant configuration canary rollout and rollback.

Tenant config changes (model params, tool permissions, resilience policies)
should reach production gradually: publish a new ``config_version`` to a
small percentage of users, watch tenant metrics, then promote to 100% or
roll back — per tenant.

Design:

- :class:`ConfigRollout` records, per tenant, a *target* version, a canary
  percentage and the version users were on before the rollout (``baseline``).
- User → version routing is a deterministic hash of ``(tenant_id, user_id)``:
  the same user always lands in the same bucket, so a conversation never sees
  its config flip mid-flight and every node computes the same answer (no
  sticky session needed).
- :class:`InMemoryConfigHistory` keeps the last N published versions so
  :meth:`ConfigRolloutManager.rollback` restores the previous version without
  re-reading an external store.

The manager is pure in-process state; a production deployment would persist
:attr:`snapshot` / reload :meth:`InMemoryConfigHistory.snapshot` into Redis,
which the interface deliberately supports (dicts in, dicts out).
"""

import hashlib
from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class RolloutState:
    """Canary rollout state for one tenant.

    Attributes:
        tenant_id: Tenant this rollout belongs to.
        target_version: The new ``config_version`` being rolled out.
        baseline_version: The version serving before the rollout (rollback
            target); 0 when no previous version is known.
        canary_percent: Percentage of users (0-100) routed to the target.
        published: Whether the target is fully promoted (canary 100).
    """

    tenant_id: str
    target_version: int
    baseline_version: int = 0
    canary_percent: int = 0
    published: bool = False


@dataclass
class ConfigHistoryEntry:
    """One published config version for a tenant."""

    tenant_id: str
    version: int
    canary_percent: int
    published: bool


class InMemoryConfigHistory:
    """Bounded per-tenant history of published config versions."""

    def __init__(self, max_entries_per_tenant: int = 10):
        if max_entries_per_tenant < 1:
            raise ValueError("max_entries_per_tenant must be >= 1")
        self._max_entries = max_entries_per_tenant
        self._entries: Dict[str, List[ConfigHistoryEntry]] = {}

    def record(self, entry: ConfigHistoryEntry) -> None:
        """Append a history entry, trimming to the per-tenant bound."""
        entries = self._entries.setdefault(entry.tenant_id, [])
        entries.append(entry)
        if len(entries) > self._max_entries:
            del entries[:len(entries) - self._max_entries]

    def previous_version(self, tenant_id: str, before_version: int) -> int:
        """Return the last published version strictly before ``before_version``.

        Returns 0 when no earlier version is on record.
        """
        previous = 0
        for entry in self._entries.get(tenant_id, []):
            if entry.version < before_version and entry.published:
                previous = entry.version
        return previous

    def snapshot(self) -> Dict[str, List[ConfigHistoryEntry]]:
        """Return a copy of the history (for persistence / inspection)."""
        return {tenant_id: list(entries) for tenant_id, entries in self._entries.items()}


def user_bucket(tenant_id: str, user_id: str) -> int:
    """Deterministic 0-99 bucket for a (tenant, user) pair.

    sha256 of ``{tenant_id}:{user_id}``; stable across processes and nodes so
    routing needs no sticky sessions and never flaps for a given user.
    """
    digest = hashlib.sha256(f"{tenant_id}:{user_id}".encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % 100


class ConfigRolloutManager:
    """Publishes tenant config versions with canary percentage and rollback."""

    def __init__(self, history: Optional[InMemoryConfigHistory] = None):
        self._history = history or InMemoryConfigHistory()
        self._rollouts: Dict[str, RolloutState] = {}

    @property
    def history(self) -> InMemoryConfigHistory:
        """The config history backing rollback decisions."""
        return self._history

    def publish(
        self,
        tenant_id: str,
        target_version: int,
        canary_percent: int = 0,
        baseline_version: Optional[int] = None,
    ) -> RolloutState:
        """Start (or adjust) a canary rollout for a tenant.

        Args:
            tenant_id: Tenant receiving the new config version.
            target_version: New ``config_version`` to roll out (must be
                greater than the currently published baseline).
            canary_percent: Initial percentage of users on the target (0-100).
            baseline_version: Version to roll back to; defaults to the last
                published version from history.

        Returns:
            The resulting :class:`RolloutState`.
        """
        if not 0 <= canary_percent <= 100:
            raise ValueError("canary_percent must be within [0, 100]")
        if baseline_version is None:
            baseline_version = self._history.previous_version(tenant_id, target_version)
        if target_version <= baseline_version:
            raise ValueError(f"target_version {target_version} must be greater than "
                             f"baseline_version {baseline_version}")
        state = RolloutState(
            tenant_id=tenant_id,
            target_version=target_version,
            baseline_version=baseline_version,
            canary_percent=canary_percent,
        )
        self._rollouts[tenant_id] = state
        return state

    def set_canary_percent(self, tenant_id: str, canary_percent: int) -> RolloutState:
        """Adjust the canary percentage of an in-flight rollout (bake stage)."""
        if not 0 <= canary_percent <= 100:
            raise ValueError("canary_percent must be within [0, 100]")
        state = self._require_rollout(tenant_id)
        state.canary_percent = canary_percent
        return state

    def promote(self, tenant_id: str) -> RolloutState:
        """Promote the target version to 100% of users."""
        state = self._require_rollout(tenant_id)
        state.canary_percent = 100
        state.published = True
        self._history.record(
            ConfigHistoryEntry(
                tenant_id=tenant_id,
                version=state.target_version,
                canary_percent=100,
                published=True,
            ))
        return state

    def rollback(self, tenant_id: str) -> Optional[RolloutState]:
        """Roll a tenant back to its baseline version.

        Returns the resulting rollout state (target = baseline, 100% of
        users), or None when there is no rollout / no baseline to restore.
        """
        state = self._rollouts.get(tenant_id)
        if state is None or state.baseline_version <= 0:
            return None
        state.target_version, state.baseline_version = state.baseline_version, state.target_version
        state.canary_percent = 100
        state.published = True
        self._history.record(
            ConfigHistoryEntry(
                tenant_id=tenant_id,
                version=state.target_version,
                canary_percent=100,
                published=True,
            ))
        return state

    def resolve_config_version(self, tenant_id: str, user_id: str, default_version: int = 1) -> int:
        """Resolve the config version a user should be served.

        Deterministic: ``user_bucket(tenant_id, user_id) < canary_percent``
        routes to the target version, everyone else stays on the baseline.
        Without a rollout, ``default_version`` is returned.
        """
        state = self._rollouts.get(tenant_id)
        if state is None:
            return default_version
        if state.canary_percent >= 100:
            return state.target_version
        if user_bucket(tenant_id, user_id) < state.canary_percent:
            return state.target_version
        return state.baseline_version if state.baseline_version > 0 else default_version

    def rollout_state(self, tenant_id: str) -> Optional[RolloutState]:
        """Return the current rollout state for a tenant (None when idle)."""
        return self._rollouts.get(tenant_id)

    def snapshot(self) -> Dict[str, RolloutState]:
        """Return a copy of all rollout states (for persistence)."""
        return dict(self._rollouts)

    def _require_rollout(self, tenant_id: str) -> RolloutState:
        state = self._rollouts.get(tenant_id)
        if state is None:
            raise ValueError(f"No active rollout for tenant '{tenant_id}'; call publish() first")
        return state
