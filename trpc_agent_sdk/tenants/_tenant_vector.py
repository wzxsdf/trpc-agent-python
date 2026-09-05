# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Tenant-scoped vector store abstraction.

Provides a minimal interface every vector backend must implement
(:class:`VectorBackend`) plus a dependency-free usable implementation
(:class:`InMemoryVectorBackend`, brute-force cosine similarity). Production
deployments plug pgvector / Milvus / Elasticsearch by registering a custom
:class:`VectorBackend` — the interface is deliberately small (upsert /
search / delete / count) and tenant-scoped by design: every record carries
a ``tenant_id`` and searches never cross tenants.
"""

import math
import threading
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence


@dataclass
class VectorRecord:
    """One vector entry in the store."""

    embedding: Sequence[float]
    """The embedding vector."""
    text: str = ""
    """Optional source text (returned with search hits)."""
    metadata: Dict[str, Any] = field(default_factory=dict)
    """Arbitrary filterable metadata."""
    id: str = ""
    """Record id; generated when empty on upsert."""
    score: float = 0.0
    """Similarity score; filled by ``search`` on results."""

    def __post_init__(self):
        if not self.id:
            self.id = uuid.uuid4().hex


class VectorBackend(ABC):
    """Abstract tenant-scoped vector store."""

    @abstractmethod
    def upsert(self, tenant_id: str, records: List[VectorRecord]) -> List[str]:
        """Insert or replace records for a tenant; returns record ids."""

    @abstractmethod
    def search(self,
               tenant_id: str,
               embedding: Sequence[float],
               top_k: int = 5,
               metadata_filter: Optional[Dict[str, Any]] = None) -> List[VectorRecord]:
        """Return the ``top_k`` most similar records within one tenant."""

    @abstractmethod
    def delete(self, tenant_id: str, record_ids: List[str]) -> int:
        """Delete records by id within one tenant; returns deleted count."""

    @abstractmethod
    def count(self, tenant_id: str) -> int:
        """Return the number of records stored for a tenant."""

    def list_records(self, tenant_id: str, limit: Optional[int] = None) -> List[VectorRecord]:
        """Return stored records for a tenant (used by migration tooling).

        Concrete backends that support migration must override this; the base
        implementation raises so a backend that silently returns nothing is
        impossible to mistake for an empty store.
        """
        raise NotImplementedError(f"{type(self).__name__} does not support list_records")


def _cosine_similarity(a: Sequence[float], b: Sequence[float]) -> float:
    """Cosine similarity between two vectors (0.0 when either is zero)."""
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot / (norm_a * norm_b)


class InMemoryVectorBackend(VectorBackend):
    """Zero-dependency in-memory vector store (cosine similarity).

    Suitable for tests and small single-node deployments; production
    multi-node deployments should implement ``VectorBackend`` on top of
    pgvector / Milvus / Elasticsearch instead.
    """

    def __init__(self):
        """Initialize an empty store."""
        self._lock = threading.Lock()
        # {tenant_id: {record_id: VectorRecord}}
        self._store: Dict[str, Dict[str, VectorRecord]] = {}

    def upsert(self, tenant_id: str, records: List[VectorRecord]) -> List[str]:
        """Insert or replace records; same-id records are overwritten."""
        ids: List[str] = []
        with self._lock:
            tenant_store = self._store.setdefault(tenant_id, {})
            for record in records:
                stored = VectorRecord(
                    embedding=list(record.embedding),
                    text=record.text,
                    metadata=dict(record.metadata),
                    id=record.id,
                )
                tenant_store[stored.id] = stored
                ids.append(stored.id)
        return ids

    def search(self,
               tenant_id: str,
               embedding: Sequence[float],
               top_k: int = 5,
               metadata_filter: Optional[Dict[str, Any]] = None) -> List[VectorRecord]:
        """Search by cosine similarity; results are copies with scores set."""
        with self._lock:
            candidates = list(self._store.get(tenant_id, {}).values())

        scored: List[VectorRecord] = []
        for record in candidates:
            if metadata_filter:
                if any(record.metadata.get(k) != v for k, v in metadata_filter.items()):
                    continue
            scored.append(
                VectorRecord(
                    embedding=list(record.embedding),
                    text=record.text,
                    metadata=dict(record.metadata),
                    id=record.id,
                    score=_cosine_similarity(embedding, record.embedding),
                ))

        scored.sort(key=lambda r: r.score, reverse=True)
        return scored[:max(top_k, 0)]

    def delete(self, tenant_id: str, record_ids: List[str]) -> int:
        """Delete records by id; missing ids are ignored."""
        deleted = 0
        with self._lock:
            tenant_store = self._store.get(tenant_id, {})
            for record_id in record_ids:
                if tenant_store.pop(record_id, None) is not None:
                    deleted += 1
        return deleted

    def count(self, tenant_id: str) -> int:
        """Return the number of records for a tenant."""
        with self._lock:
            return len(self._store.get(tenant_id, {}))

    def list_records(self, tenant_id: str, limit: Optional[int] = None) -> List[VectorRecord]:
        """Return copies of all records for a tenant (insertion order)."""
        with self._lock:
            records = list(self._store.get(tenant_id, {}).values())
        if limit is not None and limit >= 0:
            records = records[:limit]
        return [
            VectorRecord(
                embedding=list(r.embedding),
                text=r.text,
                metadata=dict(r.metadata),
                id=r.id,
            ) for r in records
        ]
