# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 THL A29 Limited, a Tencent company. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Capacity planning for multi-tenant node deployments.

Pure estimation functions that translate observed tenant metrics (from
:class:`trpc_agent_sdk.tenants.TenantMetrics`) and per-tenant budgets into
deployment numbers:

- :func:`nodes_for_qps` — horizontal node count for a target QPS given the
  per-node capacity and a headroom margin (N+1 style redundancy).
- :func:`project_month_end_usage` — linear month-to-date extrapolation of
  token / cost usage to month end.
- :func:`budget_headroom` — remaining monthly budget per tenant after
  extrapolated spend (negative → will be exceeded).
- :func:`capacity_plan` — one-shot aggregate for ops reports.

All functions are deterministic and dependency-free so they can run in CI,
cron jobs, or a small ops CLI.
"""

import math
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Dict, List, Optional


def nodes_for_qps(target_qps: float, per_node_qps: float, headroom_percent: float = 30.0) -> int:
    """Return the node count needed to serve ``target_qps``.

    Args:
        target_qps: Peak requests per second the cluster must absorb.
        per_node_qps: Sustained QPS a single node can handle.
        headroom_percent: Safety margin on top of the raw need (default 30%)
            so nodes survive traffic bursts and rolling deploys.

    Returns:
        Node count (at least 1; rounds up).
    """
    if target_qps < 0:
        raise ValueError("target_qps must be >= 0")
    if per_node_qps <= 0:
        raise ValueError("per_node_qps must be > 0")
    if headroom_percent < 0:
        raise ValueError("headroom_percent must be >= 0")
    needed = target_qps / per_node_qps * (1 + headroom_percent / 100)
    return max(1, math.ceil(needed))


def project_month_end_usage(month_to_date: float, today: Optional[date] = None) -> float:
    """Linearly extrapolate month-to-date usage to month end.

    Assumes uniform usage; the day count is calendar-based so partial first
    days are handled by the caller passing an accurate ``today``.
    """
    if month_to_date < 0:
        raise ValueError("month_to_date must be >= 0")
    today = today or date.today()
    days_in_month = _days_in_month(today)
    if today.day >= days_in_month:
        return month_to_date
    return month_to_date / today.day * days_in_month


def budget_headroom(monthly_budget_usd: float, month_to_date_cost: float, today: Optional[date] = None) -> float:
    """Return projected remaining budget at month end (USD).

    Negative values mean the tenant is projected to exceed its monthly
    budget — the signal for the capacity report / alerting.
    """
    if monthly_budget_usd < 0:
        raise ValueError("monthly_budget_usd must be >= 0")
    projected = project_month_end_usage(month_to_date_cost, today)
    return monthly_budget_usd - projected


@dataclass
class TenantCapacityRow:
    """Per-tenant capacity report row."""

    tenant_id: str
    month_to_date_requests: int
    projected_month_end_requests: int
    month_to_date_cost_usd: float
    projected_month_end_cost_usd: float
    monthly_budget_usd: float
    budget_headroom_usd: float


@dataclass
class CapacityReport:
    """Aggregate capacity plan for a deployment."""

    generated_on: date
    peak_qps: float
    per_node_qps: float
    headroom_percent: float
    nodes_required: int
    tenants: List[TenantCapacityRow] = field(default_factory=list)

    def tenants_over_budget(self) -> List[str]:
        """Tenant ids projected to exceed their monthly budget."""
        return [row.tenant_id for row in self.tenants if row.budget_headroom_usd < 0]

    def summary(self) -> Dict[str, object]:
        """JSON-able summary for ops dashboards."""
        return {
            "generated_on": self.generated_on.isoformat(),
            "peak_qps": self.peak_qps,
            "per_node_qps": self.per_node_qps,
            "headroom_percent": self.headroom_percent,
            "nodes_required": self.nodes_required,
            "tenant_count": len(self.tenants),
            "tenants_over_budget": self.tenants_over_budget(),
        }


def capacity_plan(
    peak_qps: float,
    per_node_qps: float,
    tenant_usage: List[Dict[str, float]],
    headroom_percent: float = 30.0,
    today: Optional[date] = None,
) -> CapacityReport:
    """Build a one-shot capacity report.

    Args:
        peak_qps: Observed cluster peak QPS (from tenant metrics).
        per_node_qps: Sustained per-node capacity.
        tenant_usage: One dict per tenant with keys ``tenant_id``,
            ``requests_mtd`` (month-to-date requests), ``cost_usd_mtd`` and
            ``monthly_budget_usd``.
        headroom_percent: Node headroom margin.
        today: Reference date (defaults to today).

    Returns:
        :class:`CapacityReport` with node sizing and per-tenant budget
        projections.
    """
    today = today or date.today()
    rows: List[TenantCapacityRow] = []
    for usage in tenant_usage:
        cost_mtd = float(usage.get("cost_usd_mtd", 0.0))
        requests_mtd = float(usage.get("requests_mtd", 0.0))
        budget = float(usage.get("monthly_budget_usd", 0.0))
        projected_cost = project_month_end_usage(cost_mtd, today)
        projected_requests = project_month_end_usage(requests_mtd, today)
        rows.append(
            TenantCapacityRow(
                tenant_id=str(usage.get("tenant_id", "")),
                month_to_date_requests=int(requests_mtd),
                projected_month_end_requests=int(projected_requests),
                month_to_date_cost_usd=cost_mtd,
                projected_month_end_cost_usd=projected_cost,
                monthly_budget_usd=budget,
                budget_headroom_usd=budget - projected_cost,
            ))
    return CapacityReport(
        generated_on=today,
        peak_qps=peak_qps,
        per_node_qps=per_node_qps,
        headroom_percent=headroom_percent,
        nodes_required=nodes_for_qps(peak_qps, per_node_qps, headroom_percent),
        tenants=rows,
    )


def _days_in_month(day: date) -> int:
    """Return the number of days in the month containing ``day``."""
    next_month = date(day.year + (day.month == 12), day.month % 12 + 1, 1)
    return (next_month - timedelta(days=1)).day
