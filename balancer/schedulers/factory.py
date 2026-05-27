from __future__ import annotations

from balancer.schedulers.least_connections import LeastConnectionsScheduler
from balancer.schedulers.weighted_round_robin import WeightedRoundRobinScheduler


def create_scheduler(mode: str):
    normalized = (mode or "").strip().lower()
    if normalized in {"least", "lc", "least_connections", "least-connections"}:
        return LeastConnectionsScheduler()
    if normalized in {"weighted", "wrr", "weighted_round_robin", "weighted-round-robin"}:
        return WeightedRoundRobinScheduler()
    raise ValueError(f"Unsupported scheduler mode: {mode}")


def available_scheduler_modes() -> list[str]:
    return ["least_connections", "weighted_round_robin"]
