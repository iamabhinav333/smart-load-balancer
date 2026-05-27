from __future__ import annotations

import asyncio

from balancer.schedulers.base import BaseScheduler
from balancer.state import RoutingState


class WeightedRoundRobinScheduler(BaseScheduler):
    name = "weighted_round_robin"

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._current_weights: dict[str, int] = {}

    async def select_backend(self, state: RoutingState) -> str:
        views = await state.active_views()
        if not views:
            raise RuntimeError("no healthy backends available")

        async with self._lock:
            total_weight = 0
            for view in views:
                self._current_weights.setdefault(view.backend, 0)
                self._current_weights[view.backend] += max(1, view.weight)
                total_weight += max(1, view.weight)

            chosen = max(
                views,
                key=lambda view: (
                    self._current_weights.get(view.backend, 0),
                    -view.active_connections,
                    view.backend,
                ),
            )
            self._current_weights[chosen.backend] -= total_weight
            return chosen.backend
