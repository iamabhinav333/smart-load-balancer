from __future__ import annotations

from balancer.schedulers.base import BaseScheduler
from balancer.state import RoutingState


class LeastConnectionsScheduler(BaseScheduler):
    name = "least_connections"

    async def select_backend(self, state: RoutingState) -> str:
        views = await state.active_views()
        if not views:
            raise RuntimeError("no healthy backends available")
        chosen = min(views, key=lambda view: (view.active_connections, view.request_count, -view.weight, view.backend))
        return chosen.backend
