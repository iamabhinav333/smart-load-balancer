from __future__ import annotations

from abc import ABC, abstractmethod

from balancer.state import RoutingState


class BaseScheduler(ABC):
    name = "base"

    @abstractmethod
    async def select_backend(self, state: RoutingState) -> str:
        raise NotImplementedError
