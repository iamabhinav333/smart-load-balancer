from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass


@dataclass
class BackendStats:
    healthy: bool = False
    last_checked: float = 0.0
    error: str | None = None
    request_count: int = 0
    total_latency: float = 0.0
    last_latency: float = 0.0
    active_connections: int = 0
    weight: int = 1

    @property
    def average_latency(self) -> float:
        return self.total_latency / self.request_count if self.request_count else 0.0


@dataclass(frozen=True)
class BackendView:
    backend: str
    healthy: bool
    active_connections: int
    request_count: int
    average_latency: float
    last_latency: float
    weight: int


class RoutingState:
    def __init__(self, backends: list[str], weights: dict[str, int]):
        self.backends = list(backends)
        self.weights = {backend: max(1, int(weights.get(backend, 1))) for backend in self.backends}
        self.stats = {
            backend: BackendStats(healthy=False, weight=self.weights[backend]) for backend in self.backends
        }
        self.active_backends = list(self.backends)
        self.lock = asyncio.Lock()

    async def initialize(self) -> None:
        async with self.lock:
            now = time.time()
            self.active_backends = list(self.backends)
            for backend in self.backends:
                self.stats[backend] = BackendStats(
                    healthy=False,
                    last_checked=now,
                    error="not checked yet",
                    weight=self.weights[backend],
                )

    async def record_health(self, backend: str, healthy: bool, error: str | None = None) -> tuple[bool, bool]:
        async with self.lock:
            now = time.time()
            stats = self.stats[backend]
            previous = stats.healthy
            stats.healthy = healthy
            stats.last_checked = now
            stats.error = error
            if healthy and backend not in self.active_backends:
                self.active_backends.append(backend)
            if not healthy and backend in self.active_backends:
                self.active_backends = [item for item in self.active_backends if item != backend]
            return previous, healthy

    async def start_request(self, backend: str) -> None:
        async with self.lock:
            self.stats[backend].active_connections += 1

    async def finish_request(self, backend: str, latency: float) -> None:
        async with self.lock:
            stats = self.stats[backend]
            stats.active_connections = max(0, stats.active_connections - 1)
            stats.request_count += 1
            stats.total_latency += latency
            stats.last_latency = latency

    async def active_views(self) -> list[BackendView]:
        async with self.lock:
            return [
                BackendView(
                    backend=backend,
                    healthy=self.stats[backend].healthy,
                    active_connections=self.stats[backend].active_connections,
                    request_count=self.stats[backend].request_count,
                    average_latency=self.stats[backend].average_latency,
                    last_latency=self.stats[backend].last_latency,
                    weight=self.stats[backend].weight,
                )
                for backend in self.active_backends
            ]

    async def health_snapshot(self) -> dict[str, dict]:
        async with self.lock:
            return {
                backend: {
                    "healthy": self.stats[backend].healthy,
                    "last_checked": self.stats[backend].last_checked,
                    "error": self.stats[backend].error,
                    "weight": self.stats[backend].weight,
                    "active": backend in self.active_backends,
                }
                for backend in self.backends
            }

    async def stats_snapshot(self) -> dict[str, dict]:
        async with self.lock:
            return {
                backend: {
                    "healthy": self.stats[backend].healthy,
                    "weight": self.stats[backend].weight,
                    "active_connections": self.stats[backend].active_connections,
                    "request_count": self.stats[backend].request_count,
                    "last_latency": self.stats[backend].last_latency,
                    "average_latency": self.stats[backend].average_latency,
                    "last_checked": self.stats[backend].last_checked,
                    "error": self.stats[backend].error,
                }
                for backend in self.backends
            }
