from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from balancer.distributed import SharedStateStore


@dataclass
class ReplicaProcess:
    backend_url: str
    port: int
    process: subprocess.Popen[bytes]
    metadata: dict[str, Any] = field(default_factory=dict)


class LocalAutoscaler:
    def __init__(
        self,
        store: SharedStateStore,
        *,
        host: str = "127.0.0.1",
        start_port: int = 5100,
        max_replicas: int = 3,
        min_replicas: int = 0,
        scale_up_requests_per_second: float = 8.0,
        scale_up_active_connections: int = 6,
        scale_down_idle_seconds: float = 120.0,
        cooldown_seconds: float = 20.0,
    ):
        self.store = store
        self.host = host
        self.start_port = start_port
        self.max_replicas = max(0, int(max_replicas))
        self.min_replicas = max(0, int(min_replicas))
        self.scale_up_requests_per_second = max(1.0, float(scale_up_requests_per_second))
        self.scale_up_active_connections = max(1, int(scale_up_active_connections))
        self.scale_down_idle_seconds = max(30.0, float(scale_down_idle_seconds))
        self.cooldown_seconds = max(5.0, float(cooldown_seconds))
        self._lock = asyncio.Lock()
        self._replicas: dict[str, ReplicaProcess] = {}
        self._next_port = start_port
        self._last_scale_up = 0.0
        self._last_scale_down = 0.0

    def _next_backend_port(self) -> int:
        port = self._next_port
        self._next_port += 1
        return port

    async def bootstrap_minimum_replicas(self) -> list[str]:
        spawned: list[str] = []
        while len(self._replicas) < self.min_replicas:
            spawned_url = await self.scale_up(reason="bootstrap")
            if spawned_url is None:
                break
            spawned.append(spawned_url)
        return spawned

    async def scale_up(self, reason: str) -> str | None:
        async with self._lock:
            if len(self._replicas) >= self.max_replicas:
                return None
            now = time.time()
            if self._replicas and (now - self._last_scale_up) < self.cooldown_seconds:
                return None

            port = self._next_backend_port()
            backend_url = f"http://{self.host}:{port}"
            env = os.environ.copy()
            env.update(
                {
                    "BACKEND_INSTANCE_ID": f"replica-{port}",
                    "BACKEND_PORT": str(port),
                    "BACKEND_LABEL": f"replica-{port}",
                    "BACKEND_SHARED_STATE_DIR": str(self.store.storage_dir),
                    "BACKEND_SOURCE": "autoscaled",
                    "BACKEND_WEBSOCKET_ENABLED": "1",
                }
            )
            process = subprocess.Popen([sys.executable, "-m", "backend_servers.replica"], cwd=str(Path(self.store.storage_dir).parents[0]), env=env)
            self._replicas[backend_url] = ReplicaProcess(backend_url=backend_url, port=port, process=process, metadata={"reason": reason, "spawned_at": now})
            self._last_scale_up = now
            await self.store.register_backend(
                backend_url,
                {
                    "backend_id": f"replica-{port}",
                    "kind": "autoscaled",
                    "source": "autoscaler",
                    "port": port,
                    "healthy": True,
                    "reason": reason,
                },
            )
            return backend_url

    async def maybe_scale(self, *, requests_per_second: float, active_connections: int, active_backends: int) -> dict[str, Any]:
        result: dict[str, Any] = {
            "scaled_up": False,
            "scaled_down": False,
            "reason": None,
            "replicas": len(self._replicas),
        }

        if requests_per_second >= self.scale_up_requests_per_second or active_connections >= self.scale_up_active_connections:
            spawned = await self.scale_up(reason=f"rps={requests_per_second:.2f},connections={active_connections}")
            if spawned is not None:
                result.update({"scaled_up": True, "reason": "load_threshold", "backend_url": spawned, "replicas": len(self._replicas)})
                return result

        if len(self._replicas) > self.min_replicas and active_backends <= self.min_replicas:
            now = time.time()
            if (now - self._last_scale_down) >= self.cooldown_seconds:
                backend_url, replica = next(iter(self._replicas.items()))
                replica.process.terminate()
                self._replicas.pop(backend_url, None)
                self._last_scale_down = now
                await self.store.remove_backend(backend_url)
                result.update({"scaled_down": True, "reason": "idle", "backend_url": backend_url, "replicas": len(self._replicas)})

        return result

    async def snapshot(self) -> dict[str, Any]:
        async with self._lock:
            replicas = []
            for replica in self._replicas.values():
                replicas.append(
                    {
                        "backend_url": replica.backend_url,
                        "port": replica.port,
                        "alive": replica.process.poll() is None,
                        "metadata": replica.metadata,
                    }
                )
            return {
                "replicas": replicas,
                "max_replicas": self.max_replicas,
                "min_replicas": self.min_replicas,
                "scale_up_requests_per_second": self.scale_up_requests_per_second,
                "scale_up_active_connections": self.scale_up_active_connections,
                "cooldown_seconds": self.cooldown_seconds,
            }
