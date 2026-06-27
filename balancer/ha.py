from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any


class SharedClusterState:
    def __init__(self, state_dir: str | Path, heartbeat_timeout_seconds: float = 10.0):
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.heartbeat_timeout_seconds = max(1.0, float(heartbeat_timeout_seconds))
        self._lock = asyncio.Lock()

    def _instance_path(self, instance_id: str) -> Path:
        safe_instance_id = "".join(character for character in instance_id if character.isalnum() or character in {"-", "_"})
        return self.state_dir / f"{safe_instance_id or 'instance'}.json"

    async def publish(self, instance_id: str, payload: dict[str, Any]) -> None:
        now = time.time()
        record = dict(payload)
        record.setdefault("instance_id", instance_id)
        record.setdefault("updated_at", now)
        record.setdefault("heartbeat_at", now)
        record.setdefault("heartbeat_timeout_seconds", self.heartbeat_timeout_seconds)
        serialized = json.dumps(record, ensure_ascii=True, indent=2, sort_keys=True)
        path = self._instance_path(instance_id)
        temp_path = path.with_suffix(".json.tmp")

        async with self._lock:
            temp_path.write_text(serialized, encoding="utf-8")
            os.replace(temp_path, path)

    async def snapshot(self) -> dict[str, Any]:
        now = time.time()
        instances: list[dict[str, Any]] = []

        async with self._lock:
            for path in sorted(self.state_dir.glob("*.json")):
                try:
                    data = json.loads(path.read_text(encoding="utf-8"))
                except Exception:
                    continue

                heartbeat_at = float(data.get("heartbeat_at", 0.0))
                alive = (now - heartbeat_at) <= self.heartbeat_timeout_seconds
                data["alive"] = alive
                data["state_file"] = str(path)
                instances.append(data)

        instances.sort(
            key=lambda item: (
                int(item.get("priority", 9999)),
                -float(item.get("heartbeat_at", 0.0)),
                str(item.get("instance_id", "")),
            )
        )

        active_instance = next((instance for instance in instances if instance.get("alive", False)), None)
        return {
            "generated_at": now,
            "heartbeat_timeout_seconds": self.heartbeat_timeout_seconds,
            "instances": instances,
            "active_instance": active_instance,
            "healthy_instances": [instance for instance in instances if instance.get("alive", False)],
        }


def build_local_ha_payload(
    *,
    instance_id: str,
    instance_url: str,
    priority: int,
    health_snapshot: dict[str, Any],
    stats_snapshot: dict[str, Any],
    telemetry_snapshot: dict[str, Any],
    peer_heartbeats: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "instance_id": instance_id,
        "instance_url": instance_url,
        "priority": priority,
        "service": {
            "health": health_snapshot,
            "stats": stats_snapshot,
            "telemetry": telemetry_snapshot,
        },
        "peer_heartbeats": peer_heartbeats or {},
        "heartbeat_at": time.time(),
    }


def choose_active_instance(cluster_snapshot: dict[str, Any], fallback_urls: list[str] | None = None) -> str | None:
    active = cluster_snapshot.get("active_instance")
    if isinstance(active, dict):
        instance_url = active.get("instance_url")
        if isinstance(instance_url, str) and instance_url:
            return instance_url

    if fallback_urls:
        for url in fallback_urls:
            if url:
                return url
    return None