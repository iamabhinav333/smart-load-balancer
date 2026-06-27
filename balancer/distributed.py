from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any

try:
    from redis.asyncio import Redis  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    Redis = None  # type: ignore[assignment]


def _default_state() -> dict[str, Any]:
    return {
        "backends": {},
        "sessions": {},
        "meta": {"updated_at": time.time()},
    }


class SharedStateStore:
    def __init__(self, storage_dir: str | Path, namespace: str = "distributed_state", redis_url: str | None = None):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.file_path = self.storage_dir / f"{namespace}.json"
        self.redis_url = (redis_url or os.getenv("BALANCER_REDIS_URL", "")).strip()
        self.redis_key = f"smart-load-balancer:{namespace}"
        self._redis: Redis | None = None
        self._lock = asyncio.Lock()

    async def _get_redis(self) -> Redis | None:
        if not self.redis_url or Redis is None:
            return None
        if self._redis is None:
            self._redis = Redis.from_url(self.redis_url, decode_responses=True)
        return self._redis

    async def _read_file_state(self) -> dict[str, Any]:
        if not self.file_path.exists():
            return _default_state()
        try:
            payload = json.loads(self.file_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                payload.setdefault("backends", {})
                payload.setdefault("sessions", {})
                payload.setdefault("meta", {})
                return payload
        except Exception:
            pass
        return _default_state()

    async def _write_file_state(self, state: dict[str, Any]) -> None:
        temp_path = self.file_path.with_suffix(".json.tmp")
        temp_path.write_text(json.dumps(state, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(temp_path, self.file_path)

    async def load(self) -> dict[str, Any]:
        redis_client = await self._get_redis()
        if redis_client is not None:
            raw_value = await redis_client.get(self.redis_key)
            if raw_value:
                try:
                    state = json.loads(raw_value)
                    if isinstance(state, dict):
                        return state
                except Exception:
                    pass
        return await self._read_file_state()

    async def save(self, state: dict[str, Any]) -> None:
        state = dict(state)
        state.setdefault("backends", {})
        state.setdefault("sessions", {})
        state.setdefault("meta", {})
        state["meta"]["updated_at"] = time.time()

        redis_client = await self._get_redis()
        if redis_client is not None:
            await redis_client.set(self.redis_key, json.dumps(state, ensure_ascii=True))
            return
        await self._write_file_state(state)

    async def snapshot(self) -> dict[str, Any]:
        async with self._lock:
            return await self.load()

    async def register_backend(self, backend_url: str, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        async with self._lock:
            state = await self.load()
            backends = state.setdefault("backends", {})
            now = time.time()
            record = dict(metadata or {})
            record["backend_url"] = backend_url
            record.setdefault("backend_id", backend_url)
            record.setdefault("kind", "static")
            record.setdefault("healthy", True)
            record["last_seen"] = now
            record.setdefault("created_at", now)
            backends[backend_url] = record
            await self.save(state)
            return record

    async def touch_backend(self, backend_url: str, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        async with self._lock:
            state = await self.load()
            backends = state.setdefault("backends", {})
            now = time.time()
            record = dict(backends.get(backend_url, {}))
            record.update(metadata or {})
            record["backend_url"] = backend_url
            record["last_seen"] = now
            backends[backend_url] = record
            await self.save(state)
            return record

    async def remove_backend(self, backend_url: str) -> None:
        async with self._lock:
            state = await self.load()
            state.setdefault("backends", {}).pop(backend_url, None)
            sessions = state.setdefault("sessions", {})
            stale_sessions = [session_id for session_id, session in sessions.items() if session.get("backend_url") == backend_url]
            for session_id in stale_sessions:
                sessions.pop(session_id, None)
            await self.save(state)

    async def list_backends(self) -> list[dict[str, Any]]:
        state = await self.snapshot()
        backends = state.get("backends", {})
        if not isinstance(backends, dict):
            return []
        items = list(backends.values())
        items.sort(key=lambda item: (str(item.get("kind", "")), str(item.get("backend_url", ""))))
        return items

    async def bind_session(self, session_id: str, backend_url: str, ttl_seconds: float = 1800.0) -> dict[str, Any]:
        async with self._lock:
            state = await self.load()
            sessions = state.setdefault("sessions", {})
            now = time.time()
            record = {
                "session_id": session_id,
                "backend_url": backend_url,
                "created_at": sessions.get(session_id, {}).get("created_at", now),
                "last_seen": now,
                "expires_at": now + max(60.0, float(ttl_seconds)),
            }
            sessions[session_id] = record
            await self.save(state)
            return record

    async def resolve_session(self, session_id: str) -> str | None:
        async with self._lock:
            state = await self.load()
            sessions = state.get("sessions", {})
            if not isinstance(sessions, dict):
                return None
            session = sessions.get(session_id)
            if not isinstance(session, dict):
                return None
            expires_at = float(session.get("expires_at", 0.0))
            if expires_at and expires_at < time.time():
                sessions.pop(session_id, None)
                await self.save(state)
                return None
            backend_url = session.get("backend_url")
            return str(backend_url) if backend_url else None

    async def prune_stale_backends(self, max_age_seconds: float) -> list[str]:
        async with self._lock:
            state = await self.load()
            backends = state.setdefault("backends", {})
            now = time.time()
            removed: list[str] = []
            for backend_url, record in list(backends.items()):
                if record.get("kind") == "static":
                    continue
                last_seen = float(record.get("last_seen", 0.0))
                if last_seen and (now - last_seen) > max_age_seconds:
                    removed.append(backend_url)
                    backends.pop(backend_url, None)
            if removed:
                await self.save(state)
            return removed
