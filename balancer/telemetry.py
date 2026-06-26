from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque


class RateLimiter:
    def __init__(self, max_requests: int, window_seconds: float):
        self.max_requests = max(1, int(max_requests))
        self.window_seconds = max(1.0, float(window_seconds))
        self._clients: dict[str, deque[float]] = defaultdict(deque)
        self._blocked_counts: dict[str, int] = defaultdict(int)
        self._request_counts: dict[str, int] = defaultdict(int)
        self._last_seen: dict[str, float] = {}
        self._suspicious_events: deque[dict[str, object]] = deque(maxlen=100)
        self._lock = asyncio.Lock()

    async def allow(self, client_ip: str, path: str) -> tuple[bool, dict[str, float | int]]:
        now = time.time()
        async with self._lock:
            hits = self._clients[client_ip]
            while hits and hits[0] <= now - self.window_seconds:
                hits.popleft()

            self._request_counts[client_ip] += 1
            self._last_seen[client_ip] = now

            if len(hits) >= self.max_requests:
                self._blocked_counts[client_ip] += 1
                retry_after = max(1.0, self.window_seconds - (now - hits[0])) if hits else self.window_seconds
                self._suspicious_events.append(
                    {
                        "timestamp": now,
                        "client_ip": client_ip,
                        "path": path,
                        "type": "rate_limited",
                        "request_count": len(hits),
                        "limit": self.max_requests,
                        "window_seconds": self.window_seconds,
                    }
                )
                return False, {
                    "request_count": len(hits),
                    "limit": self.max_requests,
                    "retry_after": int(retry_after),
                }

            hits.append(now)
            return True, {
                "request_count": len(hits),
                "limit": self.max_requests,
                "retry_after": 0,
            }

    async def snapshot(self) -> dict[str, object]:
        async with self._lock:
            return {
                "limit": self.max_requests,
                "window_seconds": self.window_seconds,
                "clients": [
                    {
                        "client_ip": client_ip,
                        "request_count": self._request_counts.get(client_ip, 0),
                        "blocked_count": self._blocked_counts.get(client_ip, 0),
                        "active_window_count": len(self._clients[client_ip]),
                        "last_seen": self._last_seen.get(client_ip, 0.0),
                    }
                    for client_ip in sorted(self._request_counts, key=self._request_counts.get, reverse=True)
                ],
                "recent_suspicious_events": list(self._suspicious_events),
            }


class TelemetryState:
    def __init__(self, backends: list[str], history_window_seconds: float = 60.0):
        self.backends = list(backends)
        self.history_window_seconds = max(10.0, float(history_window_seconds))
        self.total_requests = 0
        self.allowed_requests = 0
        self.blocked_requests = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.cache_entries = 0
        self.cache_ttl_seconds: float | None = None
        self.failed_requests = 0
        self.total_latency = 0.0
        self.completed_requests = 0
        self._request_events: deque[dict[str, object]] = deque(maxlen=500)
        self._completion_events: deque[dict[str, object]] = deque(maxlen=500)
        self._lock = asyncio.Lock()

    async def record_allowed_request(self, client_ip: str, path: str) -> None:
        now = time.time()
        async with self._lock:
            self.total_requests += 1
            self.allowed_requests += 1
            self._request_events.append(
                {
                    "timestamp": now,
                    "type": "allowed",
                    "client_ip": client_ip,
                    "path": path,
                }
            )

    async def record_blocked_request(self, client_ip: str, path: str, retry_after: int, limit: int) -> None:
        now = time.time()
        async with self._lock:
            self.total_requests += 1
            self.blocked_requests += 1
            self._request_events.append(
                {
                    "timestamp": now,
                    "type": "blocked",
                    "client_ip": client_ip,
                    "path": path,
                    "retry_after": retry_after,
                    "limit": limit,
                }
            )

    async def record_cache_hit(self, client_ip: str, path: str) -> None:
        now = time.time()
        async with self._lock:
            self.cache_hits += 1
            self._request_events.append(
                {
                    "timestamp": now,
                    "type": "cache_hit",
                    "client_ip": client_ip,
                    "path": path,
                }
            )

    async def record_cache_miss(self, client_ip: str, path: str) -> None:
        now = time.time()
        async with self._lock:
            self.cache_misses += 1
            self._request_events.append(
                {
                    "timestamp": now,
                    "type": "cache_miss",
                    "client_ip": client_ip,
                    "path": path,
                }
            )

    async def record_cache_store(self, ttl_seconds: float | None = None) -> None:
        now = time.time()
        async with self._lock:
            self.cache_entries += 1
            if ttl_seconds is not None:
                self.cache_ttl_seconds = float(ttl_seconds)

    async def record_cache_evict(self) -> None:
        async with self._lock:
            if self.cache_entries > 0:
                self.cache_entries -= 1

    async def record_completion(
        self,
        client_ip: str,
        backend: str,
        path: str,
        latency: float,
        status_code: int,
    ) -> None:
        now = time.time()
        failed = status_code >= 500
        async with self._lock:
            self.completed_requests += 1
            self.total_latency += latency
            if failed:
                self.failed_requests += 1
            self._completion_events.append(
                {
                    "timestamp": now,
                    "client_ip": client_ip,
                    "backend": backend,
                    "path": path,
                    "status_code": status_code,
                    "latency_ms": round(latency * 1000.0, 2),
                    "failed": failed,
                }
            )

    def _bucket_timeseries(self, events: list[dict[str, object]], now: float, buckets: int = 12) -> dict[str, list[float | str]]:
        window = self.history_window_seconds
        bucket_size = window / buckets
        start = now - window
        request_counts = [0] * buckets
        latency_totals = [0.0] * buckets
        latency_counts = [0] * buckets

        for event in events:
            timestamp = float(event["timestamp"])
            if timestamp < start:
                continue
            bucket_index = min(buckets - 1, int((timestamp - start) / bucket_size))
            request_counts[bucket_index] += 1
            latency_totals[bucket_index] += float(event["latency_ms"])
            latency_counts[bucket_index] += 1

        labels = [f"-{int(window - (bucket_size * index))}s" for index in range(buckets)]
        latency_series = [
            round(latency_totals[index] / latency_counts[index], 2) if latency_counts[index] else 0.0
            for index in range(buckets)
        ]
        return {
            "labels": labels,
            "requests_per_bucket": request_counts,
            "latency_ms_per_bucket": latency_series,
        }

    async def snapshot(self, state, rate_limiter: RateLimiter | None = None) -> dict[str, object]:
        now = time.time()
        stats_snapshot, health_snapshot, active_views = await asyncio.gather(
            state.stats_snapshot(),
            state.health_snapshot(),
            state.active_views(),
        )

        async with self._lock:
            recent_completion_events = [
                event for event in self._completion_events if float(event["timestamp"]) >= now - self.history_window_seconds
            ]
            timeseries = self._bucket_timeseries(recent_completion_events, now)
            requests_per_second = (
                len(recent_completion_events) / self.history_window_seconds if self.history_window_seconds else 0.0
            )
            average_latency_ms = (
                (self.total_latency / self.completed_requests) * 1000.0 if self.completed_requests else 0.0
            )
            recent_events = list(self._request_events)[-20:]
            recent_completions = list(self._completion_events)[-20:]

        backends = []
        total_backend_requests = 0
        for view in active_views:
            backend_stats = stats_snapshot.get(view.backend, {})
            backend_health = health_snapshot.get(view.backend, {})
            request_count = int(backend_stats.get("request_count", 0))
            total_backend_requests += request_count
            backends.append(
                {
                    "backend": view.backend,
                    "healthy": bool(backend_health.get("healthy", False)),
                    "active": bool(backend_health.get("active", False)),
                    "request_count": request_count,
                    "active_connections": int(backend_stats.get("active_connections", 0)),
                    "last_latency_ms": round(float(backend_stats.get("last_latency", 0.0)) * 1000.0, 2),
                    "average_latency_ms": round(float(backend_stats.get("average_latency", 0.0)) * 1000.0, 2),
                    "weight": int(backend_stats.get("weight", view.weight)),
                    "error": backend_stats.get("error"),
                    "share": round((request_count / total_backend_requests) * 100.0, 2) if total_backend_requests else 0.0,
                }
            )

        rate_limit_snapshot = await rate_limiter.snapshot() if rate_limiter is not None else {
            "limit": 0,
            "window_seconds": 0.0,
            "clients": [],
            "recent_suspicious_events": [],
        }

        cache_snapshot = {
            "hits": self.cache_hits,
            "misses": self.cache_misses,
            "entries": self.cache_entries,
            "ttl_seconds": self.cache_ttl_seconds or 0.0,
        }

        return {
            "generated_at": now,
            "summary": {
                "total_requests": self.total_requests,
                "allowed_requests": self.allowed_requests,
                "blocked_requests": self.blocked_requests,
                "failed_requests": self.failed_requests,
                "successful_requests": self.completed_requests - self.failed_requests,
                "completed_requests": self.completed_requests,
                "average_latency_ms": round(average_latency_ms, 2),
                "requests_per_second": round(requests_per_second, 2),
                "active_backends": len(active_views),
            },
            "backends": backends,
            "health": health_snapshot,
            "stats": stats_snapshot,
            "rate_limit": rate_limit_snapshot,
            "cache": cache_snapshot,
            "timeseries": timeseries,
            "recent_requests": recent_events,
            "recent_completions": recent_completions,
        }