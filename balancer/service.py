from __future__ import annotations

from contextlib import asynccontextmanager
import asyncio
import logging
import os
import time
from pathlib import Path

import aiohttp
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn
from starlette.responses import Response

from balancer.schedulers.factory import available_scheduler_modes, create_scheduler
from balancer.state import RoutingState
from balancer.telemetry import RateLimiter, TelemetryState


BACKENDS = [
    "http://127.0.0.1:5001",
    "http://127.0.0.1:5002",
    "http://127.0.0.1:5003",
]

BACKEND_WEIGHTS = {
    BACKENDS[0]: 1,
    BACKENDS[1]: 2,
    BACKENDS[2]: 3,
}

SCHEDULER_MODE = os.getenv("BALANCER_SCHEDULER", "least_connections")
HEALTH_CHECK_INTERVAL = float(os.getenv("BALANCER_HEALTH_INTERVAL", "5.0"))
RATE_LIMIT_MAX_REQUESTS = int(os.getenv("BALANCER_RATE_LIMIT_MAX_REQUESTS", "60"))
RATE_LIMIT_WINDOW_SECONDS = float(os.getenv("BALANCER_RATE_LIMIT_WINDOW_SECONDS", "60.0"))

logger = logging.getLogger("balancer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

state = RoutingState(BACKENDS, BACKEND_WEIGHTS)
scheduler = create_scheduler(SCHEDULER_MODE)
rate_limiter = RateLimiter(RATE_LIMIT_MAX_REQUESTS, RATE_LIMIT_WINDOW_SECONDS)
telemetry = TelemetryState(BACKENDS)
# Simple in-memory GET response cache
class SimpleCacheEntry:
    def __init__(self, content: bytes, headers: dict[str, str], media_type: str | None, expires_at: float):
        self.content = content
        self.headers = headers
        self.media_type = media_type
        self.expires_at = expires_at


class SimpleInMemoryCache:
    def __init__(self, ttl_seconds: float = 5.0):
        self._store: dict[str, SimpleCacheEntry] = {}
        self._ttl = float(ttl_seconds)
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> SimpleCacheEntry | None:
        now = time.time()
        async with self._lock:
            entry = self._store.get(key)
            if not entry:
                return None
            if entry.expires_at < now:
                # expired
                self._store.pop(key, None)
                await telemetry.record_cache_evict()
                return None
            return entry

    async def set(self, key: str, content: bytes, headers: dict[str, str], media_type: str | None, ttl: float | None = None):
        expires_at = time.time() + (ttl if ttl is not None else self._ttl)
        async with self._lock:
            self._store[key] = SimpleCacheEntry(content, headers, media_type, expires_at)
        await telemetry.record_cache_store(ttl)

    async def snapshot(self) -> dict:
        now = time.time()
        async with self._lock:
            return {"entries": len(self._store), "ttl_seconds": self._ttl}


cache_ttl = float(os.getenv("BALANCER_CACHE_TTL_SECONDS", "5.0"))
cache = SimpleInMemoryCache(cache_ttl)
ROOT_DIR = Path(__file__).resolve().parents[1]
DASHBOARD_FILE = ROOT_DIR / "dashboard" / "index.html"


def get_client_ip(request: Request) -> str:
    forwarded_for = request.headers.get("x-forwarded-for")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    real_ip = request.headers.get("x-real-ip")
    if real_ip:
        return real_ip.strip()
    if request.client is not None and request.client.host:
        return request.client.host
    return "unknown"


def is_protected_route(path: str) -> bool:
    exempt_prefixes = (
        "/health",
        "/stats",
        "/scheduler",
        "/metrics",
        "/api/metrics",
        "/dashboard",
    )
    return not path.startswith(exempt_prefixes)


async def check_backend(session: aiohttp.ClientSession, backend: str, timeout_sec: float = 1.0) -> tuple[str, bool, str | None]:
    url = backend.rstrip("/") + "/health"
    try:
        async with session.get(url, timeout=timeout_sec) as resp:
            if 200 <= resp.status < 300:
                try:
                    _ = await resp.json()
                except Exception:
                    pass
                return backend, True, None
            return backend, False, f"status:{resp.status}"
    except Exception as exc:
        return backend, False, str(exc)


async def monitor_loop(interval: float = HEALTH_CHECK_INTERVAL):
    logger.info("Starting backend monitor loop (interval=%ss)", interval)
    timeout = aiohttp.ClientTimeout(total=max(1.0, interval))
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            results = await asyncio.gather(
                *(check_backend(session, backend, timeout_sec=min(1.0, interval)) for backend in BACKENDS),
                return_exceptions=False,
            )
            now = time.time()
            for backend, healthy, error in results:
                previous, current = await state.record_health(backend, healthy, error)
                if previous != current:
                    if current:
                        logger.info("Backend healthy again: %s", backend)
                    else:
                        logger.warning("Backend unhealthy and removed: %s (error: %s)", backend, error)
            if not (await state.active_views()):
                logger.error("No active backends available after health check")
            await asyncio.sleep(interval)


app = FastAPI(title="Smart Load Balancer")


@app.middleware("http")
async def rate_limit_and_telemetry_middleware(request: Request, call_next):
    path = request.url.path
    if not is_protected_route(path):
        return await call_next(request)

    client_ip = get_client_ip(request)
    allowed, details = await rate_limiter.allow(client_ip, path)
    if not allowed:
        await telemetry.record_blocked_request(
            client_ip,
            path,
            retry_after=int(details["retry_after"]),
            limit=int(details["limit"]),
        )
        logger.warning(
            "Rate limit exceeded for %s on %s (%s requests in %ss)",
            client_ip,
            path,
            details["request_count"],
            RATE_LIMIT_WINDOW_SECONDS,
        )
        return JSONResponse(
            status_code=429,
            content={
                "detail": "Too Many Requests",
                "client_ip": client_ip,
                "limit": details["limit"],
                "window_seconds": RATE_LIMIT_WINDOW_SECONDS,
                "retry_after": details["retry_after"],
            },
            headers={"Retry-After": str(details["retry_after"])},
        )

    await telemetry.record_allowed_request(client_ip, path)
    return await call_next(request)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await state.initialize()
    monitor_task = asyncio.create_task(monitor_loop())
    try:
        yield
    finally:
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            logger.info("Monitor task cancelled")


app.router.lifespan_context = lifespan


@app.get("/")
async def root():
    return {
        "message": "Smart Load Balancer",
        "scheduler": scheduler.name,
        "available_schedulers": available_scheduler_modes(),
        "backends": BACKENDS,
        "dashboard": "/dashboard",
        "metrics": "/api/metrics",
    }


@app.get("/scheduler")
async def scheduler_info():
    return {
        "scheduler": scheduler.name,
        "backends": BACKENDS,
        "weights": BACKEND_WEIGHTS,
    }


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "scheduler": scheduler.name,
        "backends": await state.health_snapshot(),
    }


@app.get("/stats")
async def stats():
    return {
        "scheduler": scheduler.name,
        "backends": await state.stats_snapshot(),
        "active_backends": [view.backend for view in await state.active_views()],
    }


@app.get("/metrics")
async def metrics():
    return await telemetry.snapshot(state, rate_limiter)


@app.get("/api/metrics")
async def api_metrics():
    return await telemetry.snapshot(state, rate_limiter)


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    if not DASHBOARD_FILE.exists():
        raise HTTPException(status_code=500, detail="Dashboard UI is missing")
    return DASHBOARD_FILE.read_text(encoding="utf-8")


async def proxy_to_backend(request: Request, path: str):
    backend = None
    client_ip = get_client_ip(request)
    start = time.perf_counter()
    upstream_status = 502
    cache_key = None
    try:
        # Only cache GET responses for idempotent fetches
        if request.method.upper() == "GET":
            cache_key = request.url.path + ("?" + request.url.query if request.url.query else "")
            entry = await cache.get(cache_key)
            if entry is not None:
                await telemetry.record_cache_hit(client_ip, path)
                # Return cached response
                return Response(content=entry.content, status_code=200, headers=entry.headers, media_type=entry.media_type)
            else:
                await telemetry.record_cache_miss(client_ip, path)
        backend = await scheduler.select_backend(state)
        await state.start_request(backend)

        forward_url = backend.rstrip("/") + "/" + path
        if request.url.query:
            forward_url = f"{forward_url}?{request.url.query}"

        forward_headers = {key: value for key, value in request.headers.items() if key.lower() != "host"}
        timeout = aiohttp.ClientTimeout(total=30.0)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                body = await request.body()
                async with session.request(request.method, forward_url, headers=forward_headers, data=body) as upstream:
                    upstream_status = upstream.status
                    content = await upstream.read()
                    response_headers = {
                        key: value
                        for key, value in upstream.headers.items()
                        if key.lower()
                        not in {
                            "connection",
                            "keep-alive",
                            "proxy-authenticate",
                            "proxy-authorization",
                            "te",
                            "trailers",
                            "transfer-encoding",
                            "upgrade",
                            "content-length",
                        }
                    }
                    media_type = response_headers.get("Content-Type")
                        # Store GET responses in cache when status is 200
                        if request.method.upper() == "GET" and upstream.status == 200:
                            try:
                                await cache.set(cache_key, content, response_headers, media_type, ttl=cache_ttl)
                            except Exception:
                                logger.exception("Failed to store cache entry")
                    return Response(content=content, status_code=upstream.status, headers=response_headers, media_type=media_type)
            except aiohttp.ClientError as exc:
                logger.warning("Proxy error for backend %s: %s", backend, exc)
                upstream_status = 502
                raise HTTPException(status_code=502, detail=str(exc))
    finally:
        if backend is not None:
            latency = time.perf_counter() - start
            await state.finish_request(backend, latency)
            await telemetry.record_completion(client_ip, backend, path, latency, upstream_status)


@app.get("/api/data")
async def api_data(request: Request):
    return await proxy_to_backend(request, "api/data")


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"])
async def proxy(path: str, request: Request):
    if path in {"", "health", "stats", "scheduler", "dashboard", "metrics", "api/metrics"}:
        raise HTTPException(status_code=404, detail="Not found")
    return await proxy_to_backend(request, path)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
