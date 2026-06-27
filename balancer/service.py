from __future__ import annotations

from contextlib import asynccontextmanager
import asyncio
import logging
import os
import time
import uuid
from pathlib import Path

import aiohttp
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn
from starlette.responses import Response
from starlette.websockets import WebSocket, WebSocketDisconnect

from balancer.autoscaling import LocalAutoscaler
from balancer.distributed import SharedStateStore
from balancer.ha import SharedClusterState, build_local_ha_payload
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
BALANCER_HOST = os.getenv("BALANCER_HOST", "127.0.0.1")
BALANCER_PORT = int(os.getenv("BALANCER_PORT", "8000"))
BALANCER_INSTANCE_ID = os.getenv("BALANCER_INSTANCE_ID", f"lb-{os.getpid()}")
BALANCER_PRIORITY = int(os.getenv("BALANCER_PRIORITY", "1"))
BALANCER_SHARED_STATE_DIR = os.getenv("BALANCER_SHARED_STATE_DIR", str(Path(__file__).resolve().parents[1] / "ha_state"))
BALANCER_HA_HEARTBEAT_TIMEOUT_SECONDS = float(os.getenv("BALANCER_HA_HEARTBEAT_TIMEOUT_SECONDS", "10.0"))
BALANCER_HA_PUBLISH_INTERVAL_SECONDS = float(os.getenv("BALANCER_HA_PUBLISH_INTERVAL_SECONDS", "2.0"))
BALANCER_PEER_URLS = [
    url.strip().rstrip("/")
    for url in os.getenv("BALANCER_PEER_URLS", "").split(",")
    if url.strip()
]
BALANCER_SESSION_COOKIE = os.getenv("BALANCER_SESSION_COOKIE", "slb_session")
BALANCER_STATE_NAMESPACE = os.getenv("BALANCER_STATE_NAMESPACE", "distributed_state")
BALANCER_SESSION_TTL_SECONDS = float(os.getenv("BALANCER_SESSION_TTL_SECONDS", "1800.0"))
BALANCER_DISCOVERY_INTERVAL_SECONDS = float(os.getenv("BALANCER_DISCOVERY_INTERVAL_SECONDS", "5.0"))
BALANCER_AUTOSCALER_ENABLED = os.getenv("BALANCER_AUTOSCALER_ENABLED", "1") != "0"
BALANCER_AUTOSCALER_MIN_REPLICAS = int(os.getenv("BALANCER_AUTOSCALER_MIN_REPLICAS", "0"))
BALANCER_AUTOSCALER_MAX_REPLICAS = int(os.getenv("BALANCER_AUTOSCALER_MAX_REPLICAS", "3"))
BALANCER_AUTOSCALER_START_PORT = int(os.getenv("BALANCER_AUTOSCALER_START_PORT", "5100"))
BALANCER_AUTOSCALER_SCALE_UP_RPS = float(os.getenv("BALANCER_AUTOSCALER_SCALE_UP_RPS", "8.0"))
BALANCER_AUTOSCALER_SCALE_UP_CONNECTIONS = int(os.getenv("BALANCER_AUTOSCALER_SCALE_UP_CONNECTIONS", "6"))
BALANCER_AUTOSCALER_COOLDOWN_SECONDS = float(os.getenv("BALANCER_AUTOSCALER_COOLDOWN_SECONDS", "20.0"))
BALANCER_AUTOSCALER_IDLE_SECONDS = float(os.getenv("BALANCER_AUTOSCALER_IDLE_SECONDS", "120.0"))

logger = logging.getLogger("balancer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

state = RoutingState(BACKENDS, BACKEND_WEIGHTS)
scheduler = create_scheduler(SCHEDULER_MODE)
rate_limiter = RateLimiter(RATE_LIMIT_MAX_REQUESTS, RATE_LIMIT_WINDOW_SECONDS)
telemetry = TelemetryState(BACKENDS)
cluster_state = SharedClusterState(BALANCER_SHARED_STATE_DIR, BALANCER_HA_HEARTBEAT_TIMEOUT_SECONDS)
shared_state = SharedStateStore(BALANCER_SHARED_STATE_DIR, namespace=BALANCER_STATE_NAMESPACE)
autoscaler = LocalAutoscaler(
    shared_state,
    start_port=BALANCER_AUTOSCALER_START_PORT,
    max_replicas=BALANCER_AUTOSCALER_MAX_REPLICAS,
    min_replicas=BALANCER_AUTOSCALER_MIN_REPLICAS,
    scale_up_requests_per_second=BALANCER_AUTOSCALER_SCALE_UP_RPS,
    scale_up_active_connections=BALANCER_AUTOSCALER_SCALE_UP_CONNECTIONS,
    scale_down_idle_seconds=BALANCER_AUTOSCALER_IDLE_SECONDS,
    cooldown_seconds=BALANCER_AUTOSCALER_COOLDOWN_SECONDS,
) if BALANCER_AUTOSCALER_ENABLED else None
peer_heartbeats: dict[str, dict[str, object]] = {}
peer_heartbeat_lock = asyncio.Lock()
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


async def discovery_loop(interval: float = BALANCER_DISCOVERY_INTERVAL_SECONDS):
    logger.info("Starting discovery loop (interval=%ss)", interval)
    while True:
        try:
            discovered_backends = await shared_state.list_backends()
            discovered_urls = []
            for backend in discovered_backends:
                backend_url = str(backend.get("backend_url", "")).strip().rstrip("/")
                if not backend_url:
                    continue
                discovered_urls.append(backend_url)
                weight = int(backend.get("weight", 1) or 1)
                await state.add_backend(backend_url, weight=weight)
                if bool(backend.get("healthy", True)):
                    await state.record_health(backend_url, True, None)

            for backend_url in list(state.backends):
                if backend_url not in discovered_urls and backend_url not in BACKENDS:
                    await state.remove_backend(backend_url)

            if autoscaler is not None:
                metrics = await telemetry.snapshot(state, rate_limiter)
                summary = metrics.get("summary", {})
                await autoscaler.maybe_scale(
                    requests_per_second=float(summary.get("requests_per_second", 0.0)),
                    active_connections=sum(view.active_connections for view in await state.active_views()),
                    active_backends=len(await state.active_views()),
                )
        except Exception:
            logger.exception("Discovery loop failed")
        await asyncio.sleep(interval)


async def publish_cluster_state_loop(interval: float = BALANCER_HA_PUBLISH_INTERVAL_SECONDS):
    logger.info("Starting cluster state publish loop (interval=%ss)", interval)
    while True:
        try:
            telemetry_snapshot = await telemetry.snapshot(state, rate_limiter)
            async with peer_heartbeat_lock:
                heartbeat_snapshot = dict(peer_heartbeats)
            payload = build_local_ha_payload(
                instance_id=BALANCER_INSTANCE_ID,
                instance_url=f"http://{BALANCER_HOST}:{BALANCER_PORT}",
                priority=BALANCER_PRIORITY,
                health_snapshot=telemetry_snapshot.get("health", {}),
                stats_snapshot=telemetry_snapshot.get("stats", {}),
                telemetry_snapshot=telemetry_snapshot,
                peer_heartbeats=heartbeat_snapshot,
            )
            await cluster_state.publish(BALANCER_INSTANCE_ID, payload)
        except Exception:
            logger.exception("Failed to publish cluster state")
        await asyncio.sleep(interval)


async def monitor_peer_heartbeats(interval: float = HEALTH_CHECK_INTERVAL):
    if not BALANCER_PEER_URLS:
        return

    logger.info("Starting peer heartbeat monitor for %s", ", ".join(BALANCER_PEER_URLS))
    timeout = aiohttp.ClientTimeout(total=max(1.0, interval))
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            for peer_url in BALANCER_PEER_URLS:
                heartbeat_url = peer_url.rstrip("/") + "/ha/heartbeat"
                healthy = False
                details: dict[str, object] = {"peer_url": peer_url, "checked_at": time.time()}
                try:
                    async with session.get(heartbeat_url, timeout=min(1.0, interval)) as response:
                        healthy = 200 <= response.status < 300
                        details["status"] = response.status
                        if healthy:
                            try:
                                details["payload"] = await response.json()
                            except Exception:
                                details["payload"] = None
                except Exception as exc:
                    details["error"] = str(exc)

                details["healthy"] = healthy
                async with peer_heartbeat_lock:
                    previous = peer_heartbeats.get(peer_url, {}).get("healthy")
                    peer_heartbeats[peer_url] = details
                if previous is not None and bool(previous) != healthy:
                    logger.info("Peer %s heartbeat changed to %s", peer_url, "healthy" if healthy else "unhealthy")

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
    discovery_task = asyncio.create_task(discovery_loop())
    publish_task = asyncio.create_task(publish_cluster_state_loop())
    peer_task = asyncio.create_task(monitor_peer_heartbeats()) if BALANCER_PEER_URLS else None
    try:
        yield
    finally:
        monitor_task.cancel()
        discovery_task.cancel()
        publish_task.cancel()
        if peer_task is not None:
            peer_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            logger.info("Monitor task cancelled")
        try:
            await discovery_task
        except asyncio.CancelledError:
            logger.info("Discovery task cancelled")
        try:
            await publish_task
        except asyncio.CancelledError:
            logger.info("Cluster publish task cancelled")
        if peer_task is not None:
            try:
                await peer_task
            except asyncio.CancelledError:
                logger.info("Peer monitor task cancelled")


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
    metrics = await telemetry.snapshot(state, rate_limiter)
    if autoscaler is not None:
        metrics["autoscaling"] = await autoscaler.snapshot()
    metrics["discovery"] = await shared_state.snapshot()
    return metrics


@app.get("/ha/heartbeat")
async def ha_heartbeat():
    return {
        "instance_id": BALANCER_INSTANCE_ID,
        "instance_url": f"http://{BALANCER_HOST}:{BALANCER_PORT}",
        "priority": BALANCER_PRIORITY,
        "heartbeat_at": time.time(),
        "state_dir": BALANCER_SHARED_STATE_DIR,
    }


@app.get("/ha/status")
async def ha_status():
    cluster_snapshot = await cluster_state.snapshot()
    async with peer_heartbeat_lock:
        heartbeat_snapshot = dict(peer_heartbeats)
    return {
        "instance_id": BALANCER_INSTANCE_ID,
        "instance_url": f"http://{BALANCER_HOST}:{BALANCER_PORT}",
        "priority": BALANCER_PRIORITY,
        "cluster": cluster_snapshot,
        "peer_heartbeats": heartbeat_snapshot,
    }


@app.get("/api/sessions")
async def api_sessions():
    return await shared_state.snapshot()


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    if not DASHBOARD_FILE.exists():
        raise HTTPException(status_code=500, detail="Dashboard UI is missing")
    return DASHBOARD_FILE.read_text(encoding="utf-8")


def _session_id_from_headers_and_cookies(headers, cookies) -> tuple[str | None, bool]:
    cookie_session = cookies.get(BALANCER_SESSION_COOKIE) if cookies is not None else None
    if cookie_session:
        return cookie_session.strip(), False
    header_session = headers.get("x-session-id") if headers is not None else None
    if header_session:
        return header_session.strip(), False
    return f"{BALANCER_INSTANCE_ID}-{uuid.uuid4().hex}", True


async def _resolve_sticky_backend(request: Request, candidate_backends: list[str]) -> str | None:
    session_id, _ = _session_id_from_headers_and_cookies(request.headers, request.cookies)
    if session_id:
        bound_backend = await shared_state.resolve_session(session_id)
        if bound_backend and bound_backend in candidate_backends:
            return bound_backend

    if not candidate_backends:
        return None

    chosen_backend = await scheduler.select_backend(state)
    if session_id:
        await shared_state.bind_session(session_id, chosen_backend, ttl_seconds=BALANCER_SESSION_TTL_SECONDS)
    return chosen_backend


async def proxy_websocket_to_backend(client_socket: WebSocket, path: str):
    backend = None
    upstream_socket = None
    upstream_session = None
    try:
        candidate_backends = [view.backend for view in await state.active_views()]
        session_id, _ = _session_id_from_headers_and_cookies(client_socket.headers, client_socket.cookies)
        if session_id:
            bound_backend = await shared_state.resolve_session(session_id)
            if bound_backend and bound_backend in candidate_backends:
                backend = bound_backend
        if backend is None and candidate_backends:
            backend = await scheduler.select_backend(state)
            if session_id:
                await shared_state.bind_session(session_id, backend, ttl_seconds=BALANCER_SESSION_TTL_SECONDS)
        if backend is None:
            await client_socket.close(code=1013)
            return

        await client_socket.accept()
        upstream_url = backend.replace("http://", "ws://").replace("https://", "wss://") + f"/{path}"
        upstream_session = aiohttp.ClientSession()
        upstream_socket = await upstream_session.ws_connect(upstream_url)

        async def relay_client_to_backend():
            while True:
                message = await client_socket.receive()
                if message.get("type") == "websocket.disconnect":
                    break
                text = message.get("text")
                if text is not None:
                    await upstream_socket.send_str(text)

        async def relay_backend_to_client():
            async for message in upstream_socket:
                if message.type == aiohttp.WSMsgType.TEXT:
                    await client_socket.send_text(message.data)
                elif message.type in {aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
                    break

        await asyncio.gather(relay_client_to_backend(), relay_backend_to_client())
    except WebSocketDisconnect:
        return
    finally:
        if upstream_socket is not None:
            await upstream_socket.close()
        if upstream_session is not None:
            await upstream_session.close()


@app.websocket("/ws")
async def websocket_root(websocket: WebSocket):
    await proxy_websocket_to_backend(websocket, "ws")


@app.websocket("/ws/{path:path}")
async def websocket_proxy(path: str, websocket: WebSocket):
    await proxy_websocket_to_backend(websocket, path)


async def proxy_to_backend(request: Request, path: str):
    backend = None
    client_ip = get_client_ip(request)
    start = time.perf_counter()
    upstream_status = 502
    cache_key = None
    session_id, session_created = _session_id_from_headers_and_cookies(request.headers, request.cookies)
    try:
        # Only cache GET responses for idempotent fetches
        if request.method.upper() == "GET":
            cache_key = request.url.path + ("?" + request.url.query if request.url.query else "")
            entry = await cache.get(cache_key)
            if entry is not None:
                await telemetry.record_cache_hit(client_ip, path)
                # Return cached response
                cached_headers = dict(entry.headers)
                if session_id and (session_created or BALANCER_SESSION_COOKIE not in request.cookies):
                    cached_headers.setdefault("Set-Cookie", f"{BALANCER_SESSION_COOKIE}={session_id}; Path=/; HttpOnly; SameSite=Lax")
                return Response(content=entry.content, status_code=200, headers=cached_headers, media_type=entry.media_type)
            else:
                await telemetry.record_cache_miss(client_ip, path)
        candidate_backends = [view.backend for view in await state.active_views()]
        backend = await _resolve_sticky_backend(request, candidate_backends)
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
                    if session_id and (session_created or BALANCER_SESSION_COOKIE not in request.cookies):
                        response_headers.setdefault("Set-Cookie", f"{BALANCER_SESSION_COOKIE}={session_id}; Path=/; HttpOnly; SameSite=Lax")
                    # Store GET responses in cache when status is 200
                    if request.method.upper() == "GET" and upstream.status == 200 and cache_key is not None:
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
    if path in {"", "health", "stats", "scheduler", "dashboard", "metrics", "api/metrics", "ha/heartbeat", "ha/status"} or path.startswith("ha/"):
        raise HTTPException(status_code=404, detail="Not found")
    return await proxy_to_backend(request, path)


if __name__ == "__main__":
    uvicorn.run(app, host=BALANCER_HOST, port=BALANCER_PORT)
