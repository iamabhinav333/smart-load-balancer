from __future__ import annotations

from contextlib import asynccontextmanager
import asyncio
import logging
import os
import time

import aiohttp
from fastapi import FastAPI, HTTPException, Request
import uvicorn
from starlette.responses import Response

from balancer.schedulers.factory import available_scheduler_modes, create_scheduler
from balancer.state import RoutingState


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

logger = logging.getLogger("balancer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

state = RoutingState(BACKENDS, BACKEND_WEIGHTS)
scheduler = create_scheduler(SCHEDULER_MODE)


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


app = FastAPI(title="Smart Load Balancer", lifespan=lifespan)


@app.get("/")
async def root():
    return {
        "message": "Smart Load Balancer",
        "scheduler": scheduler.name,
        "available_schedulers": available_scheduler_modes(),
        "backends": BACKENDS,
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


async def proxy_to_backend(request: Request, path: str):
    backend = None
    start = time.perf_counter()
    try:
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
                    return Response(content=content, status_code=upstream.status, headers=response_headers, media_type=media_type)
            except aiohttp.ClientError as exc:
                logger.warning("Proxy error for backend %s: %s", backend, exc)
                raise HTTPException(status_code=502, detail=str(exc))
    finally:
        if backend is not None:
            latency = time.perf_counter() - start
            await state.finish_request(backend, latency)


@app.get("/api/data")
async def api_data(request: Request):
    return await proxy_to_backend(request, "api/data")


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"])
async def proxy(path: str, request: Request):
    if path in {"", "health", "stats", "scheduler"}:
        raise HTTPException(status_code=404, detail="Not found")
    return await proxy_to_backend(request, path)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
