from __future__ import annotations

from contextlib import asynccontextmanager
import logging
import os
from pathlib import Path

import aiohttp
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
import uvicorn
from starlette.responses import Response

from balancer.ha import SharedClusterState, choose_active_instance


logger = logging.getLogger("balancer.ha_gateway")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

ROOT_DIR = Path(__file__).resolve().parents[1]
GATEWAY_HOST = os.getenv("HA_GATEWAY_HOST", "127.0.0.1")
GATEWAY_PORT = int(os.getenv("HA_GATEWAY_PORT", "7999"))
STATE_DIR = os.getenv("BALANCER_SHARED_STATE_DIR", str(ROOT_DIR / "ha_state"))
HEARTBEAT_TIMEOUT = float(os.getenv("BALANCER_HA_HEARTBEAT_TIMEOUT_SECONDS", "10.0"))
FALLBACK_BALANCERS = [
    url.strip().rstrip("/")
    for url in os.getenv("HA_BALANCER_URLS", "http://127.0.0.1:8000,http://127.0.0.1:8001").split(",")
    if url.strip()
]

cluster_state = SharedClusterState(STATE_DIR, HEARTBEAT_TIMEOUT)


async def probe(url: str, timeout_seconds: float = 1.0) -> tuple[str, bool, dict[str, object] | None]:
    heartbeat_url = url.rstrip("/") + "/ha/heartbeat"
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(heartbeat_url) as response:
                if 200 <= response.status < 300:
                    try:
                        payload = await response.json()
                    except Exception:
                        payload = None
                    return url, True, payload
                return url, False, {"status": response.status}
    except Exception as exc:
        return url, False, {"error": str(exc)}


async def select_balancer_url() -> str | None:
    snapshot = await cluster_state.snapshot()
    selected = choose_active_instance(snapshot, FALLBACK_BALANCERS)
    if selected is not None:
        _, healthy, _ = await probe(selected, timeout_seconds=1.0)
        if healthy:
            return selected

    for candidate in FALLBACK_BALANCERS:
        _, healthy, _ = await probe(candidate, timeout_seconds=1.0)
        if healthy:
            return candidate

    return selected


app = FastAPI(title="Smart Load Balancer HA Gateway")


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app.router.lifespan_context = lifespan


@app.get("/health")
async def health():
    active = await select_balancer_url()
    return {
        "status": "ok" if active else "degraded",
        "active_balancer": active,
        "fallback_balancers": FALLBACK_BALANCERS,
        "state_dir": STATE_DIR,
    }


@app.get("/api/cluster")
async def api_cluster():
    return await cluster_state.snapshot()


@app.get("/")
async def root():
    active = await select_balancer_url()
    return {
        "message": "Smart Load Balancer HA Gateway",
        "active_balancer": active,
        "balancers": FALLBACK_BALANCERS,
        "cluster": "/api/cluster",
    }


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"])
async def proxy(path: str, request: Request):
    if path in {"health", "api/cluster"}:
        raise HTTPException(status_code=404, detail="Not found")

    target = await select_balancer_url()
    if target is None:
        raise HTTPException(status_code=503, detail="No healthy balancer available")

    forward_url = target.rstrip("/") + "/" + path
    if request.url.query:
        forward_url = f"{forward_url}?{request.url.query}"

    headers = {key: value for key, value in request.headers.items() if key.lower() != "host"}
    timeout = aiohttp.ClientTimeout(total=30.0)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            body = await request.body()
            async with session.request(request.method, forward_url, headers=headers, data=body) as upstream:
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
            logger.warning("HA gateway proxy error for %s: %s", target, exc)
            raise HTTPException(status_code=502, detail=str(exc))


if __name__ == "__main__":
    uvicorn.run(app, host=GATEWAY_HOST, port=GATEWAY_PORT)