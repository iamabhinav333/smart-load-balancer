from __future__ import annotations

import asyncio
import os
from pathlib import Path
import time

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import uvicorn

from balancer.distributed import SharedStateStore


BACKEND_INSTANCE_ID = os.getenv("BACKEND_INSTANCE_ID", "replica")
BACKEND_LABEL = os.getenv("BACKEND_LABEL", BACKEND_INSTANCE_ID)
BACKEND_PORT = int(os.getenv("BACKEND_PORT", "5100"))
BACKEND_HOST = os.getenv("BACKEND_HOST", "127.0.0.1")
BACKEND_SOURCE = os.getenv("BACKEND_SOURCE", "autoscaled")
BACKEND_SHARED_STATE_DIR = os.getenv("BACKEND_SHARED_STATE_DIR", str(Path(__file__).resolve().parents[1] / "ha_state"))
BACKEND_WEBSOCKET_ENABLED = os.getenv("BACKEND_WEBSOCKET_ENABLED", "1") != "0"

STORE = SharedStateStore(BACKEND_SHARED_STATE_DIR, namespace="distributed_state")

app = FastAPI(title=f"{BACKEND_LABEL}")


async def heartbeat_loop() -> None:
    backend_url = f"http://{BACKEND_HOST}:{BACKEND_PORT}"
    while True:
        try:
            await STORE.touch_backend(
                backend_url,
                {
                    "backend_id": BACKEND_INSTANCE_ID,
                    "kind": "autoscaled",
                    "source": BACKEND_SOURCE,
                    "port": BACKEND_PORT,
                    "label": BACKEND_LABEL,
                    "healthy": True,
                    "websocket_enabled": BACKEND_WEBSOCKET_ENABLED,
                },
            )
        except Exception:
            pass
        await asyncio.sleep(2.5)


@app.on_event("startup")
async def on_startup() -> None:
    await STORE.register_backend(
        f"http://{BACKEND_HOST}:{BACKEND_PORT}",
        {
            "backend_id": BACKEND_INSTANCE_ID,
            "kind": "autoscaled",
            "source": BACKEND_SOURCE,
            "port": BACKEND_PORT,
            "label": BACKEND_LABEL,
            "healthy": True,
            "websocket_enabled": BACKEND_WEBSOCKET_ENABLED,
            "created_at": time.time(),
        },
    )
    asyncio.create_task(heartbeat_loop())


@app.get("/")
async def read_root():
    return {
        "message": f"Hello from {BACKEND_LABEL}",
        "backend_id": BACKEND_INSTANCE_ID,
        "port": BACKEND_PORT,
        "source": BACKEND_SOURCE,
    }


@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "backend": BACKEND_LABEL,
        "backend_id": BACKEND_INSTANCE_ID,
        "port": BACKEND_PORT,
    }


@app.get("/api/data")
async def get_data(delay: float = 0.0):
    if delay > 0:
        await asyncio.sleep(delay)
    return {
        "data": f"Response from {BACKEND_LABEL}",
        "backend_id": BACKEND_INSTANCE_ID,
        "port": BACKEND_PORT,
        "source": BACKEND_SOURCE,
    }


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            payload = await websocket.receive_text()
            await websocket.send_text(f"{BACKEND_LABEL}:{payload}")
    except WebSocketDisconnect:
        return


if __name__ == "__main__":
    uvicorn.run(app, host=BACKEND_HOST, port=BACKEND_PORT)
