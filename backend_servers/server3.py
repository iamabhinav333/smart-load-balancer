"""
Backend Server 3 - Running on port 5003
"""

import asyncio

from fastapi import FastAPI
import uvicorn


app = FastAPI()


@app.get("/")
async def read_root():
    return {
        "message": "Hello from Server 3",
        "server_id": 3,
        "port": 5003,
    }


@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "server": "Server 3",
    }


@app.get("/api/data")
async def get_data(delay: float = 0.0):
    if delay > 0:
        await asyncio.sleep(delay)
    return {
        "data": "Response from Server 3",
        "server_id": 3,
    }


@app.get("/search")
async def search(q: str = ""):
    return {"server": "Server 3", "path": "search", "q": q}


@app.get("/profile")
async def profile(id: int | None = None):
    return {"server": "Server 3", "path": "profile", "id": id}


@app.get("/feed")
async def feed():
    return {"server": "Server 3", "path": "feed", "items": ["x", "y", "z"]}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=5003)
