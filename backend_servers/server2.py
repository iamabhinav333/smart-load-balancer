"""
Backend Server 2 - Running on port 5002
"""

from fastapi import FastAPI
import uvicorn


app = FastAPI()


@app.get("/")
async def read_root():
    return {
        "message": "Hello from Server 2",
        "server_id": 2,
        "port": 5002
    }


@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "server": "Server 2"
    }


@app.get("/api/data")
async def get_data():
    return {
        "data": "Response from Server 2",
        "server_id": 2
    }


@app.get("/search")
async def search(q: str = ""):
    return {"server": "Server 2", "path": "search", "q": q}


@app.get("/profile")
async def profile(id: int | None = None):
    return {"server": "Server 2", "path": "profile", "id": id}


@app.get("/feed")
async def feed():
    return {"server": "Server 2", "path": "feed", "items": ["a", "b"]}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=5002)
