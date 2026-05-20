"""
Backend Server 2 - Running on port 5002
"""

from fastapi import FastAPI
import uvicorn


app = FastAPI()


@app.get("/")
def read_root():
    return {
        "message": "Hello from Server 2",
        "server_id": 2,
        "port": 5002
    }


@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "server": "Server 2"
    }


@app.get("/api/data")
def get_data():
    return {
        "data": "Response from Server 2",
        "server_id": 2
    }


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=5002)
