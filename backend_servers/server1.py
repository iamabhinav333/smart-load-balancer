"""
Backend Server 1 - Running on port 8001
"""
from fastapi import FastAPI
import uvicorn

app = FastAPI()

@app.get("/")
def read_root():
    return {
        "message": "Hello from Server 1",
        "server_id": 1,
        "port": 8001
    }

@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "server": "Server 1"
    }

@app.get("/api/data")
def get_data():
    return {
        "data": "Response from Server 1",
        "server_id": 1
    }

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8001)
