from fastapi import FastAPI, HTTPException, Request
import asyncio
import aiohttp
import uvicorn
from starlette.responses import Response

# List of backend servers (change as needed)
BACKENDS = [
    "http://127.0.0.1:5001",
    "http://127.0.0.1:5002",
]

_index = 0
_lock = asyncio.Lock()

app = FastAPI(title="Smart Load Balancer")


async def get_next_backend():
    global _index
    async with _lock:
        if not BACKENDS:
            raise RuntimeError("no backends configured")
        b = BACKENDS[_index % len(BACKENDS)]
        _index += 1
        return b


@app.get("/")
async def root():
    return {"message": "Smart Load Balancer", "backends": BACKENDS}


@app.get("/health")
async def health():
    results = []
    timeout = aiohttp.ClientTimeout(total=1.0)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [session.get(f"{b}/health") for b in BACKENDS]
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        for b, resp in zip(BACKENDS, responses):
            if isinstance(resp, Exception):
                results.append({"backend": b, "status": "unreachable", "error": str(resp)})
            else:
                try:
                    info = await resp.json()
                except Exception:
                    info = None
                results.append({"backend": b, "status": "healthy", "info": info})
                await resp.release()
    return {"status": "ok", "backends": results}


@app.get("/api/data")
async def proxy_data():
    backend = await get_next_backend()
    timeout = aiohttp.ClientTimeout(total=3.0)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            async with session.get(f"{backend}/api/data") as r:
                return await r.json()
        except aiohttp.ClientError as e:
            raise HTTPException(status_code=502, detail=str(e))




# Generic proxy for dynamic paths (search, profile, feed, etc.)
@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"])
async def proxy(path: str, request: Request):
    # don't proxy root or health (handled above)
    if path in ("", "health"):
        raise HTTPException(status_code=404, detail="Not found")

    backend = await get_next_backend()
    url = backend.rstrip("/") + "/" + path
    qs = request.url.query
    if qs:
        url = f"{url}?{qs}"

    headers = {k: v for k, v in request.headers.items()}
    headers.pop("host", None)

    timeout = aiohttp.ClientTimeout(total=30.0)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            body = await request.body()
            async with session.request(request.method, url, headers=headers, data=body) as resp:
                content = await resp.read()
                resp_headers = dict(resp.headers)
                # remove hop-by-hop headers
                for h in ("Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "TE", "Trailers", "Transfer-Encoding", "Upgrade"):
                    resp_headers.pop(h, None)
                media_type = resp_headers.get("Content-Type")
                return Response(content=content, status_code=resp.status, headers=resp_headers, media_type=media_type)
        except aiohttp.ClientError as e:
            raise HTTPException(status_code=502, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)