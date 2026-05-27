import asyncio

import aiohttp


BASE_URL = "http://127.0.0.1:8000"


async def get_json(session: aiohttp.ClientSession, path: str):
    async with session.get(f"{BASE_URL}{path}") as resp:
        return resp.status, await resp.json()


async def main():
    timeout = aiohttp.ClientTimeout(total=10.0)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for path in ("/scheduler", "/health", "/stats"):
            status, data = await get_json(session, path)
            print(path, status, data)

        # Concurrent batch to exercise least connections under load.
        requests = [get_json(session, "/api/data?delay=1") for _ in range(6)]
        results = await asyncio.gather(*requests)
        print("concurrent_batch", results)

        status, stats = await get_json(session, "/stats")
        print("/stats", status, stats)


if __name__ == "__main__":
    asyncio.run(main())
