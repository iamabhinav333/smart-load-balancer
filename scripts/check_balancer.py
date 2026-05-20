import asyncio
import aiohttp


async def check(url):
    timeout = aiohttp.ClientTimeout(total=3.0)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            async with session.get(url) as r:
                data = await r.json()
                print(f"{url} {r.status} {data}")
        except Exception as e:
            print(f"{url} ERROR {e}")


async def main():
    await check('http://127.0.0.1:8000/health')
    await check('http://127.0.0.1:8000/api/data')
    await check('http://127.0.0.1:8000/api/data')
    await check('http://127.0.0.1:8000/api/data')


if __name__ == '__main__':
    asyncio.run(main())
