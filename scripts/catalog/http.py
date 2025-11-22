import asyncio, random
from contextlib import asynccontextmanager
import httpx
from aiolimiter import AsyncLimiter

RETRYABLE = {408, 425, 429, 500, 502, 503, 504}

@asynccontextmanager
async def make_client(*, timeout: float = 30.0):
   async with httpx.AsyncClient(http2=True, timeout=timeout, headers={
      "User-Agent": "game-catalog (contact: maintainer@example.com)",
      "Accept": "application/json, text/html;q=0.9,*/*;q=0.8",
   }) as client:
      yield client

class DomainLimiter:
   def __init__(self, rps: float):
      self.limiter = AsyncLimiter(rps, time_period=1)

   async def wait(self):
      await self.limiter.acquire()

async def fetch(client: httpx.AsyncClient, method: str, url: str, *,
                params=None, headers=None, json=None, data=None,
                limiter: DomainLimiter | None = None,
                max_retries: int = 5,
                retry_429_wait: float | None = None) -> httpx.Response:
   attempt = 0
   while True:
      if limiter:
         await limiter.wait()
      try:
         r = await client.request(
            method,
            url,
            params=params,
            headers=headers,
            json=json,
            data=data,
            follow_redirects=True,
         )
         if r.status_code in RETRYABLE:
            attempt += 1
            if attempt > max_retries:
               r.raise_for_status()
               return r
            wait = min(8.0, (2 ** (attempt - 1)) * 0.5 + random.random() * 0.3)
            if r.status_code == 429 and retry_429_wait is not None:
               wait = retry_429_wait
            else:
               ra = r.headers.get("Retry-After")
               if ra:
                  try: wait = float(ra)
                  except: ...
            await asyncio.sleep(wait)
            continue
         r.raise_for_status()
         return r
      except (
         httpx.ReadTimeout,
         httpx.ConnectTimeout,
         httpx.RemoteProtocolError,
         httpx.LocalProtocolError,
         httpx.ReadError,
      ):
         attempt += 1
         if attempt > max_retries:
            raise
         await asyncio.sleep(min(8.0, (2 ** (attempt - 1)) * 0.5 + random.random() * 0.2))
