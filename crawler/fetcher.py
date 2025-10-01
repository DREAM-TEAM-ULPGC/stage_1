import asyncio,ssl,contextlib,aiohttp,time
from utils import is_http_url
MAX_RETRIES=2; CONNECT_TIMEOUT=15; READ_TIMEOUT=25
ALLOWED=("text/html","application/xhtml+xml")

class Fetcher:
    def __init__(self,s,ua,robots,polite): self.s=s; self.ua=ua; self.r=robots; self.p=polite
    async def fetch(self,u):
        rp,d=await self.r.get(u)
        if d is not None: self.p.set_delay(u.split("/")[2].lower(),d)
        if not rp.can_fetch(self.ua,u): return None,None,None,0
        await self.p.wait_turn(u.split("/")[2].lower())
        for _ in range(MAX_RETRIES+1):
            try:
                t=aiohttp.ClientTimeout(connect=CONNECT_TIMEOUT,total=CONNECT_TIMEOUT+READ_TIMEOUT)
                async with self.s.get(u,headers={"User-Agent":self.ua},timeout=t,ssl=ssl.SSLContext()) as r:
                    st=r.status; ct=r.headers.get("Content-Type","").lower()
                    if not any(a in ct for a in ALLOWED):
                        with contextlib.suppress(Exception): await r.read()
                        return st,ct,None,0
                    tx=await r.text(errors="ignore"); return st,ct,tx,len(tx.encode())
            except Exception as e: await asyncio.sleep(0.5)
        return None,None,None,0
