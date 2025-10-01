import asyncio, time, argparse, sys, aiohttp, re, os
from pathlib import Path
from utils import normalize_url, same_reg_domain
from link_extractor import extract_links
from robots_cache import RobotsCache
from polite_limiter import PoliteLimiter
from fetcher import Fetcher
from writer import JsonWriter

USER_AGENT = "IR-Crawler/1.0 (+https://example.edu)"
DEFAULT_CONCURRENCY = 16
START_MARKER = "*** START OF THE PROJECT GUTENBERG EBOOK"
END_MARKER = "*** END OF THE PROJECT GUTENBERG EBOOK"

class BookCrawler:
    def __init__(self, seeds, out, max_pages=1000, conc=DEFAULT_CONCURRENCY, same_domain=False, allowed=None, ua=USER_AGENT, dl_root="datalake"):
        self.seeds = [normalize_url(u) for u in seeds]
        self.out = out
        self.m = max_pages
        self.c = max(1, conc)
        self.sd = same_domain
        self.ad = set(d.lower() for d in (allowed or []))
        self.ua = ua
        self.f = asyncio.Queue()
        self.s = set()
        self.n = 0
        self.dom = self.seeds and self.seeds[0].split("/")[2] or None
        self.dl_root = Path(dl_root)

    def _allowed(self, u):
        if self.sd and self.dom and not same_reg_domain(u, "https://" + self.dom): return False
        if self.ad and (u.split("/")[2].lower() not in self.ad): return False
        return True

    async def worker(self):
        while self.n < self.m:
            try:
                u, d = await asyncio.wait_for(self.f.get(), 1)
            except asyncio.TimeoutError:
                if self.f.empty(): return
                continue
            if u in self.s: self.f.task_done(); continue
            self.s.add(u)
            if not self._allowed(u): self.f.task_done(); continue
            st, ct, ht, sz = await self.fetcher.fetch(u)
            if ht and st and 200 <= st < 400:
                # extraer info de libro
                if START_MARKER in ht and END_MARKER in ht:
                    header, body_and_footer = ht.split(START_MARKER, 1)
                    body, footer = body_and_footer.split(END_MARKER, 1)
                    book_id_match = re.search(r'/(\d+)/pg(\d+)\.txt', u)
                    book_id = book_id_match.group(2) if book_id_match else str(self.n)
                    # guardar en datalake
                    dt = time.localtime()
                    path_dir = self.dl_root / f"{dt.tm_year:04d}{dt.tm_mon:02d}{dt.tm_mday:02d}" / f"{dt.tm_hour:02d}"
                    path_dir.mkdir(parents=True, exist_ok=True)
                    (path_dir / f"{book_id}.header.txt").write_text(header.strip(), encoding="utf-8")
                    (path_dir / f"{book_id}.body.txt").write_text(body.strip(), encoding="utf-8")
                    r = {"url": u, "depth": d, "status": st, "content_type": ct, "bytes": sz, "fetched_at": int(time.time()),
                         "book_id": book_id, "file_type": "txt", "out_links": extract_links(u, ht)}
                    self.n += 1
                    for link in r["out_links"]:
                        if link not in self.s: await self.f.put((link, d+1))
                    self.fp.write(r)
            self.f.task_done()

    async def run(self):
        conn = aiohttp.TCPConnector(limit_per_host=1, ttl_dns_cache=300)
        async with aiohttp.ClientSession(connector=conn) as s:
            self.rob = RobotsCache(s, self.ua)
            self.pol = PoliteLimiter()
            self.fetcher = Fetcher(s, self.ua, self.rob, self.pol)
            for se in self.seeds: await self.f.put((se, 0))
            self.fp = JsonWriter(self.out, mode="json")
            ws = [asyncio.create_task(self.worker()) for _ in range(self.c)]
            await asyncio.gather(*ws, return_exceptions=True)
            self.fp.close()

    @classmethod
    def cli(cls):
        p = argparse.ArgumentParser()
        p.add_argument("--seeds", nargs="+", required=True)
        p.add_argument("--out", required=True)
        p.add_argument("--max-pages", type=int, default=300)
        p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
        p.add_argument("--same-domain", action="store_true")
        p.add_argument("--allowed-domains", nargs="*", default=None)
        p.add_argument("--user-agent", default=USER_AGENT)
        args = p.parse_args()
        c = cls(args.seeds, args.out, args.max_pages, args.concurrency, args.same_domain, args.allowed_domains, args.user_agent)
        try: asyncio.run(c.run())
        except KeyboardInterrupt: print("Interrupted", file=sys.stderr)
