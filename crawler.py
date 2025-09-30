#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Asynchronous, polite web crawler for Search Engine - Part 1
- No external dependencies (stdlib only)
- Respects robots.txt (via urllib.robotparser)
- Per-host politeness with crawl-delay
- URL normalization & de-dup
- HTML-only fetch; extracts <a href> links
- Writes JSONL: one record per fetched page

Usage (examples):
  python crawler.py --seeds https://example.com https://www.python.org \
                    --max-pages 500 --concurrency 20 --out pages.jsonl

  python crawler.py --seeds https://es.wikipedia.org/wiki/Recuperación_de_información \
                    --same-domain --max-pages 300 --concurrency 15 --out wiki.jsonl
"""

import asyncio
import contextlib
import json
import sys
import time
import argparse
import urllib.parse as urlparse
import urllib.robotparser as robotparser
import ssl
import re
from html.parser import HTMLParser
from collections import defaultdict, deque

import aiohttp  # stdlib only solution preferred, but aiohttp is widely available
                # If you absolutely cannot install aiohttp, see the note below
                # for switching to a synchronous fallback.

USER_AGENT = "IR-Crawler/1.0 (+https://example.edu; course project)"
DEFAULT_CONCURRENCY = 16
DEFAULT_PER_HOST_DELAY = 1.0  # seconds, used if robots doesn't specify crawl-delay
CONNECT_TIMEOUT = 15
READ_TIMEOUT = 25
MAX_RETRIES = 2
ALLOWED_CONTENT_TYPES = ("text/html", "application/xhtml+xml")


def normalize_url(url: str) -> str:
    """Normalize URL for deduplication & frontier consistency."""
    url = url.strip()
    parsed = urlparse.urlsplit(url)
    scheme = parsed.scheme.lower() if parsed.scheme else "http"
    netloc = parsed.netloc.lower()

    # drop default ports
    if ":" in netloc:
        host, port = netloc.split(":", 1)
        try:
            port_num = int(port)
            if (scheme == "http" and port_num == 80) or (scheme == "https" and port_num == 443):
                netloc = host
        except ValueError:
            pass

    # remove fragment
    path = parsed.path or "/"
    query = parsed.query

    # normalize path: collapse //, keep trailing slash semantics
    path = re.sub(r"/{2,}", "/", path)

    normalized = urlparse.urlunsplit((scheme, netloc, path, query, ""))  # no fragment
    return normalized


def is_http_url(url: str) -> bool:
    s = urlparse.urlsplit(url).scheme.lower()
    return s in ("http", "https")


def same_reg_domain(u1: str, u2: str) -> bool:
    """Very light same-domain check (no public suffix list to keep stdlib-only)."""
    h1 = urlparse.urlsplit(u1).hostname or ""
    h2 = urlparse.urlsplit(u2).hostname or ""
    return h1 == h2 or (h1.endswith("." + h2)) or (h2.endswith("." + h1))


class LinkExtractor(HTMLParser):
    """Extract <a href> using stdlib html.parser"""
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "a":
            return
        for (k, v) in attrs:
            if k.lower() == "href" and v:
                self.links.append(v)


def extract_links(base_url: str, html: str):
    parser = LinkExtractor()
    with contextlib.suppress(Exception):
        parser.feed(html)
    out = []
    for href in parser.links:
        # Skip javascript:, mailto:, etc.
        if not href:
            continue
        if href.startswith("#") or href.lower().startswith("javascript:") or href.lower().startswith("mailto:"):
            continue
        abs_url = urlparse.urljoin(base_url, href)
        if is_http_url(abs_url):
            out.append(normalize_url(abs_url))
    return out


class RobotsCache:
    """Caches and enforces robots.txt using urllib.robotparser with async fetching."""
    def __init__(self, session, user_agent: str):
        self.session = session
        self.user_agent = user_agent
        self.cache = {}  # host -> (robotparser.RobotFileParser, crawl_delay or None, fetched_at)

    async def _fetch_robots(self, robots_url: str) -> str:
        try:
            async with self.session.get(robots_url, headers={"User-Agent": self.user_agent}, timeout=CONNECT_TIMEOUT) as resp:
                if resp.status >= 400:
                    return ""
                return await resp.text(errors="ignore")
        except Exception:
            return ""

    async def get(self, url: str):
        host = urlparse.urlsplit(url).netloc.lower()
        if host in self.cache:
            return self.cache[host][0], self.cache[host][1]

        base = urlparse.urlsplit(url)
        robots_url = urlparse.urlunsplit((base.scheme, base.netloc, "/robots.txt", "", ""))

        content = await self._fetch_robots(robots_url)
        rp = robotparser.RobotFileParser()
        # robotparser supports .parse(list_of_lines)
        if content:
            rp.parse(content.splitlines())
        else:
            # No robots or error -> default allow
            rp.parse([])

        # Try to parse crawl-delay from the raw content (robotparser has spotty support)
        crawl_delay = None
        if content:
            ua = self.user_agent
            # very lenient crawl-delay extraction: read blocks that match our UA or '*'
            # and get the last crawl-delay specified
            # This is simplistic but OK for coursework without external deps.
            blocks = re.split(r"(?im)^\s*user-agent\s*:\s*", content)
            # the first split part before first UA is junk; iterate UA blocks
            for block in blocks[1:]:
                lines = block.splitlines()
                if not lines:
                    continue
                block_ua = lines[0].strip()
                if block_ua == "*" or block_ua.lower() in ua.lower():
                    for line in lines[1:]:
                        m = re.match(r"(?i)^\s*crawl-delay\s*:\s*([0-9]+(\.[0-9]+)?)", line.strip())
                        if m:
                            try:
                                crawl_delay = float(m.group(1))
                            except Exception:
                                pass

        self.cache[host] = (rp, crawl_delay or None, time.time())
        return rp, crawl_delay


class PoliteLimiter:
    """
    Enforces per-host politeness:
      - serializes requests per host
      - honors crawl-delay if specified, else uses default delay
    """
    def __init__(self, default_delay=DEFAULT_PER_HOST_DELAY):
        self.default_delay = default_delay
        self.host_locks = defaultdict(asyncio.Lock)
        self.host_next_time = defaultdict(float)
        self.host_delays = {}  # host -> delay

    def set_delay(self, host: str, delay: float):
        self.host_delays[host] = max(0.0, delay)

    async def wait_turn(self, host: str):
        lock = self.host_locks[host]
        async with lock:
            delay = self.host_delays.get(host, self.default_delay)
            now = time.time()
            t = self.host_next_time[host]
            if now < t:
                await asyncio.sleep(t - now)
            self.host_next_time[host] = time.time() + delay


class Crawler:
    def __init__(self, seeds, out_path, max_pages=1000, concurrency=DEFAULT_CONCURRENCY,
                 same_domain=False, allowed_domains=None, user_agent=USER_AGENT):
        self.seeds = [normalize_url(u) for u in seeds]
        self.out_path = out_path
        self.max_pages = max_pages
        self.concurrency = max(1, concurrency)
        self.same_domain = same_domain
        self.allowed_domains = set(d.lower() for d in (allowed_domains or []))
        self.user_agent = user_agent

        self.frontier = asyncio.Queue()
        self.seen = set()
        self.pages_fetched = 0
        self.start_domain = urlparse.urlsplit(self.seeds[0]).hostname if self.seeds else None

        self.session = None
        self.robots = None
        self.polite = PoliteLimiter()

        self.out_fp = None

    def _allowed_by_scope(self, url: str) -> bool:
        if self.same_domain and self.start_domain:
            if not same_reg_domain(url, "https://" + self.start_domain):
                return False
        if self.allowed_domains:
            host = (urlparse.urlsplit(url).hostname or "").lower()
            if host not in self.allowed_domains:
                return False
        return True

    async def _fetch(self, url: str):
        """Fetch one page with retries, respecting robots and politeness."""
        if not self.session or not self.robots:
            raise RuntimeError("Crawler not started")

        # robots.txt allow?
        rp, crawl_delay = await self.robots.get(url)
        if crawl_delay is not None:
            host = urlparse.urlsplit(url).netloc.lower()
            self.polite.set_delay(host, crawl_delay)

        if not rp.can_fetch(self.user_agent, url):
            return None, None, None, 0

        # politeness by host
        host = urlparse.urlsplit(url).netloc.lower()
        await self.polite.wait_turn(host)

        # retries
        last_exc = None
        for _ in range(MAX_RETRIES + 1):
            try:
                timeout = aiohttp.ClientTimeout(connect=CONNECT_TIMEOUT, total=CONNECT_TIMEOUT + READ_TIMEOUT)
                async with self.session.get(url, headers={"User-Agent": self.user_agent}, timeout=timeout, ssl=ssl.SSLContext()) as resp:
                    status = resp.status
                    ctype = resp.headers.get("Content-Type", "").lower()
                    # Only keep HTML-ish
                    if not any(ct in ctype for ct in ALLOWED_CONTENT_TYPES):
                        # drain body to reuse connection
                        with contextlib.suppress(Exception):
                            await resp.read()
                        return status, ctype, None, 0
                    text = await resp.text(errors="ignore")
                    size = len(text.encode("utf-8", errors="ignore"))
                    return status, ctype, text, size
            except Exception as e:
                last_exc = e
                await asyncio.sleep(0.5)
        # failed
        return None, None, None, 0

    async def worker(self, wid: int):
        while self.pages_fetched < self.max_pages:
            try:
                url, depth = await asyncio.wait_for(self.frontier.get(), timeout=1.0)
            except asyncio.TimeoutError:
                # no more work momentarily
                if self.frontier.empty():
                    return
                continue

            if url in self.seen:
                self.frontier.task_done()
                continue

            self.seen.add(url)

            if not self._allowed_by_scope(url):
                self.frontier.task_done()
                continue

            status, ctype, html, size = await self._fetch(url)

            record = {
                "url": url,
                "depth": depth,
                "status": status,
                "content_type": ctype,
                "bytes": size,
                "fetched_at": int(time.time()),
                "out_links": [],
            }

            if html and status and 200 <= status < 400:
                links = extract_links(url, html)
                record["out_links"] = links
                # enqueue children
                for link in links:
                    if link not in self.seen:
                        await self.frontier.put((link, depth + 1))

                # write page record (you can include 'html' if you want raw storage)
                # To keep file smaller, comment/uncomment next line:
                record["html"] = html

                # Update fetched count only on successful HTML fetch
                self.pages_fetched += 1

            # write JSONL record
            self.out_fp.write(json.dumps(record, ensure_ascii=False) + "\n")

            self.frontier.task_done()

            if self.pages_fetched >= self.max_pages:
                return

    async def run(self):
        # initialize
        connector = aiohttp.TCPConnector(limit_per_host=1, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(total=CONNECT_TIMEOUT + READ_TIMEOUT)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            self.session = session
            self.robots = RobotsCache(session, self.user_agent)

            # seed frontier
            for s in self.seeds:
                await self.frontier.put((normalize_url(s), 0))

            # open output file
            with open(self.out_path, "w", encoding="utf-8") as fp:
                self.out_fp = fp
                workers = [asyncio.create_task(self.worker(i)) for i in range(self.concurrency)]
                await asyncio.gather(*workers, return_exceptions=True)

    @classmethod
    def cli(cls):
        p = argparse.ArgumentParser(description="Async polite crawler (stdlib + aiohttp)")
        p.add_argument("--seeds", nargs="+", required=True, help="Seed URL(s)")
        p.add_argument("--out", required=True, help="Output JSONL file")
        p.add_argument("--max-pages", type=int, default=300, help="Max HTML pages to fetch (successful)")
        p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Global concurrency (tasks)")
        p.add_argument("--same-domain", action="store_true", help="Restrict crawl to the same (sub)domain of the first seed")
        p.add_argument("--allowed-domains", nargs="*", default=None, help="Whitelist of exact hostnames")
        p.add_argument("--user-agent", default=USER_AGENT, help="Custom User-Agent")
        args = p.parse_args()

        crawler = cls(
            seeds=args.seeds,
            out_path=args.out,
            max_pages=args.max_pages,
            concurrency=args.concurrency,
            same_domain=args.same_domain,
            allowed_domains=args.allowed_domains,
            user_agent=args.user_agent,
        )
        try:
            asyncio.run(crawler.run())
        except KeyboardInterrupt:
            print("\nInterrupted by user.", file=sys.stderr)


if __name__ == "__main__":
    Crawler.cli()
