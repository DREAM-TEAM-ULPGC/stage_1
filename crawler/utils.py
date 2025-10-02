import random
import time
from datetime import datetime, timezone
from pathlib import Path

def ensure_parents(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def now_parts_utc():
    dt = datetime.now(timezone.utc)
    return dt.strftime("%Y%m%d"), dt.strftime("%H")

def backoff(attempt: int):
    time.sleep(min(30, (2 ** attempt) + random.random()))