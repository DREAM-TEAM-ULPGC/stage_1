import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

def ensure_parents(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def now_parts_utc():
    dt_utc = datetime.now(timezone.utc)
    dtc_canary = dt_utc + timedelta(hours=1)
    return dtc_canary.strftime("%Y%m%d"), dtc_canary.strftime("%H")

def backoff(attempt: int):
    time.sleep(min(30, (2 ** attempt) + random.random()))