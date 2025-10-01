import random
import time
from datetime import datetime, timezone
from pathlib import Path

def ensure_parents(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def now_parts_utc():
    # YYYYMMDD y HH en UTC para trazabilidad estable
    dt = datetime.now(timezone.utc)
    return dt.strftime("%Y%m%d"), dt.strftime("%H")

def backoff(attempt: int):
    # Exponencial con jitter suave
    time.sleep(min(30, (2 ** attempt) + random.random()))
