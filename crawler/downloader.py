from pathlib import Path
import requests

from .config import DATALAKE, GUT_URL, MAX_RETRIES, TIMEOUT_S
from .utils import now_parts_utc, ensure_parents, backoff
from .parsing import split_gutenberg

class DownloadError(Exception):
    pass

def download_book_to_datalake(book_id: int) -> dict:
    ymd, hh = now_parts_utc()
    out_dir = DATALAKE / ymd / hh
    out_dir.mkdir(parents=True, exist_ok=True)

    url = GUT_URL.format(id=book_id)

    text = None
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, timeout=TIMEOUT_S)
            r.raise_for_status()
            text = r.text
            break
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                return {"ok": False, "book_id": book_id, "reason": f"http_error:{e}"}
            backoff(attempt)

    split = split_gutenberg(text)
    if not split:
        return {"ok": False, "book_id": book_id, "reason": "markers_not_found"}

    header, body, _footer = split

    body_path = out_dir / f"{book_id}_body.txt"
    header_path = out_dir / f"{book_id}_header.txt"

    ensure_parents(body_path)
    body_path.write_text(body, encoding="utf-8")
    header_path.write_text(header, encoding="utf-8")

    return {
        "ok": True,
        "book_id": book_id,
        "header_path": str(header_path),
        "body_path": str(body_path),
    }
