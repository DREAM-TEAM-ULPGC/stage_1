from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

DATALAKE = ROOT / "datalake"

GUT_URL = "https://www.gutenberg.org/cache/epub/{id}/pg{id}.txt"

START_MARKERS = [
    "*** START OF THE PROJECT GUTENBERG EBOOK",
    "***START OF THE PROJECT GUTENBERG EBOOK",
]
END_MARKERS = [
    "*** END OF THE PROJECT GUTENBERG EBOOK",
    "***END OF THE PROJECT GUTENBERG EBOOK",
]

MAX_RETRIES = 4
TIMEOUT_S = 30
