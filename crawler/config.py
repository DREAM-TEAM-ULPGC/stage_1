from pathlib import Path

# Raíz del repo (ajusta si cambias la ubicación)
ROOT = Path(__file__).resolve().parents[1]

# Ubicación del datalake (requerido por el guion)
DATALAKE = ROOT / "datalake"  # YYYYMMDD/HH/<BOOK_ID>_*.txt

# URL base de Gutenberg (ID -> .txt)
GUT_URL = "https://www.gutenberg.org/cache/epub/{id}/pg{id}.txt"

# Marcadores recomendados por el guion (y variantes comunes)
START_MARKERS = [
    "*** START OF THE PROJECT GUTENBERG EBOOK",
    "***START OF THE PROJECT GUTENBERG EBOOK",
]
END_MARKERS = [
    "*** END OF THE PROJECT GUTENBERG EBOOK",
    "***END OF THE PROJECT GUTENBERG EBOOK",
]

# Red de reintentos
MAX_RETRIES = 4
TIMEOUT_S = 30
