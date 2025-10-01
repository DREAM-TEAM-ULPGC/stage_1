from pathlib import Path
import random
from crawler import BookCrawler

CONTROL_PATH = Path("control")
DOWNLOADS = CONTROL_PATH / "downloaded_books.txt"
INDEXINGS = CONTROL_PATH / "indexed_books.txt"
TOTAL_BOOKS = 70000  # total de libros disponibles en Gutenberg

def read_ids(file_path):
    if file_path.exists():
        return set(file_path.read_text(encoding="utf-8").splitlines())
    return set()

def append_id(file_path, book_id):
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(f"{book_id}\n")

def control_pipeline_step(seed_urls, out_path, max_books=10):
    CONTROL_PATH.mkdir(parents=True, exist_ok=True)
    downloaded = read_ids(DOWNLOADS)
    indexed = read_ids(INDEXINGS)
    ready_to_index = downloaded - indexed

    if ready_to_index:
        book_id = ready_to_index.pop()
        print(f"[CONTROL] Scheduling book {book_id} for indexing...")
        # Aquí se llamaría al indexer
        append_id(INDEXINGS, book_id)
        print(f"[CONTROL] Book {book_id} successfully indexed.")
    else:
        for _ in range(10):  # intentar hasta 10 veces
            candidate_id = str(random.randint(1, TOTAL_BOOKS))
            if candidate_id not in downloaded:
                print(f"[CONTROL] Downloading new book with ID {candidate_id}...")
                # Crear URL de Gutenberg
                url = f"https://www.gutenberg.org/cache/epub/{candidate_id}/pg{candidate_id}.txt"
                crawler = BookCrawler(seeds=[url], out=out_path, max_pages=1)
                import asyncio
                asyncio.run(crawler.run())
                append_id(DOWNLOADS, candidate_id)
                print(f"[CONTROL] Book {candidate_id} successfully downloaded.")
                break
