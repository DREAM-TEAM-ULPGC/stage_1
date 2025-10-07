from pathlib import Path
import random

class Control:
    def __init__(self, control_dir="control", total_books=70000):
        self.dir = Path(control_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.downloaded_file = self.dir / "downloaded_books.txt"
        self.indexed_file = self.dir / "indexed_books.txt"
        self.total_books = total_books

    def _read_ids(self, file_path):
        if file_path.exists():
            return set(file_path.read_text(encoding="utf-8").splitlines())
        return set()

    def _append_id(self, file_path, book_id):
        with file_path.open("a", encoding="utf-8") as f:
            f.write(f"{book_id}\n")

    def step(self, downloader, indexer):
        downloaded = self._read_ids(self.downloaded_file)
        indexed = self._read_ids(self.indexed_file)
        pending = downloaded - indexed

        if pending:
            book_id = pending.pop()
            indexer(book_id)
            self._append_id(self.indexed_file, book_id)
            return f"Indexed book {book_id}"

        for _ in range(10):
            book_id = str(random.randint(1, self.total_books))
            if book_id not in downloaded:
                downloader(book_id)
                self._append_id(self.downloaded_file, book_id)
                return f"Downloaded book {book_id}"

        return "Idle: no new book found"
