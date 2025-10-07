import argparse
import json
import sqlite3
from pathlib import Path
from typing import Dict, Any


def load_catalog(catalog_path: str) -> Dict[str, Dict[str, Any]]:
    p = Path(catalog_path)
    if not p.exists():
        raise FileNotFoundError(f"Catalog not found: {p}")
    with open(p, "r", encoding="utf-8") as f:
        data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError("Catalog JSON must be a dict of {book_id: metadata}")
        return data


def upsert_books(db_path: str, catalog: Dict[str, Dict[str, Any]]) -> int:
    """
    Inserts or updates rows in 'books' from a catalog dict.
    Returns the number of rows affected (inserted + updated best-effort).
    """
    db_file = Path(db_path)
    if not db_file.exists():
        raise FileNotFoundError(f"Database not found: {db_file}. Initialize it first with datamart_initializer.")

    affected = 0
    with sqlite3.connect(db_file) as conn:
        cur = conn.cursor()
        # Use UPSERT (SQLite 3.24+) on primary key conflict
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS books (
                book_id INTEGER PRIMARY KEY,
                title TEXT,
                author TEXT,
                release_date TEXT,
                language TEXT
            )
            """
        )

        sql = (
            "INSERT INTO books (book_id, title, author, release_date, language) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(book_id) DO UPDATE SET "
            "title=excluded.title, author=excluded.author, release_date=excluded.release_date, language=excluded.language"
        )

        rows = []
        for book_id_str, meta in catalog.items():
            try:
                book_id = int(book_id_str)
            except (TypeError, ValueError):
                # Skip keys that aren't numeric IDs
                continue

            title = meta.get("title")
            author = meta.get("author")
            release_date = meta.get("release_date")
            language = meta.get("language")
            rows.append((book_id, title, author, release_date, language))

        cur.executemany(sql, rows)
        affected = cur.rowcount if cur.rowcount is not None else len(rows)
        conn.commit()

    return affected


def run(catalog_path: str = "metadata/catalog.json", db_path: str = "datamart/datamart.db") -> int:
    catalog = load_catalog(catalog_path)
    return upsert_books(db_path, catalog)