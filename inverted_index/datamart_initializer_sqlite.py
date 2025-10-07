import sqlite3
from pathlib import Path
import argparse


def init_datamart(db_path: str = "datamart/datamart.db") -> None:
    """
    Initialize the SQLite database and create the 'books' table if it doesn't exist.

    Table schema:
      - book_id INTEGER PRIMARY KEY
      - title TEXT
      - author TEXT
      - release_date TEXT
      - language TEXT
    """
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_file) as conn:
        cur = conn.cursor()
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
        conn.commit()

    print(f"Datamart initialized at: {db_file}")
