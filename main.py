import argparse
import inverted_index.metadata_parser as metadata_parser
from inverted_index.metadata_store import run as store_catalog
from inverted_index.datamart_initializer_sqlite import init_datamart
from crawler.cli import main as crawler_main
from inverted_index.indexer import build_inverted_index
import time
import sys
import os
import json
from pathlib import Path


class Control:
    def __init__(
        self,
        datalake="datalake",
        catalog="metadata/catalog.json",
        progress_parser="metadata/progress_parser.json",
        db="datamart/datamart.db",
        index_output="index/inverted_index.json",
        progress_indexer="indexer/progress.json",
        progress_crawler="crawler/progress.json",
        batch_size=10,
        sleep_seconds=120,
    ):
        self.datalake = datalake
        self.catalog = catalog
        self.progress_parser = progress_parser
        self.db = db
        self.index_output = index_output
        self.progress_indexer = progress_indexer
        self.progress_crawler = progress_crawler
        self.batch_size = batch_size
        self.sleep_seconds = sleep_seconds

    def load_crawler_progress(self):
        if os.path.exists(self.progress_crawler):
            with open(self.progress_crawler, "r", encoding="utf-8") as f:
                return json.load(f).get("last_id", 0)
        return 0

    def save_crawler_progress(self, last_id: int):
        progress_file = Path(self.progress_crawler)
        progress_file.parent.mkdir(parents=True, exist_ok=True)
        with open(progress_file, "w", encoding="utf-8") as f:
            json.dump({"last_id": last_id}, f, indent=2)

    def run(self):
        print("[Initializing datamart...]")
        init_datamart(self.db)

        while True:
            print("\n[Cycle] Starting new processing cycle...\n")

            print("[1/3] Running crawler (batch of 10 books)...")
            last_id = self.load_crawler_progress()
            start_id = last_id + 1
            end_id = start_id + self.batch_size - 1
            sys.argv = ["crawler.py", "--range", str(start_id), str(end_id)]
            crawler_main()
            self.save_crawler_progress(end_id)

            print("[2/3] Running indexer...")
            build_inverted_index(
                datalake_path=self.datalake,
                output_path=self.index_output,
                progress_path=self.progress_indexer
            )

            print("[3/3] Running metadata parser...")
            metadata_parser.build_metadata_catalog(
                datalake_path=self.datalake,
                output_path=self.catalog,
                progress_path=self.progress_parser,
            )
            affected = store_catalog(self.catalog, self.db)
            print(f"Stored ~{affected} rows in 'books'.")

            print(f"Cycle completed. Sleeping {self.sleep_seconds // 60} minutes...\n")
            time.sleep(self.sleep_seconds)


def main():
    parser = argparse.ArgumentParser(
        description="Data pipeline that downloads, indexes, and processes metadata in batches of 10 books."
    )
    parser.add_argument("--datalake", default="datalake", help="Datalake root directory")
    parser.add_argument("--catalog", default="metadata/catalog.json", help="Path to output JSON catalog")
    parser.add_argument(
        "--progress-parser", default="metadata/progress_parser.json", help="Path to parser progress JSON"
    )
    parser.add_argument("--db", default="datamart/datamart.db", help="Path to SQLite database file")
    parser.add_argument("--index-output", default="index/inverted_index.json", help="Path to inverted index JSON")
    parser.add_argument("--progress-indexer", default="indexer/progress.json", help="Path to indexer progress JSON")
    parser.add_argument("--progress-crawler", default="crawler/progress.json", help="Path to crawler progress JSON")
    parser.add_argument("--batch-size", type=int, default=10, help="Batch size for crawler")
    parser.add_argument("--sleep-seconds", type=int, default=120, help="Sleep seconds between cycles")

    args = parser.parse_args()

    control = Control(
        datalake=args.datalake,
        catalog=args.catalog,
        progress_parser=args.progress_parser,
        db=args.db,
        index_output=args.index_output,
        progress_indexer=args.progress_indexer,
        progress_crawler=args.progress_crawler,
        batch_size=args.batch_size,
        sleep_seconds=args.sleep_seconds,
    )
    control.run()


if __name__ == "__main__":
    main()