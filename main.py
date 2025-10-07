import argparse
from control.control import Control


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