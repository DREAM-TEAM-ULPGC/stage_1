import argparse
import inverted_index.metadata_parser as metadata_parser
from inverted_index.metadata_store import run as store_catalog
from inverted_index.datamart_initializer import init_datamart


def main():
    parser = argparse.ArgumentParser(
        description="Initialize DB, build metadata catalog from datalake, and store it into SQLite."
    )
    parser.add_argument("--datalake", default="datalake", help="Path to datalake root directory")
    parser.add_argument("--catalog", default="metadata/catalog.json", help="Path to output catalog JSON")
    parser.add_argument(
        "--progress", default="metadata/progress_parser.json", help="Path to progress JSON for the parser"
    )
    parser.add_argument("--db", default="datamart/datamart.db", help="Path to SQLite database file")

    args = parser.parse_args()

    # 1) Initialize database and 'books' table
    print("[1/3] Initializing datamart ...")
    init_datamart(args.db)

    # 2) Build metadata catalog
    print("[2/3] Building metadata catalog ...")
    metadata_parser.build_metadata_catalog(
        datalake_path=args.datalake,
        output_path=args.catalog,
        progress_path=args.progress,
    )

    # 3) Store catalog into database
    print("[3/3] Storing catalog into database ...")
    affected = store_catalog(args.catalog, args.db)
    print(f"Done. Upserted ~{affected} rows into 'books'.")


if __name__ == "__main__":
    main()