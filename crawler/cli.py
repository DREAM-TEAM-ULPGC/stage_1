import argparse
from pathlib import Path
from .downloader import download_book_to_datalake
import time

def main():
    ap = argparse.ArgumentParser(description="Crawler Stage 1.")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--id", type=int, help="ID único de Gutenberg (ej. 1342)")
    g.add_argument("--range", nargs=2, type=int, metavar=("INICIO", "FIN"), help="Rango inclusivo de IDs (ej. 100 110)")
    g.add_argument("--list", type=Path, help="Fichero con un ID por línea")
    g.add_argument("--continuous", action="store_true", help="Run continuously downloading batches of books")

    args = ap.parse_args()
    ok, ko = 0, 0
    ids = []

    if args.continuous:
        book_id = 1
        consecutive_fails = 0
        while True:
            for _ in range(10):
                res = download_book_to_datalake(book_id)
                print(f"[{'OK' if res.get('ok') else 'FAIL'}] {book_id} -> {res.get('header_path', res.get('reason'))}")
                ok += res.get("ok", False)
                ko += not res.get("ok", False)
                consecutive_fails = 0 if res.get("ok") else consecutive_fails + 1
                if consecutive_fails >= 10:
                    print(f"Stopping after {consecutive_fails} consecutive failures.\nResumen: OK={ok}  FAIL={ko}")
                    return
                book_id += 1
            print("Batch completed. Sleeping 2 minutes...\n")
            time.sleep(60*2)
    else:
        if args.id is not None:
            ids = [args.id]
        elif args.range:
            ids = list(range(args.range[0], args.range[1] + 1))
        elif args.list:
            ids = [int(x) for x in args.list.read_text(encoding="utf-8").splitlines() if x.strip()]

        for bid in ids:
            res = download_book_to_datalake(bid)
            print(f"[{'OK' if res.get('ok') else 'FAIL'}] {bid} -> {res.get('header_path', res.get('reason'))}")
            ok += res.get("ok", False)
            ko += not res.get("ok", False)

        print(f"\nResumen: OK={ok}  FAIL={ko}")

if __name__ == "__main__":
    main()
