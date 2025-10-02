import argparse
from pathlib import Path
from .downloader import download_book_to_datalake
import time

def main():
    ap = argparse.ArgumentParser(description="Crawler Stage 1.")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--id", type=int, help="ID único de Gutenberg (ej. 1342)")
    g.add_argument("--range", nargs=2, type=int, metavar=("INICIO", "FIN"),
    help="Rango inclusivo de IDs (ej. 100 110)")
    g.add_argument("--list", type=Path, help="Fichero con un ID por línea")
    g.add_argument("--continuous", action="store_true", help="Run continously downloading batches of books from start until end")

    args = ap.parse_args()
    
    ok, ko = 0, 0

    if args.continuous:
        book_id = 1 
        consecutive_fails = 0
        max_consecutive_fails = 10
        batch_size = 10
        interval_seconds = 4 * 60
        while True:
            batch_fail_check = False
            for _ in range(batch_size):
                res = download_book_to_datalake(book_id)
                if res.get("ok"):
                    ok += 1
                    consecutive_fails = 0
                    print(f"[OK] {book_id} -> {res['header_path']} | {res['body_path']}")
                else:
                    ko += 1
                    print(f"[FAIL] {book_id} -> {res.get('reason')}")
                    if "http_error" in res.get("reason", "") or "markers_not_found" in res.get("reason", ""):
                        consecutive_fails += 1
                    else:
                        consecutive_fails = 0
                if consecutive_fails >= max_consecutive_fails:
                    batch_fail_check = True
                    break
                book_id += 1
            if batch_fail_check:
                print(f"Stopping continuous run after {consecutive_fails} consecutive failures.")
                break
            print(f"Batch completed. Sleeping for {interval_seconds} seconds...")
            time.sleep(interval_seconds)
        print(f"\nResumen: OK={ok}  FAIL={ko}")
        return
    
    ids = []
    if args.id is not None:
        ids = [args.id]
    elif args.range:
        a, b = args.range
        ids = list(range(a, b + 1))
    elif args.list:
        ids = [int(x.strip()) for x in args.list.read_text(encoding="utf-8").splitlines() if x.strip()]

    ok, ko = 0, 0
    for bid in ids:
        res = download_book_to_datalake(bid)
        if res.get("ok"):
            ok += 1
            print(f"[OK] {bid} -> {res['header_path']} | {res['body_path']}")
        else:
            ko += 1
            print(f"[FAIL] {bid} -> {res.get('reason')}")

    print(f"\nResumen: OK={ok}  FAIL={ko}")

if __name__ == "__main__":
    main()
