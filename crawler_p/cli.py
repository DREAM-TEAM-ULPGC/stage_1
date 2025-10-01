import argparse
from pathlib import Path

from .downloader import download_book_to_datalake

def main():
    ap = argparse.ArgumentParser(description="Crawler Stage 1 (solo datalake).")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--id", type=int, help="ID único de Gutenberg (ej. 1342)")
    g.add_argument("--range", nargs=2, type=int, metavar=("INICIO", "FIN"),
                   help="Rango inclusivo de IDs (ej. 100 110)")
    g.add_argument("--list", type=Path, help="Fichero con un ID por línea")

    args = ap.parse_args()

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
