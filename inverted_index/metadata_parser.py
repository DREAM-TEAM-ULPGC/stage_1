import os
import json
import re
from pathlib import Path
from typing import Dict, Optional


def load_progress(progress_path: str):
    if os.path.exists(progress_path):
        with open(progress_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"last_day": None, "last_hour": None, "last_indexed_id": -1}


def save_progress(progress_path: str, last_day: str, last_hour: str, last_id: int):
    progress_file = Path(progress_path)
    progress_file.parent.mkdir(parents=True, exist_ok=True)

    data = {
        "last_day": last_day,
        "last_hour": last_hour,
        "last_indexed_id": last_id,
    }

    with open(progress_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def extract_book_id(filename: str) -> int:
    match = re.search(r"(\d+)(?=_)", filename)
    return int(match.group(1)) if match else -1


def parse_header_metadata(text: str) -> Dict[str, Optional[str]]:
    """
    Extracts Title, Author, Release date, and Language from a header text.
    Matches are case-insensitive and line-based. Values are stripped; may be None if not found.
    """
    patterns = {
        "title": re.compile(r"^\s*Title\s*:\s*(.+?)\s*$", re.IGNORECASE | re.MULTILINE),
        "author": re.compile(r"^\s*Author\s*:\s*(.+?)\s*$", re.IGNORECASE | re.MULTILINE),
        "release_date": re.compile(r"^\s*Release\s+date\s*:\s*(.+?)\s*$", re.IGNORECASE | re.MULTILINE),
        "language": re.compile(r"^\s*Language\s*:\s*(.+?)\s*$", re.IGNORECASE | re.MULTILINE),
    }

    result: Dict[str, Optional[str]] = {k: None for k in patterns.keys()}

    for key, pattern in patterns.items():
        m = pattern.search(text)
        if m:
            value = m.group(1).strip()
            # For release_date, drop trailing Gutenberg bracket like "[eBook #1234]" if present
            if key == "release_date":
                value = re.sub(r"\s*\[eBook\s*#\d+\]\s*$", "", value, flags=re.IGNORECASE)
            result[key] = value

    return result


def build_metadata_catalog(
    datalake_path: str,
    output_path: str,
    progress_path: str = "metadata/progress_parser.json",
):
    """
    Traverses the datalake like the indexer, but parses header metadata for each book.
    Writes a JSON mapping of book_id -> {title, author, release_date, language} and persists progress.
    """
    datalake = Path(datalake_path)
    output = Path(output_path)

    progress = load_progress(progress_path)
    last_day = progress["last_day"]
    last_hour = progress["last_hour"]
    last_indexed_id = progress["last_indexed_id"]

    print(f"Last progress: day={last_day}, hour={last_hour}, id={last_indexed_id}")

    # Load existing catalog if present
    if output.exists():
        with open(output, "r", encoding="utf-8") as f:
            catalog = json.load(f)
            # Ensure dict type
            if not isinstance(catalog, dict):
                catalog = {}
    else:
        catalog = {}

    processed_any = False

    # Sort days lexicographically (YYYYMMDD works correctly as strings)
    for day_folder in sorted(datalake.iterdir(), key=lambda p: p.name):
        if not day_folder.is_dir():
            continue
        day_name = day_folder.name

        if last_day and day_name < last_day:
            continue

        # Sort hours numerically to avoid "10" < "9" lexicographic issues
        hour_folders = [p for p in day_folder.iterdir() if p.is_dir()]
        hour_folders.sort(key=lambda p: int(p.name) if p.name.isdigit() else p.name)

        for hour_folder in hour_folders:
            hour_name = hour_folder.name

            if last_day == day_name and last_hour and hour_name.isdigit() and last_hour.isdigit():
                if int(hour_name) < int(last_hour):
                    continue
            elif last_day == day_name and last_hour and hour_name < last_hour:
                # fallback to string compare if any name isn't numeric
                continue

            print(f"Processed day/hour {day_name}/{hour_name} ...")

            txt_files = list(hour_folder.glob("*.txt"))
            if not txt_files:
                continue

            book_ids = sorted(
                set(extract_book_id(f.name) for f in txt_files if extract_book_id(f.name) != -1)
            )

            for book_id in book_ids:
                if (
                    last_day == day_name
                    and last_hour == hour_name
                    and book_id <= last_indexed_id
                ):
                    continue

                header_file = hour_folder / f"{book_id}_header.txt"
                if not header_file.exists():
                    continue

                with open(header_file, "r", encoding="utf-8") as f:
                    header_text = f.read()

                meta = parse_header_metadata(header_text)

                # Only store if at least one field was found
                if any(meta.values()):
                    catalog[str(book_id)] = meta
                    processed_any = True
                    print(
                        f"Parsed metadata for book ID {book_id} ({day_name}/{hour_name}): "
                        f"title={meta.get('title')!r}, author={meta.get('author')!r}"
                    )

                last_indexed_id = max(last_indexed_id, book_id)

            # Persist after finishing the hour
            output.parent.mkdir(parents=True, exist_ok=True)
            with open(output, "w", encoding="utf-8") as out:
                json.dump(catalog, out, ensure_ascii=False, indent=2)

            save_progress(progress_path, day_name, hour_name, last_indexed_id)
            print(
                f"Progress saved: {day_name}/{hour_name} (last ID: {last_indexed_id})"
            )

    if processed_any:
        print(
            f"Finished. Last indexed day {last_day or day_name}/{last_hour or hour_name}"
        )
    else:
        print("Finished. No headers processed.")


__all__ = [
    "build_metadata_catalog",
    "parse_header_metadata",
    "load_progress",
    "save_progress",
    "extract_book_id",
]
