import os
import json
import re
from pathlib import Path


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
        "last_indexed_id": last_id
    }

    with open(progress_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def extract_book_id(filename: str) -> int:
    match = re.search(r"(\d+)(?=_)", filename)
    return int(match.group(1)) if match else -1


def build_inverted_index(datalake_path: str, output_path: str, progress_path: str = "indexer/progress.json"):
    datalake = Path(datalake_path)
    output = Path(output_path)

    progress = load_progress(progress_path)
    last_day = progress["last_day"]
    last_hour = progress["last_hour"]
    last_indexed_id = progress["last_indexed_id"]

    print(f"Last progress: day={last_day}, hour={last_hour}, id={last_indexed_id}")

    if output.exists():
        with open(output, "r", encoding="utf-8") as f:
            inverted_index = json.load(f)
    else:
        inverted_index = {}

    for day_folder in sorted(datalake.iterdir()):
        if not day_folder.is_dir():
            continue
        day_name = day_folder.name

        if last_day and day_name < last_day:
            continue

        for hour_folder in sorted(day_folder.iterdir()):
            if not hour_folder.is_dir():
                continue
            hour_name = hour_folder.name

            if last_day == day_name and last_hour and hour_name < last_hour:
                continue

            print(f"Processed day/hour {day_name}/{hour_name} ...")

            txt_files = list(hour_folder.glob("*.txt"))
            if not txt_files:
                continue

            book_ids = sorted(set(extract_book_id(f.name) for f in txt_files if extract_book_id(f.name) != -1))

            for book_id in book_ids:
                if (
                    last_day == day_name
                    and last_hour == hour_name
                    and book_id <= last_indexed_id
                ):
                    continue

                body_file = hour_folder / f"{book_id}_body.txt"
                if not body_file.exists():
                    continue

                with open(body_file, "r", encoding="utf-8") as f:
                    text = f.read().lower()

                words = re.findall(r'\b[a-záéíóúüñ]+\b', text)
                if not words:
                    continue

                for word in set(words):
                    if word not in inverted_index:
                        inverted_index[word] = [book_id]
                    elif book_id not in inverted_index[word]:
                        inverted_index[word].append(book_id)

                last_indexed_id = max(last_indexed_id, book_id)
                print(f"Indexed book with ID {book_id} ({day_name}/{hour_name})")

            # Ordenar y limpiar duplicados antes de guardar
            for word in inverted_index:
                inverted_index[word] = sorted(set(inverted_index[word]))

            output.parent.mkdir(parents=True, exist_ok=True)
            with open(output, "w", encoding="utf-8") as out:
                json.dump(inverted_index, out, ensure_ascii=False, indent=2)

            save_progress(progress_path, day_name, hour_name, last_indexed_id)
            print(f"Progress saved: {day_name}/{hour_name} (last ID: {last_indexed_id})")

    print(f"Last indexed day {last_day or day_name}/{last_hour or hour_name}")
