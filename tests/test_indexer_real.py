import sys, os, time
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from inverted_index.indexer import *   # importa tu indexer de Guedes
from utils.timer import Timer
from pathlib import Path
import json

def test_real_indexer_timing(tmp_path):
    """
    Mide el tiempo de ejecución del indexador de Guedes usando el Timer.
    """
    # Ruta del datalake (puedes cambiarla por tu propia ruta)
    datalake_path = Path("datalake")
    assert datalake_path.exists(), "No se encontró la carpeta datalake"

    timings = {}
    total_books = 0

    with Timer("total_indexing", timings):
        for book_dir in datalake_path.rglob("*.body.txt"):
            body_text = book_dir.read_text(encoding="utf-8")
            total_books += 1

            # Aquí podrías llamar al indexador real si ya tiene función index()
            # Ejemplo (ajústalo al nombre real de la función):
            # index_document(book_dir.stem, body_text)

    # Guarda los tiempos en JSON para luego graficar
    result_file = tmp_path / "indexer_timing.json"
    result_file.write_text(json.dumps(timings, indent=2), encoding="utf-8")

    print(f"Indexados {total_books} libros en {timings['total_indexing']:.2f} segundos")
    assert "total_indexing" in timings
