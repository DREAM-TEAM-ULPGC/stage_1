import json
import re
from pathlib import Path
from utils.timer import Timer

def simple_tokenizer(text):
    """Tokeniza el texto eliminando puntuación y pasando a minúsculas."""
    return re.findall(r"[a-z]+", text.lower())

def test_indexer_timing(tmp_path):
    """Prueba de rendimiento del indexer de Guedes."""
    body_file = tmp_path / "book_body.txt"
    body_file.write_text("A cat and a dog. A dog!", encoding="utf-8")

    timings = {}
    with Timer("read", timings):
        text = body_file.read_text(encoding="utf-8")

    with Timer("preprocess", timings):
        tokens = simple_tokenizer(text)

    with Timer("build_postings", timings):
        postings = {}
        for token in tokens:
            postings.setdefault(token, set()).add(1)

    with Timer("write", timings):
        out_file = tmp_path / "index.json"
        out_file.write_text(json.dumps({k: sorted(v) for k, v in postings.items()}), encoding="utf-8")

    assert out_file.exists()
    assert postings["dog"] == {1}
    assert all(v >= 0 for v in timings.values())
