from typing import Optional, Tuple
from .config import START_MARKERS, END_MARKERS

def split_gutenberg(text: str) -> Optional[Tuple[str, str, str]]:
    start_idx = -1
    for m in START_MARKERS:
        i = text.find(m)
        if i != -1:
            start_idx = i
            break

    end_idx = -1
    for m in END_MARKERS:
        i = text.find(m)
        if i != -1:
            end_idx = i
            break

    if start_idx == -1 or end_idx == -1 or end_idx <= start_idx:
        return None

    header = text[:start_idx]
    body = text[start_idx:end_idx]
    footer = text[end_idx:]
    return header.strip(), body.strip(), footer.strip()
