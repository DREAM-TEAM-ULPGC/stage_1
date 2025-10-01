import json

class JsonWriter:
    def __init__(self, path, mode="jsonl"):
        self.path = path
        self.mode = mode
        self.fp = open(path, "w", encoding="utf-8")
        if self.mode == "json":
            self.fp.write("[\n")
            self.first = True

    def write(self, record: dict):
        if self.mode == "jsonl":
            self.fp.write(json.dumps(record, ensure_ascii=False) + "\n")
        else:  # pretty JSON
            if not self.first:
                self.fp.write(",\n")
            self.fp.write(json.dumps(record, ensure_ascii=False, indent=2))
            self.first = False

    def close(self):
        if self.mode == "json":
            self.fp.write("\n]\n")
        self.fp.close()
