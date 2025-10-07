from time import perf_counter

class Timer:
    """
    Context manager para medir el tiempo de ejecución de bloques de código.
    """

    def __init__(self, name, sink_dict):
        self.name = name
        self.sink = sink_dict

    def __enter__(self):
        self.start = perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = perf_counter() - self.start
        self.sink[self.name] = self.sink.get(self.name, 0.0) + elapsed
