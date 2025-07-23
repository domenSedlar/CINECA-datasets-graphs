# Simple in-memory FIFO queue for single-threaded use
class SimpleQueue:
    def __init__(self):
        self._queue = []
    def push(self, item):
        self._queue.append(item)
    def pop(self, timeout=0):
        if self._queue:
            return self._queue.pop(0)
        return None