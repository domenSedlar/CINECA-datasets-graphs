import time

class Timer:
    def __init__(self):
        self.start_time = None
        self.total_duration = 0.0
        self.count = 0

    def start(self):
        """Mark the start of a timing interval."""
        self.start_time = time.time()

    def end(self):
        """Mark the end of the timing interval and update average tracking."""
        if self.start_time is None:
            raise RuntimeError("Timer was not started.")
        duration = time.time() - self.start_time
        self.total_duration += duration
        self.count += 1
        self.start_time = None  # reset to avoid reuse without explicit start
        return duration

    def get_avg(self):
        """Return the average of all timing intervals."""
        if self.count == 0:
            return 0.0
        return self.total_duration / self.count