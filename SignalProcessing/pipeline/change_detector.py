from river.drift import ADWIN
from common.logger import Logger

logger = Logger(__name__)

class ChangeLevelDetector:
    def __init__(self, input_queue, output_queue, delta=0.005):  # More sensitive
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.adwin = ADWIN(delta=delta)
        # Two heaps for streaming median
        import heapq
        self.low = []  # max-heap (invert values)
        self.high = []  # min-heap

    def _add_number(self, num):
        import heapq
        # Add to max-heap (as negative for max-heap behavior)
        if not self.low or num <= -self.low[0]:
            heapq.heappush(self.low, -num)
        else:
            heapq.heappush(self.high, num)
        # Balance heaps
        if len(self.low) > len(self.high) + 1:
            heapq.heappush(self.high, -heapq.heappop(self.low))
        elif len(self.high) > len(self.low):
            heapq.heappush(self.low, -heapq.heappop(self.high))

    def _get_median(self):
        if not self.low and not self.high:
            return None
        if len(self.low) > len(self.high):
            return -self.low[0]
        else:
            return (-self.low[0] + self.high[0]) / 2

    def _reset_median(self):
        self.low.clear()
        self.high.clear()

    def run(self, timeout=0):
        """
        Continuously pops data from input_queue, checks for change in 'value' field,
        and pushes to output_queue if a change is detected.
        """
        import copy
        
        while True:
            data = self.input_queue.pop(timeout=timeout)
            if data is None:
                logger.info("No data found in input queue")
                break  # Stop if no data and timeout is reached
            value = data.get('value')
            if value is not None:
                self.adwin.update(value)
                self._add_number(value)
                logger.info(f"Updating ADWIN with value: {data}")
                if self.adwin.drift_detected and (self.low or self.high):
                    median = self._get_median()
                    median_data = copy.deepcopy(data)
                    median_data['value'] = median
                    self.output_queue.push(median_data)
                    self._reset_median() 