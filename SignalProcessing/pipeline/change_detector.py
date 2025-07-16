from river.drift import ADWIN
from common.logger import Logger

logger = Logger(__name__)

class ChangeLevelDetector:
    def __init__(self, input_queue, output_queue, delta=0.005):  # More sensitive
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.adwin = ADWIN(delta=delta)

    def run(self, timeout=0):
        """
        Continuously pops data from input_queue, checks for change in 'value' field,
        and pushes to output_queue if a change is detected.
        """
        while True:
            data = self.input_queue.pop(timeout=timeout)
            if data is None:
                logger.info("No data found in input queue")
                break  # Stop if no data and timeout is reached
            value = data.get('value')
            if value is not None:
                self.adwin.update(value)
                logger.info(f"Updating ADWIN with value: {data}")
                if self.adwin.drift_detected:
                    #logger.info(f"Drift detected: {data}")
                    self.output_queue.push(data) 