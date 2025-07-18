from river.drift import ADWIN
import logging
from common.logger import Logger

logger = logging.getLogger(__name__)

class ChangeLevelDetector:
    def __init__(self, input_queue, output_queue, node_id=None, delta=0.005):  # More sensitive
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.node_id = node_id
        self.delta = delta
        self.adwins = {}
        self.medians = {}  # Use river's Quantile for median
        from river.stats import Quantile
        self.Quantile = Quantile

    def _add_number(self, sensor, num):
        # Use river's Quantile for approximate median
        if sensor not in self.medians:
            self.medians[sensor] = self.Quantile(0.5)
        self.medians[sensor].update(num)

    def _get_median(self, sensor):
        if sensor not in self.medians:
            return None
        return self.medians[sensor].get()

    def _reset_median(self, sensor):
        # Reset the Quantile object for the sensor
        self.medians[sensor] = self.Quantile(0.5)

    def process_batch(self, reading):
        """
        reading: dict with keys 'timestamp', 'node', 'sensor_data' (dict of sensor -> value)
        For each sensor, update ADWIN and median. If drift is detected for any sensor, emit a list of dicts with sensor, node, median, and timestamp for all sensors.
        """
        import copy
        drift_detected = False
        timestamp = reading.get('timestamp')
        node = reading.get('node')
        sensor_data = reading.get('sensor_data', {})
        for sensor, value in sensor_data.items():
            if value is None:
                continue
            # Get or create ADWIN for this sensor
            if sensor not in self.adwins:
                self.adwins[sensor] = ADWIN(delta=self.delta)
            adwin = self.adwins[sensor]
            adwin.update(value)
            self._add_number(sensor, value)
            if adwin.drift_detected:
                drift_detected = True

        if drift_detected:
            # Push a list of dicts: sensor, node, median, timestamp for all sensors
            output = []
            for sensor, value in sensor_data.items():
                median = self._get_median(sensor)
                self._reset_median(sensor)
                output.append({
                    'sensor': sensor,
                    'node': node,
                    'value': median,
                    'timestamp': timestamp
                })
            self.output_queue.push(output)
            logger.info(f"Pushed {len(output)} outputs to output_queue")

    def run(self, timeout=0):
        """
        Continuously pops dicts from input_queue, each mapping sensor to reading,
        and processes all sensors in batch.
        """
        while True:
            reading = self.input_queue.pop(timeout=timeout)
            if reading is None:
                logger.info("No data found in input queue")
                break
            self.process_batch(reading) 