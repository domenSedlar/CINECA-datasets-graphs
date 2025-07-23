from river.drift import ADWIN
import logging
from common.logger import Logger

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class ChangeLevelDetector:
    def __init__(self, input_queue, output_queue, delta=0.002, clock=16):  # TODO what are the right parameters?
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.delta = delta
        self.clock = clock
        self.adwins = {}
        self.medians = {}  # Use river's Quantile for median
        from river.stats import Quantile
        self.Quantile = Quantile

    def _add_number(self, node, sensor, num):
        # Use river's Quantile for approximate median, now per (node, sensor)
        key = (node, sensor)
        if key not in self.medians:
            self.medians[key] = self.Quantile(0.5)
        self.medians[key].update(num)

    def _get_median(self, node, sensor):
        key = (node, sensor)
        if key not in self.medians:
            return None
        return self.medians[key].get()

    def _reset_median(self, node, sensor):
        key = (node, sensor)
        self.medians[key] = self.Quantile(0.5)

    def process_batch(self, batch):
        """
        batch: dict mapping node_id -> node_data, where node_data is a dict with keys 'timestamp', 'sensor_data' (dict of sensor -> value)
        For each (node, sensor), update ADWIN and median. If drift is detected for any pair, emit a list of dicts with sensor, node, median, and timestamp for all pairs.
        """
        drift_detected = False
        drift_pairs = set()
        # First pass: update all, check for drift
        for node, node_data in batch.items():
            timestamp = node_data.get('timestamp')
            sensor_data = node_data.get('sensor_data', {})
            for sensor, value in sensor_data.items():
                if value is None:
                    logger.warning(f"Value is None for node: {node}, sensor: {sensor}")
                    continue
                key = (node, sensor)
                if key not in self.adwins:
                    # logger.info(f"Created new ADWIN for {key}")
                    self.adwins[key] = ADWIN(delta=self.delta, clock=self.clock)
                adwin = self.adwins[key]
                adwin.update(value)
                self._add_number(node, sensor, value)
                if sensor == 'ambient_avg' and node == 3:
                    #print(value)
                    pass
                if adwin.drift_detected:
                    drift_detected = True
                    drift_pairs.add(key)
                    logger.debug(f"Drift detected value: {value}, node: {node}, sensor: {sensor}")
                    if sensor == 'ambient_avg' and node == 3:
                        continue
                        #print("detected")

        # If any drift detected, output all medians for all pairs
        if drift_detected:
            output = []
            for (node, sensor), quantile in self.medians.items():
                median = self._get_median(node, sensor)
                # Try to get timestamp from batch if possible
                timestamp = batch.get(node, {}).get('timestamp')
                rack_id = batch.get(node, {}).get('rack_id')
                
                if (node, sensor) in drift_pairs:
                    self._reset_median(node, sensor)
                
                output.append({
                    'sensor': sensor,
                    'node': node,
                    'value': median,
                    'timestamp': timestamp,
                    'rack_id': rack_id
                })
            
            self.output_queue.put(output)
            # logger.info(f"Pushed {len(output)} outputs to output_queue")

    def run(self, timeout=0):
        """
        Continuously pops dicts from input_queue, each mapping sensor to reading,
        and processes all sensors in batch. Passes None to output_queue when done.
        """
        while True:
            reading = self.input_queue.get()
            if reading is None:
                self.output_queue.put(None)
                break
            print(reading)
            self.process_batch(reading) 