from river.drift import ADWIN
import logging
from common.logger import Logger

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class ChangeLevelDetector:
    STOP_SIGNAL = object()
    def __init__(self, input_queue, output_queue, delta=0.001, clock=3):
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.delta = delta
        self.clock = clock
        self.adwins = {}
        self.medians = {}  # Use river's Quantile for median
        self.queue = {} # for each node, sensor pair, contains a list of the last few readings
        from river.stats import Quantile
        self.Quantile = Quantile


    def _add_to_queue(self, node_id, sensor_id, reading):
        if node_id not in self.queue:
            self.queue[node_id] = {}
        if sensor_id not in self.queue[node_id]:
            self.queue[node_id][sensor_id] = []
        self.queue[node_id][sensor_id].append(reading)
        while len(self.queue[node_id][sensor_id]) > self.clock/2:
            self.queue[node_id][sensor_id].pop(0)

    def _feed_queue2median(self, node, sensor):
        key = (node, sensor)
        for reading in self.queue[node][sensor]:
            self.medians[key].update(reading)


    def _add_to_median(self, node, sensor, num):
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
        self._feed_queue2median(node, sensor)

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
                    # logger.warning(f"Value is None for node: {node}, sensor: {sensor}, timestamp: {timestamp}")
                    continue
                key = (node, sensor)
                if key not in self.adwins:
                    # logger.info(f"Created new ADWIN for {key}")
                    self.adwins[key] = ADWIN(delta=self.delta, clock=self.clock)

                adwin = self.adwins[key]
                adwin.update(value)
                self._add_to_median(node, sensor, value)
                self._add_to_queue(node, sensor, value)

                if sensor == 'ambient_avg' and node == 3:
                    # print(value, "\t\t\t", timestamp)
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

    def run(self, timeout=0, stop_event=None):
        """
        Continuously pops dicts from input_queue, each mapping sensor to reading,
        and processes all sensors in batch. Passes None to output_queue when done.
        If stop_event is provided (threading.Event), will break if stop_event.is_set().
        """
        while True:
            if stop_event is not None and stop_event.is_set():
                logger.info("ChangeLevelDetector.run detected stop_event set, breaking loop.")
                self.output_queue.put(None)
                break
            reading = self.input_queue.get()
            if reading is None:
                self.output_queue.put(None)
                break
            self.process_batch(reading) 