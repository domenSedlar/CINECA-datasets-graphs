from river.drift import ADWIN
import logging
from common.logger import Logger
import psutil
import os
from river.stats import Quantile

from common.my_timer import Timer
from common.logger import Logger
from common.memory_utils import force_memory_cleanup, log_memory_usage
logger = Logger(name=__name__.split('.')[-1], log_dir='logs').get_logger_real()

class ChangeLevelDetector:
    STOP_SIGNAL = object()
    def __init__(self, input_queue, output_queue, delta=0.5, clock=3):  # Increased delta from 0.001 to 0.01
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.delta = delta # Adwin parameter
        self.clock = clock # Adwin parameter
        self.adwins = {} # dictionary, contains an adwin object for each sensor. (Used for change detection)
        self.medians = {}  # dictionary that keeps track of the median for each sensor
        self.queue = {} # for each node - sensor pair, contains a list of the last few readings,

        # Bellow are properties used for debugging
        self.filtered_count = 0
        self.total_count = 0
        self.drift_count = 0
        self.t = Timer() 


    def _add_to_queue(self, node_id, sensor_id, reading):
        self.queue[node_id][sensor_id].append(reading)
        while len(self.queue[node_id][sensor_id]) > self.clock/2: # Make sure only the latest couple of values are in the queue
            self.queue[node_id][sensor_id].pop(0)


    def _feed_queue2median(self, node, sensor):
        while self.queue[node][sensor]:
            reading = self.queue[node][sensor].popleft()
            self.medians[(node, sensor)].update(reading)

    def _add_to_median(self, node, sensor, num):
        # Use river's Quantile for approximate median, now per (node, sensor)
        key = (node, sensor)
        if key not in self.medians:
            self.medians[key] = Quantile(0.5)
        self.medians[key].update(num)


    def _get_median(self, node, sensor):
        return self.medians[(node, sensor)].get()

    def _reset_median(self, node, sensor):
        key = (node, sensor)
        self.medians[key] = Quantile(0.5)
        self._feed_queue2median(node, sensor) # change detection sometimes misses a couple of values, so we add them back


    def _process_node(self, node, node_data):
        drift_detected = False
        drift_pairs = set()
        sensor_data = node_data.get('sensor_data', {})

        for sensor, value in sensor_data.items():
            key = (node, sensor)

            if key not in self.adwins:
                # logger.info(f"Created new ADWIN for {key}")
                self.adwins[key] = ADWIN(delta=self.delta, clock=self.clock)

            if value is None or not isinstance(value, (int, float)) or math.isnan(value) or math.isinf(value):
                continue

            adwin = self.adwins[key]
            adwin.update(value)
            self.medians[key].update(value)
            self.queue[node][sensor].append(value)

            if adwin.drift_detected:
                drift_detected = True
                drift_pairs.add(key)

        return drift_detected, drift_pairs
    
    def _process_node_group(self, node_group):
        drift_detected = False
        drift_pairs = set()
        for node, node_data in node_group:
            local_drift, local_pairs = self._process_node(node, node_data)
            if local_drift:
                drift_detected = True
                drift_pairs.update(local_pairs)
        return drift_detected, drift_pairs

    def process_batch(self, batch):
        """
        batch: dict mapping node_id -> node_data, where node_data is a dict with keys 'timestamp', 'sensor_data' (dict of sensor -> value)
        For each (node, sensor), update ADWIN and median. If drift is detected for any pair, emit a list of dicts with sensor, node, median, and timestamp for all pairs.
        """
        self.t.start()
        self.total_count += 1
        drift_pairs = set()
        drift_detected = False
        # First pass: update all, check for drift
        max_workers = 32
        node_items = list(batch.items())
        chunk_size = max(1, len(node_items) // max_workers)
        node_chunks = [node_items[i:i+chunk_size] for i in range(0, len(node_items), chunk_size)]

        futures = [self.executor.submit(self._process_node_group, chunk) for chunk in node_chunks]
        for future in as_completed(futures):
            local_drift, local_pairs = future.result()
            if local_drift:
                drift_detected = True
                drift_pairs.update(local_pairs)

        self.t.end()
        self.t2.start()

        # If any drift detected, output all medians for all pairs
        if drift_detected:
            self.drift_count += 1
            output = []
            for (node, sensor) in self.adwins.keys():
                median = self._get_median(node, sensor)
                # Try to get timestamp from batch if possible
                timestamp = batch.get(node, {}).get('timestamp')
                rack_id = batch.get(node, {}).get('rack_id')
                
                if (node, sensor) in drift_pairs:
                    self._reset_median(node, sensor)

                self.output_queue.put({
                    'sensor': sensor,
                    'node': node,
                    'value': median,
                    'timestamp': timestamp,
                    'rack_id': rack_id
                })

            self.output_queue.put("BATCH_END")

        self.t2.end()
        
        # Log filtering effectiveness every 1000 batches
        if hasattr(self, '_batch_count'):
            self._batch_count += 1
        else:
            self._batch_count = 1
            
        if self._batch_count % 50 == 0:
            filter_rate = (self.drift_count / self.total_count * 100) if self.total_count > 0 else 0
            logger.info(f"[filtering] {self.drift_count}/{self.total_count} ({filter_rate:.1f}% passed through)")
            logger.info(f"[time] {self.t.get_avg()}s processing per one reading")
            logger.info(f"[time] {self.t2.get_avg()}s outputing per one reading")


    def run(self, timeout=0, stop_event=None):
        """
        Continuously pops dicts from input_queue, each mapping sensor to reading,
        and processes all sensors in batch. Passes None to output_queue when done.
        If stop_event is provided (threading.Event), will break if stop_event.is_set().
        """
        batch_count = 0
        while True:
            if stop_event is not None and stop_event.is_set():
                logger.info("ChangeLevelDetector.run detected stop_event set, breaking loop.")
                self.output_queue.put("BATCH_END")
                self.output_queue.put(None)
                break
            if self.input_queue.empty():
                # logger.info("waiting for readings...")
                reading = self.input_queue.get()
                # logger.info("got readings")
            else:
                reading = self.input_queue.get()
            if reading is None:
                self.output_queue.put(None)
                break
            if batch_count % 200 == 0:
                log_memory_usage(f"ChangeDetector.run batch {batch_count}",buffer=self.input_queue, output_queue=self.output_queue)
                force_memory_cleanup()
                logger.info(f"processed {batch_count} batches")
            self.process_batch(reading)
            batch_count += 1