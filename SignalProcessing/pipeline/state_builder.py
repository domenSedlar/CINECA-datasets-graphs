import logging
import copy
import psutil
import os
import time
import ctypes
import gc
import platform
logger = logging.getLogger(__name__)

def log_memory_usage(context="StateBuilder.run", input_queue=None, output_queue=None, input_var_name="input_queue", output_var_name="output_queue"):
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    in_size = input_queue.qsize() if input_queue is not None else 'NA'
    out_size = output_queue.qsize() if output_queue is not None else 'NA'
    print(f"[MEMORY]\t\t{context}\t\tRSS={mem_info.rss/1024/1024:.2f}MB\t\tVMS={mem_info.vms/1024/1024:.2f}MB\t\tThreads={process.num_threads()}\t\t{input_var_name}={in_size}\t\t{output_var_name}={out_size}", flush=True)

def force_memory_cleanup():
    """Cross-platform memory cleanup"""
    # Force garbage collection
    gc.collect()
    
    # Platform-specific memory release
    if platform.system() == "Linux":
        try:
            ctypes.CDLL("libc.so.6").malloc_trim(0)
        except Exception:
            pass  # malloc_trim not available
    elif platform.system() == "Windows":
        try:
            # Windows equivalent - use kernel32 to release memory
            kernel32 = ctypes.windll.kernel32
            kernel32.SetProcessWorkingSetSize(-1, -1, -1)
        except Exception:
            pass  # Windows memory management not available

class StateBuilder:
    def __init__(self, input_queue, output_queue, max_queue_size=1000, batch_size=10):
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.states = {} # {node_id: {timestamp: timestamp, rack_id: rack_id, sensor_data: {sensor_id: value, ...}}, ...}
        self.max_queue_size = 50  # Reduced from 100 for more aggressive backpressure
        self.batch_size = 5  # Reduced from 10 for more aggressive memory management
        self.pending_states = []  # Buffer for batch writing

    def run(self, timeout=0):
        logger.info("StateBuilder: Starting up")
        batch_count = 0
        while True:
            # Check backpressure - if output queue is too full, wait
            while self.output_queue.qsize() > self.max_queue_size:
                time.sleep(0.1)  # Wait 100ms before checking again
                logger.debug(f"StateBuilder: Backpressure - queue size {self.output_queue.qsize()} > {self.max_queue_size}")
            
            data_list = self.input_queue.get()
            if data_list is None:
                # Flush any remaining states in batch
                if self.pending_states:
                    self.output_queue.put(copy.deepcopy(self.pending_states))
                self.output_queue.put(None)
                logger.debug(f"No data found in input queue")
                break
            logger.debug(f"Received batch of {len(data_list)} sensor updates")
            
            for sensor_data in data_list:
                sensor = sensor_data.get('sensor')
                node = sensor_data.get('node')
                value = sensor_data.get('value')
                timestamp = sensor_data.get('timestamp')
                rack_id = sensor_data.get('rack_id')

                # If node is not in states, always create it and write the sensor value (even if value is None)
                if node not in self.states:
                    self.states[node] = {
                        'timestamp': timestamp,
                        'rack_id': rack_id,
                        'sensor_data': {sensor: value}
                    }
                    continue  # skip the rest, as we've already written the value (even if None)

                # If sensor is not in this node's sensor_data, write the value (even if None)
                if sensor not in self.states[node]['sensor_data']:
                    self.states[node]['timestamp'] = timestamp
                    self.states[node]['rack_id'] = rack_id
                    self.states[node]['sensor_data'][sensor] = value
                    continue

                # Otherwise, only update if all fields are present (i.e., value is not None)
                if None in (sensor, node, value, timestamp, rack_id):
                    logger.warning(f"Incomplete sensor data: {sensor_data}")
                    continue

                # Always update timestamp and rack_id to the latest, and update sensor value
                self.states[node]['timestamp'] = timestamp
                self.states[node]['rack_id'] = rack_id
                self.states[node]['sensor_data'][sensor] = value

            # Add current states to pending batch
            self.pending_states.append(copy.deepcopy(self.states))
            
            # If batch is full or queue is getting large, flush the batch
            if len(self.pending_states) >= self.batch_size or self.output_queue.qsize() > self.max_queue_size // 2:
                if self.pending_states:
                    self.output_queue.put(copy.deepcopy(self.pending_states))
                    self.pending_states = []
                    # Force memory cleanup after batch writing
                    force_memory_cleanup()
                    logger.debug(f"StateBuilder: Flushed batch of {len(self.pending_states)} states, queue size: {self.output_queue.qsize()}")
            
            batch_count += 1
            # Log more frequently to debug
            if batch_count % 10 == 0:  # Changed from 100 to 10 for more frequent logging
                log_memory_usage(f"StateBuilder.run batch {batch_count}", input_queue=self.input_queue, output_queue=self.output_queue, input_var_name="change_queue", output_var_name="state_queue")