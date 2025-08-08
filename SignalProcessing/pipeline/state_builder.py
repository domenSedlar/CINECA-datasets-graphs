import copy

from common.memory_utils import log_memory_usage, force_memory_cleanup
from common.logger import Logger
logger = Logger(name=__name__.split('.')[-1], log_dir='logs').get_logger()

class StateBuilder:
    def __init__(self, input_queue, output_queue, batch_size=5, max_queue_size=50):
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.states = {} # {node_id: {timestamp: timestamp, rack_id: rack_id, sensor_data: {sensor_id: value, ...}}, ...}
        self.batch_size = batch_size
        self.max_queue_size = max_queue_size
        self.pending_states = []  # Buffer for batch writing

    def run(self, timeout=0):
        logger.info("StateBuilder: Starting up")
        batch_count = 0
        while True:
            data_list = self.input_queue.get()
            if data_list is None:
                # Flush any remaining states in batch
                if self.pending_states:
                    self.output_queue.put(self.pending_states)
                self.output_queue.put(None)
                logger.debug(f"No data found in input queue")
                break

            logger.debug(f"Received batch of {len(data_list)} sensor updates")
            
            if isinstance(data_list, dict):
                sensor_data = data_list
                sensor = sensor_data.get('sensor')
                node = sensor_data.get('node')
                value = sensor_data.get('value')
                timestamp = sensor_data.get('timestamp')
                rack_id = sensor_data.get('rack_id')

                # If node is not in states, always create it and write the sensor value (even if value is None)
                if node not in self.states:
                    self.states[node] = {
                        'node': int(node),
                        'timestamp': timestamp,
                        'rack_id': rack_id,
                        sensor: value
                    }
                    continue  # skip the rest, as we've already written the value (even if None)

                self.states[node]['timestamp'] = timestamp

                # If sensor is not in this node's sensor_data, write the value (even if None)
                if sensor not in self.states[node]:
                    self.states[node][sensor] = value
                    continue

                # Otherwise, only update if all fields are present (i.e., value is not None)
                if None in (sensor, node, value):
                    # logger.warning(f"Incomplete sensor data: {sensor_data}")
                    continue

                # Always update timestamp and rack_id to the latest, and update sensor value

                self.states[node][sensor] = value

            # Add current states to pending batch
            if data_list == "BATCH_END":
                batch_count += 1

                for node, d in self.states.items():

                    self.pending_states.append(copy.copy(d)) # d = {node: , rack_id: , timestamp: , sensor1: , sensor2: ,...}
                
                            # Log more frequently to debug
                if batch_count % 100 == 0:  # Changed from 100 to 10 for more frequent logging
                    log_memory_usage(f"StateBuilder.run batch {batch_count}", input_queue=self.input_queue, output_queue=self.output_queue)
            # If batch is full or queue is getting large, flush the batch
            if len(self.pending_states) >= self.batch_size or self.output_queue.qsize() > self.max_queue_size // 2:
                if self.pending_states:
                    if self.output_queue.full():
                        logger.info("full")
                        self.output_queue.put(copy.copy(self.pending_states))
                        logger.info("continuing")
                    else:
                        self.output_queue.put(copy.copy(self.pending_states))
                        
                    logger.debug(f"StateBuilder: Flushed batch of {len(self.pending_states)} states, queue size: {self.output_queue.qsize()}")

                    self.pending_states = []
                    # Force memory cleanup after batch writing
                    force_memory_cleanup()
            
