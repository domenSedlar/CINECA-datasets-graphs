import logging
logger = logging.getLogger(__name__)

class StateBuilder:
    def __init__(self, input_queue, output_queue, node_id):
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.node_id = node_id
        self.state = {}

    def run(self, timeout=0):
        while True:
            data_list = self.input_queue.pop(timeout=timeout)
            if data_list is None:
                logger.info(f"No data found in input queue for node {self.node_id}")
                break
            logger.info(f"Received batch of {len(data_list)} sensor updates for node {self.node_id}")
            for sensor_data in data_list:
                sensor = sensor_data.get('sensor')
                if sensor is not None:
                    logger.info(f"Updating state for sensor '{sensor}' on node {self.node_id}: {sensor_data}")
                    self.state[sensor] = sensor_data
            # Emit a compact state for this node
            if self.state:
                first_sensor = next(iter(self.state.values()))
                node = first_sensor.get('node')
                timestamp = first_sensor.get('timestamp')
                sensor_data = {k: v.get('value') for k, v in self.state.items()}
                compact_state = {
                    'node': node,
                    'timestamp': timestamp,
                    'sensor_data': sensor_data
                }
                logger.info(f"Pushing compact state for node {node} at {timestamp} with sensors: {list(sensor_data.keys())}")
                self.output_queue.push(compact_state)