import logging
logger = logging.getLogger(__name__)

class StateBuilder:
    def __init__(self, input_queue, output_queue):
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.states = {} # {node_id: {timestamp: timestamp, rack_id: rack_id, sensor_data: {sensor_id: value, ...}}, ...}

    def run(self, timeout=0):
        while True:
            data_list = self.input_queue.pop(timeout=timeout)
            if data_list is None:
                logger.info(f"No data found in input queue")
                break
            logger.info(f"Received batch of {len(data_list)} sensor updates")
            
            for sensor_data in data_list:
                sensor = sensor_data.get('sensor')
                node = sensor_data.get('node')
                value = sensor_data.get('value')
                timestamp = sensor_data.get('timestamp')
                rack_id = sensor_data.get('rack_id')
                if None in (sensor, node, value, timestamp, rack_id):
                    logger.warning(f"Incomplete sensor data: {sensor_data}")
                    continue
                if node not in self.states:
                    self.states[node] = {
                        'timestamp': timestamp,
                        'rack_id': rack_id,
                        'sensor_data': {}
                    }
                # Always update timestamp and rack_id to the latest
                self.states[node]['timestamp'] = timestamp
                self.states[node]['rack_id'] = rack_id
                self.states[node]['sensor_data'][sensor] = value
                logger.info(f"Updating state for sensor '{sensor}' on node {node}: value={value}, timestamp={timestamp}, rack_id={rack_id}")

            self.output_queue.push(self.states)