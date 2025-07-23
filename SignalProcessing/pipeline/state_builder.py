import logging
import copy
logger = logging.getLogger(__name__)

class StateBuilder:
    def __init__(self, input_queue, output_queue):
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.states = {} # {node_id: {timestamp: timestamp, rack_id: rack_id, sensor_data: {sensor_id: value, ...}}, ...}

    def run(self, timeout=0):
        while True:
            data_list = self.input_queue.get()
            if data_list is None:
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

            self.output_queue.put(copy.deepcopy(self.states))