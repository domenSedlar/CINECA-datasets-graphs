import json
import logging
import pandas as pd
logger = logging.getLogger(__name__)

def default_serializer(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    return str(obj)

class StatePersister:
    def __init__(self, input_queue, output_file='latest_state.json'):
        self.input_queue = input_queue
        self.output_file = output_file

    def run(self, timeout=0):
        while True:
            state = self.input_queue.pop(timeout=timeout)
            logger.info(f"Received state from input_queue in StatePersister")
            if state is None:
                logger.info(f"No state to write to {self.output_file}")
                break
            with open(self.output_file, 'a') as f:
                logger.info(f"Writing state to {self.output_file}")
                f.write(json.dumps(state, default=default_serializer) + '\n')