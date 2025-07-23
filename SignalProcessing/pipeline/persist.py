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
        rows_written = 0
        while True:
            state = self.input_queue.get()
            if state is None:
                break
            logger.debug(f"Received state from input_queue in StatePersister")
            with open(self.output_file, 'a') as f:
                logger.debug(f"Writing state to {self.output_file}")
                f.write(json.dumps(state, default=default_serializer) + '\n')
            rows_written += 1
            if rows_written % 1000 == 0:
                logger.info(f"StatePersister: Written {rows_written} rows.")