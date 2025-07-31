import json
from collections import defaultdict
from queue import Queue
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd

class StateFileReader:
    def __init__(self, buffer, state_file='StateFiles/state.parquet'):
        self.state_file = state_file
        self.buffer = buffer

    def read_and_emit(self):
        """
        Reads the state file line by line and puts each line into the buffer.
        Each line contains a JSON object with node data.
        """

        pq_file = pq.ParquetFile(self.state_file)

        state = {}
        current_t = None

        # Read in very small chunks using pyarrow iter_batches
        for batch in pq_file.iter_batches(batch_size=100):
            batch_df = batch.to_pandas()
            for _, row in batch_df.iterrows():
                state[row["node"]] = row.to_dict()
                if current_t is None:
                    current_t = row["timestamp"]
                elif current_t != row["timestamp"]:
                    print(current_t)
                    self.buffer.put(state)
                    current_t = row["timestamp"]
                    
        self.buffer.put(state)
        self.buffer.put(None)


if __name__ == '__main__':
    STATE_FILE = 'StateFiles/threaded_pipeline_state.json'
    buffer = Queue()
    reader = StateFileReader(STATE_FILE, buffer)
    reader.read_and_emit()

    while True:
        state = buffer.get()
        if state is None:
            break
        print(state)
    