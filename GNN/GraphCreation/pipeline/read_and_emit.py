import json
from collections import defaultdict
from queue import Queue
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
from copy import deepcopy

class StateFileReader:
    def __init__(self, buffer, state_file='StateFiles/state.parquet'):
        self.state_file = state_file
        self.buffer = buffer

    def read_and_emit(self, stop_event=None, num_limit=None, lim_nodes=None, skip_None=False):
        """
        Reads the state file line by line and puts each line into the buffer.
        Each line contains a JSON object with node data.
        """

        pq_file = pq.ParquetFile(self.state_file)

        state = {}
        current_t = None

        count = 0

        for batch in pq_file.iter_batches(batch_size=100):
            if stop_event and stop_event.is_set():
                print("reader: detected stop_event set, breaking loop.")
                break
            # Convert to Python objects column-wise without Pandas
            timestamps = batch.column("timestamp")
            nodes = batch.column("node")
            vals = batch.column("value")
            
            # Convert once for the entire batch to Python scalars
            # Avoid per-row overhead
            ts_values = timestamps.to_pylist()
            node_values = nodes.to_pylist()

            # Precompute the row dicts once
            # This avoids deepcopies of the same Arrow Row multiple times
            all_rows = batch.to_pylist()

            for i, ts in enumerate(ts_values):
                
                if nodes is not None and not (int(nodes[i]) in lim_nodes): # so we can limit to certain nodes
                    continue
                if skip_None and not (vals[i].is_valid):
                    continue
                # print(vals[i])
                if current_t is None:
                    current_t = ts
                elif ts != current_t:
                    self.buffer.put(deepcopy(state))
                    state.clear()
                    current_t = ts
                    count += 1

                state[int(node_values[i])] = all_rows[i] # TODO node_values might not be int
            
            if num_limit is not None and count >= num_limit:
                print("reached row limit in persist")
                break

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
    