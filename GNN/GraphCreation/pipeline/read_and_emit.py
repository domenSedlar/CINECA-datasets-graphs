import json
from collections import defaultdict
from queue import Queue
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
from copy import deepcopy
import datetime


class StateFileReader:
    def __init__(self, buffer, state_file='StateFiles/state.parquet', val_file=None, rows_in_mem=64, skip_None=True):
        self.state_file = state_file
        self.buffer = buffer
        self.val_gen = self._init_val_gen(val_file, rows_in_mem, skip_None)
        self.curr_val = next(self.val_gen)

    def _init_val_gen(self, val_file, rows_in_mem, skip_None):
        pq_file = pq.ParquetFile(val_file)

        def row_generator(pq_file, batch_size=rows_in_mem, skip_None=skip_None):
            pq_file  # Make pq_file accessible in this function
            # Read in very small chunks using pyarrow iter_batches
            for batch in pq_file.iter_batches(batch_size=batch_size, columns=["value", "timestamp"]):
                vals = batch.column("value")
                timestamps = batch.column("timestamp")
                for i, ts in enumerate(timestamps):
                    if skip_None and not vals[i].is_valid:
                        continue
                    yield {"timestamp": ts, "value": vals[i]}
        
        return row_generator(pq_file)
    
    def _next_val(self, ts): # TODO add parameter for how far we look
        """
            return value, from the time interval after ts
        """
        if isinstance(ts, str):
            ts = datetime.datetime.fromisoformat(ts)

        try:
            while self.curr_val["timestamp"].as_py() <= ts:
                self.curr_val = next(self.val_gen)
        except StopIteration:
            # if generator is exhausted; just keep returning the last value
            pass # TODO what *should* we return after running out of data?
        
        return self.curr_val["value"]

    def read_and_emit(self, stop_event=None, num_limit=None, lim_nodes={2}, skip_None=False): # TODO modify this method to correctly handle multiple nodes
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

                if current_t is None:
                    current_t = ts
                elif ts != current_t:
                    self.buffer.put(deepcopy(state)) # TODO is deepcopy necessary?
                    state.clear()
                    current_t = ts
                    count += 1

                state[int(node_values[i])] = all_rows[i] # TODO node_values might not be int
                state[int(node_values[i])]["value"] = float(self._next_val(ts))

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
    