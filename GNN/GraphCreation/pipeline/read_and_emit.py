import json
from collections import defaultdict
from queue import Queue
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow as pa
import pandas as pd
from copy import deepcopy
import datetime


class StateFileReader:
    def __init__(self, buffer, state_file=['StateFiles/state.parquet'], val_file=None, rows_in_mem=64, skip_None=True):
        self.state_file = state_file
        self.buffer = buffer
        
        self.curr_val = {}
        self.val_gen = {}
        for f in val_file:
            id = int(f.split('/')[-1].split('.')[0])
            self.val_gen[id] = self._init_val_gen(f, rows_in_mem, skip_None)
            self.curr_val[id] = next(self.val_gen[id])

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
    
    def _next_val(self, id, ts, max_dist_scalar, time_diff_buff=None):
        """
            Return the next available value after the given timestamp `ts`.
            max_dist_scalar ~ The maximum allowed gap between `ts` and the next available timestamp.
                If the gap exceeds this threshold(15min * max_dist_scalar), `None` is returned.
                If the gap is larger it might no longer be relevant
        """
        if isinstance(ts, str):
            ts = datetime.datetime.fromisoformat(ts)

        try:
            while self.curr_val[id]["timestamp"].as_py() <= ts:
                self.curr_val[id] = next(self.val_gen[id])
        except StopIteration:
            return None
            pass # TODO what *should* we return after running out of data?
        
        # check distance
        if time_diff_buff is not None:
            td = self.curr_val[id]["timestamp"].as_py() - ts
            if td <= datetime.timedelta(hours=48):
                time_diff_buff.put({"diff": td.total_seconds()//60, "node": id, "original_timestamp": ts})

        if self.curr_val[id]["timestamp"].as_py() - ts > datetime.timedelta(minutes=15 * max_dist_scalar):
            return None

        return self.curr_val[id]["value"]

    def read_and_emit(self, start_ts=None, end_ts=None, stop_event=None, num_limit=None, lim_nodes=[2], skip_None=True, max_dist_scalar=8, time_diff_buff=None):
        """
        Reads the state file line by line and puts each line into the buffer.
        Each line contains a JSON object with node data.
        """

        pq_dataset = ds.dataset(self.state_file)
        s_ids = [str(i) for i in lim_nodes]
        scanner = pq_dataset.scanner(batch_size=100, filter=(pc.field("timestamp") >= str(start_ts)) & (pc.field("timestamp") < str(end_ts)) & (pc.field("node").isin(lim_nodes))) # TODO make this filter better

        state = {}
        current_t = None
        b = False
        count = 0

        for batch in scanner.to_reader():
            if stop_event and stop_event.is_set():
                print("reader: detected stop_event set, breaking loop.")
                break
            # Convert to Python objects column-wise without Pandas
            timestamps = batch.column("timestamp")
            nodes = batch.column("node")
            
            # Convert once for the entire batch to Python scalars
            # Avoid per-row overhead
            ts_values = timestamps.to_pylist()
            node_ids = nodes.to_pylist()

            # Precompute the row dicts once
            # This avoids deepcopies of the same Arrow Row multiple times
            all_rows = batch.to_pylist()

            for i, ts in enumerate(ts_values):
                if nodes is not None and not (int(nodes[i]) in lim_nodes): # so we can limit to certain nodes
                    continue

                val = self._next_val(node_ids[i], ts, max_dist_scalar, time_diff_buff=time_diff_buff)
                if val is None and skip_None:
                    continue

                if current_t is None:
                    current_t = ts
                elif ts != current_t:
                    self.buffer.put(deepcopy(state))
                    state.clear()
                    current_t = ts
                    count += 1

                state[int(node_ids[i])] = all_rows[i]
                state[int(node_ids[i])]["value"] = int(val) # TODO what if value is None

            if b:
                break

            if num_limit is not None and count >= num_limit:
                print("reached row limit in persist")
                break

        self.buffer.put(state)
        self.buffer.put(None)

        if time_diff_buff is not None:
            time_diff_buff.put(None)


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
    