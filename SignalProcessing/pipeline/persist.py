import json
import logging
import pandas as pd
import psutil
import os
import ctypes
import gc
import platform
from pipeline.memory_utils import force_memory_cleanup
logger = logging.getLogger(__name__)

def default_serializer(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    return str(obj)

def log_memory_usage(context="StatePersister.run", input_queue=None, var_name="input_queue"):
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    in_size = input_queue.qsize() if input_queue is not None else 'NA'
    print(f"[MEMORY]\t\t{context}\t\tRSS={mem_info.rss/1024/1024:.2f}MB\t\tVMS={mem_info.vms/1024/1024:.2f}MB\t\tThreads={process.num_threads()}\t\t{var_name}={in_size}", flush=True)

class StatePersister:
    def __init__(self, input_queue, output_file='latest_state.json', batch_write_size=25):
        self.input_queue = input_queue
        self.output_file = output_file
        self.batch_write_size = 25  # Reduced from 100 for more aggressive memory management
        self.state_buffer = []  # Buffer for batch writing

    def run(self, timeout=0):
        rows_written = 0
        batch_count = 0
        while True:
            state_data = self.input_queue.get()
            if state_data is None:
                # Flush any remaining states in buffer
                if self.state_buffer:
                    self._write_batch(self.state_buffer)
                break
            logger.debug(f"Received state from input_queue in StatePersister")
            
            # Handle both single states and batched states
            if isinstance(state_data, list):
                # Batched states from StateBuilder
                for state in state_data:
                    self.state_buffer.append(state)
                    rows_written += 1
            else:
                # Single state (backward compatibility)
                self.state_buffer.append(state_data)
                rows_written += 1
            
            # Write batch if buffer is full
            if len(self.state_buffer) >= self.batch_write_size:
                self._write_batch(self.state_buffer)
                self.state_buffer = []
                # Force memory cleanup after batch writing
                force_memory_cleanup()
            
            if rows_written % 1000 == 0:
                logger.info(f"StatePersister: Written {rows_written} rows.")
            batch_count += 1
            if batch_count % 100 == 0:
                log_memory_usage(f"StatePersister.run batch {batch_count}", input_queue=self.input_queue, var_name="state_queue")
    
    def _write_batch(self, states_batch):
        """Write a batch of states to file, maintaining order"""
        with open(self.output_file, 'a') as f:
            for state in states_batch:
                f.write(json.dumps(state, default=default_serializer) + '\n')
        logger.debug(f"StatePersister: Wrote batch of {len(states_batch)} states to {self.output_file}")