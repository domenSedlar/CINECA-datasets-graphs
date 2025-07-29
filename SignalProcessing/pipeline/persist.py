import json
import logging
import pandas as pd
import psutil
import os
import ctypes
import gc
import platform

from common.memory_utils import force_memory_cleanup, log_memory_usage, get_queue_state

from common.logger import Logger
logger = Logger(name=__name__.split('.')[-1], log_dir='logs').get_logger()

def default_serializer(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    return str(obj)

class StatePersister:
    def __init__(self, input_queue, output_file='latest_state.json', batch_write_size=25):
        self.input_queue = input_queue
        self.output_file = output_file
        self.batch_write_size = 25  # Reduced from 100 for more aggressive memory management
        self.state_buffer = []  # Buffer for batch writing

    def run(self, timeout=0):
        logger.info("Initilizing")
        rows_written = 0
        batch_count = 0
        while True:
            if self.input_queue.empty():
                logger.info("waiting, queue empty")
                state_data = self.input_queue.get()
                logger.info("continuing")
            else:
                state_data = self.input_queue.get()
            
            if state_data is None:
                # Flush any remaining states in buffer
                logger.info("No more data, flushing")
                if self.state_buffer:
                    self._write_batch(self.state_buffer)
                logger.info("Ending loop")
                break
            
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