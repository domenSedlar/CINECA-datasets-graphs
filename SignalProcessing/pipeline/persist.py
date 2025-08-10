import json
import logging
import pandas as pd
import psutil
import os
import ctypes
import gc
import platform
import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq

from common.memory_utils import force_memory_cleanup, log_memory_usage, get_queue_state

from common.logger import Logger
logger = Logger(name=__name__.split('.')[-1], log_dir='logs').get_logger_real()

def default_serializer(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    return str(obj)

class StatePersister:
    def __init__(self, input_queue, output_file='latest_state.json', batch_write_size=50):
        self.input_queue = input_queue
        self.output_file = output_file
        self.batch_write_size = batch_write_size
        self.state_buffer = []  # Buffer for batch writing

        self.writer = None
        self.schema = schema = pa.schema([
    pa.field("node", pa.int64(), nullable=True),
    pa.field("timestamp", pa.string(), nullable=True),
    pa.field("rack_id", pa.string(), nullable=True),
    pa.field("ambient_avg", pa.float64(), nullable=True),
    pa.field("dimm0_temp_avg", pa.float64(), nullable=True),
    pa.field("dimm10_temp_avg", pa.float64(), nullable=True),
    pa.field("dimm11_temp_avg", pa.float64(), nullable=True),
    pa.field("dimm12_temp_avg", pa.float64(), nullable=True),
    pa.field("dimm13_temp_avg", pa.float64(), nullable=True),
    pa.field("dimm14_temp_avg", pa.float64(), nullable=True),
    pa.field("dimm15_temp_avg", pa.float64(), nullable=True),
    pa.field("dimm1_temp_avg", pa.float64(), nullable=True),
    pa.field("dimm2_temp_avg", pa.float64(), nullable=True),
    pa.field("dimm3_temp_avg", pa.float64(), nullable=True),
    pa.field("dimm4_temp_avg", pa.float64(), nullable=True),
    pa.field("dimm5_temp_avg", pa.float64(), nullable=True),
    pa.field("dimm6_temp_avg", pa.float64(), nullable=True),
    pa.field("dimm7_temp_avg", pa.float64(), nullable=True),
    pa.field("dimm8_temp_avg", pa.float64(), nullable=True),
    pa.field("dimm9_temp_avg", pa.float64(), nullable=True),
    pa.field("fan0_0_avg", pa.float64(), nullable=True),
    pa.field("fan0_1_avg", pa.float64(), nullable=True),
    pa.field("fan1_0_avg", pa.float64(), nullable=True),
    pa.field("fan1_1_avg", pa.float64(), nullable=True),
    pa.field("fan2_0_avg", pa.float64(), nullable=True),
    pa.field("fan2_1_avg", pa.float64(), nullable=True),
    pa.field("fan3_0_avg", pa.float64(), nullable=True),
    pa.field("fan3_1_avg", pa.float64(), nullable=True),
    pa.field("fan_disk_power_avg", pa.float64(), nullable=True),
    pa.field("gpu0_core_temp_avg", pa.float64(), nullable=True),
    pa.field("gpu0_mem_temp_avg", pa.float64(), nullable=True),
    pa.field("gpu1_core_temp_avg", pa.float64(), nullable=True),
    pa.field("gpu1_mem_temp_avg", pa.float64(), nullable=True),
    pa.field("gpu3_core_temp_avg", pa.float64(), nullable=True),
    pa.field("gpu3_mem_temp_avg", pa.float64(), nullable=True),
    pa.field("gpu4_core_temp_avg", pa.float64(), nullable=True),
    pa.field("gpu4_mem_temp_avg", pa.float64(), nullable=True),
    pa.field("gv100card0_avg", pa.float64(), nullable=True),
    pa.field("gv100card1_avg", pa.float64(), nullable=True),
    pa.field("gv100card3_avg", pa.float64(), nullable=True),
    pa.field("gv100card4_avg", pa.float64(), nullable=True),
    pa.field("p0_core10_temp_avg", pa.float64(), nullable=True),
    pa.field("p0_core11_temp_avg", pa.float64(), nullable=True),
    pa.field("p0_core12_temp_avg", pa.float64(), nullable=True),
    pa.field("p0_core13_temp_avg", pa.float64(), nullable=True),
    pa.field("p0_core14_temp_avg", pa.float64(), nullable=True),
    pa.field("p0_core15_temp_avg", pa.float64(), nullable=True),
    pa.field("p0_core16_temp_avg", pa.float64(), nullable=True),
    pa.field("p0_core17_temp_avg", pa.float64(), nullable=True),
    pa.field("p0_core18_temp_avg", pa.float64(), nullable=True),
    pa.field("p0_core19_temp_avg", pa.float64(), nullable=True),
    pa.field("p0_core20_temp_avg", pa.float64(), nullable=True),
    pa.field("p0_core21_temp_avg", pa.float64(), nullable=True),
    pa.field("p0_core22_temp_avg", pa.float64(), nullable=True),
    pa.field("p0_core23_temp_avg", pa.float64(), nullable=True),
    pa.field("p0_core8_temp_avg", pa.float64(), nullable=True),
    pa.field("p0_core9_temp_avg", pa.float64(), nullable=True),
    pa.field("p0_io_power_avg", pa.float64(), nullable=True),
    pa.field("p0_mem_power_avg", pa.float64(), nullable=True),
    pa.field("p0_power_avg", pa.float64(), nullable=True),
    pa.field("p0_vdd_temp_avg", pa.float64(), nullable=True),
    pa.field("p1_core10_temp_avg", pa.float64(), nullable=True),
    pa.field("p1_core11_temp_avg", pa.float64(), nullable=True),
    pa.field("p1_core14_temp_avg", pa.float64(), nullable=True),
    pa.field("p1_core15_temp_avg", pa.float64(), nullable=True),
    pa.field("p1_core18_temp_avg", pa.float64(), nullable=True),
    pa.field("p1_core19_temp_avg", pa.float64(), nullable=True),
    pa.field("p1_core20_temp_avg", pa.float64(), nullable=True),
    pa.field("p1_core21_temp_avg", pa.float64(), nullable=True),
    pa.field("p1_core2_temp_avg", pa.float64(), nullable=True),
    pa.field("p1_core3_temp_avg", pa.float64(), nullable=True),
    pa.field("p1_core4_temp_avg", pa.float64(), nullable=True),
    pa.field("p1_core5_temp_avg", pa.float64(), nullable=True),
    pa.field("p1_core6_temp_avg", pa.float64(), nullable=True),
    pa.field("p1_core7_temp_avg", pa.float64(), nullable=True),
    pa.field("p1_core8_temp_avg", pa.float64(), nullable=True),
    pa.field("p1_core9_temp_avg", pa.float64(), nullable=True),
    pa.field("p1_io_power_avg", pa.float64(), nullable=True),
    pa.field("p1_mem_power_avg", pa.float64(), nullable=True),
    pa.field("p1_power_avg", pa.float64(), nullable=True),
    pa.field("p1_vdd_temp_avg", pa.float64(), nullable=True),
    pa.field("pcie_avg", pa.float64(), nullable=True),
    pa.field("ps0_input_power_avg", pa.float64(), nullable=True),
    pa.field("ps0_input_voltag_avg", pa.float64(), nullable=True),
    pa.field("ps0_output_curre_avg", pa.float64(), nullable=True),
    pa.field("ps0_output_volta_avg", pa.float64(), nullable=True),
    pa.field("ps1_input_power_avg", pa.float64(), nullable=True),
    pa.field("ps1_input_voltag_avg", pa.float64(), nullable=True),
    pa.field("ps1_output_curre_avg", pa.float64(), nullable=True),
    pa.field("ps1_output_volta_avg", pa.float64(), nullable=True),
    pa.field("total_power_avg", pa.float64(), nullable=True),
    pa.field("value", pa.float64(), nullable=True),
])

    def run(self, timeout=0):
        logger.info("Initilizing")
        rows_written = 0
        batch_count = 0
        while True:
            if self.input_queue.empty():
                # logger.info("waiting, queue empty")
                state_data = self.input_queue.get()
                # logger.info("continuing")
            else:
                state_data = self.input_queue.get()
            
            if state_data is None:
                # Flush any remaining states in buffer
                logger.info("No more data, flushing")
                if self.state_buffer:
                    self._write_batch(self.state_buffer)
                    gc.collect()
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
            
            if rows_written % 500 == 0:
                logger.info(f"StatePersister: Written {rows_written} rows.")
                force_memory_cleanup()
            batch_count += 1

        self.writer.close()

        
    
    def _write_batch(self, states_batch):
        """Write a batch of states to file, maintaining order"""
        # logger.info("writting")
        flat_data = states_batch

        df = pd.DataFrame(flat_data)
        table = pa.Table.from_pandas(df, schema=self.schema)

        if self.writer is None:
            self.writer = pq.ParquetWriter(
                self.output_file,
                self.schema,
                use_dictionary=True,
            )
        self.writer.write_table(table)
        del df, flat_data, table
        # logger.info("wrote")