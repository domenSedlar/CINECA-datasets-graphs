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
        self._init_schema() # Creates two schemas named self.schema and self.other_schema. The second one is used only for the tar file with the fan data named "other.tar".


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
        
        self.writer.close()

        
    
    def _write_batch(self, states_batch):
        """Write a batch of states to file, maintaining order"""
        schema = self.other_schema

        flat_data = []
        for state in states_batch:
            for _, entery in state.items():
                flat_data.append(entery)

        v = flat_data[0]['value']
        flat_data[0]['value'] = 0
        df = pd.DataFrame(flat_data)
        table = pa.Table.from_pandas(df, schema=schema)

        if self.writer is None:


            self.writer = pq.ParquetWriter(
                self.output_file,
                schema,
                use_dictionary=True,
            )
        self.writer.write_table(table)

    def _init_schema(self):
        """
            initilizes two schemas for the parquet files. 
            the first is used for most nodes, 
            while the second one is used for fan data found in the "other.tar" file
        """
        self.schema = pa.schema([
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
        columns_for_other = [
        "ambient_avg", "ambient_std", "ambient_min", "ambient_max",
        "fan0_0_avg", "fan0_0_std", "fan0_0_min", "fan0_0_max",
        "fan0_1_avg", "fan0_1_std", "fan0_1_min", "fan0_1_max",
        "fan1_0_avg", "fan1_0_std", "fan1_0_min", "fan1_0_max",
        "fan1_1_avg", "fan1_1_std", "fan1_1_min", "fan1_1_max",
        "fan2_0_avg", "fan2_0_std", "fan2_0_min", "fan2_0_max",
        "fan2_1_avg", "fan2_1_std", "fan2_1_min", "fan2_1_max",
        "fan3_0_avg", "fan3_0_std", "fan3_0_min", "fan3_0_max",
        "fan3_1_avg", "fan3_1_std", "fan3_1_min", "fan3_1_max",
        "gv100card0_avg", "gv100card0_std", "gv100card0_min", "gv100card0_max",
        "gv100card1_avg", "gv100card1_std", "gv100card1_min", "gv100card1_max",
        "gv100card3_avg", "gv100card3_std", "gv100card3_min", "gv100card3_max",
        "gv100card4_avg", "gv100card4_std", "gv100card4_min", "gv100card4_max",
        "p0_vdd_temp_avg", "p0_vdd_temp_std", "p0_vdd_temp_min", "p0_vdd_temp_max",
        "p1_vdd_temp_avg", "p1_vdd_temp_std", "p1_vdd_temp_min", "p1_vdd_temp_max",
        "pcie_avg", "pcie_std", "pcie_min", "pcie_max",
        "ps0_input_power_avg", "ps0_input_power_std", "ps0_input_power_min", "ps0_input_power_max",
        "ps0_input_voltag_avg", "ps0_input_voltag_std", "ps0_input_voltag_min", "ps0_input_voltag_max",
        "ps0_output_curre_avg", "ps0_output_curre_std", "ps0_output_curre_min", "ps0_output_curre_max",
        "ps0_output_volta_avg", "ps0_output_volta_std", "ps0_output_volta_min", "ps0_output_volta_max",
        "ps1_input_power_avg", "ps1_input_power_std", "ps1_input_power_min", "ps1_input_power_max",
        "ps1_input_voltag_avg", "ps1_input_voltag_std", "ps1_input_voltag_min", "ps1_input_voltag_max",
        "ps1_output_curre_avg", "ps1_output_curre_std", "ps1_output_curre_min", "ps1_output_curre_max",
        "ps1_output_volta_avg", "ps1_output_volta_std", "ps1_output_volta_min", "ps1_output_volta_max",
    ]

        # Create an empty DataFrame with all float columns
        self.other_schema = pa.schema(
            [
                pa.field("node", pa.int64(), nullable=True),
                pa.field("timestamp", pa.string(), nullable=True),
                pa.field("rack_id", pa.string(), nullable=True),]
            + [pa.field(col, pa.float64(), nullable=True) for col in columns_for_other if "avg" in col]
            + [
                pa.field("value", pa.float64(), nullable=True),
                pa.field("__index_level_0__", pa.float64(), nullable=True),
            ]
            )
