import os
import tarfile
import io
import pyarrow.parquet as pq
import pandas as pd
import logging
from pipeline.file_reading.node_sensor_manager import NodeSensorManager
from simple_queue import SimpleQueue as Queue
import json
import time
import copy
import psutil
import ctypes
import gc
import platform
from common.memory_utils import log_memory_usage, get_queue_state, force_memory_cleanup
import datetime

from common.logger import Logger
logger = Logger(name=__name__.split('.')[-1], log_dir='logs').get_logger_real()

class NodeManager:
    def __init__(self, buffer: Queue, tarfiles_path='./TarFiles/', interval_seconds=60*15, limit_nodes=None, rows_in_mem=10, temp_dir="D:/temp_parquet_files", limit_racks=False):
        self.files_path = tarfiles_path # only read the first 2 racks for testing
        self.interval_seconds = interval_seconds
        self.buffer = buffer
        self.sensor_columns = None # limits which sensors we read
        self.setup_par_files(rows_in_mem, temp_dir, limit_nodes, limit_racks=limit_racks)
        
    def setup_tar_files(self, rows_in_mem, temp_dir, limit_nodes):
                # Scan all tar files and .parquet members in a single pass, collecting node info and earliest timestamp
        nodes = []
        tar_paths = []
        earliest_timestamp = None
        table = None

        max_rows = 0
        max_file = None
        node_expected_rows = {}  # node_id -> expected row count
        node_processed_rows = {}  # node_id -> processed row count

        for tar_filename in os.listdir(self.files_path):
            if not tar_filename.endswith('.tar'): 
                continue
            if '-' in tar_filename: # skip tar files that are not for a single rack
                continue

            tar_path = os.path.join(self.files_path, tar_filename)
            try:
                with tarfile.open(tar_path, 'r') as tar:
                    for member in tar.getmembers():
                        if member.isfile() and member.name.endswith('.parquet'):
                            base = os.path.basename(member.name)
                            node_id_str = base.split('.')[0] # extract node_id from filename
                            try:
                                node_id = int(node_id_str)
                            except ValueError:
                                logger.warning(f"Skipping file with invalid node_id: {base}")
                                continue
                            nodes.append(node_id)
                            tar_paths.append(tar_path)
                            # Find earliest timestamp in this parquet file

                            earliest_timestamp = datetime.datetime.fromisoformat("2020-03-09 11:45:00+00:00")
                            max_rows = 86650

                            continue

                            try:
                                file_obj = tar.extractfile(member)
                                if file_obj is None:
                                    continue
                                parquet_bytes = file_obj.read()
                                table = pq.ParquetFile(io.BytesIO(parquet_bytes))
                                first_batch = next(table.iter_batches(batch_size=1, columns=['timestamp']))
                                df = first_batch.to_pandas()
                                if not df.empty:
                                    min_ts = df['timestamp'].iloc[0]
                                    if not pd.isnull(min_ts):
                                        if earliest_timestamp is None or min_ts < earliest_timestamp:
                                            earliest_timestamp = min_ts
                                # Count rows in this parquet file
                                nrows = table.metadata.num_rows if table.metadata is not None else None
                                if nrows is not None:
                                    node_expected_rows[node_id] = nrows
                                    node_processed_rows[node_id] = 0
                                if nrows is not None and nrows > max_rows:
                                    max_rows = nrows
                                    max_file = f'{tar_filename}:{member.name}'
                            except Exception as e:
                                logger.warning(f"Failed to read timestamp from {member.name} in {tar_path}: {e}")
                if(limit_nodes is not None and len(nodes) > limit_nodes):
                    break
            except Exception as e:
                logger.error(f"Error processing tar file {tar_path}: {e}")

        logger.info(f"Earliest measurement timestamp across all files: {earliest_timestamp}")
        logger.info(f"File with most rows: {max_file} with {max_rows} rows")
        self.earliest_timestamp = earliest_timestamp

        self.node_expected_rows = node_expected_rows
        self.node_processed_rows = node_processed_rows

        if table is not None: # only read the avg columns, not the min/max/std
            all_columns = table.schema.names
            sensor_columns = [col for col in all_columns if col != 'timestamp' and not (col.endswith('_min') or col.endswith('_max') or col.endswith('_std'))]
            self.sensor_columns = sensor_columns

        # Create NodeSensorManager for each node
        self.node_managers = {}
        i = 0
        for node_id, tar_path in zip(nodes, tar_paths):
            i+=1
            rack_id = os.path.splitext(os.path.basename(tar_path))[0]
            self.node_managers[node_id] = NodeSensorManager(
                node_id=node_id,
                tar_path=tar_path,
                rack_id=rack_id,
                current_time=earliest_timestamp,
                sensor_columns=self.sensor_columns,
                interval_seconds=self.interval_seconds,
                rows_in_mem=rows_in_mem,
                temp_dir=temp_dir
            )
            if i % 100 == 0:
                logger.info(f"intilized {i} nodes")
            # Force memory cleanup after creating each manager
            force_memory_cleanup()
        
        # Force garbage collection after all managers are created
        force_memory_cleanup()

        # self.node_expected_rows = node_expected_rows
        # self.node_processed_rows = node_processed_rows

    def setup_par_files(self, rows_in_mem, temp_dir, limit_nodes, limit_racks=False):
        # Scan all tar files and .parquet members in a single pass, collecting node info and earliest timestamp
        nodes = []
        file_paths = []
        earliest_timestamp = datetime.datetime.fromisoformat("2020-03-09 11:45:00+00:00")
        max_rows = 86650
        table = None

        max_file = "15.parquet"
        node_expected_rows = {}  # node_id -> expected row count
        node_processed_rows = {}  # node_id -> processed row count

        for folder in os.listdir(self.files_path):
            if (not os.path.basename(folder) == "0") and limit_racks:
                continue
            if not os.path.isdir(os.path.join(self.files_path, folder)): 
                continue
            if '-' in folder: # skip tar files that are not for a single rack
                continue

            for file in os.listdir(os.path.join(self.files_path, folder)):
                if(limit_nodes is not None and len(nodes) > limit_nodes):
                    break
                if not file.endswith(".parquet"):
                    continue
                node_id_str = file.split('.')[0]
                try:
                    node_id = int(node_id_str)
                except ValueError:
                    logger.warning(f"Skipping file with invalid node_id: {node_id_str}")
                    continue
                nodes.append(node_id)
                file_paths.append(os.path.join(self.files_path, folder, file))

        logger.info(f"Earliest measurement timestamp across all files: {earliest_timestamp}")
        logger.info(f"File with most rows: {max_file} with {max_rows} rows")
        self.earliest_timestamp = earliest_timestamp

        self.node_expected_rows = node_expected_rows
        self.node_processed_rows = node_processed_rows

        if table is not None: # only read the avg columns, not the min/max/std
            all_columns = table.schema.names
            sensor_columns = [col for col in all_columns if col != 'timestamp' and not (col.endswith('_min') or col.endswith('_max') or col.endswith('_std'))]
            self.sensor_columns = sensor_columns

        # Create NodeSensorManager for each node
        self.node_managers = {}
        i = 0
        logger.info(len(nodes))
        logger.info(len(file_paths))
        for node_id, file in zip(nodes, file_paths):
            i+=1
            rack_id = file.split("/")[-1].split("\\")[0]
            self.node_managers[node_id] = NodeSensorManager(
                node_id=node_id,
                file_path=file,
                rack_id=rack_id,
                current_time=earliest_timestamp,
                sensor_columns=self.sensor_columns,
                interval_seconds=self.interval_seconds,
                rows_in_mem=rows_in_mem,
                temp_dir=temp_dir
            )
            if i % 100 == 0:
                logger.info(f"intilized {i} nodes")
            # Force memory cleanup after creating each manager
            force_memory_cleanup()
        
        # Force garbage collection after all managers are created
        force_memory_cleanup()

        # self.node_expected_rows = node_expected_rows
        # self.node_processed_rows = node_processed_rows

    def iterate_batches(self, limit_rows=None, stop_event=None, final_log_frequency=50):
        """
        Yields a dictionary {node_id: reading} for each batch,
        until all NodeSensorManagers are exhausted.
        """

        logger.info("Starting node_manager")

        active_nodes = set(self.node_managers.keys())
        unactive_nodes = set()
        batch = {}
        rows_processed = 0

        start_time = time.time()
        last_log_time = start_time

        log_frequency = 5

        batch_count = 0
        while active_nodes:
            if stop_event and stop_event.is_set():
                logger.info("NodeManager.iterate_batches detected stop_event set, breaking loop.")
                break
            to_remove = []
                            
            logger.debug("reading data from nodes")

            for node_id in active_nodes:
                if stop_event and stop_event.is_set():
                    logger.info("NodeManager.iterate_batches detected stop_event set inside node loop, breaking.")
                    break
                reading = self.node_managers[node_id].next_readings()
                if reading is None:
                    to_remove.append(node_id)
                else:
                    batch[node_id] = copy.copy(reading)
                    # self.node_processed_rows[node_id] += 1
            for node_id in to_remove:
                active_nodes.remove(node_id)
                unactive_nodes.add(node_id)
            if batch:
                # logger.debug("pushing row")
                if self.buffer.full():
                    # logger.info("buffer is full")
                    self.buffer.put(copy.copy(batch))
                else:
                    self.buffer.put(copy.copy(batch))
                # logger.debug("pushed")
                # print(batch)
                rows_processed += 1

                batch_count += 1
                if rows_processed % log_frequency == 0:
                    now = time.time()
                    interval = now - last_log_time
                    total = now - start_time
                    logger.info(f"NodeManager: Processed {rows_processed} batches. Last {log_frequency} in {interval:.2f}s, total elapsed {total:.2f}s.")
                    last_log_time = now

                    log_memory_usage(f"NodeManager.iterate_batches batch {batch_count}", buffer=self.buffer, var_name="buffer_queue")
                    logger.debug(get_queue_state({
                        "buffer" : self.buffer
                    }))
                    # Force memory cleanup periodically
                    force_memory_cleanup()
                    
                    if log_frequency < final_log_frequency:
                        log_frequency *= 10
                if limit_rows is not None and rows_processed >= limit_rows:
                    break

        self.buffer.put(None)
        # After processing, log expected vs actual rows per node
        logger.info("NodeManager: Checking processed row counts per node...")
        for node_id, man in self.node_managers.items():
            if self.expected_rows[node_id] != self.processed_rows[node_id]:
                logger.warning(f"Node {node_id}: Expected {self.expected_rows[node_id]} rows, processed {self.processed_rows[node_id]} rows!")
            else:
                logger.info(f"Node {node_id}: Processed all {self.processed_rows[node_id]} rows as expected.")

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Node Manager Batch Iterator")
    parser.add_argument('--tar-path', required=False, default='./TarFiles/', help='List of tar file paths')
    parser.add_argument('--interval-seconds', type=int, default=60*15, required=False, help='Interval in seconds')
    args = parser.parse_args()

    # Assuming NodeManager is the class containing this method
    manager = NodeManager(buffer=Queue(), tarfiles_path=args.tar_path, interval_seconds=args.interval_seconds)
    manager.iterate_batches(limit_rows=10)
    

if __name__ == "__main__":
    main()

