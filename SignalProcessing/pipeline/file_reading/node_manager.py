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

import logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(filename)s: %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("run_pipeline")

def log_memory_usage(context="NodeManager.iterate_batches", buffer=None, var_name="buffer"):
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    queue_size = buffer.qsize() if buffer is not None else 'NA'
    print(f"[MEMORY]\t\t{context}\t\tRSS={mem_info.rss/1024/1024:.2f}MB\t\tVMS={mem_info.vms/1024/1024:.2f}MB\t\tThreads={process.num_threads()}\t\t{var_name}={queue_size}", flush=True)

def force_memory_cleanup():
    """Cross-platform memory cleanup"""
    # Force garbage collection
    gc.collect()
    
    # Platform-specific memory release
    if platform.system() == "Linux":
        try:
            ctypes.CDLL("libc.so.6").malloc_trim(0)
        except Exception:
            pass  # malloc_trim not available
    elif platform.system() == "Windows":
        try:
            # Windows equivalent - use kernel32 to release memory
            kernel32 = ctypes.windll.kernel32
            kernel32.SetProcessWorkingSetSize(-1, -1, -1)
        except Exception:
            pass  # Windows memory management not available

class NodeManager:
    def __init__(self, buffer: Queue, tarfiles_path='./TarFiles/', interval_seconds=60*15):
        logger = logging.getLogger(__name__)

        self.tarfiles_path = tarfiles_path # only read the first 2 racks for testing
        self.interval_seconds = interval_seconds
        self.buffer = buffer
        self.sensor_columns = None # limits which sensors we read

        nodes = []
        tar_paths = []

        # Scan all tar files and .parquet members in a single pass, collecting node info and earliest timestamp
        nodes = []
        tar_paths = []
        earliest_timestamp = None
        table = None

        max_rows = 0
        max_file = None
        node_expected_rows = {}  # node_id -> expected row count
        node_processed_rows = {}  # node_id -> processed row count
        for tar_filename in os.listdir(self.tarfiles_path):
            if not tar_filename.endswith('.tar'): 
                continue
            if '-' in tar_filename: # skip tar files that are not for a single rack
                continue

            tar_path = os.path.join(self.tarfiles_path, tar_filename)
            try:
                with tarfile.open(tar_path, 'r') as tar:
                    for member in tar.getmembers():
                        if member.isfile() and member.name.endswith('.parquet'):
                            base = os.path.basename(member.name)
                            node_id_str = base.split('.')[0] # extract node_id from filename
                            try:
                                node_id = int(node_id_str)
                            except ValueError:
                                logger.info(f"Skipping file with invalid node_id: {base}")
                                continue
                            nodes.append(node_id)
                            tar_paths.append(tar_path)
                            # Find earliest timestamp in this parquet file
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
                if(len(nodes) < -1):
                    break
            except Exception as e:
                logger.error(f"Error processing tar file {tar_path}: {e}")

        logger.info(f"Earliest measurement timestamp across all files: {earliest_timestamp}")
        logger.info(f"File with most rows: {max_file} with {max_rows} rows")
        self.earliest_timestamp = earliest_timestamp

        if table is not None: # only read the avg columns, not the min/max/std
            all_columns = table.schema.names
            sensor_columns = [col for col in all_columns if col != 'timestamp' and not (col.endswith('_min') or col.endswith('_max') or col.endswith('_std'))]
            self.sensor_columns = sensor_columns

        self.node_managers = {}
        for node_id, tar_path in zip(nodes, tar_paths):
            self.node_managers[node_id] = NodeSensorManager(
                node_id, tar_path, self.interval_seconds, self.sensor_columns
            )
            # Force memory cleanup after creating each manager
            force_memory_cleanup()
        
        # Force garbage collection after all managers are created
        force_memory_cleanup()

        # self.node_expected_rows = node_expected_rows
        # self.node_processed_rows = node_processed_rows

    def iterate_batches(self, limit_rows=None, stop_event=None):
        """
        Yields a dictionary {node_id: reading} for each batch,
        until all NodeSensorManagers are exhausted.
        """
        active_nodes = set(self.node_managers.keys())
        unactive_nodes = set()
        batch = {}
        rows_processed = 0

        start_time = time.time()
        last_log_time = start_time

        log_frequency = 1

        batch_count = 0
        while active_nodes:
            if stop_event and stop_event.is_set():
                logger.info("NodeManager.iterate_batches detected stop_event set, breaking loop.")
                break
            to_remove = []
            for node_id in active_nodes:
                if stop_event and stop_event.is_set():
                    logger.info("NodeManager.iterate_batches detected stop_event set inside node loop, breaking.")
                    break
                reading = self.node_managers[node_id].next_readings()
                if reading is None:
                    to_remove.append(node_id)
                else:
                    batch[node_id] = reading
            for node_id in to_remove:
                active_nodes.remove(node_id)
                unactive_nodes.add(node_id)
            if batch:
                self.buffer.put(copy.deepcopy(batch))
                # print(batch)
                rows_processed += 1
                if batch_count % 50 == 0:  # Reduced from 100 for more frequent monitoring
                    log_memory_usage(f"NodeManager.iterate_batches batch {batch_count}", buffer=self.buffer, var_name="buffer_queue")
                    # Force memory cleanup periodically
                    force_memory_cleanup()
                batch_count += 1
                if rows_processed % log_frequency == 0:
                    now = time.time()
                    interval = now - last_log_time
                    total = now - start_time
                    logger.info(f"NodeManager: Processed {rows_processed} batches. Last {log_frequency} in {interval:.2f}s, total elapsed {total:.2f}s.")
                    last_log_time = now
                    if log_frequency < 1000:
                        log_frequency *= 10
                if limit_rows is not None and rows_processed >= limit_rows:
                    break
        self.buffer.put(None)
        # After processing, log expected vs actual rows per node
        logger.info("NodeManager: Checking processed row counts per node...")
        for node_id, manager in self.node_managers.items():
            expected = manager.get_expected_row_count()
            actual = manager.get_processed_row_count()
            if expected != actual:
                logger.warning(f"Node {node_id}: Expected {expected} rows, processed {actual} rows!")
            else:
                logger.info(f"Node {node_id}: Processed all {actual} rows as expected.")

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

