import os
import tarfile
import io
import pyarrow.parquet as pq
import pandas as pd
import logging
from pipeline.file_reading.node_sensor_manager import NodeSensorManager
from simple_queue import SimpleQueue as Queue


import logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(filename)s: %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("run_pipeline")

class NodeManager:
    def __init__(self, buffer: Queue, tarfiles_path='./TarFiles/', interval_seconds=60*15):
        logger = logging.getLogger(__name__)

        self.tarfiles_path = tarfiles_path
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
                            except Exception as e:
                                logger.warning(f"Failed to read timestamp from {member.name} in {tar_path}: {e}")
            except Exception as e:
                logger.error(f"Error processing tar file {tar_path}: {e}")

        logger.info(f"Earliest measurement timestamp across all files: {earliest_timestamp}")
        self.earliest_timestamp = earliest_timestamp

        if table is not None: # only read the avg columns, not the min/max/std
            all_columns = table.schema.names
            sensor_columns = [col for col in all_columns if col != 'timestamp' and not (col.endswith('_min') or col.endswith('_max') or col.endswith('_std'))] 
            self.sensor_columns = sensor_columns

        self.node_managers = {}
        for node, tar_path in zip(nodes, tar_paths):
            rack_id = os.path.splitext(os.path.basename(tar_path))[0]
            manager = NodeSensorManager(node, tar_path, rack_id=rack_id, current_time=earliest_timestamp, sensor_columns=self.sensor_columns, interval_seconds=self.interval_seconds)
            self.node_managers[node] = manager

    def iterate_batches(self, limit_rows=None):
        """
        Yields a dictionary {node_id: reading} for each batch,
        until all NodeSensorManagers are exhausted.
        """
        active_nodes = set(self.node_managers.keys())
        unactive_nodes = set()
        batch = {}
        rows_processed = 0

        while active_nodes:
            to_remove = []
            for node_id in active_nodes:
                reading = self.node_managers[node_id].next_readings()
                if reading is None:
                    to_remove.append(node_id)
                else:
                    batch[node_id] = reading
            for node_id in to_remove:
                active_nodes.remove(node_id)
                unactive_nodes.add(node_id)
            if batch:
                logger.info(f"Pushing batch to buffer: {batch}")
                self.buffer.push(batch)
                rows_processed += 1
                if limit_rows is not None and rows_processed >= limit_rows:
                    break
                # output in format: {node_id: {timestamp: timestamp, sensor_data: {sensor_id: value, ...}}, ...}
                # INSERT_YOUR_CODE
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

