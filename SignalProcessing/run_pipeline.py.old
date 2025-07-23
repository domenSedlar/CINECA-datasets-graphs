import os
import sys
from pipeline.reader import read_tar_parquet
from pipeline.change_detector import ChangeLevelDetector
from pipeline.change_emitter import ChangeEmitter
from pipeline.state_builder import StateBuilder
from pipeline.persist import StatePersister
from pipeline.node_sensor_manager import NodeSensorManager
from pipeline.kafka_queue import KafkaQueue  # NEW: Import KafkaQueue

import logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(filename)s: %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("run_pipeline")

# Simple in-memory FIFO queue for single-threaded use
class SimpleQueue:
    def __init__(self):
        self._queue = []
    def push(self, item):
        self._queue.append(item)
    def pop(self, timeout=0):
        if self._queue:
            return self._queue.pop(0)
        return None


def run_pipeline(node, tar_path, output_file):
    # Remove output file if exists
    if os.path.exists(output_file):
        os.remove(output_file)

    # Queues between pipeline stages (Kafka topics)
    buffer_queue = KafkaQueue(topic=f"buffer_queue_{node}")  # topic per node
    change_queue = KafkaQueue(topic=f"change_queue_{node}")
    state_queue = KafkaQueue(topic=f"state_queue_{node}")
    emit_queue = KafkaQueue(topic=f"emit_queue_{node}")
    persist_queue = KafkaQueue(topic=f"persist_queue_{node}")
    state_builder = StateBuilder(emit_queue, persist_queue, node_id=node)
    state_persister = StatePersister(persist_queue, output_file=output_file)

    # 1. Use NodeSensorManager to read data for node 92 only
    logger.info(f"Initializing NodeSensorManager for node {node} and tar file {tar_path}")
    manager = NodeSensorManager(node, tar_path)
    detector = ChangeLevelDetector(buffer_queue, change_queue, node_id=node, delta=0.1)

    logger.info("Pushing first readings for all sensors as a batch to buffer_queue")
    first_reading = manager.next_readings()
    if first_reading is not None:
        buffer_queue.push(first_reading.copy())
    reading_batch_count = 1
    while True:
        next_reading = manager.next_readings()
        if not next_reading or (next_reading['sensor_data'] is None):
            logger.info(f"No more readings to process after {reading_batch_count} batches.")
            break
        if reading_batch_count == 1000:
            logger.info(f"Pushed {reading_batch_count} batches to buffer_queue")
            break
        buffer_queue.push(next_reading.copy())
        reading_batch_count += 1

    logger.info(f"Total batches pushed to buffer_queue: {reading_batch_count}")
    buffer_queue.flush()

    # 2. Run the detector
    logger.info("Running ChangeLevelDetector on buffered readings")
    detector.run()
    logger.info("ChangeLevelDetector finished processing")
    change_queue.flush()
    buffer_queue.close()
    
    # 3. Collect all outputs into a single queue for downstream processing
    logger.info("Collecting outputs from change_queue into state_queue")
    output_count = 0
    while True:
        data = change_queue.pop()
        if data is None:
            logger.info(f"Collected {output_count} outputs from change_queue")
            break
        state_queue.push(data)
        output_count += 1
    state_queue.flush()
    change_queue.close()

    # 4. Continue with the rest of the pipeline
    logger.info("Initializing ChangeEmitter, StateBuilder, and StatePersister")
    change_emitter = ChangeEmitter(state_queue, emit_queue)
    state_builder = StateBuilder(emit_queue, persist_queue, node_id=node)
    state_persister = StatePersister(persist_queue, output_file=output_file)

    logger.info("Running ChangeEmitter")
    change_emitter.run()
    logger.info("ChangeEmitter finished")
    emit_queue.flush()
    state_queue.close()

    logger.info("Running StateBuilder")
    state_builder.run()
    logger.info("StateBuilder finished")
    emit_queue.close()
    persist_queue.flush()


    logger.info("Running StatePersister")
    state_persister.run()
    logger.info("StatePersister finished")



    # Ensure all Kafka messages are flushed and sent
    buffer_queue.close()
    change_queue.close()
    state_queue.close()
    emit_queue.close()
    persist_queue.close()

    print(f"Pipeline complete. Output written to {output_file}")

def run_pipeline_parallel(nodes, tar_paths, output_file):
    # Determine rack_id from tar_paths
    if len(set(tar_paths)) == 1:
        rack_id = os.path.splitext(os.path.basename(tar_paths[0]))[0]
    else:
        rack_id = "multiple"

    # Find the earliest measurement timestamp
    import tarfile
    import pyarrow.parquet as pq
    import io
    import pandas as pd
    earliest_timestamp = None
    for tar_path in set(tar_paths):  # Avoid duplicate tar files
        with tarfile.open(tar_path, 'r') as tar:
            for member in tar.getmembers():
                if member.isfile() and member.name.endswith('.parquet'):
                    file_obj = tar.extractfile(member)
                    parquet_bytes = file_obj.read()
                    table = pq.ParquetFile(io.BytesIO(parquet_bytes))
                    # Only need the first row, as files are sorted by timestamp
                    first_batch = next(table.iter_batches(batch_size=1, columns=['timestamp']))
                    df = first_batch.to_pandas()
                    if not df.empty:
                        min_ts = df['timestamp'].iloc[0]
                        if not pd.isnull(min_ts):
                            if earliest_timestamp is None or min_ts < earliest_timestamp:
                                earliest_timestamp = min_ts
    
    logger.info(f"Earliest measurement timestamp across all files: {earliest_timestamp}")
    # Remove output file if exists
    if os.path.exists(output_file):
        os.remove(output_file)

    # Queues between pipeline stages (Kafka topics)
    buffer_queue = KafkaQueue(topic="buffer_queue_parallel")
    change_queue = KafkaQueue(topic="change_queue_parallel")
    state_queue = KafkaQueue(topic="state_queue_parallel")
    emit_queue = KafkaQueue(topic="emit_queue_parallel")
    persist_queue = KafkaQueue(topic="persist_queue_parallel")
    state_builder = StateBuilder(emit_queue, persist_queue)
    state_persister = StatePersister(persist_queue, output_file=output_file)

    # 1. Use NodeSensorManager to read data for nodes
    logger.info(f"Initializing NodeSensorManager")
    node_managers = {}
    for node, tar_path in zip(nodes, tar_paths):
        rack_id = os.path.splitext(os.path.basename(tar_path))[0]
        manager = NodeSensorManager(node, tar_path, rack_id=rack_id, earliest_timestamp=earliest_timestamp)
        node_managers[node] = manager
    

    logger.info("Pushing first readings for all sensors as a batch to buffer_queue")
    
    new_readings = {} # (node) -> {(sensor) -> (reading)}
    no_readings = True
    for node, manager in node_managers.items():    
        first_reading = manager.next_readings()
        new_readings[node] = first_reading
        if first_reading is not None:
            no_readings = False

    if not no_readings:
        buffer_queue.push(new_readings.copy())

    reading_batch_count = 0

    while True:
        new_readings = {}
        no_readings = True
        should_exit = False
        for node, manager in node_managers.items():
            next_reading = manager.next_readings()
            if not next_reading or (next_reading['sensor_data'] is None):
                logger.info(f"No more readings to process after {reading_batch_count} batches.")
            else:
                new_readings[node] = next_reading
                no_readings = False
                reading_batch_count += 1
            if reading_batch_count == -1:
                logger.info(f"Pushed {reading_batch_count} batches to buffer_queue")
                should_exit = True
                break
            if reading_batch_count % 50000 == 0:
                logger.info(f"Pushed {reading_batch_count} batches to buffer_queue")
        if not no_readings:
            buffer_queue.push(new_readings.copy())
        else:
            logger.info(f"No more readings to process after {reading_batch_count} batches.")
            break
        if should_exit:
            break


    logger.info(f"Total batches pushed to buffer_queue: {reading_batch_count}")

    # 2. Run the detector
    logger.info("Running ChangeLevelDetector on buffered readings")
    detector = ChangeLevelDetector(buffer_queue, change_queue, delta=0.1)
    detector.run()
    logger.info("ChangeLevelDetector finished processing")

    # 3. Collect all outputs into a single queue for downstream processing
    logger.info("Collecting outputs from change_queue into state_queue")
    output_count = 0
    while True:
        data = change_queue.pop()
        if data is None:
            logger.info(f"Collected {output_count} outputs from change_queue")
            break
        state_queue.push(data)
        output_count += 1

    # 4. Continue with the rest of the pipeline
    logger.info("Initializing ChangeEmitter, StateBuilder, and StatePersister")
    change_emitter = ChangeEmitter(state_queue, emit_queue)
    state_builder = StateBuilder(emit_queue, persist_queue)
    state_persister = StatePersister(persist_queue, output_file=output_file)

    logger.info("Running ChangeEmitter")
    change_emitter.run()
    logger.info("ChangeEmitter finished")

    logger.info("Running StateBuilder")
    state_builder.run()
    logger.info("StateBuilder finished")

    logger.info("Running StatePersister")
    state_persister.run()
    logger.info("StatePersister finished")

    # Ensure all Kafka messages are flushed and sent
    buffer_queue.close()
    change_queue.close()
    state_queue.close()
    emit_queue.close()
    persist_queue.close()

    print(f"Pipeline complete. Output written to {output_file}")

def main():
    import tarfile
    import json
    tar_dir = "./TarFiles/"
    output_dir = "./outputs/"

    nodes = []
    tar_paths = []

    os.makedirs(output_dir, exist_ok=True)

    # List all tar files in the directory
    parquet_files_processed = 0
    for tar_filename in os.listdir(tar_dir):
        if not tar_filename.endswith('.tar'):
            continue
        if '-' in tar_filename:
            continue
        tar_path = os.path.join(tar_dir, tar_filename)
        try:
            with tarfile.open(tar_path, 'r') as tar:
                # List all .parquet files in the tar
                for member in tar.getmembers(): # runs pipeline for each node in the tar file
                    if parquet_files_processed == -1:
                        break
                    
                    if member.isfile() and member.name.endswith('.parquet'):
                        # Extract node_id from filename (e.g., '23.parquet' -> 23)
                        base = os.path.basename(member.name)
                        node_id_str = base.split('.')[0]
                        try:
                            node_id = int(node_id_str)
                        except ValueError:
                            logger.info(f"Skipping file with invalid node_id: {base}")
                            continue
                        nodes.append(node_id)
                        tar_paths.append(tar_path)
                        parquet_files_processed += 1
        except Exception as e:
            logger.error(f"Error processing tar file {tar_path}: {e}")

    # After collecting all nodes and tar_paths, create a single output file
    output_file = os.path.join(output_dir, "all_nodes_state.json")

    run_pipeline_parallel(nodes, tar_paths, output_file)

    logger.info(f"Pipeline complete. Output written to {output_file}")


def main_squential():
    logger.info("Starting pipeline")
    import tarfile
    tar_dir = "./TarFiles/"
    output_dir = "./outputs/"
    os.makedirs(output_dir, exist_ok=True)

    # List all tar files in the directory
    for tar_filename in os.listdir(tar_dir):

        if not tar_filename.endswith('.tar'):
            continue
        if '-' in tar_filename:
            continue
        tar_path = os.path.join(tar_dir, tar_filename)
        try:
            with tarfile.open(tar_path, 'r') as tar:
                # List all .parquet files in the tar
                for member in tar.getmembers(): # runs pipeline for each node in the tar file
                    if member.isfile() and member.name.endswith('.parquet'):
                        # Extract node_id from filename (e.g., '23.parquet' -> 23)
                        base = os.path.basename(member.name)
                        node_id_str = base.split('.')[0]
                        try:
                            node_id = int(node_id_str)
                        except ValueError:
                            logger.info(f"Skipping file with invalid node_id: {base}")
                            continue
                        # Unique output file per node/tar
                        output_file = os.path.join(output_dir, f"{os.path.splitext(tar_filename)[0]}_node{node_id}_state.json")
                        run_pipeline(node_id, tar_path, output_file)
        except Exception as e:
            logger.error(f"Error processing tar file {tar_path}: {e}")

if __name__ == "__main__":
    main() 