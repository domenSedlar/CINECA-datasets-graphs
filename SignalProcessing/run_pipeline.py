import os
import sys
from pipeline.reader import read_tar_parquet
from pipeline.change_detector import ChangeLevelDetector
from pipeline.change_emitter import ChangeEmitter
from pipeline.state_builder import StateBuilder
from pipeline.persist import StatePersister
from pipeline.node_sensor_manager import NodeSensorManager

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

    # Queues between pipeline stages
    change_queues = {}  # (node, sensor) -> SimpleQueue for each detector's output

    detectors = {}      # (node, sensor) -> ChangeLevelDetector
    state_queue = SimpleQueue()
    emit_queue = SimpleQueue()
    persist_queue = SimpleQueue()
    state_builder = StateBuilder(emit_queue, persist_queue, node_id=node)
    state_persister = StatePersister(persist_queue, output_file=output_file)

    # 1. Use NodeSensorManager to read data for node 92 only
    logger.info(f"Initializing NodeSensorManager for node {node} and tar file {tar_path}")
    manager = NodeSensorManager(node, tar_path)
    buffer_queue = SimpleQueue()
    change_queue = SimpleQueue()
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

    # 2. Run the detector
    logger.info("Running ChangeLevelDetector on buffered readings")
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
    state_builder = StateBuilder(emit_queue, persist_queue, node_id=node)
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

    print(f"Pipeline complete. Output written to {output_file}")

def main():
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
                for member in tar.getmembers():
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