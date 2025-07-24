import threading
import queue
from pipeline.file_reading.node_manager import NodeManager
from pipeline.changes.change_detector import ChangeLevelDetector
from pipeline.state_builder import StateBuilder
from pipeline.persist import StatePersister
import logging
import os
import datetime

logging.basicConfig(
    level=logging.debug,
    format="[%(levelname)s] %(filename)s: %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("run_pipeline")

def main():
    # Set up queues for each stage
    buffer_queue = queue.Queue()
    change_queue = queue.Queue()
    state_queue = queue.Queue()

    output_file = f'./outputs/threaded_pipeline_state_{datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.json'

    # Remove output file if it exists
    if os.path.exists(output_file):
        os.remove(output_file)

    # Create the stop event
    stop_event = threading.Event()

    # Set up pipeline stages
    node_manager = NodeManager(buffer=buffer_queue)
    change_detector = ChangeLevelDetector(buffer_queue, change_queue)
    state_builder = StateBuilder(change_queue, state_queue)
    state_persister = StatePersister(state_queue, output_file=output_file)

    # Create threads
    threads = [
        threading.Thread(target=lambda: node_manager.iterate_batches(limit_rows=1000, stop_event=stop_event), name="NodeManagerThread"),
        threading.Thread(target=change_detector.run, name="ChangeLevelDetectorThread"),
        threading.Thread(target=state_builder.run, name="StateBuilderThread"),
        threading.Thread(target=state_persister.run, name="StatePersisterThread"),
    ]

    # Start threads
    for t in threads:
        t.start()

    try:
        while any(t.is_alive() for t in threads):
            for t in threads:
                t.join(timeout=0.5)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received! Setting stop event and sending sentinels.")
        stop_event.set()
        for _ in range(2):
            buffer_queue.put(None)
            change_queue.put(None)
            state_queue.put(None)
        logger.info("Sentinels sent to all queues.")
        for t in threads:
            t.join(timeout=5)
        logger.info("Pipeline killed by user.")

    logger.info(f"Pipeline complete. Output written to {output_file}")

if __name__ == "__main__":
    main()
