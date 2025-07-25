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

def run():
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
        threading.Thread(target=lambda: node_manager.iterate_batches(stop_event=stop_event), name="NodeManagerThread"),
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



def run_first_two_stages():
    """
    Runs only the first two stages of the pipeline: NodeManager and ChangeLevelDetector.
    Tracks and prints how many rows (batches) are passed to both queues.
    """
    import queue
    import threading
    from pipeline.file_reading.node_manager import NodeManager
    from pipeline.changes.change_detector import ChangeLevelDetector

    buffer_queue = queue.Queue()
    change_queue = queue.Queue()

    stop_event = threading.Event()

    node_manager = NodeManager(buffer=buffer_queue)
    change_detector = ChangeLevelDetector(buffer_queue, change_queue)

    # Counters for batches passed to each queue
    buffer_queue_count = 0
    change_queue_count = 0
    counters_reset = False

    # Wrap the original put method to count puts to buffer_queue
    orig_buffer_put = buffer_queue.put
    def buffer_put_counted(item):
        nonlocal buffer_queue_count
        orig_buffer_put(item)
        if item is not None:
            buffer_queue_count += 1
    buffer_queue.put = buffer_put_counted

    # Wrap the original put method to count puts to change_queue
    orig_change_put = change_queue.put
    def change_put_counted(item):
        nonlocal change_queue_count, buffer_queue_count, counters_reset
        orig_change_put(item)
        if item is not None:
            change_queue_count += 1
            if not counters_reset and change_queue_count == 1000: # for the first 1000 items, the data is incomplete, so they dont provide an accurete assessment
                logger.info("Resetting counters after 1000 items in change_queue.")
                buffer_queue_count = 0
                change_queue_count = 0
                counters_reset = True
    change_queue.put = change_put_counted

    node_manager_thread = threading.Thread(target=lambda: node_manager.iterate_batches(stop_event=stop_event), name="NodeManagerThread")
    change_detector_thread = threading.Thread(target=change_detector.run, name="ChangeLevelDetectorThread")

    node_manager_thread.start()
    change_detector_thread.start()

    try:
        while node_manager_thread.is_alive() or change_detector_thread.is_alive():
            node_manager_thread.join(timeout=0.5)
            change_detector_thread.join(timeout=0.5)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received! Setting stop event and sending sentinels.")
        stop_event.set()
        buffer_queue.put(None)
        change_queue.put(None)
        node_manager_thread.join(timeout=5)
        change_detector_thread.join(timeout=5)
        logger.info("Pipeline killed by user.")

    # Print or process outputs from change_queue
    while not change_queue.empty():
        output = change_queue.get()
        if output is not None:
            print(output)

    logger.info(f"First two stages complete. Rows passed to buffer_queue: {buffer_queue_count}, rows passed to change_queue: {change_queue_count}")
    print(f"Rows passed to buffer_queue: {buffer_queue_count}")
    print(f"Rows passed to change_queue: {change_queue_count}")


def main():
    run_first_two_stages()

if __name__ == "__main__":
    main()
