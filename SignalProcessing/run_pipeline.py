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



def run_first_two_stages(batches, delta=0.005, clock=10):
    """
    Runs only the ChangeLevelDetector stage using the provided batches.
    Tracks and prints how many rows (batches) are passed to both queues.
    Returns the result string and elapsed time for this parameter set.
    """
    import queue
    import threading
    import time
    from pipeline.changes.change_detector import ChangeLevelDetector

    start_time = time.time()
    buffer_queue = queue.Queue()
    change_queue = queue.Queue()

    # Counters for batches passed to each queue
    change_queue_count = 0

    # Wrap the original put method to count puts to change_queue
    orig_change_put = change_queue.put
    def change_put_counted(item):
        nonlocal change_queue_count
        if item is not None:
            change_queue_count += 1
            if change_queue_count % 1000 == 0:
                logger.info(f"{change_queue_count} items have been placed into change_queue.")
        orig_change_put(item)
    change_queue.put = change_put_counted

    # Feed the batches into buffer_queue
    for batch in batches:
        buffer_queue.put(batch)
    buffer_queue.put(None)  # Signal end of input

    change_detector = ChangeLevelDetector(buffer_queue, change_queue, delta=delta, clock=clock)

    # Run the change detector in the main thread (no need for threading)
    change_detector.run()

    elapsed = time.time() - start_time
    ratio = change_queue_count / len(batches)
    return delta, clock, ratio, elapsed

def collect_rows():
    """
    Runs NodeManager and collects all batches. Returns the list of batches from index 10000 (inclusive) to 30000 (exclusive).
    """
    import queue
    import threading
    from pipeline.file_reading.node_manager import NodeManager

    buffer_queue = queue.Queue()
    stop_event = threading.Event()
    node_manager = NodeManager(buffer=buffer_queue)
    batches = []

    def run_node_manager():
        node_manager.iterate_batches(stop_event=stop_event)
        buffer_queue.put(None)

    t = threading.Thread(target=run_node_manager)
    t.start()

    while True:
        batch = buffer_queue.get()
        if batch is None:
            break
        batches.append(batch)
    t.join()
    # Return the slice from 10000 (inclusive) to 30000 (exclusive)
    return batches[10000:30000]

def evaluate_parameters():
    from concurrent.futures import ThreadPoolExecutor, as_completed
    parameters_delta = [0.5, 0.1, 0.05, 0.02, 0.01, 0.005, 0.002, 0.001]
    parameters_clock = [3, 6, 8, 12, 16, 20, 24]

    batches = collect_rows()

    futures = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        for delta in parameters_delta:
            for clock in parameters_clock:
                futures.append(executor.submit(run_first_two_stages, batches, delta, clock))
        for future in as_completed(futures):
            result = future.result()
            print(result)

def main():
    run()
    # evaluate_parameters()

if __name__ == "__main__":
    main()
