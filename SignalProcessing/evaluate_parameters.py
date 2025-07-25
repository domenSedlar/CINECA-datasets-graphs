import threading
import queue
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pipeline.changes.change_detector import ChangeLevelDetector
from pipeline.file_reading.node_manager import NodeManager
import logging

def run_first_two_stages(batches, delta=0.005, clock=10, stop_event=None, change_queues=None):
    """
    Runs only the ChangeLevelDetector stage using the provided batches.
    Tracks and prints how many rows (batches) are passed to both queues.
    Returns the result string and elapsed time for this parameter set.
    If stop_event is set, will break early and send STOP_SIGNAL to the change_queue.
    If change_queues is provided, appends the change_queue for external signaling.
    """
    buffer_queue = queue.Queue()
    change_queue = queue.Queue()
    if change_queues is not None:
        change_queues.append(change_queue)

    # Counters for batches passed to each queue
    change_queue_count = 0

    # Wrap the original put method to count puts to change_queue
    orig_change_put = change_queue.put
    def change_put_counted(item):
        nonlocal change_queue_count
        if item is not None:
            change_queue_count += 1
            if change_queue_count % 100 == 0:
                logging.info(f"{change_queue_count} items have been placed into change_queue.")
        orig_change_put(item)
    change_queue.put = change_put_counted

    # Feed the batches into buffer_queue
    for batch in batches:
        if stop_event is not None and stop_event.is_set():
            buffer_queue.put(ChangeLevelDetector.STOP_SIGNAL)
            break
        buffer_queue.put(batch)
    buffer_queue.put(None)  # Signal end of input

    change_detector = ChangeLevelDetector(buffer_queue, change_queue, delta=delta, clock=clock)

    start_time = time.time()
    change_detector.run()
    elapsed = time.time() - start_time
    ratio = change_queue_count / len(batches)
    return delta, clock, ratio, elapsed

def collect_rows(limit=30000):
    """
    Runs NodeManager and collects all batches. Returns the list of batches from index 10000 (inclusive) to 30000 (exclusive).
    """
    buffer_queue = queue.Queue()
    stop_event = threading.Event()
    node_manager = NodeManager(buffer=buffer_queue)
    batches = []

    def run_node_manager():
        node_manager.iterate_batches(stop_event=stop_event, limit_rows=limit)
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
    parameters_delta = [0.5, 0.1, 0.05, 0.02, 0.01, 0.005, 0.002, 0.001]
    parameters_clock = [3, 6, 8, 12, 16, 20, 24]

    batches = collect_rows(limit=30001)

    futures = []
    change_queues = []
    stop_event = threading.Event()
    with ThreadPoolExecutor(max_workers=8) as executor:
        try:
            for delta in parameters_delta:
                for clock in parameters_clock:
                    futures.append(executor.submit(run_first_two_stages, batches, delta, clock, stop_event, change_queues))
            for future in as_completed(futures):
                result = future.result()
                print(result)
        except KeyboardInterrupt:
            print("KeyboardInterrupt received! Sending stop signal to all change queues...")
            stop_event.set()
            for q in change_queues:
                q.put(ChangeLevelDetector.STOP_SIGNAL)

if __name__ == "__main__":
    evaluate_parameters() 