import threading
import queue
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from pipeline.changes.change_detector import ChangeLevelDetector
from pipeline.file_reading.node_manager import NodeManager
import logging
import gc

def run_first_two_stages(batches, delta=0.005, clock=10, stop_event=None):
    """
    Runs only the ChangeLevelDetector stage using the provided batches.
    Tracks and prints how many rows (batches) are passed to both queues.
    Returns the result string and elapsed time for this parameter set.
    If stop_event is set, will break early and send None to the change_queue.
    """
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
            if change_queue_count % 100 == 0:
                logging.info(f"{change_queue_count} items have been placed into change_queue.")
    change_queue.put = change_put_counted

    # Feed the batches into buffer_queue
    for batch in batches:
        if stop_event is not None and stop_event.is_set():
            buffer_queue.put(None)
            print(delta, clock, None, None, flush=True, sep=";")
            return delta, clock, None, None  # Return early if stopped
        buffer_queue.put(batch)
    buffer_queue.put(None)  # Signal end of input

    change_detector = ChangeLevelDetector(buffer_queue, change_queue, delta=delta, clock=clock)

    start_time = time.time()
    change_detector.run(stop_event=stop_event)
    elapsed = time.time() - start_time
    batches_processed = len(batches) - buffer_queue.qsize()
    if batches_processed > 0:
        ratio = change_queue_count / batches_processed
    else:
        ratio = None
    gc.collect()
    print(delta, clock, ratio, elapsed, flush=True, sep=";")

def collect_rows(limit=10000, skip_rows=10000, stop_event=None):
    """
    Runs NodeManager and collects all batches. Returns the list of batches from index 10000 (inclusive) to 10000+limit (exclusive).
    If stop_event is set, will break early.
    """
    buffer_queue = queue.Queue()
    if stop_event is None:
        stop_event = threading.Event()
    node_manager = NodeManager(buffer=buffer_queue)
    batches = []

    def run_node_manager():
        node_manager.iterate_batches(stop_event=stop_event, limit_rows=limit+skip_rows+1)
        buffer_queue.put(None)

    t = threading.Thread(target=run_node_manager)
    t.start()

    while True:
        if stop_event is not None and stop_event.is_set():
            break
        batch = buffer_queue.get()
        if batch is None:
            break
        batches.append(batch)
    t.join()
    # Return the slice from 10000 (inclusive) to 10000+limit (exclusive)
    return batches[skip_rows:limit + skip_rows]

def evaluate_parameters():
    parameters_delta = [0.5, 0.1, 0.05, 0.02, 0.01, 0.005, 0.002, 0.001]
    parameters_clock = [3, 6, 8, 12, 16, 20, 24]

    stop_event = threading.Event()
    try:
        batches = collect_rows(limit=10000, stop_event=stop_event)
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt received! Sending stop signal to all change queues...")
        stop_event.set()
        raise

    for delta in parameters_delta:
        for clock in parameters_clock:
            run_first_two_stages(batches, delta, clock, stop_event)

    return

    futures = []
    done = set()

    with ThreadPoolExecutor(max_workers=8) as executor:
        try:
            # Submit all tasks
            for delta in parameters_delta:
                for clock in parameters_clock:
                    futures.append(executor.submit(run_first_two_stages, batches, delta, clock, stop_event))

            # Poll for results
            while len(done) < len(futures):
                try:
                    finished, _ = wait(futures, timeout=1, return_when=FIRST_COMPLETED)
                    for future in finished:
                        if future not in done:
                            try:
                                result = future.result()
                                logging.info(result)
                            except Exception as e:
                                logging.error(f"Exception in worker thread: {e}")
                            done.add(future)
                except KeyboardInterrupt:
                    logging.info("KeyboardInterrupt received! Setting stop_event...")
                    stop_event.set()
                    break  # Exit the polling loop

        except Exception as e:
            logging.exception("Unhandled exception in main thread")
            stop_event.set()
        finally:
            # Optionally wait for all threads to acknowledge stop_event
            logging.info("Shutting down executor...")
            executor.shutdown(wait=False)

if __name__ == "__main__":
    evaluate_parameters() 