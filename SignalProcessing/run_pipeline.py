import threading
import queue
from pipeline.file_reading.node_manager import NodeManager
from pipeline.changes.change_detector import ChangeLevelDetector
from pipeline.state_builder import StateBuilder
from pipeline.persist import StatePersister
from common.memory_utils import MemoryMonitor
import os
import datetime

from common.logger import Logger
logger = Logger(name=__name__.split('.')[-1], log_dir='logs').get_logger_real()

def run():
    limit_nodes = None
    limit_racks = True
    delta=0.5
    clock=3
    bq_max_size=300
    rows_in_mem=300
    temp_dir_loc="E:/temp_parquet_files"

    vars_to_log = ['limit_nodes', 'limit_racks', 'delta', 'clock', 'bq_max_size', 'rows_in_mem']
    log_message = ""
    for var in vars_to_log:
        log_message += var + ": " + str(locals()[var]) + ", "
    logger.info(log_message)

    # Initialize memory monitor
    memory_monitor = MemoryMonitor(log_interval=50)
    
        # Set up queues for each stage with size limits for backpressure
    # Create queues with smaller sizes for more aggressive memory management
    buffer_queue = queue.Queue(maxsize=bq_max_size)     # NodeManager → ChangeLevelDetector (reduced from 200)
    change_queue = queue.Queue(maxsize=500)     # ChangeLevelDetector → StateBuilder (reduced from 100)
    state_queue = queue.Queue(maxsize=500)     # StateBuilder → StatePersister (reduced from 500)

    output_file = f'./outputs/threaded_pipeline_state_{datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.parquet'

    # Remove output file if it exists
    if os.path.exists(output_file):
        os.remove(output_file)

    # Create the stop event
    stop_event = threading.Event()

    # Set up pipeline stages
    node_manager = NodeManager(buffer=buffer_queue, limit_nodes=limit_nodes, temp_dir=temp_dir_loc, rows_in_mem=rows_in_mem, limit_racks=limit_racks)
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

    logger.info(f"Started all threads")

    try:
        while any(t.is_alive() for t in threads):
            # Monitor memory usage
            memory_monitor.check_memory("Pipeline-Main")
            
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

    # Print final memory summary
    summary = memory_monitor.get_summary()
    logger.info(f"Pipeline complete. Output written to {output_file}")
    logger.info(f"Memory Summary: Initial={summary['initial_memory']:.2f}MB, "
                f"Final={summary['current_memory']:.2f}MB, "
                f"Peak={summary['peak_memory']:.2f}MB, "
                f"Total Δ={summary['total_delta']:+.2f}MB, "
                f"Stable={summary['memory_stable']}, "
                f"Elapsed={summary['elapsed_time']:.1f}s")

# Parameter sweep and evaluation logic has been moved to evaluate_parameters.py
# To run parameter sweeps, use: python evaluate_parameters.py

if __name__ == "__main__":
    run()
