import threading
import queue
import multiprocessing
from pipeline.file_reading.node_manager import NodeManager
from pipeline.changes.change_detector import ChangeLevelDetector
from pipeline.state_builder import StateBuilder
from pipeline.persist import StatePersister
from common.memory_utils import MemoryMonitor
import os
import datetime
import sys

from concurrent.futures import ProcessPoolExecutor

from common.logger import Logger

def node_manager_process(buffer_queue, stop_event, limit_nodes, limit_racks, rows_in_mem):
    """NodeManager process function that can be pickled"""
    node_manager = NodeManager(
        buffer=buffer_queue, 
        limit_nodes=limit_nodes, 
        rows_in_mem=rows_in_mem, 
        limit_racks=limit_racks
    )
    node_manager.iterate_batches(stop_event=stop_event, final_log_frequency=500)


def change_detector_process(buffer_queue, change_queue, delta, clock):
    """ChangeDetector process function that can be pickled"""
    change_detector = ChangeLevelDetector(buffer_queue, change_queue, delta=delta, clock=clock)
    change_detector.run()

def state_builder_process(change_queue, state_queue):
    """StateBuilder process function that can be pickled"""
    state_builder = StateBuilder(change_queue, state_queue)
    state_builder.run()

def state_persister_process(state_queue, output_file, batch_write_size=25):
    """StatePersister process function that can be pickled"""
    state_persister = StatePersister(state_queue, output_file=output_file, batch_write_size=batch_write_size)
    state_persister.run()

def run(limit_racks = None):
    logger = Logger(name=__name__.split('.')[-1], log_dir='logs', rack=limit_racks).get_logger_real()

    if limit_racks is None:
        limit_racks = int(sys.argv[1])

    limit_nodes = None
    delta=0.5
    clock=3
    rows_in_mem=500
    bq_max_size=2*rows_in_mem

    vars_to_log = ['limit_nodes', 'limit_racks', 'delta', 'clock', 'bq_max_size', 'rows_in_mem', 'batch_write_size']
    log_message = ""
    for var in vars_to_log:
        log_message += var + ": " + str(locals()[var]) + ", "
    logger.info(log_message)

    # Initialize memory monitor
    memory_monitor = MemoryMonitor(log_interval=50)
    
    # Set up queues for each stage with size limits for backpressure
    # Use multiprocessing.Queue for inter-process communication
    buffer_queue = multiprocessing.Queue(maxsize=bq_max_size)     # NodeManager → ChangeLevelDetector
    change_queue = multiprocessing.Queue(maxsize=bq_max_size*10*1000)     # ChangeLevelDetector → StateBuilder
    state_queue = multiprocessing.Queue(maxsize=bq_max_size)     # StateBuilder → StatePersister


    output_file = f'./outputs/threaded_pipeline_state_{datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}_rack{limit_racks}.parquet'

    # Remove output file if it exists
    if os.path.exists(output_file):
        os.remove(output_file)

    # Create the stop event (multiprocessing.Event)
    stop_event = multiprocessing.Event()

    # Create processes with function-based targets that can be pickled
    processes = [
        multiprocessing.Process(
            target=node_manager_process, 
            args=(buffer_queue, stop_event, limit_nodes, limit_racks, rows_in_mem),
            name="NodeManagerProcess"
        ),
        multiprocessing.Process(
            target=change_detector_process, 
            args=(buffer_queue, change_queue, delta, clock),
            name="ChangeLevelDetectorProcess"
        ),
        multiprocessing.Process(
            target=state_builder_process, 
            args=(change_queue, state_queue),
            name="StateBuilderProcess"
        ),
        multiprocessing.Process(
            target=state_persister_process, 
            args=(state_queue, output_file),
            name="StatePersisterProcess"
        ),
    ]

    # Start processes
    for p in processes:
        p.start()

    logger.info(f"Started all processes")

    try:
        while any(p.is_alive() for p in processes):
            # Monitor memory usage
            memory_monitor.check_memory("Pipeline-Main")
            
            for p in processes:
                p.join(timeout=0.5)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received! Setting stop event and sending sentinels.")
        stop_event.set()
        for _ in range(2):
            buffer_queue.put(None)
            change_queue.put(None)
            state_queue.put(None)
        logger.info("Sentinels sent to all queues.")
        for p in processes:
            p.join(timeout=5)
        logger.info("Pipeline killed by user.")
    
    return None

def run_wrapper(i):
    return run(limit_racks=int(i))


def process_all(num_workers=2):
    print("num of workers: ", num_workers)
    print("starting...")
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        executor.map(run_wrapper, range(50))

    print("All tasks completed.")

def process_rack(rack_id=None):
    run(limit_racks=rack_id)

if __name__ == "__main__":
    num_workers = 1 # each worker starts 4 processes, so if you have 32 cores, seting this to 5 will result in the use of 25 cores
    # each worker processes one rack at a time 
    
    process_rack(rack_id=0) # change rack id to the number of the rack you wish to process

    # Use the following function instead, if running on a system with a bunch of cores and ram
    # process_all(num_workers) # will process all racks
