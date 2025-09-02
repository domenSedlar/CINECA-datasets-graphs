import sys
import os
import threading
import time
from queue import Queue
import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), 'pipeline'))

from .pipeline.read_and_emit import StateFileReader
from .pipeline.graph_builder import GraphBuilder, GraphTypes
from .pipeline.persist import GraphStorage

import argparse

def run(reader_output_queue = Queue(), builder_output_queue = Queue(), state_file='StateFiles/state.parquet', stop_event = threading.Event()):
    # Create objects
    reader = StateFileReader(buffer=reader_output_queue, state_file=state_file)
    builder = GraphBuilder(buffer=reader_output_queue, output_queue=builder_output_queue, graph_type=GraphTypes.NodeTree)
    unique_run_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_filename = f'all_graphs_{unique_run_id}.pkl'
    # storage = GraphStorage(input_queue=builder_output_queue, filename=unique_filename)

    # Create threads
    threads = [
        threading.Thread(target=reader.read_and_emit, name="StateFileReaderThread", kwargs={"stop_event": stop_event}),
        threading.Thread(target=builder.build_graph, name="GraphBuilderThread", kwargs={"stop_event": stop_event}),
        # threading.Thread(target=storage.run, name="GraphStorageThread"),
    ]

    # Start threads
    for thread in threads:
        thread.start()

    try:
        while any(thread.is_alive() for thread in threads):
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("KeyboardInterrupt received! Setting stop event and sending sentinels.")
        stop_event.set()
        reader_output_queue.put(None)
        builder_output_queue.put(None)


if __name__ == '__main__':
    run()