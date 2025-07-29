import sys
import os
import threading
import time
from queue import Queue

sys.path.append(os.path.join(os.path.dirname(__file__), 'pipeline'))

from pipeline.read_and_emit import StateFileReader
from pipeline.graph_builder import GraphBuilder
from pipeline.persist import GraphStorage

import argparse

def main():
    # Create queues
    reader_output_queue = Queue()
    builder_output_queue = Queue()

    # Create objects
    reader = StateFileReader(buffer=reader_output_queue)
    builder = GraphBuilder(buffer=reader_output_queue, output_queue=builder_output_queue)
    import datetime
    unique_run_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_filename = f'all_graphs_{unique_run_id}.pkl'
    storage = GraphStorage(input_queue=builder_output_queue, filename=unique_filename)

    # Create threads
    threads = [
        threading.Thread(target=reader.read_and_emit, name="StateFileReaderThread"),
        threading.Thread(target=builder.build_graph, name="GraphBuilderThread"),
        threading.Thread(target=storage.run, name="GraphStorageThread"),
    ]

    # Start threads
    for thread in threads:
        thread.start()

    try:
        while any(thread.is_alive() for thread in threads):
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("KeyboardInterrupt received! Setting stop event and sending sentinels.")
        for _ in range(2):
            reader_output_queue.put(None)
            builder_output_queue.put(None)


if __name__ == '__main__':
    main() 