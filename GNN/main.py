from queue import Queue
import time
import threading

import cProfile

from GraphCreation import run_pipeline
from my_model import MyModel

def profile_thread(target, *args, **kwargs):
    def wrapped(*args, **kwargs):
        profiler = cProfile.Profile()
        profiler.enable()
        result = target(*args, **kwargs)
        profiler.disable()
        profiler.dump_stats(f"{threading.current_thread().name}.prof")        
        return result
    return wrapped

def main():
    q_limit = 100 # TODO do we need this?
    reader_output_queue = Queue() 
    builder_output_queue = Queue()
    state_file='GraphCreation/StateFiles/state.parquet'
    stop_event = threading.Event()

    model = MyModel(builder_output_queue, train_on=30000, repeat=171) # TODO set optional parameters

    kwargs_graph_creation = {
        "reader_output_queue" : reader_output_queue,
        "builder_output_queue" : builder_output_queue,
        "state_file" : state_file,
        "stop_event" : stop_event,
        "num_limit" : 50000,
        "nodes" : {2},
        "skip_None": True,
        "val_file": 'GraphCreation/StateFiles/2.parquet'
    }

        # Create threads
    threads = [
        threading.Thread(target=run_pipeline.run, name="GraphCreatorThread", kwargs=kwargs_graph_creation),
        threading.Thread(target=profile_thread(model.train), name="GNNthread", kwargs={"stop_event": stop_event}),
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
    main()