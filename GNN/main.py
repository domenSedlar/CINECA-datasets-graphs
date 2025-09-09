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
        profiler.dump_stats(f"{threading.current_thread().name}2.prof")        
        return result
    return wrapped

def run(counter_weight=1, oversampling=1, max_dist_scalar=2):
    q_limit = 100 # TODO do we need this?
    reader_output_queue = Queue() 
    builder_output_queue = Queue()
    state_file='GraphCreation/StateFiles/state.parquet'
    stop_event = threading.Event()

    model = MyModel(
        builder_output_queue, 
        train_on=15000,                     # will train on the first 'train_on' number of graphs it recieves from the queue
        repeat=30,                          # for how many epochs we train the model
        counter_weight=counter_weight,      # higher number(1 being lowest) puts a lower weight for the class 0, which means everything is fine.(0 makes up most of the dataset)
        oversampling=oversampling,          # oversampling of non zero values. (1 means we dont oversample, 2 that we double)
        hidden_channels=32
        ) # TODO set optional parameters

    node_id = 4

    kwargs_graph_creation = {
        "reader_output_queue" : reader_output_queue,
        "builder_output_queue" : builder_output_queue,
        "state_file" : state_file,          # location of the state file
        "val_file": 'GraphCreation/StateFiles/' + str(node_id) + '.parquet', # location of the file containing values
        "stop_event" : stop_event, 
        "num_limit" :30000,                 # How many rows to read from the state file (None for all)
        "nodes" : {node_id},                # list of nodes we use
        "skip_None": True,                  # do we skip rows with no valid class?
        "max_dist_scalar": max_dist_scalar # how close does the machine state need to be for it to be relevant. (in 15 min intervals)
            
            # sometimes there are intervals of time where the machine status wasn't being monitored
            # if the gap is small, we can just return the next value
            # if large, the machine status might no longer be relavent to the current timestamp
            # max_dist_scalar tells the program how large the gap is allowed to be.
            # where gap = max_dist_scalar * 15 min.
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


def main():
    run()

if __name__ == '__main__':
    main()