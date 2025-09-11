from queue import Queue
import time
import threading

import cProfile

from GraphCreation import run_pipeline
from my_model2 import MyModel
from test_filter import filter

def profile_thread(target, *args, **kwargs):
    def wrapped(*args, **kwargs):
        profiler = cProfile.Profile()
        profiler.enable()
        result = target(*args, **kwargs)
        profiler.disable()
        # profiler.dump_stats(f"{threading.current_thread().name}2.prof")        
        return result
    return wrapped
    """    
    model = MyModel(
        filter_out_queue, 
        train_on=2,                     # will train on the first 'train_on' number of graphs it recieves from the queue
        repeat=170,                          # for how many epochs we train the model
        oversampling=oversampling,          # oversampling of non zero values. (1 means we dont oversample, 2 that we double)
        hidden_channels=64
        ) # TODO set optional parameters
    """
def run(counter_weight=1, oversampling=1, max_dist_scalar=2):
    q_limit = 100 # TODO do we need this?
    reader_output_queue = Queue() 
    builder_output_queue = Queue()
    filter_out_queue = Queue()
    state_file='GraphCreation/StateFiles/state.parquet'
    stop_event = threading.Event()

    model = MyModel(builder_output_queue)

    node_id = 3

    kwargs_graph_creation = {
        "reader_output_queue" : reader_output_queue,
        "builder_output_queue" : builder_output_queue,
        "state_file" : state_file,          # location of the state file
        "val_file": 'GraphCreation/StateFiles/' + str(node_id) + '.parquet', # location of the file containing values
        "stop_event" : stop_event, 
        "num_limit" :1010,                 # How many rows to read from the state file (None for all)
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
        # threading.Thread(target=filter, name="filterThread", kwargs={"in_q":builder_output_queue, "out_q": filter_out_queue,"stop_event": stop_event}),
        threading.Thread(target=profile_thread(model.train), name="GNNthread"),
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