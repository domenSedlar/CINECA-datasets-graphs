from queue import Queue
import time
import threading

import cProfile

from GraphCreation import run_pipeline
from my_model2 import MyModel
from test_filter import filter
import datetime

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

def run_graph_creation(train_kwargs, test_kwargs):
    run_pipeline.run(**test_kwargs)
    run_pipeline.run(**train_kwargs)

def run(counter_weight=1, oversampling=1, max_dist_scalar=2):
    train_reader_output_queue = Queue() 
    train_builder_output_queue = Queue()
    test_reader_output_queue = Queue() 
    test_builder_output_queue = Queue()
    filter_out_queue = Queue()
    state_file='GraphCreation/StateFiles/state.parquet'
    stop_event = threading.Event()

    model = MyModel(train_builder_output_queue, test_builder_output_queue)

    node_ids = {2, 19}

    train_start_ts = datetime.datetime.fromisoformat("2020-07-01 00:00:00+00:00").astimezone()
    train_end_ts = datetime.datetime.fromisoformat("2020-08-01 00:00:00+00:00").astimezone()
    #test_start_ts = datetime.datetime.fromtimestamp(1589208300000 / 1000).astimezone()# dividing by 1000 to remove miliseconds, since datatime.fromtimestamp function doesnt expect them
    test_start_ts = datetime.datetime.fromisoformat("2020-12-01 00:00:00+00:00").astimezone()
    test_end_ts = datetime.datetime.fromisoformat("2021-01-01 00:00:00+00:00").astimezone()

    kwargs_graph_creation = {
        "reader_output_queue" : train_reader_output_queue,
        "builder_output_queue" : train_builder_output_queue,
        "state_file" : state_file,          # location of the state file
        "val_file": ['GraphCreation/StateFiles/' + str(n) + '.parquet' for n in node_ids], # location of the files containing values
        "stop_event" : stop_event, 
        "num_limit" : None,                 # How many rows to read from the state file (None for all)
        "nodes" : node_ids,                # list of nodes we use
        "skip_None": True,                  # do we skip rows with no valid class?
        "max_dist_scalar": max_dist_scalar, # how close does the machine state need to be for it to be relevant. (in 15 min intervals)
        "start_ts":train_start_ts,
        "end_ts":train_end_ts
            
            # sometimes there are intervals of time where the machine status wasn't being monitored
            # if the gap is small, we can just return the next value
            # if large, the machine status might no longer be relavent to the current timestamp
            # max_dist_scalar tells the program how large the gap is allowed to be.
            # where gap = max_dist_scalar * 15 min.
    }

    training_kwargs = kwargs_graph_creation.copy()
    test_kwargs = kwargs_graph_creation.copy()
    test_kwargs["reader_output_queue"] = test_reader_output_queue
    test_kwargs["builder_output_queue"] = test_builder_output_queue
    test_kwargs["start_ts"] = test_start_ts
    test_kwargs["end_ts"] = test_end_ts
    test_kwargs["num_limit"] = None

        # Create threads
    threads = [
        threading.Thread(target=run_graph_creation, name="GraphCreatorThread", kwargs={"train_kwargs":training_kwargs, "test_kwargs":test_kwargs}),
        # threading.Thread(target=filter, name="filterThread", kwargs={"in_q":train_builder_output_queue, "out_q": filter_out_queue,"stop_event": stop_event}),
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

def main():

    run()

if __name__ == '__main__':
    main()