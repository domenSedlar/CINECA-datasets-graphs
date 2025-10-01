from queue import Queue
import time
import threading
import os

import cProfile

from GraphCreation import run_pipeline
from model.my_model import MyModel
from test_filter import filter
import datetime
from model.get_dataloaders import MyLoader

def profile_thread(target, *args, **kwargs):
    def wrapped(*args, **kwargs):
        profiler = cProfile.Profile()
        profiler.enable()
        result = target(*args, **kwargs)
        profiler.disable()
        # profiler.dump_stats(f"{threading.current_thread().name}2.prof")        
        return result
    return wrapped

def run_graph_creation(train_kwargs, test_kwargs, valid_kwargs):
    run_pipeline.run(**test_kwargs)
    run_pipeline.run(**train_kwargs)
    run_pipeline.run(**valid_kwargs)

def list_val_files(nodes):
    ls = []
    base_dir = os.path.abspath(os.path.join(os.getcwd(), "..", "SignalProcessing", "TarFiles"))
    for root, _, files in os.walk(base_dir):
        for file in files:
            id = file.split(".")[0]
            if int(id) in nodes:
                ls.append(os.path.join(root, file))
    
    return ls

def list_state_files(nodes):
    racks = []
    for n in nodes: # first we need to find out in which racks our nodes are
        for r in range(45):
            if r*20 <= n and n < r*20 + 20:
                if not r in racks:
                    racks.append(r)
                continue

    ls = []
    base_dir = os.path.abspath(os.path.join(os.getcwd(), "GraphCreation", "StateFiles"))
    for root, _, files in os.walk(base_dir):
        for file in files:
            for r in racks:
                if f"rack{r}.parquet" in file: 
                    ls.append(os.path.join(root, file))
                    continue
    return ls

        
def get_loader(train_start_ts, train_end_ts, test_start_ts, test_end_ts, valid_start_ts, valid_end_ts, node_ids, state_file, max_dist_scalar=4, invalid=[]):

    train_reader_output_queue = Queue() 
    train_builder_output_queue = Queue()
    test_reader_output_queue = Queue() 
    test_builder_output_queue = Queue()
    valid_reader_output_queue = Queue() 
    valid_builder_output_queue = Queue()
    filter_out_queue = Queue()
    stop_event = threading.Event()

    kwargs_graph_creation = {
        "reader_output_queue" : train_reader_output_queue,
        "builder_output_queue" : train_builder_output_queue,
        "state_file" : list_state_files(node_ids),          # location of the state files. You can also replace this function with just a list of paths to them
        #"val_file": ['GraphCreation/StateFiles/' + str(n) + '.parquet' for n in node_ids], # location of the files containing values
        "val_file": list_val_files(node_ids), # location of the files containing values
        "stop_event" : stop_event, 
        "num_limit" : None,                 # How many rows to read from the state file (None for all)
        "nodes" : node_ids,                # list of nodes we use
        "skip_None": True,                  # do we skip rows with no valid class?
        "max_dist_scalar": max_dist_scalar, # how close does the machine state need to be for it to be relevant. (in 15 min intervals)
        "start_ts":train_start_ts,
        "end_ts":train_end_ts,
        "invalid":invalid
            
            # sometimes there are intervals of time where the machine status wasn't being monitored
            # if the gap is small, we can just return the next value
            # if large, the machine status might no longer be relavent to the current timestamp
            # max_dist_scalar tells the program how large the gap is allowed to be.
            # where gap = max_dist_scalar * 15 min.
    }

    training_kwargs = kwargs_graph_creation.copy()
    test_kwargs = kwargs_graph_creation.copy()
    valid_kwargs = kwargs_graph_creation.copy()
    test_kwargs["reader_output_queue"] = test_reader_output_queue
    test_kwargs["builder_output_queue"] = test_builder_output_queue
    test_kwargs["start_ts"] = test_start_ts
    test_kwargs["end_ts"] = test_end_ts
    test_kwargs["num_limit"] = None

    valid_kwargs["reader_output_queue"] = valid_reader_output_queue
    valid_kwargs["builder_output_queue"] = valid_builder_output_queue
    valid_kwargs["start_ts"] = valid_start_ts
    valid_kwargs["end_ts"] = valid_end_ts

    ds = MyLoader(train_builder_output_queue, test_builder_output_queue, valid_builder_output_queue)
        # Create threads
    threads = [
        threading.Thread(target=run_graph_creation, name="GraphCreatorThread", kwargs={"train_kwargs":training_kwargs, "test_kwargs":test_kwargs, "valid_kwargs": valid_kwargs}),
        # threading.Thread(target=filter, name="filterThread", kwargs={"in_q":train_builder_output_queue, "out_q": filter_out_queue,"stop_event": stop_event}),

        threading.Thread(target=profile_thread(ds._init_data), name="dataloader", kwargs={"stop_event": stop_event}),
        # threading.Thread(target=storage.run, name="GraphStorageThread"),
    ]

    # Start threads
    for thread in threads:
        print("starting run pipeline thread")
        thread.start()

    try:
        while any(thread.is_alive() for thread in threads):
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("KeyboardInterrupt received! Setting stop event and sending sentinels.")
        stop_event.set()

    return ds

def run(dataset, adjust_weights=False, dropout=0.002232071679031126, llr=0.002530762230047059, aggr_method='add', pool_method="mean", num_of_layers=3, hidden_channels=64, train_for=171):
    model = MyModel(dataset=dataset, dropout=dropout, adjust_weights=adjust_weights, llr=llr, aggr_method=aggr_method, pool_method=pool_method, num_of_layers=num_of_layers, hidden_channels=hidden_channels)
    stop_event = threading.Event()

    # Container to hold the output
    results = {}

    def target_func(stop_event, results, train_for):
        # Train the model and store the final metric (e.g., AUC)
        auc = model.train(stop_event=stop_event, train_for=train_for)  # assume train returns AUC
        results["auc"] = auc

    thread = threading.Thread(target=target_func, name="dataloader", kwargs={"stop_event": stop_event, "results": results, "train_for": train_for})
    
    thread.start()
    try:
        while thread.is_alive():
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("KeyboardInterrupt received! Setting stop event.")
        stop_event.set()
    
    thread.join()  # ensure thread finishes
    return results.get("auc")


def main():
    node_ids = [886] # list here the nodes you wish to train on
    # Add the paths to the processed files containing data for the nodes here. (remove the paths which don't exist)
    state_file=['GraphCreation/StateFiles/state.parquet', "GraphCreation/StateFiles/threaded_pipeline_state_2025-08-10_09-30-02_rack1.parquet", "GraphCreation/StateFiles/threaded_pipeline_state_2025-08-10_13-31-41_rack44.parquet"]
    # the program assumes the original files will be present in the same StateFiles folder, with their names unchanged. (dont create any new folders inside of the StateFiles folder)

    train_start_ts = datetime.datetime.fromisoformat("2022-01-01 00:00:00+00:00").astimezone()
    train_end_ts = datetime.datetime.fromisoformat("2022-07-01 00:00:00+00:00").astimezone()
    #test_start_ts = datetime.datetime.fromtimestamp(1589208300000 / 1000).astimezone()# dividing by 1000 to remove miliseconds, since datatime.fromtimestamp function doesnt expect them
    test_start_ts = datetime.datetime.fromisoformat("2022-07-01 00:00:00+00:00").astimezone()
    test_end_ts = datetime.datetime.fromisoformat("2022-10-02 00:00:00+00:00").astimezone()
    #valid_start_ts = datetime.datetime.fromisoformat("2021-10-25 00:00:00+00:00").astimezone()
    #valid_end_ts = datetime.datetime.fromisoformat("2021-11-01 00:00:00+00:00").astimezone()
    valid_start_ts = datetime.datetime.fromisoformat("2020-09-01 00:00:00+00:00").astimezone()
    valid_end_ts = datetime.datetime.fromisoformat("2020-12-01 00:00:00+00:00").astimezone()

    # the top of the get_loader function has variables defining which time intervals are used for training, validation and the final test dataset. You can edit them there if you wish.
    run(get_loader(train_start_ts, train_end_ts, test_start_ts, test_end_ts, valid_start_ts, valid_end_ts, node_ids, state_file))

if __name__ == '__main__':
    main()