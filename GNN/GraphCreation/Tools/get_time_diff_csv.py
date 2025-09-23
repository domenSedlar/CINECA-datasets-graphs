import sys
import os
import threading
import time
from queue import Queue
import datetime
import cProfile
import csv
sys.path.append(os.path.join(os.path.dirname(__file__), 'pipeline'))

from pipeline.read_and_emit import StateFileReader
from pipeline.graph_builder import GraphBuilder, GraphTypes
from pipeline.persist import GraphStorage

def out_csv(stop_event=None, buffer=None, file="time_diff.csv"):
    """
        Writes to csv values recieved from the buffer
    """

    with open(file, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=';')
        
        # Write header row (excluding "value", "node", "timestamp")
        header_written = False
        print("workin")
        while True:
            val = buffer.get()
            if val is None:
                break
            if stop_event and stop_event.is_set():
                # print("out_csv: detected stop_event set, breaking loop.")
                break

                        # Write header once
            if not header_written:
                headers = [k for k in val.keys()]
                writer.writerow(headers)
                header_written = True
            
            # Write row values
            row = [v for k, v in val.items()]
            writer.writerow(row)

    print("done")

def consumer(stop_event=None, buffer=None):
    """
        Consumes items from the buffer and does nothing with it.
        Doing this might be better than just keeping it in memmory
    """

    while True:
        val = buffer.get()
        if val is None:
            break
        if stop_event and stop_event.is_set():
            # print("out_csv: detected stop_event set, breaking loop.")
            break

def run(reader_output_queue = Queue(), builder_output_queue = Queue(), state_file='StateFiles/state.parquet', val_file='StateFiles/2.parquet', start_ts=None, end_ts=None, stop_event = threading.Event(), num_limit=None, nodes = {2}, graph_type=GraphTypes.NodeTree, skip_None=True, max_dist_scalar=8):
    """
        For each row in the state parquet files, it checks where the closest valid value is (value column tells us if the node is running) in the original.
        It saves this data to a csv file

        At the start of this function edit the variables to change which nodes we check, and to what file we output.
    """

    # you can edit these four lines, to limit which section of the data the program processes
    node_ids = [2, 3, 4, 5, 6, 19, 32] 
    file="time_diff.csv"
    start_ts = datetime.datetime.fromisoformat("2020-03-01 00:00:00+00:00")
    end_ts = datetime.datetime.fromisoformat("2022-10-21 07:30:00+00:00")


    reader = StateFileReader(
        buffer=reader_output_queue, 
        state_file=['StateFiles/state.parquet', "StateFiles/threaded_pipeline_state_2025-08-10_09-30-02_rack1.parquet"], 
        val_file=['StateFiles/' + str(n) + '.parquet' for n in node_ids], 
        skip_None=skip_None
    )
    time_diff_buff = Queue()

    # Create threads
    threads = [
        threading.Thread(target=reader.read_and_emit, name="StateFileReaderThread", kwargs={"stop_event": stop_event, "num_limit":num_limit, "lim_nodes":node_ids, "skip_None":skip_None, "max_dist_scalar":max_dist_scalar, "start_ts":start_ts, "end_ts":end_ts, "time_diff_buff": time_diff_buff, "max_dist_scalar": 2}),
        threading.Thread(target=out_csv, name="csvbuilder", kwargs={"stop_event": stop_event, "buffer": time_diff_buff}),
        threading.Thread(target=consumer, name="consumer", kwargs={"stop_event": stop_event, "buffer": reader_output_queue, "file": file}),

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