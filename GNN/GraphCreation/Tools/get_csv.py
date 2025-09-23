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

def out_csv(stop_event=None, buffer=None, file="data.csv"):
    """
        Saves data from the buffer to a csv file
    """

    ones = 0
    zeros = 0

    with open(file, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=';')
        
        # Write header row (excluding "value", "node", "timestamp")
        header_written = False
        while True:
            val = buffer.get()
            if val is None:
                break
            if stop_event and stop_event.is_set():
                # print("out_csv: detected stop_event set, breaking loop.")
                break
            val = val[2]
            if val["value"] == 0:

                zeros += 1
            else:

                ones += 1
                        # Write header once
            if not header_written:
                headers = [k for k in val.keys() if k not in ("value", "node", "timestamp")]
                headers.append("value")
                writer.writerow(headers)
                header_written = True
            
            # Write row values
            row = [v for k, v in val.items() if k not in ("value", "node", "timestamp")]
            row.append(val["value"])
            writer.writerow(row)

            if (ones + zeros) % 1000 == 0:
                print("ones: ", (ones/(ones+zeros))*100, "%")
    print("done")


def run(start_ts, end_ts, output_file, reader_output_queue = Queue(), builder_output_queue = Queue(), state_file='StateFiles/state.parquet', val_file={'StateFiles/2.parquet'}, stop_event = threading.Event(), num_limit=None, nodes = {2}, graph_type=GraphTypes.NodeTree, skip_None=True, max_dist_scalar=8):
    """
        Reads the specified data, and writes it to a csv file
    """
    
    # Create objects
    reader = StateFileReader(buffer=reader_output_queue, state_file=state_file, val_file=val_file, skip_None=skip_None)


    # Create threads
    threads = [
        threading.Thread(target=reader.read_and_emit, name="StateFileReaderThread", kwargs={"stop_event": stop_event, "num_limit":num_limit, "lim_nodes":nodes, "skip_None":skip_None, "max_dist_scalar":max_dist_scalar, "start_ts":start_ts, "end_ts":end_ts}),
        threading.Thread(target=out_csv, name="csvbuilder", kwargs={"stop_event": stop_event, "buffer": reader_output_queue, "file": output_file}),
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
    start_ts = datetime.datetime.fromisoformat("2022-02-01 00:00:00+00:00")
    end_ts = datetime.datetime.fromisoformat("2022-03-21 07:30:00+00:00")
    output_file = "data.csv"

    run(start_ts, end_ts, output_file)