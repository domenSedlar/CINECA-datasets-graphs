import os
import sys
from pipeline.reader import read_tar_parquet
from pipeline.change_detector import ChangeLevelDetector
from pipeline.change_emitter import ChangeEmitter
from pipeline.state_builder import StateBuilder
from pipeline.persist import StatePersister

# Simple in-memory FIFO queue for single-threaded use
class SimpleQueue:
    def __init__(self):
        self._queue = []
    def push(self, item):
        self._queue.append(item)
    def pop(self, timeout=0):
        if self._queue:
            return self._queue.pop(0)
        return None

def main():
    tar_path = os.path.join('TarFiles', '20-04.tar')
    output_file = 'test_latest_state.json'

    # Remove output file if exists
    if os.path.exists(output_file):
        os.remove(output_file)

    # Queues between pipeline stages
    buffer_queue = SimpleQueue()
    change_queue = SimpleQueue()
    state_queue = SimpleQueue()
    emit_queue = SimpleQueue()

    # Pipeline stages
    change_detector = ChangeLevelDetector(buffer_queue, change_queue)
    change_emitter = ChangeEmitter(change_queue, state_queue)
    state_builder = StateBuilder(state_queue, emit_queue)
    state_persister = StatePersister(emit_queue, output_file=output_file)

    # 1. Read tar file and emit rows into buffer_queue
    id = -1
    i = 0
    for row in read_tar_parquet(tar_path):
        if(id == -1):
            id = row['node']
        else:
            if(i == 10):
                print(f"read node {id}")
                break
            elif(id != row['node']):
                i += 1
                id = row['node']
                print(f"read node {id}")
                continue
        # Convert pandas.Timestamp to int for JSON serialization
        if 'timestamp' in row and isinstance(row['timestamp'], __import__('pandas').Timestamp):
            row['timestamp'] = int(row['timestamp'].timestamp())
        buffer_queue.push(row)

    # 2. Run each stage until input is empty
    change_detector.run()
    change_emitter.run()
    state_builder.run()
    state_persister.run()

    print(f"Pipeline complete. Output written to {output_file}")

if __name__ == "__main__":
    main() 