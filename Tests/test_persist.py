from pipeline.ingest_redis_queue import IngestRedisQueue
from pipeline.persist import StatePersister
import json
import os
import time

def main():
    input_queue = IngestRedisQueue(queue_name='test_persist_input')
    input_queue.redis.delete('test_persist_input')
    output_file = 'test_latest_state.json'
    if os.path.exists(output_file):
        os.remove(output_file)

    # Push a few state dicts
    states = [
        {'A': {'node': 'A', 'timestamp': 0, 'value': 10}},
        {'A': {'node': 'A', 'timestamp': 100, 'value': 20}, 'B': {'node': 'B', 'timestamp': 12, 'value': 20}},
    ]
    for state in states:
        input_queue.push(state)

    persister = StatePersister(input_queue, output_file=output_file)
    for _ in range(len(states)):
        persister.run(timeout=1)
        # Print the file contents after each write
        with open(output_file) as f:
            print(f"Current file contents after update:\n{f.read()}\n---")

if __name__ == "__main__":
    main() 