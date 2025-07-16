from pipeline.ingest_redis_queue import IngestRedisQueue
from pipeline.state_builder import StateBuilder
import time

def main():
    # Clear any old data in the queues
    input_queue = IngestRedisQueue(queue_name='test_state_input')
    output_queue = IngestRedisQueue(queue_name='test_state_output')
    input_queue.redis.delete('test_state_input')
    output_queue.redis.delete('test_state_output')

    # Interleaved test data for two nodes
    test_data = [
        {'node': 'A', 'timestamp': 0, 'value': 10},
        {'node': 'B', 'timestamp': 0, 'value': 10},
        {'node': 'A', 'timestamp': 100, 'value': 20},
        {'node': 'B', 'timestamp': 12, 'value': 20},
    ]
    for row in test_data:
        input_queue.push(row)

    # Run the state builder (non-blocking, will exit when input is empty)
    state_builder = StateBuilder(input_queue, output_queue)
    # Run in a separate thread or process in production; here, just run directly
    for _ in range(len(test_data)):
        state_builder.run(timeout=1)

    # Print results from output queue
    print("Emitted states:")
    while True:
        result = output_queue.pop(timeout=1)
        if result is None:
            break
        print(result)

if __name__ == "__main__":
    main() 