from pipeline.ingest_redis_queue import IngestRedisQueue
from pipeline.change_detector import ChangeLevelDetector
import time
from river.drift import ADWIN

def main():
    # Clear any old data in the queues
    input_queue = IngestRedisQueue(queue_name='test_input')
    output_queue = IngestRedisQueue(queue_name='test_output')
    input_queue.redis.delete('test_input')
    output_queue.redis.delete('test_output')

    # Push test data (simulate a stream with some changes)
    test_data = (
        [{'timestamp': i, 'value': 10} for i in range(50)] +
        [{'timestamp': 50 + i, 'value': 500} for i in range(50)] +
        [{'timestamp': 100 + i, 'value': 10} for i in range(50)]
    )
    for row in test_data:
        input_queue.put(row)

    # Run the change detector (non-blocking, will exit when input is empty)
    detector = ChangeLevelDetector(input_queue, output_queue, delta=0.1)
    detector.run(timeout=1)

    # Print results from output queue
    print("Detected changes:")
    while True:
        result = output_queue.pop(timeout=1)
        if result is None:
            break
        print(result)

if __name__ == "__main__":
    main() 