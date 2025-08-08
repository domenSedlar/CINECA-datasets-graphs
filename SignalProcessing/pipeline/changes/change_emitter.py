import logging
logger = logging.getLogger(__name__)
import json
import os
from simple_queue import SimpleQueue as Queue
from .change_detector import ChangeLevelDetector

class ChangeEmitter:
    def __init__(self, input_queue, output_queue):
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.detector = ChangeLevelDetector(input_queue, output_queue)

    def run(self, timeout=0):
        self.detector.run(timeout=timeout)

if __name__ == "__main__":
    # Read the JSON file
    tmp_path = os.path.join(os.path.dirname(__file__), "tmp.json")
    with open(tmp_path, "r") as f:
        f.seek(0)
        data = [json.loads(line) for line in f if line.strip()]

    batch = data

    # Setup queues
    input_queue = Queue()
    output_queue = Queue()

    # Push the batch into the input queue
    for b in batch:
        input_queue.put(b)
    # Import and run the detector
    detector = ChangeLevelDetector(input_queue, output_queue)
    detector.run(timeout=0)

    # Print the outputs
    while not output_queue.empty():
        print(output_queue.get()) 