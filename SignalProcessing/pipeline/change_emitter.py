import logging
logger = logging.getLogger(__name__)

class ChangeEmitter:
    def __init__(self, input_queue, output_queue):
        self.input_queue = input_queue
        self.output_queue = output_queue

    def run(self, timeout=0): # does nothing, it was in the sketch of the pipeline, We can make it do something later.
        while True:
            data = self.input_queue.pop(timeout=timeout)
            logger.info(f"Received data items from input_queue in ChangeEmitter")
            if data is None:
                break  # Wait for more data instead of breaking
            self.output_queue.push(data) 