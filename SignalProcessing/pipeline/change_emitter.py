class ChangeEmitter:
    def __init__(self, input_queue, output_queue):
        self.input_queue = input_queue
        self.output_queue = output_queue

    def run(self, timeout=0):
        while True:
            data = self.input_queue.pop(timeout=timeout)
            if data is None:
                break  # Wait for more data instead of breaking
            self.output_queue.push(data) 