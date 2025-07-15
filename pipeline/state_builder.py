class StateBuilder:
    def __init__(self, input_queue, output_queue):
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.state = {}

    def run(self, timeout=0):
        while True:
            data = self.input_queue.pop(timeout=timeout)
            if data is None:
                break
            node = data.get('node')
            if node is not None:
                self.state[node] = data
                # Emit a copy of the entire state dict
                self.output_queue.push(self.state.copy()) 