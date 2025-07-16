import json

class StatePersister:
    def __init__(self, input_queue, output_file='latest_state.json'):
        self.input_queue = input_queue
        self.output_file = output_file

    def run(self, timeout=0):
        while True:
            state = self.input_queue.pop(timeout=timeout)
            if state is None:
                break
            with open(self.output_file, 'a') as f:
                f.write(json.dumps(state) + '\n')