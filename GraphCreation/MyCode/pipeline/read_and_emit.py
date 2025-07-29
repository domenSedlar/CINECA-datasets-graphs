import json
from collections import defaultdict
from queue import Queue

class StateFileReader:
    def __init__(self, buffer, state_file='StateFiles/threaded_pipeline_state.json'):
        self.state_file = state_file
        self.buffer = buffer

    def read_and_emit(self):
        """
        Reads the state file line by line and puts each line into the buffer.
        Each line contains a JSON object with node data.
        """
        try:
            with open(self.state_file, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    if not line.strip():
                        continue
                    
                    try:
                        # Parse the JSON line
                        state_data = json.loads(line.strip())
                        
                        # Put the parsed data into the buffer
                        self.buffer.put(state_data)
                        
                    except json.JSONDecodeError as e:
                        print(f"Error parsing JSON on line {line_num}: {e}")
                        continue
                    except Exception as e:
                        print(f"Error processing line {line_num}: {e}")
                        continue
                        
        except FileNotFoundError:
            print(f"State file not found: {self.state_file}")
        except Exception as e:
            print(f"Error reading state file: {e}")
        
        # Signal end of data by putting None into buffer
        self.buffer.put(None)


if __name__ == '__main__':
    STATE_FILE = 'StateFiles/threaded_pipeline_state.json'
    buffer = Queue()
    reader = StateFileReader(STATE_FILE, buffer)
    reader.read_and_emit()

    while True:
        state = buffer.get()
        if state is None:
            break
        print(state)
    