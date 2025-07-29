import json
from collections import defaultdict

STATE_FILE = 'state.json'

def read_state_file(state_file):
    """
    Reads the state file line by line and emits time series for each node.
    Returns a dict: {node_id: [(timestamp, value), ...]}
    """
    node_series = defaultdict(list)
    with open(state_file, 'r') as f:
        for line in f:
            if not line.strip():
                continue
            state = json.loads(line)
            for node_id, entry in state.items():
                node_series[node_id].append((entry['timestamp'], entry['value']))
    return node_series

if __name__ == '__main__':
    node_series = read_state_file(STATE_FILE)
    # Buffer: sort by timestamp for each node
    for node_id in node_series:
        node_series[node_id].sort()
    # For demonstration, print the first 5 entries for each node
    for node_id, series in node_series.items():
        print(f'Node {node_id}: {series[:5]}') 