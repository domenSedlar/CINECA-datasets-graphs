class GraphEmitter:
    """
    Formats graphs into a format that can be persisted.
    """
    def __init__(self, input_queue, output_queue):
        self.input_queue = input_queue
        self.output_queue = output_queue
        
    def emit_graphs(self):
if __name__ == '__main__':
    from graph_builder import build_graph_for_node
    from read_and_emit import read_state_file, STATE_FILE
    node_series = read_state_file(STATE_FILE)
    graphs = {node_id: build_graph_for_node(series) for node_id, series in node_series.items()}
    emitted_graphs = emit_graphs(graphs)
    # For demonstration, print the emitted graphs
    for node_id, graph in emitted_graphs.items():
        print(f'Emitted graph for node {node_id}: {graph}') 