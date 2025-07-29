import os
import json

def persist_graphs(graphs, output_dir='output_graphs'):
    """
    Saves each graph to a separate JSON file in the output directory.
    """
    os.makedirs(output_dir, exist_ok=True)
    for node_id, graph in graphs.items():
        out_path = os.path.join(output_dir, f'node_{node_id}.json')
        with open(out_path, 'w') as f:
            json.dump(graph, f, indent=2)
    print(f'Saved {len(graphs)} graphs to {output_dir}')

if __name__ == '__main__':
    from graph_emitter import emit_graphs
    from graph_builder import build_graph_for_node
    from read_and_emit import read_state_file, STATE_FILE
    node_series = read_state_file(STATE_FILE)
    graphs = {node_id: build_graph_for_node(series) for node_id, series in node_series.items()}
    emitted_graphs = emit_graphs(graphs)
    persist_graphs(emitted_graphs) 