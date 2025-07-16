import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'pipeline'))

from pipeline.read_and_emit import read_state_file, STATE_FILE
from pipeline.graph_builder import build_graph_for_node
from pipeline.graph_emitter import emit_graphs
from pipeline.persist import persist_graphs

import argparse

def main():
    parser = argparse.ArgumentParser(description='Run the state-to-graph pipeline.')
    parser.add_argument('--state-file', type=str, default=STATE_FILE, help='Path to the state file (default: ../state.json)')
    parser.add_argument('--output-dir', type=str, default='output_graphs', help='Directory to save output graphs')
    args = parser.parse_args()

    print(f'Reading state file: {args.state_file}')
    node_series = read_state_file(args.state_file)
    print('Building graphs for each node...')
    graphs = {node_id: build_graph_for_node(series) for node_id, series in node_series.items()}
    print('Emitting graphs...')
    emitted_graphs = emit_graphs(graphs)
    print(f'Persisting graphs to {args.output_dir}...')
    persist_graphs(emitted_graphs, output_dir=args.output_dir)
    print('Pipeline completed successfully.')

if __name__ == '__main__':
    main() 