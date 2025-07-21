import importlib

from ts2g2.core.model import TimeseriesArrayStream, Timeseries
from ts2g2.generation.strategies import RandomWalkWithRestartSequenceGenerationStrategy
from ts2g2.timeseries.strategies import TimeseriesToGraphStrategy, TimeseriesEdgeVisibilityConstraintsHorizontal, EdgeWeightingStrategyNull
import networkx as nx
import matplotlib.pyplot as plt

# Optionally import networkx for further processing or visualization
# import networkx as nx

def build_graph_for_node(timeseries):
    """
    Given a list of (timestamp, value) tuples, build a graph using ts2g2.
    Returns the ts2g2 graph object (or a serializable representation).
    """
    # Sort by timestamp just in case
    timeseries = sorted(timeseries)
    values = [float(v) for _, v in timeseries]
    stream = TimeseriesArrayStream(values)
    ts = Timeseries(stream)
    ts2g = TimeseriesToGraphStrategy([TimeseriesEdgeVisibilityConstraintsHorizontal()], "undirected", EdgeWeightingStrategyNull())
    g = ts2g.to_graph(stream)
    #nx.draw(g.graph)
    #plt.show()
    # For persistence, return a serializable representation (e.g., edge list)
    return {
        'edges': list(g.graph.edges()),
        'nodes': list(g.graph.nodes()),
        'values': values
    }

if __name__ == '__main__':
    from read_and_emit import read_state_file, STATE_FILE
    node_series = read_state_file(STATE_FILE)
    graphs = {}
    for node_id, series in node_series.items():
        graphs[node_id] = build_graph_for_node(series)
    # For demonstration, print the graph info for each node
    for node_id, graph in graphs.items():
        print(f'Node {node_id}: {graph}') 