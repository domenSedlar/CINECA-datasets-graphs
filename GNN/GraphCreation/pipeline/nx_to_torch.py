import networkx as nx
import torch
from torch_geometric.utils import from_networkx

class Nx2T1Conv:
    num_node_features = 2 # (sensor name, value)
    num_classes = 4 # 0 ~ ok, 1 ~ down, 2 ~ unreachable, 4 ~ unknown

    def __init__(self):
        pass

    def conv(nx_graph):
        # y ~ classification of the graph
        nx_graph.graph["y"] = torch.tensor(int(round(nx_graph.graph["value"]))) # TODO value should be int, but isnt allways

        # x ~ features of nodes
        for node, attrs in nx_graph.nodes(data=True):
            node_id = node # TODO make gnn differentiate nodes
            node_value = attrs["value"]

            nx_graph.nodes[node]["x"] = torch.tensor([float(node_value)])

        data = from_networkx(nx_graph)

        return data