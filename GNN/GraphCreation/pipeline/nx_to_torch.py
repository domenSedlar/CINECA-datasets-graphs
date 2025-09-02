import networkx as nx
import torch
from torch_geometric.utils import from_networkx

class Nx2T1Conv:
    num_node_features = 1 # (value)
    num_classes = 4 # 0 ~ ok, 1 ~ down, 2 ~ unreachable, 4 ~ unknown

    def __init__(self):
        pass

    def conv(self, nx_graph):
        # y ~ classification of the graph
        if nx_graph.graph["value"] is not None:
             # TODO value should be int, but isnt allways
            y_val = int(round(nx_graph.graph["value"]))
            nx_graph.graph["y"] = torch.tensor([y_val], dtype=torch.long)
        else:
            #TODO what do we do with None values?
            y_val = 0
            nx_graph.graph["y"] = torch.tensor([y_val], dtype=torch.long)


        # x ~ features of nodes
        for node in nx_graph.nodes(data=False):
            node_id = node # TODO maybe make gnn differentiate nodes
            if "value" in nx_graph.nodes[node].keys():
                if nx_graph.nodes[node]["value"] is not None:
                    node_value = nx_graph.nodes[node]["value"]
                else:
                    node_value = 0
                nx_graph.nodes[node]["position"] = (0,0)
            else:
                node_value = 0
                nx_graph.nodes[node]["value"] = 0
                nx_graph.nodes[node]["type"] = 0

            nx_graph.nodes[node]["x"] = torch.tensor([float(node_value)])
            # print(nx_graph.nodes[node])

        data = from_networkx(nx_graph)

        return data