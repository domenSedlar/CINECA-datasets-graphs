import networkx as nx
import torch
from torch_geometric.utils import from_networkx

class Nx2T1Conv:
    num_node_features = 1 # (value)
    num_classes = 4 # 0 ~ ok, 1 ~ down, 2 ~ unreachable, 3 ~ unknown

    def __init__(self):
        pass

    def conv(self, nx_graph):
        # y ~ classification of the graph
        if nx_graph.graph["value"] is not None:
             # TODO value should be int, but isnt allways
            y_val = int(round(nx_graph.graph["value"]))
            nx_graph.graph["y"] = torch.tensor([y_val], dtype=torch.long)
            # print("y:",nx_graph.graph["value"], y_val)
        else:
            #TODO what do we do with None values?
            y_val = 3
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
        del data.graph_value # TODO check what graph_value even is
        return data
    

class Nx2T1Conv2: # this one also keeps track of node type

    # TODO add more graph types
    sensor_types = ["temp", "pow", "other", "root"]

    num_node_features = 1 + len(sensor_types) # (type, value)
    num_classes = 4 # 0 ~ ok, 1 ~ down, 2 ~ unreachable, 3 ~ unknown

    def __init__(self):
        pass

    def conv(self, nx_graph):
        # y ~ classification of the graph
        if nx_graph.graph["value"] is not None:
             # TODO value should be int, but isnt allways
            y_val = int(round(nx_graph.graph["value"]))
            nx_graph.graph["y"] = torch.tensor([y_val], dtype=torch.int)
            # print("y:",nx_graph.graph["value"], y_val)
        else:
            #TODO what do we do with None values?
            y_val = 3
            nx_graph.graph["y"] = torch.tensor([y_val], dtype=torch.int)


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
                nx_graph.nodes[node]["type"] = "root"

            type_idx = self.sensor_types.index(nx_graph.nodes[node]["type"])

            # One-hot encode type
            type_onehot = torch.nn.functional.one_hot(
                torch.tensor(type_idx),
                num_classes=len(self.sensor_types)
            ).float()

            # Concatenate [value] + one-hot
            node_feat = torch.cat([torch.tensor([node_value], dtype=torch.float32), type_onehot], dim=0)


            nx_graph.nodes[node]["x"] = node_feat
            # print(nx_graph.nodes[node])

        data = from_networkx(nx_graph)
        del data.graph_value # TODO check what graph_value even is
        return data