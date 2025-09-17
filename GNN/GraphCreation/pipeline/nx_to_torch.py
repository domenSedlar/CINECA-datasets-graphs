import networkx as nx
import torch
from torch_geometric.utils import from_networkx

"""
    This file contains Classes that convert networkx graphs to a format that pytorch can use.
    Nx2T is short for networkx to torch
"""

class Nx2TMulti: # this one also keeps track of node type
    """
        Converts networkx graphs to a format for torch.
        And lables them.
        This class uses multi class labeling.

    """
    sensor_types = ["temp", "power", "fan", "input", "output", "other", "root"]

    num_node_features = 1 + len(sensor_types) # (type, value)
    num_classes = 4 # 0 ~ ok, 1 ~ down, 2 ~ unreachable, 3 ~ unknown

    def __init__(self):
        pass

    def conv(self, nx_graph):
        # y ~ classification of the graph
        if nx_graph.graph["value"] is not None:
            y_val = int(round(nx_graph.graph["value"]))
            nx_graph.graph["y"] = torch.tensor([y_val], dtype=torch.long)
            # print("y:",nx_graph.graph["value"], y_val)
        else:
            y_val = 3
            nx_graph.graph["y"] = torch.tensor([y_val], dtype=torch.long)


        # x ~ features of nodes
        for node in nx_graph.nodes(data=False):
            node_id = node
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
    
class Nx2TBin:
    """
        Converts networkx graphs to a format for torch.
        And lables them.
        This class uses binary labeling.

    """    
    sensor_types = ["temp", "power", "fan", "input", "output", "other", "root"]

    num_node_features = 1 + len(sensor_types)  + 2 # (type, value, pos_x, pos_y)
    num_classes = 2 # 0 ~ ok, 1 ~ down

    def __init__(self):
        pass

    def conv(self, nx_graph):
        # y ~ classification of the graph
        nodes = list(nx_graph.nodes())
        # nodes = [nodes[i] for i in range(len(nodes)//2)]
        # print(len(nodes))

        nx_graph = nx.subgraph(nx_graph, [nodes[i] for i in range(int(round(len(nodes))))])
        if nx_graph.graph.get("value") is not None:
            y_val = 0 if int(nx_graph.graph["value"]) == 0 else 1
        else:
            y_val = 0
        y_tensor = torch.tensor([y_val], dtype=torch.long)

        nodes = list(nx_graph.nodes())

        # Values (default 0 if missing/None)
        values = torch.tensor(
            [nx_graph.nodes[n].get("value", 0) or 0 for n in nodes],
            dtype=torch.float32
        ).unsqueeze(1)  # shape: [num_nodes, 1]
        pos_x = torch.tensor(
            [nx_graph.nodes[n].get("pos_x", 0) or 0 for n in nodes],
            dtype=torch.float32
        ).unsqueeze(1)  # shape: [num_nodes, 1]
        pos_y = torch.tensor(
            [nx_graph.nodes[n].get("pos_y", 0) or 0 for n in nodes],
            dtype=torch.float32
        ).unsqueeze(1)  # shape: [num_nodes, 1]

        # Types → indices
        type_indices = [
            self.sensor_types.index(nx_graph.nodes[n].get("type", "root"))
            for n in nodes
        ]
        type_indices = torch.tensor(type_indices, dtype=torch.long)

        # One-hot encode all at once
        type_onehots = torch.nn.functional.one_hot(
            type_indices, num_classes=len(self.sensor_types)
        ).float()  # shape: [num_nodes, num_types]

        # Concatenate [value] + one-hot
        x = torch.cat([values, pos_x, pos_y, type_onehots*values], dim=1)  # shape: [num_nodes, num_node_features] # TODO you're not supposed to multiply oneshot embadings like this. Instead implement this in the forward function of the model
        

        # --- Build PyG Data object ---
        data = from_networkx(nx_graph)
        if hasattr(data, "graph_value"):
            del data.graph_value  # drop if present
        data.x = x
        data.y = y_tensor
        return data