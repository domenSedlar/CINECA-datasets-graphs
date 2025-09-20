import torch
from torch_geometric.datasets import TUDataset
from torch.nn import Linear
import torch.nn.functional as F
from torch_geometric.nn import GCNConv, GraphConv
from torch_geometric.nn import global_mean_pool, global_max_pool
from torcheval.metrics.functional import multiclass_auroc, binary_auroc
from torch_geometric.loader import DataLoader

class GCN(torch.nn.Module):
    def __init__(self, hidden_channels, num_node_features,num_classes,dropout=0, aggr_method="add", pool_method="mean", num_of_layers=0):
        """
            if you dont wish to use dropout set it to 0 (default)
        """

        self.dropout = dropout
        super(GCN, self).__init__()
        torch.manual_seed(12345)
        self.conv1 = GraphConv(num_node_features, hidden_channels, aggr = aggr_method) # aggr = 'add' | 'mean' | 'max'
        self.convs = [GraphConv(hidden_channels, hidden_channels, aggr=aggr_method) for _ in range(num_of_layers)]
        #self.conv2 = GraphConv(hidden_channels, hidden_channels)
        #self.conv3 = GraphConv(hidden_channels, hidden_channels)
        
        self.lin = Linear(hidden_channels, num_classes)
        self.pool_method = pool_method

    def forward(self, x, edge_index, batch):
        # 1. Obtain node embeddings 
        x = self.conv1(x, edge_index)
        for i in self.convs:
            x = x.relu()
            x = i(x,edge_index)
        # x = x.relu()
        #x = self.conv2(x, edge_index)
        #x = x.relu()
        #x = self.conv3(x, edge_index)

        # 2. Readout layer
        if self.pool_method == "max":
            x = global_max_pool(x, batch)  # [batch_size, hidden_channels]
        else:
            x = global_mean_pool(x, batch)

        # 3. Apply a final classifier
        if self.dropout != 0:
            x = F.dropout(x, p=self.dropout, training=self.training)
        x = self.lin(x)
        
        return x