import torch
from torch_geometric.datasets import TUDataset
from torch.nn import Linear
import torch.nn.functional as F
from torch_geometric.nn import GCNConv, GraphConv
from torch_geometric.nn import global_mean_pool
from torcheval.metrics.functional import multiclass_auroc, binary_auroc
from torch_geometric.loader import DataLoader

class GCN(torch.nn.Module):
    def __init__(self, hidden_channels, num_node_features,num_classes,dropout=0):
        self.dropout = dropout
        super(GCN, self).__init__()
        torch.manual_seed(12345)
        self.conv1 = GraphConv(num_node_features, hidden_channels, aggr = 'add') # aggr = 'add' | 'mean' | 'max'
        #self.conv2 = GraphConv(hidden_channels, hidden_channels)
        #self.conv3 = GraphConv(hidden_channels, hidden_channels)
        
        self.lin = Linear(hidden_channels, num_classes)

    def forward(self, x, edge_index, batch):
        # 1. Obtain node embeddings 
        x = self.conv1(x, edge_index)
        # x = x.relu()
        #x = self.conv2(x, edge_index)
        #x = x.relu()
        #x = self.conv3(x, edge_index)

        # 2. Readout layer
        x = global_mean_pool(x, batch)  # [batch_size, hidden_channels]

        # 3. Apply a final classifier
        if self.dropout != 0:
            x = F.dropout(x, p=0.1, training=self.training)
        x = self.lin(x)
        
        return x