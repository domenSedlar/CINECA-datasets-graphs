import torch
from torch.nn import Linear
import torch.nn.functional as F
from torch_geometric.nn import GNN, GraphConv
from torch_geometric.nn import global_mean_pool
from torch_geometric.loader import DataLoader


class GC(torch.nn.Module):
    def __init__(self, hidden_channels):
        super(GNN, self).__init__()
        torch.manual_seed(12345)

        num_node_features = 2 # (sensor name, value)
        num_classes = 4 # 0 ~ ok, 1 ~ down, 2 ~ unreachable, 4 ~ unknown

        self.conv1 = GraphConv(num_node_features, hidden_channels)
        self.conv2 = GraphConv(hidden_channels, hidden_channels)
        self.conv3 = GraphConv(hidden_channels, hidden_channels)
        self.lin = Linear(hidden_channels, num_classes)

    def forward(self, x, edge_index, batch):
        # 1. Obtain node embeddings 
        x = self.conv1(x, edge_index)
        x = x.relu()
        x = self.conv2(x, edge_index)
        x = x.relu()
        x = self.conv3(x, edge_index)

        # 2. Readout layer
        x = global_mean_pool(x, batch)  # [batch_size, hidden_channels]

        # 3. Apply a final classifier
        x = F.dropout(x, p=0.5, training=self.training)
        x = self.lin(x)
        
        return x
    

class MyModel:
    def __init__(self, buffer, hidden_channels=64, train_on=50):
        self.model = GC(hidden_channels)
        self.t = train_on
        self.train_dataset = []
        self.test_dataset = []
        self.recieving = True

        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=0.01)
        self.criterion = torch.nn.CrossEntropyLoss()

    def _train_on(self, data):
        out = self.model(data.x, data.edge_index, data.batch)
        loss = self.criterion(out, data.y)
        loss.backward()
        self.optimizer.step()
        self.optimizer.zero_grad()

    # Training function
    def _train(self, train_loader=None):
        self.model.train()

        while self.recieving and self.train_dataset < self.t:
            val = self.buffer.get()
            if val is None:
                self.recieving = False
                break

            self.train_dataset.append(self.buffer.get())
            self._train_on(val)

        if train_loader is None:
            return

        for data in train_loader:
            self._train_on(data)

    def _test_ex(self, data):
        out = self.model(data.x, data.edge_index, data.batch)
        pred = out.argmax(dim=1)
        return int((pred == data.y).sum())

    # Testing function
    def test(self, loader=None):
        self.model.eval()
        correct = 0
        if loader is None:
            while self.recieving:
                val = self.buffer.get()
                if val is None:
                    self.recieving = False
                    break
                correct += self._test_ex(val)
                self.test_dataset.append(val)
        else:    
            for data in loader:
                correct += self._test_ex(data)
        
        return correct / len(loader.dataset)

    def train(self):
        # Training loop
        self._train()
        train_loader = DataLoader(self.train_dataset, batch_size=64, shuffle=True)
        train_acc = self.test(train_loader)
        test_acc = self.test()
        test_loader = DataLoader(self.test_dataset, batch_size=64, shuffle=False)

        for epoch in range(1, 171):
            self._train()
            train_acc = self.test(train_loader)
            test_acc = self.test(test_loader)
            print(f'Epoch: {epoch:03d}, Train Acc: {train_acc:.4f}, Test Acc: {test_acc:.4f}')