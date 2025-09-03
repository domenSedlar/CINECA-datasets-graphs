import torch
from torch.nn import Linear
import torch.nn.functional as F
from torch_geometric.nn import GraphConv
from torch_geometric.nn import global_mean_pool
from torch_geometric.loader import DataLoader
from GraphCreation.pipeline.nx_to_torch import Nx2T1Conv2


class GNN(torch.nn.Module):
    def __init__(self, hidden_channels):
        super(GNN, self).__init__()
        torch.manual_seed(12345)

        self.converter = Nx2T1Conv2()

        self.conv1 = GraphConv(self.converter.num_node_features, hidden_channels)
        self.conv2 = GraphConv(hidden_channels, hidden_channels)
        self.conv3 = GraphConv(hidden_channels, hidden_channels)
        self.lin = Linear(hidden_channels, self.converter.num_classes)

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
    def __init__(self, buffer, hidden_channels=64, train_on=50, repeat=171):
        self.model = GNN(hidden_channels)
        self.conv = self.model.converter
        self.buffer = buffer
        self.t = train_on
        self.train_dataset = []
        self.test_dataset = []
        self.recieving = True
        self.repeat = repeat
        self.num_zeros_train = 0
        self.num_zeros_test = 0 

        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=0.01)
        self.criterion = torch.nn.CrossEntropyLoss()

    def _train_on(self, data):
        out = self.model(data.x, data.edge_index, data.batch)
        loss = self.criterion(out, data.y)
        loss.backward()
        self.optimizer.step()
        self.optimizer.zero_grad()

    def _print_graph_data(graph):
        print(f'Number of nodes: {graph.num_nodes}')
        print(f'Number of edges: {graph.num_edges}')
        print(f'Average node degree: {graph.num_edges / graph.num_nodes:.2f}')
        print(f'Has isolated nodes: {graph.has_isolated_nodes()}')
        print(f'Has self-loops: {graph.has_self_loops()}')
        print(f'Is undirected: {graph.is_undirected()}')

    # Training function
    def _train(self, train_loader=None, stop_event=None):
        self.model.train()

        while self.recieving and len(self.train_dataset) < self.t:
            if stop_event and stop_event.is_set():
                print("MyModel detected stop_event set in _train, breaking loop.")
                return
            val = self.buffer.get()
            if val is None:
                print("val is none in _train")
                self.recieving = False
                break
            if(val.graph["value"] == 0):
                self.num_zeros_train += 1
            val = self.conv.conv(val)
            self.train_dataset.append(val)
            self._train_on(val)

        if train_loader is None:
            return

        for data in train_loader:
            if stop_event and stop_event.is_set():
                print("MyModel detected stop_event set in _train, breaking loop.")
                break
            self._train_on(data)

    def _test_ex(self, data):
        out = self.model(data.x, data.edge_index, data.batch)
        pred = out.argmax(dim=1)
        # print("_test_ex", pred, data.y)
        return int((pred == data.y).sum())

    # Testing function
    def test(self, test_loader=None, stop_event=None):
        self.model.eval()
        correct = 0
        c = 0
        if test_loader is None:
            print("loader is None")
            while self.recieving:
                if stop_event and stop_event.is_set():
                    print("MyModel detected stop_event set in _test, breaking loop.")
                    break
                val = self.buffer.get()
                if val is None:
                    self.recieving = False
                    print("val is None")
                    break
                if(val.graph["value"] == 0):
                    self.num_zeros_test += 1
                val = self.conv.conv(val)
                correct += self._test_ex(val)
                c += 1
                self.test_dataset.append(val)
        else:
            c = len(test_loader.dataset)
            for data in test_loader:
                if stop_event and stop_event.is_set():
                    print("MyModel detected stop_event set in _test, breaking loop.")
                    break
                correct += self._test_ex(data)
        print(correct, c)
        if c == 0:
            return -1
        return correct / c

    def train(self, stop_event=None):
        self._train(stop_event=stop_event) 
        train_loader = DataLoader(self.train_dataset, batch_size=64, shuffle=True) # TODO what should batch size be
        train_acc = self.test(test_loader=train_loader, stop_event=stop_event)
        test_acc = self.test(stop_event=stop_event)
        test_loader = DataLoader(self.test_dataset, batch_size=64, shuffle=False)

        for epoch in range(1, self.repeat): # TODO how many times should this run
            if stop_event and stop_event.is_set():
                print("MyModel detected stop_event set, breaking loop.")
                break
            self._train(train_loader=train_loader, stop_event=stop_event)
            train_acc = self.test(test_loader=train_loader, stop_event=stop_event)
            test_acc = self.test(test_loader=test_loader, stop_event=stop_event)
            print(f'Epoch: {epoch:03d}, Train Acc: {train_acc:.4f}, Test Acc: {test_acc:.4f}')

        print("number of graphs with value 0 in training data:", self.num_zeros_train, "ratio:",self.num_zeros_train / len(self.train_dataset))
        print("number of graphs with value 0 in training data:", self.num_zeros_test, "ratio:",  self.num_zeros_test / len(self.test_dataset))