import torch
from torch_geometric.data import Batch
from torch.nn import Linear
import torch.nn.functional as F
from torch_geometric.nn import GraphConv
from torch_geometric.nn import global_mean_pool
from torch_geometric.loader import DataLoader
from torcheval.metrics.functional import multiclass_auroc, binary_auroc
from GraphCreation.pipeline.nx_to_torch import Nx2TBin


class GNN(torch.nn.Module):
    def __init__(self, hidden_channels):
        super(GNN, self).__init__()
        torch.manual_seed(12345)

        self.converter = Nx2TBin()

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
    def __init__(self, buffer, hidden_channels=64, train_on=50, repeat=171, counter_weight=1, oversampling=1):
        """
            repeat: how many times to train on the dataset
            counter_weight: how many times should 0 class be weighted lower. Where 1 means it should be treated as the other classes
            oversampling: (int) how many times should we readd non zero values to the training dataset(1 means we dont oversample)
        """
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
        self.oversampling = oversampling

        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=0.01)
        
        w = 1000 / self.conv.num_classes

        temp_counts = []
        for _ in range(self.conv.num_classes):
             temp_counts.append(w)

        temp_counts[0] = w * counter_weight

        counts = torch.tensor(temp_counts, dtype=torch.float)
        total = counts.sum()

        class_weights = total / (len(counts) * counts)
        
        self.criterion = torch.nn.CrossEntropyLoss(weight=class_weights)

    def _train_on(self, data): # Trains on 1 batch
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
    # Either recieves data from the queue, or creates batches from the saved data.
    # then uses this to call the training method
    def _train(self, train_loader, stop_event=None):
        self.model.train()

        for data in train_loader:
            if stop_event and stop_event.is_set():
                print("MyModel detected stop_event set in _train, breaking loop.")
                break
            self._train_on(data)

    # Tests the model on the given batch
    def _test_ex(self, data):
        out = self.model(data.x, data.edge_index, data.batch)
        pred = out.argmax(dim=1)
        probs = F.softmax(out, dim=1) # TODO i'm not sure if normalization is required
        # print("_test_ex", pred, data.y)
        return {"correct": int((pred == data.y).sum()), "probs": probs}

    # Testing function
    # Either recieves data from the queue, or creates batches from the saved data.
    # then uses this to call the test method
    def test(self, test_loader, stop_event=None):
        self.model.eval()
        correct = 0
        c = 0

        all_probs = []
        all_labels = []

        c = len(test_loader.dataset)
        for data in test_loader:
            if stop_event and stop_event.is_set():
                print("MyModel detected stop_event set in _test, breaking loop.")
                break
            res = self._test_ex(data)

            correct += res["correct"]
            all_probs.append(res["probs"])
            all_labels.append(data.y.detach().cpu())
#                print(correct, c)

        if c == 0:
            return {"acc": -1, "auc": -1}

        acc = correct / c

        # Concatenate all predictions and labels
        all_probs = torch.cat(all_probs, dim=0)
        all_labels = torch.cat(all_labels, dim=0)

        # Handle binary vs multiclass
        try:
            if all_probs.shape[1] == 2:  # binary classification
                auc = binary_auroc(all_probs[:, 1], all_labels)
            else:  # multiclass
                auc = multiclass_auroc(all_probs, all_labels, num_classes=self.conv.num_classes)
        except ValueError:
            auc = -1  # happens if only one class present in labels

        return {"acc": acc, "auc": auc}

    def _init_training_data(self, stop_event):

        while self.recieving and len(self.train_dataset) < self.t:
            if stop_event and stop_event.is_set():
                print("MyModel detected stop_event set in _init_training_data, breaking loop.")
                return
            val = self.buffer.get()
            if val is None:
                print("val is none in _train")
                self.recieving = False

            if(val.graph["value"] == 0):
                self.num_zeros_train += 1
            val = self.conv.conv(val)
            if(val.y.item() != 0):
                for i in range(self.oversampling):
                    self.train_dataset.append(val)
            else:
                self.train_dataset.append(val)

    def _init_test_data(self, stop_event):
            while self.recieving: # TODO batch multiple graphs for eval
                if stop_event and stop_event.is_set():
                    print("MyModel detected stop_event set in _test, breaking loop.")
                    return
                val = self.buffer.get()
                if val is None:
                    self.recieving = False
                    print("val is None")
                    break
                if(val.graph["value"] == 0):
                    self.num_zeros_test += 1
                val = self.conv.conv(val)
                self.test_dataset.append(val)

    def _init_data(self, stop_event):
        self._init_training_data(stop_event=stop_event)
        self._init_test_data(stop_event=stop_event)

    # The main method to run the model
    # It runs both training and tests
    # and outputs the resault
    def train(self, stop_event=None):
        self._init_data(stop_event=stop_event)
        train_loader = DataLoader(self.train_dataset, batch_size=64, shuffle=True) # TODO what should batch size be
        test_loader = DataLoader(self.test_dataset, batch_size=64, shuffle=False)

        for epoch in range(1, self.repeat): # TODO how many times should this run
            if stop_event and stop_event.is_set():
                print("MyModel detected stop_event set, breaking loop.")
                break
            self._train(train_loader=train_loader, stop_event=stop_event)

            train_res = self.test(test_loader=train_loader, stop_event=stop_event)
            test_res = self.test(test_loader=test_loader, stop_event=stop_event)
            print(f'Epoch: {epoch:03d}, Train Acc: {train_res["acc"]:.4f}, Test Acc: {test_res["acc"]:.4f}')
            print(f'Epoch: {epoch:03d}, Train AUC: {train_res["auc"]}, Test AUC: {test_res["auc"]}')

        train_res = self.test(test_loader=train_loader, stop_event=stop_event)
        test_res = self.test(test_loader=test_loader, stop_event=stop_event)
        print(f'Epoch: {epoch:03d}, Train Acc: {train_res["acc"]:.4f}, Test Acc: {test_res["acc"]:.4f}')
        print(f'Epoch: {epoch:03d}, Train AUC: {train_res["auc"]}, Test AUC: {test_res["auc"]}')

        print("number of graphs with value 0 in training data:", self.num_zeros_train, "ratio:",self.num_zeros_train / len(self.train_dataset))
        print("number of graphs with value 0 in test data:", self.num_zeros_test, "ratio:",  self.num_zeros_test / len(self.test_dataset))