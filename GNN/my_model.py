import torch
from torch_geometric.data import Batch
from torch.nn import Linear
import torch.nn.functional as F
from torch_geometric.nn import GraphConv, GCNConv
from torch_geometric.nn import global_mean_pool
from torch_geometric.loader import DataLoader
from torcheval.metrics.functional import multiclass_auroc, binary_auroc
from GraphCreation.pipeline.nx_to_torch import Nx2TBin


class GNN(torch.nn.Module):
    def __init__(self, hidden_channels):
        super(GNN, self).__init__()
        torch.manual_seed(12345)

        self.converter = Nx2TBin()

        self.conv1 = GCNConv(self.converter.num_node_features, hidden_channels)
        self.conv2 = GCNConv(hidden_channels, hidden_channels)
        self.conv3 = GCNConv(hidden_channels, hidden_channels)
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
    def __init__(self, buffer, hidden_channels=64, train_on=50, repeat=171, oversampling=1, class_weighting=True):
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
        self.repeat = repeat
        self.label_diversety = [0 for _ in range(self.conv.num_classes)]
        self.num_zeros_train = 0
        self.num_zeros_test = 0
        self.oversampling = oversampling
        self.class_weighting = class_weighting

        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=0.01)
        
        self.criterion = torch.nn.CrossEntropyLoss()

    def _train_on(self, batch):
        """Adjust the weights based on the results of this batch"""
        batch.x = torch.nn.functional.normalize(batch.x) # normalizes node features TODO learn what this means
        out = self.model(batch.x, batch.edge_index, batch.batch)
        loss = self.criterion(out, batch.y)
        loss.backward()
        for name, param in self.model.named_parameters():
            if param.grad is None:
                print(f"No gradient for {name}")
            else:
                print(f"Gradient for {name} norm: {param.grad.norm().item():.6f}")
        self.optimizer.step()
        self.optimizer.zero_grad()

    def _print_graph_data(graph):
        print(f'Number of nodes: {graph.num_nodes}')
        print(f'Number of edges: {graph.num_edges}')
        print(f'Average node degree: {graph.num_edges / graph.num_nodes:.2f}')
        print(f'Has isolated nodes: {graph.has_isolated_nodes()}')
        print(f'Has self-loops: {graph.has_self_loops()}')
        print(f'Is undirected: {graph.is_undirected()}')

    def _train(self, train_loader, stop_event=None):
        """gathers batches from train_loader and trains on them"""
        self.model.train()

        for data in train_loader:
            if stop_event and stop_event.is_set():
                print("MyModel detected stop_event set in _train, breaking loop.")
                break

            before = {name: param.clone() for name, param in self.model.named_parameters()}
            self._train_on(data)

            for name, param in self.model.named_parameters():
                change = (param - before[name]).abs().sum().item()
                print(f"{name} changed by {change:.6f}")

    def _test_ex(self, batch):
        """
        Tests the model on the given batch
        Returns how many correct predictions the model made, and what the probabilities were
        """
        out = self.model(batch.x, batch.edge_index, batch.batch)
        print("out in test ex: ",out)
        pred = out.argmax(dim=1)
        probs = F.softmax(out, dim=1) # TODO i'm not sure if normalization is required
        print("batch.y.type in test ex", batch.y.dtype)  # Should be torch.long        
        return {"correct": int((pred == batch.y).sum()), "probs": probs}

    # Testing function
    # Either recieves data from the queue, or creates batches from the saved data.
    # then uses this to call the test method
    def test(self, test_loader, stop_event=None, size_of_data=-1):
        """
        Gathers batches from and calls a test on them. 
        Returns a dict containing 
            - acc = num of correct guesses / num of examples
            - auc = AUC ROC score 
        """
        self.model.eval()
        correct = 0
        c = size_of_data

        all_probs = []
        all_labels = []

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
        
        while len(self.train_dataset) < self.t:
            if stop_event and stop_event.is_set():
                print("MyModel detected stop_event set in _init_training_data, breaking loop.")
                return
            val = self.buffer.get()
            if val is None:
                print("val is none in _train")
                break
            if len(self.train_dataset) % 1000 == 0:
                print(len(self.train_dataset), " in init train data")
            val = self.conv.conv(val)
            print("graph data:")
            print(val.y.item())
            self.label_diversety[val.y.item()] += 1
            if(val.y.item() != 0):
                for i in range(self.oversampling):
                    self.train_dataset.append(val)
            else:
                self.train_dataset.append(val)
            
            print(val.x)
            print("")
        
        print(self.label_diversety)
        self.num_zeros_train = self.label_diversety[0]

    def _init_test_data(self, stop_event):
            while True: # TODO batch multiple graphs for eval
                if stop_event and stop_event.is_set():
                    print("MyModel detected stop_event set in _init_test_data, breaking loop.")
                    return
                val = self.buffer.get()
                if val is None:
                    print("val is None")
                    break
                if(val.graph["value"] == 0):
                    self.num_zeros_test += 1
                if len(self.test_dataset) % 1000 == 0:
                    print(len(self.test_dataset), " in init test data")
                val = self.conv.conv(val)
                self.test_dataset.append(val)

    def _set_class_weights(self):
        print("setting weights")
        counts = torch.tensor(self.label_diversety, dtype=torch.float)
        total = counts.sum()

        class_weights = total / (len(counts) * counts) # less frequent classes have a stronger weight

        self.criterion = torch.nn.CrossEntropyLoss(weight=class_weights)
        print("set weights")
        print(class_weights)

    def _init_data(self, stop_event):
        self._init_training_data(stop_event=stop_event)
        self._init_test_data(stop_event=stop_event)

        if self.class_weighting:
            self._set_class_weights()

    def train(self, stop_event=None):
        """
        The main method to run the model
        It runs for multiple epochs, and outputs the results of each one
        """
        self._init_data(stop_event=stop_event)
        train_loader = DataLoader(self.train_dataset, batch_size=64, shuffle=True) # TODO what should batch size be
        test_loader = DataLoader(self.test_dataset, batch_size=64, shuffle=False)
        print("number of graphs with value 0 in training data:", self.num_zeros_train, "ratio:",self.num_zeros_train / len(self.train_dataset))
        print("number of graphs with value 0 in test data:", self.num_zeros_test, "ratio:",  self.num_zeros_test / len(self.test_dataset))
        for epoch in range(1, self.repeat): # TODO how many times should this run
            if stop_event and stop_event.is_set():
                print("MyModel detected stop_event set, breaking loop.")
                break
            self._train(train_loader=train_loader, stop_event=stop_event)

            train_res = self.test(test_loader=train_loader, stop_event=stop_event, size_of_data=len(self.train_dataset))
            test_res = self.test(test_loader=test_loader, stop_event=stop_event, size_of_data=len(self.test_dataset))
            print(f'Epoch: {epoch:03d}, Train Acc: {train_res["acc"]:.4f}, Test Acc: {test_res["acc"]:.4f}')
            print(f'Epoch: {epoch:03d}, Train AUC: {train_res["auc"]}, Test AUC: {test_res["auc"]}')

        train_res = self.test(test_loader=train_loader, stop_event=stop_event, size_of_data=len(self.train_dataset))
        test_res = self.test(test_loader=test_loader, stop_event=stop_event, size_of_data=len(self.test_dataset))
        print(f'Epoch: {epoch:03d}, Train Acc: {train_res["acc"]:.4f}, Test Acc: {test_res["acc"]:.4f}')
        print(f'Epoch: {epoch:03d}, Train AUC: {train_res["auc"]}, Test AUC: {test_res["auc"]}')

        print("number of graphs with value 0 in training data:", self.num_zeros_train, "ratio:",self.num_zeros_train / len(self.train_dataset))
        print("number of graphs with value 0 in test data:", self.num_zeros_test, "ratio:",  self.num_zeros_test / len(self.test_dataset))

        batch = next(iter(train_loader))
        out = self.model(batch.x, batch.edge_index, batch.batch)
        preds = out.argmax(dim=1)
        print("Predictions:", preds)
        print("Labels:     ", batch.y)