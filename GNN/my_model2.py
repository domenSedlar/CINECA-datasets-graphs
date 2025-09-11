import torch
from torch_geometric.datasets import TUDataset
from torch.nn import Linear
import torch.nn.functional as F
from torch_geometric.nn import GCNConv
from torch_geometric.nn import global_mean_pool
from torcheval.metrics.functional import multiclass_auroc, binary_auroc
from torch_geometric.loader import DataLoader

from my_gcn import GCN
from get_dataloaders import MyLoader

class MyModel:
    def __init__(self, buffer):
        self.dataset = MyLoader(buffer)

        self.model = GCN(128, self.dataset.num_node_features, self.dataset.num_classes)

        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=0.1)
        self.criterion = torch.nn.CrossEntropyLoss()

    # Training function
    def _train(self ,train_loader):
        self.model.train()
        for data in train_loader:
            out = self.model(data.x, data.edge_index, data.batch)
            loss = self.criterion(out, data.y)
            loss.backward()
            self.optimizer.step()
            self.optimizer.zero_grad()

    # Testing function
    # Testing function
    def test(self, loader):
        self.model.eval()
        all_probs = []
        all_labels = []

        correct = 0
        for data in loader:
            out = self.model(data.x, data.edge_index, data.batch)
            pred = out.argmax(dim=1)
            correct += int((pred == data.y).sum())
            probs = F.softmax(out, dim=1) # TODO i'm not sure if normalization is required
            all_probs.append(probs)
            all_labels.append(data.y.detach().cpu())

        all_probs = torch.cat(all_probs, dim=0)
        all_labels = torch.cat(all_labels, dim=0)
        auc = multiclass_auroc(all_probs, all_labels, num_classes=self.dataset.num_classes)
        print("auc:", auc)
        return correct / len(loader.dataset)

    def train(self):
                
        # Training loop
        train_loader = self.dataset.get_train_loader()
        test_loader = self.dataset.get_test_loader()

        for epoch in range(1, 171):
            self._train(train_loader)
            train_acc = self.test(train_loader)
            test_acc = self.test(test_loader)
            print(f'Epoch: {epoch:03d}, Train Acc: {train_acc:.4f}, Test Acc: {test_acc:.4f}')