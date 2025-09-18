import torch
from torch_geometric.datasets import TUDataset
from torch.nn import Linear
import torch.nn.functional as F
from torch_geometric.nn import GCNConv
from torch_geometric.nn import global_mean_pool
from torcheval.metrics.functional import multiclass_auroc, binary_auroc
from torch_geometric.loader import DataLoader

from datetime import datetime
import os

from model.my_gcn import GCN
from model.get_dataloaders import MyLoader

class MyModel:
    def __init__(self, train_buffer, test_buffer, valid_builder_output_queue, adjust_weights=False, dropout=0):
        self.adjust_weights = adjust_weights

        self.dataset = MyLoader(train_buffer, test_buffer, valid_builder_output_queue)

        self.model = GCN(256, self.dataset.num_node_features, self.dataset.num_classes, dropout=dropout)

        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=0.0001)

        self.criterion = torch.nn.CrossEntropyLoss()

    # Training function
    def _train(self ,train_loader, stop_event=None):
        self.model.train()
        for data in train_loader:
            if stop_event and stop_event.is_set():
                print("MyModel detected stop_event set in _train, breaking loop.")
                return
            out = self.model(data.x, data.edge_index, data.batch)
            loss = self.criterion(out, data.y)
            loss.backward()
            self.optimizer.step()
            self.optimizer.zero_grad()

    # Testing function
    # Testing function
    def test(self, loader, stop_event=None):
        self.model.eval()
        all_probs = []
        all_labels = []

        correct = 0
        for data in loader:
            if stop_event and stop_event.is_set():
                print("MyModel detected stop_event set in test, breaking loop.")
                return {"auc":-1, "acc": -1}
            out = self.model(data.x, data.edge_index, data.batch)
            pred = out.argmax(dim=1)
            correct += int((pred == data.y).sum())
            probs = F.softmax(out, dim=1)
            all_probs.append(probs)
            all_labels.append(data.y.detach().cpu())

        all_probs = torch.cat(all_probs, dim=0)
        all_labels = torch.cat(all_labels, dim=0)
        auc = multiclass_auroc(all_probs, all_labels, num_classes=self.dataset.num_classes)
        # print("auc:", auc)
        return {"auc":auc, "acc": correct/len(loader.dataset)}
    
    def _set_class_weights(self):
        print("setting weights")
        counts = torch.tensor(self.dataset.get_training_distribution(), dtype=torch.float)
        total = counts.sum()

        class_weights = total / (len(counts) * counts) # less frequent classes have a stronger weight

        self.criterion = torch.nn.CrossEntropyLoss(weight=class_weights)
        print("set weights")
        print(class_weights)

    def train(self, stop_event=None, max_no_improvement=25):
        self.dataset._init_data(stop_event=stop_event)
        f_nm = os.path.join("models","model_" + datetime.now().strftime("%Y%m%d_%H%M%S") + ".pth")
        best_auc = 0
        best_epoch = 0

        if self.adjust_weights:
            self._set_class_weights()

        # Training loop
        train_loader = self.dataset.get_train_loader(stop_event=stop_event)
        test_loader = self.dataset.get_test_loader(stop_event=stop_event)
        valid_loader = self.dataset.get_valid_loader(stop_event=stop_event)

        self.dataset.out_diversity()

        for epoch in range(1, 171):
            if stop_event and stop_event.is_set():
                print("MyModel detected stop_event set in train, breaking loop.")
                self.dataset.out_diversity()
                return
            
            self._train(train_loader, stop_event=stop_event)
            # train_auc = self.test(train_loader, stop_event=stop_event)
            valid_res = self.test(valid_loader, stop_event=stop_event)
            print(f'Epoch: {epoch:03d}')
            # print(f'Train acc: {train_auc["acc"]:.4f}, Train AUC: {train_auc["auc"]:.4f}')
            print(f'Valid acc: {valid_res["acc"]:.4f}, Valid AUC: {valid_res["auc"]:.4f}')
            print()
            if valid_res["auc"] > best_auc:
                best_auc = valid_res["auc"]
                torch.save(self.model.state_dict(), f_nm)
                best_epoch = epoch
            if epoch - best_epoch > max_no_improvement:
                print("no improvment, breaking")
                break

        valid_res = self.test(valid_loader, stop_event=stop_event)
        train_auc = self.test(train_loader, stop_event=stop_event)
        test_auc = self.test(test_loader, stop_event=stop_event)

        print(f'Epoch: 170')
        print(f'Train acc: {train_auc["acc"]:.4f}, Train AUC: {train_auc["auc"]:.4f}')
        print(f'Valid acc: {valid_res["acc"]:.4f}, Valid AUC: {valid_res["auc"]:.4f}')
        print(f'Test acc: {test_auc["acc"]:.4f}, Test AUC: {test_auc["auc"]:.4f}')

        self.model.load_state_dict(torch.load(f_nm))

        valid_res = self.test(valid_loader, stop_event=stop_event)
        train_auc = self.test(train_loader, stop_event=stop_event)
        test_auc = self.test(test_loader, stop_event=stop_event)

        print(f'Epoch: {best_epoch:03d}')
        print(f'Train acc: {train_auc["acc"]:.4f}, Train AUC: {train_auc["auc"]:.4f}')
        print(f'Valid acc: {valid_res["acc"]:.4f}, Valid AUC: {valid_res["auc"]:.4f}')
        print(f'Test acc: {test_auc["acc"]:.4f}, Test AUC: {test_auc["auc"]:.4f}')

        #print(f'Epoch: {epoch:03d}, Train AUC: {train_auc["auc"]:.4f}, Test AUC: {test_auc["auc"]:.4f}')
        #print(f'Train acc: {train_auc["acc"]:.4f}, Test acc: {test_auc["acc"]:.4f}')
        #print()        
        self.dataset.out_diversity()