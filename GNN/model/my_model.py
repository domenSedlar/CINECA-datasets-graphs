import torch
from torch_geometric.datasets import TUDataset
from torch.nn import Linear
import torch.nn.functional as F
from torch_geometric.nn import GCNConv
from torch_geometric.nn import global_mean_pool
from torcheval.metrics.functional import multiclass_auroc, binary_auroc
from torch_geometric.loader import DataLoader

from sklearn.utils.class_weight import compute_class_weight
import numpy as np

import copy
from datetime import datetime
import os

from model.my_gcn import GCN
from model.get_dataloaders import MyLoader

class MyModel:
    def __init__(self, dataset, adjust_weights=False, dropout=0, llr=0.0001, aggr_method="add", pool_method = "mean", num_of_layers=0, hidden_channels=128):
        """
            Optional parameters:
                adjust_weights: when true graph classes will be weighted inversly based on their frequency in the test dataset
                dropout: value of the models dropout, when 0 (default) no dropout is applied
                llr: learning rate
                aggr_method: add | max | mean
                pool_method: add | max | mean
        """
        self.adjust_weights = adjust_weights

        self.dataset = dataset

        self.model = GCN(hidden_channels, self.dataset.num_node_features, self.dataset.num_classes, dropout=dropout, aggr_method=aggr_method, pool_method=pool_method, num_of_layers=num_of_layers)

        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=llr)

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

    # 
    def test(self, loader, stop_event=None):
        """
        Testing function
        returnes a dictionary with keys "auc", "acc"
        """
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
        #counts = torch.tensor(self.dataset.get_training_distribution(), dtype=torch.float)
        #total = counts.sum()

        #class_weights = total / (len(counts) * counts) # less frequent classes have a stronger weight
        y= self.dataset.get_training_distribution()
        w =  torch.tensor(compute_class_weight(class_weight="balanced", classes=np.unique(y), y=y), dtype=torch.float)

        self.criterion = torch.nn.CrossEntropyLoss(weight=w)
        #print("set weights")
        #print(class_weights)

    def train(self, stop_event=None, max_no_improvement=50, save_best=True, train_for=171):
        """
            The main method.
            Retrieves the training, validation and testing dataloaders, from get_dataloaders.
            Trains for 171 epochs or until no improvment is reached for x epochs.
            At the end the method outputs the results from the model which preformed best on the validation set during training. 
        """
        print("starting training")
        f_nm = os.path.join("models","model_" + datetime.now().strftime("%Y%m%d_%H%M%S") + ".pth")
        best_auc = 0
        best_epoch = 0
        best_w = None

        # Training loop
        train_loader = self.dataset.get_train_loader(stop_event=stop_event)
        test_loader = self.dataset.get_test_loader(stop_event=stop_event)
        valid_loader = self.dataset.get_valid_loader(stop_event=stop_event)

        e=0

        if self.adjust_weights:
            self._set_class_weights()

        for epoch in range(1, train_for):
            e=epoch
            if stop_event and stop_event.is_set():
                print("MyModel detected stop_event set in train, breaking loop.")
                self.dataset.out_diversity()
                break
            
            self._train(train_loader, stop_event=stop_event)
            # train_auc = self.test(train_loader, stop_event=stop_event)
            valid_res = self.test(valid_loader, stop_event=stop_event)
            #print(f'Epoch: {epoch:03d}')
            # print(f'Train acc: {train_auc["acc"]:.4f}, Train AUC: {train_auc["auc"]:.4f}')
            print(f'Valid acc: {valid_res["acc"]:.4f}, Valid AUC: {valid_res["auc"]:.4f}')
            #print()
            if epoch < 10:
                continue
            if valid_res["auc"] > best_auc:
                best_auc = valid_res["auc"]
                #torch.save(self.model.state_dict(), f_nm)
                best_w = copy.deepcopy(self.model.state_dict())
                best_epoch = epoch
            if epoch - best_epoch > max_no_improvement:
                print("no improvment, breaking")
                break

        valid_res = self.test(valid_loader)
        train_auc = self.test(train_loader)
        test_auc = self.test(test_loader)

        print(f'Epoch: {e}')
        print(f'Train acc: {train_auc["acc"]:.4f}, Train AUC: {train_auc["auc"]:.4f}')
        print(f'Valid acc: {valid_res["acc"]:.4f}, Valid AUC: {valid_res["auc"]:.4f}')
        print(f'Test acc: {test_auc["acc"]:.4f}, Test AUC: {test_auc["auc"]:.4f}')

        self.model.load_state_dict(best_w)

        valid_res = self.test(valid_loader)
        train_auc = self.test(train_loader)
        test_auc = self.test(test_loader)

        print(f'Epoch: {best_epoch:03d}')
        print(f'Train acc: {train_auc["acc"]:.4f}, Train AUC: {train_auc["auc"]:.4f}')
        print(f'Valid acc: {valid_res["acc"]:.4f}, Valid AUC: {valid_res["auc"]:.4f}')
        print(f'Test acc: {test_auc["acc"]:.4f}, Test AUC: {test_auc["auc"]:.4f}')

        #print(f'Epoch: {epoch:03d}, Train AUC: {train_auc["auc"]:.4f}, Test AUC: {test_auc["auc"]:.4f}')
        #print(f'Train acc: {train_auc["acc"]:.4f}, Test acc: {test_auc["acc"]:.4f}')
        #print()        
        self.dataset.out_diversity()

        if save_best:
            torch.save(self.model.state_dict(), f_nm)

        return test_auc["auc"].item()