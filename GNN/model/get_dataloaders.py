from torch_geometric.loader import DataLoader

from GraphCreation.pipeline.nx_to_torch import Nx2TBin

class MyLoader:
    """
        Class for organization of datasets
    """
    def __init__(self, train_buffer, test_buffer, valid_buffer):
        self.conv = Nx2TBin()
        self.train_buffer = train_buffer
        self.test_buffer = test_buffer
        self.valid_buffer = valid_buffer

        self.num_classes = self.conv.num_classes
        self.num_node_features = self.conv.num_node_features

        self.test_label_distribution = [0 for i in range(self.num_classes)]
        self.train_label_distribution = [0 for i in range(self.num_classes)]
        self.valid_label_distribution = [0 for i in range(self.num_classes)]

        self.test_dataset = []
        self.train_dataset = []
        self.valid_dataset = []

        self.prev_y = 0

    def _init_training_data(self, stop_event=None):
        """
            Retrieves data from the buffer.
            Converts it to a pytorch graph using a converter from nx_to_torch
        """
        while True:
            if stop_event and stop_event.is_set():
                print("MyLoader detected stop_event set in _init_training_data, breaking loop.")
                return
            val = self.train_buffer.get()
            if val is None:
                print("val is none in init train data")
                break
            if len(self.train_dataset) % 1000 == 0:
                print(len(self.train_dataset), " in init train data")

            val = self.conv.conv(val)
            # print(val.y.item())
            self.train_label_distribution[val.y.item()] += 1

            self.train_dataset.append(val)
            
    def _init_test_data(self, stop_event=None):
        """
            Retrieves data from the buffer.
            Converts it to a pytorch graph using a converter from nx_to_torch
        """
        while True:
            if stop_event and stop_event.is_set():
                print("MyLoader detected stop_event set in _init_test_data, breaking loop.")
                return
            
            val = self.test_buffer.get()

            if val is None:
                print("val is None")
                break

            if len(self.test_dataset) % 1000 == 0:
                print(len(self.test_dataset), " in init test data")

            val = self.conv.conv(val)

            self.test_label_distribution[val.y.item()] += 1
            self.test_dataset.append(val)

    def _init_valid_data(self, stop_event=None):
        """
            Retrieves data from the buffer.
            Converts it to a pytorch graph using a converter from nx_to_torch
        """
        while True:
            if stop_event and stop_event.is_set():
                print("MyLoader detected stop_event set in _init_valid_data, breaking loop.")
                return
            
            val = self.valid_buffer.get()

            if val is None:
                print("val is None")
                break

            if len(self.valid_dataset) % 1000 == 0:
                print(len(self.valid_dataset), " in init valid data")

            val = self.conv.conv(val)

            self.valid_label_distribution[val.y.item()] += 1

            self.valid_dataset.append(val)

    def out_diversity(self):
        print("train dataset label distribution: ", self.train_label_distribution, "in % 0:", (self.train_label_distribution[0]/len(self.train_dataset))*100,", 1:", self.train_label_distribution[1]/len(self.train_dataset)*100)
        print("test dataset label distribution: ", self.test_label_distribution, "in % 0:", (self.test_label_distribution[0]/len(self.test_dataset))*100,", 1:", (self.test_label_distribution[1]/len(self.test_dataset))*100)
        print("valid dataset label distribution: ", self.valid_label_distribution, "in % 0:", (self.valid_label_distribution[0]/len(self.valid_dataset))*100,", 1:", (self.valid_label_distribution[1]/len(self.valid_dataset))*100)

    def get_training_distribution(self):
        return self.train_label_distribution

    def _init_data(self, stop_event=None):
        self._init_training_data(stop_event=stop_event)
        self._init_test_data(stop_event=stop_event)
        self._init_valid_data(stop_event=stop_event)
        self.out_diversity()

    def get_train_loader(self, stop_event=None):
        self.train_loader = DataLoader(self.train_dataset, batch_size=64, shuffle=True)
        return self.train_loader
    
    def get_test_loader(self, stop_event=None):
        self.test_loader = DataLoader(self.test_dataset, batch_size=64, shuffle=False)
        return self.test_loader
    
    def get_valid_loader(self, stop_event=None):
        self.valid_loader = DataLoader(self.valid_dataset, batch_size=64, shuffle=False)
        return self.valid_loader