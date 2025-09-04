## GNN
### Main.py
Starts both graph creation and and the model. Feeding the output from graph creation to the gnn.

### Graph creation
#### read and emit
Reads the data from the processed parquet files. And for each row it uses the following method to give it its value parameter.

```python
    def _next_val(self, ts): # TODO add parameter for how far we look
        """
            return value, from the time interval after ts
        """
        if isinstance(ts, str):
            ts = datetime.datetime.fromisoformat(ts)

        try:
            while self.curr_val["timestamp"].as_py() <= ts:
                self.curr_val = next(self.val_gen)
        except StopIteration:
            # if generator is exhausted; just keep returning the last value
            pass # TODO what *should* we return after running out of data?
        return self.curr_val["value"]
```

- The generator reads rows from the original file. 
- It finds the first non null value which comes after the current timestamp.
- Some nodes have a lot of missing data for the value parameter, Im not sure what to do if there are no near values for a timestamp. 
- 90% of the values are 0 (which means that the node is running correctly).

this is then fed to graph creation which outputs graphs to the model

#### Conversion of graphs
nx_to_torch.py converts networkx graphs to data for torch. It gives each node 2 features. First its value, second what type of a node it is, using one hot encodings from torch.
### Predictions
I used this as the starting point https://colab.research.google.com/drive/1I8a0DfQ3fI7Njc62__mVXUlcAleUclnb?usp=sharing#scrollTo=cNgkR8SRaU_P
And kept a lot of things the same for now, like number of hidden layers being 3, with each having 64 channels. 

Both the train and test methods first try to first recieve data from the queue. They then save that data, into a list. This data is then used when we try to train or test the model again.

The data is split into data for training, and data for testing. The model is then trained and tested multiple times. The results being outputed.