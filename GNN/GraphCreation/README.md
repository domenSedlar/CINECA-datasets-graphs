# Graph Creation

## How to Use

  

1. Navigate to the **GraphCreation** directory.

2. Place your `.parquet` files into the `StateFiles` directory. (these files will be referred to as state files)

    - If the directory does not exist, create it.

    - You can use either:

        - Parquet files modified by the **Signal Processing** step (described below), or

        - The [original dataset files](https://zenodo.org/records/7541722).

3. If using processed files, then also place there the original `.parquet` files for the nodes you wish to process (in the code these files are referred to as value files, or val files, because we mainly only use their value column).

4. Open the file run pipeline inside of the main function. Replace the run() line with the commented out line. And pass in the list of paths for the files you placed inside of the StateFiles folder. (You can check the default parameter values for an example)
### Running

```bash

# (Optional) Create a virtual environment

pip install -r requirements.txt

python -m run_pipeline

```

## How it works

### Read and emit

Pushes rows from the specified file, to the buffer in the format:

```{node: {node, timestamp, rack_id, metric1, metric2,..., value},... }```


The read and emit takes state files, and val files for parameters.

All columns except the value column are taken from the state files. The value column is taken from the val files, using a method called `next_val`. This method finds what the node next state will be in 15 minutes after the timestamp. If this value is None, it will either return the next closest value, or None if the next closest value is too far(what is too far is determined by a parameter).


### Graph builder
- Recieves sensor data for nodes from the buffer.
- Updates node values, if the graph exists, or creates a graph.
  
#### Graph types
This class can create graphs of different shapes

*Note: In this readme 'node' will refer to different computing nodes inside of the computer.*
*while only 'vertex' or 'point' will refer to any point on a graph*


**RackClique** 

This graph type joins multiple nodes and racks into one graph.

*shape*:
- the rack points form a clique.
- nodes are connected to their respective racks
- each node has multiple points connecting to it which represent different sensor types
- For every sensor value, for a given node, there is a vertex connected to its sensor type, for that node.

**NodeClique**

*shape*:
- the node points form a clique
- each sensor point is connected to the node it belongs to

**NodeTree**

This graph creates one graph for each node.

*shape*:
- sensor points are connected to the node point which is the root. 

*features*:
- graph value ~ value column recieved from the buffer
- for sensor points
	- sensor type
	- value
- for the node
	- position


gnn expects all the nodes to have the same features, so when converting to pytorch graphs we make sure all the nodes have the same data.

## Other tools
The `Tools` directory, stores a couple of programs used to gather data about the dataset.
*run* them by calling `python -m Tools.<tool name>`


**assess_label_distribution.py**
- Counts the number of zeros, unknowns, and other values in the file.
- And prints out the distribution of them in the first half, and the second half of the file.
- format of the output: `datetime num-of-known-values num-zeros num-ones zeros/all`
- date time tells you when it stopped reading for this half.

**get_csv.py**

Reads the data inside of `StateFiles` directory, from the specified time interval, and writes it to a csv file.
At the bottom of the file you can change:
- the time interval
- the paths where the data is
- which nodes we collect data for
- how the csv file is named

**get_time_diff_csv.py**
- For each row in the state parquet files, it checks where the closest valid value is (value column tells us if the node is running) in the original.
- It saves this data to a csv file
- At the start of the run function edit the variables to change which nodes we check the time interval we check, and name of the output file.
