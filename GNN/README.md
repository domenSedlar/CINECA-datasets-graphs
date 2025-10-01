# GNN

This program trains the GNN on the provided data, and tests it. The GNN tires to predict if a node, given its current state, will crash in the next 15 minutes.

  

The program has two parts. The graph Creation part, which creates the graphs for the model to train on, and the part which sets up and uses the gnn. Both parts have a readme which describes them.

  

## How to use

1. Inside of the `GraphCreation` directory create a `StateFiles` folder

  

2. Place your `.parquet` files into the `StateFiles` directory.

  

    - If the directory does not exist, create it.

  

    - You can use either:

  

        - Parquet files modified by the **Signal Processing** step , or

  

        - The [original dataset files](https://zenodo.org/records/7541722).

  
    - The program assumes the file names will end with `rack{id}.parquet`. If they don't you'll need to provide the list of files yourself in the `main.py` file on line 74

3. Place the [original dataset files](https://zenodo.org/records/7541722) to `/SignalProcessing/TarFiles/` and extract them.

  

4. Open the main.py in the `GNN` directory. Go to the main() function and enter in the correct values for the variables.

  

5. run:

```cmd

# (Optional) Create a virtual environment

  

pip install -r requirements.txt

  

python -m main

```

  

## Other programs

Description of the other programs found in the `GNN` directory. Run them the same way main is ran.


### train_on_changing_intervals.py

- The program collects a list of valid months from a csv file. The invalid months were removed because there wasn't enough data for them, or because the node was turned of for the whole month.

- First 6 valid months for training dataset, then 3 for validation, 3 for test dataset. And we run the model on this and save the result.

- Then we shift for 3 months and repeat until use all the valid months.

- the node this runs for is specified in the functions parameter

### run_multicore.py

- runs train_on_changing_intervals for all nodes

- It runs the program on multiple cores.


### test_params_ray.py

- Uses ray tune to find the best hyper parameters for the model.

- Run this the same way you run main.py

- While the test dataset won't affect the results of this program, make it small as it will still be loaded into memory.

  

`test_params.py` is the same thing but less efficient.

### test filter

*test_filter.py*

No longer used for anything. It was placed in the pipeline between graphcreation and the model to control how many graphs of a certain label will be present in the dataset.