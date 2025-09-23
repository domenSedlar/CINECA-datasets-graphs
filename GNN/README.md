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

3. If using processed files, then also place there the original `.parquet` files for the nodes you wish to process (in the code these files are referred to as value files, or val files, because we mainly only use their value column).

4. Open the main.py in the `GNN` directory. Go to the main() function and enter in the correct values for the variables.

5. run:
```cmd
# (Optional) Create a virtual environment

pip install -r requirements.txt

python -m main
```

## Other programs
Description of the other programs found in the `GNN` directory.

### test_params_ray.py
- Uses ray tune to find the best hyper parameters for the model. 
- Run this the same way you run main.py
- While the test dataset won't affect the results of this program, make it small as it will still be loaded into memory.

`test_params.py` is the same thing but less efficient.
### test filter
*test_filter.py*
No longer used for anything. It was placed in the pipeline between graphcreation and the model to control how many graphs of a certain label will be present in the dataset.