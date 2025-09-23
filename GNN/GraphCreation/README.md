## Graph Creation
### How to Use

1. Navigate to the **GraphCreation** directory.
2. Place your `.parquet` file into the `StateFiles` directory.
    - If the directory does not exist, create it.
    - You can use either:
        - Parquet files modified by the **Signal Processing** step (described below), or
        - The [original dataset files](https://zenodo.org/records/7541722).
3. Also place there the orignal `.parquet` for the nodes you wish to process (in the code these files are refered to as value files, or val files, because we mainly only use their value column). 
4. By default, the program reads the file named `state.parquet`.
    - To change this, modify the `state_file` variable at the top of the `main` function in **run_pipeline.py**.
#### Running
```bash
# (Optional) Create a virtual environment
pip install -r requirements.txt
python -m run_pipeline
```
### How it works
#### Read and emit
Pushes rows from the specified file, to the buffer in the format:
```{node: {node, timestamp, rack_id, metric1, metric2,...},... }```
 