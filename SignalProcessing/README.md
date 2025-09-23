## Signal processing

### How to Use

1. Navigate to the **SignalProcessing** directory.

2. Create a directory named `TarFiles`.

    - Place your `.tar` files inside.

    - Extract them so they become regular folders containing the `.parquet` files.

    - Dataset link: [Zenodo Record](https://zenodo.org/records/7541722)

3. Create two more directories:

    - `outputs` — will store processed parquet files.

    - `logs` — will store logs.

  

#### Configuration

  

At the top of the `run` function in **run_pipeline.py**, you can adjust:

- `limit_racks` (int | None) — Process only a specified rack by ID. `None` means process all racks as one.

- `limit_nodes` (int | None) — Limit the number of nodes for faster testing.

- `delta` (float, 0–1) — Sensitivity of change detection (ADWIN parameter).

- `clock` (int) — Frequency of checks (ADWIN parameter).

- `rows_in_mem` (int) — Number of rows loaded into memory at once.

- `bq_max_size` — Default is `2 * rows_in_mem`, number of items, each queue can hold

  

At the bottom of the file, there is also **an option to change which rack you process**, and an option to process all of them in parallel. See limit_racks, at the bottom of the file.

  

#### Running

```bash

# (Optional) Create a virtual environment

pip install -r requirements.txt

python -m run_pipeline

```

  

### How it works

#### File Reading

- `node_manager` reads parquet files from the `TarFiles` directory for the specified rack.

- It processes data in batches and pushes each batch into a queue.

- Each queue element contains one row of synchronized sensor readings for all nodes at a given timestamp.

- If a node has no data for a timestamp:

    - Readings are set to `None`, or

    - If available, replaced with the last known reading.

  

Format of a queue element:

  

```python

{

    node_id:{

    'timestamp': to_json_serializable_timestamp(self.current_time),

    'rack_id': str,

    'sensor_data': {'<metric name>': float}

    }

}

```

  

#### Outputs

- The program outputs one parquet file for the rack, into the outputs directory.

- row format:``` node, timestamp, rack_id, metric1, metric2, ..., metric_k```

- Each row contains a significant state for one of the nodes. The rows should be sorted by timestamp.

## Other tools

The tools directory stores a couple of programs used to gather data about the dataset.
*run* them by calling `python -m Tools.<tool name>`


*distribution_per_month.py*

Outputs the distribution of node states, for each month/node pair in the csv format.


*eval_diversity.py*

prints the distribution of node states for each node


*evaluate_parameters.py*

This program checks how the different parameters affect the change detection.


*last_month_distribution.py*

Checks what the value distribution is in the last month in the dataset
