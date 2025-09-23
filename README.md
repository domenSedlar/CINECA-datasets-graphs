# CINECA-datasets-graphs

## Overview

This repository contains three programs for working with the [M100 dataset](https://zenodo.org/records/7541722):
1. **Signal Processing** — Reads raw sensor data, detects significant state changes, and outputs new parquet files.
2. **Graph Creation** — Reads processed state files and prepares them for graph-based analysis.
3. **GNN** — Trains and tests a model on the graphs, from graph creation

Each part is further described inside of their directories.

