# CINECA-datasets-graphs
## Signal processing
**How to run the pipeline**

First download the dataset from [here](https://zenodo.org/records/7541722), and place it into *./SignalProcessing/TarFiles/*.
Then run:
```cmd
cd SignalProcessing
pip install -r requirements.txt

# to run the pipeline
python -m run_pipeline

# to see how different parameters affect the change detector
python -m evaluate_parameters.py > file.csv
```
