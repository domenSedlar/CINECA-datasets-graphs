import tarfile
import io
import pandas as pd
from common.logger import Logger

logger = Logger(__name__)

# This file is deprecated, we're using node_sensor_manager.py to read the data

def read_tar_parquet(tar_path):
    """
    Yields each row (as a dict) from every .parquet file inside the tar archive.
    """
    with tarfile.open(tar_path, 'r') as tar:
        logger.info(f"Reading tar file: {tar_path}")
        for member in tar.getmembers():
            if member.isfile() and member.name.endswith('.parquet'):
                file_obj = tar.extractfile(member)
                if file_obj is not None:
                    # Read parquet file from file-like object
                    df = pd.read_parquet(io.BytesIO(file_obj.read()))
                    for _, row in df.iterrows():
                        yield row.to_dict() 


def data_for_node(node_id, tar_path):
    """
    Yields each row (as a dict) from every .parquet file inside the tar archive.
    """
    with tarfile.open(tar_path, 'r') as tar:
        for member in tar.getmembers():
            if member.isfile() and member.name.endswith('.parquet'):
                file_obj = tar.extractfile(member)
                if file_obj is not None:
                    # Read parquet file from file-like object
                    df = pd.read_parquet(io.BytesIO(file_obj.read()))
                    for _, row in df.iterrows():
                        if 'node' not in row or pd.isna(row['node']):
                            print(f"Row missing 'node': {row}")
                            continue

                        if str(row['node']) == str(node_id):
                            # Extract the parent directory (folder) name where the parquet file is located
                            row['sensor'] = member.name.rsplit('/', 2)[-2] if '/' in member.name else ''
                            yield row.to_dict()
