import tarfile
import io
import pandas as pd
from common.logger import Logger

logger = Logger(__name__)

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