import tarfile
import io
import pandas as pd
from collections import defaultdict
from datetime import timedelta
import ctypes

from common.logger import Logger
logger = Logger(name=__name__.split('.')[-1], log_dir='logs').get_logger()

class NodeSensorManager:
    def __init__(self, node_id, tar_path=None, file_path=None, rack_id=None, current_time=None, sensor_columns=None, timestamp_col='timestamp', interval_seconds=60*15, expected_rows=None, rows_in_mem=10, temp_dir="D:/temp_parquet_files"): # manages sensor data for a single node
        self.node_id = node_id
        self.tar_path = tar_path
        self.rack_id = rack_id
        self.timestamp_col = timestamp_col
        self.interval = timedelta(seconds=interval_seconds)
        self.sensor_generator = None
        self.current_readings = None
        # Only subtract interval if current_time is not None
        if current_time is not None:
            self.current_time = current_time - self.interval
        else:
            self.current_time = None
        self._first_reading_yielded = False if current_time is None else True
        self.sensor_columns = sensor_columns
        self.expected_rows = expected_rows
        self.processed_rows = 0
        if tar_path is not None:
            self._prepare_generators_from_tar(rows_in_mem, temp_dir=temp_dir)
        elif file_path is not None:
            self._prepare_generators(rows_in_mem, file_path)

    def _prepare_generators_from_tar(self, rows_in_mem, temp_dir="D:/temp_parquet_files"):
        import tarfile
        import pyarrow.parquet as pq
        import gc
        import psutil
        import os
        import tempfile
        
        # Read the specific {node_id}.parquet file using temp files on D: drive
        tar_file_path = f"{self.tar_path}"
        parquet_filename = f"{self.node_id}.parquet"
        
        # Monitor memory before processing
        # process = psutil.Process(os.getpid())
        # mem_before = process.memory_info().rss / 1024 / 1024
        # logger.debug(f"Node {self.node_id}: Memory before processing: {mem_before:.2f}MB")
        
        with tarfile.open(tar_file_path, 'r') as tar:
            member = tar.getmember(parquet_filename)
            file_size = member.size
            # logger.debug(f"Node {self.node_id}: Processing parquet file of size {file_size/1024/1024:.2f}MB")
            
            # Create temp file on D: drive to avoid C: space issues
            os.makedirs(temp_dir, exist_ok=True)
            
            with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet', dir=temp_dir) as temp_file:
                # Stream the file content in chunks to avoid loading entire file into memory
                file_obj = tar.extractfile(member)
                if file_obj is None:
                    raise ValueError(f"Could not extract {parquet_filename} from {self.tar_path}")
                
                # Copy file in chunks to avoid memory spike
                chunk_size = 1024 * 1024  # 1MB chunks
                while True:
                    chunk = file_obj.read(chunk_size)
                    if not chunk:
                        break
                    temp_file.write(chunk)
                
                temp_file.flush()
                temp_file_path = temp_file.name
                del file_obj  # Close file handle immediately
            
            # Create streaming parquet reader with pyarrow
            pq_file = pq.ParquetFile(temp_file_path)
            
            # Determine sensor columns if not already set
            if self.sensor_columns is None:
                all_columns = pq_file.schema.names
                self.sensor_columns = [col for col in all_columns if col != self.timestamp_col][:3]
            
            # Use pyarrow streaming to avoid memory mapping
            def row_generator(pq_file, batch_size=rows_in_mem, logger=logger):
                pq_file  # Make pq_file accessible in this function
                try:
                    # Read in very small chunks using pyarrow iter_batches
                    for batch in pq_file.iter_batches(batch_size=batch_size, columns=[self.timestamp_col] + self.sensor_columns):
                        batch_df = batch.to_pandas()
                        logger.debug("fresh batch")
                        for _, row in batch_df.iterrows():
                            yield row
                        # Force garbage collection after each batch
                        gc.collect()
                        del batch_df
                        # Force C library to release memory back to OS periodically
                        try:
                            ctypes.CDLL("libc.so.6").malloc_trim(0)
                        except Exception as e:
                            pass  # malloc_trim not available on this system
                finally:
                    # Clean up temp file immediately after processing
                    try:
                        del pq_file  # Explicitly delete pyarrow file object
                        gc.collect()  # Force cleanup
                        # Force C library to release memory back to OS
                        try:
                            ctypes.CDLL("libc.so.6").malloc_trim(0)
                        except Exception as e:
                            pass  # malloc_trim not available on this system
                        # Delete temp file
                        try:
                            os.unlink(temp_file_path)
                        except:
                            pass
                    except:
                        pass
            
            self.sensor_generator = row_generator(pq_file)
            
            # Force garbage collection after setup
            gc.collect()
            
            # Monitor memory after processing
            # mem_after = process.memory_info().rss / 1024 / 1024
            # logger.debug(f"Node {self.node_id}: Memory after processing: {mem_after:.2f}MB (delta: {mem_after - mem_before:.2f}MB)")

    def _prepare_generators(self, rows_in_mem, par_file):
        import tarfile
        import pyarrow.parquet as pq
        import gc
        import psutil
        import os
        import tempfile
                
        pq_file = pq.ParquetFile(par_file)
        
        # Determine sensor columns if not already set
        if self.sensor_columns is None:
            all_columns = pq_file.schema.names
            self.sensor_columns = [col for col in all_columns if col != 'timestamp' and not (col.endswith('_min') or col.endswith('_max') or col.endswith('_std'))]
        
        # Use pyarrow streaming to avoid memory mapping
        def row_generator(pq_file, batch_size=rows_in_mem, logger=logger):
            pq_file  # Make pq_file accessible in this function
            try:
                # Read in very small chunks using pyarrow iter_batches
                for batch in pq_file.iter_batches(batch_size=batch_size, columns=[self.timestamp_col] + self.sensor_columns):
                    batch_df = batch.to_pandas()
                    logger.debug("fresh batch")
                    for _, row in batch_df.iterrows():
                        yield row
                    # Force garbage collection after each batch
                    gc.collect()
                    del batch_df
                    # Force C library to release memory back to OS periodically
                    try:
                        ctypes.CDLL("libc.so.6").malloc_trim(0)
                    except Exception as e:
                        pass  # malloc_trim not available on this system
            finally:
                # Clean up temp file immediately after processing
                try:
                    del pq_file  # Explicitly delete pyarrow file object
                    gc.collect()  # Force cleanup
                    # Force C library to release memory back to OS
                    try:
                        ctypes.CDLL("libc.so.6").malloc_trim(0)
                    except Exception as e:
                        pass  # malloc_trim not available on this system                    
                except:
                    pass
        
        self.sensor_generator = row_generator(pq_file)
        
        # Force garbage collection after setup
        gc.collect()
        
        # Monitor memory after processing
        # mem_after = process.memory_info().rss / 1024 / 1024
        # logger.debug(f"Node {self.node_id}: Memory after processing: {mem_after:.2f}MB (delta: {mem_after - mem_before:.2f}MB)")


    def _sanitize_sensor_values(self, row): # if the value is None, use the last valid value from current_readings when it exists
        import math
        import pandas as pd
        sanitized = {}
        for k in self.sensor_columns:
            v = row.get(k, None)
            if v is None or (isinstance(v, float) and math.isnan(v)) or (hasattr(pd, 'isna') and pd.isna(v)) or not isinstance(v, (int, float)):
                # Use last valid value from current_readings if available, else None
                if self.current_readings and k in self.current_readings and self.current_readings[k] is not None:
                    sanitized[k] = self.current_readings[k]
                else:
                    sanitized[k] = None
            else:
                sanitized[k] = v
                
        return sanitized

    def next_readings(self, allowed_offset_seconds=0):
        """
        Advance to the next sensor reading if it is within the allowed offset.
        If the next entry's timestamp is later than current_time + interval + allowed_offset_seconds,
        return a dict with all sensor keys set to None, do not update current_readings, but advance current_time by interval.
        The next call will check the same buffered row until the time catches up.
        """
        import pandas as pd
        if not hasattr(self, '_buffered_row'):
            self._buffered_row = None
        if not hasattr(self, '_first_reading_yielded'):
            self._first_reading_yielded = False

        # Convert timestamp to ISO format for JSON serialization
        def to_json_serializable_timestamp(ts):
            if ts is None:
                return None
            if isinstance(ts, (pd.Timestamp,)):
                return ts.isoformat()
            if hasattr(ts, 'isoformat'):
                return ts.isoformat()
            return str(ts)

        try:
            logger.debug("trying to read")
            # Use buffered row if available, else get next from generator
            if self._buffered_row is not None:
                next_row = self._buffered_row
                logger.debug("collected buffered row")
            else:
                next_row = next(self.sensor_generator)
                logger.debug("got row")
            next_time = next_row[self.timestamp_col]
            expected_next_time = self.current_time + self.interval
            allowed_next_time = expected_next_time + timedelta(seconds=allowed_offset_seconds)
            if next_time > allowed_next_time: # if next reading is too far in the future, buffer the row for next interval
                # Buffer the row for future calls
                self._buffered_row = next_row
                sensor_data = {k: None for k in self.sensor_columns}
                self.current_time = expected_next_time
                logger.debug("buffered row, and returned")
                return { # for now we return None, as we have no data for this interval
                    'timestamp': to_json_serializable_timestamp(self.current_time),
                    'rack_id': self.rack_id,
                    'sensor_data': sensor_data
                }
            else:
                self._buffered_row = None
                self.current_time = next_time
                logger.debug("sanitizing")
                self.current_readings = self._sanitize_sensor_values(next_row)
                self.processed_rows += 1
                logger.debug("returning")
                return {
                    'timestamp': to_json_serializable_timestamp(self.current_time),
                    'rack_id': self.rack_id,
                    'sensor_data': self.current_readings.copy() if self.current_readings else None # TODO I dont think we need to copy current readings here
                }
        except StopIteration:
            self.current_time = None
            self.current_readings = None
            return None

    def get_processed_row_count(self):
        return self.processed_rows

    def get_expected_row_count(self):
        return self.expected_rows