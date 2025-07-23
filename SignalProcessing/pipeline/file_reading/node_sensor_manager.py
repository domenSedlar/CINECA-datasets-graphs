import tarfile
import io
import pandas as pd
from collections import defaultdict
from datetime import timedelta

class NodeSensorManager:
    def __init__(self, node_id, tar_path, rack_id, current_time=None, sensor_columns=None, timestamp_col='timestamp', interval_seconds=60*15): # manages sensor data for a single node
        self.node_id = node_id
        self.tar_path = tar_path
        self.rack_id = rack_id
        self.timestamp_col = timestamp_col
        self.interval = timedelta(seconds=interval_seconds)
        self.sensor_generator = None
        self.current_readings = None
        self.current_time = current_time - self.interval
        self._first_reading_yielded = False if current_time is None else True
        self.sensor_columns = sensor_columns
        self._prepare_generators()


    def _prepare_generators(self):
        import pyarrow.parquet as pq
        # Open the tar file (assume it's named '{node_id}.tar' and located at self.tar_path)
        tar_file_path = f"{self.tar_path}"
        parquet_filename = f"{self.node_id}.parquet"
        with tarfile.open(tar_file_path, 'r') as tar:
            member = tar.getmember(parquet_filename)
            file_obj = tar.extractfile(member)
            parquet_bytes = file_obj.read()

        # Use pyarrow to read the parquet file from bytes
        table = pq.ParquetFile(io.BytesIO(parquet_bytes))

        if self.sensor_columns is None:
            # Determine the first 3 sensor columns (excluding timestamp) - for testing we limit to only 3 columns so it runs faster, will be removed soon
            all_columns = table.schema.names
            self.sensor_columns = [col for col in all_columns if col != self.timestamp_col][:3]

        def row_generator():
            for batch in table.iter_batches(batch_size=100, columns=[self.timestamp_col] + self.sensor_columns):
                batch_df = batch.to_pandas()
                for _, row in batch_df.iterrows():
                    yield row

        self.sensor_generator = row_generator() # reads the data when called

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
        if not hasattr(self, '_buffered_row'):
            self._buffered_row = None
        if not hasattr(self, '_first_reading_yielded'):
            self._first_reading_yielded = False
        try:
            # On the very first call, yield the first row
            if not self._first_reading_yielded:
                next_row = next(self.sensor_generator)
                self.current_time = next_row[self.timestamp_col]
                self.current_readings = self._sanitize_sensor_values(next_row)
                self._first_reading_yielded = True
                return {
                    'timestamp': self.current_time,
                    'rack_id': self.rack_id,
                    'sensor_data': self.current_readings.copy() if self.current_readings else None
                }
            # Use buffered row if available, else get next from generator
            if self._buffered_row is not None:
                next_row = self._buffered_row
            else:
                next_row = next(self.sensor_generator)
            next_time = next_row[self.timestamp_col]
            expected_next_time = self.current_time + self.interval
            allowed_next_time = expected_next_time + timedelta(seconds=allowed_offset_seconds)
            if next_time > allowed_next_time: # if next reading is too far in the future, buffer the row for next interval
                # Buffer the row for future calls
                self._buffered_row = next_row
                sensor_data = {k: None for k in self._sensor_columns}
                self.current_time = expected_next_time
                return { # for now we return None, as we have no data for this interval
                    'timestamp': self.current_time,
                    'rack_id': self.rack_id,
                    'sensor_data': sensor_data
                }
            else:
                self._buffered_row = None
                self.current_time = next_time
                self.current_readings = self._sanitize_sensor_values(next_row)
                return {
                    'timestamp': self.current_time,
                    'rack_id': self.rack_id,
                    'sensor_data': self.current_readings.copy() if self.current_readings else None
                }
        except StopIteration:
            self.current_time = None
            self.current_readings = None
            return None