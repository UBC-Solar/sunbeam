import subprocess

import fuse
from fuse import FUSE, Operations
import errno
import logging
from data_tools import InfluxClient, FluxQuery, FluxStatement
from dotenv import load_dotenv
from io import StringIO
from functools import lru_cache
logging.basicConfig(level=logging.DEBUG)


AVAILABLE_FILE_FORMATS = ['csv']

load_dotenv()


class InfluxDBFS(Operations):
    def __init__(self):
        self._influx_client = InfluxClient()
        super().__init__()

    @lru_cache()
    def _list_buckets(self) -> list[str]:
        return self._influx_client.get_buckets()

    @lru_cache(maxsize=30)
    def _query_file(self, bucket, measurement, field):
        query = FluxQuery().from_bucket(bucket).range(start="2024-07-07T02:23:57Z", stop="2024-07-07T02:34:15Z").filter(field=field, measurement=measurement)
        csv_buffer = StringIO()
        self._influx_client.query_dataframe(query).to_csv(csv_buffer, index=False)
        logging.debug(f"Querying: {bucket}:{measurement}:{field}...")
        return csv_buffer.getvalue()

    @lru_cache()
    def _list_measurements(self, bucket: str) -> list[str]:
        return self._influx_client.get_measurements_in_bucket(bucket)

    @lru_cache()
    def _list_fields(self, bucket: str, measurement: str) -> list[str]:
        return self._influx_client.get_fields_in_measurement(bucket, measurement)

    def readdir(self, path, fh):
        logging.debug(f"readdir called with path: {path}")
        parts = path.strip("/").split("/")
        if len(parts) == 1 and parts[0] == "":
            return ['.', '..'] + self._list_buckets()
        elif len(parts) == 1:
            return ['.', '..'] + self._list_measurements(parts[0])
        elif len(parts) == 2:
            return ['.', '..'] + AVAILABLE_FILE_FORMATS
        elif len(parts) == 3:
            return ['.', '..'] + [f"{field}.csv" for field in self._list_fields(parts[0], parts[1])]
        else:
            return ['.', '..']

    def getattr(self, path, fh=None):
        print(f"getattr called with path: {path}")

        # Handle root directory
        if path == '/':
            return {
                'st_mode': (0o755 | 0o040000),  # Directory mode
                'st_nlink': 2,
            }

        # Handle bucket directories
        path_elements = path.strip('/').split('/')
        bucket_name = path_elements[0]
        if bucket_name in self._list_buckets():
            if path == f'/{bucket_name}':
                return {
                    'st_mode': (0o755 | 0o040000),  # Directory mode
                    'st_nlink': 2,
                }
            # Handle measurements
            measurement_name = path_elements[1]
            if measurement_name in self._list_measurements(bucket_name):
                if len(path_elements) == 2:
                    return {
                        'st_mode': (0o755 | 0o040000),  # Directory mode
                        'st_nlink': 2,
                    }
                file_format = path_elements[2]
                if file_format in AVAILABLE_FILE_FORMATS:
                    if len(path_elements) == 3:
                        return {
                            'st_mode': (0o755 | 0o040000),  # Directory mode
                            'st_nlink': 2,
                        }
                    # Handle fields (CSV files)
                    field_name = path_elements[3].split(".")[0]
                    if field_name in self._list_fields(bucket_name, measurement_name):
                        data = self._query_file(bucket_name, measurement_name, field_name)
                        size = len(data.encode('utf-8'))
                        logging.debug(f"Query Cache: {self._query_file.cache_info()}")
                        logging.debug(f"{bucket_name}:{measurement_name}:{field_name} Size: {size}")
                        return {
                            'st_mode': (0o644 | 0o100000),  # File mode
                            'st_nlink': 1,
                            'st_size': len(data.encode('utf-8')),  # Example file size
                        }

        # Path does not exist
        raise fuse.FuseOSError(errno.ENOENT)

    def read(self, path, size, offset, fh):
        parts = path.strip("/").split("/")
        if len(parts) == 4:
            data = self._query_file(parts[0], parts[1], parts[3].split(".")[0])
            logging.debug(f"Query Cache: {self._query_file.cache_info()}")
            logging.debug(f"{parts[0]}:{parts[1]}:{parts[3]} Size: {len(data.encode('utf-8'))}")
            return data.encode('utf-8')
        else:
            return b""

    def listxattr(self, path):
        return []


# Mount the filesystem
if __name__ == '__main__':
    try:
        fuse = FUSE(InfluxDBFS(), '/Users/joshuariefman/Solar/influxdb', foreground=True)
    except Exception:
        subprocess.run(["umount /Users/joshuariefman/Solar/influxdb"])
