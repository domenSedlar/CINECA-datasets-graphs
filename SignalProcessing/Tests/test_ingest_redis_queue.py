from pipeline.reader import read_tar_parquet
from pipeline.ingest_redis_queue import IngestRedisQueue

def main():
    queue = IngestRedisQueue()
    
    tar_path = "TarFiles/20-04.tar"
    row_count = 0
    for row in read_tar_parquet(tar_path):
        row_count += 1
        queue.put(row)
        if row_count >= 5:
            break


if __name__ == "__main__":
    main() 