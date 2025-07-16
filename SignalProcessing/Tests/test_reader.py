from pipeline.reader import read_tar_parquet

def main():
    tar_path = "TarFiles/20-04.tar"
    row_count = 0
    for row in read_tar_parquet(tar_path):
        print(row)
        row_count += 1
        if row_count >= 5:
            break

if __name__ == "__main__":
    main() 