import pyarrow.parquet as pq
import os

def eval(file):
    """
        Counts the number of zeros, unknowns, and other values in the file.
        And prints out the distribution of them in the first half, and the second half of the file.
        format of the output:
            datetime num-of-known-values num-zeros num-ones zeros/all
        
        date time tells you when it stopped reading for this half.
    """
    pq_file = pq.ParquetFile(file)
    
    zeros = 0
    ones = 0
    nones = 0
    alls = 0

    mid = ((81847- 9258)//2)
    lts = None

    for batch in pq_file.iter_batches(batch_size=128, columns=["value", "timestamp"]):
        vals = batch.column("value")
        tss = batch.column("timestamp")

        for i, row in enumerate(vals):
            lts = tss[i]
            alls += 1
            if not row.is_valid:
                nones += 1
            elif int(row) == 3:
                nones += 1
            elif int(row) == 0:
                zeros += 1
            else:
                ones += 1
            
            if (alls - nones) == mid:
                print(tss[i], alls-nones, zeros, ones, zeros/(zeros+ones))
                zeros = 0
                ones = 0

    print(lts, alls-nones, zeros, ones, zeros/(zeros+ones))

            

def main():
    # Get the directory of the current file (main.py)
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    node_id = "19"
    rootdir = os.path.join(BASE_DIR, "StateFiles", f"{node_id}.parquet")

    eval(rootdir)

if __name__ == "__main__":
    main()