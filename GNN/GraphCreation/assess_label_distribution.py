import pyarrow.parquet as pq
import os

def eval(file):
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
    rootdir = '.\\StateFiles\\19.parquet'
    eval(rootdir)

if __name__ == "__main__":
    main()