import pyarrow.parquet as pq
import os
import pyarrow.compute as pc
import pandas as pd

def eval(file):
    
    zeros = 0
    ones = 0
    pq_file = pq.ParquetFile(file)

    # --- Step 1: Get the max timestamp (last row, since sorted) ---
    last_batch = pq_file.read_row_groups([pq_file.num_row_groups - 1], columns=["timestamp"])
    max_ts = pc.max(last_batch["timestamp"]).as_py()
    final_year = max_ts.year
    final_month = max_ts.month

    # --- Step 2: iterate and count in final month ---
    for batch in pq_file.iter_batches(batch_size=128, columns=["value", "timestamp"]):
        ts = batch["timestamp"].cast("timestamp[ms]")  # cast removes tz

        # compute year/month arrays
        years = pc.year(ts)        # Array
        months = pc.month(ts)      # Array

        # build boolean mask
        mask_year = pc.equal(years, final_year)
        mask_month = pc.equal(months, final_month)
        mask = pc.and_(mask_year, mask_month)

        # filter values by mask
        values = pc.filter(batch["value"], mask)
        tss = pc.filter(batch["timestamp"], mask)

        # count zeros/ones
        zeros += pc.sum(pc.equal(values, 0)).as_py() or 0
        ones  += pc.sum(pc.equal(values,1)).as_py() or 0
        ones  += pc.sum(pc.equal(values,2)).as_py() or 0

        #print("node:", file.split('/')[-1].split('.')[0], "Year/month:", final_year, "/", final_month)
        #print(f"Zeros: {zeros}, Ones: {ones}")
        #print(f"Zeros: {100*zeros/(zeros+ones)}%, Ones: {100*ones/(zeros+ones)}%")
    if zeros + ones == 0:
        return ["Year/month:"+ str(final_year)+ "/"+ str(final_month),"node: " + file.split('/')[-1].split('.')[0]]
    print("node:", file.split('/')[-1].split('.')[0], "Year/month:", final_year, "/", final_month)
    print(f"Zeros: {zeros}, Ones: {ones}")
    print(f"Zeros: {100*zeros/(zeros+ones)}%, Ones: {100*ones/(zeros+ones)}%")
    return ["Zeros: " + str(zeros),"Ones: " + str(ones),"0 in %: "+str(100*zeros/(zeros+ones)),"Year/month:"+ str(final_year)+ "/"+ str(final_month),"node: " + file.split('/')[-1].split('.')[0]]

def main():
    eval(".\TarFiles\\34\\690.parquet")

if __name__ == "__main__":
    main()