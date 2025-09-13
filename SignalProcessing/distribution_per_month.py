import pyarrow.parquet as pq
import os
import pyarrow.compute as pc
import pyarrow as pa
import pandas as pd

import pyarrow.parquet as pq
import pyarrow.compute as pc
from collections import defaultdict
import datetime
import json
import csv

def eval(file):
    # --- Step 1: Prepare range of months ---
    start = datetime.date(2020, 3, 1)
    end = datetime.date(2022, 9, 1)
    months_range = []
    cur = start
    while cur <= end:
        months_range.append((cur.year, cur.month))
        if cur.month == 12:
            cur = datetime.date(cur.year+1, 1, 1)
        else:
            cur = datetime.date(cur.year, cur.month+1, 1)


    # --- Step 2: Setup counters ---
    counts = defaultdict(lambda: {"zeros": 0, "ones": 0})


    pq_file = pq.ParquetFile(file)


    # --- Step 3: Iterate over batches ---
    for batch in pq_file.iter_batches(batch_size=128, columns=["value", "timestamp"]):
        ts = batch["timestamp"].cast("timestamp[ms]")

        years = pc.year(ts)
        months = pc.month(ts)

        for (y, m) in months_range:
            mask_year = pc.equal(years, y)
            mask_month = pc.equal(months, m)
            mask = pc.and_(mask_year, mask_month)

            values = pc.filter(batch["value"], mask)

            counts[(y, m)]["zeros"] += pc.sum(pc.equal(values, 0)).as_py() or 0
            # treat 1 and 2 as "ones"
            valid_values_array = pa.array([1,2])
            counts[(y, m)]["ones"] += pc.sum(pc.is_in(values, valid_values_array)).as_py() or 0


    # --- Step 4: Format results ---
    results = []
    node = file.split('\\')[-1].split('.')[0]
    for (y, m) in months_range:
        zeros = counts[(y, m)]["zeros"]
        ones = counts[(y, m)]["ones"]
        if zeros + ones == 0:
            continue
        else:
            results.append({
            "Zeros": zeros,
            "Ones": ones,
            "dist": 100*zeros/(zeros+ones),
            "Year/month":f"{y}/{m:02}",
            "node": int(node)
            })
    
    print(node)

    return results

def main():
    rootdir = '.\\TarFiles\\'
    li = []


    for subdir, dirs, files in os.walk(rootdir):
        for file in files:
            if str(file).endswith(".py"):
                continue
            result = eval(os.path.join(subdir, file))
            for res in result:
                li.append([res["dist"], res["Zeros"], res["Ones"], res["node"], res["Year/month"]])
    
    li = sorted(li)
    with open("data.csv", mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=';')
        
        # Write header row (excluding "value", "node", "timestamp")
        
        writer.writerow(["dist", "Zeros", "Ones", "node", "Year/month"])
        
        for row in li:
            writer.writerow(row)

if __name__ == "__main__":
    main()