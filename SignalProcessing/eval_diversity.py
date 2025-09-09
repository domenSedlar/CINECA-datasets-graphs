import pyarrow.parquet as pq
import os

def eval(file):
    pq_file = pq.ParquetFile(file)
    
    all = 0
    nones = 0
    zeros = 0
    others = 0
    ones = 0
    twos = 0

    for batch in pq_file.iter_batches(batch_size=128, columns=["value"]):
        vals = batch.column("value")
        for row in vals:
            all += 1
            if not row.is_valid:
                nones += 1
            elif int(row) == 0:
                zeros += 1
            else:
                others += 1
                if int(row) == 1:
                    ones += 1
                elif int(row) == 2:
                    twos += 1
    if (all - nones) == 0:
        ratio = 1
    else:
        ratio = others/(all-nones)

    return [ratio,all,nones,zeros,others,ones,twos, "node_id= " + file.split('/')[-1].split('.')[0]]


def main():
    rootdir = './TarFiles/'
    li = []


    for subdir, dirs, files in os.walk(rootdir):
        for file in files:
            if str(file).endswith(".py"):
                continue
            li.append(eval(os.path.join(subdir, file)))
    
    for v in sorted(li):
        print(v)

if __name__ == "__main__":
    main()