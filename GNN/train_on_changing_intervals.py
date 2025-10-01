import datetime
from main import get_loader, run
from model.my_model import MyModel

from dateutil.relativedelta import relativedelta
import csv

def get_valid_months(node):
    s = datetime.datetime.fromisoformat("2020-05-01 00:00:00+00:00").astimezone() # start of the dataset
    e = datetime.datetime.fromisoformat("2022-10-01 00:00:00+00:00").astimezone() # end of the dataset
    m = relativedelta(months=1)
    valid = []
    invalid = []


    with open('out2.csv', mode='r', newline='') as file:    
        reader = csv.reader(file)
        next(reader) # throw away the header
        for row in reader:
            if int(row[0]) == node:
                i = 0
                while s < e:
                    if int(row[1+i]) > 0:
                        invalid.append(s)
                    else:
                        valid.append(s)
                    s += m
                    i += 1
                
                valid.append(e)
                return (invalid, valid)

    return ([],[])


def main(node, stop_event=None):
    print("starting for node", node)
    node_ids = [node] # list here the nodes you wish to train on
    # Add the paths to the processed files containing data for the nodes here. (remove the paths which don't exist)
    state_file=[f"GraphCreation/StateFiles/threaded_pipeline_state_2025-08-10_09-30-02_rack{j}.parquet" for j in range(0, 45)]
    state_file.append("GraphCreation/StateFiles/threaded_pipeline_state_2025-08-10_09-30-02_other.parquet")

    m = 0
    
    target_date = datetime.datetime.fromisoformat("2022-10-01 00:00:00+00:00").astimezone()

    head = ['state_id', 'node', 'AUC', 'train_start_date', 'train_end_date', 'test_start_date', 'test_end_date', 'valid_start_date', 'valid_end_date']

    with open(str(node) + 'out3.csv', mode='w', newline='') as out_file:
        writer = csv.DictWriter(out_file, fieldnames=head)
        writer.writeheader()

        (invalid, valid) = get_valid_months(node_ids[0])
        print("got months")
        
        # first 6 valid months for training dataset, then 3 for validation, 3 for test dataset
        train_start_ts = valid[0]
        train_end_ts = valid[6]
        valid_start_ts = valid[6]
        valid_end_ts = valid[9]
        test_start_ts = valid[11]
        test_end_ts = valid[14]
        m = 3

        dataset = get_loader(train_start_ts, train_end_ts, test_start_ts, test_end_ts, valid_start_ts, valid_end_ts, node_ids, state_file, invalid=invalid)
        print("got dataset")
        model = MyModel(dataset, adjust_weights=False, dropout=0.002232071679031126, llr=0.002530762230047059, aggr_method='add', pool_method="mean", num_of_layers=3, hidden_channels=64)
        print("got model")
        # first train a little more
        auc = model.train(train_for=100)
        print("trained once")
        row = dict(zip(head, [0, node_ids[0],auc, train_start_ts, train_end_ts, test_start_ts, test_end_ts, valid_start_ts, valid_end_ts]))       
        writer.writerow(row)
        print("wrote")

        i = 1
        while test_end_ts < target_date:
            print("in while loop")
            if stop_event and stop_event.is_set():
                return
            
            if len(valid) <= 11 + m:
                break

            train_start_ts = valid[0+m]
            train_end_ts = valid[6+m]
            valid_start_ts = valid[6+m]
            valid_end_ts = valid[9+m]
            test_start_ts = valid[11+m]
            if len(valid) <= 14 +m:
                test_end_ts = target_date
            else:
                test_end_ts = valid[14+m]

            # We dont need to re read the same data
            new_dataset = get_loader(train_start_ts, train_start_ts, test_start_ts, test_end_ts, valid_start_ts, valid_end_ts, node_ids, state_file)
            new_dataset.train_dataset = dataset.test_dataset + dataset.valid_dataset
            new_dataset.train_label_distribution = [dataset.test_label_distribution[j] + dataset.valid_label_distribution[j] for j in range(dataset.num_classes)]

            dataset = new_dataset
            model.dataset = dataset
            auc = model.train(train_for=30)
            row = dict(zip(head, [i, node_ids[0],auc, train_start_ts, train_end_ts, test_start_ts, test_end_ts, valid_start_ts, valid_end_ts]))
            writer.writerow(row)
            i+=1
            m+=3
            print("m=",m)
            print()


if __name__ == '__main__':
    main(886)