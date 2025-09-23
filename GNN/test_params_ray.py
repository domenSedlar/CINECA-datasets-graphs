from ray import tune
from ray.tune.schedulers import ASHAScheduler
from ray.tune.search.optuna import OptunaSearch
import ray
from ray.tune.search import ConcurrencyLimiter

from main import run
from main import get_loader
from model.get_dataloaders import MyLoader


def trainable(config, r=None):
    ds_dict = r

    ds = MyLoader(None, None, None)
    ds.num_classes = ds_dict['num_classes']
    ds.num_node_features = ds_dict['num_node_features']

    ds.test_label_distribution = ds_dict['test_label_distribution']
    ds.train_label_distribution = ds_dict['train_label_distribution']
    ds.valid_label_distribution = ds_dict['valid_label_distribution']

    ds.test_dataset = ds_dict['test_dataset']
    ds.train_dataset = ds_dict['train_dataset']
    ds.valid_dataset = ds_dict['valid_dataset']

    ds.prev_y = ds_dict['prev_y']

    kwargs = {
        "dataset": ds,
        "hidden_channels": int(config["hidden_channels"]),
        "num_of_layers": int(config["num_of_layers"]),
        "llr": float(config["llr"]),
        "adjust_weights": bool(config["adjust_weights"]),
        "dropout": float(config["dropout"]),
        "pool_method": config["pool_method"],
        "aggr_method": config["aggr_method"],
    }

    score = run(**kwargs)  # your eval function
    tune.report({"score":score})



def main():
    print("in ray")
    ds = get_loader()

    ds_dict = {
        'train_buffer': None,
        'test_buffer': None,
        'valid_buffer': None,
        'num_classes': ds.num_classes,
        'num_node_features': ds.num_node_features,
        'test_label_distribution': ds.test_label_distribution,
        'train_label_distribution': ds.train_label_distribution,
        'valid_label_distribution': ds.valid_label_distribution,
        'test_dataset': ds.test_dataset,
        'train_dataset': ds.train_dataset,
        'valid_dataset': ds.valid_dataset,
        'prev_y': ds.prev_y
    }

    r = ray.put(ds_dict)

    
    # Define search space
    search_space = {
        "hidden_channels": tune.choice([64, 128, 256]),
        "num_of_layers": tune.randint(0, 4),
        "llr": tune.loguniform(0.00001, 0.01),
        "adjust_weights": tune.choice([False, True]),
        "dropout": tune.uniform(0.0, 0.5),
        "pool_method": tune.choice(["max", "mean"]),
        "aggr_method": tune.choice(["add", "mean", "max"]),
    }

    algo = OptunaSearch()
    # algo = ConcurrencyLimiter(algo, max_concurrent=4)
    train_model=tune.with_resources(trainable, {"cpu": 1})
    train_model = tune.with_parameters(train_model, r=r)

    tuner = tune.Tuner(
        train_model,
        param_space=search_space,
        tune_config=tune.TuneConfig(
            metric="score",
            mode="max",
            search_alg=algo,
            num_samples=100,
            trial_name_creator=lambda trial: f"trial_{trial.trial_id}",
            trial_dirname_creator=lambda trial: f"trial_{trial.trial_id}"
        ),
    )

    try:
        results = tuner.fit()
    except KeyboardInterrupt:
        print("Optimization manually stopped!")
        results = tuner.get_results()

    best_result = results.get_best_result(metric="score", mode="max")
    print("Best trial:")
    print("  Value (score):", best_result.metrics["score"])
    print("  Params:", best_result.config)

if __name__ == "__main__":
    main()