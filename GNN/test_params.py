from main import run
from main import get_loader
import optuna

def main():
    ds = get_loader()

    channels = [128, 64]
    layers = [0, 1]
    rate = [0.0001, 0.001, 0.01, 0.05]
    adjust_weights = [False, True]
    dropout = [0, 0.01, 0.1, 0.2]
    pool = ["max", "mean"]
    aggr = ["add", "mean", "max"]

    for c in channels:
        for l in layers:
            for llr in rate:
                for w in adjust_weights:
                    for dp in dropout:
                        for pm in pool:
                            for a in aggr:
                                kwargs = {
                                    "dataset":ds,
                                    "llr":llr,
                                    "adjust_weights": w,
                                    "dropout": dp,
                                    "pool_method": pm,
                                    "aggr_method": a,
                                    "num_of_layers":l, 
                                    "hidden_channels":c
                                }
                                run(**kwargs)
                                print("")
                                print(kwargs)
                                print("")
                                print("")

def meow():
    ds = get_loader()

    def objective(trial):
        kwargs = {
            "dataset": ds,
            "hidden_channels": trial.suggest_categorical("hidden_channels", [64, 128, 256]),
            "num_of_layers": trial.suggest_int("num_of_layers", 0, 3),
            "llr": trial.suggest_loguniform("llr", 0.00001, 0.01),
            "adjust_weights": trial.suggest_categorical("adjust_weights", [False, True]),
            "dropout": trial.suggest_uniform("dropout", 0.0, 0.5),
            "pool_method": trial.suggest_categorical("pool_method", ["max", "mean"]),
            "aggr_method": trial.suggest_categorical("aggr_method", ["add", "mean", "max"]),
        }
        
        score = run(**kwargs)  # e.g., validation accuracy
        return score

    study = optuna.create_study(direction="maximize")
    try:
        study.optimize(objective, n_trials=50)
    except KeyboardInterrupt:
        print("Optimization manually stopped!")
    print("Best trial:")
    print("  Value (score):", study.best_trial.value)
    print("  Params:", study.best_trial.params)

if __name__ == '__main__':
    meow()