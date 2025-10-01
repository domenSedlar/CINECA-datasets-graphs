from train_on_changing_intervals import main as run
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import Manager


def run_wrapper(i, stop_event):
    if stop_event.is_set():
        return f"Task {i} skipped (stop event set)."
    return run(i, stop_event=stop_event)


def main(num_workers=4):
    print("num of workers:", num_workers)
    print("starting...")

    with Manager() as manager:
        stop_event = manager.Event()

        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            futures = {executor.submit(run_wrapper, i, stop_event): i for i in range(1000)}

            try:
                for f in as_completed(futures):
                    result = f.result()
                    if result is not None:
                        print(result)
            except KeyboardInterrupt:
                print("KeyboardInterrupt detected, stopping...")
                stop_event.set()
                for f in futures:
                    f.cancel()

    print("All tasks completed (or stopped).")


if __name__ == '__main__':
    main()