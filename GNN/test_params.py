from main import run

def main():
    oversampling = [1, 2, 4, 8]
    counter_weight = [1, 2, 4, 8]
    max_dist = [1, 4, 8, 16]

    for i in oversampling:
        for j in counter_weight:
            for k in max_dist:
                print("")
                print("")
                print("sampling: ", i, "weight: ", j, "dist: ", max_dist)
                run(oversampling=i,counter_weight=j, max_dist_scalar=k)

if __name__ == '__main__':
    main()