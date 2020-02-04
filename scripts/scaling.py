import matplotlib.pyplot as plt
import numpy as np
import seaborn as sbs
import json


def get_linear_coefs(x1, y1, x2, y2):
    a = (y2 - y1) / (x2 - x1)
    b = y1 - a * x1
    return a, b


if __name__ == "__main__":
    plt.figure(dpi=300)
    n_nodes = [50, 100, 200]
    n_events = [31747, 65634, 118245]
    a, b = get_linear_coefs(n_nodes[0], n_events[0], n_nodes[1], n_events[1])
    linear_scale = [a * n + b for n in n_nodes]
    plt.plot(n_nodes, n_events, marker='o', markersize=7, label='Events processed', linewidth=3)
    plt.plot(n_nodes, linear_scale, label="linear scaling", linestyle='dashed', linewidth=3)
    plt.legend()
    plt.xlabel("Nodes count")
    plt.ylabel("Events processed (Full G4 Sim)")
    plt.title("Events processed in 2-hours event service jobs on Cori KNL nodes")
    plt.savefig("scaling_events.png", dpi="figure")
    plt.show()