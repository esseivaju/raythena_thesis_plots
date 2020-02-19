import matplotlib.pyplot as plt
from plt_utils import formatted_figure


def get_linear_coefs(x1, y1, x2, y2):
    a = (y2 - y1) / (x2 - x1)
    b = y1 - a * x1
    return a, b


if __name__ == "__main__":
    n_nodes = [50, 100, 200]
    n_cores = [x * 136 for x in n_nodes]
    data = n_nodes
    n_events = [31747, 65634, 118245]
    a, b = get_linear_coefs(data[0], n_events[0], data[1], n_events[1])
    linear_scale = [a * n + b for n in data]

    def wrapper(data, n_events, linear_scale):
        plt.plot(data, n_events, marker='o', markersize=7, label='Events processed', linewidth=3)
        plt.plot(data, linear_scale, label="linear scaling", linestyle='dashed', linewidth=3)

    formatted_figure(None, 16, "Cores", "Events processed (Full G4 Sim", "Events processed in 2-hours ES jobs on Cori KNL",
                     "scaling_events.png", 300, None, None, None, wrapper, data, n_events, linear_scale)