import matplotlib.pyplot as plt
import numpy as np
import json
from plt_utils import formatted_figure


def plot_cpu_usage(y, save_to=None, title=None, xlabel=None, ylabel=None):
    percentile_25 = np.percentile(y, 25)
    percentile_50 = np.percentile(y, 50)
    percentile_75 = np.percentile(y, 75)
    title = f"{title}\n25-percentile: {percentile_25}, 50-percentile: {percentile_50}, 75-percentile: {percentile_75}"
    n = len(y)
    x = np.linspace(0, n, n)

    def wrapper(x, y):
        plt.plot(x, y, alpha=.6, lw=.3)
        plt.fill_between(x, y, alpha=.3)
    formatted_figure(None, 16, xlabel, ylabel, title, save_to, 300, None, None, False, wrapper, x, y)


if __name__ == "__main__":
    n_nodes = [50, 100, 199, 200]
    for n in n_nodes:
        with open(f"data/job_{n}_nodes/cpu_monitor_driver_{n}_nodes.json") as f:
            cpu_info = json.load(f)

        plot_cpu_usage(cpu_info['system_usage'], save_to=f"driver_cpu_system_{n}_nodes.png",
                       title="Driver node",
                       xlabel="Time (seconds)",
                       ylabel="System cpu utilization (% of total number of cores)")
        plot_cpu_usage(cpu_info['process_usage'], save_to=f"driver_cpu_process_{n}_nodes.png",
                       title="Driver process",
                       xlabel="Time (seconds)",
                       ylabel="Driver cpu utilization (% of a single core)")

        with open(f"data/job_{n}_nodes/cpu_monitor_worker_{n}_nodes.json") as f:
            cpu_info = json.load(f)

        plot_cpu_usage(cpu_info['system_usage'], save_to=f"worker_cpu_system_{n}_nodes.png",
                       title="Worker node",
                       xlabel="Time (seconds)",
                       ylabel="System cpu utilization (% of total number of cores)")
        plot_cpu_usage(cpu_info['process_usage'], save_to=f"worker_cpu_process_{n}_nodes.png",
                       title="Worker process",
                       xlabel="Time (seconds)",
                       ylabel="Worker cpu utilization (% of a single core)")