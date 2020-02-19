import matplotlib.pyplot as plt
import numpy as np
import json
from dateutil.parser import parse

from plt_utils import formatted_figure

def get_actor_occupancy(actor):
    first_event = []
    wait_per_event = []
    for key, worker in actor.items():
        if key != "init":
            start_times = []
            stop_times = []
            for key, event in worker.items():
                if key != 'init':
                    start_times.append(parse(event['start time']))
                    if len(start_times) == 1:
                        first_event.append(start_times[0])
                    stop_times.append(parse(event.get('stop time', event['start time'])))

            assert len(start_times) == len(stop_times)
            if not start_times:
                continue
            start_times.sort()
            stop_times.sort()
            start_times.pop(0)
            prev_stop = stop_times.pop(0)
            deltas = []
            while start_times:
                cur_start = start_times.pop(0)
                delta = cur_start - prev_stop
                deltas.append(delta)
                wait_per_event.append(delta.total_seconds())
                prev_stop = stop_times.pop(0)
    return wait_per_event, first_event


def benchmark_job(n, start_time):
    with open(f"data/job_{n}_nodes/benchmark.json") as f:
        data: dict = json.load(f)

    wait_per_event = []
    start_delay = []
    for actor in data.values():
        w, f = get_actor_occupancy(actor)
        wait_per_event += w
        start_delay += f

    delta_start = []
    for worker_start in start_delay:
        delta_start.append((worker_start - start_time).total_seconds())
    print(f"Nevents: {len(wait_per_event)}")

    return wait_per_event, delta_start


if __name__ == "__main__":
    n_nodes = [50, 100, 199, 200]
    job_starts = [parse("2020-01-22 14:48:49,533"), parse("2020-01-21 05:29:43,884"), parse("2020-01-26 02:37:50,162"), parse("2020-01-25 16:29:36,593")]
    lat = []
    start_delay = []
    for nodes, start_time in zip(n_nodes, job_starts):
        l, d = benchmark_job(nodes, start_time)
        lat.append(l)
        start_delay.append(d)

    labels = [f"{n} nodes" for n in n_nodes]
    labels = [""] + labels
    ticks = [i for i in range(len(n_nodes)+1)]

    title_sz = 16
    dpi = 300

    formatted_figure(None, title_sz, "", "AthenaMP latency between events (s)",
                     "Latency between events in AthenaMP workers", "latency_box.png", dpi, ticks, labels, None,
                     plt.boxplot, np.array(lat), showfliers=False)

    formatted_figure(None, title_sz, "AthenaMP latency between events (s)", "Number of events",
                     "Latency between events in AthenaMP workers", "latency.png", dpi, None, None, True,
                     plt.hist, np.array(lat), label=labels, alpha=0.3)

    formatted_figure(None, title_sz, "", "Start delay, ray setup + AthenaMP init  (s)", "Ray + AthenaMP initialization",
                     "delay_box.png", dpi, ticks, labels, None, plt.boxplot, np.array(start_delay), showfliers=False)

    formatted_figure(None, title_sz, "Start delay, ray setup + AthenaMP init (s)", "AthenaMP workers count",
                     "Ray + AthenaMP initialization", "delay.png", dpi, None, None, True,
                     plt.hist, np.array(start_delay), label=labels, alpha=0.3)
