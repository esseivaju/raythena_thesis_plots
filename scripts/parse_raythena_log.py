import re
from dateutil.parser import parse
import matplotlib.pyplot as plt
import numpy as np


def extract_timestamp(line):
    field_end = re.search("\|", line).span()[0] - 1
    return parse(line[:field_end])


def extract_actor(line):
    return re.search("Actor_([0-9]{1,3}\.){3}[0-9]{1,3}", line).group()


def is_line_driver_send_ranges_to_actor(line):
    res = re.search("Sending event ranges request to the driver", line)
    if res:
        return extract_actor(line)


def is_line_actor_received_ranges(line):
    res = re.search("Received response to event ranges request", line)
    if res:
        return extract_actor(line)


def is_line_pilot_payload_started(line):
    res = re.search("Pilot payload started with PID [0-9]+", line)
    if res:
        return extract_actor(line)


def is_line_handle_http_ranges_request(line):
    res = re.search("Started handling POST /server/panda/getEventRanges", line)
    if res:
        return extract_actor(line)


def is_line_sent_ranges_request_to_payload(line):
    res = re.search("Finished handling POST /server/panda/getEventRanges", line)
    if res:
        return extract_actor(line)


def extract_driver(line):
    return re.search("Driver_node:([0-9]{1,3}\.){3}[0-9]{1,3}", line).group()


def is_line_harvester_ranges_request(line):
    res = re.search("Sending event ranges request to harvester", line)
    if res:
        return extract_driver(line)


def is_line_harvester_ranges_reply(line):
    res = re.search("received reply to event ranges request", line)
    if res:
        return extract_driver(line)


def find_req_reply(file, req_pattern, rep_pattern):
    with open(file) as f:
        lines = f.readlines()
    requests_by_actor = dict()
    for line in lines:
        actor_id = req_pattern(line)
        if actor_id:
            if actor_id not in requests_by_actor:
                requests_by_actor[actor_id] = []
            timestamp = extract_timestamp(line)
            requests_by_actor[actor_id].append((timestamp, 'ranges_request'))

        actor_id = rep_pattern(line)
        if actor_id:
            if actor_id not in requests_by_actor:
                requests_by_actor[actor_id] = []
            timestamp = extract_timestamp(line)
            requests_by_actor[actor_id].append((timestamp, 'ranges_reply'))
    return requests_by_actor


def find_ranges_requests_to_harvester(file):
    return find_req_reply(file, is_line_harvester_ranges_request, is_line_harvester_ranges_reply)


def fing_ranges_requests_to_payload(file):
    return find_req_reply(file, is_line_handle_http_ranges_request, is_line_sent_ranges_request_to_payload)


def find_ranges_requests_to_driver(file):
    return find_req_reply(file, is_line_driver_send_ranges_to_actor, is_line_actor_received_ranges)


def get_deltas(data):
    deltas_driver = []
    for requests in data.values():
        while requests:
            req = requests.pop(0)
            if not requests:
                break
            rep = requests.pop(0)
            deltas_driver.append((rep[0] - req[0]).total_seconds())
    return deltas_driver

def find_pilot_start_times(file):
    with open(file) as f:
        lines = f.readlines()
    requests_by_actor = dict()
    for line in lines:
        actor_id = is_line_pilot_payload_started(line)
        if actor_id:
            requests_by_actor[actor_id] = extract_timestamp(line)
    return requests_by_actor

def get_raythena_start_time(file):
    with open(file) as f:
        line = f.readline()
        return extract_timestamp(line)


if __name__ == "__main__":
    deltas_driver_by_job = []
    deltas_payload_by_job = []
    deltas_harvester = []
    delta_payload_start = []
    delta_payload_start_raythena = []
    n_nodes = [50, 100, 199, 200]
    ray_cluster_setup = []
    jobs_start = [parse("2020-01-22 14:48:49,533"), parse("2020-01-21 05:29:43,884"), parse("2020-01-26 02:37:50,162"), parse("2020-01-25 16:29:36,593")]

    for n, start_time in zip(n_nodes, jobs_start):
        file = f"data/job_{n}_nodes/raythena.log"
        raythena_start = get_raythena_start_time(file)
        cluster_setup_time = raythena_start - start_time
        ray_cluster_setup.append(cluster_setup_time.total_seconds())
        requests_by_actor = find_ranges_requests_to_driver(file)
        req_to_payload_by_actor = fing_ranges_requests_to_payload(file)
        harvester_req = find_ranges_requests_to_harvester(file)

        pilot_start = find_pilot_start_times(file)

        payload_delta_job = []
        payload_delta_raythena = []
        for start in pilot_start.values():
            payload_delta_job.append((start - start_time).total_seconds())
            payload_delta_raythena.append((start - raythena_start).total_seconds())

        delta_payload_start_raythena.append(payload_delta_raythena)
        delta_payload_start.append(payload_delta_job)
        deltas_harvester.append(get_deltas(harvester_req))
        deltas_payload_by_job.append(get_deltas(req_to_payload_by_actor))
        deltas_driver_by_job.append(get_deltas(requests_by_actor))

    labels=[f"{n} nodes" for n in n_nodes]
    ticks = [i for i in range(len(n_nodes)+1)]
    plt.figure(dpi=300)
    plt.plot(n_nodes, ray_cluster_setup)

    plt.figure(dpi=300)
    plt.xlabel("Delay (s)")
    plt.ylabel("Actors count")
    plt.title(f"Time between Slurm job start and Pilot 2 start on KNL nodes")
    plt.hist(np.array(delta_payload_start), alpha=0.3, histtype='bar', label=labels)
    plt.legend()
    plt.savefig(f"pilot_start_delay.png", dpi="figure")

    plt.figure(dpi=300)
    plt.xlabel("")
    plt.ylabel("Delay (s))")
    plt.title(f"Time between Slurm job start and Pilot 2 start on KNL nodes")
    plt.boxplot(np.array(delta_payload_start), showfliers=False)
    plt.xticks(ticks, [""] + labels)
    plt.savefig(f"pilot_start_delay_box.png", dpi='figure')


    plt.figure(dpi=300)
    plt.xlabel("Delay (s)")
    plt.ylabel("Actors count")
    plt.title(f"Time between Raythena start and Pilot 2 start on KNL nodes")
    plt.hist(np.array(delta_payload_start_raythena), alpha=0.3, histtype='bar', label=labels)
    plt.legend()
    plt.savefig(f"pilot_start_delay_raythena.png", dpi="figure")

    plt.figure(dpi=300)
    plt.xlabel("")
    plt.ylabel("Delay (s))")
    plt.title(f"Time between Raythena start and Pilot 2 start on KNL nodes")
    plt.boxplot(np.array(delta_payload_start_raythena), showfliers=False)
    plt.xticks(ticks, [""] + labels)
    plt.savefig(f"pilot_start_delay_raythena_box.png", dpi='figure')

    plt.figure(dpi=300)
    plt.xlabel("Request latency (s)")
    plt.ylabel("Requests count")
    plt.title(f"Event ranges request latency between actors - driver on KNL nodes")
    plt.hist(np.array(deltas_driver_by_job), range=(0, 2), bins=10, alpha=0.3, histtype='bar', label=labels)
    plt.legend()
    plt.savefig(f"ranges_request_latency_020.png", dpi="figure")

    plt.figure(dpi=300)
    plt.xlabel("")
    plt.ylabel("Request latency (s)")
    plt.title(f"Event ranges request latency between actors - driver on KNL nodes")
    plt.boxplot(np.array(deltas_driver_by_job), showfliers=False)
    plt.xticks(ticks, [""] + labels)
    plt.savefig(f"ranges_request_latency_020_box.png", dpi='figure')

    plt.figure(dpi=300)
    plt.xlabel("Request latency (s)")
    plt.ylabel("Requests count")
    plt.title(f"Event ranges request latency between Raythena - Pilot 2 on KNL nodes")
    plt.hist(np.array(deltas_payload_by_job), alpha=0.3, histtype='bar', label=labels)
    plt.legend()
    plt.savefig(f"ranges_request_latency_http.png", dpi="figure")

    plt.figure(dpi=300)
    plt.xlabel("")
    plt.ylabel("Request latency (s)")
    plt.title(f"Event ranges request latency between Raythena - Pilot 2 on KNL nodes")
    plt.boxplot(np.array(deltas_payload_by_job), showfliers=False)
    plt.xticks(ticks, [""] + labels)
    plt.savefig(f"ranges_request_latency_http_box.png", dpi='figure')

    plt.figure(dpi=300)
    plt.xlabel("Request latency (s)")
    plt.ylabel("Requests count")
    plt.title(f"Event ranges request latency between Raythena - Harvester")
    plt.hist(np.array(deltas_harvester), alpha=0.3, histtype='bar', label=labels)
    plt.legend()
    plt.savefig(f"ranges_request_latency_harvester.png", dpi="figure")

    plt.figure(dpi=300)
    plt.xlabel("")
    plt.ylabel("Request latency (s)")
    plt.title(f"Event ranges request latency between Raythena - Harvester")
    plt.boxplot(np.array(deltas_harvester), showfliers=False)
    plt.xticks(ticks, [""] + labels)
    plt.savefig(f"ranges_request_latency_harvester_box.png", dpi='figure')

