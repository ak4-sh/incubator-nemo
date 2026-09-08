#!/usr/bin/env python3
"""Validate repeated CloudLab VM worker invocation lifecycles from preserved logs."""

import argparse
import collections
import json
import re
from pathlib import Path


REACTIVATION_EVENT = re.compile(
    r"SPONGE_WORKER_REACTIVATION transition=([A-Z_]+).*?requestId=(\d+)"
)
INVOCATION_EVENT = re.compile(
    r"SPONGE_WORKER_INVOCATION backend=cloudlab-vm "
    r"transition=([A-Z_]+) requestId=(\d+).*?invocation=(\d+).*?reason=([A-Z_]+)"
)
TASK_EXECUTOR_ERROR = re.compile(r"No task executor\s+([^\s]+)")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--master-log-dir", required=True, type=Path)
    parser.add_argument("--worker-log-dir", required=True, type=Path)
    parser.add_argument("--minimum-reactivations-per-worker", type=int, default=0)
    parser.add_argument("--output-json", required=True, type=Path)
    return parser.parse_args()


def log_files(directory):
    if not directory.is_dir():
        return []
    return sorted(path for path in directory.rglob("*") if path.is_file())


def read_lines(paths):
    for path in paths:
        try:
            with path.open("r", encoding="utf-8", errors="replace") as stream:
                for line_number, line in enumerate(stream, start=1):
                    yield path, line_number, line
        except OSError:
            continue


def counter_to_dict(counter):
    return {str(key): value for key, value in sorted(counter.items())}


def invocation_counter_to_list(counter):
    return [
        {"requestId": key[0], "invocation": key[1], "count": count}
        for key, count in sorted(counter.items())
    ]


def main():
    args = parse_args()
    if args.minimum_reactivations_per_worker < 0:
        raise SystemExit("--minimum-reactivations-per-worker must be non-negative")

    master_paths = log_files(args.master_log_dir)
    worker_paths = log_files(args.worker_log_dir)
    errors = []

    if not master_paths:
        errors.append(f"no master logs found under {args.master_log_dir}")
    if not worker_paths:
        errors.append(f"no VM worker logs found under {args.worker_log_dir}")

    master_transitions = collections.defaultdict(collections.Counter)
    invocation_transitions = collections.defaultdict(collections.Counter)
    invocation_reasons = {}
    task_executor_errors = []

    for path, line_number, line in read_lines(master_paths):
        match = REACTIVATION_EVENT.search(line)
        if match:
            transition, request_id = match.groups()
            master_transitions[transition][int(request_id)] += 1

        match = INVOCATION_EVENT.search(line)
        if match:
            transition, request_id, invocation, reason = match.groups()
            key = (int(request_id), int(invocation))
            invocation_transitions[transition][key] += 1
            invocation_reasons[key] = reason

        match = TASK_EXECUTOR_ERROR.search(line)
        if match:
            task_executor_errors.append({
                "file": str(path),
                "line": line_number,
                "task": match.group(1),
            })

    for path, line_number, line in read_lines(worker_paths):
        match = INVOCATION_EVENT.search(line)
        if match:
            transition, request_id, invocation, reason = match.groups()
            key = (int(request_id), int(invocation))
            invocation_transitions[transition][key] += 1
            invocation_reasons[key] = reason

        match = TASK_EXECUTOR_ERROR.search(line)
        if match:
            task_executor_errors.append({
                "file": str(path),
                "line": line_number,
                "task": match.group(1),
            })

    requested = invocation_transitions["INVOCATION_REQUESTED"]
    started = invocation_transitions["INVOCATION_STARTED"]
    finished = invocation_transitions["INVOCATION_FINISHED"]

    if requested != started:
        errors.append("CloudLab invocation-request and invocation-start sets/counts differ")
    if any(count != 1 for count in requested.values()):
        errors.append("one or more CloudLab invocations were requested more than once")
    if any(count != 1 for count in started.values()):
        errors.append("one or more CloudLab invocations started more than once")
    if any(key not in started or count != 1 for key, count in finished.items()):
        errors.append("one or more finished CloudLab invocations lack exactly one matching start")

    activation_requested = master_transitions["ACTIVATION_REQUESTED"]
    activation_acknowledged = master_transitions["ACTIVATION_ACKNOWLEDGED"]
    if activation_requested != activation_acknowledged:
        errors.append("worker activation requests and activation acknowledgements differ")

    end_sent = master_transitions["END_SENT"]
    end_acknowledged = master_transitions["END_ACKNOWLEDGED"]
    if end_sent != end_acknowledged:
        errors.append("worker END messages and END acknowledgements differ")

    deferred = master_transitions["DEFERRED"]
    deferred_released = master_transitions["DEFERRED_RELEASED"]
    if deferred != deferred_released:
        errors.append("deferred worker activations and released deferred activations differ")

    reactivation_counts = collections.Counter()
    for key in requested:
        if invocation_reasons.get(key) == "REACTIVATION":
            reactivation_counts[key[0]] += 1

    if reactivation_counts != activation_requested:
        errors.append("logical worker activations and CloudLab reactivation invocations differ")

    finished_per_worker = collections.Counter()
    unfinished_per_worker = collections.defaultdict(list)
    for request_id, invocation in finished:
        finished_per_worker[request_id] += 1
    for request_id, invocation in started:
        if (request_id, invocation) not in finished:
            unfinished_per_worker[request_id].append(invocation)

    if finished_per_worker != end_acknowledged:
        errors.append("finished CloudLab invocations and END acknowledgements differ")
    for request_id, invocations in unfinished_per_worker.items():
        if len(invocations) > 1:
            errors.append(
                f"worker {request_id} has multiple unfinished invocations: {sorted(invocations)}"
            )
        requested_for_worker = [
            invocation
            for worker_id, invocation in requested
            if worker_id == request_id
        ]
        if requested_for_worker and max(invocations) != max(requested_for_worker):
            errors.append(
                f"worker {request_id} has a non-latest unfinished invocation: "
                f"unfinished={sorted(invocations)}, requested={sorted(requested_for_worker)}"
            )

    if args.minimum_reactivations_per_worker:
        deepest_reuse = max(reactivation_counts.values(), default=0)
        if deepest_reuse < args.minimum_reactivations_per_worker:
            errors.append(
                "no worker reached the required reuse depth: "
                f"observed={deepest_reuse}, "
                f"required={args.minimum_reactivations_per_worker}"
            )

    if task_executor_errors:
        errors.append(f"found {len(task_executor_errors)} 'No task executor' errors")

    payload = {
        "errors": errors,
        "invocations": {
            "finished": invocation_counter_to_list(finished),
            "requested": invocation_counter_to_list(requested),
            "started": invocation_counter_to_list(started),
        },
        "masterTransitions": {
            transition: counter_to_dict(counter)
            for transition, counter in sorted(master_transitions.items())
        },
        "minimumReactivationsPerWorker": args.minimum_reactivations_per_worker,
        "passed": not errors,
        "reactivationsPerWorker": counter_to_dict(reactivation_counts),
        "taskExecutorErrors": task_executor_errors,
    }
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    raise SystemExit(0 if payload["passed"] else 1)


if __name__ == "__main__":
    main()
