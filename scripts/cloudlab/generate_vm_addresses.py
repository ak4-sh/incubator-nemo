#!/usr/bin/env python3
import argparse


def main():
    parser = argparse.ArgumentParser(description="Generate vm_addresses.txt for CloudLab VM workers")
    parser.add_argument("--nodes", nargs="+", required=True)
    parser.add_argument("--first-port", type=int, default=25321)
    parser.add_argument("--workers-per-node", type=int, default=32)
    parser.add_argument("--output", default="vm_addresses.txt")
    args = parser.parse_args()

    lines = []
    # Support comma-separated node lists passed as a single string
    nodes = []
    for n in args.nodes:
        nodes.extend([x.strip() for x in n.split(",") if x.strip()])
    for node in nodes:
        for i in range(args.workers_per_node):
            worker_id = len(lines)
            lines.append(f"{node}:{args.first_port + i},cloudlab-vm-{worker_id}")

    with open(args.output, "w") as f:
        f.write("\n".join(lines) + "\n")

    print(f"Generated {args.output}: {len(lines)} workers")


if __name__ == "__main__":
    main()
