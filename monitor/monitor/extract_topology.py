import argparse
from os import path
import sys
from json import JSONEncoder, dump
from typing import Dict, Set

from plan_parser import EvaluationPlan

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("plan", help="Path to json plan file")
    parser.add_argument("output", help="Output file path")
    parser.add_argument("-m", "--mode", choices=["local", "ips"], default="local")
    parser.add_argument("--ips", required=True, help="If local mode: One ip address, if ips mode: file of node ids and ips")

    args = parser.parse_args()

    if not path.isfile(args.plan):
        sys.stderr.write("Plan file not found.\n")
        exit(1)

    plan = EvaluationPlan.parse_from_flink(args.plan)
    
    ips = {}
    ip_to_id = {}
    port = 5001
    if args.mode == "ips":
        with open(args.ips, "r") as file:
            lines = file.readlines()
            for line in lines:
                parts = line.strip("\n").split(",")
                ips[parts[0]] = (parts[1], port)
                ip_to_id[parts[1] + ":" + str(port)] = parts[0]
                port += 1
    else:
        for node in plan.nodes:
            ips[node] = (args.ips, port)
            ip_to_id[args.ips + ":" + str(port)] = node
            port += 1

    topology: Dict[str, Dict] = {}
    control_port = 5101
    for node in plan.nodes.values():
        nodeIp = ips[node.nodeId]
        forwarding = {}
        connections: Set[str] = set()
        for rule in node.forwardRules:
            for dest in rule.toNodes:
                destIp = ips[dest]
                forwarding[dest] = destIp[0] + ":" + str(destIp[1])
                connections.add(forwarding[dest])

        topology[node.nodeId] = {
            "ip": nodeIp[0],
            "port": nodeIp[1],
            "control_port": control_port,
            "forwarding_table": forwarding,
            "connection": connections
        }

        control_port += 1

    for node in topology.values():
        for conn in node["connection"]:
            topology[ip_to_id[conn]]["connection"].add(node["ip"] + ":" + str(node["port"]))

    class SetEncoder(JSONEncoder):
        def default(self, obj):
            if isinstance(obj, set):
                return list(obj)
            
            return JSONEncoder.default(self, obj)

    with open(args.output, "w") as file:
        dump(dict(sorted(topology.items())), file, cls=SetEncoder)

    print("Wrote topology to", args.output)

if __name__ == "__main__":
    main()
