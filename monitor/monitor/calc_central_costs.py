from os import path
import sys
from typing import Optional, Tuple, Union
import networkx as nx
from control_message import TopologyConfig
from plan_parser import EvaluationPlan, PlanNode

def calc_centralized_cost(plan: EvaluationPlan, topology: TopologyConfig, central_node: Optional[str] = None, with_central_node: bool = False) -> Union[Tuple[int, str], int]:
    if central_node == None:
        central_node_rates = 0
        for node in plan.nodes.values():
            node_rates = sum(node.eventRates.values())
            if node_rates > central_node_rates:
                central_node = node.nodeId
                central_node_rates = node_rates

    if central_node == None:
        raise ValueError("Could not determine central node of plan.")

    edges = set()
    for node in topology.nodes.values():
        for conn in node.connections:
            conn_node = topology.ip_to_node[conn].id
            edges.add((node.id, conn_node))

    graph = nx.Graph(edges)

    central_costs = 0
    for node in plan.nodes.values():
        if node.nodeId == central_node:
            continue

        hops: int = nx.shortest_path_length(graph, node.nodeId, central_node)
        central_costs += hops * sum(node.eventRates.values())

    if with_central_node:
        return central_costs, central_node
    
    return central_costs


def main():
    if len(sys.argv) < 3:
        sys.stderr.write("Usage: calc_central_costs.py [plan_file] [topology_file]\n")
        exit(1)

    plan_path = sys.argv[1]
    if not path.isfile(plan_path):
        sys.stderr.write("Plan file does not exist.\n")
        exit(1)

    if plan_path.endswith('.json'):
        plan = EvaluationPlan.parse_from_flink(plan_path)
    else:
        plan = EvaluationPlan.parse(plan_path)

    topology_file = sys.argv[2]
    if not path.isfile(topology_file):
        sys.stderr.write("Topology file does not exist.\n")
        exit(1)

    topology = TopologyConfig.parse(topology_file)

    costs, central_node = calc_centralized_cost(plan, topology, with_central_node=True) # type: ignore

    print(f"Centralized cost with central node {central_node}:", costs)

if __name__ == "__main__":
    main()