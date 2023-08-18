import itertools
import pathlib
import sys
from typing import Dict, List

from plan_parser import EvaluationPlan
import pygraphviz as pgv

def main():
    plan_path = pathlib.Path(sys.argv[1])

    if plan_path.name.endswith(".json"):
        plan = EvaluationPlan.parse_from_flink(plan_path.as_posix())
    else:
        plan = EvaluationPlan.parse(plan_path.as_posix())

    G = pgv.AGraph(directed=True, colorscheme="paired12")

    edges: Dict[str, Dict[str, set[str]]] = {}

    for node in plan.nodes.values():
        node_rates = [ e for e, r in node.eventRates.items() if r > 0 ]
        label = node.nodeId + ": " + ",".join(node_rates)
        if node.projections:
            if node_rates:
                label += ",\n"
            
            label += ",\n".join(node.projections.keys())

        G.add_node(node.nodeId, xlabel=label)

        if node.nodeId not in edges:
            from_node = {}
            edges[node.nodeId] = from_node
        else:
            from_node = edges[node.nodeId]

        for rule in node.forwardRules:
            for to in rule.toNodes:
                if to not in from_node:
                    from_node[to] = set()
                
                from_node[to].add(rule.projection + f" ({rule.fromNode})")
    
    colors = [ "blue", "red", "green", "blueviolet", "aqua", "aquamarine", "darkgreen", "coral", "deeppink", "gold", "navy", "yellow", "thistle1" ]
    color_i = 0
    for from_node_id, to_nodes in edges.items():
        for to_node_id, projs in to_nodes.items():
            primitives = [p for p in projs if len(p) == 1]
            complexes = [p for p in projs if len(p) > 1]
            edge_label = ",".join(primitives)
            if complexes:
                if primitives:
                    edge_label += ",\n"
                
                edge_label += ",\n".join(complexes)

            color = colors[color_i % len(colors)]
            G.add_edge(from_node_id, to_node_id, label=edge_label, fontcolor=color, color=color, len=1.5)
            color_i += 1

    G.layout()

    G.draw(plan_path.parent.joinpath("graph.png"))
    print(G.string())

if __name__ == "__main__":
    main()
