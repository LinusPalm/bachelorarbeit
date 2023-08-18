from dataclasses import dataclass
import itertools
from json import dump, dumps, load
import logging
from typing import Dict, List, Tuple
from query_parser import extract_leafs, get_id_constraints, get_sequence_constraints, is_sub_projection, parse_query
import string

@dataclass
class ForwardRule():
    projection: str
    fromNode: str
    toNodes: List[str]

    def to_json(self):
        return { "event": self.projection, "from": self.fromNode, "to": self.toNodes }

@dataclass
class ProjectionPart():
    value: str
    sources: Dict[str, float]

    def __str__(self) -> str:
        return f"{self.value} {self.sources}"

    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, ProjectionPart):
            return super().__eq__(__o)
        elif isinstance(__o, str):
            return self.value == __o
        else:
            return False

@dataclass
class Projection():
    value: str
    selectivity: float
    parts: List[ProjectionPart]
    is_used: bool
    is_forwarded: bool

    @staticmethod
    def from_json(json: dict):
        if "inputs" in json:
            inputs = json["inputs"]
        else:
            inputs = [ json["input_1"], json["input_2"] ]

        return Projection(
            json["query_name"],
            json["selectivity"],
            [ ProjectionPart(input, {}) for input in inputs ],
            is_used=False,
            is_forwarded=False
        )

@dataclass
class PlanNode():
    nodeId: str
    forwardRules: List[ForwardRule]
    projections: Dict[str, List[Projection]]
    eventRates: Dict[str, int]

class EvaluationPlan():
    eventRates: List[Dict[str, int]]
    """ Event rates for each node. """
    
    totalEventRates: Dict[str, int] # Key: Primitive event
    """ Map of primitive event types to their total rate in the plan. """

    nodes: Dict[str, PlanNode]
    outputRates: Dict[str, float] # Key: Projection

    def __init__(self, eventRates: List[Dict[str, int]], nodes: Dict[str, PlanNode]) -> None:
        self.eventRates = eventRates
        self.nodes = nodes

        totalEventRates = {}
        for node in self.nodes.values():
            for event, rate in node.eventRates.items():
                if rate > 0:
                    if event not in totalEventRates:
                        totalEventRates[event] = rate
                    else:
                        totalEventRates[event] += rate

            all_proj = itertools.chain.from_iterable(node.projections.values())
            for proj_list in node.projections.values():
                for proj in proj_list:
                    proj.is_forwarded = any(r for r in node.forwardRules if r.fromNode == node.nodeId and r.projection == proj.value)
                    proj.is_used = any(p for p in all_proj if proj.value in p.parts)

        self.totalEventRates = totalEventRates
        self.outputRates = self.calc_output_rates(totalEventRates)
        self.add_input_sources(link_only=True)

    def find_source_projection(self, projection: str, node: PlanNode):
        if projection in node.projections:
            return node.projections[projection]

        for proj in node.projections:
            if is_sub_projection(projection, proj):
                return node.projections[proj]
            
        return None

    def add_input_sources(self, link_only = False):
        """
        Connects all forward rules with their destinations by adding sources and selectivities to projections at the destination.
        """

        # Inform forward rule targets about their sources
        for node in self.nodes.values():
            for proj in itertools.chain.from_iterable(node.projections.values()):
                for part in proj.parts:
                    if len(part.value) > 1:
                        if part.value in node.projections:
                            part.sources[node.nodeId] = node.projections[part.value][0].selectivity
                    else:
                        if node.eventRates[part.value] > 0:
                            part.sources[node.nodeId] = 1

            for rule in node.forwardRules:
                if link_only:
                    sourceId = node.nodeId
                    selectivity = 1
                else:
                    if len(rule.projection) > 1:
                        # Complex event
                        if rule.fromNode == node.nodeId:
                            source_proj = self.find_source_projection(rule.projection, node)
                            if source_proj == None:
                                raise RuntimeError(f"Could not find source for {rule.projection} on node {node.nodeId}")
                            
                            selectivity = source_proj[0].selectivity
                            sourceId = node.nodeId
                        else:
                            fromNode = self.nodes[rule.fromNode]
                            fromProj = rule.projection

                            prevNode = fromNode
                            while fromProj not in fromNode.projections:
                                subMatch = next(iter([p for p in fromNode.projections.keys() if is_sub_projection(rule.projection, p)]), None) 
                                if subMatch != None:
                                    fromProj = subMatch
                                    break

                                fromNode = self.nodes[next(r.fromNode for r in fromNode.forwardRules if r.projection == rule.projection)]
                                if fromNode == prevNode:
                                    raise KeyError(f"Could not find source for projection {rule}")
                                
                                prevNode = fromNode

                            selectivity = fromNode.projections[fromProj][0].selectivity
                            sourceId = fromNode.nodeId
                    else:
                        # Primitive events have selectivity 1
                        selectivity = 1
                        fromNode = node
                        while fromNode.eventRates[rule.projection] == 0:
                            fromNode = self.nodes[next(r.fromNode for r in fromNode.forwardRules if r.projection == rule.projection)]

                        sourceId = fromNode.nodeId
                        

                # Find receiving projections and add self as source
                for to in rule.toNodes:
                    for proj_list in self.nodes[to].projections.values():
                        for proj in proj_list:
                            for part in proj.parts:
                                if part.value == rule.projection:
                                    part.sources[sourceId] = selectivity
                                    break


    def calc_output_rates(self, totalEventRates: Dict[str, int]):
        """
        Calculates and returns the output rate for each projection using total event rates and selectivities.
        """
        outputRates: Dict[str, float] = {}

        # Shorter projections are parts of longer ones,
        # so we calculate them first and reuse their selectivities
        allProjs = sorted([p for node in self.nodes.values() for p in itertools.chain.from_iterable(node.projections.values()) ], key=lambda p: len(p.value))

        selectivities: Dict[str, float] = {}

        for proj in allProjs:
            if proj.value in outputRates:
                continue

            # Not 100% sure why this is needed to get the correct selectivity
            selectivity = proj.selectivity
            for part in proj.parts:
                if len(part.value) > 1:
                    try:
                        partSelectivity = selectivities[part.value]
                    except KeyError:
                        print(f"Projection:", proj.value, "FROM", [ p.value for p in proj.parts ])
                        raise

                    selectivity *= partSelectivity

            selectivities[proj.value] = selectivity
            
            parsed = parse_query(proj.value)
            outputRates[proj.value] = parsed.calc_rate(totalEventRates) * selectivity

        return outputRates

    """
    Removes a projection from the node and adds forward rules for its parts instead.

    If the node contains a forward rule for this local projection,
    the destinations of that rule are also updated to receive the parts instead.
    
    def redirect_projection(self, nodeId: str, projection: str):
        
        node = self.nodes[nodeId]
        toBeRemoved = node.projections[projection]

        if not any(p for p in itertools.chain.from_iterable(node.projections.values()) if projection in p.parts):
            # If the result of the removed projection is not used as an input
            # in another local projection, remove the projection completely
            node.projections.pop(projection)

        return self.forward_inputs(node, toBeRemoved, node.nodeId)"""


    def forward_inputs_of_other(self, node: PlanNode, projection: Projection, from_node_id: str, other_node_id: str):
        oldForward = next((r for r in node.forwardRules if r.projection == projection.value and r.fromNode == from_node_id), None)
        if oldForward != None:
            # Add a new forward rule for each input of the removed projection,
            # unless there already is a rule for the input 
            newRules = []
            for part in projection.parts:
                if not any(r for r in node.forwardRules if r.projection == part.value and r.fromNode == from_node_id):
                    newRules.append(ForwardRule(part.value, node.nodeId, oldForward.toNodes))

            node.forwardRules.remove(oldForward)
            node.forwardRules += newRules

            for_other = []
            for to in oldForward.toNodes:
                # Change the destinations of the old forward rule, 
                # to expect the inputs of the projection instead
                changes = self.apply_change(projection, fromNodeId=from_node_id, toNodeId=to)
                if to == other_node_id:
                    for_other = changes
            
            return (oldForward.toNodes, for_other)

        return ([], [])

    def forward_inputs(self, node: PlanNode, projection: Projection, fromNodeId: str) -> List[str]:
        return self.forward_inputs_of_other(node, projection, fromNodeId, "")[0]


    def apply_change(self, removedProj: Projection, fromNodeId: str, toNodeId: str):
        """
        Apply the following change to plan:
        toNode will no longer receive projection from fromNode.
        """

        projection = removedProj.value
        new_part_values = sorted([ p.value for p in removedProj.parts ])
        toNode = self.nodes[toNodeId]

        newProjections: Dict[str, List[Projection]] = {}
        changes: List[Tuple[Projection, bool]] = []

        same_projections = [ p for p in toNode.projections.get(projection, []) if sorted([ pv.value for pv in p.parts ]) == new_part_values ]
 
        has_same_projection = len(same_projections) > 0
        for same_proj in same_projections:
            for part in same_proj.parts:
                part.sources[fromNodeId] = 1.0

        new_parts = []
        for harmful_part in new_part_values:
            new_sources = { fromNodeId: 1.0 }
            if toNode.eventRates.get(harmful_part, 0) > 0 or harmful_part in toNode.projections:
                new_sources[toNodeId] = 1
            
            new_parts.append(ProjectionPart(harmful_part, new_sources))

        def remove_source_from_part(proj: Projection, part: ProjectionPart, source_id: str):
            if source_id in part.sources:
                part.sources.pop(source_id)
            else:
                logging.error(f"[plan_parser] Source node id {source_id} not in part sources of {part} of projection '{proj.value} from {[ p.value for p in proj.parts]}', Has same: {has_same_projection}")
        
        for proj in itertools.chain.from_iterable(toNode.projections.values()):
            affectedParts = [ p for p in proj.parts if p.value == projection ]
            for part in affectedParts:
                if has_same_projection:
                    remove_source_from_part(proj, part, fromNodeId)
                else:
                    if len(part.sources) == 1 and not any(p for p in self.nodes[fromNodeId].projections.get(part.value, []) if sorted([ pv.value for pv in p.parts ]) != new_part_values):
                        # This node is the only source, old projection can be removed safely
                        proj.parts.remove(part)
                        proj.parts += new_parts
                        changes.append((proj, True))
                    else:
                        # Multiple nodes supply this projection, can't just remove it
                        if len(part.sources) > 1:
                            remove_source_from_part(proj, part, fromNodeId)

                        proj_parts = new_parts + [ ProjectionPart(p.value, p.sources.copy()) for p in proj.parts if p != part ]
                        new_proj = Projection(proj.value, proj.selectivity, proj_parts, proj.is_used, proj.is_forwarded)
                        newProjections[proj.value] = toNode.projections[proj.value] + [ new_proj ]
                        
                        changes.append((proj, False))

        toNode.projections = { **toNode.projections, **newProjections }
        return changes

    def to_json(self, path: str):
        def convert_projection(proj: Projection):
            inputs = [ p.value for p in proj.parts ]

            return {
                "query_name": proj.value,
                "output_selection": list(set(extract_leafs(proj.value))),
                "inputs": inputs,
                "selectivity": proj.selectivity,
                "sequence_constraints": get_sequence_constraints(proj.value, inputs),
                "id_constraints": get_id_constraints(inputs),
                "predicate_checks": 1,
                "time_window_size": 60
            }

        with open(path, "w") as file:
            json = {
                "nodes": {
                    node.nodeId: {
                        "event_rates": node.eventRates,
                        "forward_rules": [ rule.to_json() for rule in node.forwardRules ],
                        "queries": [ convert_projection(proj) for proj in itertools.chain.from_iterable(node.projections.values()) ]
                    } for node in self.nodes.values()
                }
            }

            dump(json, file)

    def to_json_str(self):
        return dumps({
            "nodes": {
                node.nodeId: {
                    "forward_rules": [ rule.to_json() for rule in node.forwardRules ],
                    "queries": [ { "name": proj.value, "inputs": [ { "part": p.value, "sources": list(p.sources.keys()) } for p in proj.parts ] } for proj in itertools.chain.from_iterable(node.projections.values()) ]
                } for node in self.nodes.values()
            }
        })

    @staticmethod
    def parse(planFile: str):
        with open(planFile, "r") as f:
            eventRates: List[Dict[str, int]] = []
            line = f.readline()
            while not line.startswith("-"):
                eventRates.append({ string.ascii_uppercase[index]: int(rate) for (index, rate) in enumerate(line.split()) })
                line = f.readline()

            line = f.readline()

            while not line.startswith("~~"):
                line = f.readline()

            nodes: Dict[str, PlanNode] = {}
            while line == "~~\n":
                nodeId = f.readline()[4:-1]

                f.readline() # --
                f.readline() # Forward rules:

                forwardRules: List[ForwardRule] = []

                line = f.readline()
                while not line == "\n":
                    projSep = line.index("-")
                    projection = line[:projSep - 1].replace(" ", "")

                    # - [ETB:(A: node0);(B: ANY) ... FROM:[node0]
                    fromNodeStart = line.index("FROM:") + 10
                    fromNodeEnd = line.index("]", fromNodeStart)
                    fromNode = line[fromNodeStart:fromNodeEnd]

                    # FROM:[node0] TO:[node1;node2]
                    toNode = [id[4:] for id in line[fromNodeEnd + 6:-4].split(";")]

                    forwardRules.append(ForwardRule(projection, fromNode, toNode))
                    line = f.readline()

                projections: Dict[str, List[Projection]] = {}

                f.readline() # --
                projHeading = f.readline() # Projections to process:
                if projHeading != "\n":
                    line = f.readline()
                    while not line == "\n":
                        # SELECT ... FROM ... WITH selectionRate= 0.12345
                        fromIndex = line.index("FROM")
                        withIndex = line.index("WITH", fromIndex)

                        projection = line[7:fromIndex - 1].replace(" ", "")
                        fromParts = line[fromIndex + 5:withIndex - 1].split("; ")
                        selectivity = float(line[line.index("=", withIndex) + 2:-1])

                        parts = []
                        for i, part in enumerate(fromParts):
                            if len(part) == 1:
                                if eventRates[int(nodeId)][part] > 0:
                                    # Primitive events have selectivity 1
                                    eventSources = { nodeId: 1.0 }
                                else:
                                    eventSources = {}
                            else:
                                part = part.replace(" ", "")
                                if part in projections:
                                    eventSources = { nodeId: projections[part][0].selectivity }
                                else:
                                    eventSources = {}

                            parts.append(ProjectionPart(part, eventSources))

                        if projection in projections:
                            duplicates = projections[projection]
                            for duplicate in duplicates:
                                part_str = [ p.value for p in parts ]
                                if sorted([ p.value for p in duplicate.parts ]) == sorted(part_str):
                                    raise UserWarning(f"Duplicate projection on node {nodeId}: {projection} FROM {part_str}")

                        if projection not in projections:
                            projections[projection] = []

                        projections[projection].append(Projection(projection, selectivity, parts, False, False))

                        line = f.readline()
                
                nodes[nodeId] = PlanNode(nodeId, forwardRules, projections, eventRates[int(nodeId)])
                line = f.readline() # ~~ or \n

        return EvaluationPlan(eventRates, nodes)

    @staticmethod
    def parse_from_flink(planJsonFile: str):
        with open(planJsonFile, "r") as f:
            json: dict = load(f)

        nodes = {}
        rates: List[Dict[str, int]] = []
        for nodeId, node in json["nodes"].items():
            rates.append(node["event_rates"])

            queries = itertools.groupby(node["queries"], key=lambda i: i["query_name"])

            rules = []
            rule_ids = set()
            for r in node["forward_rules"]:
                rule = ForwardRule(r["event"], r["from"], r["to"])
                rule_id = (rule.projection, rule.fromNode, *sorted(rule.toNodes))
                if not rule_id in rule_ids:
                    rules.append(rule)

            nodes[nodeId] = PlanNode(
                nodeId,
                rules,
                { q[0]: [ Projection.from_json(qi) for qi in q[1] ] for q in queries },
                node["event_rates"]
            )

        return EvaluationPlan(rates, nodes)
